import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test, { type TestContext } from "node:test";
import { promisify } from "node:util";

import Database from "better-sqlite3";

import { AppServerRpcError, CodexAppServer } from "../src/app-server.js";
import type { AccessMode } from "../src/config.js";
import {
  addManagedThreadTask,
  addWorkspaceTask,
  DaemonRequestError,
  getQueueStatus,
  pauseQueue,
  resumeQueueRun,
  startDaemon,
  startQueueRun,
  type AppServerController,
  type CompletedTurn,
  type ThreadRecoverySnapshot,
  type UnattendedRequest,
} from "../src/daemon.js";
import { resolvePaths } from "../src/paths.js";
import { createFakeCodex } from "./fake-codex.js";

const DEFAULT_CONTINUATION_PROMPT =
  "Inspect the current Thread and Workspace state, continue the unfinished Task, and do not repeat work that is already complete.";
const execFileAsync = promisify(execFile);
const cliPath = path.resolve(new URL("../src/cli.js", import.meta.url).pathname);

interface RateLimitSnapshot {
  limitId: string;
  primary: { resetsAt: number | null; usedPercent: number } | null;
  rateLimitReachedType: string | null;
  secondary: { resetsAt: number | null; usedPercent: number } | null;
}

interface RateLimits {
  rateLimits: RateLimitSnapshot | null;
  rateLimitsByLimitId: Record<string, RateLimitSnapshot> | null;
}

class ManualClock {
  readonly scheduledWaits: string[] = [];
  #now: Date;
  #waiters: Array<{
    resolve(): void;
    signal: AbortSignal;
    until: Date;
  }> = [];

  constructor(now: string) {
    this.#now = new Date(now);
  }

  now(): Date {
    return new Date(this.#now);
  }

  waitUntil(until: Date, signal: AbortSignal): Promise<void> {
    this.scheduledWaits.push(until.toISOString());
    if (until <= this.#now) return Promise.resolve();
    return new Promise((resolve, reject) => {
      const waiter = { resolve, signal, until };
      this.#waiters.push(waiter);
      signal.addEventListener("abort", () => {
        this.#waiters = this.#waiters.filter((candidate) => candidate !== waiter);
        reject(signal.reason);
      }, { once: true });
    });
  }

  advanceTo(now: string): void {
    this.#now = new Date(now);
    const ready = this.#waiters.filter((waiter) => waiter.until <= this.#now);
    this.#waiters = this.#waiters.filter((waiter) => waiter.until > this.#now);
    for (const waiter of ready) {
      if (!waiter.signal.aborted) waiter.resolve();
    }
  }
}

class FakeAppServer implements AppServerController {
  readonly quotaPauseOnTurnNumbers = new Set<number>();
  readonly rateLimitReads: Array<Error | RateLimits> = [];
  readonly resumedThreads: string[] = [];
  readonly turnStartErrors: Array<Error | undefined> = [];
  readonly turnAccess: Array<{ accessMode: AccessMode; workspace: string }> = [];
  readonly turns: Array<{ prompt: string; threadId: string; turnId: string }> = [];
  recoveryThread: ThreadRecoverySnapshot | undefined;
  startThreadCalls = 0;
  startThreadWait: Promise<void> | undefined;
  #completedListeners = new Set<(turn: CompletedTurn) => void>();
  #usageLimitListeners = new Set<
    (event: { threadId: string; turnId: string }) => void
  >();
  #unattendedRequestListeners = new Set<
    (request: UnattendedRequest) => void
  >();

  async startAndProbe() {
    return { state: "ready", codexVersion: "codex-cli quota-test" } as const;
  }

  async close() {}

  async readThread(): Promise<{ threadId: string; workspace: string }> {
    throw new Error("not used");
  }

  async readThreadForReconciliation(): Promise<ThreadRecoverySnapshot> {
    if (!this.recoveryThread) throw new Error("recovery Thread is unavailable");
    return this.recoveryThread;
  }

  async resumeThread(threadId: string) {
    this.resumedThreads.push(threadId);
    return { threadId };
  }

  async startThread() {
    this.startThreadCalls += 1;
    await this.startThreadWait;
    return { threadId: "thread-quota" };
  }

  async startTurn(
    threadId: string,
    prompt: string,
    workspace?: string,
    accessMode?: AccessMode,
  ) {
    const error = this.turnStartErrors.shift();
    if (error) throw error;
    const turnId = `turn-${this.turns.length + 1}`;
    this.turns.push({ prompt, threadId, turnId });
    if (workspace && accessMode) this.turnAccess.push({ accessMode, workspace });
    if (this.quotaPauseOnTurnNumbers.has(this.turns.length)) {
      this.emitUsageLimitExceeded(threadId, turnId);
    }
    return { turnId };
  }

  async interruptTurn(): Promise<void> {}

  async readRateLimits(): Promise<RateLimits> {
    const next = this.rateLimitReads.shift();
    if (!next) throw new Error("unexpected rate-limit read");
    if (next instanceof Error) throw next;
    return next;
  }

  onTurnCompleted(listener: (turn: CompletedTurn) => void): () => void {
    this.#completedListeners.add(listener);
    return () => this.#completedListeners.delete(listener);
  }

  onTurnStarted(): () => void {
    return () => undefined;
  }

  onUsageLimitExceeded(
    listener: (event: { threadId: string; turnId: string }) => void,
  ): () => void {
    this.#usageLimitListeners.add(listener);
    return () => this.#usageLimitListeners.delete(listener);
  }

  onUnattendedRequest(
    listener: (request: UnattendedRequest) => void,
  ): () => void {
    this.#unattendedRequestListeners.add(listener);
    return () => this.#unattendedRequestListeners.delete(listener);
  }

  onUnexpectedExit(): () => void {
    return () => undefined;
  }

  emitUsageLimitExceeded(threadId: string, turnId: string): void {
    for (const listener of this.#usageLimitListeners) listener({ threadId, turnId });
  }

  emitCompleted(threadId: string, turnId: string): void {
    for (const listener of this.#completedListeners) {
      listener({ status: "completed", threadId, turnId });
    }
  }

  emitFailed(
    threadId: string,
    turnId: string,
    codexErrorInfo: unknown,
    message: string,
  ): void {
    for (const listener of this.#completedListeners) {
      listener({
        error: { codexErrorInfo, message },
        status: "failed",
        threadId,
        turnId,
      });
    }
  }

  emitInterrupted(threadId: string, turnId: string): void {
    for (const listener of this.#completedListeners) {
      listener({ status: "interrupted", threadId, turnId });
    }
  }

  emitUnattended(request: UnattendedRequest): void {
    for (const listener of this.#unattendedRequestListeners) listener(request);
  }
}

test("Full Access applies to every Task and Continuation in its Queue Run", async (t) => {
  const { appServer, clock, paths, workspace } = await createEnvironment(t);
  appServer.rateLimitReads.push(
    rateLimits({
      codex: bucket("codex", "rate_limit_reached", 100, epoch("2026-09-04T10:05:00.000Z")),
    }),
    rateLimits({ codex: bucket("codex", null, 10, null) }),
  );
  await addWorkspaceTask(paths, workspace, "Continue with Full Access");

  await startQueueRun(paths, { kind: "until_idle" }, "full");
  appServer.emitUsageLimitExceeded("thread-quota", "turn-1");
  await waitForTaskState(paths, "waiting_for_quota");
  clock.advanceTo("2026-09-04T10:05:02.000Z");
  await waitForTurnCount(appServer, 2);

  assert.deepEqual(appServer.turnAccess, [
    { accessMode: "full", workspace },
    { accessMode: "full", workspace },
  ]);
  assert.equal((await getQueueStatus(paths)).queueRun?.accessMode, "full");
});

test("an Until Idle Queue Run is persisted and visible through status", async (t) => {
  const { paths, workspace } = await createEnvironment(t);

  await addWorkspaceTask(paths, workspace, "Keep going until idle");
  await startQueueRun(paths, { kind: "until_idle" });

  const snapshot = await getQueueStatus(paths);
  assert.deepEqual(snapshot.queueRun, {
    accessMode: "configured",
    id: 1,
    runPolicy: "until_idle",
    startedAt: "2026-09-04T10:00:00.000Z",
  });
  assert.equal(snapshot.pauseReason, undefined);
});

test("an Until Idle Queue Run is durably ended when the Queue becomes idle", async (t) => {
  const { appServer, paths, workspace } = await createEnvironment(t);

  await addWorkspaceTask(paths, workspace, "Complete this Queue Run");
  await startQueueRun(paths, { kind: "until_idle" });
  appServer.emitCompleted("thread-quota", "turn-1");
  await settle();

  const snapshot = await getQueueStatus(paths);
  assert.equal(snapshot.state, "idle");
  assert.equal(snapshot.queueRun?.endedAt, "2026-09-04T10:00:00.000Z");
});

test("Cutoff Time pauses after the active Turn without starting the next Task", async (t) => {
  const { appServer, clock, paths, workspace } = await createEnvironment(t);

  await addWorkspaceTask(paths, workspace, "Finish the active work");
  await addWorkspaceTask(paths, workspace, "Do not start this work");
  await startQueueRun(paths, {
    cutoffTime: "2026-09-04T10:05:00.000Z",
    kind: "cutoff_time",
  });
  assert.equal(appServer.turns.length, 1);

  clock.advanceTo("2026-09-04T10:05:00.000Z");
  await settle();
  let snapshot = await getQueueStatus(paths);
  assert.equal(snapshot.state, "paused");
  assert.equal(snapshot.pauseReason, "cutoff_reached");
  assert.equal(snapshot.tasks[0]?.state, "running");
  assert.equal(snapshot.tasks[1]?.state, "queued");
  await pauseQueue(paths);
  assert.equal((await getQueueStatus(paths)).pauseReason, "cutoff_reached");

  appServer.emitCompleted("thread-quota", "turn-1");
  await settle();
  snapshot = await getQueueStatus(paths);
  assert.equal(snapshot.state, "paused");
  assert.equal(snapshot.tasks[0]?.state, "completed");
  assert.equal(snapshot.tasks[1]?.state, "queued");
  assert.equal(appServer.turns.length, 1);
  assert.deepEqual(snapshot.queueRun, {
    accessMode: "configured",
    cutoffTime: "2026-09-04T10:05:00.000Z",
    endedAt: "2026-09-04T10:05:00.000Z",
    id: 1,
    runPolicy: "cutoff_time",
    startedAt: "2026-09-04T10:00:00.000Z",
  });
});

test("manual pause lets the active Turn finish and resume starts a new Queue Run", async (t) => {
  const { appServer, paths, workspace } = await createEnvironment(t);

  await addWorkspaceTask(paths, workspace, "Finish before pausing");
  await startQueueRun(paths, { kind: "until_idle" });
  await addManagedThreadTask(paths, "thread-quota", "Start after resume");
  await pauseQueue(paths);

  let snapshot = await getQueueStatus(paths);
  assert.equal(snapshot.state, "paused");
  assert.equal(snapshot.pauseReason, "manual");
  assert.equal(snapshot.tasks[0]?.state, "running");
  assert.equal(appServer.turns.length, 1);

  appServer.emitCompleted("thread-quota", "turn-1");
  await settle();
  snapshot = await getQueueStatus(paths);
  assert.equal(snapshot.state, "paused");
  assert.equal(snapshot.tasks[0]?.state, "completed");
  assert.equal(snapshot.tasks[1]?.state, "queued");
  assert.equal(appServer.turns.length, 1);

  await resumeQueueRun(paths, { kind: "until_idle" });
  await waitForTurnCount(appServer, 2);
  snapshot = await getQueueStatus(paths);
  assert.equal(snapshot.state, "running");
  assert.equal(snapshot.queueRun?.id, 2);
  assert.equal(snapshot.queueRun?.runPolicy, "until_idle");
  assert.equal(snapshot.pauseReason, undefined);
});

test("crossing Cutoff Time during a Quota Pause does not start a Continuation", async (t) => {
  const { appServer, clock, paths, workspace } = await createEnvironment(t);
  appServer.rateLimitReads.push(
    rateLimits({
      codex: bucket("codex", "rate_limit_reached", 100, epoch("2026-09-04T10:10:00.000Z")),
    }),
  );

  await addWorkspaceTask(paths, workspace, "Wait for quota");
  await startQueueRun(paths, {
    cutoffTime: "2026-09-04T10:05:00.000Z",
    kind: "cutoff_time",
  });
  appServer.emitUsageLimitExceeded("thread-quota", "turn-1");
  await waitForTaskState(paths, "waiting_for_quota");

  clock.advanceTo("2026-09-04T10:20:00.000Z");
  await settle();
  const snapshot = await getQueueStatus(paths);
  assert.equal(snapshot.state, "paused");
  assert.equal(snapshot.pauseReason, "cutoff_reached");
  assert.equal(snapshot.tasks[0]?.state, "waiting_for_quota");
  assert.equal(appServer.turns.length, 1);
  assert.equal(appServer.resumedThreads.length, 0);
});

test("daemon restart cannot bypass an expired Cutoff Time", async (t) => {
  const { appServer, clock, daemon, paths, workspace } = await createEnvironment(t);

  await addWorkspaceTask(paths, workspace, "Do not resume after cutoff");
  await startQueueRun(paths, {
    cutoffTime: "2026-09-04T10:05:00.000Z",
    kind: "cutoff_time",
  });
  await daemon.close();
  clock.advanceTo("2026-09-04T10:20:00.000Z");

  const replacementAppServer = new FakeAppServer();
  replacementAppServer.recoveryThread = {
    status: "active",
    threadId: "thread-quota",
    turns: [{ status: "in_progress", turnId: "turn-1" }],
  };
  const replacement = await startDaemon({
    appServer: replacementAppServer,
    clock,
    paths,
  });
  assert.equal(replacement.kind, "started");
  if (replacement.kind !== "started") throw new Error("replacement did not start");
  try {
    const snapshot = await getQueueStatus(paths);
    assert.equal(snapshot.state, "paused");
    assert.equal(snapshot.pauseReason, "cutoff_reached");
    assert.equal(snapshot.queueRun?.endedAt, "2026-09-04T10:20:00.000Z");
    assert.equal(replacementAppServer.turns.length, 0);
    assert.equal(appServer.turns.length, 1);
  } finally {
    await replacement.daemon.close();
  }
});

test("daemon restart preserves Full Access confirmation for a Continuation", async (t) => {
  const { appServer, clock, daemon, paths, workspace } = await createEnvironment(t);
  appServer.rateLimitReads.push(
    rateLimits({
      codex: bucket("codex", "rate_limit_reached", 100, epoch("2026-09-04T10:05:00.000Z")),
    }),
  );
  await addWorkspaceTask(paths, workspace, "Resume after restart");
  await startQueueRun(paths, { kind: "until_idle" }, "full");
  appServer.emitUsageLimitExceeded("thread-quota", "turn-1");
  await waitForTaskState(paths, "waiting_for_quota");
  await daemon.close();

  const replacementAppServer = new FakeAppServer();
  replacementAppServer.recoveryThread = {
    status: "idle",
    threadId: "thread-quota",
    turns: [{
      error: { codexErrorInfo: "usageLimitExceeded", message: "quota reached" },
      status: "failed",
      turnId: "turn-1",
    }],
  };
  replacementAppServer.rateLimitReads.push(
    rateLimits({ codex: bucket("codex", null, 10, null) }),
  );
  const replacement = await startDaemon({
    appServer: replacementAppServer,
    clock,
    paths,
  });
  assert.equal(replacement.kind, "started");
  if (replacement.kind !== "started") throw new Error("replacement did not start");
  try {
    clock.advanceTo("2026-09-04T10:05:02.000Z");
    await waitForTurnCount(replacementAppServer, 1);
    assert.deepEqual(replacementAppServer.turnAccess, [{
      accessMode: "full",
      workspace,
    }]);
  } finally {
    await replacement.daemon.close();
  }
});

test("a Turn is not started when Cutoff Time passes while its Thread is starting", async (t) => {
  const { appServer, clock, paths, workspace } = await createEnvironment(t);
  let releaseThreadStart: (() => void) | undefined;
  appServer.startThreadWait = new Promise((resolve) => {
    releaseThreadStart = resolve;
  });

  await addWorkspaceTask(paths, workspace, "Do not cross the cutoff");
  const starting = startQueueRun(paths, {
    cutoffTime: "2026-09-04T10:05:00.000Z",
    kind: "cutoff_time",
  });
  while (appServer.startThreadCalls === 0) await settle();
  clock.advanceTo("2026-09-04T10:05:00.000Z");
  await settle();
  releaseThreadStart?.();
  assert.equal(await starting, "paused");

  const snapshot = await getQueueStatus(paths);
  assert.equal(snapshot.state, "paused");
  assert.equal(snapshot.pauseReason, "cutoff_reached");
  assert.equal(snapshot.tasks[0]?.state, "queued");
  assert.equal(appServer.turns.length, 0);
});

test("a structured quota pause waits for the matching reset before continuing in the same Thread", async (t) => {
  const { appServer, clock, paths, workspace } = await createEnvironment(t);
  appServer.rateLimitReads.push(
    rateLimits({
      codex: bucket("codex", "rate_limit_reached", 100, epoch("2026-09-04T10:05:00.000Z")),
      other: bucket("other", null, 80, epoch("2026-09-04T18:00:00.000Z")),
    }),
    rateLimits({
      codex: bucket("codex", null, 0, epoch("2026-09-04T15:05:00.000Z")),
      other: bucket("other", null, 85, epoch("2026-09-04T18:00:00.000Z")),
    }),
  );

  await addWorkspaceTask(paths, workspace, "Apply the database migration");
  await startQueueRun(paths, { kind: "until_idle" });
  assert.deepEqual(appServer.turns, [{
    prompt: "Apply the database migration",
    threadId: "thread-quota",
    turnId: "turn-1",
  }]);

  appServer.emitUsageLimitExceeded("thread-quota", "turn-1");
  const waiting = await waitForTaskState(paths, "waiting_for_quota");
  assert.deepEqual(waiting.tasks[0], {
    activeTurnId: "turn-1",
    id: 1,
    managedThreadId: "thread-quota",
    quotaLimitId: "codex",
    quotaLimitType: "rate_limit_reached",
    quotaResetAt: "2026-09-04T10:05:00.000Z",
    state: "waiting_for_quota",
    workspace,
  });
  const quotaEvent = (await readFile(paths.eventLogPath, "utf8"))
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line) as Record<string, unknown>)
    .find((event) => {
      const transition = event.stateTransition as Record<string, unknown> | undefined;
      return event.eventType === "task.state_changed"
        && transition?.to === "waiting_for_quota";
    });
  assert.deepEqual(quotaEvent, {
    eventType: "task.state_changed",
    quotaResetAt: "2026-09-04T10:05:00.000Z",
    stateTransition: { from: "running", to: "waiting_for_quota" },
    taskId: 1,
    threadId: "thread-quota",
    timestamp: "2026-09-04T10:00:00.000Z",
    turnId: "turn-1",
  });

  clock.advanceTo("2026-09-04T10:05:00.000Z");
  await settle();
  assert.equal(appServer.turns.length, 1);

  // A suspended system may not run the timer at the requested instant. Waking
  // later still causes one authoritative rate-limit read before continuation.
  clock.advanceTo("2026-09-04T10:20:00.000Z");
  await waitForTurnCount(appServer, 2);
  assert.deepEqual(appServer.resumedThreads, ["thread-quota"]);
  assert.deepEqual(appServer.turns[1], {
    prompt: DEFAULT_CONTINUATION_PROMPT,
    threadId: "thread-quota",
    turnId: "turn-2",
  });
  assert.notEqual(appServer.turns[1]?.prompt, "Apply the database migration");

  appServer.emitCompleted("thread-quota", "turn-2");
  const completed = await waitForTaskState(paths, "completed");
  assert.equal(completed.state, "idle");
});

test("only structured UsageLimitExceeded errors emit a Quota Pause signal", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "codex-resumer-quota-error-test-"));
  const fakeCodex = await createFakeCodex(root, {
    turnErrors: [
      { codexErrorInfo: "other", message: "CLI says UsageLimitExceeded" },
      { codexErrorInfo: "usageLimitExceeded", message: "localized message" },
    ],
  });
  const appServer = new CodexAppServer({
    command: fakeCodex.command,
    env: { ...process.env, FAKE_CODEX_LOG: fakeCodex.logPath },
  });
  t.after(async () => {
    await appServer.close();
    await rm(root, { recursive: true, force: true });
  });
  assert.equal((await appServer.startAndProbe()).state, "ready");
  const events: Array<{ threadId: string; turnId: string }> = [];
  appServer.onUsageLimitExceeded((event) => events.push(event));
  const { threadId } = await appServer.startThread(root);

  await appServer.startTurn(threadId, "first", root, "configured");
  await settle();
  assert.deepEqual(events, []);

  await appServer.startTurn(threadId, "second", root, "configured");
  await settle();
  assert.deepEqual(events, [{ threadId, turnId: "turn-fake-2" }]);
});

test("unattended App Server requests are safely rejected and never approved", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "codex-resumer-unattended-test-"));
  const methods = [
    "item/commandExecution/requestApproval",
    "item/fileChange/requestApproval",
    "item/permissions/requestApproval",
    "item/tool/requestUserInput",
    "mcpServer/elicitation/request",
  ];
  const fakeCodex = await createFakeCodex(root, {
    completeTurn: false,
    unattendedRequestMethods: methods,
  });
  const appServer = new CodexAppServer({
    command: fakeCodex.command,
    env: { ...process.env, FAKE_CODEX_LOG: fakeCodex.logPath },
  });
  t.after(async () => {
    await appServer.close();
    await rm(root, { recursive: true, force: true });
  });
  assert.equal((await appServer.startAndProbe()).state, "ready");
  const requests: string[] = [];
  appServer.onUnattendedRequest((request) => requests.push(request.kind));
  const { threadId } = await appServer.startThread(root);

  await appServer.startTurn(threadId, "Need a human", root, "configured");
  for (let attempt = 0; attempt < 100 && requests.length < methods.length; attempt += 1) {
    await settle();
  }

  let responses: Array<{ id?: number | string; result?: unknown } | undefined> = [];
  for (let attempt = 0; attempt < 100 && responses.length < methods.length; attempt += 1) {
    responses = (await readFile(fakeCodex.logPath, "utf8"))
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line) as {
        message?: { id?: number | string; result?: unknown };
      })
      .map((record) => record.message)
      .filter((message) =>
        typeof message?.id === "string"
        || (typeof message?.id === "number" && message.id >= 1000)
      );
    if (responses.length < methods.length) await settle();
  }
  assert.deepEqual(requests, [
    "command_approval",
    "file_change_approval",
    "permission_request",
    "user_input",
    "mcp_elicitation",
  ]);
  assert.deepEqual(responses, [
    { id: 1000, result: { decision: "cancel" } },
    { id: 1001, result: { decision: "cancel" } },
    { id: 1002, result: { permissions: {} } },
    { id: 1003, result: { answers: {} } },
    { id: "request-mcp", result: { action: "cancel" } },
  ]);
  assert.equal(
    JSON.stringify(responses).includes("accept"),
    false,
  );
});

test("an unattended Turn returning a terminal event releases its Managed Thread", async (t) => {
  const { appServer, paths, workspace } = await createEnvironment(t);
  await addWorkspaceTask(paths, workspace, "Wait for a human");
  await startQueueRun(paths, { kind: "until_idle" });

  appServer.emitUnattended({
    kind: "user_input",
    threadId: "thread-quota",
    turnId: "turn-1",
  });
  await waitForTaskState(paths, "needs_attention");
  appServer.emitInterrupted("thread-quota", "turn-1");
  await settle();

  const database = new Database(paths.databasePath, { readonly: true });
  t.after(() => database.close());
  assert.deepEqual(
    database.prepare("SELECT state FROM managed_threads WHERE id = ?")
      .get("thread-quota"),
    { state: "idle" },
  );
  assert.deepEqual(
    database.prepare("SELECT state FROM turns WHERE id = ?").get("turn-1"),
    { state: "interrupted" },
  );
});

test("an App Server Access Mode rejection remains structured and is not downgraded", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "codex-resumer-access-error-test-"));
  const rejection = {
    code: -32012,
    data: { reason: "sandbox mode denied by managed requirements" },
    message: "requested sandbox policy is not allowed",
  };
  const fakeCodex = await createFakeCodex(root, { turnStartRpcError: rejection });
  const appServer = new CodexAppServer({
    command: fakeCodex.command,
    env: { ...process.env, FAKE_CODEX_LOG: fakeCodex.logPath },
  });
  t.after(async () => {
    await appServer.close();
    await rm(root, { recursive: true, force: true });
  });
  assert.equal((await appServer.startAndProbe()).state, "ready");
  const { threadId } = await appServer.startThread(root);

  await assert.rejects(
    appServer.startTurn(threadId, "Do not downgrade access", root, "full"),
    (error: unknown) => {
      assert.ok(error instanceof AppServerRpcError);
      assert.equal(error.method, "turn/start");
      assert.equal(error.rpcCode, rejection.code);
      assert.deepEqual(error.data, rejection.data);
      return true;
    },
  );
  const turnRequests = (await readFile(fakeCodex.logPath, "utf8"))
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line) as { message?: { method?: string } })
    .filter((record) => record.message?.method === "turn/start");
  assert.equal(turnRequests.length, 1);
});

test("the daemon preserves a structured App Server permission rejection", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "codex-resumer-daemon-access-error-test-"));
  const workspace = path.join(root, "workspace");
  await mkdir(workspace);
  const paths = resolvePaths({
    HOME: root,
    XDG_CONFIG_HOME: path.join(root, "config"),
    XDG_RUNTIME_DIR: path.join(root, "runtime"),
    XDG_STATE_HOME: path.join(root, "state"),
  });
  const rejection = {
    code: -32012,
    data: { reason: "sandbox mode denied by managed requirements" },
    message: "requested sandbox policy is not allowed",
  };
  const fakeCodex = await createFakeCodex(root, { turnStartRpcError: rejection });
  const appServer = new CodexAppServer({
    command: fakeCodex.command,
    env: { ...process.env, FAKE_CODEX_LOG: fakeCodex.logPath },
  });
  const result = await startDaemon({ appServer, paths });
  assert.equal(result.kind, "started");
  if (result.kind !== "started") throw new Error("daemon did not start");
  t.after(async () => {
    await result.daemon.close();
    await rm(root, { recursive: true, force: true });
  });
  await addWorkspaceTask(paths, workspace, "Keep the selected Access Mode");

  await assert.rejects(
    startQueueRun(paths, { kind: "until_idle" }, "full"),
    (error: unknown) => {
      assert.ok(error instanceof DaemonRequestError);
      assert.equal(error.code, "app_server_rpc_error");
      assert.deepEqual(error.details, {
        data: rejection.data,
        method: "turn/start",
        rpcCode: rejection.code,
      });
      return true;
    },
  );
  const snapshot = await getQueueStatus(paths);
  assert.equal(snapshot.state, "paused");
  assert.equal(snapshot.pauseReason, "needs_attention");
});

test("an automatic Continuation persists a structured Access Mode rejection", async (t) => {
  const { appServer, clock, paths, workspace } = await createEnvironment(t);
  const rejection = new AppServerRpcError(
    "requested sandbox policy is not allowed",
    "turn/start",
    -32012,
    { reason: "sandbox mode denied by managed requirements" },
  );
  appServer.turnStartErrors.push(undefined, rejection);
  appServer.rateLimitReads.push(
    rateLimits({
      codex: bucket("codex", "rate_limit_reached", 100, epoch("2026-09-04T10:05:00.000Z")),
    }),
    rateLimits({ codex: bucket("codex", null, 10, null) }),
  );
  await addWorkspaceTask(paths, workspace, "Keep the confirmed Access Mode");
  await startQueueRun(paths, { kind: "until_idle" }, "full");
  appServer.emitUsageLimitExceeded("thread-quota", "turn-1");
  await waitForTaskState(paths, "waiting_for_quota");

  clock.advanceTo("2026-09-04T10:05:02.000Z");
  const snapshot = await waitForSnapshot(
    paths,
    (candidate) => candidate.pauseReason === "needs_attention",
  );

  assert.equal(snapshot.state, "paused");
  assert.equal(snapshot.tasks[0]?.state, "needs_attention");
  assert.deepEqual(
    (snapshot as typeof snapshot & {
      error?: { code: string; details?: unknown; message: string };
    }).error,
    {
      code: "app_server_rpc_error",
      details: {
        data: { reason: "sandbox mode denied by managed requirements" },
        method: "turn/start",
        rpcCode: -32012,
      },
      message: "requested sandbox policy is not allowed",
    },
  );
});

test("transient service failures use at most three exponential retries", async (t) => {
  const { appServer, clock, paths, workspace } = await createEnvironment(t);
  await addWorkspaceTask(paths, workspace, "Original work");
  await addWorkspaceTask(paths, workspace, "Do not advance");
  await startQueueRun(paths, { kind: "until_idle" });

  appServer.emitFailed("thread-quota", "turn-1", "serverOverloaded", "busy");
  await waitForScheduledWaitCount(clock, 1);
  clock.advanceTo("2026-09-04T10:00:01.000Z");
  await waitForTurnCount(appServer, 2);

  appServer.emitFailed("thread-quota", "turn-2", "internalServerError", "again");
  await waitForScheduledWaitCount(clock, 2);
  clock.advanceTo("2026-09-04T10:00:03.000Z");
  await waitForTurnCount(appServer, 3);

  appServer.emitFailed(
    "thread-quota",
    "turn-3",
    { responseStreamDisconnected: { httpStatusCode: 503 } },
    "disconnected",
  );
  await waitForScheduledWaitCount(clock, 3);
  clock.advanceTo("2026-09-04T10:00:07.000Z");
  await waitForTurnCount(appServer, 4);

  appServer.emitFailed("thread-quota", "turn-4", "serverOverloaded", "still busy");
  const snapshot = await waitForSnapshot(
    paths,
    (candidate) => candidate.tasks[0]?.state === "needs_attention",
  );

  assert.deepEqual(clock.scheduledWaits, [
    "2026-09-04T10:00:01.000Z",
    "2026-09-04T10:00:03.000Z",
    "2026-09-04T10:00:07.000Z",
  ]);
  assert.deepEqual(
    appServer.turns.map((turn) => turn.prompt),
    [
      "Original work",
      DEFAULT_CONTINUATION_PROMPT,
      DEFAULT_CONTINUATION_PROMPT,
      DEFAULT_CONTINUATION_PROMPT,
    ],
  );
  assert.equal(snapshot.state, "paused");
  assert.equal(snapshot.pauseReason, "needs_attention");
  assert.equal(snapshot.tasks[1]?.state, "queued");
});

test("pausing during transient backoff leaves the Task explicitly resolvable", async (t) => {
  const { appServer, clock, paths, workspace } = await createEnvironment(t);
  await addWorkspaceTask(paths, workspace, "Original work");
  await startQueueRun(paths, { kind: "until_idle" });

  appServer.emitFailed("thread-quota", "turn-1", "serverOverloaded", "busy");
  await waitForScheduledWaitCount(clock, 1);
  await pauseQueue(paths);

  const snapshot = await getQueueStatus(paths);
  assert.equal(snapshot.state, "paused");
  assert.equal(snapshot.pauseReason, "manual");
  assert.equal(snapshot.tasks[0]?.state, "needs_attention");

  clock.advanceTo("2026-09-04T10:00:01.000Z");
  await settle();
  assert.equal(appServer.turns.length, 1);
  await assert.rejects(
    resumeQueueRun(paths, { kind: "until_idle" }),
    /Resolve Task 1 before resuming the Queue/,
  );
});

test("daemon restart during transient backoff becomes Needs Attention", async (t) => {
  const { appServer, clock, daemon, paths, workspace } = await createEnvironment(t);
  await addWorkspaceTask(paths, workspace, "Original work");
  await startQueueRun(paths, { kind: "until_idle" });

  appServer.emitFailed("thread-quota", "turn-1", "serverOverloaded", "busy");
  await waitForScheduledWaitCount(clock, 1);
  await daemon.close();

  const replacementAppServer = new FakeAppServer();
  const replacement = await startDaemon({
    appServer: replacementAppServer,
    clock,
    paths,
  });
  assert.equal(replacement.kind, "started");
  if (replacement.kind !== "started") throw new Error("replacement did not start");
  try {
    const snapshot = await getQueueStatus(paths);
    assert.equal(snapshot.state, "paused");
    assert.equal(snapshot.pauseReason, "needs_attention");
    assert.equal(snapshot.tasks[0]?.state, "needs_attention");
    assert.equal(replacementAppServer.turns.length, 0);
  } finally {
    await replacement.daemon.close();
  }
});

test("automatic Queue advance persists a structured Access Mode rejection", async (t) => {
  const { appServer, paths, workspace } = await createEnvironment(t);
  const rejection = new AppServerRpcError(
    "requested sandbox policy is not allowed",
    "turn/start",
    -32012,
    { reason: "sandbox mode denied by managed requirements" },
  );
  appServer.turnStartErrors.push(undefined, rejection);
  await addWorkspaceTask(paths, workspace, "Finish first");
  await startQueueRun(paths, { kind: "until_idle" }, "full");
  await addManagedThreadTask(paths, "thread-quota", "Reject second");

  appServer.emitCompleted("thread-quota", "turn-1");
  const snapshot = await waitForSnapshot(
    paths,
    (candidate) => candidate.pauseReason === "needs_attention",
  );

  assert.equal(snapshot.state, "paused");
  assert.equal(snapshot.tasks[0]?.state, "completed");
  assert.equal(snapshot.tasks[1]?.state, "needs_attention");
  assert.deepEqual(
    (snapshot as typeof snapshot & {
      error?: { code: string; details?: unknown; message: string };
    }).error,
    {
      code: "app_server_rpc_error",
      details: {
        data: { reason: "sandbox mode denied by managed requirements" },
        method: "turn/start",
        rpcCode: -32012,
      },
      message: "requested sandbox policy is not allowed",
    },
  );
});

test("a missing reset time uses bounded exponential polling", async (t) => {
  const { appServer, clock, paths, workspace } = await createEnvironment(t);
  const unavailable = rateLimits({
    codex: bucket("codex", "rate_limit_reached", 100, null),
  });
  appServer.rateLimitReads.push(
    unavailable,
    unavailable,
    unavailable,
    unavailable,
    unavailable,
    unavailable,
    rateLimits({ codex: bucket("codex", null, 20, null) }),
  );

  await addWorkspaceTask(paths, workspace, "Long running work");
  await startQueueRun(paths, { kind: "until_idle" });
  appServer.emitUsageLimitExceeded("thread-quota", "turn-1");
  await waitForTaskState(paths, "waiting_for_quota");

  for (const timestamp of [
    "2026-09-04T10:01:00.000Z",
    "2026-09-04T10:03:00.000Z",
    "2026-09-04T10:07:00.000Z",
    "2026-09-04T10:15:00.000Z",
    "2026-09-04T10:30:00.000Z",
  ]) {
    clock.advanceTo(timestamp);
    await settle();
    assert.equal(appServer.turns.length, 1);
  }
  clock.advanceTo("2026-09-04T10:45:00.000Z");
  await waitForTurnCount(appServer, 2);

  assert.deepEqual(clock.scheduledWaits, [
    "2026-09-04T10:01:00.000Z",
    "2026-09-04T10:03:00.000Z",
    "2026-09-04T10:07:00.000Z",
    "2026-09-04T10:15:00.000Z",
    "2026-09-04T10:30:00.000Z",
    "2026-09-04T10:45:00.000Z",
  ]);
});

test("the reached legacy bucket is not hidden by an unrelated multi-bucket view", async (t) => {
  const { appServer, clock, paths, workspace } = await createEnvironment(t);
  appServer.rateLimitReads.push(
    {
      rateLimits: bucket(
        "codex",
        "rate_limit_reached",
        100,
        epoch("2026-09-04T10:05:00.000Z"),
      ),
      rateLimitsByLimitId: {
        other: bucket("other", null, 20, epoch("2026-09-04T18:00:00.000Z")),
      },
    },
    {
      rateLimits: bucket("codex", null, 10, null),
      rateLimitsByLimitId: {
        other: bucket("other", null, 25, epoch("2026-09-04T18:00:00.000Z")),
      },
    },
  );

  await addWorkspaceTask(paths, workspace, "Continue the right work");
  await startQueueRun(paths, { kind: "until_idle" });
  appServer.emitUsageLimitExceeded("thread-quota", "turn-1");
  const waiting = await waitForTaskState(paths, "waiting_for_quota");
  assert.equal(waiting.tasks[0]?.quotaLimitId, "codex");
  assert.equal(waiting.tasks[0]?.quotaResetAt, "2026-09-04T10:05:00.000Z");

  clock.advanceTo("2026-09-04T10:05:02.000Z");
  await waitForTurnCount(appServer, 2);
  assert.equal(appServer.turns[1]?.threadId, "thread-quota");
});

test("polling discovers a reached bucket after the initial rate-limit read fails", async (t) => {
  const { appServer, clock, paths, workspace } = await createEnvironment(t);
  appServer.rateLimitReads.push(
    new Error("rate limits temporarily unavailable"),
    {
      rateLimits: null,
      rateLimitsByLimitId: {
        codex: bucket(
          "codex",
          "rate_limit_reached",
          100,
          epoch("2026-09-04T10:05:00.000Z"),
        ),
        other: bucket("other", null, 20, epoch("2026-09-04T18:00:00.000Z")),
      },
    },
    {
      rateLimits: null,
      rateLimitsByLimitId: {
        codex: bucket("codex", null, 10, null),
        other: bucket("other", null, 25, epoch("2026-09-04T18:00:00.000Z")),
      },
    },
  );

  await addWorkspaceTask(paths, workspace, "Recover after a transient read failure");
  await startQueueRun(paths, { kind: "until_idle" });
  appServer.emitUsageLimitExceeded("thread-quota", "turn-1");
  await waitForTaskState(paths, "waiting_for_quota");

  clock.advanceTo("2026-09-04T10:01:00.000Z");
  const identified = await waitForQuotaLimit(paths, "codex");
  assert.equal(identified.tasks[0]?.quotaResetAt, "2026-09-04T10:05:00.000Z");
  const learnedReset = (await readFile(paths.eventLogPath, "utf8"))
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line) as Record<string, unknown>)
    .find((event) => event.eventType === "quota.reset_updated");
  assert.deepEqual(learnedReset, {
    eventType: "quota.reset_updated",
    quotaResetAt: "2026-09-04T10:05:00.000Z",
    taskId: 1,
    threadId: "thread-quota",
    timestamp: "2026-09-04T10:01:00.000Z",
    turnId: "turn-1",
  });
  clock.advanceTo("2026-09-04T10:05:02.000Z");
  await waitForTurnCount(appServer, 2);
  assert.equal(appServer.turns[1]?.threadId, "thread-quota");
});

test("Until Idle recovers from repeated Quota Pauses", async (t) => {
  const { appServer, clock, paths, workspace } = await createEnvironment(t);
  appServer.quotaPauseOnTurnNumbers.add(2);
  appServer.rateLimitReads.push(
    rateLimits({
      codex: bucket("codex", "rate_limit_reached", 100, epoch("2026-09-04T10:05:00.000Z")),
    }),
    rateLimits({ codex: bucket("codex", null, 10, null) }),
    rateLimits({
      codex: bucket("codex", "rate_limit_reached", 100, epoch("2026-09-04T11:00:00.000Z")),
    }),
    rateLimits({ codex: bucket("codex", null, 5, null) }),
  );

  await addWorkspaceTask(paths, workspace, "Finish every step");
  await startQueueRun(paths, { kind: "until_idle" });
  appServer.emitUsageLimitExceeded("thread-quota", "turn-1");
  await waitForTaskState(paths, "waiting_for_quota");
  clock.advanceTo("2026-09-04T10:05:02.000Z");
  await waitForTurnCount(appServer, 2);

  const secondPause = await waitForTaskState(paths, "waiting_for_quota");
  assert.equal(secondPause.tasks[0]?.activeTurnId, "turn-2");
  assert.equal(secondPause.tasks[0]?.quotaResetAt, "2026-09-04T11:00:00.000Z");
  clock.advanceTo("2026-09-04T11:00:02.000Z");
  await waitForTurnCount(appServer, 3);

  assert.deepEqual(
    appServer.turns.map((turn) => ({ prompt: turn.prompt, threadId: turn.threadId })),
    [
      { prompt: "Finish every step", threadId: "thread-quota" },
      { prompt: DEFAULT_CONTINUATION_PROMPT, threadId: "thread-quota" },
      { prompt: DEFAULT_CONTINUATION_PROMPT, threadId: "thread-quota" },
    ],
  );
  appServer.emitCompleted("thread-quota", "turn-3");
  assert.equal((await waitForTaskState(paths, "completed")).state, "idle");
});

test("the global Continuation prompt has a default and can be replaced", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "codex-resumer-config-test-"));
  const workspace = path.join(root, "workspace");
  await mkdir(workspace);
  const env = {
    ...process.env,
    HOME: root,
    XDG_CONFIG_HOME: path.join(root, "config"),
    XDG_RUNTIME_DIR: path.join(root, "runtime"),
    XDG_STATE_HOME: path.join(root, "state"),
  };
  const paths = resolvePaths(env);
  const runCli = async (...args: string[]): Promise<string> => {
    const { stdout } = await execFileAsync(process.execPath, [cliPath, ...args], {
      encoding: "utf8",
      env,
    });
    return stdout;
  };
  assert.equal(
    await runCli("config", "show"),
    `Access Mode: Configured Access\nContinuation prompt: ${DEFAULT_CONTINUATION_PROMPT}\n`,
  );
  const clock = new ManualClock("2026-09-04T10:00:00.000Z");
  const appServer = new FakeAppServer();
  appServer.rateLimitReads.push(
    rateLimits({
      codex: bucket("codex", "rate_limit_reached", 100, epoch("2026-09-04T10:05:00.000Z")),
    }),
    rateLimits({ codex: bucket("codex", null, 10, null) }),
  );
  const result = await startDaemon({ appServer, clock, paths });
  assert.equal(result.kind, "started");
  if (result.kind !== "started") throw new Error("daemon did not start");
  t.after(async () => {
    await result.daemon.close();
    await rm(root, { recursive: true, force: true });
  });
  await addWorkspaceTask(paths, workspace, "Original prompt");
  await startQueueRun(paths, { kind: "until_idle" });
  appServer.emitUsageLimitExceeded("thread-quota", "turn-1");
  await waitForTaskState(paths, "waiting_for_quota");
  assert.equal(
    await runCli(
      "config",
      "set",
      "continuationPrompt",
      "Check what is unfinished, then carry on carefully.",
    ),
    "Continuation prompt updated.\n",
  );
  clock.advanceTo("2026-09-04T10:05:02.000Z");
  await waitForTurnCount(appServer, 2);

  assert.equal(
    appServer.turns[1]?.prompt,
    "Check what is unfinished, then carry on carefully.",
  );
  assert.equal(
    await runCli("config", "show"),
    "Access Mode: Configured Access\n"
      + "Continuation prompt: Check what is unfinished, then carry on carefully.\n",
  );
});

async function createEnvironment(t: TestContext) {
  const root = await mkdtemp(path.join(tmpdir(), "codex-resumer-quota-test-"));
  const workspace = path.join(root, "workspace");
  await mkdir(workspace);
  const paths = resolvePaths({
    HOME: root,
    XDG_CONFIG_HOME: path.join(root, "config"),
    XDG_RUNTIME_DIR: path.join(root, "runtime"),
    XDG_STATE_HOME: path.join(root, "state"),
  });
  const clock = new ManualClock("2026-09-04T10:00:00.000Z");
  const appServer = new FakeAppServer();
  const result = await startDaemon({ appServer, clock, paths });
  assert.equal(result.kind, "started");
  if (result.kind !== "started") throw new Error("daemon did not start");
  t.after(async () => {
    await result.daemon.close();
    await rm(root, { recursive: true, force: true });
  });
  return { appServer, clock, daemon: result.daemon, paths, workspace };
}

function bucket(
  limitId: string,
  rateLimitReachedType: string | null,
  usedPercent: number,
  resetsAt: number | null,
): RateLimitSnapshot {
  return {
    limitId,
    primary: { resetsAt, usedPercent },
    rateLimitReachedType,
    secondary: null,
  };
}

function epoch(value: string): number {
  return new Date(value).getTime() / 1_000;
}

function rateLimits(
  rateLimitsByLimitId: Record<string, RateLimitSnapshot>,
): RateLimits {
  return {
    rateLimits: rateLimitsByLimitId.codex ?? null,
    rateLimitsByLimitId,
  };
}

async function settle(): Promise<void> {
  await new Promise((resolve) => setImmediate(resolve));
  await new Promise((resolve) => setImmediate(resolve));
}

async function waitForTaskState(
  paths: ReturnType<typeof resolvePaths>,
  state: string,
) {
  return waitForSnapshot(paths, (snapshot) => snapshot.tasks[0]?.state === state);
}

async function waitForTurnCount(appServer: FakeAppServer, count: number): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (appServer.turns.length === count) return;
    await settle();
  }
}

async function waitForScheduledWaitCount(
  clock: ManualClock,
  count: number,
): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (clock.scheduledWaits.length === count) return;
    await settle();
  }
}

async function waitForQuotaLimit(
  paths: ReturnType<typeof resolvePaths>,
  limitId: string,
) {
  return waitForSnapshot(
    paths,
    (snapshot) => snapshot.tasks[0]?.quotaLimitId === limitId,
  );
}

async function waitForSnapshot(
  paths: ReturnType<typeof resolvePaths>,
  matches: (snapshot: Awaited<ReturnType<typeof getQueueStatus>>) => boolean,
) {
  let snapshot = await getQueueStatus(paths);
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (matches(snapshot)) return snapshot;
    await settle();
    snapshot = await getQueueStatus(paths);
  }
  return snapshot;
}
