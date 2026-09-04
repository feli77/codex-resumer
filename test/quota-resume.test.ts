import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test, { type TestContext } from "node:test";
import { promisify } from "node:util";

import { CodexAppServer } from "../src/app-server.js";
import {
  addWorkspaceTask,
  getQueueStatus,
  startDaemon,
  startQueueRun,
  type AppServerController,
  type CompletedTurn,
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
  readonly turns: Array<{ prompt: string; threadId: string; turnId: string }> = [];
  #completedListeners = new Set<(turn: CompletedTurn) => void>();
  #usageLimitListeners = new Set<
    (event: { threadId: string; turnId: string }) => void
  >();

  async startAndProbe() {
    return { state: "ready", codexVersion: "codex-cli quota-test" } as const;
  }

  async close() {}

  async readThread(): Promise<{ threadId: string; workspace: string }> {
    throw new Error("not used");
  }

  async resumeThread(threadId: string) {
    this.resumedThreads.push(threadId);
    return { threadId };
  }

  async startThread() {
    return { threadId: "thread-quota" };
  }

  async startTurn(threadId: string, prompt: string) {
    const turnId = `turn-${this.turns.length + 1}`;
    this.turns.push({ prompt, threadId, turnId });
    if (this.quotaPauseOnTurnNumbers.has(this.turns.length)) {
      this.emitUsageLimitExceeded(threadId, turnId);
    }
    return { turnId };
  }

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

  onUsageLimitExceeded(
    listener: (event: { threadId: string; turnId: string }) => void,
  ): () => void {
    this.#usageLimitListeners.add(listener);
    return () => this.#usageLimitListeners.delete(listener);
  }

  emitUsageLimitExceeded(threadId: string, turnId: string): void {
    for (const listener of this.#usageLimitListeners) listener({ threadId, turnId });
  }

  emitCompleted(threadId: string, turnId: string): void {
    for (const listener of this.#completedListeners) {
      listener({ status: "completed", threadId, turnId });
    }
  }
}

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
  await startQueueRun(paths);
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

  await appServer.startTurn(threadId, "first");
  await settle();
  assert.deepEqual(events, []);

  await appServer.startTurn(threadId, "second");
  await settle();
  assert.deepEqual(events, [{ threadId, turnId: "turn-fake-2" }]);
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
  await startQueueRun(paths);
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
  await startQueueRun(paths);
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
  await startQueueRun(paths);
  appServer.emitUsageLimitExceeded("thread-quota", "turn-1");
  await waitForTaskState(paths, "waiting_for_quota");

  clock.advanceTo("2026-09-04T10:01:00.000Z");
  const identified = await waitForQuotaLimit(paths, "codex");
  assert.equal(identified.tasks[0]?.quotaResetAt, "2026-09-04T10:05:00.000Z");
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
  await startQueueRun(paths);
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
    `Continuation prompt: ${DEFAULT_CONTINUATION_PROMPT}\n`,
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
  await startQueueRun(paths);
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
    "Continuation prompt: Check what is unfinished, then carry on carefully.\n",
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
  return { appServer, clock, paths, workspace };
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
  let snapshot = await getQueueStatus(paths);
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (snapshot.tasks[0]?.state === state) return snapshot;
    await settle();
    snapshot = await getQueueStatus(paths);
  }
  return snapshot;
}

async function waitForTurnCount(appServer: FakeAppServer, count: number): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (appServer.turns.length === count) return;
    await settle();
  }
}

async function waitForQuotaLimit(
  paths: ReturnType<typeof resolvePaths>,
  limitId: string,
) {
  let snapshot = await getQueueStatus(paths);
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (snapshot.tasks[0]?.quotaLimitId === limitId) return snapshot;
    await settle();
    snapshot = await getQueueStatus(paths);
  }
  return snapshot;
}
