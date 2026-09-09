import assert from "node:assert/strict";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test, { type TestContext } from "node:test";

import Database from "better-sqlite3";

import {
  addWorkspaceTask,
  getQueueStatus,
  startDaemon,
  startQueueRun,
  type AccountRateLimits,
  type AppServerController,
  type Clock,
  type CompletedTurn,
  type UsageLimitExceeded,
} from "../src/daemon.js";
import { resolvePaths } from "../src/paths.js";

interface ObservedThread {
  status: "active" | "idle" | "not_loaded" | "system_error";
  threadId: string;
  turns: Array<{
    error?: { codexErrorInfo?: unknown; message: string };
    status: "completed" | "failed" | "in_progress" | "interrupted";
    turnId: string;
  }>;
}

class ReconciliationAppServer implements AppServerController {
  readonly inspectedThreads: string[] = [];
  readonly rateLimitReads: AccountRateLimits[] = [];
  readonly resumedThreads: string[] = [];
  readonly startedTurns: Array<{ prompt: string; threadId: string }> = [];
  observedThread: ObservedThread | undefined;
  observedThreads: ObservedThread[] = [];
  reconciliationError: Error | undefined;
  nextThreadId = "thread-recovery";
  nextTurnId = "turn-1";
  #completedListeners = new Set<(turn: CompletedTurn) => void>();
  #usageLimitListeners = new Set<(event: UsageLimitExceeded) => void>();

  async startAndProbe() {
    return { state: "ready", codexVersion: "codex-cli reconciliation-test" } as const;
  }

  async close() {}

  async readRateLimits() {
    return this.rateLimitReads.shift()
      ?? { rateLimits: null, rateLimitsByLimitId: null };
  }

  async readThread(threadId: string) {
    return { threadId, workspace: "/unused" };
  }

  async readThreadForReconciliation(threadId: string): Promise<ObservedThread> {
    this.inspectedThreads.push(threadId);
    if (this.reconciliationError) throw this.reconciliationError;
    const nextObservedThread = this.observedThreads.shift();
    if (nextObservedThread) return nextObservedThread;
    if (!this.observedThread) throw new Error("Thread state is unavailable");
    return this.observedThread;
  }

  async resumeThread(threadId: string) {
    this.resumedThreads.push(threadId);
    return { threadId };
  }

  async startThread() {
    return { threadId: this.nextThreadId };
  }

  async startTurn(threadId: string, prompt: string) {
    this.startedTurns.push({ prompt, threadId });
    return { turnId: this.nextTurnId };
  }

  async interruptTurn() {}

  onTurnCompleted(listener: (turn: CompletedTurn) => void): () => void {
    this.#completedListeners.add(listener);
    return () => this.#completedListeners.delete(listener);
  }

  onTurnStarted(): () => void {
    return () => undefined;
  }

  onUnattendedRequest(): () => void {
    return () => undefined;
  }

  onUnexpectedExit(): () => void {
    return () => undefined;
  }

  onUsageLimitExceeded(listener: (event: UsageLimitExceeded) => void): () => void {
    this.#usageLimitListeners.add(listener);
    return () => this.#usageLimitListeners.delete(listener);
  }

  emitCompleted(threadId: string, turnId: string): void {
    for (const listener of this.#completedListeners) {
      listener({ status: "completed", threadId, turnId });
    }
  }

  emitUsageLimitExceeded(threadId: string, turnId: string): void {
    for (const listener of this.#usageLimitListeners) listener({ threadId, turnId });
  }
}

class ManualClock implements Clock {
  #now = new Date("2026-09-09T10:00:00.000Z");
  #waiters: Array<{ resolve(): void; signal: AbortSignal; until: Date }> = [];

  now(): Date {
    return new Date(this.#now);
  }

  waitUntil(until: Date, signal: AbortSignal): Promise<void> {
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

test("unexpected restart resumes listening to an active Turn without submitting a prompt", async (t) => {
  const { firstAppServer, paths, replacementAppServer, workspace } =
    await createRestartedEnvironment(t, {
      status: "active",
      threadId: "thread-recovery",
      turns: [{ status: "in_progress", turnId: "turn-1" }],
    });

  assert.deepEqual(replacementAppServer.inspectedThreads, [
    "thread-recovery",
    "thread-recovery",
  ]);
  assert.deepEqual(replacementAppServer.resumedThreads, ["thread-recovery"]);
  assert.equal(firstAppServer.startedTurns.length, 1);
  assert.equal(replacementAppServer.startedTurns.length, 0);
  assert.equal((await getQueueStatus(paths)).tasks[0]?.state, "running");

  replacementAppServer.emitCompleted("thread-recovery", "turn-1");
  const completed = await waitForTaskState(paths, "completed");
  assert.equal(completed.state, "idle");
  assert.equal(completed.tasks[0]?.workspace, workspace);
  assert.equal(replacementAppServer.startedTurns.length, 0);
});

test("restart reconciliation catches completion while resuming an active Thread", async (t) => {
  const active: ObservedThread = {
    status: "active",
    threadId: "thread-recovery",
    turns: [{ status: "in_progress", turnId: "turn-1" }],
  };
  const completed: ObservedThread = {
    status: "idle",
    threadId: "thread-recovery",
    turns: [{ status: "completed", turnId: "turn-1" }],
  };
  const { paths, replacementAppServer } = await createRestartedEnvironment(
    t,
    [active, completed],
  );

  const snapshot = await waitForTaskState(paths, "completed");
  assert.equal(snapshot.state, "idle");
  assert.deepEqual(replacementAppServer.inspectedThreads, [
    "thread-recovery",
    "thread-recovery",
  ]);
  assert.deepEqual(replacementAppServer.resumedThreads, ["thread-recovery"]);
  assert.equal(replacementAppServer.startedTurns.length, 0);
});

test("unexpected restart completes a successful Turn and advances the Queue", async (t) => {
  const { paths, replacementAppServer } = await createRestartedEnvironment(
    t,
    {
      status: "idle",
      threadId: "thread-recovery",
      turns: [{ status: "completed", turnId: "turn-1" }],
    },
    ["Start after recovery"],
  );

  const snapshot = await waitForTaskState(paths, "completed");
  assert.equal(snapshot.tasks[1]?.state, "running");
  assert.deepEqual(replacementAppServer.startedTurns, [{
    prompt: "Start after recovery",
    threadId: "thread-after-recovery",
  }]);
  assert.equal(
    replacementAppServer.startedTurns.some((turn) => turn.prompt === "Do this once"),
    false,
  );
});

test("unexpected restart reconciles a quota failure before starting a Continuation", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "codex-resumer-quota-restart-test-"));
  const workspace = path.join(root, "workspace");
  await mkdir(workspace);
  const paths = resolvePaths({
    HOME: root,
    XDG_CONFIG_HOME: path.join(root, "config"),
    XDG_RUNTIME_DIR: path.join(root, "runtime"),
    XDG_STATE_HOME: path.join(root, "state"),
  });
  const clock = new ManualClock();
  const firstAppServer = new ReconciliationAppServer();
  firstAppServer.rateLimitReads.push(exhaustedRateLimits());
  const first = await startDaemon({ appServer: firstAppServer, clock, paths });
  assert.equal(first.kind, "started");
  if (first.kind !== "started") throw new Error("first daemon did not start");
  await addWorkspaceTask(paths, workspace, "Do not replay this prompt");
  await startQueueRun(paths, { kind: "until_idle" });
  firstAppServer.emitUsageLimitExceeded("thread-recovery", "turn-1");
  await waitForTaskState(paths, "waiting_for_quota");
  await first.daemon.close();

  const replacementAppServer = new ReconciliationAppServer();
  replacementAppServer.observedThread = {
    status: "idle",
    threadId: "thread-recovery",
    turns: [{
      error: { codexErrorInfo: "usageLimitExceeded", message: "quota reached" },
      status: "failed",
      turnId: "turn-1",
    }],
  };
  replacementAppServer.nextTurnId = "turn-2";
  replacementAppServer.rateLimitReads.push(availableRateLimits());
  const replacement = await startDaemon({
    appServer: replacementAppServer,
    clock,
    paths,
  });
  assert.equal(replacement.kind, "started");
  if (replacement.kind !== "started") throw new Error("replacement daemon did not start");
  t.after(async () => {
    await replacement.daemon.close();
    await rm(root, { recursive: true, force: true });
  });

  assert.deepEqual(replacementAppServer.inspectedThreads, ["thread-recovery"]);
  assert.equal((await getQueueStatus(paths)).tasks[0]?.state, "waiting_for_quota");
  assert.equal(replacementAppServer.startedTurns.length, 0);

  clock.advanceTo("2026-09-09T10:05:02.000Z");
  await waitForTurnCount(replacementAppServer, 1);
  assert.deepEqual(replacementAppServer.startedTurns, [{
    prompt: "Inspect the current Thread and Workspace state, continue the unfinished Task, and do not repeat work that is already complete.",
    threadId: "thread-recovery",
  }]);
});

test("unexpected restart sends an interrupted Turn to Needs Attention", async (t) => {
  const { paths, replacementAppServer } = await createRestartedEnvironment(t, {
    status: "idle",
    threadId: "thread-recovery",
    turns: [{ status: "interrupted", turnId: "turn-1" }],
  });

  const snapshot = await waitForTaskState(paths, "needs_attention");
  assert.equal(snapshot.state, "paused");
  assert.equal(snapshot.pauseReason, "needs_attention");
  assert.equal(replacementAppServer.startedTurns.length, 0);
});

test("unexpected restart sends a non-quota Turn failure to Needs Attention", async (t) => {
  const { paths, replacementAppServer } = await createRestartedEnvironment(
    t,
    {
      status: "idle",
      threadId: "thread-recovery",
      turns: [{
        error: { codexErrorInfo: "unauthorized", message: "login expired" },
        status: "failed",
        turnId: "turn-1",
      }],
    },
    ["Do not start"],
  );

  const snapshot = await waitForTaskState(paths, "needs_attention");
  assert.equal(snapshot.state, "paused");
  assert.deepEqual(snapshot.error, {
    code: "turn_failed",
    details: { codexErrorInfo: "unauthorized" },
    message: "login expired",
  });
  assert.equal(snapshot.tasks[1]?.state, "queued");
  assert.equal(replacementAppServer.startedTurns.length, 0);
});

test("unexpected restart does not retry a recovered transient Turn failure", async (t) => {
  const { paths, replacementAppServer } = await createRestartedEnvironment(t, {
    status: "idle",
    threadId: "thread-recovery",
    turns: [{
      error: { codexErrorInfo: "serverOverloaded", message: "service busy" },
      status: "failed",
      turnId: "turn-1",
    }],
  });

  const snapshot = await waitForTaskState(paths, "needs_attention");
  assert.equal(snapshot.state, "paused");
  assert.equal(snapshot.error?.message, "service busy");
  await new Promise((resolve) => setTimeout(resolve, 1_100));
  assert.equal(replacementAppServer.startedTurns.length, 0);
});

test("unexpected restart sends idle state without a successful Turn to Needs Attention", async (t) => {
  const { paths, replacementAppServer } = await createRestartedEnvironment(t, {
    status: "idle",
    threadId: "thread-recovery",
    turns: [],
  });

  const snapshot = await waitForTaskState(paths, "needs_attention");
  assert.equal(snapshot.state, "paused");
  assert.deepEqual(snapshot.error, {
    code: "turn_reconciliation_uncertain",
    details: { threadStatus: "idle", turnId: "turn-1" },
    message: "The active Turn could not be confirmed after daemon restart.",
  });
  assert.equal(replacementAppServer.startedTurns.length, 0);
});

test("unexpected restart sends an unavailable Turn state to Needs Attention", async (t) => {
  const { paths, replacementAppServer } = await createRestartedEnvironment(
    t,
    new Error("Thread read failed"),
  );

  const snapshot = await waitForTaskState(paths, "needs_attention");
  assert.equal(snapshot.state, "paused");
  assert.equal(snapshot.error?.code, "turn_reconciliation_unavailable");
  assert.match(snapshot.error?.message ?? "", /could not be read after daemon restart/i);
  assert.equal(replacementAppServer.startedTurns.length, 0);
});

test("unexpected restart sends incomplete local Turn state to Needs Attention", async (t) => {
  const { paths, replacementAppServer } = await createRestartedEnvironment(
    t,
    {
      status: "active",
      threadId: "thread-recovery",
      turns: [{ status: "in_progress", turnId: "turn-1" }],
    },
    [],
    true,
  );

  const snapshot = await waitForTaskState(paths, "needs_attention");
  assert.equal(snapshot.state, "paused");
  assert.equal(snapshot.error?.code, "turn_reconciliation_uncertain");
  assert.deepEqual(replacementAppServer.inspectedThreads, []);
  assert.equal(replacementAppServer.startedTurns.length, 0);
});

async function createRestartedEnvironment(
  t: TestContext,
  observedThread: Error | ObservedThread | ObservedThread[],
  laterPrompts: string[] = [],
  clearActiveTurn = false,
) {
  const root = await mkdtemp(path.join(tmpdir(), "codex-resumer-restart-test-"));
  let replacementDaemon: { close(): Promise<void> } | undefined;
  t.after(async () => {
    await replacementDaemon?.close();
    await rm(root, { recursive: true, force: true });
  });
  const workspace = path.join(root, "workspace");
  await mkdir(workspace);
  const paths = resolvePaths({
    HOME: root,
    XDG_CONFIG_HOME: path.join(root, "config"),
    XDG_RUNTIME_DIR: path.join(root, "runtime"),
    XDG_STATE_HOME: path.join(root, "state"),
  });
  const firstAppServer = new ReconciliationAppServer();
  const first = await startDaemon({ appServer: firstAppServer, paths });
  assert.equal(first.kind, "started");
  if (first.kind !== "started") throw new Error("first daemon did not start");
  await addWorkspaceTask(paths, workspace, "Do this once");
  for (const prompt of laterPrompts) {
    await addWorkspaceTask(paths, workspace, prompt);
  }
  await startQueueRun(paths, { kind: "until_idle" });
  await first.daemon.close();
  if (clearActiveTurn) {
    const database = new Database(paths.databasePath);
    database.prepare("UPDATE tasks SET active_turn_id = NULL WHERE id = 1").run();
    database.close();
  }

  const replacementAppServer = new ReconciliationAppServer();
  if (observedThread instanceof Error) {
    replacementAppServer.reconciliationError = observedThread;
  } else if (Array.isArray(observedThread)) {
    replacementAppServer.observedThreads.push(...observedThread);
  } else {
    replacementAppServer.observedThread = observedThread;
  }
  replacementAppServer.nextThreadId = "thread-after-recovery";
  replacementAppServer.nextTurnId = "turn-2";
  const replacement = await startDaemon({ appServer: replacementAppServer, paths });
  assert.equal(replacement.kind, "started");
  if (replacement.kind !== "started") throw new Error("replacement daemon did not start");
  replacementDaemon = replacement.daemon;
  return { firstAppServer, paths, replacementAppServer, workspace };
}

async function waitForTaskState(
  paths: ReturnType<typeof resolvePaths>,
  expected: string,
) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const snapshot = await getQueueStatus(paths);
    if (snapshot.tasks[0]?.state === expected) return snapshot;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`Task did not reach ${expected}`);
}

async function waitForTurnCount(
  appServer: ReconciliationAppServer,
  expected: number,
): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (appServer.startedTurns.length >= expected) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`App Server did not start ${expected} Turn(s)`);
}

function exhaustedRateLimits(): AccountRateLimits {
  return {
    rateLimits: {
      limitId: "codex",
      primary: { resetsAt: 1_788_948_300, usedPercent: 100 },
      rateLimitReachedType: "rate_limit_reached",
      secondary: null,
    },
    rateLimitsByLimitId: null,
  };
}

function availableRateLimits(): AccountRateLimits {
  return {
    rateLimits: {
      limitId: "codex",
      primary: { resetsAt: null, usedPercent: 0 },
      rateLimitReachedType: null,
      secondary: null,
    },
    rateLimitsByLimitId: null,
  };
}
