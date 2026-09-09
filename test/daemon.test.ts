import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";

import { CodexAppServer } from "../src/app-server.js";
import {
  getDaemonStatus,
  startDaemon,
  stopDaemon,
  type AppServerController,
} from "../src/daemon.js";
import { resolvePaths } from "../src/paths.js";
import { createFakeCodex } from "./fake-codex.js";

const unusedTaskExecution: Pick<
  AppServerController,
  | "interruptTurn"
  | "readRateLimits"
  | "readThread"
  | "readThreadForReconciliation"
  | "resumeThread"
  | "startThread"
  | "startTurn"
  | "onTurnCompleted"
  | "onTurnStarted"
  | "onUnattendedRequest"
  | "onUnexpectedExit"
  | "onUsageLimitExceeded"
> = {
  async readRateLimits() {
    return { rateLimits: null, rateLimitsByLimitId: null };
  },
  async readThread() {
    throw new Error("not used by daemon lifecycle tests");
  },
  async readThreadForReconciliation() {
    throw new Error("not used by daemon lifecycle tests");
  },
  async resumeThread() {
    throw new Error("not used by daemon lifecycle tests");
  },
  async startThread() {
    throw new Error("not used by daemon lifecycle tests");
  },
  async startTurn() {
    throw new Error("not used by daemon lifecycle tests");
  },
  async interruptTurn() {
    throw new Error("not used by daemon lifecycle tests");
  },
  onTurnCompleted() {
    return () => undefined;
  },
  onTurnStarted() {
    return () => undefined;
  },
  onUnattendedRequest() {
    return () => undefined;
  },
  onUnexpectedExit() {
    return () => undefined;
  },
  onUsageLimitExceeded() {
    return () => undefined;
  },
};

async function createTestEnvironment() {
  const root = await mkdtemp(path.join(tmpdir(), "codex-resumer-test-"));
  return {
    root,
    paths: resolvePaths({
      HOME: root,
      XDG_CONFIG_HOME: path.join(root, "config"),
      XDG_RUNTIME_DIR: path.join(root, "runtime"),
      XDG_STATE_HOME: path.join(root, "state"),
    }),
  };
}

test("daemon status reports stopped before the daemon has started", async (t) => {
  const { root, paths } = await createTestEnvironment();
  t.after(() => rm(root, { recursive: true, force: true }));

  assert.deepEqual(await getDaemonStatus(paths), { state: "stopped" });
});

test("daemon status crosses the private socket boundary", async (t) => {
  const { root, paths } = await createTestEnvironment();
  const appServer: AppServerController = {
    ...unusedTaskExecution,
    async startAndProbe() {
      return { state: "ready", codexVersion: "codex-cli 0.test" };
    },
    async close() {},
  };

  const result = await startDaemon({
    appServer,
    clock: { now: () => new Date("2026-09-04T01:02:03.000Z") },
    paths,
  });
  assert.equal(result.kind, "started");
  if (result.kind !== "started") return;
  t.after(async () => {
    await result.daemon.close();
    await rm(root, { recursive: true, force: true });
  });

  assert.deepEqual(await getDaemonStatus(paths), {
    state: "running",
    pid: process.pid,
    codexVersion: "codex-cli 0.test",
    startedAt: "2026-09-04T01:02:03.000Z",
  });
  assert.equal((await stat(paths.configDir)).mode & 0o777, 0o700);
  assert.equal((await stat(paths.stateDir)).mode & 0o777, 0o700);
  assert.equal((await stat(paths.runtimeDir)).mode & 0o777, 0o700);
  assert.equal((await stat(paths.socketPath)).mode & 0o777, 0o600);
  assert.equal((await stat(paths.statusPath)).mode & 0o777, 0o600);
});

test("repeated start keeps a single daemon and stop uses its request boundary", async (t) => {
  const { root, paths } = await createTestEnvironment();
  let firstProbeCalls = 0;
  let secondProbeCalls = 0;
  const firstAppServer: AppServerController = {
    ...unusedTaskExecution,
    async startAndProbe() {
      firstProbeCalls += 1;
      return { state: "ready", codexVersion: "codex-cli first" };
    },
    async close() {},
  };
  const secondAppServer: AppServerController = {
    ...unusedTaskExecution,
    async startAndProbe() {
      secondProbeCalls += 1;
      return { state: "ready", codexVersion: "codex-cli second" };
    },
    async close() {},
  };

  const first = await startDaemon({ appServer: firstAppServer, paths });
  assert.equal(first.kind, "started");
  if (first.kind !== "started") return;
  t.after(async () => {
    await first.daemon.close();
    await rm(root, { recursive: true, force: true });
  });

  const second = await startDaemon({ appServer: secondAppServer, paths });
  assert.equal(second.kind, "already-running");
  assert.equal(firstProbeCalls, 1);
  assert.equal(secondProbeCalls, 0);

  assert.equal(await stopDaemon(paths), "stopped");
  assert.deepEqual(await getDaemonStatus(paths), { state: "stopped" });
  assert.equal(await stopDaemon(paths), "already-stopped");
});

test("a concurrent start waits behind the single daemon startup", async (t) => {
  const { root, paths } = await createTestEnvironment();
  let releaseFirstProbe: (() => void) | undefined;
  let markFirstProbeEntered: (() => void) | undefined;
  const firstProbeEntered = new Promise<void>((resolve) => {
    markFirstProbeEntered = resolve;
  });
  const firstProbeCanFinish = new Promise<void>((resolve) => {
    releaseFirstProbe = resolve;
  });
  let secondProbeCalls = 0;

  const firstStart = startDaemon({
    appServer: {
      ...unusedTaskExecution,
      async startAndProbe() {
        markFirstProbeEntered?.();
        await firstProbeCanFinish;
        return { state: "ready", codexVersion: "codex-cli first" };
      },
      async close() {},
    },
    paths,
  });
  await firstProbeEntered;
  const secondStartPromise = startDaemon({
    appServer: {
      ...unusedTaskExecution,
      async startAndProbe() {
        secondProbeCalls += 1;
        return { state: "ready", codexVersion: "codex-cli second" };
      },
      async close() {},
    },
    paths,
  });
  releaseFirstProbe?.();
  const firstResult = await firstStart;
  const secondStart = await secondStartPromise;
  t.after(async () => {
    if (firstResult.kind === "started") await firstResult.daemon.close();
    if (secondStart.kind === "started") await secondStart.daemon.close();
    await rm(root, { recursive: true, force: true });
  });

  assert.equal(firstResult.kind, "started");
  assert.equal(secondStart.kind, "already-running");
  assert.equal(secondProbeCalls, 0);
});

test("a replacement daemon waits for the previous App Server to close", async (t) => {
  const { root, paths } = await createTestEnvironment();
  let markCloseEntered: (() => void) | undefined;
  let allowClose: (() => void) | undefined;
  const closeEntered = new Promise<void>((resolve) => {
    markCloseEntered = resolve;
  });
  const closeCanFinish = new Promise<void>((resolve) => {
    allowClose = resolve;
  });
  const first = await startDaemon({
    appServer: {
      ...unusedTaskExecution,
      async startAndProbe() {
        return { state: "ready", codexVersion: "codex-cli first" };
      },
      async close() {
        markCloseEntered?.();
        await closeCanFinish;
      },
    },
    paths,
  });
  assert.equal(first.kind, "started");
  if (first.kind !== "started") return;

  const stopping = stopDaemon(paths);
  await closeEntered;
  let secondProbeCalls = 0;
  const secondStart = startDaemon({
    appServer: {
      ...unusedTaskExecution,
      async startAndProbe() {
        secondProbeCalls += 1;
        return { state: "ready", codexVersion: "codex-cli second" };
      },
      async close() {},
    },
    paths,
  });
  await new Promise((resolve) => setTimeout(resolve, 25));
  const probeCallsBeforeCloseFinished = secondProbeCalls;
  allowClose?.();
  await stopping;
  const second = await secondStart;
  t.after(async () => {
    await first.daemon.close();
    if (second.kind === "started") await second.daemon.close();
    await rm(root, { recursive: true, force: true });
  });

  assert.equal(probeCallsBeforeCloseFinished, 0);
  assert.equal(second.kind, "started");
  assert.equal((await getDaemonStatus(paths)).state, "running");
});

test("daemon status preserves an unauthenticated startup result", async (t) => {
  const { root, paths } = await createTestEnvironment();
  t.after(() => rm(root, { recursive: true, force: true }));
  let closed = false;
  const appServer: AppServerController = {
    ...unusedTaskExecution,
    async startAndProbe() {
      return {
        state: "unauthenticated",
        codexVersion: "codex-cli 0.test",
        message: "ChatGPT authentication required; run `codex login`.",
      };
    },
    async close() {
      closed = true;
    },
  };

  const result = await startDaemon({
    appServer,
    clock: { now: () => new Date("2026-09-04T02:03:04.000Z") },
    paths,
  });

  assert.equal(result.kind, "refused");
  assert.equal(closed, true);
  assert.deepEqual(await getDaemonStatus(paths), {
    state: "unauthenticated",
    codexVersion: "codex-cli 0.test",
    message: "ChatGPT authentication required; run `codex login`.",
    checkedAt: "2026-09-04T02:03:04.000Z",
  });
});

test("daemon refuses missing App Server capabilities and reports the Codex version", async (t) => {
  const { root, paths } = await createTestEnvironment();
  t.after(() => rm(root, { recursive: true, force: true }));
  const appServer: AppServerController = {
    ...unusedTaskExecution,
    async startAndProbe() {
      return {
        state: "incompatible",
        codexVersion: "codex-cli 0.old",
        message: "Required Codex App Server capabilities are unavailable.",
        missingCapabilities: ["request:turn/interrupt"],
      };
    },
    async close() {},
  };

  const result = await startDaemon({
    appServer,
    clock: { now: () => new Date("2026-09-04T03:04:05.000Z") },
    paths,
  });

  assert.equal(result.kind, "refused");
  assert.deepEqual(await getDaemonStatus(paths), {
    state: "incompatible",
    codexVersion: "codex-cli 0.old",
    message: "Required Codex App Server capabilities are unavailable.",
    missingCapabilities: ["request:turn/interrupt"],
    checkedAt: "2026-09-04T03:04:05.000Z",
  });
});

test("daemon probes the installed App Server protocol and current ChatGPT login", async (t) => {
  const { root, paths } = await createTestEnvironment();
  const fakeCodex = await createFakeCodex(root);
  let close: (() => Promise<void>) | undefined;
  t.after(async () => {
    await close?.();
    await rm(root, { recursive: true, force: true });
  });

  const result = await startDaemon({
    appServer: new CodexAppServer({
      command: fakeCodex.command,
      env: { ...process.env, FAKE_CODEX_LOG: fakeCodex.logPath },
    }),
    paths,
  });
  assert.equal(result.kind, "started");
  if (result.kind !== "started") return;
  close = result.daemon.close;

  assert.equal((await getDaemonStatus(paths)).state, "running");
  assert.equal(await stopDaemon(paths), "stopped");

  const records = (await readFile(fakeCodex.logPath, "utf8"))
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line) as {
      type: string;
      message?: { method?: string };
    });
  assert.deepEqual(
    records
      .filter((record) => record.type === "message")
      .map((record) => record.message?.method),
    ["initialize", "initialized", "account/read", "account/rateLimits/read"],
  );
});

test("App Server reads structured Thread and Turn state for restart reconciliation", async (t) => {
  const { root } = await createTestEnvironment();
  const fakeCodex = await createFakeCodex(root, {
    threadReadState: {
      status: "active",
      turns: [{ id: "turn-recovery", status: "inProgress" }],
    },
  });
  const appServer = new CodexAppServer({
    command: fakeCodex.command,
    env: {
      ...process.env,
      FAKE_CODEX_IMPORTED_WORKSPACE: root,
      FAKE_CODEX_LOG: fakeCodex.logPath,
    },
  });
  t.after(async () => {
    await appServer.close();
    await rm(root, { recursive: true, force: true });
  });
  assert.equal((await appServer.startAndProbe()).state, "ready");

  assert.deepEqual(await appServer.readThreadForReconciliation("thread-recovery"), {
    status: "active",
    threadId: "thread-recovery",
    turns: [{ status: "in_progress", turnId: "turn-recovery" }],
  });
});

test("installed protocol probe names a missing required capability", async (t) => {
  const { root, paths } = await createTestEnvironment();
  t.after(() => rm(root, { recursive: true, force: true }));
  const fakeCodex = await createFakeCodex(root, {
    omitClientRequest: "turn/interrupt",
  });

  await startDaemon({
    appServer: new CodexAppServer({
      command: fakeCodex.command,
      env: { ...process.env, FAKE_CODEX_LOG: fakeCodex.logPath },
    }),
    clock: { now: () => new Date("2026-09-04T04:05:06.000Z") },
    paths,
  });

  assert.deepEqual(await getDaemonStatus(paths), {
    state: "incompatible",
    codexVersion: "codex-cli 9.fake",
    message: "Required Codex App Server capabilities are unavailable.",
    missingCapabilities: ["client-request:turn/interrupt"],
    checkedAt: "2026-09-04T04:05:06.000Z",
  });
});

test("installed protocol probe rejects a quota payload without reset times", async (t) => {
  const { root, paths } = await createTestEnvironment();
  let close: (() => Promise<void>) | undefined;
  t.after(async () => {
    await close?.();
    await rm(root, { recursive: true, force: true });
  });
  const fakeCodex = await createFakeCodex(root, {
    omitRateLimitResetTime: true,
  });

  const result = await startDaemon({
    appServer: new CodexAppServer({
      command: fakeCodex.command,
      env: { ...process.env, FAKE_CODEX_LOG: fakeCodex.logPath },
    }),
    paths,
  });
  if (result.kind === "started") close = result.daemon.close;

  const status = await getDaemonStatus(paths);
  assert.equal(status.state, "incompatible");
  if (status.state !== "incompatible") return;
  assert.equal(status.codexVersion, "codex-cli 9.fake");
  assert.ok(status.missingCapabilities.includes("payload:account/rateLimits/read"));
});

test("live App Server probe reports a missing ChatGPT login", async (t) => {
  const { root, paths } = await createTestEnvironment();
  t.after(() => rm(root, { recursive: true, force: true }));
  const fakeCodex = await createFakeCodex(root, { authenticated: false });

  await startDaemon({
    appServer: new CodexAppServer({
      command: fakeCodex.command,
      env: { ...process.env, FAKE_CODEX_LOG: fakeCodex.logPath },
    }),
    clock: { now: () => new Date("2026-09-04T05:06:07.000Z") },
    paths,
  });

  assert.deepEqual(await getDaemonStatus(paths), {
    state: "unauthenticated",
    codexVersion: "codex-cli 9.fake",
    message: "ChatGPT authentication required; run `codex login`.",
    checkedAt: "2026-09-04T05:06:07.000Z",
  });
});

test("live App Server probe classifies a structured auth error as unauthenticated", async (t) => {
  const { root, paths } = await createTestEnvironment();
  t.after(() => rm(root, { recursive: true, force: true }));
  const fakeCodex = await createFakeCodex(root, { authRpcError: true });

  await startDaemon({
    appServer: new CodexAppServer({
      command: fakeCodex.command,
      env: { ...process.env, FAKE_CODEX_LOG: fakeCodex.logPath },
    }),
    clock: { now: () => new Date("2026-09-04T06:07:08.000Z") },
    paths,
  });

  assert.deepEqual(await getDaemonStatus(paths), {
    state: "unauthenticated",
    codexVersion: "codex-cli 9.fake",
    message: "ChatGPT authentication required; run `codex login`.",
    checkedAt: "2026-09-04T06:07:08.000Z",
  });
});
