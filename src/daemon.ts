import {
  chmod,
  lstat,
  mkdir,
  open,
  readFile,
  rename,
  unlink,
  writeFile,
  type FileHandle,
} from "node:fs/promises";
import { createConnection, createServer, type Server, type Socket } from "node:net";

import type { DaemonPaths } from "./paths.js";
import { readConfiguration } from "./config.js";
import { StateStore, type QueueSnapshot } from "./state-store.js";
import { handleTaskRequest, TaskService } from "./task-service.js";

export interface Clock {
  now(): Date;
  waitUntil?(until: Date, signal: AbortSignal): Promise<void>;
}

export interface RateLimitWindow {
  resetsAt: number | null;
  usedPercent: number;
}

export interface RateLimitSnapshot {
  limitId: string | null;
  primary: RateLimitWindow | null;
  rateLimitReachedType: string | null;
  secondary: RateLimitWindow | null;
}

export interface AccountRateLimits {
  rateLimits: RateLimitSnapshot | null;
  rateLimitsByLimitId: Record<string, RateLimitSnapshot> | null;
}

export interface UsageLimitExceeded {
  threadId: string;
  turnId: string;
}

export interface AppServerController {
  startAndProbe(): Promise<AppServerProbeResult>;
  readRateLimits(): Promise<AccountRateLimits>;
  readThread(threadId: string): Promise<{ threadId: string; workspace: string }>;
  resumeThread(threadId: string): Promise<{ threadId: string }>;
  startThread(workspace: string): Promise<{ threadId: string }>;
  startTurn(threadId: string, prompt: string): Promise<{ turnId: string }>;
  onTurnCompleted(listener: (turn: CompletedTurn) => void): () => void;
  onUsageLimitExceeded(listener: (event: UsageLimitExceeded) => void): () => void;
  close(): Promise<void>;
}

export interface CompletedTurn {
  status: "completed" | "failed" | "interrupted";
  threadId: string;
  turnId: string;
}

export type AppServerProbeResult =
  | { state: "ready"; codexVersion: string }
  | { state: "unauthenticated"; codexVersion: string; message: string }
  | {
      state: "incompatible";
      codexVersion: string;
      message: string;
      missingCapabilities: string[];
    };

export type DaemonStatus =
  | { state: "stopped" }
  | {
      state: "running";
      pid: number;
      codexVersion: string;
      startedAt: string;
    }
  | {
      state: "unauthenticated";
      codexVersion: string;
      message: string;
      checkedAt: string;
    }
  | {
      state: "incompatible";
      codexVersion: string;
      message: string;
      missingCapabilities: string[];
      checkedAt: string;
    };

export interface RunningDaemon {
  close(): Promise<void>;
}

export type StartDaemonResult =
  | { kind: "started"; daemon: RunningDaemon }
  | { kind: "already-running"; status: Extract<DaemonStatus, { state: "running" }> }
  | { kind: "already-starting" }
  | {
      kind: "refused";
      status: Extract<DaemonStatus, { state: "unauthenticated" | "incompatible" }>;
    };

interface StartDaemonOptions {
  appServer: AppServerController;
  clock?: Clock;
  paths: DaemonPaths;
}

const systemClock: Clock = { now: () => new Date() };

export async function startDaemon({
  appServer,
  clock = systemClock,
  paths,
}: StartDaemonOptions): Promise<StartDaemonResult> {
  const existing = await requestRunningStatus(paths);
  if (existing) return { kind: "already-running", status: existing };

  await preparePrivateDirectories(paths);
  const lockResult = await acquireDaemonLock(paths);
  if (!lockResult) return { kind: "already-starting" };
  if ("status" in lockResult) {
    return { kind: "already-running", status: lockResult.status };
  }
  const daemonLock = lockResult;
  let lockOwnedByRunningDaemon = false;

  try {
    const concurrentlyStartedDaemon = await requestRunningStatus(paths);
    if (concurrentlyStartedDaemon) {
      return { kind: "already-running", status: concurrentlyStartedDaemon };
    }
    await unlinkIfExists(paths.socketPath);

    const probe = await appServer.startAndProbe();
    if (probe.state !== "ready") {
      const status = {
        ...probe,
        checkedAt: clock.now().toISOString(),
      };
      await writeStatus(paths.statusPath, status);
      await appServer.close();
      return { kind: "refused", status };
    }

    const status: Extract<DaemonStatus, { state: "running" }> = {
      state: "running",
      pid: process.pid,
      codexVersion: probe.codexVersion,
      startedAt: clock.now().toISOString(),
    };
    let store: StateStore;
    try {
      store = new StateStore(paths.databasePath);
    } catch (error) {
      await appServer.close();
      throw error;
    }
    const taskService = new TaskService(
      appServer,
      store,
      clock,
      async () => (await readConfiguration(paths.configPath)).continuationPrompt,
    );
    const unsubscribeTurnCompleted = appServer.onTurnCompleted((turn) => {
      taskService.handleTurnCompleted(turn);
    });
    const unsubscribeUsageLimit = appServer.onUsageLimitExceeded((event) => {
      taskService.handleUsageLimitExceeded(event);
    });
    let closing: Promise<void> | undefined;
    const close = (): Promise<void> => {
      closing ??= closeDaemon(
        server,
        appServer,
        paths,
        daemonLock,
        store,
        taskService,
        [unsubscribeTurnCompleted, unsubscribeUsageLimit],
      );
      return closing;
    };
    const server = createDaemonServer(status, taskService, () => {
      void close();
    });

    try {
      await listen(server, paths.socketPath);
    } catch (error) {
      unsubscribeTurnCompleted();
      unsubscribeUsageLimit();
      store.close();
      await appServer.close();
      const runningWinner = await requestRunningStatus(paths);
      if (runningWinner) return { kind: "already-running", status: runningWinner };
      throw error;
    }

    await chmod(paths.socketPath, 0o600);
    await writeStatus(paths.statusPath, status);

    lockOwnedByRunningDaemon = true;
    return { kind: "started", daemon: { close } };
  } finally {
    if (!lockOwnedByRunningDaemon) await daemonLock.release();
  }
}

export async function addWorkspaceTask(
  paths: DaemonPaths,
  workspace: string,
  prompt: string,
): Promise<number> {
  const result = await requestResult(paths.socketPath, {
    method: "task/add",
    params: { workspace, prompt },
  });
  if (!isRecord(result) || typeof result.taskId !== "number") {
    throw new Error("daemon returned an invalid Task result");
  }
  return result.taskId;
}

export async function addManagedThreadTask(
  paths: DaemonPaths,
  threadId: string,
  prompt: string,
): Promise<number> {
  const result = await requestResult(paths.socketPath, {
    method: "task/add",
    params: { threadId, prompt },
  });
  if (!isRecord(result) || typeof result.taskId !== "number") {
    throw new Error("daemon returned an invalid Task result");
  }
  return result.taskId;
}

export async function importManagedThread(
  paths: DaemonPaths,
  threadId: string,
): Promise<{ threadId: string; workspace: string }> {
  const result = await requestResult(paths.socketPath, {
    method: "thread/import",
    params: { threadId },
  });
  if (
    !isRecord(result)
    || typeof result.threadId !== "string"
    || typeof result.workspace !== "string"
  ) {
    throw new Error("daemon returned an invalid Managed Thread result");
  }
  return { threadId: result.threadId, workspace: result.workspace };
}

export async function moveTask(
  paths: DaemonPaths,
  taskId: number,
  relativeTaskId: number,
  placement: "after" | "before",
): Promise<void> {
  const result = await requestResult(paths.socketPath, {
    method: "task/move",
    params: { taskId, relativeTaskId, placement },
  });
  if (!isRecord(result) || result.taskId !== taskId) {
    throw new Error("daemon returned an invalid Task move result");
  }
}

export async function cancelTask(paths: DaemonPaths, taskId: number): Promise<void> {
  const result = await requestResult(paths.socketPath, {
    method: "task/cancel",
    params: { taskId },
  });
  if (!isRecord(result) || result.taskId !== taskId) {
    throw new Error("daemon returned an invalid Task cancellation result");
  }
}

export async function startQueueRun(
  paths: DaemonPaths,
): Promise<"started" | "already-running" | "idle"> {
  const result = await requestResult(paths.socketPath, { method: "queue/start" });
  if (
    !isRecord(result)
    || (result.state !== "started" && result.state !== "already-running" && result.state !== "idle")
  ) {
    throw new Error("daemon returned an invalid Queue start result");
  }
  return result.state;
}

export async function getQueueStatus(paths: DaemonPaths): Promise<QueueSnapshot> {
  const result = await requestResult(paths.socketPath, { method: "queue/status" });
  if (!isQueueSnapshot(result)) throw new Error("daemon returned an invalid Queue status");
  return result;
}

export async function getDaemonStatus(paths: DaemonPaths): Promise<DaemonStatus> {
  const running = await requestRunningStatus(paths);
  if (running) return running;

  const stored = await readStatus(paths.statusPath);
  if (stored?.state === "unauthenticated" || stored?.state === "incompatible") {
    return stored;
  }
  return { state: "stopped" };
}

export async function stopDaemon(
  paths: DaemonPaths,
): Promise<"stopped" | "already-stopped"> {
  try {
    const response = await sendRequest(paths.socketPath, { method: "stop" });
    if (!isRecord(response.result) || response.result.state !== "stopping") {
      return "already-stopped";
    }
  } catch {
    return "already-stopped";
  }

  for (let attempt = 0; attempt < 50; attempt += 1) {
    if (!(await requestRunningStatus(paths))) return "stopped";
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error("daemon did not stop within 500ms");
}

async function preparePrivateDirectories(paths: DaemonPaths): Promise<void> {
  await Promise.all([
    ensurePrivateDirectory(paths.configDir),
    ensurePrivateDirectory(paths.stateDir),
    ensurePrivateDirectory(paths.runtimeDir),
  ]);
}

interface StartupLock {
  release(): Promise<void>;
}

async function acquireDaemonLock(
  paths: DaemonPaths,
): Promise<StartupLock | { status: Extract<DaemonStatus, { state: "running" }> } | undefined> {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    const running = await requestRunningStatus(paths);
    if (running) return { status: running };

    try {
      const handle = await open(paths.lockPath, "wx", 0o600);
      await handle.writeFile(`${process.pid}\n`, "utf8");
      return lockHandle(handle, paths.lockPath);
    } catch (error) {
      if (!hasErrorCode(error, "EEXIST")) throw error;
      if (await lockOwnerIsAlive(paths.lockPath)) {
        await new Promise((resolve) => setTimeout(resolve, 50));
        continue;
      }

      const stalePath = `${paths.lockPath}.stale-${process.pid}`;
      try {
        await rename(paths.lockPath, stalePath);
        await unlinkIfExists(stalePath);
      } catch (renameError) {
        if (!hasErrorCode(renameError, "ENOENT")) throw renameError;
      }
    }
  }
  return undefined;
}

function lockHandle(handle: FileHandle, lockPath: string): StartupLock {
  let released = false;
  return {
    async release() {
      if (released) return;
      released = true;
      await handle.close();
      await unlinkIfExists(lockPath);
    },
  };
}

async function lockOwnerIsAlive(lockPath: string): Promise<boolean> {
  try {
    const contents = await readFile(lockPath, "utf8");
    const pid = Number.parseInt(contents.trim(), 10);
    if (!Number.isSafeInteger(pid) || pid <= 0) return false;
    try {
      process.kill(pid, 0);
      return true;
    } catch (error) {
      return !hasErrorCode(error, "ESRCH");
    }
  } catch (error) {
    if (hasErrorCode(error, "ENOENT")) return false;
    throw error;
  }
}

async function ensurePrivateDirectory(directory: string): Promise<void> {
  await mkdir(directory, { recursive: true, mode: 0o700 });
  const metadata = await lstat(directory);
  if (!metadata.isDirectory() || metadata.isSymbolicLink()) {
    throw new Error(`Refusing to use non-directory path: ${directory}`);
  }
  const uid = process.getuid?.();
  if (uid !== undefined && metadata.uid !== uid) {
    throw new Error(`Refusing to use directory owned by another user: ${directory}`);
  }
  await chmod(directory, 0o700);
}

function createDaemonServer(
  status: Extract<DaemonStatus, { state: "running" }>,
  taskService: TaskService,
  onStop: () => void,
): Server {
  return createServer((socket) => handleConnection(socket, status, taskService, onStop));
}

function handleConnection(
  socket: Socket,
  status: Extract<DaemonStatus, { state: "running" }>,
  taskService: TaskService,
  onStop: () => void,
): void {
  let input = "";
  let handled = false;
  socket.setEncoding("utf8");
  socket.on("data", (chunk: string) => {
    input += chunk;
    const newline = input.indexOf("\n");
    if (newline === -1 || handled) return;
    handled = true;

    try {
      const request: unknown = JSON.parse(input.slice(0, newline));
      if (!isRecord(request)) {
        socket.end(`${JSON.stringify({ error: "invalid daemon request" })}\n`);
        return;
      }
      if (request.method === "status") {
        socket.end(`${JSON.stringify({ result: status })}\n`);
      } else if (request.method === "stop") {
        socket.end(`${JSON.stringify({ result: { state: "stopping" } })}\n`, onStop);
      } else {
        void handleTaskRequest(request, taskService)
          .then((result) => socket.end(`${JSON.stringify({ result })}\n`))
          .catch((error: unknown) => {
            socket.end(`${JSON.stringify({ error: errorMessage(error) })}\n`);
          });
      }
    } catch {
      socket.end(`${JSON.stringify({ error: "invalid daemon request" })}\n`);
    }
  });
}

function listen(server: Server, socketPath: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const onError = (error: Error): void => reject(error);
    server.once("error", onError);
    server.listen(socketPath, () => {
      server.off("error", onError);
      resolve();
    });
  });
}

async function closeDaemon(
  server: Server,
  appServer: AppServerController,
  paths: DaemonPaths,
  daemonLock: StartupLock,
  store: StateStore,
  taskService: TaskService,
  unsubscribe: Array<() => void>,
): Promise<void> {
  try {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => (error ? reject(error) : resolve()));
    });
    for (const stopListening of unsubscribe) stopListening();
    taskService.close();
    await appServer.close();
    store.close();
    await unlinkIfExists(paths.socketPath);
    await writeStatus(paths.statusPath, { state: "stopped" });
  } finally {
    await daemonLock.release();
  }
}

async function requestRunningStatus(
  paths: DaemonPaths,
): Promise<Extract<DaemonStatus, { state: "running" }> | undefined> {
  try {
    const response = await sendRequest(paths.socketPath, { method: "status" });
    return isRunningStatus(response.result) ? response.result : undefined;
  } catch {
    return undefined;
  }
}

function sendRequest(
  socketPath: string,
  request: { method: string; params?: unknown },
): Promise<{ result?: unknown; error?: unknown }> {
  return new Promise((resolve, reject) => {
    const socket = createConnection(socketPath);
    let input = "";
    const timeout = setTimeout(() => {
      socket.destroy();
      reject(new Error("daemon request timed out"));
    }, 500);

    const finish = (callback: () => void): void => {
      clearTimeout(timeout);
      callback();
    };

    socket.setEncoding("utf8");
    socket.once("error", (error) => finish(() => reject(error)));
    socket.once("connect", () => {
      socket.write(`${JSON.stringify(request)}\n`);
    });
    socket.on("data", (chunk: string) => {
      input += chunk;
      const newline = input.indexOf("\n");
      if (newline === -1) return;
      socket.end();
      finish(() => {
        try {
          resolve(JSON.parse(input.slice(0, newline)) as { result?: unknown; error?: unknown });
        } catch (error) {
          reject(error);
        }
      });
    });
  });
}

async function requestResult(
  socketPath: string,
  request: { method: string; params?: unknown },
): Promise<unknown> {
  const response = await sendRequest(socketPath, request);
  if (typeof response.error === "string") throw new Error(response.error);
  return response.result;
}

function isQueueSnapshot(value: unknown): value is QueueSnapshot {
  if (!isRecord(value) || !Array.isArray(value.tasks)) return false;
  if (value.state !== "paused" && value.state !== "running" && value.state !== "idle") {
    return false;
  }
  return value.tasks.every((task) =>
    isRecord(task)
    && typeof task.id === "number"
    && typeof task.workspace === "string"
    && (
      task.state === "queued"
      || task.state === "running"
      || task.state === "waiting_for_quota"
      || task.state === "completed"
      || task.state === "cancelled"
    )
    && (task.managedThreadId === undefined || typeof task.managedThreadId === "string")
    && (task.activeTurnId === undefined || typeof task.activeTurnId === "string")
    && (task.quotaLimitId === undefined || typeof task.quotaLimitId === "string")
    && (task.quotaLimitType === undefined || typeof task.quotaLimitType === "string")
    && (task.quotaResetAt === undefined || typeof task.quotaResetAt === "string")
  );
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function isRunningStatus(
  value: unknown,
): value is Extract<DaemonStatus, { state: "running" }> {
  if (!isRecord(value)) return false;
  return value.state === "running"
    && typeof value.pid === "number"
    && typeof value.codexVersion === "string"
    && typeof value.startedAt === "string";
}

async function readStatus(statusPath: string): Promise<DaemonStatus | undefined> {
  try {
    const value: unknown = JSON.parse(await readFile(statusPath, "utf8"));
    if (!isRecord(value) || typeof value.state !== "string") return undefined;
    if (value.state === "stopped") return { state: "stopped" };
    if (value.state === "running" && isRunningStatus(value)) return value;
    if (
      value.state === "unauthenticated"
      && typeof value.codexVersion === "string"
      && typeof value.message === "string"
      && typeof value.checkedAt === "string"
    ) {
      return {
        state: value.state,
        codexVersion: value.codexVersion,
        message: value.message,
        checkedAt: value.checkedAt,
      };
    }
    if (
      value.state === "incompatible"
      && typeof value.codexVersion === "string"
      && typeof value.message === "string"
      && typeof value.checkedAt === "string"
      && Array.isArray(value.missingCapabilities)
      && value.missingCapabilities.every((item) => typeof item === "string")
    ) {
      return {
        state: value.state,
        codexVersion: value.codexVersion,
        message: value.message,
        missingCapabilities: value.missingCapabilities,
        checkedAt: value.checkedAt,
      };
    }
  } catch {
    return undefined;
  }
  return undefined;
}

async function writeStatus(statusPath: string, status: DaemonStatus): Promise<void> {
  await writeFile(statusPath, `${JSON.stringify(status)}\n`, { mode: 0o600 });
  await chmod(statusPath, 0o600);
}

async function unlinkIfExists(targetPath: string): Promise<void> {
  await unlink(targetPath).catch((error: unknown) => {
    if (!hasErrorCode(error, "ENOENT")) throw error;
  });
}

function hasErrorCode(error: unknown, code: string): boolean {
  return isRecord(error) && error.code === code;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
