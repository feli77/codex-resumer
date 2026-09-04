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

export interface Clock {
  now(): Date;
}

export interface AppServerController {
  startAndProbe(): Promise<AppServerProbeResult>;
  close(): Promise<void>;
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
    let closing: Promise<void> | undefined;
    const close = (): Promise<void> => {
      closing ??= closeDaemon(server, appServer, paths, daemonLock);
      return closing;
    };
    const server = createDaemonServer(status, () => {
      void close();
    });

    try {
      await listen(server, paths.socketPath);
    } catch (error) {
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
  onStop: () => void,
): Server {
  return createServer((socket) => handleConnection(socket, status, onStop));
}

function handleConnection(
  socket: Socket,
  status: Extract<DaemonStatus, { state: "running" }>,
  onStop: () => void,
): void {
  let input = "";
  socket.setEncoding("utf8");
  socket.on("data", (chunk: string) => {
    input += chunk;
    const newline = input.indexOf("\n");
    if (newline === -1) return;

    try {
      const request = JSON.parse(input.slice(0, newline)) as { method?: unknown };
      if (request.method === "status") {
        socket.end(`${JSON.stringify({ result: status })}\n`);
      } else if (request.method === "stop") {
        socket.end(`${JSON.stringify({ result: { state: "stopping" } })}\n`, onStop);
      } else {
        socket.end(`${JSON.stringify({ error: "unknown daemon request" })}\n`);
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
): Promise<void> {
  try {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => (error ? reject(error) : resolve()));
    });
    await appServer.close();
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
  request: { method: string },
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
