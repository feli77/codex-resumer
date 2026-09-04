#!/usr/bin/env node

import { spawn } from "node:child_process";
import path from "node:path";

import { CodexAppServer } from "./app-server.js";
import {
  addWorkspaceTask,
  getDaemonStatus,
  getQueueStatus,
  startDaemon,
  startQueueRun,
  stopDaemon,
  type DaemonStatus,
  type RunningDaemon,
} from "./daemon.js";
import type { QueueSnapshot } from "./state-store.js";
import { resolvePaths, type DaemonPaths } from "./paths.js";

const usage = `Usage:
  codex-resumer daemon <start|stop|status>
  codex-resumer task add --workspace <path> <prompt>
  codex-resumer queue <start|status>

Manage the daemon and run a Workspace Task.
`;

async function main(args: string[]): Promise<number> {
  if (args.length === 1 && (args[0] === "--help" || args[0] === "-h")) {
    process.stdout.write(usage);
    return 0;
  }
  const paths = resolvePaths();
  if (args[0] === "daemon" && args[1] && args.length === 2) {
    switch (args[1]) {
      case "start":
        return startDetached(paths);
      case "stop": {
        const result = await stopDaemon(paths);
        process.stdout.write(
          result === "stopped" ? "Daemon stopped.\n" : "Daemon is already stopped.\n",
        );
        return 0;
      }
      case "status":
        process.stdout.write(`${renderStatus(await getDaemonStatus(paths))}\n`);
        return 0;
      case "run":
        return runForeground(paths);
    }
  }

  if (
    args[0] === "task"
    && args[1] === "add"
    && args[2] === "--workspace"
    && args[3]
    && args[4]
  ) {
    const taskId = await addWorkspaceTask(
      paths,
      path.resolve(args[3]),
      args.slice(4).join(" "),
    );
    process.stdout.write(`Task ${taskId} added.\n`);
    return 0;
  }

  if (args[0] === "queue" && args[1] === "start" && args.length === 2) {
    const result = await startQueueRun(paths);
    process.stdout.write(
      result === "started"
        ? "Queue started.\n"
        : result === "already-running"
          ? "Queue already has a running Task.\n"
          : "Queue is idle.\n",
    );
    return 0;
  }

  if (args[0] === "queue" && args[1] === "status" && args.length === 2) {
    process.stdout.write(renderQueueStatus(await getQueueStatus(paths)));
    return 0;
  }

  process.stderr.write(usage);
  return 2;
}

async function startDetached(paths: DaemonPaths): Promise<number> {
  const current = await getDaemonStatus(paths);
  if (current.state === "running") {
    process.stdout.write("Daemon is already running.\n");
    return 0;
  }

  const entryPoint = process.argv[1];
  if (!entryPoint) throw new Error("Unable to locate the Codex Resumer entry point");
  const child = spawn(
    process.execPath,
    [...process.execArgv, entryPoint, "daemon", "run"],
    { detached: true, env: process.env, stdio: "ignore" },
  );
  let exited = false;
  child.once("exit", () => {
    exited = true;
  });
  child.unref();

  for (let attempt = 0; attempt < 200; attempt += 1) {
    const status = await getDaemonStatus(paths);
    if (status.state === "running") {
      process.stdout.write("Daemon started.\n");
      return 0;
    }
    if (exited) {
      process.stderr.write(`${renderStatus(status)}\n`);
      return 1;
    }
    await delay(50);
  }

  process.stderr.write("Daemon did not become ready within 10 seconds.\n");
  return 1;
}

async function runForeground(paths: DaemonPaths): Promise<number> {
  const result = await startDaemon({
    appServer: new CodexAppServer(),
    paths,
  });
  if (result.kind === "refused") return 1;
  if (result.kind === "already-running") return 0;
  if (result.kind === "already-starting") {
    for (let attempt = 0; attempt < 200; attempt += 1) {
      if ((await getDaemonStatus(paths)).state === "running") return 0;
      await delay(50);
    }
    return 1;
  }

  installShutdownHandlers(result.daemon);
  return 0;
}

function installShutdownHandlers(daemon: RunningDaemon): void {
  let shuttingDown = false;
  const shutdown = (): void => {
    if (shuttingDown) return;
    shuttingDown = true;
    void daemon.close().catch((error: unknown) => {
      process.stderr.write(`Failed to stop daemon: ${errorMessage(error)}\n`);
      process.exitCode = 1;
    });
  };
  process.once("SIGINT", shutdown);
  process.once("SIGTERM", shutdown);
}

function renderStatus(status: DaemonStatus): string {
  switch (status.state) {
    case "running":
      return `Daemon is running (PID ${status.pid}, ${status.codexVersion}).`;
    case "stopped":
      return "Daemon is stopped.";
    case "unauthenticated":
      return `Daemon is unauthenticated: ${status.message} Codex: ${status.codexVersion}.`;
    case "incompatible":
      return `Daemon is incompatible: ${status.message} Missing: ${status.missingCapabilities.join(", ")}. Codex: ${status.codexVersion}.`;
  }
}

function renderQueueStatus(snapshot: QueueSnapshot): string {
  const lines = [`Queue is ${snapshot.state}.`];
  for (const task of snapshot.tasks) {
    lines.push(`Task ${task.id}: ${task.state}`);
    lines.push(`  Workspace ${task.workspace}`);
    if (task.managedThreadId || task.activeTurnId) {
      lines.push(
        `  Thread ${task.managedThreadId ?? "pending"}, Turn ${task.activeTurnId ?? "pending"}`,
      );
    }
  }
  return `${lines.join("\n")}\n`;
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

void main(process.argv.slice(2))
  .then((exitCode) => {
    process.exitCode = exitCode;
  })
  .catch((error: unknown) => {
    process.stderr.write(`${errorMessage(error)}\n`);
    process.exitCode = 1;
  });
