#!/usr/bin/env node

import { spawn } from "node:child_process";
import path from "node:path";

import { CodexAppServer } from "./app-server.js";
import { readConfiguration, setContinuationPrompt } from "./config.js";
import {
  addManagedThreadTask,
  addWorkspaceTask,
  cancelTask,
  getDaemonStatus,
  getQueueStatus,
  importManagedThread,
  moveTask,
  pauseQueue,
  resumeQueueRun,
  startDaemon,
  startQueueRun,
  stopDaemon,
  type DaemonStatus,
  type RunningDaemon,
} from "./daemon.js";
import { normalizeCutoffTime, type RunPolicy } from "./run-policy.js";
import type { QueueSnapshot } from "./state-store.js";
import { resolvePaths, type DaemonPaths } from "./paths.js";

const usage = `Usage:
  codex-resumer daemon <start|stop|status>
  codex-resumer task add (--workspace <path> | --thread <thread-id>) [<prompt>]
  codex-resumer task <list|cancel <task-id>>
  codex-resumer task move <task-id> (--before|--after) <task-id>
  codex-resumer thread import <thread-id>
  codex-resumer queue start (--until-idle | --cutoff <timestamp>)
  codex-resumer queue pause
  codex-resumer queue resume (--until-idle | --cutoff <timestamp>)
  codex-resumer queue status
  codex-resumer config show
  codex-resumer config set continuationPrompt <prompt>

Manage the daemon and a FIFO Queue across Managed Threads and Workspaces.
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

  if (args[0] === "task" && args[1] === "add") {
    const { target, promptArguments } = parseTaskAdd(args.slice(2));
    const prompt = promptArguments.length > 0
      ? promptArguments.join(" ")
      : await readStdin();
    const taskId = target.kind === "workspace"
      ? await addWorkspaceTask(paths, path.resolve(target.value), prompt)
      : await addManagedThreadTask(paths, target.value, prompt);
    process.stdout.write(`Task ${taskId} added.\n`);
    return 0;
  }

  if (args[0] === "thread" && args[1] === "import" && args[2] && args.length === 3) {
    const thread = await importManagedThread(paths, args[2]);
    process.stdout.write(
      `Thread ${thread.threadId} imported for Workspace ${thread.workspace}.\n`,
    );
    return 0;
  }

  if (args[0] === "task" && args[1] === "move" && args.length === 5) {
    const taskId = parseTaskId(args[2]);
    const placement = args[3] === "--before"
      ? "before"
      : args[3] === "--after"
        ? "after"
        : undefined;
    const relativeTaskId = parseTaskId(args[4]);
    if (!placement) throw new Error("task move requires --before or --after.");
    await moveTask(paths, taskId, relativeTaskId, placement);
    process.stdout.write(`Task ${taskId} moved.\n`);
    return 0;
  }

  if (args[0] === "task" && args[1] === "cancel" && args.length === 3) {
    const taskId = parseTaskId(args[2]);
    await cancelTask(paths, taskId);
    process.stdout.write(`Task ${taskId} cancelled.\n`);
    return 0;
  }

  if (args[0] === "task" && args[1] === "list" && args.length === 2) {
    process.stdout.write(renderQueueStatus(await getQueueStatus(paths)));
    return 0;
  }

  if (args[0] === "queue" && args[1] === "start") {
    const result = await startQueueRun(
      paths,
      parseRunPolicy(args.slice(2), "queue start"),
    );
    process.stdout.write(
      result === "started"
        ? "Queue started.\n"
        : result === "already-running"
          ? "Queue already has a running Task.\n"
          : result === "paused"
            ? "Queue is paused.\n"
            : "Queue is idle.\n",
    );
    return 0;
  }

  if (args[0] === "queue" && args[1] === "pause" && args.length === 2) {
    await pauseQueue(paths);
    process.stdout.write("Queue paused.\n");
    return 0;
  }

  if (args[0] === "queue" && args[1] === "resume") {
    const result = await resumeQueueRun(
      paths,
      parseRunPolicy(args.slice(2), "queue resume"),
    );
    process.stdout.write(
      result === "started"
        ? "Queue resumed.\n"
        : result === "already-running"
          ? "Queue is already running.\n"
          : result === "paused"
            ? "Queue is paused.\n"
            : "Queue is idle.\n",
    );
    return 0;
  }

  if (args[0] === "queue" && args[1] === "status" && args.length === 2) {
    process.stdout.write(renderQueueStatus(await getQueueStatus(paths)));
    return 0;
  }

  if (args[0] === "config" && args[1] === "show" && args.length === 2) {
    const config = await readConfiguration(paths.configPath);
    process.stdout.write(`Continuation prompt: ${config.continuationPrompt}\n`);
    return 0;
  }

  if (
    args[0] === "config"
    && args[1] === "set"
    && args[2] === "continuationPrompt"
    && args.length >= 4
  ) {
    await setContinuationPrompt(paths, args.slice(3).join(" "));
    process.stdout.write("Continuation prompt updated.\n");
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
  if (snapshot.pauseReason) {
    lines.push(`Pause reason: ${renderPauseReason(snapshot.pauseReason)}.`);
  }
  if (snapshot.queueRun) {
    lines.push(
      `Queue Run ${snapshot.queueRun.id}: ${
        snapshot.queueRun.runPolicy === "until_idle" ? "Until Idle" : "Cutoff Time"
      }`,
    );
    lines.push(`  Started ${snapshot.queueRun.startedAt}`);
    if (snapshot.queueRun.cutoffTime) {
      lines.push(`  Cutoff Time ${snapshot.queueRun.cutoffTime}`);
    }
    if (snapshot.queueRun.endedAt) {
      lines.push(`  Ended ${snapshot.queueRun.endedAt}`);
    }
  }
  for (const task of snapshot.tasks) {
    lines.push(`Task ${task.id}: ${task.state}`);
    lines.push(`  Workspace ${task.workspace}`);
    lines.push(
      `  Thread ${task.managedThreadId ?? "new"}, Turn ${task.activeTurnId ?? "pending"}`,
    );
    if (task.state === "waiting_for_quota") {
      lines.push(
        `  Quota ${task.quotaLimitId ?? "unknown"}`
        + `${task.quotaLimitType ? ` (${task.quotaLimitType})` : ""}, `
        + `reset ${task.quotaResetAt ?? "unknown"}`,
      );
    }
  }
  return `${lines.join("\n")}\n`;
}

function renderPauseReason(reason: NonNullable<QueueSnapshot["pauseReason"]>): string {
  switch (reason) {
    case "cutoff_reached":
      return "Cutoff Time reached";
    case "manual":
      return "manual pause";
    case "not_started":
      return "not started";
    case "needs_attention":
      return "Needs Attention";
    case "run_policy_required":
      return "Run Policy required";
  }
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function readStdin(): Promise<string> {
  process.stdin.setEncoding("utf8");
  let input = "";
  for await (const chunk of process.stdin) input += chunk;
  return input;
}

function parseTaskAdd(args: string[]): {
  target: { kind: "thread" | "workspace"; value: string };
  promptArguments: string[];
} {
  const targets: Array<{ kind: "thread" | "workspace"; value: string }> = [];
  const promptArguments: string[] = [];
  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    if (argument === "--thread" || argument === "--workspace") {
      const value = args[index + 1];
      if (!value) throw new Error(`${argument} requires a value.`);
      targets.push({
        kind: argument === "--thread" ? "thread" : "workspace",
        value,
      });
      index += 1;
    } else {
      if (argument?.startsWith("--")) throw new Error(`Unknown option: ${argument}`);
      if (argument !== undefined) promptArguments.push(argument);
    }
  }
  if (targets.length !== 1) {
    throw new Error("task add requires exactly one Managed Thread or Workspace.");
  }
  const target = targets[0];
  if (!target) throw new Error("task add requires exactly one target.");
  return { target, promptArguments };
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function parseTaskId(value: string | undefined): number {
  const taskId = Number(value);
  if (!Number.isSafeInteger(taskId) || taskId <= 0) {
    throw new Error(`Invalid Task ID: ${value ?? "missing"}`);
  }
  return taskId;
}

function parseRunPolicy(args: string[], command: string): RunPolicy {
  if (args.length === 1 && args[0] === "--until-idle") {
    return { kind: "until_idle" };
  }
  if (args.length === 2 && args[0] === "--cutoff") {
    const cutoff = args[1];
    if (cutoff) {
      return { cutoffTime: normalizeCutoffTime(cutoff), kind: "cutoff_time" };
    }
  }
  throw new Error(`${command} requires --until-idle or --cutoff <timestamp>.`);
}

void main(process.argv.slice(2))
  .then((exitCode) => {
    process.exitCode = exitCode;
  })
  .catch((error: unknown) => {
    process.stderr.write(`${errorMessage(error)}\n`);
    process.exitCode = 1;
  });
