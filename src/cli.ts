#!/usr/bin/env node

import { spawn } from "node:child_process";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { createInterface } from "node:readline/promises";

import { CodexAppServer } from "./app-server.js";
import {
  readConfiguration,
  setAccessMode,
  setContinuationPrompt,
  type AccessMode,
} from "./config.js";
import {
  addManagedThreadTask,
  addWorkspaceTask,
  cancelTask,
  completeTask,
  getDaemonStatus,
  getQueueStatus,
  importManagedThread,
  moveTask,
  pauseQueue,
  retryTask,
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
  codex-resumer daemon start
  codex-resumer daemon stop [--force]
  codex-resumer daemon status
  codex-resumer task add (--workspace <path> | --thread <thread-id>) [<prompt>]
  codex-resumer task <list|cancel <task-id>|complete <task-id>>
  codex-resumer task retry <task-id> [<new-prompt>]
  codex-resumer task move <task-id> (--before|--after) <task-id>
  codex-resumer thread import <thread-id>
  codex-resumer queue start (--until-idle | --cutoff <timestamp>) [--yes]
  codex-resumer queue pause
  codex-resumer queue resume (--until-idle | --cutoff <timestamp>) [--yes]
  codex-resumer queue status
  codex-resumer logs read
  codex-resumer logs follow
  codex-resumer config show
  codex-resumer config set accessMode <configured|full>
  codex-resumer config set continuationPrompt <prompt>

Manage the daemon and a FIFO Queue across Managed Threads and Workspaces.
`;

async function main(args: string[]): Promise<number> {
  if (args.length === 1 && (args[0] === "--help" || args[0] === "-h")) {
    process.stdout.write(usage);
    return 0;
  }
  const paths = resolvePaths();
  if (args[0] === "daemon" && args[1]) {
    switch (args[1]) {
      case "start":
        if (args.length !== 2) break;
        return startDetached(paths);
      case "stop": {
        if (
          args.length !== 2
          && !(args.length === 3 && args[2] === "--force")
        ) break;
        const result = await stopDaemon(paths, args[2] === "--force");
        process.stdout.write(
          result === "stopped" ? "Daemon stopped.\n" : "Daemon is already stopped.\n",
        );
        return 0;
      }
      case "status":
        if (args.length !== 2) break;
        process.stdout.write(`${renderStatus(await getDaemonStatus(paths))}\n`);
        return 0;
      case "run":
        if (args.length !== 2) break;
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

  if (args[0] === "task" && args[1] === "retry" && args[2]) {
    const taskId = parseTaskId(args[2]);
    const prompt = args.length > 3 ? args.slice(3).join(" ") : await readStdin();
    await retryTask(paths, taskId, prompt);
    process.stdout.write(`Task ${taskId} is ready to retry.\n`);
    return 0;
  }

  if (args[0] === "task" && args[1] === "complete" && args.length === 3) {
    const taskId = parseTaskId(args[2]);
    await completeTask(paths, taskId);
    process.stdout.write(`Task ${taskId} completed manually.\n`);
    return 0;
  }

  if (args[0] === "task" && args[1] === "list" && args.length === 2) {
    process.stdout.write(renderQueueStatus(await getQueueStatus(paths)));
    return 0;
  }

  if (args[0] === "queue" && args[1] === "start") {
    return runManualQueueCommand(paths, "start", args.slice(2));
  }

  if (args[0] === "queue" && args[1] === "pause" && args.length === 2) {
    await pauseQueue(paths);
    process.stdout.write("Queue paused.\n");
    return 0;
  }

  if (args[0] === "queue" && args[1] === "resume") {
    return runManualQueueCommand(paths, "resume", args.slice(2));
  }

  if (args[0] === "queue" && args[1] === "status" && args.length === 2) {
    process.stdout.write(renderQueueStatus(await getQueueStatus(paths)));
    return 0;
  }

  if (args[0] === "logs" && args[1] === "read" && args.length === 2) {
    process.stdout.write(await readEventLog(paths.eventLogPath));
    return 0;
  }

  if (args[0] === "logs" && args[1] === "follow" && args.length === 2) {
    await followEventLog(paths.eventLogPath);
    return 0;
  }

  if (args[0] === "config" && args[1] === "show" && args.length === 2) {
    const config = await readConfiguration(paths.configPath);
    process.stdout.write(
      `Access Mode: ${renderAccessMode(config.accessMode)}\n`
      + `Continuation prompt: ${config.continuationPrompt}\n`,
    );
    return 0;
  }

  if (
    args[0] === "config"
    && args[1] === "set"
    && args[2] === "accessMode"
    && (args[3] === "configured" || args[3] === "full")
    && args.length === 4
  ) {
    await setAccessMode(paths, args[3]);
    process.stdout.write(
      `Access Mode updated to ${renderAccessMode(args[3])}.\n`,
    );
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

async function runManualQueueCommand(
  paths: DaemonPaths,
  command: "resume" | "start",
  args: string[],
): Promise<number> {
  const options = parseQueueRunOptions(args, `queue ${command}`);
  const accessMode = (await readConfiguration(paths.configPath)).accessMode;
  await acknowledgeAccessMode(accessMode, options.yes);
  const result = command === "start"
    ? await startQueueRun(paths, options.runPolicy, accessMode)
    : await resumeQueueRun(paths, options.runPolicy, accessMode);
  const message = result === "started"
    ? `Queue ${command === "start" ? "started" : "resumed"}.`
    : result === "already-running"
      ? command === "start"
        ? "Queue already has a running Task."
        : "Queue is already running."
      : result === "paused"
        ? "Queue is paused."
        : "Queue is idle.";
  process.stdout.write(`${message}\n`);
  return 0;
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
      return `Daemon is running (PID ${status.pid}, ${status.codexVersion}).\n`
        + "Next: run `codex-resumer queue status`.";
    case "stopped":
      return "Daemon is stopped.\nNext: run `codex-resumer daemon start`.";
    case "unauthenticated":
      return `Daemon is unauthenticated: ${status.message} Codex: ${status.codexVersion}.\n`
        + "Next: run `codex login`, then `codex-resumer daemon start`.";
    case "incompatible":
      return `Daemon is incompatible: ${status.message} Missing: ${status.missingCapabilities.join(", ")}. Codex: ${status.codexVersion}.\n`
        + "Next: update Codex CLI, then run `codex-resumer daemon start`.";
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
    lines.push(
      `  Access Mode ${renderAccessMode(snapshot.queueRun.accessMode)}`,
    );
    lines.push(`  Started ${snapshot.queueRun.startedAt}`);
    if (snapshot.queueRun.cutoffTime) {
      lines.push(`  Cutoff Time ${snapshot.queueRun.cutoffTime}`);
    }
    if (snapshot.queueRun.endedAt) {
      lines.push(`  Ended ${snapshot.queueRun.endedAt}`);
    }
  }
  if (snapshot.error) {
    lines.push(`Error ${snapshot.error.code}: ${snapshot.error.message}`);
    if (snapshot.error.details !== undefined) {
      lines.push(`  Details ${JSON.stringify(snapshot.error.details)}`);
    }
  }
  for (const task of snapshot.tasks) {
    lines.push(`Task ${task.id}: ${task.state} (${renderTaskState(task.state)})`);
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
  const attentionTask = snapshot.tasks.find((task) => task.state === "needs_attention");
  if (attentionTask) {
    lines.push(
      `Next: run \`codex-resumer task retry ${attentionTask.id} "<new prompt>"\`, `
      + `\`codex-resumer task complete ${attentionTask.id}\`, or `
      + `\`codex-resumer task cancel ${attentionTask.id}\`; then resume the Queue.`,
    );
  } else if (snapshot.state === "paused") {
    lines.push("Next: run `codex-resumer queue resume --until-idle` when ready.");
  } else if (snapshot.state === "idle") {
    lines.push(
      "Next: add a Task with `codex-resumer task add`, then start the Queue.",
    );
  } else {
    lines.push("Next: run `codex-resumer queue pause` to stop starting new Turns.");
  }
  return `${lines.join("\n")}\n`;
}

function renderTaskState(state: QueueSnapshot["tasks"][number]["state"]): string {
  switch (state) {
    case "queued":
      return "Queued Task";
    case "running":
      return "Running Task";
    case "waiting_for_quota":
      return "Quota Pause";
    case "needs_attention":
      return "Needs Attention";
    case "completed":
      return "Completed Task";
    case "cancelled":
      return "Cancelled Task";
  }
}

function renderAccessMode(accessMode: AccessMode): string {
  return accessMode === "configured" ? "Configured Access" : "Full Access";
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

async function readEventLog(eventLogPath: string): Promise<string> {
  try {
    return await readFile(eventLogPath, "utf8");
  } catch (error) {
    if (isRecord(error) && error.code === "ENOENT") return "";
    throw error;
  }
}

async function followEventLog(eventLogPath: string): Promise<void> {
  let offset = 0;
  let stopped = false;
  const stop = (): void => {
    stopped = true;
  };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
  try {
    while (!stopped) {
      const contents = await readEventLog(eventLogPath);
      if (contents.length < offset) offset = 0;
      if (contents.length > offset) {
        process.stdout.write(contents.slice(offset));
        offset = contents.length;
      }
      if (!stopped) await delay(100);
    }
  } finally {
    process.off("SIGINT", stop);
    process.off("SIGTERM", stop);
  }
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
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

function parseQueueRunOptions(
  args: string[],
  command: string,
): { runPolicy: RunPolicy; yes: boolean } {
  const yesCount = args.filter((argument) => argument === "--yes").length;
  if (yesCount > 1) throw new Error(`${command} accepts --yes only once.`);
  return {
    runPolicy: parseRunPolicy(
      args.filter((argument) => argument !== "--yes"),
      command,
    ),
    yes: yesCount === 1,
  };
}

async function acknowledgeAccessMode(
  accessMode: "configured" | "full",
  yes: boolean,
): Promise<void> {
  if (accessMode === "configured") {
    process.stderr.write(
      "Configured Access uses your current Codex permissions; approvals may block unattended Queue Runs.\n",
    );
    return;
  }

  process.stderr.write(
    "WARNING: Full Access allows Codex to modify the system and use the network without approval restrictions.\n",
  );
  if (yes) return;
  if (!process.stdin.isTTY || !process.stderr.isTTY) {
    throw new Error(
      "Full Access requires explicit confirmation; rerun with --yes if you understand and accept the risk.",
    );
  }

  const prompt = createInterface({ input: process.stdin, output: process.stderr });
  try {
    const answer = await prompt.question("Continue with Full Access? [y/N] ");
    if (answer.trim().toLowerCase() !== "y" && answer.trim().toLowerCase() !== "yes") {
      throw new Error("Full Access was not confirmed.");
    }
  } finally {
    prompt.close();
  }
}

void main(process.argv.slice(2))
  .then((exitCode) => {
    process.exitCode = exitCode;
  })
  .catch((error: unknown) => {
    process.stderr.write(`${renderCliError(error)}\n`);
    process.exitCode = 1;
  });

function renderCliError(error: unknown): string {
  const message = errorMessage(error);
  if (message.includes("`") || /rerun with --yes/i.test(message)) return message;
  return `${message}\nNext: run \`codex-resumer --help\` to review the command syntax.`;
}
