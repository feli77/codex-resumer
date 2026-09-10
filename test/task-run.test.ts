import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { appendFile, mkdir, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test, { type TestContext } from "node:test";
import { promisify } from "node:util";

import Database from "better-sqlite3";

import { createFakeCodex } from "./fake-codex.js";

const execFileAsync = promisify(execFile);
const cliPath = path.resolve(new URL("../src/cli.js", import.meta.url).pathname);
type FakeCodexOptions = NonNullable<Parameters<typeof createFakeCodex>[1]>;

async function createCliTestEnvironment(
  t: TestContext,
  fakeCodexOptions: FakeCodexOptions = {},
) {
  const root = await mkdtemp(path.join(tmpdir(), "codex-resumer-task-test-"));
  const workspace = path.join(root, "workspace");
  await mkdir(workspace);
  const fakeCodex = await createFakeCodex(root, fakeCodexOptions);
  const stateDir = path.join(root, "state");
  const env = {
    ...process.env,
    HOME: root,
    PATH: `${root}:${process.env.PATH ?? ""}`,
    FAKE_CODEX_LOG: fakeCodex.logPath,
    FAKE_CODEX_IMPORTED_WORKSPACE: workspace,
    XDG_CONFIG_HOME: path.join(root, "config"),
    XDG_RUNTIME_DIR: path.join(root, "runtime"),
    XDG_STATE_HOME: stateDir,
  };
  const runCli = async (...args: string[]): Promise<string> => {
    const { stdout } = await runCliResult(...args);
    return stdout;
  };
  const runCliResult = (...args: string[]) =>
    execFileAsync(process.execPath, [cliPath, ...args], {
      cwd: root,
      encoding: "utf8",
      env,
      timeout: 10_000,
    });
  const runCliWithInput = (input: string, ...args: string[]): Promise<string> =>
    new Promise((resolve, reject) => {
      const child = spawn(process.execPath, [cliPath, ...args], {
        cwd: root,
        env,
      });
      let stdout = "";
      let stderr = "";
      child.stdout.setEncoding("utf8").on("data", (chunk: string) => {
        stdout += chunk;
      });
      child.stderr.setEncoding("utf8").on("data", (chunk: string) => {
        stderr += chunk;
      });
      child.once("error", reject);
      child.once("close", (code) => {
        if (code === 0) resolve(stdout);
        else reject(new Error(stderr));
      });
      child.stdin.end(input);
    });
  t.after(async () => {
    await runCli("daemon", "stop", "--force").catch(async () => {
      await runCli("daemon", "stop").catch(() => undefined);
    });
    await rm(root, { recursive: true, force: true });
  });
  return {
    env,
    fakeCodex,
    root,
    runCli,
    runCliResult,
    runCliWithInput,
    stateDir,
    workspace,
  };
}

test("manual Queue Runs disclose and acknowledge their Access Mode", async (t) => {
  const { runCli, runCliResult, workspace } = await createCliTestEnvironment(t, {
    completeTurnSynchronously: true,
  });
  await runCli("daemon", "start");
  await runCli("task", "add", "--workspace", workspace, "Configured work");

  const configured = await runCliResult("queue", "start", "--until-idle");
  assert.match(configured.stderr, /Configured Access/);
  assert.match(configured.stderr, /approvals may block unattended Queue Runs/i);

  await runCli("config", "set", "accessMode", "full");
  await runCli("task", "add", "--workspace", workspace, "Full Access work");
  await assert.rejects(
    runCliResult("queue", "resume", "--until-idle"),
    (error: unknown) => {
      assert.ok(error instanceof Error && "stderr" in error);
      assert.match(String(error.stderr), /Full Access allows Codex to modify the system/i);
      assert.match(String(error.stderr), /rerun with --yes/i);
      return true;
    },
  );

  const full = await runCliResult("queue", "resume", "--until-idle", "--yes");
  assert.equal(full.stdout, "Queue resumed.\n");
  assert.match(full.stderr, /Full Access allows Codex to modify the system/i);
});

test("Configured Access clears sticky Full Access on a Managed Thread", async (t) => {
  const { fakeCodex, runCli, workspace } = await createCliTestEnvironment(t, {
    codexApprovalPolicy: null,
    codexSandboxMode: null,
    codexSandboxWorkspaceWrite: null,
    completeTurnSynchronously: true,
  });
  await runCli("daemon", "start");
  await runCli("config", "set", "accessMode", "full");
  await runCli("task", "add", "--workspace", workspace, "Elevated work");
  await runCli("queue", "start", "--until-idle", "--yes");

  await runCli("config", "set", "accessMode", "configured");
  await runCli("task", "add", "--thread", "thread-fake", "Configured work");
  await runCli("queue", "resume", "--until-idle");

  const messages = (await readFile(fakeCodex.logPath, "utf8"))
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line) as {
      message?: { method?: string; params?: Record<string, unknown> };
    })
    .map((record) => record.message)
    .filter((message) => message?.method === "turn/start");
  assert.deepEqual(messages.map((message) => ({
    approvalPolicy: message?.params?.approvalPolicy,
    sandboxPolicy: message?.params?.sandboxPolicy,
  })), [
    {
      approvalPolicy: "never",
      sandboxPolicy: { type: "dangerFullAccess" },
    },
    {
      approvalPolicy: null,
      sandboxPolicy: null,
    },
  ]);
});

test("Configured Access explicitly disables network access for read-only sandboxes", async (t) => {
  const { fakeCodex, runCli, workspace } = await createCliTestEnvironment(t, {
    codexSandboxMode: "read-only",
    completeTurnSynchronously: true,
  });
  await runCli("daemon", "start");
  await runCli("task", "add", "--workspace", workspace, "Read-only work");
  await runCli("queue", "start", "--until-idle");

  const turnStart = (await readFile(fakeCodex.logPath, "utf8"))
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line) as {
      message?: { method?: string; params?: Record<string, unknown> };
    })
    .map((record) => record.message)
    .find((message) => message?.method === "turn/start");
  assert.deepEqual(turnStart?.params?.sandboxPolicy, {
    networkAccess: false,
    type: "readOnly",
  });
});

test("a Workspace Task runs once and completes without an output marker", async (t) => {
  const { fakeCodex, root, runCli, stateDir, workspace } = await createCliTestEnvironment(t, {
    completeTurnSynchronously: true,
  });

  assert.match(await runCli("daemon", "start"), /Daemon started/);
  assert.equal(
    await runCli("task", "add", "--workspace", "workspace", "Create a note"),
    "Task 1 added.\n",
  );
  assert.equal(await runCli("queue", "start", "--until-idle"), "Queue started.\n");

  let status = "";
  for (let attempt = 0; attempt < 100; attempt += 1) {
    status = await runCli("queue", "status");
    if (status.includes("Task 1: completed")) break;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  assert.match(status, /Queue is idle/);
  assert.match(status, /Queue Run 1: Until Idle/);
  assert.match(status, /Task 1: completed/);
  assert.match(status, /Thread thread-fake, Turn turn-fake/);
  assert.match(status, /Thread state idle/);
  assert.doesNotMatch(status, /Create a note/);
  assert.match(
    status,
    /Next: run `codex-resumer task add --workspace \. "<prompt>"`, then `codex-resumer queue start --until-idle`\./,
  );

  const records = (await readFile(fakeCodex.logPath, "utf8"))
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line) as {
      type: string;
      message?: { method?: string; params?: Record<string, unknown> };
    });
  const requests = records
    .filter((record) => record.type === "message")
    .map((record) => record.message)
    .filter((message) =>
      message?.method === "thread/start"
      || message?.method === "config/read"
      || message?.method === "turn/start"
    );
  assert.deepEqual(requests, [
    { method: "thread/start", id: 4, params: { cwd: workspace } },
    {
      method: "config/read",
      id: 5,
      params: { cwd: workspace, includeLayers: false },
    },
    {
      method: "turn/start",
      id: 6,
      params: {
        approvalPolicy: "on-request",
        threadId: "thread-fake",
        input: [{ type: "text", text: "Create a note" }],
        sandboxPolicy: {
          excludeSlashTmp: true,
          excludeTmpdirEnvVar: false,
          networkAccess: true,
          type: "workspaceWrite",
          writableRoots: [root],
        },
      },
    },
  ]);

  const database = new Database(
    path.join(stateDir, "codex-resumer", "state.sqlite3"),
    { readonly: true },
  );
  t.after(() => database.close());
  assert.deepEqual(
    database.prepare(`
      SELECT id, workspace, state, prompt, managed_thread_id, active_turn_id
      FROM tasks
    `).get(),
    {
      id: 1,
      workspace,
      state: "completed",
      prompt: null,
      managed_thread_id: "thread-fake",
      active_turn_id: "turn-fake",
    },
  );
  assert.deepEqual(
    database.prepare("SELECT id, task_id, state FROM turns").get(),
    { id: "turn-fake", task_id: 1, state: "completed" },
  );
  assert.deepEqual(
    database.prepare("SELECT id, workspace, state FROM managed_threads").get(),
    { id: "thread-fake", workspace, state: "idle" },
  );
  assert.deepEqual(database.prepare("SELECT state FROM queue").get(), {
    state: "running",
  });
});

test("event logs audit Task state without retaining terminal prompts", async (t) => {
  const { runCli, stateDir, workspace } = await createCliTestEnvironment(t, {
    completeTurnSynchronously: true,
  });
  const completedPrompt = "private completed prompt 7f6f4b";
  const cancelledPrompt = "private cancelled prompt 60d024";

  await runCli("daemon", "start");
  await runCli("task", "add", "--workspace", workspace, completedPrompt);
  await runCli("queue", "start", "--until-idle");
  await waitForQueueStatus(runCli, "Task 1: completed");
  await runCli("task", "add", "--workspace", workspace, cancelledPrompt);
  await runCli("task", "cancel", "2");

  const logContents = await runCli("logs", "read");
  const events = logContents.trim().split("\n").map((line) => JSON.parse(line) as {
    eventType: string;
    stateTransition?: { from: string | null; to: string };
    taskId?: number;
    threadId?: string;
    timestamp: string;
    turnId?: string;
  });
  assert.ok(events.every((event) => !Number.isNaN(Date.parse(event.timestamp))));
  assert.deepEqual(
    events
      .filter((event) => event.eventType === "task.state_changed")
      .map((event) => ({
        stateTransition: event.stateTransition,
        taskId: event.taskId,
        threadId: event.threadId,
        turnId: event.turnId,
      })),
    [
      {
        stateTransition: { from: null, to: "queued" },
        taskId: 1,
        threadId: undefined,
        turnId: undefined,
      },
      {
        stateTransition: { from: "queued", to: "running" },
        taskId: 1,
        threadId: undefined,
        turnId: undefined,
      },
      {
        stateTransition: { from: "running", to: "completed" },
        taskId: 1,
        threadId: "thread-fake",
        turnId: "turn-fake",
      },
      {
        stateTransition: { from: null, to: "queued" },
        taskId: 2,
        threadId: undefined,
        turnId: undefined,
      },
      {
        stateTransition: { from: "queued", to: "cancelled" },
        taskId: 2,
        threadId: undefined,
        turnId: undefined,
      },
    ],
  );
  assert.doesNotMatch(logContents, /private completed prompt|private cancelled prompt/);

  const applicationStateDir = path.join(stateDir, "codex-resumer");
  assert.equal(
    (await stat(path.join(applicationStateDir, "events.jsonl"))).mode & 0o777,
    0o600,
  );
  assert.equal(
    (await stat(path.join(applicationStateDir, "state.sqlite3"))).mode & 0o777,
    0o600,
  );
  const database = new Database(path.join(applicationStateDir, "state.sqlite3"), {
    readonly: true,
  });
  t.after(() => database.close());
  assert.deepEqual(
    database.prepare("SELECT id, state, prompt FROM tasks ORDER BY id").all(),
    [
      { id: 1, state: "completed", prompt: null },
      { id: 2, state: "cancelled", prompt: null },
    ],
  );
});

test("logs follow streams existing and newly appended events", async (t) => {
  const { env, root, runCli, stateDir } = await createCliTestEnvironment(t);
  await runCli("daemon", "start");
  const child = spawn(process.execPath, [cliPath, "logs", "follow"], {
    cwd: root,
    env,
  });
  let stdout = "";
  let stderr = "";
  child.stdout.setEncoding("utf8").on("data", (chunk: string) => {
    stdout += chunk;
  });
  child.stderr.setEncoding("utf8").on("data", (chunk: string) => {
    stderr += chunk;
  });
  t.after(() => {
    if (child.exitCode === null) child.kill("SIGTERM");
  });

  const appended = JSON.stringify({
    eventType: "test.appended",
    timestamp: "2026-09-10T00:00:00.000Z",
  });
  await appendFile(
    path.join(stateDir, "codex-resumer", "events.jsonl"),
    `${appended}\n`,
  );
  for (let attempt = 0; attempt < 100 && !stdout.includes(appended); attempt += 1) {
    if (child.exitCode !== null) break;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }

  assert.equal(child.exitCode, null, stderr);
  assert.match(stdout, /"eventType":"queue.state_changed"/);
  assert.match(stdout, /"eventType":"test.appended"/);
  child.kill("SIGTERM");
});

test("a Thread is explicitly imported with its App Server Workspace", async (t) => {
  const { runCli, workspace } = await createCliTestEnvironment(t);

  await runCli("daemon", "start");
  assert.equal(
    await runCli("thread", "import", "thread-imported"),
    `Thread thread-imported imported for Workspace ${workspace}.\n`,
  );
});

test("thread import rejects a Thread the App Server cannot read", async (t) => {
  const { runCli } = await createCliTestEnvironment(t);

  await runCli("daemon", "start");
  await assert.rejects(
    runCli("thread", "import", "thread-missing"),
    (error: unknown) => {
      assert.ok(error instanceof Error && "stderr" in error);
      assert.match(String(error.stderr), /Thread does not exist/);
      return true;
    },
  );
});

test("a Managed Thread cannot be rebound to another Workspace", async (t) => {
  const { env, root, runCli } = await createCliTestEnvironment(t);
  const otherWorkspace = path.join(root, "other-workspace");
  await mkdir(otherWorkspace);

  await runCli("daemon", "start");
  await runCli("thread", "import", "thread-imported");
  await runCli("daemon", "stop");
  env.FAKE_CODEX_IMPORTED_WORKSPACE = otherWorkspace;
  await runCli("daemon", "start");

  await assert.rejects(
    runCli("thread", "import", "thread-imported"),
    (error: unknown) => {
      assert.ok(error instanceof Error && "stderr" in error);
      assert.match(String(error.stderr), /already bound to Workspace/);
      return true;
    },
  );
});

test("a Task targeting a Managed Thread resumes that Thread", async (t) => {
  const { fakeCodex, runCli } = await createCliTestEnvironment(t, {
    completeTurnSynchronously: true,
  });

  await runCli("daemon", "start");
  await runCli("thread", "import", "thread-imported");
  assert.equal(
    await runCli("task", "add", "--thread", "thread-imported", "Continue work"),
    "Task 1 added.\n",
  );
  assert.equal(await runCli("queue", "start", "--until-idle"), "Queue started.\n");

  const status = await runCli("queue", "status");
  assert.match(status, /Task 1: completed/);
  assert.match(status, /Thread thread-imported, Turn turn-fake/);

  const methods = (await readFakeMessages(fakeCodex.logPath)).map(
    (message) => message.method,
  );
  assert.deepEqual(
    methods.filter((method) => method?.startsWith("thread/")),
    ["thread/read", "thread/resume"],
  );
});

test("task add accepts a multiline prompt from stdin", async (t) => {
  const { fakeCodex, runCli, runCliWithInput, workspace } =
    await createCliTestEnvironment(t, { completeTurnSynchronously: true });

  await runCli("daemon", "start");
  assert.equal(
    await runCliWithInput(
      "First line\nSecond line\n",
      "task",
      "add",
      "--workspace",
      workspace,
    ),
    "Task 1 added.\n",
  );
  await runCli("queue", "start", "--until-idle");

  const turnStart = (await readFakeMessages(fakeCodex.logPath)).find(
    (message) => message.method === "turn/start",
  );
  assert.deepEqual(turnStart?.params?.input, [{
    type: "text",
    text: "First line\nSecond line\n",
  }]);
});

test("task add rejects more than one target", async (t) => {
  const { runCli, workspace } = await createCliTestEnvironment(t);

  await runCli("daemon", "start");
  await runCli("thread", "import", "thread-imported");
  await assert.rejects(
    runCli(
      "task",
      "add",
      "--workspace",
      workspace,
      "--thread",
      "thread-imported",
      "Do work",
    ),
    taskTargetError,
  );
});

test("Tasks run FIFO across Managed Threads and new Workspace Threads", async (t) => {
  const { fakeCodex, runCli, workspace } = await createCliTestEnvironment(t, {
    completeTurnSynchronously: true,
  });

  await runCli("daemon", "start");
  await runCli("thread", "import", "thread-imported");
  await runCli("task", "add", "--workspace", workspace, "First");
  await runCli("task", "add", "--thread", "thread-imported", "Second");
  await runCli("task", "add", "--workspace", workspace, "Third");
  assert.equal(await runCli("queue", "start", "--until-idle"), "Queue started.\n");

  const status = await waitForQueueStatus(runCli, "Task 3: completed");
  assert.match(status, /Task 1: completed[\s\S]*Task 2: completed[\s\S]*Task 3: completed/);

  const messages = await readFakeMessages(fakeCodex.logPath);
  assert.deepEqual(
    messages
      .filter((message) => message.method === "turn/start")
      .map((message) => {
        const input = message.params?.input as Array<{ text?: string }> | undefined;
        return input?.[0]?.text;
      }),
    ["First", "Second", "Third"],
  );
  assert.deepEqual(
    messages
      .map((message) => message.method)
      .filter((method) => method?.startsWith("thread/")),
    ["thread/read", "thread/start", "thread/resume", "thread/start"],
  );
});

test("queued Tasks can be added, moved, and cancelled while a Task is active", async (t) => {
  const { fakeCodex, runCli, stateDir, workspace } = await createCliTestEnvironment(t, {
    turnCompletionDelayMs: 500,
  });

  await runCli("daemon", "start");
  await runCli("task", "add", "--workspace", workspace, "Active secret");
  await runCli("task", "add", "--workspace", workspace, "Second secret");
  await runCli("queue", "start", "--until-idle");
  await runCli("task", "add", "--workspace", workspace, "Cancel secret");
  await runCli("task", "add", "--workspace", workspace, "Move secret");
  assert.equal(await runCli("task", "move", "4", "--before", "2"), "Task 4 moved.\n");
  assert.equal(await runCli("task", "cancel", "3"), "Task 3 cancelled.\n");

  const list = await runCli("task", "list");
  assert.match(
    list,
    /Task 1: running[\s\S]*Task 4: queued[\s\S]*Task 2: queued[\s\S]*Task 3: cancelled/,
  );
  assert.match(list, /Task 1: running[\s\S]*Thread thread-fake, Turn turn-fake/);
  assert.match(list, /Task 1: running[\s\S]*Thread state active/);
  assert.doesNotMatch(list, /Active secret|Second secret|Cancel secret|Move secret/);

  const database = new Database(
    path.join(stateDir, "codex-resumer", "state.sqlite3"),
    { readonly: true },
  );
  t.after(() => database.close());
  assert.deepEqual(
    database.prepare("SELECT state, prompt FROM tasks WHERE id = 3").get(),
    { state: "cancelled", prompt: null },
  );

  const completed = await waitForQueueStatus(runCli, "Task 2: completed");
  assert.match(completed, /Queue is idle/);
  assert.deepEqual(
    (await readFakeMessages(fakeCodex.logPath))
      .filter((message) => message.method === "turn/start")
      .map((message) => {
        const input = message.params?.input as Array<{ text?: string }> | undefined;
        return input?.[0]?.text;
      }),
    ["Active secret", "Move secret", "Second secret"],
  );
});

test("a queued Task cannot move before the active Task", async (t) => {
  const { fakeCodex, runCli, workspace } = await createCliTestEnvironment(t, {
    completeTurn: false,
  });

  await runCli("daemon", "start");
  await runCli("task", "add", "--workspace", workspace, "Active");
  await runCli("task", "add", "--workspace", workspace, "Queued");
  await runCli("queue", "start", "--until-idle");

  await assert.rejects(
    runCli("task", "move", "2", "--before", "1"),
    (error: unknown) => {
      assert.ok(error instanceof Error && "stderr" in error);
      assert.match(String(error.stderr), /cannot move before the active Task/);
      return true;
    },
  );
  assert.match(
    await runCli("task", "list"),
    /Task 1: running[\s\S]*Task 2: queued/,
  );
  const methods = (await readFakeMessages(fakeCodex.logPath)).map(
    (message) => message.method,
  );
  assert.equal(methods.filter((method) => method === "turn/start").length, 1);
  assert.equal(methods.includes("turn/interrupt"), false);
});

test("issue 3 Queue state is migrated without losing Task IDs", async (t) => {
  const { runCli, stateDir, workspace } = await createCliTestEnvironment(t);
  await createIssue3Database(stateDir, workspace);

  await runCli("daemon", "start");
  assert.match(await runCli("task", "list"), /Task 1: queued/);
  assert.equal(await runCli("task", "cancel", "1"), "Task 1 cancelled.\n");
  assert.equal(
    await runCli("task", "add", "--workspace", workspace, "New Task"),
    "Task 2 added.\n",
  );
});

test("task add rejects a missing Workspace before a Turn can start", async (t) => {
  const { fakeCodex, root, runCli } = await createCliTestEnvironment(t);

  await runCli("daemon", "start");
  await assert.rejects(
    runCli("task", "add", "--workspace", path.join(root, "missing"), "Do work"),
    workspaceError,
  );
  assert.doesNotMatch(await readFile(fakeCodex.logPath, "utf8"), /"method":"turn\/start"/);
});

test("queue start rechecks a Workspace that became unavailable", async (t) => {
  const { fakeCodex, runCli, workspace } = await createCliTestEnvironment(t);

  await runCli("daemon", "start");
  await runCli("task", "add", "--workspace", workspace, "Do work");
  await rm(workspace, { recursive: true });
  await assert.rejects(runCli("queue", "start", "--until-idle"), workspaceError);
  assert.doesNotMatch(await readFile(fakeCodex.logPath, "utf8"), /"method":"turn\/start"/);
});

test("Queue start, pause, and resume never duplicate the active Turn", async (t) => {
  const { fakeCodex, runCli, workspace } = await createCliTestEnvironment(t, {
    completeTurn: false,
  });

  await runCli("daemon", "start");
  await runCli("task", "add", "--workspace", workspace, "Keep working");
  assert.equal(await runCli("queue", "start", "--until-idle"), "Queue started.\n");
  assert.equal(
    await runCli("queue", "start", "--until-idle"),
    "Queue already has a running Task.\n",
  );
  assert.match(await runCli("queue", "status"), /Task 1: running/);
  assert.equal(await runCli("queue", "pause"), "Queue paused.\n");
  assert.match(
    await runCli("queue", "status"),
    /Queue is paused\.[\s\S]*Pause reason: manual pause/,
  );
  const manualPause = (await runCli("logs", "read"))
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line) as Record<string, unknown>)
    .filter((event) => {
      const transition = event.stateTransition as Record<string, unknown> | undefined;
      return event.eventType === "queue.state_changed" && transition?.to === "paused";
    })
    .at(-1);
  assert.deepEqual(
    {
      taskId: manualPause?.taskId,
      threadId: manualPause?.threadId,
      turnId: manualPause?.turnId,
    },
    { taskId: 1, threadId: "thread-fake", turnId: "turn-fake" },
  );
  assert.equal(
    await runCli("queue", "resume", "--until-idle"),
    "Queue resumed.\n",
  );
  assert.match(await runCli("queue", "status"), /Queue Run 2: Until Idle/);

  const records = (await readFile(fakeCodex.logPath, "utf8"))
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line) as { message?: { method?: string } });
  assert.equal(
    records.filter((record) => record.message?.method === "turn/start").length,
    1,
  );
});

test("normal daemon stop pauses the Queue until an explicit resume", async (t) => {
  const { fakeCodex, runCli, workspace } = await createCliTestEnvironment(t, {
    completeTurnSynchronously: true,
  });

  await runCli("daemon", "start");
  await runCli("task", "add", "--workspace", workspace, "Finish before stop");
  await runCli("queue", "start", "--until-idle");
  await waitForQueueStatus(runCli, "Task 1: completed");
  await runCli("task", "add", "--workspace", workspace, "Wait for resume");

  assert.equal(await runCli("daemon", "stop"), "Daemon stopped.\n");
  assert.equal(await runCli("daemon", "start"), "Daemon started.\n");
  const status = await runCli("queue", "status");
  assert.match(status, /Queue is paused\.[\s\S]*Pause reason: manual pause/);
  assert.match(status, /Task 2: queued/);
  assert.equal(
    (await readFakeMessages(fakeCodex.logPath))
      .filter((message) => message.method === "turn/start").length,
    1,
  );
});

test("normal daemon stop refuses while a Turn is active", async (t) => {
  const { fakeCodex, runCli, runCliResult, workspace } =
    await createCliTestEnvironment(t, { completeTurn: false });

  await runCli("daemon", "start");
  await runCli("task", "add", "--workspace", workspace, "Keep running");
  await runCli("queue", "start", "--until-idle");

  await assert.rejects(
    runCliResult("daemon", "stop"),
    (error: unknown) => {
      assert.ok(error instanceof Error && "stderr" in error);
      assert.match(String(error.stderr), /active Turn is still running/i);
      assert.match(String(error.stderr), /`codex-resumer daemon stop --force`/i);
      return true;
    },
  );
  assert.match(await runCli("daemon", "status"), /Daemon is running/);
  assert.equal(
    (await readFakeMessages(fakeCodex.logPath))
      .filter((message) => message.method === "turn/interrupt").length,
    0,
  );
});

test("daemon stop --force interrupts the active Turn and pauses before exiting", async (t) => {
  const { fakeCodex, runCli, workspace } = await createCliTestEnvironment(t, {
    completeTurn: false,
  });

  await runCli("daemon", "start");
  await runCli("task", "add", "--workspace", workspace, "Interrupt this");
  await runCli("task", "add", "--workspace", workspace, "Do not start");
  await runCli("queue", "start", "--until-idle");

  assert.equal(await runCli("daemon", "stop", "--force"), "Daemon stopped.\n");
  const messagesAfterStop = await readFakeMessages(fakeCodex.logPath);
  assert.deepEqual(
    messagesAfterStop
      .filter((message) => message.method === "turn/interrupt")
      .map((message) => message.params),
    [{ threadId: "thread-fake", turnId: "turn-fake" }],
  );

  await runCli("daemon", "start");
  const status = await runCli("queue", "status");
  assert.match(status, /Queue is paused\.[\s\S]*Pause reason: Needs Attention/);
  assert.match(status, /Task 1: needs_attention[\s\S]*Task 2: queued/);
  assert.equal(
    (await readFakeMessages(fakeCodex.logPath))
      .filter((message) => message.method === "turn/start").length,
    1,
  );
});

test("a non-quota Turn failure pauses the Queue as Needs Attention", async (t) => {
  const { fakeCodex, runCli, workspace } = await createCliTestEnvironment(t, {
    turnErrors: [{
      codexErrorInfo: "unauthorized",
      message: "login expired; command output SECRET_OUTPUT_7218",
    }],
  });

  await runCli("daemon", "start");
  await runCli("task", "add", "--workspace", workspace, "First");
  await runCli("task", "add", "--workspace", workspace, "Do not start");
  await runCli("queue", "start", "--until-idle");

  const status = await waitForQueueStatus(runCli, "Task 1: needs_attention");
  assert.match(status, /Queue is paused\.[\s\S]*Pause reason: Needs Attention/);
  assert.match(status, /Error turn_failed: login expired/);
  assert.match(status, /Task 1: needs_attention \(Needs Attention\)/);
  assert.match(status, /Next: run `codex-resumer task retry 1/);
  const events = await runCli("logs", "read");
  assert.doesNotMatch(events, /SECRET_OUTPUT_7218/);
  assert.match(
    events,
    /"errorSummary":\{"code":"turn_failed","message":"An operational error was recorded/,
  );
  assert.equal(
    (await readFakeMessages(fakeCodex.logPath))
      .filter((message) => message.method === "turn/start").length,
    1,
  );
});

test("task retry requires a new prompt and waits for explicit Queue resume", async (t) => {
  const { fakeCodex, runCli, runCliWithInput, workspace } =
    await createCliTestEnvironment(t, {
      completeTurnSynchronously: true,
      turnErrors: [{ codexErrorInfo: "badRequest", message: "fix the request" }],
    });

  await runCli("daemon", "start");
  await runCli("task", "add", "--workspace", workspace, "Old prompt");
  await runCli("queue", "start", "--until-idle");
  await waitForQueueStatus(runCli, "Task 1: needs_attention");

  await assert.rejects(
    runCliWithInput("", "task", "retry", "1"),
    (error: unknown) => {
      assert.ok(error instanceof Error);
      assert.match(error.message, /new retry prompt must not be empty/i);
      return true;
    },
  );
  assert.equal(
    await runCli("task", "retry", "1", "Use", "the", "corrected", "input"),
    "Task 1 is ready to retry.\n",
  );

  let status = await runCli("queue", "status");
  assert.match(status, /Queue is paused\.[\s\S]*Task 1: queued/);
  assert.equal(
    (await readFakeMessages(fakeCodex.logPath))
      .filter((message) => message.method === "turn/start").length,
    1,
  );

  assert.equal(await runCli("queue", "resume", "--until-idle"), "Queue resumed.\n");
  status = await waitForQueueStatus(runCli, "Task 1: completed");
  assert.match(status, /Queue is idle/);
  assert.deepEqual(
    (await readFakeMessages(fakeCodex.logPath))
      .filter((message) => message.method === "turn/start")
      .map((message) => {
        const input = message.params?.input as Array<{ text?: string }> | undefined;
        return input?.[0]?.text;
      }),
    ["Old prompt", "Use the corrected input"],
  );
  await assert.rejects(
    runCli("task", "retry", "1", "Try again"),
    (error: unknown) => {
      assert.ok(error instanceof Error);
      assert.match(error.message, /does not need attention/i);
      return true;
    },
  );
});

test("task complete resolves Needs Attention without resuming the Queue", async (t) => {
  const { fakeCodex, runCli, stateDir, workspace } =
    await createCliTestEnvironment(t, {
      turnErrors: [{ codexErrorInfo: "other", message: "inspect manually" }],
    });

  await runCli("daemon", "start");
  await runCli("task", "add", "--workspace", workspace, "Sensitive prompt");
  await runCli("task", "add", "--workspace", workspace, "Later work");
  await runCli("queue", "start", "--until-idle");
  await waitForQueueStatus(runCli, "Task 1: needs_attention");

  assert.equal(await runCli("task", "complete", "1"), "Task 1 completed manually.\n");
  const status = await runCli("queue", "status");
  assert.match(status, /Queue is paused\.[\s\S]*Task 1: completed[\s\S]*Task 2: queued/);
  assert.equal(
    (await readFakeMessages(fakeCodex.logPath))
      .filter((message) => message.method === "turn/start").length,
    1,
  );

  const database = new Database(
    path.join(stateDir, "codex-resumer", "state.sqlite3"),
    { readonly: true },
  );
  t.after(() => database.close());
  assert.deepEqual(
    database.prepare("SELECT state, prompt FROM tasks WHERE id = 1").get(),
    { state: "completed", prompt: null },
  );
});

test("cancelling an active Task interrupts its Turn and pauses the Queue", async (t) => {
  const { fakeCodex, runCli, stateDir, workspace } =
    await createCliTestEnvironment(t, { completeTurn: false });

  await runCli("daemon", "start");
  await runCli("task", "add", "--workspace", workspace, "Active secret");
  await runCli("task", "add", "--workspace", workspace, "Later secret");
  await runCli("queue", "start", "--until-idle");

  assert.equal(await runCli("task", "cancel", "1"), "Task 1 cancelled.\n");
  const status = await runCli("queue", "status");
  assert.match(status, /Queue is paused\.[\s\S]*Task 1: cancelled[\s\S]*Task 2: queued/);
  assert.doesNotMatch(status, /Active secret|Later secret/);

  const messages = await readFakeMessages(fakeCodex.logPath);
  assert.deepEqual(
    messages
      .filter((message) => message.method === "turn/interrupt")
      .map((message) => message.params),
    [{ threadId: "thread-fake", turnId: "turn-fake" }],
  );
  assert.equal(
    messages.filter((message) => message.method === "turn/start").length,
    1,
  );

  const database = new Database(
    path.join(stateDir, "codex-resumer", "state.sqlite3"),
    { readonly: true },
  );
  t.after(() => database.close());
  assert.deepEqual(
    database.prepare("SELECT state, prompt FROM tasks WHERE id = 1").get(),
    { state: "cancelled", prompt: null },
  );
});

test("task cancel resolves Needs Attention and leaves Workspace changes intact", async (t) => {
  const { runCli, stateDir, workspace } = await createCliTestEnvironment(t, {
    turnErrors: [{ codexErrorInfo: "other", message: "review partial work" }],
  });
  const partialWork = path.join(workspace, "partial.txt");
  await writeFile(partialWork, "keep this\n");

  await runCli("daemon", "start");
  await runCli("task", "add", "--workspace", workspace, "Sensitive prompt");
  await runCli("queue", "start", "--until-idle");
  await waitForQueueStatus(runCli, "Task 1: needs_attention");

  assert.equal(await runCli("task", "cancel", "1"), "Task 1 cancelled.\n");
  assert.match(await runCli("queue", "status"), /Queue is paused\.[\s\S]*Task 1: cancelled/);
  assert.equal(await readFile(partialWork, "utf8"), "keep this\n");

  const database = new Database(
    path.join(stateDir, "codex-resumer", "state.sqlite3"),
    { readonly: true },
  );
  t.after(() => database.close());
  assert.deepEqual(
    database.prepare("SELECT state, prompt FROM tasks WHERE id = 1").get(),
    { state: "cancelled", prompt: null },
  );
});

test("an unattended user-input request becomes Needs Attention", async (t) => {
  const { fakeCodex, runCli, workspace } = await createCliTestEnvironment(t, {
    completeTurn: false,
    unattendedRequestMethods: ["item/tool/requestUserInput"],
  });

  await runCli("daemon", "start");
  await runCli("task", "add", "--workspace", workspace, "Ask if blocked");
  await runCli("task", "add", "--workspace", workspace, "Do not start");
  await runCli("queue", "start", "--until-idle");

  const status = await waitForQueueStatus(runCli, "Task 1: needs_attention");
  assert.match(status, /Queue is paused\.[\s\S]*Pause reason: Needs Attention/);
  assert.match(status, /Error unattended_request: Codex requested user input/);
  assert.match(status, /Task 1: needs_attention[\s\S]*Task 2: queued/);
  assert.equal(
    (await readFakeMessages(fakeCodex.logPath))
      .filter((message) => message.method === "turn/start").length,
    1,
  );
});

test("an App Server process failure becomes Needs Attention", async (t) => {
  const { fakeCodex, runCli, workspace } = await createCliTestEnvironment(t, {
    completeTurn: false,
    exitAfterTurnStart: true,
  });

  await runCli("daemon", "start");
  await runCli("task", "add", "--workspace", workspace, "Active work");
  await runCli("task", "add", "--workspace", workspace, "Do not start");
  await runCli("queue", "start", "--until-idle");

  const status = await waitForQueueStatus(runCli, "Task 1: needs_attention");
  assert.match(status, /Queue is paused\.[\s\S]*Pause reason: Needs Attention/);
  assert.match(status, /Error app_server_process_exit:/);
  assert.match(status, /Task 1: needs_attention[\s\S]*Task 2: queued/);
  assert.equal(
    (await readFakeMessages(fakeCodex.logPath))
      .filter((message) => message.method === "turn/start").length,
    1,
  );
});

test("external Managed Thread activity pauses without interrupting the owned Turn", async (t) => {
  const { fakeCodex, runCli, workspace } = await createCliTestEnvironment(t, {
    completeTurn: false,
    externalTurnStarted: true,
  });

  await runCli("daemon", "start");
  await runCli("task", "add", "--workspace", workspace, "Owned work");
  await runCli("task", "add", "--workspace", workspace, "Do not start");
  await runCli("queue", "start", "--until-idle");

  const status = await waitForQueueStatus(runCli, "Error external_thread_activity:");
  assert.match(status, /Queue is paused\.[\s\S]*Pause reason: Needs Attention/);
  assert.match(status, /Task 1: running[\s\S]*Task 2: queued/);
  const messages = await readFakeMessages(fakeCodex.logPath);
  assert.equal(
    messages.filter((message) => message.method === "turn/interrupt").length,
    0,
  );
  assert.equal(
    messages.filter((message) => message.method === "turn/start").length,
    1,
  );
  const externalActivity = (await runCli("logs", "read"))
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line) as Record<string, unknown>)
    .find((event) => event.eventType === "thread.external_activity");
  assert.deepEqual(
    {
      errorSummary: externalActivity?.errorSummary,
      taskId: externalActivity?.taskId,
      threadId: externalActivity?.threadId,
      turnId: externalActivity?.turnId,
    },
    {
      errorSummary: {
        code: "external_thread_activity",
        message: "An operational error was recorded; inspect Queue status for details.",
      },
      taskId: 1,
      threadId: "thread-fake",
      turnId: "turn-external",
    },
  );
});

test("Queue resume cannot skip an unresolved Needs Attention Task", async (t) => {
  const { fakeCodex, runCli, workspace } = await createCliTestEnvironment(t, {
    turnErrors: [{ codexErrorInfo: "badRequest", message: "needs correction" }],
  });

  await runCli("daemon", "start");
  await runCli("task", "add", "--workspace", workspace, "First");
  await runCli("task", "add", "--workspace", workspace, "Second");
  await runCli("queue", "start", "--until-idle");
  await waitForQueueStatus(runCli, "Task 1: needs_attention");

  await assert.rejects(
    runCli("queue", "resume", "--until-idle"),
    (error: unknown) => {
      assert.ok(error instanceof Error);
      assert.match(error.message, /resolve Task 1 before resuming the Queue/i);
      return true;
    },
  );
  assert.equal(
    (await readFakeMessages(fakeCodex.logPath))
      .filter((message) => message.method === "turn/start").length,
    1,
  );
});

function workspaceError(error: unknown): boolean {
  assert.ok(error instanceof Error && "stderr" in error);
  assert.match(String(error.stderr), /Workspace is not an accessible directory/);
  return true;
}

function taskTargetError(error: unknown): boolean {
  assert.ok(error instanceof Error && "stderr" in error);
  assert.match(String(error.stderr), /exactly one Managed Thread or Workspace/);
  return true;
}

async function readFakeMessages(
  logPath: string,
): Promise<Array<{ method?: string; params?: Record<string, unknown> }>> {
  return (await readFile(logPath, "utf8"))
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line) as {
      type: string;
      message?: { method?: string; params?: Record<string, unknown> };
    })
    .filter((record) => record.type === "message")
    .map((record) => record.message ?? {});
}

async function waitForQueueStatus(
  runCli: (...args: string[]) => Promise<string>,
  expected: string,
): Promise<string> {
  let status = "";
  for (let attempt = 0; attempt < 100; attempt += 1) {
    status = await runCli("queue", "status");
    if (status.includes(expected)) return status;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  return status;
}

async function createIssue3Database(stateDir: string, workspace: string): Promise<void> {
  const applicationStateDir = path.join(stateDir, "codex-resumer");
  await mkdir(applicationStateDir, { recursive: true });
  const database = new Database(path.join(applicationStateDir, "state.sqlite3"));
  try {
    database.exec(`
      CREATE TABLE queue (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        state TEXT NOT NULL CHECK (state IN ('paused', 'running')),
        updated_at TEXT NOT NULL
      );
      CREATE TABLE tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        workspace TEXT NOT NULL,
        prompt TEXT,
        state TEXT NOT NULL CHECK (state IN ('queued', 'running', 'completed')),
        managed_thread_id TEXT,
        active_turn_id TEXT,
        created_at TEXT NOT NULL,
        started_at TEXT,
        completed_at TEXT
      );
      CREATE UNIQUE INDEX one_running_task
        ON tasks(state) WHERE state = 'running';
      CREATE TABLE managed_threads (
        id TEXT PRIMARY KEY,
        workspace TEXT NOT NULL,
        state TEXT NOT NULL CHECK (state IN ('active', 'idle')),
        created_at TEXT NOT NULL,
        last_turn_completed_at TEXT
      );
      CREATE TABLE turns (
        id TEXT PRIMARY KEY,
        task_id INTEGER NOT NULL REFERENCES tasks(id),
        managed_thread_id TEXT NOT NULL REFERENCES managed_threads(id),
        state TEXT NOT NULL CHECK (state IN ('in_progress', 'completed')),
        started_at TEXT NOT NULL,
        completed_at TEXT
      );
    `);
    database.prepare(`
      INSERT INTO queue (id, state, updated_at) VALUES (1, 'paused', ?)
    `).run("2026-09-04T00:00:00.000Z");
    database.prepare(`
      INSERT INTO tasks (workspace, prompt, state, created_at)
      VALUES (?, 'Existing Task', 'queued', ?)
    `).run(workspace, "2026-09-04T00:00:00.000Z");
  } finally {
    database.close();
  }
}
