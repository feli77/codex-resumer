import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { promisify } from "node:util";

import Database from "better-sqlite3";

import { createFakeCodex } from "./fake-codex.js";

const execFileAsync = promisify(execFile);
const cliPath = path.resolve(new URL("../src/cli.js", import.meta.url).pathname);

test("a Workspace Task runs once and completes without an output marker", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "codex-resumer-task-test-"));
  const workspace = path.join(root, "workspace");
  await mkdir(workspace);
  const fakeCodex = await createFakeCodex(root);
  const stateDir = path.join(root, "state");
  const env = {
    ...process.env,
    HOME: root,
    PATH: `${root}:${process.env.PATH ?? ""}`,
    FAKE_CODEX_LOG: fakeCodex.logPath,
    XDG_CONFIG_HOME: path.join(root, "config"),
    XDG_RUNTIME_DIR: path.join(root, "runtime"),
    XDG_STATE_HOME: stateDir,
  };
  const runCli = async (...args: string[]): Promise<string> => {
    const { stdout } = await execFileAsync(process.execPath, [cliPath, ...args], {
      cwd: root,
      encoding: "utf8",
      env,
      timeout: 10_000,
    });
    return stdout;
  };
  t.after(async () => {
    await runCli("daemon", "stop").catch(() => undefined);
    await rm(root, { recursive: true, force: true });
  });

  assert.match(await runCli("daemon", "start"), /Daemon started/);
  assert.equal(
    await runCli("task", "add", "--workspace", "workspace", "Create a note"),
    "Task 1 added.\n",
  );
  assert.equal(await runCli("queue", "start"), "Queue started.\n");

  let status = "";
  for (let attempt = 0; attempt < 100; attempt += 1) {
    status = await runCli("queue", "status");
    if (status.includes("Task 1: completed")) break;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  assert.match(status, /Queue is idle/);
  assert.match(status, /Task 1: completed/);
  assert.match(status, /Thread thread-fake, Turn turn-fake/);
  assert.doesNotMatch(status, /Create a note/);

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
    .filter((message) => message?.method === "thread/start" || message?.method === "turn/start");
  assert.deepEqual(requests, [
    { method: "thread/start", id: 4, params: { cwd: workspace } },
    {
      method: "turn/start",
      id: 5,
      params: {
        threadId: "thread-fake",
        input: [{ type: "text", text: "Create a note" }],
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
});

test("task add rejects a missing Workspace before a Turn can start", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "codex-resumer-task-test-"));
  const fakeCodex = await createFakeCodex(root);
  const env = {
    ...process.env,
    HOME: root,
    PATH: `${root}:${process.env.PATH ?? ""}`,
    FAKE_CODEX_LOG: fakeCodex.logPath,
    XDG_CONFIG_HOME: path.join(root, "config"),
    XDG_RUNTIME_DIR: path.join(root, "runtime"),
    XDG_STATE_HOME: path.join(root, "state"),
  };
  const runCli = (...args: string[]) => execFileAsync(process.execPath, [cliPath, ...args], {
    encoding: "utf8",
    env,
    timeout: 10_000,
  });
  t.after(async () => {
    await runCli("daemon", "stop").catch(() => undefined);
    await rm(root, { recursive: true, force: true });
  });

  await runCli("daemon", "start");
  const missingWorkspace = path.join(root, "missing");
  await assert.rejects(
    runCli("task", "add", "--workspace", missingWorkspace, "Do work"),
    (error: unknown) => {
      assert.ok(error instanceof Error && "stderr" in error);
      assert.match(String(error.stderr), /Workspace is not an accessible directory/);
      return true;
    },
  );

  const log = await readFile(fakeCodex.logPath, "utf8");
  assert.doesNotMatch(log, /"method":"turn\/start"/);
});

test("queue start rechecks a Workspace that became unavailable", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "codex-resumer-task-test-"));
  const workspace = path.join(root, "workspace");
  await mkdir(workspace);
  const fakeCodex = await createFakeCodex(root);
  const env = {
    ...process.env,
    HOME: root,
    PATH: `${root}:${process.env.PATH ?? ""}`,
    FAKE_CODEX_LOG: fakeCodex.logPath,
    XDG_CONFIG_HOME: path.join(root, "config"),
    XDG_RUNTIME_DIR: path.join(root, "runtime"),
    XDG_STATE_HOME: path.join(root, "state"),
  };
  const runCli = (...args: string[]) => execFileAsync(process.execPath, [cliPath, ...args], {
    encoding: "utf8",
    env,
    timeout: 10_000,
  });
  t.after(async () => {
    await runCli("daemon", "stop").catch(() => undefined);
    await rm(root, { recursive: true, force: true });
  });

  await runCli("daemon", "start");
  await runCli("task", "add", "--workspace", workspace, "Do work");
  await rm(workspace, { recursive: true });
  await assert.rejects(
    runCli("queue", "start"),
    (error: unknown) => {
      assert.ok(error instanceof Error && "stderr" in error);
      assert.match(String(error.stderr), /Workspace is not an accessible directory/);
      return true;
    },
  );

  const log = await readFile(fakeCodex.logPath, "utf8");
  assert.doesNotMatch(log, /"method":"turn\/start"/);
});

test("starting the Queue again does not dispatch another Turn", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "codex-resumer-task-test-"));
  const workspace = path.join(root, "workspace");
  await mkdir(workspace);
  const fakeCodex = await createFakeCodex(root, { completeTurn: false });
  const env = {
    ...process.env,
    HOME: root,
    PATH: `${root}:${process.env.PATH ?? ""}`,
    FAKE_CODEX_LOG: fakeCodex.logPath,
    XDG_CONFIG_HOME: path.join(root, "config"),
    XDG_RUNTIME_DIR: path.join(root, "runtime"),
    XDG_STATE_HOME: path.join(root, "state"),
  };
  const runCli = async (...args: string[]): Promise<string> => {
    const { stdout } = await execFileAsync(process.execPath, [cliPath, ...args], {
      encoding: "utf8",
      env,
      timeout: 10_000,
    });
    return stdout;
  };
  t.after(async () => {
    await runCli("daemon", "stop").catch(() => undefined);
    await rm(root, { recursive: true, force: true });
  });

  await runCli("daemon", "start");
  await runCli("task", "add", "--workspace", workspace, "Keep working");
  assert.equal(await runCli("queue", "start"), "Queue started.\n");
  assert.equal(
    await runCli("queue", "start"),
    "Queue already has a running Task.\n",
  );
  assert.match(await runCli("queue", "status"), /Task 1: running/);

  const records = (await readFile(fakeCodex.logPath, "utf8"))
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line) as { type: string; message?: { method?: string } });
  assert.equal(
    records.filter((record) => record.message?.method === "turn/start").length,
    1,
  );
});
