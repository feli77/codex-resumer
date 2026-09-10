import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

if (process.env.CODEX_RESUMER_RUN_REAL_SMOKE !== "1") {
  throw new Error(
    "Real smoke test disabled. Set CODEX_RESUMER_RUN_REAL_SMOKE=1 to authorize one harmless Codex Turn.",
  );
}

const repository = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const executable = path.join(repository, "dist", "src", "cli.js");
const temporaryDirectory = mkdtempSync(path.join(tmpdir(), "codex-resumer-smoke-"));
const workspace = path.join(temporaryDirectory, "workspace");
mkdirSync(workspace);
const environment = {
  ...process.env,
  XDG_CONFIG_HOME: path.join(temporaryDirectory, "config"),
  XDG_RUNTIME_DIR: path.join(temporaryDirectory, "runtime"),
  XDG_STATE_HOME: path.join(temporaryDirectory, "state"),
};
const run = (...arguments_) => execFileSync(
  process.execPath,
  [executable, ...arguments_],
  { encoding: "utf8", env: environment, timeout: 30_000 },
);

try {
  execFileSync("codex", ["login", "status"], {
    encoding: "utf8",
    stdio: "pipe",
    timeout: 10_000,
  });
  execFileSync("npm", ["run", "build"], {
    cwd: repository,
    encoding: "utf8",
    stdio: "pipe",
  });
  assert.match(run("config", "show"), /Access Mode: Configured Access/);
  assert.match(run("daemon", "start"), /Daemon started/);
  assert.match(run("daemon", "status"), /Daemon is running/);
  run(
    "task",
    "add",
    "--workspace",
    workspace,
    "This is a harmless smoke test. Do not run commands or modify files. Reply briefly that the Task is complete.",
  );
  execFileSync(
    process.execPath,
    [executable, "queue", "start", "--until-idle"],
    { encoding: "utf8", env: environment, stdio: "pipe", timeout: 30_000 },
  );

  let status = "";
  for (let attempt = 0; attempt < 120; attempt += 1) {
    status = run("queue", "status");
    if (status.includes("Task 1: completed")) break;
    if (status.includes("Needs Attention")) {
      throw new Error(`Smoke Task needs attention:\n${status}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 1_000));
  }
  assert.match(status, /Task 1: completed/);
  assert.match(status, /Queue is idle/);

  assert.match(run("daemon", "stop"), /Daemon stopped/);
  assert.match(run("daemon", "start"), /Daemon started/);
  assert.match(run("daemon", "status"), /Daemon is running/);
  assert.match(run("queue", "status"), /Task 1: completed/);
  process.stdout.write("Configured Access smoke test passed, including daemon restart.\n");
} finally {
  try {
    run("daemon", "stop", "--force");
  } catch {
    // The daemon may not have started or may already be stopped.
  }
  rmSync(temporaryDirectory, { force: true, recursive: true });
}
