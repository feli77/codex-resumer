import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { promisify } from "node:util";

import { createFakeCodex } from "./fake-codex.js";

const execFileAsync = promisify(execFile);
const cliPath = path.resolve(new URL("../src/cli.js", import.meta.url).pathname);

test("CLI starts one detached daemon, reports it, and stops it", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "codex-resumer-cli-test-"));
  await createFakeCodex(root);
  const env = {
    ...process.env,
    HOME: root,
    PATH: `${root}:${process.env.PATH ?? ""}`,
    FAKE_CODEX_LOG: path.join(root, "fake-codex.jsonl"),
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

  assert.match(await runCli("--help"), /codex-resumer daemon <start\|stop\|status>/);
  assert.match(await runCli("daemon", "start"), /Daemon started/);
  assert.match(await runCli("daemon", "start"), /already running/);
  assert.match(await runCli("daemon", "status"), /Daemon is running.*codex-cli 9\.fake/);
  assert.match(await runCli("daemon", "stop"), /Daemon stopped/);
  assert.match(await runCli("daemon", "status"), /Daemon is stopped/);
});
