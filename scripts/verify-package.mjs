import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repository = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const temporaryDirectory = mkdtempSync(path.join(tmpdir(), "codex-resumer-package-"));

try {
  const packageName = execFileSync(
    "npm",
    ["pack", "--pack-destination", temporaryDirectory],
    { cwd: repository, encoding: "utf8" },
  ).trim().split("\n").at(-1);
  assert.ok(packageName, "npm pack did not return a package filename");
  const packagePath = path.join(temporaryDirectory, packageName);
  const installation = path.join(temporaryDirectory, "installation");
  execFileSync(
    "npm",
    ["install", "--prefix", installation, packagePath],
    { encoding: "utf8", stdio: "pipe" },
  );

  const executable = path.join(
    installation,
    "node_modules",
    ".bin",
    "codex-resumer",
  );
  const help = execFileSync(executable, ["--help"], { encoding: "utf8" });
  for (const command of [
    "daemon start",
    "queue status",
    "task add",
    "thread import",
    "logs follow",
    "config show",
  ]) {
    assert.match(help, new RegExp(`codex-resumer ${command}`));
  }
  process.stdout.write("Packed package installed and its CLI command surface loaded.\n");
} finally {
  rmSync(temporaryDirectory, { force: true, recursive: true });
}
