import { chmod, mkdir, readFile, rename, writeFile } from "node:fs/promises";

import type { DaemonPaths } from "./paths.js";

export const DEFAULT_CONTINUATION_PROMPT =
  "Inspect the current Thread and Workspace state, continue the unfinished Task, and do not repeat work that is already complete.";

export interface Configuration {
  continuationPrompt: string;
}

export async function readConfiguration(configPath: string): Promise<Configuration> {
  let contents: string;
  try {
    contents = await readFile(configPath, "utf8");
  } catch (error) {
    if (hasErrorCode(error, "ENOENT")) {
      return { continuationPrompt: DEFAULT_CONTINUATION_PROMPT };
    }
    throw error;
  }
  let value: unknown;
  try {
    value = JSON.parse(contents);
  } catch {
    throw new Error(`Invalid Codex Resumer configuration: ${configPath}`);
  }
  if (
    !isRecord(value)
    || typeof value.continuationPrompt !== "string"
    || value.continuationPrompt.trim().length === 0
  ) {
    throw new Error(`Invalid Codex Resumer configuration: ${configPath}`);
  }
  return { continuationPrompt: value.continuationPrompt };
}

export async function setContinuationPrompt(
  paths: Pick<DaemonPaths, "configDir" | "configPath">,
  prompt: string,
): Promise<void> {
  if (prompt.trim().length === 0) {
    throw new Error("Continuation prompt must not be empty.");
  }
  await mkdir(paths.configDir, { recursive: true, mode: 0o700 });
  await chmod(paths.configDir, 0o700);
  const temporaryPath = `${paths.configPath}.${process.pid}.tmp`;
  await writeFile(
    temporaryPath,
    `${JSON.stringify({ continuationPrompt: prompt }, null, 2)}\n`,
    { mode: 0o600 },
  );
  await chmod(temporaryPath, 0o600);
  await rename(temporaryPath, paths.configPath);
}

function hasErrorCode(error: unknown, code: string): boolean {
  return isRecord(error) && error.code === code;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
