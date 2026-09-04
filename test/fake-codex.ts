import { chmod, writeFile } from "node:fs/promises";
import path from "node:path";

interface FakeCodexOptions {
  authenticated?: boolean;
  omitClientRequest?: string;
}

export async function createFakeCodex(
  root: string,
  options: FakeCodexOptions = {},
): Promise<{
  command: string;
  logPath: string;
}> {
  const command = path.join(root, "codex");
  const logPath = path.join(root, "fake-codex.jsonl");
  const clientRequests = [
    "initialize",
    "account/read",
    "account/rateLimits/read",
    "thread/start",
    "thread/resume",
    "thread/read",
    "turn/start",
    "turn/interrupt",
  ].filter((method) => method !== options.omitClientRequest);
  const account = options.authenticated === false
    ? null
    : { type: "chatgpt", email: null, planType: "plus" };
  const source = `#!/usr/bin/env node
import { appendFileSync, mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import readline from "node:readline";

const args = process.argv.slice(2);
const logPath = process.env.FAKE_CODEX_LOG;
const log = (value) => {
  if (logPath) appendFileSync(logPath, JSON.stringify(value) + "\\n");
};
log({ type: "args", args });

if (args[0] === "--version") {
  console.log("codex-cli 9.fake");
  process.exit(0);
}

if (args[0] === "app-server" && args[1] === "generate-json-schema") {
  const outIndex = args.indexOf("--out");
  const out = args[outIndex + 1];
  mkdirSync(path.join(out, "v2"), { recursive: true });
  const methods = (values) => ({
    oneOf: values.map((method) => ({ properties: { method: { enum: [method] } } })),
  });
  writeFileSync(path.join(out, "ClientRequest.json"), JSON.stringify(methods(${JSON.stringify(clientRequests)})));
  writeFileSync(path.join(out, "ServerNotification.json"), JSON.stringify(methods([
    "account/updated",
    "account/rateLimits/updated",
    "error",
    "thread/started",
    "turn/started",
    "turn/completed",
  ])));
  writeFileSync(path.join(out, "ServerRequest.json"), JSON.stringify(methods([
    "item/commandExecution/requestApproval",
    "item/fileChange/requestApproval",
    "item/permissions/requestApproval",
    "item/tool/requestUserInput",
    "mcpServer/elicitation/request",
  ])));
  writeFileSync(path.join(out, "v2", "ErrorNotification.json"), JSON.stringify({
    definitions: { CodexErrorInfo: { enum: ["usageLimitExceeded"] } },
  }));
  writeFileSync(path.join(out, "v2", "TurnCompletedNotification.json"), JSON.stringify({
    definitions: { TurnStatus: { enum: ["inProgress", "completed", "interrupted", "failed"] } },
  }));
  process.exit(0);
}

if (args[0] !== "app-server" || args[1] !== "--stdio") process.exit(2);

const lines = readline.createInterface({ input: process.stdin });
lines.on("line", (line) => {
  const message = JSON.parse(line);
  log({ type: "message", message });
  if (message.method === "initialize") {
    console.log(JSON.stringify({ id: message.id, result: {
      codexHome: "/tmp/fake-codex-home",
      platformFamily: "unix",
      platformOs: "linux",
      userAgent: "fake-codex",
    } }));
  } else if (message.method === "account/read") {
    console.log(JSON.stringify({ id: message.id, result: {
      account: ${JSON.stringify(account)},
      requiresOpenaiAuth: true,
    } }));
  } else if (message.method === "account/rateLimits/read") {
    console.log(JSON.stringify({ id: message.id, result: {
      rateLimits: null,
      rateLimitsByLimitId: null,
    } }));
  }
});
`;
  await writeFile(command, source, { mode: 0o700 });
  await chmod(command, 0o700);
  return { command, logPath };
}
