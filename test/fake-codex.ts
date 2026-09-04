import { chmod, writeFile } from "node:fs/promises";
import path from "node:path";

interface FakeCodexOptions {
  authenticated?: boolean;
  authRpcError?: boolean;
  completeTurn?: boolean;
  completeTurnSynchronously?: boolean;
  omitClientRequest?: string;
  omitRateLimitResetTime?: boolean;
  turnCompletionDelayMs?: number;
  turnErrors?: Array<{ codexErrorInfo: string; message: string }>;
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
  const properties = (...names) => Object.fromEntries(names.map((name) => [name, {}]));
  const turnDefinitions = {
    Turn: {
      properties: properties("id", "status", "error", "items"),
      required: ["id", "status", "items"],
    },
    TurnStatus: { enum: ["inProgress", "completed", "interrupted", "failed"] },
  };
  const threadDefinitions = {
    ...turnDefinitions,
    Thread: {
      properties: properties("id", "cwd", "status", "turns"),
      required: ["id", "cwd", "status", "turns"],
    },
  };
  const rateLimitDefinitions = {
    RateLimitSnapshot: {
      properties: properties("primary", "secondary", "rateLimitReachedType"),
    },
    RateLimitWindow: {
      properties: properties("usedPercent", "windowDurationMins", "resetsAt"),
      required: ["usedPercent"],
    },
  };
  const schemaFiles = {
    "v2/GetAccountRateLimitsResponse.json": {
      properties: properties("rateLimits", "rateLimitsByLimitId"),
      required: ["rateLimits"],
      definitions: rateLimitDefinitions,
    },
    "v2/AccountRateLimitsUpdatedNotification.json": {
      properties: properties("rateLimits"),
      required: ["rateLimits"],
      definitions: rateLimitDefinitions,
    },
    "v2/ErrorNotification.json": {
      properties: properties("error", "threadId", "turnId", "willRetry"),
      required: ["error", "threadId", "turnId", "willRetry"],
      definitions: { CodexErrorInfo: { enum: ["usageLimitExceeded", "unauthorized"] } },
    },
    "v2/ThreadStartParams.json": {
      properties: properties("cwd", "approvalPolicy", "sandbox"),
    },
    "v2/ThreadResumeParams.json": {
      properties: properties("threadId"),
      required: ["threadId"],
    },
    "v2/ThreadReadParams.json": {
      properties: properties("threadId", "includeTurns"),
      required: ["threadId"],
    },
    "v2/ThreadStartResponse.json": {
      properties: properties("thread"),
      required: ["thread"],
      definitions: threadDefinitions,
    },
    "v2/ThreadResumeResponse.json": {
      properties: properties("thread"),
      required: ["thread"],
      definitions: threadDefinitions,
    },
    "v2/ThreadReadResponse.json": {
      properties: properties("thread"),
      required: ["thread"],
      definitions: threadDefinitions,
    },
    "v2/TurnStartParams.json": {
      properties: properties("threadId", "input", "approvalPolicy", "sandboxPolicy"),
      required: ["threadId", "input"],
    },
    "v2/TurnInterruptParams.json": {
      properties: properties("threadId", "turnId"),
      required: ["threadId", "turnId"],
    },
    "v2/TurnStartResponse.json": {
      properties: properties("turn"),
      required: ["turn"],
      definitions: turnDefinitions,
    },
    "v2/TurnStartedNotification.json": {
      properties: properties("threadId", "turn"),
      required: ["threadId", "turn"],
      definitions: turnDefinitions,
    },
    "v2/TurnCompletedNotification.json": {
      properties: properties("threadId", "turn"),
      required: ["threadId", "turn"],
      definitions: turnDefinitions,
    },
    "CommandExecutionRequestApprovalParams.json": {
      properties: properties("itemId", "threadId", "turnId"),
      required: ["itemId", "threadId", "turnId"],
    },
    "CommandExecutionRequestApprovalResponse.json": {
      properties: properties("decision"),
      required: ["decision"],
      enum: ["decline", "cancel"],
    },
    "FileChangeRequestApprovalParams.json": {
      properties: properties("itemId", "threadId", "turnId"),
      required: ["itemId", "threadId", "turnId"],
    },
    "FileChangeRequestApprovalResponse.json": {
      properties: properties("decision"),
      required: ["decision"],
      enum: ["decline", "cancel"],
    },
    "PermissionsRequestApprovalParams.json": {
      properties: properties("itemId", "threadId", "turnId", "permissions"),
      required: ["itemId", "threadId", "turnId", "permissions"],
    },
    "PermissionsRequestApprovalResponse.json": {
      properties: properties("permissions"),
      required: ["permissions"],
    },
    "ToolRequestUserInputParams.json": {
      properties: properties("itemId", "threadId", "turnId", "questions"),
      required: ["itemId", "threadId", "turnId", "questions"],
    },
    "ToolRequestUserInputResponse.json": {
      properties: properties("answers"),
      required: ["answers"],
    },
    "McpServerElicitationRequestParams.json": {
      properties: properties("serverName", "threadId", "turnId"),
      required: ["serverName", "threadId"],
      enum: ["form", "url"],
    },
    "McpServerElicitationRequestResponse.json": {
      properties: properties("action", "content"),
      required: ["action"],
      enum: ["decline", "cancel"],
    },
  };
  if (${String(options.omitRateLimitResetTime === true)}) {
    delete schemaFiles["v2/GetAccountRateLimitsResponse.json"]
      .definitions.RateLimitWindow.properties.resetsAt;
  }
  for (const [relativePath, schema] of Object.entries(schemaFiles)) {
    const target = path.join(out, relativePath);
    mkdirSync(path.dirname(target), { recursive: true });
    writeFileSync(target, JSON.stringify(schema));
  }
  process.exit(0);
}

if (args[0] !== "app-server" || args[1] !== "--stdio") process.exit(2);

const lines = readline.createInterface({ input: process.stdin });
const threads = new Map();
let startedThreadCount = 0;
let turnCount = 0;
const turnErrors = ${JSON.stringify(options.turnErrors ?? [])};
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
    if (${String(options.authRpcError === true)}) {
      console.log(JSON.stringify({ id: message.id, error: {
        code: -32000,
        message: "authentication failed",
        data: { codexErrorInfo: "unauthorized" },
      } }));
    } else {
      console.log(JSON.stringify({ id: message.id, result: {
        account: ${JSON.stringify(account)},
        requiresOpenaiAuth: true,
      } }));
    }
  } else if (message.method === "account/rateLimits/read") {
    console.log(JSON.stringify({ id: message.id, result: {
      rateLimits: null,
      rateLimitsByLimitId: null,
    } }));
  } else if (message.method === "thread/start") {
    startedThreadCount += 1;
    const threadId = startedThreadCount === 1
      ? "thread-fake"
      : "thread-fake-" + startedThreadCount;
    const thread = {
      id: threadId,
      cwd: message.params.cwd,
      status: "idle",
      turns: [],
    };
    threads.set(threadId, message.params.cwd);
    console.log(JSON.stringify({ id: message.id, result: { thread } }));
    console.log(JSON.stringify({ method: "thread/started", params: { thread } }));
  } else if (message.method === "thread/read") {
    if (message.params.threadId === "thread-missing") {
      console.log(JSON.stringify({ id: message.id, error: {
        code: -32001,
        message: "Thread does not exist",
      } }));
      return;
    }
    const workspace = threads.get(message.params.threadId)
      ?? process.env.FAKE_CODEX_IMPORTED_WORKSPACE;
    const thread = {
      id: message.params.threadId,
      cwd: workspace,
      status: "idle",
      turns: [],
    };
    threads.set(message.params.threadId, workspace);
    console.log(JSON.stringify({ id: message.id, result: { thread } }));
  } else if (message.method === "thread/resume") {
    const thread = {
      id: message.params.threadId,
      cwd: threads.get(message.params.threadId),
      status: "idle",
      turns: [],
    };
    console.log(JSON.stringify({ id: message.id, result: { thread } }));
  } else if (message.method === "turn/start") {
    turnCount += 1;
    const turnId = turnCount === 1 ? "turn-fake" : "turn-fake-" + turnCount;
    const turn = { id: turnId, status: "inProgress", items: [] };
    const response = JSON.stringify({ id: message.id, result: { turn } });
    const turnError = turnErrors[turnCount - 1];
    if (turnError) {
      const error = JSON.stringify({ method: "error", params: {
        error: turnError,
        threadId: message.params.threadId,
        turnId,
        willRetry: false,
      } });
      const failed = JSON.stringify({ method: "turn/completed", params: {
        threadId: message.params.threadId,
        turn: { ...turn, status: "failed", error: turnError },
      } });
      process.stdout.write(response + "\\n" + error + "\\n" + failed + "\\n");
    } else if (${String(options.completeTurn !== false)}) {
      const completed = JSON.stringify({ method: "turn/completed", params: {
        threadId: message.params.threadId,
        turn: { ...turn, status: "completed" },
      } });
      if (${String(options.completeTurnSynchronously === true)}) {
        process.stdout.write(response + "\\n" + completed + "\\n");
      } else {
        console.log(response);
        setTimeout(
          () => console.log(completed),
          ${String(options.turnCompletionDelayMs ?? 10)},
        );
      }
    } else console.log(response);
  }
});
`;
  await writeFile(command, source, { mode: 0o700 });
  await chmod(command, 0o700);
  return { command, logPath };
}
