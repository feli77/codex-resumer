import { execFile, spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { readFile, rm, mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import readline, { type Interface as ReadLineInterface } from "node:readline";
import { promisify } from "node:util";

import type {
  AccountRateLimits,
  AppServerController,
  AppServerProbeResult,
  CompletedTurn,
  RateLimitSnapshot,
  StartedTurn,
  UnattendedRequest,
  UnattendedRequestKind,
  UsageLimitExceeded,
} from "./daemon.js";
import type { AccessMode } from "./config.js";

const execFileAsync = promisify(execFile);

const requiredClientRequests = [
  "initialize",
  "account/read",
  "account/rateLimits/read",
  "config/read",
  "thread/start",
  "thread/resume",
  "thread/read",
  "turn/start",
  "turn/interrupt",
] as const;

const requiredServerNotifications = [
  "account/updated",
  "account/rateLimits/updated",
  "error",
  "thread/started",
  "turn/started",
  "turn/completed",
] as const;

const requiredServerRequests = [
  "item/commandExecution/requestApproval",
  "item/fileChange/requestApproval",
  "item/permissions/requestApproval",
  "item/tool/requestUserInput",
  "mcpServer/elicitation/request",
] as const;

const requiredTurnStatuses = [
  "inProgress",
  "completed",
  "interrupted",
  "failed",
] as const;

interface SchemaObjectContract {
  definition?: string;
  literals?: readonly string[];
  properties?: readonly string[];
  required?: readonly string[];
}

interface SchemaFileContract {
  capability: string;
  file: string;
  objects: readonly SchemaObjectContract[];
}

const schemaFileContracts: readonly SchemaFileContract[] = [
  {
    capability: "payload:account/rateLimits/read",
    file: "v2/GetAccountRateLimitsResponse.json",
    objects: [
      { properties: ["rateLimits", "rateLimitsByLimitId"], required: ["rateLimits"] },
      {
        definition: "RateLimitSnapshot",
        properties: ["primary", "secondary", "rateLimitReachedType"],
      },
      {
        definition: "RateLimitWindow",
        properties: ["usedPercent", "windowDurationMins", "resetsAt"],
        required: ["usedPercent"],
      },
    ],
  },
  {
    capability: "payload:account/rateLimits/updated",
    file: "v2/AccountRateLimitsUpdatedNotification.json",
    objects: [
      { properties: ["rateLimits"], required: ["rateLimits"] },
      {
        definition: "RateLimitSnapshot",
        properties: ["primary", "secondary", "rateLimitReachedType"],
      },
      {
        definition: "RateLimitWindow",
        properties: ["usedPercent", "windowDurationMins", "resetsAt"],
      },
    ],
  },
  {
    capability: "payload:error",
    file: "v2/ErrorNotification.json",
    objects: [{
      properties: ["error", "threadId", "turnId", "willRetry"],
      required: ["error", "threadId", "turnId", "willRetry"],
      literals: [
        "httpConnectionFailed",
        "internalServerError",
        "rateLimitExceeded",
        "responseStreamConnectionFailed",
        "responseStreamDisconnected",
        "responseTooManyFailedAttempts",
        "serverOverloaded",
        "unauthorized",
        "usageLimitExceeded",
      ],
    }],
  },
  {
    capability: "payload:config/read-request",
    file: "v2/ConfigReadParams.json",
    objects: [{ properties: ["cwd", "includeLayers"] }],
  },
  {
    capability: "payload:config/read-response",
    file: "v2/ConfigReadResponse.json",
    objects: [
      { properties: ["config"], required: ["config"] },
      {
        definition: "Config",
        properties: ["approval_policy", "sandbox_mode", "sandbox_workspace_write"],
      },
    ],
  },
  {
    capability: "payload:thread/start-request",
    file: "v2/ThreadStartParams.json",
    objects: [{ properties: ["cwd", "approvalPolicy", "sandbox"] }],
  },
  {
    capability: "payload:thread/resume-request",
    file: "v2/ThreadResumeParams.json",
    objects: [{ properties: ["threadId"], required: ["threadId"] }],
  },
  {
    capability: "payload:thread/read-request",
    file: "v2/ThreadReadParams.json",
    objects: [{ properties: ["threadId", "includeTurns"], required: ["threadId"] }],
  },
  threadResponseContract("start", "v2/ThreadStartResponse.json"),
  threadResponseContract("resume", "v2/ThreadResumeResponse.json"),
  threadResponseContract("read", "v2/ThreadReadResponse.json"),
  {
    capability: "payload:turn/start-request",
    file: "v2/TurnStartParams.json",
    objects: [{
      properties: ["threadId", "input", "approvalPolicy", "sandboxPolicy"],
      required: ["threadId", "input"],
      literals: ["dangerFullAccess", "never", "readOnly", "workspaceWrite"],
    }],
  },
  {
    capability: "payload:turn/interrupt-request",
    file: "v2/TurnInterruptParams.json",
    objects: [{
      properties: ["threadId", "turnId"],
      required: ["threadId", "turnId"],
    }],
  },
  turnContract("start", "v2/TurnStartResponse.json", false),
  turnContract("started", "v2/TurnStartedNotification.json", true),
  turnContract("completed", "v2/TurnCompletedNotification.json", true),
  ...approvalContract(
    "item/commandExecution/requestApproval",
    "CommandExecutionRequestApprovalParams.json",
    "CommandExecutionRequestApprovalResponse.json",
    "decision",
    ["decline", "cancel"],
  ),
  ...approvalContract(
    "item/fileChange/requestApproval",
    "FileChangeRequestApprovalParams.json",
    "FileChangeRequestApprovalResponse.json",
    "decision",
    ["decline", "cancel"],
  ),
  ...approvalContract(
    "item/permissions/requestApproval",
    "PermissionsRequestApprovalParams.json",
    "PermissionsRequestApprovalResponse.json",
    "permissions",
  ),
  ...approvalContract(
    "item/tool/requestUserInput",
    "ToolRequestUserInputParams.json",
    "ToolRequestUserInputResponse.json",
    "answers",
  ),
  {
    capability: "payload:mcpServer/elicitation/request",
    file: "McpServerElicitationRequestParams.json",
    objects: [{
      properties: ["serverName", "threadId", "turnId"],
      required: ["serverName", "threadId"],
      literals: ["form", "url"],
    }],
  },
  {
    capability: "payload:mcpServer/elicitation/response",
    file: "McpServerElicitationRequestResponse.json",
    objects: [{
      properties: ["action", "content"],
      required: ["action"],
      literals: ["decline", "cancel"],
    }],
  },
];

interface CodexAppServerOptions {
  command?: string;
  env?: NodeJS.ProcessEnv;
  requestTimeoutMs?: number;
}

interface PendingRequest {
  method: string;
  reject(error: Error): void;
  resolve(value: unknown): void;
  timeout: NodeJS.Timeout;
}

export class AppServerRpcError extends Error {
  constructor(
    message: string,
    readonly method: string,
    readonly rpcCode: number | undefined,
    readonly data: unknown,
  ) {
    super(message);
    this.name = "AppServerRpcError";
  }
}

export class CodexAppServer implements AppServerController {
  readonly #command: string;
  readonly #env: NodeJS.ProcessEnv;
  readonly #requestTimeoutMs: number;
  #child: ChildProcessWithoutNullStreams | undefined;
  #lineReader: ReadLineInterface | undefined;
  #nextRequestId = 0;
  #pending = new Map<number, PendingRequest>();
  #turnCompletedListeners = new Set<(turn: CompletedTurn) => void>();
  #turnStartedListeners = new Set<(turn: StartedTurn) => void>();
  #unattendedRequestListeners = new Set<
    (request: UnattendedRequest) => void
  >();
  #unexpectedExitListeners = new Set<(error: Error) => void>();
  #usageLimitListeners = new Set<(event: UsageLimitExceeded) => void>();
  #closePromise: Promise<void> | undefined;

  constructor(options: CodexAppServerOptions = {}) {
    this.#command = options.command ?? "codex";
    this.#env = options.env ?? process.env;
    this.#requestTimeoutMs = options.requestTimeoutMs ?? 5_000;
  }

  async startAndProbe(): Promise<AppServerProbeResult> {
    let codexVersion: string;
    try {
      codexVersion = await this.#readVersion();
    } catch {
      return incompatible(
        "unavailable",
        ["codex-cli"],
        "The Codex CLI could not be executed.",
      );
    }

    let missingCapabilities: string[];
    try {
      missingCapabilities = await this.#inspectProtocolSchema();
    } catch {
      return incompatible(
        codexVersion,
        ["protocol-schema"],
        "The installed Codex App Server protocol schema could not be inspected.",
      );
    }
    if (missingCapabilities.length > 0) {
      return incompatible(
        codexVersion,
        missingCapabilities,
        "Required Codex App Server capabilities are unavailable.",
      );
    }

    try {
      await this.#startProcess();
      await this.#request("initialize", {
        clientInfo: {
          name: "codex_resumer",
          title: "Codex Resumer",
          version: "0.1.0",
        },
      });
      this.#notify("initialized", {});

      try {
        const accountResult = await this.#request("account/read", {
          refreshToken: false,
        });
        if (!hasChatGptAccount(accountResult)) {
          return unauthenticated(codexVersion);
        }
        await this.#request("account/rateLimits/read");
      } catch (error) {
        if (isAuthenticationError(error)) {
          return unauthenticated(codexVersion);
        }
        throw error;
      }
      return { state: "ready", codexVersion };
    } catch {
      await this.close();
      return incompatible(
        codexVersion,
        ["live-app-server-probe"],
        "The installed Codex App Server did not complete its startup probe.",
      );
    }
  }

  async close(): Promise<void> {
    if (this.#closePromise) return this.#closePromise;
    const child = this.#child;
    if (!child || child.exitCode !== null || child.signalCode !== null) return;

    this.#closePromise = new Promise<void>((resolve) => {
      const forceTimer = setTimeout(() => child.kill("SIGKILL"), 1_000);
      child.once("exit", () => {
        clearTimeout(forceTimer);
        this.#lineReader?.close();
        this.#rejectPending(new Error("Codex App Server stopped"));
        resolve();
      });
      child.kill("SIGTERM");
    });
    return this.#closePromise;
  }

  async startThread(workspace: string): Promise<{ threadId: string }> {
    const result = await this.#request("thread/start", { cwd: workspace });
    if (!isRecord(result) || !isRecord(result.thread) || typeof result.thread.id !== "string") {
      throw new Error("Codex App Server returned an invalid Thread response");
    }
    return { threadId: result.thread.id };
  }

  async readThread(threadId: string): Promise<{ threadId: string; workspace: string }> {
    const result = await this.#request("thread/read", {
      threadId,
      includeTurns: false,
    });
    if (
      !isRecord(result)
      || !isRecord(result.thread)
      || result.thread.id !== threadId
      || typeof result.thread.cwd !== "string"
    ) {
      throw new Error("Codex App Server returned an invalid Thread response");
    }
    return { threadId, workspace: result.thread.cwd };
  }

  async resumeThread(threadId: string): Promise<{ threadId: string }> {
    const result = await this.#request("thread/resume", { threadId });
    if (
      !isRecord(result)
      || !isRecord(result.thread)
      || result.thread.id !== threadId
    ) {
      throw new Error("Codex App Server returned an invalid Thread response");
    }
    return { threadId };
  }

  async readRateLimits(): Promise<AccountRateLimits> {
    return parseAccountRateLimits(await this.#request("account/rateLimits/read"));
  }

  async startTurn(
    threadId: string,
    prompt: string,
    workspace: string,
    accessMode: AccessMode,
  ): Promise<{ turnId: string }> {
    const access = accessMode === "full"
      ? fullAccess()
      : await this.#readConfiguredAccess(workspace);
    const result = await this.#request("turn/start", {
      approvalPolicy: access.approvalPolicy,
      threadId,
      input: [{ type: "text", text: prompt }],
      sandboxPolicy: access.sandboxPolicy,
    });
    if (!isRecord(result) || !isRecord(result.turn) || typeof result.turn.id !== "string") {
      throw new Error("Codex App Server returned an invalid Turn response");
    }
    return { turnId: result.turn.id };
  }

  async interruptTurn(threadId: string, turnId: string): Promise<void> {
    await this.#request("turn/interrupt", { threadId, turnId });
  }

  async #readConfiguredAccess(workspace: string): Promise<TurnAccess> {
    const result = await this.#request("config/read", {
      cwd: workspace,
      includeLayers: false,
    });
    if (!isRecord(result) || !isRecord(result.config)) {
      throw new Error("Codex App Server returned invalid access configuration");
    }
    return {
      approvalPolicy: parseApprovalPolicy(result.config.approval_policy),
      sandboxPolicy: parseSandboxPolicy(
        result.config.sandbox_mode,
        result.config.sandbox_workspace_write,
      ),
    };
  }

  onTurnCompleted(listener: (turn: CompletedTurn) => void): () => void {
    this.#turnCompletedListeners.add(listener);
    return () => this.#turnCompletedListeners.delete(listener);
  }

  onTurnStarted(listener: (turn: StartedTurn) => void): () => void {
    this.#turnStartedListeners.add(listener);
    return () => this.#turnStartedListeners.delete(listener);
  }

  onUnattendedRequest(
    listener: (request: UnattendedRequest) => void,
  ): () => void {
    this.#unattendedRequestListeners.add(listener);
    return () => this.#unattendedRequestListeners.delete(listener);
  }

  onUnexpectedExit(listener: (error: Error) => void): () => void {
    this.#unexpectedExitListeners.add(listener);
    return () => this.#unexpectedExitListeners.delete(listener);
  }

  onUsageLimitExceeded(listener: (event: UsageLimitExceeded) => void): () => void {
    this.#usageLimitListeners.add(listener);
    return () => this.#usageLimitListeners.delete(listener);
  }

  async #readVersion(): Promise<string> {
    const { stdout } = await execFileAsync(this.#command, ["--version"], {
      encoding: "utf8",
      env: this.#env,
      timeout: 10_000,
    });
    return stdout.trim() || "unknown";
  }

  async #inspectProtocolSchema(): Promise<string[]> {
    const schemaDirectory = await mkdtemp(
      path.join(tmpdir(), "codex-resumer-schema-"),
    );
    try {
      await execFileAsync(
        this.#command,
        ["app-server", "generate-json-schema", "--out", schemaDirectory],
        { encoding: "utf8", env: this.#env, timeout: 10_000 },
      );

      const [clientSchema, notificationSchema, requestSchema] =
        await Promise.all([
          readJson(path.join(schemaDirectory, "ClientRequest.json")),
          readJson(path.join(schemaDirectory, "ServerNotification.json")),
          readJson(path.join(schemaDirectory, "ServerRequest.json")),
        ]);

      const clientRequests = collectMethods(clientSchema);
      const notifications = collectMethods(notificationSchema);
      const serverRequests = collectMethods(requestSchema);
      const missing: string[] = [];

      for (const method of requiredClientRequests) {
        if (!clientRequests.has(method)) missing.push(`client-request:${method}`);
      }
      for (const method of requiredServerNotifications) {
        if (!notifications.has(method)) missing.push(`server-notification:${method}`);
      }
      for (const method of requiredServerRequests) {
        if (!serverRequests.has(method)) missing.push(`server-request:${method}`);
      }
      missing.push(...await missingSchemaContracts(schemaDirectory));
      return missing;
    } finally {
      await rm(schemaDirectory, { recursive: true, force: true });
    }
  }

  async #startProcess(): Promise<void> {
    const child = spawn(this.#command, ["app-server", "--stdio"], {
      env: this.#env,
      stdio: ["pipe", "pipe", "pipe"],
    });
    this.#child = child;
    child.stderr.resume();
    this.#lineReader = readline.createInterface({ input: child.stdout });
    this.#lineReader.on("line", (line) => this.#handleLine(line));
    child.once("exit", (code, signal) => {
      const error = new Error(
        `Codex App Server exited unexpectedly (${signal ?? `code ${code ?? "unknown"}`}).`,
      );
      this.#rejectPending(error);
      if (!this.#closePromise) {
        for (const listener of this.#unexpectedExitListeners) listener(error);
      }
    });

    await new Promise<void>((resolve, reject) => {
      child.once("spawn", resolve);
      child.once("error", reject);
    });
  }

  #request(method: string, params?: unknown): Promise<unknown> {
    const child = this.#child;
    if (!child) return Promise.reject(new Error("Codex App Server is not running"));
    const id = ++this.#nextRequestId;
    const message = params === undefined ? { method, id } : { method, id, params };

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.#pending.delete(id);
        reject(new Error(`Codex App Server request timed out: ${method}`));
      }, this.#requestTimeoutMs);
      this.#pending.set(id, { method, resolve, reject, timeout });
      child.stdin.write(`${JSON.stringify(message)}\n`, (error) => {
        if (!error) return;
        const pending = this.#pending.get(id);
        if (!pending) return;
        clearTimeout(pending.timeout);
        this.#pending.delete(id);
        pending.reject(error);
      });
    });
  }

  #notify(method: string, params: unknown): void {
    this.#child?.stdin.write(`${JSON.stringify({ method, params })}\n`);
  }

  #handleLine(line: string): void {
    let message: unknown;
    try {
      message = JSON.parse(line);
    } catch {
      this.#rejectPending(new Error("Codex App Server returned invalid JSON"));
      return;
    }
    if (!isRecord(message)) return;
    const unattendedRequest = parseUnattendedRequest(message);
    if (
      unattendedRequest
      && (typeof message.id === "number" || typeof message.id === "string")
    ) {
      this.#respond(message.id, safeUnattendedResponse(unattendedRequest.kind));
      for (const listener of this.#unattendedRequestListeners) {
        listener(unattendedRequest);
      }
      return;
    }
    if (message.method === "error") {
      const usageLimit = parseUsageLimitExceeded(message.params);
      if (usageLimit) {
        for (const listener of this.#usageLimitListeners) listener(usageLimit);
      }
      return;
    }
    if (message.method === "turn/completed") {
      const completed = parseCompletedTurn(message.params);
      if (completed) {
        for (const listener of this.#turnCompletedListeners) listener(completed);
      }
      return;
    }
    if (message.method === "turn/started") {
      const started = parseStartedTurn(message.params);
      if (started) {
        for (const listener of this.#turnStartedListeners) listener(started);
      }
      return;
    }
    if (typeof message.id !== "number") return;
    const pending = this.#pending.get(message.id);
    if (!pending) return;
    clearTimeout(pending.timeout);
    this.#pending.delete(message.id);
    if (message.error !== undefined) {
      const rpcError = isRecord(message.error) ? message.error : {};
      pending.reject(
        new AppServerRpcError(
          typeof rpcError.message === "string"
            ? rpcError.message
            : "Codex App Server rejected the request",
          pending.method,
          typeof rpcError.code === "number" ? rpcError.code : undefined,
          rpcError.data,
        ),
      );
    } else {
      pending.resolve(message.result);
    }
  }

  #rejectPending(error: Error): void {
    for (const pending of this.#pending.values()) {
      clearTimeout(pending.timeout);
      pending.reject(error);
    }
    this.#pending.clear();
  }

  #respond(id: number | string, result: unknown): void {
    this.#child?.stdin.write(`${JSON.stringify({ id, result })}\n`);
  }
}

type ApprovalPolicy =
  | "never"
  | "on-request"
  | "untrusted"
  | {
      granular: {
        mcp_elicitations: boolean;
        request_permissions?: boolean;
        rules: boolean;
        sandbox_approval: boolean;
        skill_approval?: boolean;
      };
    }
  | null;

type SandboxPolicy =
  | { type: "dangerFullAccess" }
  | { networkAccess: false; type: "readOnly" }
  | {
      excludeSlashTmp: boolean;
      excludeTmpdirEnvVar: boolean;
      networkAccess: boolean;
      type: "workspaceWrite";
      writableRoots: string[];
    }
  | null;

interface TurnAccess {
  approvalPolicy: ApprovalPolicy;
  sandboxPolicy: SandboxPolicy;
}

function fullAccess(): TurnAccess {
  return {
    approvalPolicy: "never",
    sandboxPolicy: { type: "dangerFullAccess" },
  };
}

function parseApprovalPolicy(value: unknown): ApprovalPolicy {
  if (
    value === null
    || value === "never"
    || value === "on-request"
    || value === "untrusted"
  ) return value;
  if (!isRecord(value) || !isRecord(value.granular)) {
    throw new Error("Codex App Server returned invalid access configuration");
  }
  const granular = value.granular;
  if (
    typeof granular.mcp_elicitations !== "boolean"
    || typeof granular.rules !== "boolean"
    || typeof granular.sandbox_approval !== "boolean"
    || (
      granular.request_permissions !== undefined
      && typeof granular.request_permissions !== "boolean"
    )
    || (
      granular.skill_approval !== undefined
      && typeof granular.skill_approval !== "boolean"
    )
  ) {
    throw new Error("Codex App Server returned invalid access configuration");
  }
  return {
    granular: {
      mcp_elicitations: granular.mcp_elicitations,
      ...(granular.request_permissions === undefined
        ? {}
        : { request_permissions: granular.request_permissions }),
      rules: granular.rules,
      sandbox_approval: granular.sandbox_approval,
      ...(granular.skill_approval === undefined
        ? {}
        : { skill_approval: granular.skill_approval }),
    },
  };
}

function parseSandboxPolicy(mode: unknown, options: unknown): SandboxPolicy {
  if (mode === null) return null;
  if (mode === "danger-full-access") return { type: "dangerFullAccess" };
  if (mode === "read-only") return { networkAccess: false, type: "readOnly" };
  if (mode !== "workspace-write") {
    throw new Error("Codex App Server returned invalid access configuration");
  }
  if (options !== null && !isRecord(options)) {
    throw new Error("Codex App Server returned invalid access configuration");
  }
  const configured = options ?? {};
  if (!isRecord(configured)) {
    throw new Error("Codex App Server returned invalid access configuration");
  }
  const writableRoots = configured.writable_roots ?? [];
  if (
    !Array.isArray(writableRoots)
    || !writableRoots.every((root) => typeof root === "string")
    || (
      configured.network_access !== undefined
      && typeof configured.network_access !== "boolean"
    )
    || (
      configured.exclude_slash_tmp !== undefined
      && typeof configured.exclude_slash_tmp !== "boolean"
    )
    || (
      configured.exclude_tmpdir_env_var !== undefined
      && typeof configured.exclude_tmpdir_env_var !== "boolean"
    )
  ) {
    throw new Error("Codex App Server returned invalid access configuration");
  }
  return {
    excludeSlashTmp: configured.exclude_slash_tmp ?? false,
    excludeTmpdirEnvVar: configured.exclude_tmpdir_env_var ?? false,
    networkAccess: configured.network_access ?? false,
    type: "workspaceWrite",
    writableRoots,
  };
}

function parseUsageLimitExceeded(value: unknown): UsageLimitExceeded | undefined {
  if (
    !isRecord(value)
    || typeof value.threadId !== "string"
    || typeof value.turnId !== "string"
    || !isRecord(value.error)
    || value.error.codexErrorInfo !== "usageLimitExceeded"
  ) {
    return undefined;
  }
  return { threadId: value.threadId, turnId: value.turnId };
}

function parseAccountRateLimits(value: unknown): AccountRateLimits {
  if (!isRecord(value)) {
    throw new Error("Codex App Server returned invalid rate limits");
  }
  const rateLimits = value.rateLimits === null
    ? null
    : parseRateLimitSnapshot(value.rateLimits);
  let rateLimitsByLimitId: Record<string, RateLimitSnapshot> | null = null;
  if (value.rateLimitsByLimitId !== null && value.rateLimitsByLimitId !== undefined) {
    if (!isRecord(value.rateLimitsByLimitId)) {
      throw new Error("Codex App Server returned invalid rate limits");
    }
    rateLimitsByLimitId = {};
    for (const [limitId, snapshot] of Object.entries(value.rateLimitsByLimitId)) {
      rateLimitsByLimitId[limitId] = parseRateLimitSnapshot(snapshot);
    }
  }
  return { rateLimits, rateLimitsByLimitId };
}

function parseRateLimitSnapshot(value: unknown): RateLimitSnapshot {
  if (!isRecord(value)) {
    throw new Error("Codex App Server returned invalid rate limits");
  }
  return {
    limitId: typeof value.limitId === "string" ? value.limitId : null,
    primary: parseRateLimitWindow(value.primary),
    rateLimitReachedType: typeof value.rateLimitReachedType === "string"
      ? value.rateLimitReachedType
      : null,
    secondary: parseRateLimitWindow(value.secondary),
  };
}

function parseRateLimitWindow(
  value: unknown,
): RateLimitSnapshot["primary"] {
  if (value === null || value === undefined) return null;
  if (!isRecord(value) || typeof value.usedPercent !== "number") {
    throw new Error("Codex App Server returned invalid rate limits");
  }
  return {
    resetsAt: typeof value.resetsAt === "number" ? value.resetsAt : null,
    usedPercent: value.usedPercent,
  };
}

function parseCompletedTurn(value: unknown): CompletedTurn | undefined {
  if (!isRecord(value) || typeof value.threadId !== "string" || !isRecord(value.turn)) {
    return undefined;
  }
  const { id, status } = value.turn;
  if (typeof id !== "string") return undefined;
  if (status !== "completed" && status !== "failed" && status !== "interrupted") {
    return undefined;
  }
  const error = parseTurnError(value.turn.error);
  return {
    threadId: value.threadId,
    turnId: id,
    status,
    ...(error ? { error } : {}),
  };
}

function parseStartedTurn(value: unknown): StartedTurn | undefined {
  if (
    !isRecord(value)
    || typeof value.threadId !== "string"
    || !isRecord(value.turn)
    || typeof value.turn.id !== "string"
  ) return undefined;
  return { threadId: value.threadId, turnId: value.turn.id };
}

function parseTurnError(value: unknown): CompletedTurn["error"] {
  if (!isRecord(value) || typeof value.message !== "string") return undefined;
  return {
    message: value.message,
    ...(value.codexErrorInfo === null || value.codexErrorInfo === undefined
      ? {}
      : { codexErrorInfo: value.codexErrorInfo }),
  };
}

function parseUnattendedRequest(value: Record<string, unknown>):
  UnattendedRequest | undefined {
  if (!isRecord(value.params)) return undefined;
  const { threadId, turnId } = value.params;
  if (typeof threadId !== "string") return undefined;
  const kinds: Record<string, UnattendedRequestKind> = {
    "item/commandExecution/requestApproval": "command_approval",
    "item/fileChange/requestApproval": "file_change_approval",
    "item/permissions/requestApproval": "permission_request",
    "item/tool/requestUserInput": "user_input",
    "mcpServer/elicitation/request": "mcp_elicitation",
  };
  const kind = typeof value.method === "string" ? kinds[value.method] : undefined;
  return kind
    ? {
      kind,
      threadId,
      ...(typeof turnId === "string" ? { turnId } : {}),
    }
    : undefined;
}

function safeUnattendedResponse(kind: UnattendedRequestKind): unknown {
  switch (kind) {
    case "command_approval":
    case "file_change_approval":
      return { decision: "cancel" };
    case "permission_request":
      return { permissions: {} };
    case "user_input":
      return { answers: {} };
    case "mcp_elicitation":
      return { action: "cancel" };
  }
}

function incompatible(
  codexVersion: string,
  missingCapabilities: string[],
  message: string,
): Extract<AppServerProbeResult, { state: "incompatible" }> {
  return { state: "incompatible", codexVersion, missingCapabilities, message };
}

function unauthenticated(
  codexVersion: string,
): Extract<AppServerProbeResult, { state: "unauthenticated" }> {
  return {
    state: "unauthenticated",
    codexVersion,
    message: "ChatGPT authentication required; run `codex login`.",
  };
}

function isAuthenticationError(error: unknown): boolean {
  return error instanceof AppServerRpcError
    && (error.rpcCode === 401 || containsLiteral(error.data, "unauthorized"));
}

function threadResponseContract(
  method: "start" | "resume" | "read",
  file: string,
): SchemaFileContract {
  return {
    capability: `payload:thread/${method}-response`,
    file,
    objects: [
      { properties: ["thread"], required: ["thread"] },
      {
        definition: "Thread",
        properties: ["id", "cwd", "status", "turns"],
        required: ["id", "cwd", "status", "turns"],
      },
      {
        definition: "Turn",
        properties: ["id", "status", "error"],
        required: ["id", "status"],
      },
    ],
  };
}

function turnContract(
  method: "start" | "started" | "completed",
  file: string,
  includesThreadId: boolean,
): SchemaFileContract {
  return {
    capability: `payload:turn/${method}`,
    file,
    objects: [
      {
        properties: includesThreadId ? ["threadId", "turn"] : ["turn"],
        required: includesThreadId ? ["threadId", "turn"] : ["turn"],
      },
      {
        definition: "Turn",
        properties: ["id", "status", "error"],
        required: ["id", "status"],
      },
      { definition: "TurnStatus", literals: requiredTurnStatuses },
    ],
  };
}

function approvalContract(
  method: string,
  paramsFile: string,
  responseFile: string,
  responseProperty: string,
  responseLiterals: readonly string[] = [],
): readonly SchemaFileContract[] {
  return [
    {
      capability: `payload:${method}`,
      file: paramsFile,
      objects: [{
        properties: ["itemId", "threadId", "turnId"],
        required: ["itemId", "threadId", "turnId"],
      }],
    },
    {
      capability: `payload:${method}-response`,
      file: responseFile,
      objects: [{
        properties: [responseProperty],
        required: [responseProperty],
        literals: responseLiterals,
      }],
    },
  ];
}

async function missingSchemaContracts(schemaDirectory: string): Promise<string[]> {
  const results = await Promise.all(schemaFileContracts.map(async (contract) => {
    try {
      const schema = await readJson(path.join(schemaDirectory, contract.file));
      return contract.objects.every((objectContract) =>
        matchesObjectContract(schema, objectContract)
      ) ? undefined : contract.capability;
    } catch {
      return contract.capability;
    }
  }));
  return results.filter((result): result is string => result !== undefined);
}

function matchesObjectContract(
  schema: unknown,
  contract: SchemaObjectContract,
): boolean {
  let target = schema;
  if (contract.definition) {
    if (!isRecord(schema) || !isRecord(schema.definitions)) return false;
    target = schema.definitions[contract.definition];
  }
  if (!isRecord(target)) return false;

  if (contract.properties) {
    const properties = target.properties;
    if (!isRecord(properties)) return false;
    if (!contract.properties.every((property) => property in properties)) return false;
  }
  if (contract.required) {
    const required = target.required;
    if (!Array.isArray(required)) return false;
    if (!contract.required.every((property) => required.includes(property))) return false;
  }
  return !contract.literals
    || contract.literals.every((literal) => containsLiteral(target, literal));
}

function hasChatGptAccount(result: unknown): boolean {
  if (!isRecord(result) || !isRecord(result.account)) return false;
  return result.account.type === "chatgpt";
}

async function readJson(filePath: string): Promise<unknown> {
  return JSON.parse(await readFile(filePath, "utf8")) as unknown;
}

function collectMethods(value: unknown, methods = new Set<string>()): Set<string> {
  if (Array.isArray(value)) {
    for (const item of value) collectMethods(item, methods);
    return methods;
  }
  if (!isRecord(value)) return methods;

  const method = value.method;
  if (isRecord(method) && Array.isArray(method.enum)) {
    for (const item of method.enum) {
      if (typeof item === "string") methods.add(item);
    }
  }
  for (const child of Object.values(value)) collectMethods(child, methods);
  return methods;
}

function containsLiteral(value: unknown, expected: string): boolean {
  if (value === expected) return true;
  if (Array.isArray(value)) {
    return value.some((item) => containsLiteral(item, expected));
  }
  if (isRecord(value)) {
    return Object.values(value).some((item) => containsLiteral(item, expected));
  }
  return false;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
