import { execFile, spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { readFile, rm, mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import readline, { type Interface as ReadLineInterface } from "node:readline";
import { promisify } from "node:util";

import type {
  AppServerController,
  AppServerProbeResult,
} from "./daemon.js";

const execFileAsync = promisify(execFile);

const requiredClientRequests = [
  "initialize",
  "account/read",
  "account/rateLimits/read",
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

interface CodexAppServerOptions {
  command?: string;
  env?: NodeJS.ProcessEnv;
  requestTimeoutMs?: number;
}

interface PendingRequest {
  reject(error: Error): void;
  resolve(value: unknown): void;
  timeout: NodeJS.Timeout;
}

export class CodexAppServer implements AppServerController {
  readonly #command: string;
  readonly #env: NodeJS.ProcessEnv;
  readonly #requestTimeoutMs: number;
  #child: ChildProcessWithoutNullStreams | undefined;
  #lineReader: ReadLineInterface | undefined;
  #nextRequestId = 0;
  #pending = new Map<number, PendingRequest>();
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

      const accountResult = await this.#request("account/read", {
        refreshToken: false,
      });
      if (!hasChatGptAccount(accountResult)) {
        return {
          state: "unauthenticated",
          codexVersion,
          message: "ChatGPT authentication required; run `codex login`.",
        };
      }

      await this.#request("account/rateLimits/read");
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

      const [clientSchema, notificationSchema, requestSchema, errorSchema, turnSchema] =
        await Promise.all([
          readJson(path.join(schemaDirectory, "ClientRequest.json")),
          readJson(path.join(schemaDirectory, "ServerNotification.json")),
          readJson(path.join(schemaDirectory, "ServerRequest.json")),
          readJson(path.join(schemaDirectory, "v2", "ErrorNotification.json")),
          readJson(path.join(schemaDirectory, "v2", "TurnCompletedNotification.json")),
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
      if (!containsLiteral(errorSchema, "usageLimitExceeded")) {
        missing.push("error-code:usageLimitExceeded");
      }
      for (const status of requiredTurnStatuses) {
        if (!containsLiteral(turnSchema, status)) missing.push(`turn-status:${status}`);
      }
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
    child.once("exit", () => {
      this.#rejectPending(new Error("Codex App Server exited during startup"));
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
      this.#pending.set(id, { resolve, reject, timeout });
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
    if (!isRecord(message) || typeof message.id !== "number") return;
    const pending = this.#pending.get(message.id);
    if (!pending) return;
    clearTimeout(pending.timeout);
    this.#pending.delete(message.id);
    if (message.error !== undefined) {
      pending.reject(new Error("Codex App Server rejected a startup request"));
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
}

function incompatible(
  codexVersion: string,
  missingCapabilities: string[],
  message: string,
): Extract<AppServerProbeResult, { state: "incompatible" }> {
  return { state: "incompatible", codexVersion, missingCapabilities, message };
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
