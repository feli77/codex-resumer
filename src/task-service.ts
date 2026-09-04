import { constants as fsConstants } from "node:fs";
import { access, realpath, stat } from "node:fs/promises";
import path from "node:path";

import type { AppServerController, Clock, CompletedTurn } from "./daemon.js";
import { StateStore, type QueueSnapshot, type QueuedTask } from "./state-store.js";

export class TaskService {
  #completionBeforeTurnStart: CompletedTurn | undefined;

  constructor(
    private readonly appServer: AppServerController,
    private readonly store: StateStore,
    private readonly clock: Clock,
  ) {}

  async add(workspace: string, prompt: string): Promise<{ taskId: number }> {
    if (prompt.trim().length === 0) throw new Error("Task prompt must not be empty.");
    const normalizedWorkspace = await accessibleWorkspace(workspace);
    return {
      taskId: this.store.addWorkspaceTask(
        normalizedWorkspace,
        prompt,
        this.clock.now(),
      ),
    };
  }

  async start(): Promise<{ state: "started" | "already-running" | "idle" }> {
    const next = this.store.startNextTask(this.clock.now());
    if (next.kind !== "started") return { state: next.kind };
    try {
      await accessibleWorkspace(next.task.workspace);
    } catch (error) {
      this.store.releaseTaskBeforeDispatch(next.task.id, this.clock.now());
      throw error;
    }
    await this.dispatch(next.task);
    return { state: "started" };
  }

  snapshot(): QueueSnapshot {
    return this.store.snapshot();
  }

  handleTurnCompleted(turn: CompletedTurn): void {
    if (turn.status !== "completed") return;
    if (!this.store.completeTurn(turn.threadId, turn.turnId, this.clock.now())) {
      this.#completionBeforeTurnStart = turn;
    }
  }

  async dispatch(task: QueuedTask): Promise<void> {
    const { threadId } = await this.appServer.startThread(task.workspace);
    this.store.recordManagedThread(task, threadId, this.clock.now());
    const { turnId } = await this.appServer.startTurn(threadId, task.prompt);
    this.store.recordTurnStarted(task.id, threadId, turnId, this.clock.now());
    const completion = this.#completionBeforeTurnStart;
    if (completion?.threadId === threadId && completion.turnId === turnId) {
      this.#completionBeforeTurnStart = undefined;
      this.store.completeTurn(threadId, turnId, this.clock.now());
    }
  }
}

export async function handleTaskRequest(
  request: Record<string, unknown>,
  taskService: TaskService,
): Promise<unknown> {
  switch (request.method) {
    case "task/add": {
      if (
        !isRecord(request.params)
        || typeof request.params.workspace !== "string"
        || typeof request.params.prompt !== "string"
      ) {
        throw new Error("task/add requires a Workspace and prompt.");
      }
      return taskService.add(request.params.workspace, request.params.prompt);
    }
    case "queue/start":
      return taskService.start();
    case "queue/status":
      return taskService.snapshot();
    default:
      throw new Error("unknown daemon request");
  }
}

async function accessibleWorkspace(workspace: string): Promise<string> {
  const absoluteWorkspace = path.resolve(workspace);
  try {
    const canonicalWorkspace = await realpath(absoluteWorkspace);
    const metadata = await stat(canonicalWorkspace);
    if (!metadata.isDirectory()) throw new Error("not a directory");
    await access(
      canonicalWorkspace,
      fsConstants.R_OK | fsConstants.W_OK | fsConstants.X_OK,
    );
    return canonicalWorkspace;
  } catch {
    throw new Error(`Workspace is not an accessible directory: ${absoluteWorkspace}`);
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
