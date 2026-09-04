import { constants as fsConstants } from "node:fs";
import { access, realpath, stat } from "node:fs/promises";
import path from "node:path";

import type { AppServerController, Clock, CompletedTurn } from "./daemon.js";
import { StateStore, type QueueSnapshot, type QueuedTask } from "./state-store.js";

export class TaskService {
  #automaticAdvance: Promise<void> = Promise.resolve();
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

  addToManagedThread(threadId: string, prompt: string): { taskId: number } {
    if (prompt.trim().length === 0) throw new Error("Task prompt must not be empty.");
    return {
      taskId: this.store.addManagedThreadTask(threadId, prompt, this.clock.now()),
    };
  }

  async importThread(
    threadId: string,
  ): Promise<{ threadId: string; workspace: string }> {
    if (threadId.trim().length === 0) throw new Error("Thread ID must not be empty.");
    const thread = await this.appServer.readThread(threadId);
    const workspace = await accessibleWorkspace(thread.workspace);
    this.store.importManagedThread(thread.threadId, workspace, this.clock.now());
    return { threadId: thread.threadId, workspace };
  }

  async start(): Promise<{ state: "started" | "already-running" | "idle" }> {
    return { state: await this.#startNextTask() };
  }

  async #startNextTask(): Promise<"started" | "already-running" | "idle"> {
    const next = this.store.startNextTask(this.clock.now());
    if (next.kind !== "started") return next.kind;
    try {
      await accessibleWorkspace(next.task.workspace);
      await this.dispatch(next.task);
    } catch (error) {
      this.store.releaseTaskBeforeDispatch(next.task.id, this.clock.now());
      throw error;
    }
    return "started";
  }

  snapshot(): QueueSnapshot {
    return this.store.snapshot();
  }

  move(
    taskId: number,
    relativeTaskId: number,
    placement: "after" | "before",
  ): { taskId: number } {
    this.store.moveQueuedTask(taskId, relativeTaskId, placement);
    return { taskId };
  }

  cancel(taskId: number): { taskId: number } {
    this.store.cancelQueuedTask(taskId, this.clock.now());
    return { taskId };
  }

  handleTurnCompleted(turn: CompletedTurn): void {
    if (turn.status !== "completed") return;
    if (!this.store.completeTurn(turn.threadId, turn.turnId, this.clock.now())) {
      this.#completionBeforeTurnStart = turn;
    } else {
      this.#scheduleAutomaticAdvance();
    }
  }

  async dispatch(task: QueuedTask): Promise<void> {
    const { threadId } = task.managedThreadId
      ? await this.appServer.resumeThread(task.managedThreadId)
      : await this.appServer.startThread(task.workspace);
    if (task.managedThreadId) {
      this.store.activateManagedThread(threadId);
    } else {
      this.store.recordManagedThread(task, threadId, this.clock.now());
    }
    const { turnId } = await this.appServer.startTurn(threadId, task.prompt);
    this.store.recordTurnStarted(task.id, threadId, turnId, this.clock.now());
    const completion = this.#completionBeforeTurnStart;
    if (completion?.threadId === threadId && completion.turnId === turnId) {
      this.#completionBeforeTurnStart = undefined;
      this.store.completeTurn(threadId, turnId, this.clock.now());
      this.#scheduleAutomaticAdvance();
    }
  }

  #scheduleAutomaticAdvance(): void {
    this.#automaticAdvance = this.#automaticAdvance
      .then(async () => {
        try {
          await this.#startNextTask();
        } catch {
          // The shared start operation pauses the Queue and restores the Task.
        }
      })
      .catch(() => undefined);
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
        || typeof request.params.prompt !== "string"
      ) {
        throw new Error("task/add requires one target and a prompt.");
      }
      const hasWorkspace = typeof request.params.workspace === "string";
      const hasThread = typeof request.params.threadId === "string";
      if (hasWorkspace === hasThread) {
        throw new Error("task/add requires exactly one Managed Thread or Workspace.");
      }
      return hasWorkspace
        ? taskService.add(request.params.workspace as string, request.params.prompt)
        : taskService.addToManagedThread(
          request.params.threadId as string,
          request.params.prompt,
        );
    }
    case "queue/start":
      return taskService.start();
    case "queue/status":
      return taskService.snapshot();
    case "task/move": {
      if (
        !isRecord(request.params)
        || !isTaskId(request.params.taskId)
        || !isTaskId(request.params.relativeTaskId)
        || (request.params.placement !== "after" && request.params.placement !== "before")
      ) {
        throw new Error("task/move requires two Task IDs and a placement.");
      }
      return taskService.move(
        request.params.taskId,
        request.params.relativeTaskId,
        request.params.placement,
      );
    }
    case "task/cancel": {
      if (!isRecord(request.params) || !isTaskId(request.params.taskId)) {
        throw new Error("task/cancel requires a Task ID.");
      }
      return taskService.cancel(request.params.taskId);
    }
    case "thread/import": {
      if (!isRecord(request.params) || typeof request.params.threadId !== "string") {
        throw new Error("thread/import requires a Thread ID.");
      }
      return taskService.importThread(request.params.threadId);
    }
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

function isTaskId(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}
