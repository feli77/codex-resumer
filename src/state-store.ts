import { chmodSync } from "node:fs";

import Database from "better-sqlite3";

export type QueueState = "paused" | "running" | "idle";
export type TaskState = "queued" | "running" | "completed";

export interface QueuedTask {
  id: number;
  prompt: string;
  workspace: string;
}

export interface TaskSummary {
  activeTurnId: string | undefined;
  id: number;
  managedThreadId: string | undefined;
  state: TaskState;
  workspace: string;
}

export interface QueueSnapshot {
  state: QueueState;
  tasks: TaskSummary[];
}

interface TaskRow {
  active_turn_id: string | null;
  id: number;
  managed_thread_id: string | null;
  prompt: string | null;
  state: TaskState;
  workspace: string;
}

export class StateStore {
  readonly #database: Database.Database;

  constructor(databasePath: string) {
    this.#database = new Database(databasePath);
    chmodSync(databasePath, 0o600);
    this.#database.pragma("foreign_keys = ON");
    this.#database.exec(`
      CREATE TABLE IF NOT EXISTS queue (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        state TEXT NOT NULL CHECK (state IN ('paused', 'running')),
        updated_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        workspace TEXT NOT NULL,
        prompt TEXT,
        state TEXT NOT NULL CHECK (state IN ('queued', 'running', 'completed')),
        managed_thread_id TEXT,
        active_turn_id TEXT,
        created_at TEXT NOT NULL,
        started_at TEXT,
        completed_at TEXT
      );

      CREATE UNIQUE INDEX IF NOT EXISTS one_running_task
        ON tasks(state) WHERE state = 'running';

      CREATE TABLE IF NOT EXISTS managed_threads (
        id TEXT PRIMARY KEY,
        workspace TEXT NOT NULL,
        state TEXT NOT NULL CHECK (state IN ('active', 'idle')),
        created_at TEXT NOT NULL,
        last_turn_completed_at TEXT
      );

      CREATE TABLE IF NOT EXISTS turns (
        id TEXT PRIMARY KEY,
        task_id INTEGER NOT NULL REFERENCES tasks(id),
        managed_thread_id TEXT NOT NULL REFERENCES managed_threads(id),
        state TEXT NOT NULL CHECK (state IN ('in_progress', 'completed')),
        started_at TEXT NOT NULL,
        completed_at TEXT
      );
    `);
    this.#database.prepare(`
      INSERT OR IGNORE INTO queue (id, state, updated_at)
      VALUES (1, 'paused', ?)
    `).run(new Date().toISOString());
  }

  addWorkspaceTask(workspace: string, prompt: string, now: Date): number {
    const result = this.#database.prepare(`
      INSERT INTO tasks (workspace, prompt, state, created_at)
      VALUES (?, ?, 'queued', ?)
    `).run(workspace, prompt, now.toISOString());
    return Number(result.lastInsertRowid);
  }

  startNextTask(now: Date):
    | { kind: "started"; task: QueuedTask }
    | { kind: "already-running" }
    | { kind: "idle" } {
    return this.#database.transaction(() => {
      const running = this.#database.prepare(
        "SELECT id FROM tasks WHERE state = 'running'",
      ).get();
      if (running) return { kind: "already-running" } as const;

      this.#database.prepare(`
        UPDATE queue SET state = 'running', updated_at = ? WHERE id = 1
      `).run(now.toISOString());
      const task = this.#database.prepare(`
        SELECT id, workspace, prompt FROM tasks
        WHERE state = 'queued' ORDER BY id LIMIT 1
      `).get() as QueuedTask | undefined;
      if (!task) return { kind: "idle" } as const;

      this.#database.prepare(`
        UPDATE tasks SET state = 'running', started_at = ? WHERE id = ?
      `).run(now.toISOString(), task.id);
      return { kind: "started", task } as const;
    })();
  }

  releaseTaskBeforeDispatch(taskId: number, now: Date): void {
    this.#database.transaction(() => {
      this.#database.prepare(`
        UPDATE tasks SET state = 'queued', started_at = NULL
        WHERE id = ? AND state = 'running'
      `).run(taskId);
      this.#database.prepare(`
        UPDATE queue SET state = 'paused', updated_at = ? WHERE id = 1
      `).run(now.toISOString());
    })();
  }

  recordManagedThread(
    task: QueuedTask,
    managedThreadId: string,
    now: Date,
  ): void {
    this.#database.transaction(() => {
      this.#database.prepare(`
        INSERT INTO managed_threads (id, workspace, state, created_at)
        VALUES (?, ?, 'active', ?)
      `).run(managedThreadId, task.workspace, now.toISOString());
      this.#database.prepare(`
        UPDATE tasks SET managed_thread_id = ? WHERE id = ? AND state = 'running'
      `).run(managedThreadId, task.id);
    })();
  }

  recordTurnStarted(
    taskId: number,
    managedThreadId: string,
    turnId: string,
    now: Date,
  ): void {
    this.#database.transaction(() => {
      this.#database.prepare(`
        INSERT INTO turns (id, task_id, managed_thread_id, state, started_at)
        VALUES (?, ?, ?, 'in_progress', ?)
      `).run(turnId, taskId, managedThreadId, now.toISOString());
      this.#database.prepare(`
        UPDATE tasks SET active_turn_id = ? WHERE id = ? AND state = 'running'
      `).run(turnId, taskId);
    })();
  }

  completeTurn(managedThreadId: string, turnId: string, now: Date): boolean {
    return this.#database.transaction(() => {
      const turn = this.#database.prepare(`
        SELECT task_id FROM turns
        WHERE id = ? AND managed_thread_id = ? AND state = 'in_progress'
      `).get(turnId, managedThreadId) as { task_id: number } | undefined;
      if (!turn) return false;

      const completedAt = now.toISOString();
      this.#database.prepare(`
        UPDATE turns SET state = 'completed', completed_at = ? WHERE id = ?
      `).run(completedAt, turnId);
      this.#database.prepare(`
        UPDATE managed_threads
        SET state = 'idle', last_turn_completed_at = ? WHERE id = ?
      `).run(completedAt, managedThreadId);
      this.#database.prepare(`
        UPDATE tasks SET state = 'completed', prompt = NULL, completed_at = ?
        WHERE id = ? AND state = 'running'
      `).run(completedAt, turn.task_id);
      return true;
    })();
  }

  snapshot(): QueueSnapshot {
    const queue = this.#database.prepare(
      "SELECT state FROM queue WHERE id = 1",
    ).get() as { state: "paused" | "running" };
    const tasks = this.#database.prepare(`
      SELECT id, workspace, state, managed_thread_id, active_turn_id, prompt
      FROM tasks ORDER BY id
    `).all() as TaskRow[];
    const hasUnfinishedTask = tasks.some((task) => task.state !== "completed");
    return {
      state: queue.state === "running" && !hasUnfinishedTask ? "idle" : queue.state,
      tasks: tasks.map((task) => ({
        id: task.id,
        workspace: task.workspace,
        state: task.state,
        managedThreadId: task.managed_thread_id ?? undefined,
        activeTurnId: task.active_turn_id ?? undefined,
      })),
    };
  }

  close(): void {
    this.#database.close();
  }
}
