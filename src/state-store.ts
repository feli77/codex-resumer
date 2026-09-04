import { chmodSync } from "node:fs";

import Database from "better-sqlite3";

export type QueueState = "paused" | "running" | "idle";
export type TaskState =
  | "queued"
  | "running"
  | "waiting_for_quota"
  | "completed"
  | "cancelled";

export interface QueuedTask {
  id: number;
  managedThreadId: string | undefined;
  prompt: string;
  workspace: string;
}

export interface TaskSummary {
  activeTurnId: string | undefined;
  id: number;
  managedThreadId: string | undefined;
  quotaLimitId?: string;
  quotaLimitType?: string;
  quotaResetAt?: string;
  state: TaskState;
  workspace: string;
}

export interface QuotaWait {
  limitId: string | undefined;
  limitType: string | undefined;
  pollAttempt: number;
  resetAt: Date | undefined;
  taskId: number;
  threadId: string;
  turnId: string;
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
  quota_limit_id: string | null;
  quota_limit_type: string | null;
  quota_reset_at: string | null;
  state: TaskState;
  workspace: string;
}

interface OrderedTaskRow {
  id: number;
  state: TaskState;
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
        state TEXT NOT NULL CHECK (
          state IN ('queued', 'running', 'waiting_for_quota', 'completed', 'cancelled')
        ),
        managed_thread_id TEXT,
        active_turn_id TEXT,
        quota_limit_id TEXT,
        quota_limit_type TEXT,
        quota_reset_at TEXT,
        quota_poll_attempt INTEGER NOT NULL DEFAULT 0,
        queue_position INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        started_at TEXT,
        completed_at TEXT,
        cancelled_at TEXT
      );

      CREATE UNIQUE INDEX IF NOT EXISTS one_active_task
        ON tasks((1)) WHERE state IN ('running', 'waiting_for_quota');

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
        state TEXT NOT NULL CHECK (state IN ('in_progress', 'quota_paused', 'completed')),
        started_at TEXT NOT NULL,
        completed_at TEXT
      );
    `);
    this.#migrateTaskQueue();
    this.#migrateQuotaPause();
    this.#database.prepare(`
      INSERT OR IGNORE INTO queue (id, state, updated_at)
      VALUES (1, 'paused', ?)
    `).run(new Date().toISOString());
  }

  addWorkspaceTask(workspace: string, prompt: string, now: Date): number {
    const result = this.#database.prepare(`
      INSERT INTO tasks (workspace, prompt, state, queue_position, created_at)
      VALUES (?, ?, 'queued', (SELECT COALESCE(MAX(queue_position), 0) + 1 FROM tasks), ?)
    `).run(workspace, prompt, now.toISOString());
    return Number(result.lastInsertRowid);
  }

  addManagedThreadTask(threadId: string, prompt: string, now: Date): number {
    const thread = this.#database.prepare(
      "SELECT workspace FROM managed_threads WHERE id = ?",
    ).get(threadId) as { workspace: string } | undefined;
    if (!thread) throw new Error(`Managed Thread is not imported: ${threadId}`);
    const result = this.#database.prepare(`
      INSERT INTO tasks (
        workspace, prompt, state, managed_thread_id, queue_position, created_at
      )
      VALUES (
        ?, ?, 'queued', ?,
        (SELECT COALESCE(MAX(queue_position), 0) + 1 FROM tasks), ?
      )
    `).run(thread.workspace, prompt, threadId, now.toISOString());
    return Number(result.lastInsertRowid);
  }

  importManagedThread(threadId: string, workspace: string, now: Date): void {
    const existing = this.#database.prepare(
      "SELECT workspace FROM managed_threads WHERE id = ?",
    ).get(threadId) as { workspace: string } | undefined;
    if (existing) {
      if (existing.workspace !== workspace) {
        throw new Error(
          `Managed Thread ${threadId} is already bound to Workspace ${existing.workspace}.`,
        );
      }
      return;
    }
    this.#database.prepare(`
      INSERT INTO managed_threads (id, workspace, state, created_at)
      VALUES (?, ?, 'idle', ?)
    `).run(threadId, workspace, now.toISOString());
  }

  startNextTask(now: Date):
    | { kind: "started"; task: QueuedTask }
    | { kind: "already-running" }
    | { kind: "idle" } {
    return this.#database.transaction(() => {
      const running = this.#database.prepare(
        "SELECT id FROM tasks WHERE state IN ('running', 'waiting_for_quota')",
      ).get();
      if (running) return { kind: "already-running" } as const;

      this.#database.prepare(`
        UPDATE queue SET state = 'running', updated_at = ? WHERE id = 1
      `).run(now.toISOString());
      const task = this.#database.prepare(`
        SELECT id, workspace, prompt, managed_thread_id AS managedThreadId FROM tasks
        WHERE state = 'queued' ORDER BY queue_position, id LIMIT 1
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

  activateManagedThread(threadId: string): void {
    const result = this.#database.prepare(`
      UPDATE managed_threads SET state = 'active' WHERE id = ?
    `).run(threadId);
    if (result.changes !== 1) {
      throw new Error(`Managed Thread is not imported: ${threadId}`);
    }
  }

  moveQueuedTask(
    taskId: number,
    relativeTaskId: number,
    placement: "after" | "before",
  ): void {
    this.#database.transaction(() => {
      const tasks = this.#database.prepare(`
        SELECT id, state FROM tasks ORDER BY queue_position, id
      `).all() as OrderedTaskRow[];
      const task = tasks.find((candidate) => candidate.id === taskId);
      if (!task) throw new Error(`Task does not exist: ${taskId}`);
      if (task.state !== "queued") throw new Error(`Task ${taskId} is not queued.`);
      if (!tasks.some((candidate) => candidate.id === relativeTaskId)) {
        throw new Error(`Task does not exist: ${relativeTaskId}`);
      }
      if (taskId === relativeTaskId) return;

      const reordered = tasks.filter((candidate) => candidate.id !== taskId);
      const relativeIndex = reordered.findIndex(
        (candidate) => candidate.id === relativeTaskId,
      );
      reordered.splice(relativeIndex + (placement === "after" ? 1 : 0), 0, task);
      const activeIndex = reordered.findIndex(
        (candidate) => candidate.state === "running" || candidate.state === "waiting_for_quota",
      );
      const movedIndex = reordered.findIndex((candidate) => candidate.id === taskId);
      if (activeIndex !== -1 && movedIndex < activeIndex) {
        throw new Error(`Task ${taskId} cannot move before the active Task.`);
      }

      const update = this.#database.prepare(
        "UPDATE tasks SET queue_position = ? WHERE id = ?",
      );
      for (const [index, candidate] of reordered.entries()) {
        update.run(index + 1, candidate.id);
      }
    })();
  }

  cancelQueuedTask(taskId: number, now: Date): void {
    const result = this.#database.prepare(`
      UPDATE tasks
      SET state = 'cancelled', prompt = NULL, cancelled_at = ?
      WHERE id = ? AND state = 'queued'
    `).run(now.toISOString(), taskId);
    if (result.changes === 1) return;
    const task = this.#database.prepare("SELECT state FROM tasks WHERE id = ?")
      .get(taskId) as { state: TaskState } | undefined;
    if (!task) throw new Error(`Task does not exist: ${taskId}`);
    throw new Error(`Task ${taskId} is not queued.`);
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

  hasActiveTurn(managedThreadId: string, turnId: string): boolean {
    return this.#database.prepare(`
      SELECT 1
      FROM tasks
      WHERE managed_thread_id = ? AND active_turn_id = ? AND state = 'running'
    `).get(managedThreadId, turnId) !== undefined;
  }

  recordQuotaPause(
    managedThreadId: string,
    turnId: string,
    quota: {
      limitId: string | undefined;
      limitType: string | undefined;
      resetAt: Date | undefined;
    },
  ): QuotaWait | undefined {
    return this.#database.transaction(() => {
      const task = this.#database.prepare(`
        SELECT id FROM tasks
        WHERE managed_thread_id = ? AND active_turn_id = ? AND state = 'running'
      `).get(managedThreadId, turnId) as { id: number } | undefined;
      if (!task) return undefined;

      this.#database.prepare(`
        UPDATE turns SET state = 'quota_paused'
        WHERE id = ? AND managed_thread_id = ? AND state = 'in_progress'
      `).run(turnId, managedThreadId);
      this.#database.prepare(`
        UPDATE tasks
        SET state = 'waiting_for_quota', quota_limit_id = ?, quota_limit_type = ?,
            quota_reset_at = ?, quota_poll_attempt = 0
        WHERE id = ? AND state = 'running'
      `).run(
        quota.limitId ?? null,
        quota.limitType ?? null,
        quota.resetAt?.toISOString() ?? null,
        task.id,
      );
      return {
        limitId: quota.limitId,
        limitType: quota.limitType,
        pollAttempt: 0,
        resetAt: quota.resetAt,
        taskId: task.id,
        threadId: managedThreadId,
        turnId,
      };
    })();
  }

  quotaWait(taskId: number): QuotaWait | undefined {
    const row = this.#database.prepare(`
      SELECT id, managed_thread_id, active_turn_id, quota_limit_id,
             quota_limit_type, quota_reset_at, quota_poll_attempt
      FROM tasks WHERE id = ? AND state = 'waiting_for_quota'
    `).get(taskId) as {
      active_turn_id: string;
      id: number;
      managed_thread_id: string;
      quota_limit_id: string | null;
      quota_limit_type: string | null;
      quota_poll_attempt: number;
      quota_reset_at: string | null;
    } | undefined;
    if (!row) return undefined;
    return {
      limitId: row.quota_limit_id ?? undefined,
      limitType: row.quota_limit_type ?? undefined,
      pollAttempt: row.quota_poll_attempt,
      resetAt: row.quota_reset_at ? new Date(row.quota_reset_at) : undefined,
      taskId: row.id,
      threadId: row.managed_thread_id,
      turnId: row.active_turn_id,
    };
  }

  updateQuotaWait(
    taskId: number,
    quota: {
      limitId: string | undefined;
      limitType: string | undefined;
      pollAttempt: number;
      resetAt: Date | undefined;
    },
  ): QuotaWait | undefined {
    const result = this.#database.prepare(`
      UPDATE tasks
      SET quota_limit_id = ?, quota_limit_type = ?, quota_reset_at = ?,
          quota_poll_attempt = ?
      WHERE id = ? AND state = 'waiting_for_quota'
    `).run(
      quota.limitId ?? null,
      quota.limitType ?? null,
      quota.resetAt?.toISOString() ?? null,
      quota.pollAttempt,
      taskId,
    );
    return result.changes === 1 ? this.quotaWait(taskId) : undefined;
  }

  isQueueRunning(): boolean {
    return this.#database.prepare(
      "SELECT 1 FROM queue WHERE id = 1 AND state = 'running'",
    ).get() !== undefined;
  }

  recordContinuationTurnStarted(
    taskId: number,
    managedThreadId: string,
    turnId: string,
    now: Date,
  ): boolean {
    return this.#database.transaction(() => {
      const task = this.#database.prepare(`
        SELECT id FROM tasks
        WHERE id = ? AND managed_thread_id = ? AND state = 'waiting_for_quota'
      `).get(taskId, managedThreadId);
      if (!task) return false;
      this.#database.prepare(`
        INSERT INTO turns (id, task_id, managed_thread_id, state, started_at)
        VALUES (?, ?, ?, 'in_progress', ?)
      `).run(turnId, taskId, managedThreadId, now.toISOString());
      this.#database.prepare(`
        UPDATE tasks
        SET state = 'running', active_turn_id = ?, quota_limit_id = NULL,
            quota_limit_type = NULL, quota_reset_at = NULL, quota_poll_attempt = 0
        WHERE id = ? AND state = 'waiting_for_quota'
      `).run(turnId, taskId);
      return true;
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
        UPDATE tasks
        SET state = 'completed', prompt = NULL, completed_at = ?,
            quota_limit_id = NULL, quota_limit_type = NULL,
            quota_reset_at = NULL, quota_poll_attempt = 0
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
      SELECT id, workspace, state, managed_thread_id, active_turn_id, prompt,
             quota_limit_id, quota_limit_type, quota_reset_at
      FROM tasks ORDER BY queue_position, id
    `).all() as TaskRow[];
    const hasUnfinishedTask = tasks.some(
      (task) =>
        task.state === "queued"
        || task.state === "running"
        || task.state === "waiting_for_quota",
    );
    return {
      state: queue.state === "running" && !hasUnfinishedTask ? "idle" : queue.state,
      tasks: tasks.map((task) => ({
        id: task.id,
        workspace: task.workspace,
        state: task.state,
        managedThreadId: task.managed_thread_id ?? undefined,
        activeTurnId: task.active_turn_id ?? undefined,
        ...(task.quota_limit_id === null ? {} : { quotaLimitId: task.quota_limit_id }),
        ...(task.quota_limit_type === null
          ? {}
          : { quotaLimitType: task.quota_limit_type }),
        ...(task.quota_reset_at === null ? {} : { quotaResetAt: task.quota_reset_at }),
      })),
    };
  }

  close(): void {
    this.#database.close();
  }

  #migrateTaskQueue(): void {
    const columns = this.#database.pragma("table_info(tasks)") as Array<{ name: string }>;
    if (columns.some((column) => column.name === "queue_position")) return;

    this.#database.pragma("foreign_keys = OFF");
    try {
      this.#database.transaction(() => {
        this.#database.exec(`
          DROP INDEX IF EXISTS one_running_task;
          ALTER TABLE turns RENAME TO turns_before_queue;
          ALTER TABLE tasks RENAME TO tasks_before_queue;

          CREATE TABLE tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace TEXT NOT NULL,
            prompt TEXT,
            state TEXT NOT NULL CHECK (
              state IN (
                'queued', 'running', 'waiting_for_quota', 'completed', 'cancelled'
              )
            ),
            managed_thread_id TEXT,
            active_turn_id TEXT,
            quota_limit_id TEXT,
            quota_limit_type TEXT,
            quota_reset_at TEXT,
            quota_poll_attempt INTEGER NOT NULL DEFAULT 0,
            queue_position INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            started_at TEXT,
            completed_at TEXT,
            cancelled_at TEXT
          );
          INSERT INTO tasks (
            id, workspace, prompt, state, managed_thread_id, active_turn_id,
            quota_limit_id, quota_limit_type, quota_reset_at, quota_poll_attempt,
            queue_position, created_at, started_at, completed_at, cancelled_at
          )
          SELECT
            id, workspace, prompt, state, managed_thread_id, active_turn_id,
            NULL, NULL, NULL, 0, id, created_at, started_at, completed_at, NULL
          FROM tasks_before_queue;

          CREATE TABLE turns (
            id TEXT PRIMARY KEY,
            task_id INTEGER NOT NULL REFERENCES tasks(id),
            managed_thread_id TEXT NOT NULL REFERENCES managed_threads(id),
            state TEXT NOT NULL CHECK (
              state IN ('in_progress', 'quota_paused', 'completed')
            ),
            started_at TEXT NOT NULL,
            completed_at TEXT
          );
          INSERT INTO turns (
            id, task_id, managed_thread_id, state, started_at, completed_at
          )
          SELECT id, task_id, managed_thread_id, state, started_at, completed_at
          FROM turns_before_queue;

          DROP TABLE turns_before_queue;
          DROP TABLE tasks_before_queue;
          CREATE UNIQUE INDEX one_active_task
            ON tasks((1)) WHERE state IN ('running', 'waiting_for_quota');
        `);
      })();
    } finally {
      this.#database.pragma("foreign_keys = ON");
    }
    const violations = this.#database.pragma("foreign_key_check") as unknown[];
    if (violations.length > 0) throw new Error("Task Queue migration failed.");
  }

  #migrateQuotaPause(): void {
    const columns = this.#database.pragma("table_info(tasks)") as Array<{ name: string }>;
    if (columns.some((column) => column.name === "quota_limit_id")) return;

    this.#database.pragma("foreign_keys = OFF");
    try {
      this.#database.transaction(() => {
        this.#database.exec(`
          DROP INDEX IF EXISTS one_running_task;
          DROP INDEX IF EXISTS one_active_task;
          ALTER TABLE turns RENAME TO turns_before_quota_pause;
          ALTER TABLE tasks RENAME TO tasks_before_quota_pause;

          CREATE TABLE tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace TEXT NOT NULL,
            prompt TEXT,
            state TEXT NOT NULL CHECK (
              state IN (
                'queued', 'running', 'waiting_for_quota', 'completed', 'cancelled'
              )
            ),
            managed_thread_id TEXT,
            active_turn_id TEXT,
            quota_limit_id TEXT,
            quota_limit_type TEXT,
            quota_reset_at TEXT,
            quota_poll_attempt INTEGER NOT NULL DEFAULT 0,
            queue_position INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            started_at TEXT,
            completed_at TEXT,
            cancelled_at TEXT
          );
          INSERT INTO tasks (
            id, workspace, prompt, state, managed_thread_id, active_turn_id,
            quota_limit_id, quota_limit_type, quota_reset_at, quota_poll_attempt,
            queue_position, created_at, started_at, completed_at, cancelled_at
          )
          SELECT
            id, workspace, prompt, state, managed_thread_id, active_turn_id,
            NULL, NULL, NULL, 0, queue_position, created_at, started_at,
            completed_at, cancelled_at
          FROM tasks_before_quota_pause;

          CREATE TABLE turns (
            id TEXT PRIMARY KEY,
            task_id INTEGER NOT NULL REFERENCES tasks(id),
            managed_thread_id TEXT NOT NULL REFERENCES managed_threads(id),
            state TEXT NOT NULL CHECK (
              state IN ('in_progress', 'quota_paused', 'completed')
            ),
            started_at TEXT NOT NULL,
            completed_at TEXT
          );
          INSERT INTO turns (
            id, task_id, managed_thread_id, state, started_at, completed_at
          )
          SELECT id, task_id, managed_thread_id, state, started_at, completed_at
          FROM turns_before_quota_pause;

          DROP TABLE turns_before_quota_pause;
          DROP TABLE tasks_before_quota_pause;
          CREATE UNIQUE INDEX one_active_task
            ON tasks((1)) WHERE state IN ('running', 'waiting_for_quota');
        `);
      })();
    } finally {
      this.#database.pragma("foreign_keys = ON");
    }
    const violations = this.#database.pragma("foreign_key_check") as unknown[];
    if (violations.length > 0) throw new Error("Quota Pause migration failed.");
  }
}
