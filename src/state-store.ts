import { chmodSync } from "node:fs";

import Database from "better-sqlite3";

import type { AccessMode } from "./config.js";
import { EventLog } from "./event-log.js";
import type { RunPolicy } from "./run-policy.js";
import type { StructuredError } from "./structured-error.js";

export type QueueState = "paused" | "running" | "idle";
export type PauseReason =
  | "cutoff_reached"
  | "manual"
  | "needs_attention"
  | "not_started"
  | "run_policy_required";
export type TaskState =
  | "queued"
  | "running"
  | "waiting_for_quota"
  | "needs_attention"
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

export interface QuotaPauseDetails {
  limitId: string | undefined;
  limitType: string | undefined;
  resetAt: Date | undefined;
}

export interface QuotaPause extends QuotaPauseDetails {
  pollAttempt: number;
  taskId: number;
  threadId: string;
  turnId: string;
  workspace: string;
}

export interface TransientRetry {
  attempt: number;
  taskId: number;
  threadId: string;
  workspace: string;
}

export interface RecoveryTask {
  activeTurnId: string | undefined;
  id: number;
  managedThreadId: string | undefined;
  state: "running" | "waiting_for_quota";
}

export interface TaskCancellation {
  interrupt?: { threadId: string; turnId: string };
  taskId: number;
}

export interface QueueSnapshot {
  error?: StructuredError;
  pauseReason?: PauseReason;
  queueRun?: {
    accessMode: AccessMode;
    cutoffTime?: string;
    endedAt?: string;
    id: number;
    runPolicy: RunPolicy["kind"];
    startedAt: string;
  };
  state: QueueState;
  tasks: TaskSummary[];
}

interface QueueRunRow {
  access_mode: AccessMode;
  cutoff_time: string | null;
  ended_at: string | null;
  id: number;
  run_policy: RunPolicy["kind"];
  started_at: string;
}

type PersistedQueueRunState =
  | { kind: "idle" | "paused" }
  | { kind: "running"; queueRun: QueueRunRow };

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

function createTasksTable(ifMissing = false): string {
  return `
    CREATE TABLE ${ifMissing ? "IF NOT EXISTS " : ""}tasks (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      workspace TEXT NOT NULL,
      prompt TEXT,
      state TEXT NOT NULL CHECK (
        state IN (
          'queued', 'running', 'waiting_for_quota', 'needs_attention',
          'completed', 'cancelled'
        )
      ),
      managed_thread_id TEXT,
      active_turn_id TEXT,
      quota_limit_id TEXT,
      quota_limit_type TEXT,
      quota_reset_at TEXT,
      quota_poll_attempt INTEGER NOT NULL DEFAULT 0,
      transient_retry_count INTEGER NOT NULL DEFAULT 0,
      queue_position INTEGER NOT NULL,
      created_at TEXT NOT NULL,
      started_at TEXT,
      completed_at TEXT,
      cancelled_at TEXT
    );
  `;
}

function createTurnsTable(ifMissing = false): string {
  return `
    CREATE TABLE ${ifMissing ? "IF NOT EXISTS " : ""}turns (
      id TEXT PRIMARY KEY,
      task_id INTEGER NOT NULL REFERENCES tasks(id),
      managed_thread_id TEXT NOT NULL REFERENCES managed_threads(id),
      state TEXT NOT NULL CHECK (
        state IN ('in_progress', 'quota_paused', 'completed', 'failed', 'interrupted')
      ),
      started_at TEXT NOT NULL,
      completed_at TEXT
    );
  `;
}

function createActiveTaskIndex(ifMissing = false): string {
  return `
    CREATE UNIQUE INDEX ${ifMissing ? "IF NOT EXISTS " : ""}one_active_task
      ON tasks((1)) WHERE state IN ('running', 'waiting_for_quota');
  `;
}

export class StateStore {
  readonly #database: Database.Database;

  constructor(databasePath: string, eventLog?: EventLog) {
    this.#database = new Database(databasePath);
    chmodSync(databasePath, 0o600);
    this.#database.function(
      "codex_resumer_event",
      (payload: string) => {
        eventLog?.recordDatabaseEvent(payload);
        return 0;
      },
    );
    this.#database.pragma("foreign_keys = ON");
    this.#database.exec(`
      CREATE TABLE IF NOT EXISTS queue (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        state TEXT NOT NULL CHECK (state IN ('paused', 'running')),
        pause_reason TEXT,
        error_code TEXT,
        error_message TEXT,
        error_details TEXT,
        updated_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS queue_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_policy TEXT NOT NULL CHECK (run_policy IN ('until_idle', 'cutoff_time')),
        access_mode TEXT NOT NULL CHECK (access_mode IN ('configured', 'full')),
        cutoff_time TEXT,
        started_at TEXT NOT NULL,
        ended_at TEXT,
        CHECK (
          (run_policy = 'until_idle' AND cutoff_time IS NULL)
          OR (run_policy = 'cutoff_time' AND cutoff_time IS NOT NULL)
        )
      );

      ${createTasksTable(true)}
      ${createActiveTaskIndex(true)}

      CREATE TABLE IF NOT EXISTS managed_threads (
        id TEXT PRIMARY KEY,
        workspace TEXT NOT NULL,
        state TEXT NOT NULL CHECK (state IN ('active', 'idle')),
        created_at TEXT NOT NULL,
        last_turn_completed_at TEXT
      );

      ${createTurnsTable(true)}
    `);
    this.#migrateTaskQueue();
    this.#migrateQuotaPause();
    this.#migrateNeedsAttention();
    this.#migrateQueuePauseReason();
    this.#migrateQueueError();
    this.#migrateQueueRunAccessMode();
    this.#installEventLogTriggers();
    this.#database.prepare(`
      INSERT OR IGNORE INTO queue (id, state, pause_reason, updated_at)
      VALUES (1, 'paused', 'not_started', ?)
    `).run(new Date().toISOString());
  }

  startQueueRun(
    runPolicy: RunPolicy,
    accessMode: AccessMode,
    now: Date,
  ): "started" | "already-running" {
    return this.#database.transaction(() => {
      if (this.#queueRunState(now).kind === "running") {
        return "already-running" as const;
      }
      const unresolved = this.#database.prepare(`
        SELECT id FROM tasks
        WHERE state = 'needs_attention' ORDER BY queue_position, id LIMIT 1
      `).get() as { id: number } | undefined;
      if (unresolved) {
        throw new Error(
          `Resolve Task ${unresolved.id} before resuming the Queue.`,
        );
      }
      if (
        runPolicy.kind === "cutoff_time"
        && new Date(runPolicy.cutoffTime).getTime() <= now.getTime()
      ) {
        throw new Error("Cutoff Time must be in the future.");
      }

      this.#database.prepare(`
        INSERT INTO queue_runs (run_policy, access_mode, cutoff_time, started_at)
        VALUES (?, ?, ?, ?)
      `).run(
        runPolicy.kind,
        accessMode,
        runPolicy.kind === "cutoff_time" ? runPolicy.cutoffTime : null,
        now.toISOString(),
      );
      this.#database.prepare(`
        UPDATE queue
        SET state = 'running', pause_reason = NULL, error_code = NULL,
            error_message = NULL, error_details = NULL, updated_at = ?
        WHERE id = 1
      `).run(now.toISOString());
      return "started" as const;
    })();
  }

  restoreQueueRun(
    now: Date,
  ): { accessMode: AccessMode; runPolicy: RunPolicy } | undefined {
    return this.#database.transaction(() => {
      const state = this.#queueRunState(now);
      if (state.kind !== "running") return undefined;
      if (this.#markDeferredRetryNeedsAttention()) {
        this.#pauseQueue(
          "needs_attention",
          now,
          {
            code: "transient_retry_interrupted",
            message: "The daemon stopped while waiting to retry a transient failure.",
          },
        );
        return undefined;
      }
      const activeRun = state.queueRun;
      if (activeRun.cutoff_time === null) {
        return {
          accessMode: activeRun.access_mode,
          runPolicy: { kind: "until_idle" } as const,
        };
      }
      return {
        accessMode: activeRun.access_mode,
        runPolicy: {
          cutoffTime: activeRun.cutoff_time,
          kind: "cutoff_time",
        } as const,
      };
    })();
  }

  readRecoveryTask(): RecoveryTask | undefined {
    const task = this.#database.prepare(`
      SELECT id, state, managed_thread_id, active_turn_id
      FROM tasks
      WHERE state IN ('running', 'waiting_for_quota')
      LIMIT 1
    `).get() as {
      active_turn_id: string | null;
      id: number;
      managed_thread_id: string | null;
      state: "running" | "waiting_for_quota";
    } | undefined;
    return task
      ? {
        activeTurnId: task.active_turn_id ?? undefined,
        id: task.id,
        managedThreadId: task.managed_thread_id ?? undefined,
        state: task.state,
      }
      : undefined;
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
    | { kind: "idle" | "paused" } {
    return this.#database.transaction(() => {
      const state = this.#queueRunState(now);
      if (state.kind !== "running") return { kind: state.kind };
      const running = this.#database.prepare(
        "SELECT id FROM tasks WHERE state IN ('running', 'waiting_for_quota')",
      ).get();
      if (running) return { kind: "already-running" } as const;

      const task = this.#database.prepare(`
        SELECT id, workspace, prompt, managed_thread_id AS managedThreadId FROM tasks
        WHERE state = 'queued' ORDER BY queue_position, id LIMIT 1
      `).get() as QueuedTask | undefined;
      if (!task) {
        this.#endActiveQueueRun(now);
        return { kind: "idle" } as const;
      }

      this.#database.prepare(`
        UPDATE tasks SET state = 'running', started_at = ? WHERE id = ?
      `).run(now.toISOString(), task.id);
      return { kind: "started", task } as const;
    })();
  }

  releaseTaskBeforeDispatch(
    taskId: number,
    error: StructuredError,
    now: Date,
  ): void {
    this.#database.transaction(() => {
      this.#database.prepare(`
        UPDATE managed_threads SET state = 'idle'
        WHERE id = (
          SELECT managed_thread_id FROM tasks WHERE id = ? AND state = 'running'
        )
      `).run(taskId);
      this.#database.prepare(`
        UPDATE tasks SET state = 'needs_attention'
        WHERE id = ? AND state = 'running'
      `).run(taskId);
      this.#pauseQueue("needs_attention", now, error);
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

  cancelTask(taskId: number, now: Date): TaskCancellation {
    return this.#database.transaction(() => {
      const task = this.#database.prepare(`
        SELECT state, managed_thread_id, active_turn_id
        FROM tasks WHERE id = ?
      `).get(taskId) as {
        active_turn_id: string | null;
        managed_thread_id: string | null;
        state: TaskState;
      } | undefined;
      if (!task) throw new Error(`Task does not exist: ${taskId}`);
      if (
        task.state !== "queued"
        && task.state !== "running"
        && task.state !== "waiting_for_quota"
        && task.state !== "needs_attention"
      ) {
        throw new Error(`Task ${taskId} cannot be cancelled from ${task.state}.`);
      }

      this.#database.prepare(`
        UPDATE tasks SET state = 'cancelled', prompt = NULL, cancelled_at = ?
        WHERE id = ?
      `).run(now.toISOString(), taskId);
      if (task.managed_thread_id !== null) {
        this.#database.prepare(`
          UPDATE managed_threads SET state = 'idle' WHERE id = ?
        `).run(task.managed_thread_id);
      }
      if (task.state === "running" || task.state === "waiting_for_quota") {
        this.#pauseQueue("manual", now);
      }
      return {
        taskId,
        ...(task.state === "running"
          && task.managed_thread_id !== null
          && task.active_turn_id !== null
          ? {
            interrupt: {
              threadId: task.managed_thread_id,
              turnId: task.active_turn_id,
            },
          }
          : {}),
      };
    })();
  }

  retryNeedsAttentionTask(taskId: number, prompt: string): void {
    const result = this.#database.prepare(`
      UPDATE tasks
      SET state = 'queued', prompt = ?, active_turn_id = NULL,
          quota_limit_id = NULL, quota_limit_type = NULL, quota_reset_at = NULL,
          quota_poll_attempt = 0, transient_retry_count = 0, started_at = NULL
      WHERE id = ? AND state = 'needs_attention'
    `).run(prompt, taskId);
    if (result.changes === 1) return;
    const task = this.#database.prepare("SELECT state FROM tasks WHERE id = ?")
      .get(taskId) as { state: TaskState } | undefined;
    if (!task) throw new Error(`Task does not exist: ${taskId}`);
    throw new Error(`Task ${taskId} does not need attention.`);
  }

  completeNeedsAttentionTask(taskId: number, now: Date): void {
    const completedAt = now.toISOString();
    const result = this.#database.prepare(`
      UPDATE tasks
      SET state = 'completed', prompt = NULL, completed_at = ?,
          quota_limit_id = NULL, quota_limit_type = NULL, quota_reset_at = NULL,
          quota_poll_attempt = 0
      WHERE id = ? AND state = 'needs_attention'
    `).run(completedAt, taskId);
    if (result.changes === 1) return;
    const task = this.#database.prepare("SELECT state FROM tasks WHERE id = ?")
      .get(taskId) as { state: TaskState } | undefined;
    if (!task) throw new Error(`Task does not exist: ${taskId}`);
    throw new Error(`Task ${taskId} does not need attention.`);
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

  hasTurn(managedThreadId: string, turnId: string): boolean {
    return this.#database.prepare(`
      SELECT 1 FROM turns WHERE managed_thread_id = ? AND id = ?
    `).get(managedThreadId, turnId) !== undefined;
  }

  recordInactiveTurnCompletion(
    managedThreadId: string,
    turnId: string,
    status: "completed" | "failed" | "interrupted",
    now: Date,
  ): void {
    this.#database.transaction(() => {
      const result = this.#database.prepare(`
        UPDATE turns SET state = ?, completed_at = ?
        WHERE managed_thread_id = ? AND id = ?
          AND state IN ('in_progress', 'quota_paused')
      `).run(status, now.toISOString(), managedThreadId, turnId);
      if (result.changes === 1) {
        this.#database.prepare(`
          UPDATE managed_threads SET state = 'idle' WHERE id = ?
        `).run(managedThreadId);
      }
    })();
  }

  recordQuotaPause(
    managedThreadId: string,
    turnId: string,
    quota: QuotaPauseDetails,
  ): QuotaPause | undefined {
    return this.#database.transaction(() => {
      const task = this.#database.prepare(`
        SELECT id, workspace FROM tasks
        WHERE managed_thread_id = ? AND active_turn_id = ? AND state = 'running'
      `).get(managedThreadId, turnId) as { id: number; workspace: string } | undefined;
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
        workspace: task.workspace,
      };
    })();
  }

  readQuotaPause(taskId: number): QuotaPause | undefined {
    const row = this.#database.prepare(`
      SELECT id, managed_thread_id, active_turn_id, quota_limit_id,
             quota_limit_type, quota_reset_at, quota_poll_attempt, workspace
      FROM tasks WHERE id = ? AND state = 'waiting_for_quota'
    `).get(taskId) as {
      active_turn_id: string;
      id: number;
      managed_thread_id: string;
      quota_limit_id: string | null;
      quota_limit_type: string | null;
      quota_poll_attempt: number;
      quota_reset_at: string | null;
      workspace: string;
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
      workspace: row.workspace,
    };
  }

  readWaitingQuotaPause(): QuotaPause | undefined {
    const task = this.#database.prepare(`
      SELECT id FROM tasks WHERE state = 'waiting_for_quota' LIMIT 1
    `).get() as { id: number } | undefined;
    return task ? this.readQuotaPause(task.id) : undefined;
  }

  updateQuotaPause(
    taskId: number,
    quota: QuotaPauseDetails & { pollAttempt: number },
  ): QuotaPause | undefined {
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
    return result.changes === 1 ? this.readQuotaPause(taskId) : undefined;
  }

  isQueueRunning(): boolean {
    const queue = this.#database.prepare(
      "SELECT state FROM queue WHERE id = 1",
    ).get() as { state: "paused" | "running" };
    return queue.state === "running" && this.#latestQueueRun()?.ended_at === null;
  }

  pauseAtCutoff(cutoffTime: string, now: Date): boolean {
    return this.#database.transaction(() => {
      const activeRun = this.#latestQueueRun();
      if (
        !activeRun
        || activeRun.ended_at !== null
        || activeRun.cutoff_time !== cutoffTime
        || new Date(cutoffTime).getTime() > now.getTime()
      ) return false;
      this.#markDeferredRetryNeedsAttention();
      this.#pauseQueue("cutoff_reached", now);
      return true;
    })();
  }

  pause(now: Date): void {
    this.#database.transaction(() => {
      const queue = this.#database.prepare(
        "SELECT state FROM queue WHERE id = 1",
      ).get() as { state: "paused" | "running" };
      if (queue.state === "paused") return;
      this.#markDeferredRetryNeedsAttention();
      this.#pauseQueue("manual", now);
    })();
  }

  canStartTurn(now: Date): boolean {
    return this.#database.transaction(() => {
      return this.#queueRunState(now).kind === "running";
    })();
  }

  releaseUndispatchedTask(taskId: number): void {
    this.#database.transaction(() => {
      this.#database.prepare(`
        UPDATE managed_threads SET state = 'idle'
        WHERE id = (
          SELECT managed_thread_id FROM tasks
          WHERE id = ? AND state = 'running' AND active_turn_id IS NULL
        )
      `).run(taskId);
      this.#database.prepare(`
        UPDATE tasks SET state = 'queued', started_at = NULL
        WHERE id = ? AND state = 'running' AND active_turn_id IS NULL
      `).run(taskId);
    })();
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

  recordQuotaRecoveryFailure(
    taskId: number,
    error: StructuredError,
    now: Date,
  ): boolean {
    return this.#database.transaction(() => {
      const task = this.#database.prepare(`
        SELECT managed_thread_id FROM tasks
        WHERE id = ? AND state = 'waiting_for_quota'
      `).get(taskId) as { managed_thread_id: string } | undefined;
      if (!task) return false;

      this.#database.prepare(`
        UPDATE managed_threads SET state = 'idle' WHERE id = ?
      `).run(task.managed_thread_id);
      this.#database.prepare(`
        UPDATE tasks SET state = 'needs_attention'
        WHERE id = ? AND state = 'waiting_for_quota'
      `).run(taskId);
      this.#pauseQueue("needs_attention", now, error);
      return true;
    })();
  }

  recordTurnNeedsAttention(
    managedThreadId: string,
    turnId: string,
    turnState: "failed" | "interrupted",
    error: StructuredError,
    now: Date,
  ): boolean {
    return this.#database.transaction(() => {
      const turn = this.#database.prepare(`
        SELECT task_id FROM turns
        WHERE id = ? AND managed_thread_id = ? AND state = 'in_progress'
      `).get(turnId, managedThreadId) as { task_id: number } | undefined;
      if (!turn) return false;

      const completedAt = now.toISOString();
      this.#database.prepare(`
        UPDATE turns SET state = ?, completed_at = ? WHERE id = ?
      `).run(turnState, completedAt, turnId);
      this.#database.prepare(`
        UPDATE managed_threads SET state = 'idle' WHERE id = ?
      `).run(managedThreadId);
      this.#database.prepare(`
        UPDATE tasks SET state = 'needs_attention'
        WHERE id = ? AND state = 'running'
      `).run(turn.task_id);
      this.#pauseQueue("needs_attention", now, error);
      return true;
    })();
  }

  recordUnattendedRequest(
    managedThreadId: string,
    turnId: string | undefined,
    error: StructuredError,
    now: Date,
  ): boolean {
    return this.#database.transaction(() => {
      const task = this.#database.prepare(`
        SELECT id FROM tasks
        WHERE managed_thread_id = ? AND state = 'running'
          AND (? IS NULL OR active_turn_id = ?)
      `).get(managedThreadId, turnId ?? null, turnId ?? null) as
        { id: number } | undefined;
      if (!task) return false;
      this.#database.prepare(`
        UPDATE tasks SET state = 'needs_attention' WHERE id = ?
      `).run(task.id);
      this.#pauseQueue("needs_attention", now, error);
      return true;
    })();
  }

  recordTransientFailure(
    managedThreadId: string,
    turnId: string,
    now: Date,
  ): TransientRetry | "exhausted" | undefined {
    return this.#database.transaction(() => {
      const task = this.#database.prepare(`
        SELECT tasks.id, tasks.workspace, tasks.transient_retry_count
        FROM turns
        JOIN tasks ON tasks.id = turns.task_id
        WHERE turns.id = ? AND turns.managed_thread_id = ?
          AND turns.state = 'in_progress' AND tasks.state = 'running'
      `).get(turnId, managedThreadId) as {
        id: number;
        transient_retry_count: number;
        workspace: string;
      } | undefined;
      if (!task) return undefined;
      if (task.transient_retry_count >= 3) return "exhausted" as const;

      const completedAt = now.toISOString();
      const attempt = task.transient_retry_count + 1;
      this.#database.prepare(`
        UPDATE turns SET state = 'failed', completed_at = ? WHERE id = ?
      `).run(completedAt, turnId);
      this.#database.prepare(`
        UPDATE managed_threads SET state = 'idle' WHERE id = ?
      `).run(managedThreadId);
      this.#database.prepare(`
        UPDATE tasks SET transient_retry_count = ? WHERE id = ?
      `).run(attempt, task.id);
      return {
        attempt,
        taskId: task.id,
        threadId: managedThreadId,
        workspace: task.workspace,
      };
    })();
  }

  recordTransientRetryStarted(
    taskId: number,
    managedThreadId: string,
    turnId: string,
    now: Date,
  ): boolean {
    return this.#database.transaction(() => {
      const task = this.#database.prepare(`
        SELECT id FROM tasks
        WHERE id = ? AND managed_thread_id = ? AND state = 'running'
      `).get(taskId, managedThreadId);
      if (!task) return false;
      this.#database.prepare(`
        INSERT INTO turns (id, task_id, managed_thread_id, state, started_at)
        VALUES (?, ?, ?, 'in_progress', ?)
      `).run(turnId, taskId, managedThreadId, now.toISOString());
      this.#database.prepare(`
        UPDATE tasks SET active_turn_id = ? WHERE id = ?
      `).run(turnId, taskId);
      return true;
    })();
  }

  recordTaskNeedsAttention(
    taskId: number,
    error: StructuredError,
    now: Date,
  ): boolean {
    return this.#database.transaction(() => {
      const task = this.#database.prepare(`
        SELECT managed_thread_id FROM tasks
        WHERE id = ? AND state IN ('queued', 'running', 'waiting_for_quota')
      `).get(taskId) as { managed_thread_id: string | null } | undefined;
      if (!task) return false;
      if (task.managed_thread_id !== null) {
        this.#database.prepare(`
          UPDATE managed_threads SET state = 'idle' WHERE id = ?
        `).run(task.managed_thread_id);
      }
      this.#database.prepare(`
        UPDATE tasks SET state = 'needs_attention' WHERE id = ?
      `).run(taskId);
      this.#pauseQueue("needs_attention", now, error);
      return true;
    })();
  }

  recordActiveTaskNeedsAttention(
    error: StructuredError,
    now: Date,
  ): boolean {
    const task = this.#database.prepare(`
      SELECT id FROM tasks
      WHERE state IN ('running', 'waiting_for_quota') LIMIT 1
    `).get() as { id: number } | undefined;
    return task ? this.recordTaskNeedsAttention(task.id, error, now) : false;
  }

  recordExternalThreadActivity(
    managedThreadId: string,
    turnId: string,
    now: Date,
  ): boolean {
    return this.#database.transaction(() => {
      const managedThread = this.#database.prepare(`
        SELECT 1 FROM managed_threads WHERE id = ?
      `).get(managedThreadId);
      if (!managedThread) return false;
      this.#pauseQueue(
        "needs_attention",
        now,
        {
          code: "external_thread_activity",
          details: { threadId: managedThreadId, turnId },
          message: "External activity was detected on a Managed Thread.",
        },
      );
      this.#database.prepare(`
        SELECT codex_resumer_event(json_object(
          'eventType', 'thread.external_activity',
          'taskId', (
            SELECT id FROM tasks
            WHERE managed_thread_id = ?
              AND state IN ('running', 'waiting_for_quota')
            LIMIT 1
          ),
          'threadId', ?,
          'turnId', ?,
          'errorCode', 'external_thread_activity'
        ))
      `).get(managedThreadId, managedThreadId, turnId);
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
    const queue = this.#database.prepare(`
      SELECT state, pause_reason, error_code, error_message, error_details
      FROM queue WHERE id = 1
    `).get() as {
      error_code: string | null;
      error_details: string | null;
      error_message: string | null;
      pause_reason: PauseReason | null;
      state: "paused" | "running";
    };
    const queueRun = this.#latestQueueRun();
    const tasks = this.#database.prepare(`
      SELECT id, workspace, state, managed_thread_id, active_turn_id, prompt,
             quota_limit_id, quota_limit_type, quota_reset_at
      FROM tasks ORDER BY queue_position, id
    `).all() as TaskRow[];
    const hasUnfinishedTask = tasks.some(
      (task) =>
        task.state === "queued"
        || task.state === "running"
        || task.state === "waiting_for_quota"
        || task.state === "needs_attention",
    );
    return {
      state: queue.state === "running"
          && (queueRun?.ended_at !== null || !hasUnfinishedTask)
        ? "idle"
        : queue.state,
      ...(queue.error_code === null || queue.error_message === null
        ? {}
        : {
          error: {
            code: queue.error_code,
            ...(queue.error_details === null
              ? {}
              : { details: JSON.parse(queue.error_details) as unknown }),
            message: queue.error_message,
          },
        }),
      ...(queue.pause_reason === null ? {} : { pauseReason: queue.pause_reason }),
      ...(queueRun
        ? {
          queueRun: {
            accessMode: queueRun.access_mode,
            id: queueRun.id,
            runPolicy: queueRun.run_policy,
            startedAt: queueRun.started_at,
            ...(queueRun.cutoff_time === null
              ? {}
              : { cutoffTime: queueRun.cutoff_time }),
            ...(queueRun.ended_at === null ? {} : { endedAt: queueRun.ended_at }),
          },
        }
        : {}),
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

  #installEventLogTriggers(): void {
    this.#database.exec(`
      CREATE TRIGGER IF NOT EXISTS event_log_task_insert
      AFTER INSERT ON tasks
      BEGIN
        SELECT codex_resumer_event(json_object(
          'eventType', 'task.state_changed',
          'taskId', NEW.id,
          'threadId', NEW.managed_thread_id,
          'turnId', NEW.active_turn_id,
          'fromState', NULL,
          'toState', NEW.state,
          'quotaResetAt', NEW.quota_reset_at
        ));
      END;

      CREATE TRIGGER IF NOT EXISTS event_log_task_state
      AFTER UPDATE OF state ON tasks
      WHEN OLD.state IS NOT NEW.state
      BEGIN
        SELECT codex_resumer_event(json_object(
          'eventType', 'task.state_changed',
          'taskId', NEW.id,
          'threadId', NEW.managed_thread_id,
          'turnId', NEW.active_turn_id,
          'fromState', OLD.state,
          'toState', NEW.state,
          'quotaResetAt', NEW.quota_reset_at
        ));
      END;

      CREATE TRIGGER IF NOT EXISTS event_log_quota_reset
      AFTER UPDATE OF quota_reset_at ON tasks
      WHEN OLD.quota_reset_at IS NOT NEW.quota_reset_at
      BEGIN
        SELECT codex_resumer_event(json_object(
          'eventType', 'quota.reset_updated',
          'taskId', NEW.id,
          'threadId', NEW.managed_thread_id,
          'turnId', NEW.active_turn_id,
          'quotaResetAt', NEW.quota_reset_at
        ));
      END;

      CREATE TRIGGER IF NOT EXISTS event_log_turn_insert
      AFTER INSERT ON turns
      BEGIN
        SELECT codex_resumer_event(json_object(
          'eventType', 'turn.state_changed',
          'taskId', NEW.task_id,
          'threadId', NEW.managed_thread_id,
          'turnId', NEW.id,
          'fromState', NULL,
          'toState', NEW.state
        ));
      END;

      CREATE TRIGGER IF NOT EXISTS event_log_turn_state
      AFTER UPDATE OF state ON turns
      WHEN OLD.state IS NOT NEW.state
      BEGIN
        SELECT codex_resumer_event(json_object(
          'eventType', 'turn.state_changed',
          'taskId', NEW.task_id,
          'threadId', NEW.managed_thread_id,
          'turnId', NEW.id,
          'fromState', OLD.state,
          'toState', NEW.state
        ));
      END;

      CREATE TRIGGER IF NOT EXISTS event_log_queue_insert
      AFTER INSERT ON queue
      BEGIN
        SELECT codex_resumer_event(json_object(
          'eventType', 'queue.state_changed',
          'fromState', NULL,
          'toState', NEW.state,
          'errorCode', NEW.error_code
        ));
      END;

      CREATE TRIGGER IF NOT EXISTS event_log_queue_update
      AFTER UPDATE OF state, pause_reason, error_code ON queue
      WHEN OLD.state IS NOT NEW.state
        OR OLD.pause_reason IS NOT NEW.pause_reason
        OR OLD.error_code IS NOT NEW.error_code
      BEGIN
        SELECT codex_resumer_event(json_object(
          'eventType', 'queue.state_changed',
          'taskId', (SELECT id FROM tasks
            WHERE state IN ('needs_attention', 'running', 'waiting_for_quota')
            ORDER BY state = 'needs_attention' DESC, queue_position, id LIMIT 1),
          'threadId', (SELECT managed_thread_id FROM tasks
            WHERE state IN ('needs_attention', 'running', 'waiting_for_quota')
            ORDER BY state = 'needs_attention' DESC, queue_position, id LIMIT 1),
          'turnId', (SELECT active_turn_id FROM tasks
            WHERE state IN ('needs_attention', 'running', 'waiting_for_quota')
            ORDER BY state = 'needs_attention' DESC, queue_position, id LIMIT 1),
          'fromState', OLD.state,
          'toState', NEW.state,
          'errorCode', NEW.error_code
        ));
      END;

      CREATE TRIGGER IF NOT EXISTS event_log_queue_run_insert
      AFTER INSERT ON queue_runs
      BEGIN
        SELECT codex_resumer_event(json_object(
          'eventType', 'queue_run.started'
        ));
      END;

      CREATE TRIGGER IF NOT EXISTS event_log_queue_run_end
      AFTER UPDATE OF ended_at ON queue_runs
      WHEN OLD.ended_at IS NULL AND NEW.ended_at IS NOT NULL
      BEGIN
        SELECT codex_resumer_event(json_object(
          'eventType', 'queue_run.ended'
        ));
      END;

      CREATE TRIGGER IF NOT EXISTS event_log_thread_insert
      AFTER INSERT ON managed_threads
      BEGIN
        SELECT codex_resumer_event(json_object(
          'eventType', 'thread.managed',
          'threadId', NEW.id
        ));
      END;
    `);
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

          ${createTasksTable()}
          INSERT INTO tasks (
            id, workspace, prompt, state, managed_thread_id, active_turn_id,
            quota_limit_id, quota_limit_type, quota_reset_at, quota_poll_attempt,
            queue_position, created_at, started_at, completed_at, cancelled_at
          )
          SELECT
            id, workspace, prompt, state, managed_thread_id, active_turn_id,
            NULL, NULL, NULL, 0, id, created_at, started_at, completed_at, NULL
          FROM tasks_before_queue;

          ${createTurnsTable()}
          INSERT INTO turns (
            id, task_id, managed_thread_id, state, started_at, completed_at
          )
          SELECT id, task_id, managed_thread_id, state, started_at, completed_at
          FROM turns_before_queue;

          DROP TABLE turns_before_queue;
          DROP TABLE tasks_before_queue;
          ${createActiveTaskIndex()}
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

          ${createTasksTable()}
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

          ${createTurnsTable()}
          INSERT INTO turns (
            id, task_id, managed_thread_id, state, started_at, completed_at
          )
          SELECT id, task_id, managed_thread_id, state, started_at, completed_at
          FROM turns_before_quota_pause;

          DROP TABLE turns_before_quota_pause;
          DROP TABLE tasks_before_quota_pause;
          ${createActiveTaskIndex()}
        `);
      })();
    } finally {
      this.#database.pragma("foreign_keys = ON");
    }
    const violations = this.#database.pragma("foreign_key_check") as unknown[];
    if (violations.length > 0) throw new Error("Quota Pause migration failed.");
  }

  #migrateNeedsAttention(): void {
    const columns = this.#database.pragma("table_info(tasks)") as Array<{ name: string }>;
    if (columns.some((column) => column.name === "transient_retry_count")) return;

    this.#database.pragma("foreign_keys = OFF");
    try {
      this.#database.transaction(() => {
        this.#database.exec(`
          DROP INDEX IF EXISTS one_active_task;
          ALTER TABLE turns RENAME TO turns_before_needs_attention;
          ALTER TABLE tasks RENAME TO tasks_before_needs_attention;

          ${createTasksTable()}
          INSERT INTO tasks (
            id, workspace, prompt, state, managed_thread_id, active_turn_id,
            quota_limit_id, quota_limit_type, quota_reset_at, quota_poll_attempt,
            transient_retry_count, queue_position, created_at, started_at,
            completed_at, cancelled_at
          )
          SELECT
            id, workspace, prompt, state, managed_thread_id, active_turn_id,
            quota_limit_id, quota_limit_type, quota_reset_at, quota_poll_attempt,
            0, queue_position, created_at, started_at, completed_at, cancelled_at
          FROM tasks_before_needs_attention;

          ${createTurnsTable()}
          INSERT INTO turns (
            id, task_id, managed_thread_id, state, started_at, completed_at
          )
          SELECT id, task_id, managed_thread_id, state, started_at, completed_at
          FROM turns_before_needs_attention;

          DROP TABLE turns_before_needs_attention;
          DROP TABLE tasks_before_needs_attention;
          ${createActiveTaskIndex()}
        `);
      })();
    } finally {
      this.#database.pragma("foreign_keys = ON");
    }
    const violations = this.#database.pragma("foreign_key_check") as unknown[];
    if (violations.length > 0) throw new Error("Needs Attention migration failed.");
  }

  #migrateQueuePauseReason(): void {
    const columns = this.#database.pragma("table_info(queue)") as Array<{ name: string }>;
    if (columns.some((column) => column.name === "pause_reason")) return;
    this.#database.exec("ALTER TABLE queue ADD COLUMN pause_reason TEXT");
    this.#database.prepare(`
      UPDATE queue SET pause_reason = 'not_started' WHERE state = 'paused'
    `).run();
    this.#database.prepare(`
      UPDATE queue
      SET state = 'paused', pause_reason = 'run_policy_required'
      WHERE state = 'running'
    `).run();
  }

  #migrateQueueError(): void {
    const columns = this.#database.pragma("table_info(queue)") as Array<{ name: string }>;
    const names = new Set(columns.map((column) => column.name));
    if (!names.has("error_code")) {
      this.#database.exec("ALTER TABLE queue ADD COLUMN error_code TEXT");
    }
    if (!names.has("error_message")) {
      this.#database.exec("ALTER TABLE queue ADD COLUMN error_message TEXT");
    }
    if (!names.has("error_details")) {
      this.#database.exec("ALTER TABLE queue ADD COLUMN error_details TEXT");
    }
  }

  #latestQueueRun(): QueueRunRow | undefined {
    return this.#database.prepare(`
      SELECT id, run_policy, access_mode, cutoff_time, started_at, ended_at
      FROM queue_runs ORDER BY id DESC LIMIT 1
    `).get() as QueueRunRow | undefined;
  }

  #migrateQueueRunAccessMode(): void {
    const columns = this.#database.pragma("table_info(queue_runs)") as Array<{ name: string }>;
    if (columns.some((column) => column.name === "access_mode")) return;
    this.#database.exec(`
      ALTER TABLE queue_runs
      ADD COLUMN access_mode TEXT NOT NULL DEFAULT 'configured'
        CHECK (access_mode IN ('configured', 'full'));
    `);
  }

  #markDeferredRetryNeedsAttention(): boolean {
    const result = this.#database.prepare(`
      UPDATE tasks
      SET state = 'needs_attention'
      WHERE state = 'running'
        AND active_turn_id IN (SELECT id FROM turns WHERE state = 'failed')
    `).run();
    return result.changes > 0;
  }

  #queueRunState(now: Date): PersistedQueueRunState {
    const queue = this.#database.prepare(
      "SELECT state FROM queue WHERE id = 1",
    ).get() as { state: "paused" | "running" };
    if (queue.state === "paused") return { kind: "paused" };
    const activeRun = this.#latestQueueRun();
    if (!activeRun || activeRun.ended_at !== null) return { kind: "idle" };
    if (
      activeRun.cutoff_time !== null
      && new Date(activeRun.cutoff_time).getTime() <= now.getTime()
    ) {
      this.#markDeferredRetryNeedsAttention();
      this.#pauseQueue("cutoff_reached", now);
      return { kind: "paused" };
    }
    return { kind: "running", queueRun: activeRun };
  }

  #endActiveQueueRun(now: Date): void {
    this.#database.prepare(`
      UPDATE queue_runs SET ended_at = ?
      WHERE id = (SELECT id FROM queue_runs ORDER BY id DESC LIMIT 1)
        AND ended_at IS NULL
    `).run(now.toISOString());
  }

  #pauseQueue(
    reason: PauseReason,
    now: Date,
    error?: StructuredError,
  ): void {
    this.#database.prepare(`
      UPDATE queue
      SET state = 'paused', pause_reason = ?, error_code = ?,
          error_message = ?, error_details = ?, updated_at = ?
      WHERE id = 1
    `).run(
      reason,
      error?.code ?? null,
      error?.message ?? null,
      error?.details === undefined ? null : JSON.stringify(error.details),
      now.toISOString(),
    );
    this.#endActiveQueueRun(now);
  }
}
