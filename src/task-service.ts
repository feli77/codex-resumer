import { constants as fsConstants } from "node:fs";
import { access, realpath, stat } from "node:fs/promises";
import path from "node:path";

import { DEFAULT_CONTINUATION_PROMPT } from "./config.js";
import type {
  AccountRateLimits,
  AppServerController,
  Clock,
  CompletedTurn,
  RateLimitSnapshot,
  UsageLimitExceeded,
} from "./daemon.js";
import {
  StateStore,
  type QueueSnapshot,
  type QueuedTask,
  type QuotaPause,
  type QuotaPauseDetails,
} from "./state-store.js";

const QUOTA_RESET_SAFETY_MS = 2_000;
const QUOTA_POLL_INITIAL_MS = 60_000;
const QUOTA_POLL_MAX_MS = 15 * 60_000;

export class TaskService {
  #automaticAdvance: Promise<void> = Promise.resolve();
  #completionBeforeTurnStart: CompletedTurn | undefined;
  #quotaBeforeTurnStart: UsageLimitExceeded | undefined;
  #quotaRecovery: Promise<void> = Promise.resolve();
  readonly #stopping = new AbortController();

  constructor(
    private readonly appServer: AppServerController,
    private readonly store: StateStore,
    private readonly clock: Clock,
    private readonly continuationPrompt: () => Promise<string> = async () =>
      DEFAULT_CONTINUATION_PROMPT,
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

  handleUsageLimitExceeded(event: UsageLimitExceeded): void {
    if (!this.store.hasActiveTurn(event.threadId, event.turnId)) {
      this.#quotaBeforeTurnStart = event;
      return;
    }
    this.#scheduleQuotaPause(event);
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
    this.#reconcileTurnStart(threadId, turnId);
  }

  #reconcileTurnStart(threadId: string, turnId: string): void {
    const quota = this.#quotaBeforeTurnStart;
    if (quota?.threadId === threadId && quota.turnId === turnId) {
      this.#quotaBeforeTurnStart = undefined;
      this.#scheduleQuotaPause(quota);
    }
    const completion = this.#completionBeforeTurnStart;
    if (completion?.threadId === threadId && completion.turnId === turnId) {
      this.#completionBeforeTurnStart = undefined;
      this.store.completeTurn(threadId, turnId, this.clock.now());
      this.#scheduleAutomaticAdvance();
    }
  }

  close(): void {
    this.#stopping.abort(new Error("daemon stopped"));
  }

  #scheduleQuotaPause(event: UsageLimitExceeded): void {
    this.#automaticAdvance = this.#automaticAdvance
      .then(async () => {
        const rateLimits = await this.appServer.readRateLimits()
          .catch(() => emptyRateLimits());
        const quotaPause = this.store.recordQuotaPause(
          event.threadId,
          event.turnId,
          selectApplicableQuota(rateLimits),
        );
        if (quotaPause) this.#scheduleQuotaRecovery(quotaPause);
      })
      .catch(() => undefined);
  }

  #scheduleQuotaRecovery(quotaPause: QuotaPause): void {
    this.#quotaRecovery = this.#quotaRecovery
      .then(() => this.#recoverFromQuota(quotaPause))
      .catch(() => undefined);
  }

  async #recoverFromQuota(quotaPause: QuotaPause): Promise<void> {
    let current: QuotaPause | undefined = quotaPause;
    while (current && !this.#stopping.signal.aborted) {
      await waitUntil(
        this.clock,
        nextQuotaCheck(current, this.clock.now()),
        this.#stopping.signal,
      );
      if (this.#stopping.signal.aborted || !this.store.isQueueRunning()) return;
      current = this.store.readQuotaPause(quotaPause.taskId);
      if (!current) return;

      const rateLimits = await this.appServer.readRateLimits()
        .catch(() => emptyRateLimits());
      if (!quotaIsAvailable(rateLimits, current.limitId)) {
        const refreshed = quotaForLimit(rateLimits, current.limitId);
        const resetAt = refreshed.resetAt;
        const resetIsAhead = resetAt !== undefined
          && resetAt.getTime() + QUOTA_RESET_SAFETY_MS > this.clock.now().getTime();
        current = this.store.updateQuotaPause(current.taskId, {
          limitId: refreshed.limitId ?? current.limitId,
          limitType: refreshed.limitType ?? current.limitType,
          pollAttempt: resetIsAhead ? 0 : current.pollAttempt + 1,
          resetAt,
        });
        continue;
      }
      if (!this.store.isQueueRunning()) return;
      await this.#startContinuation(current);
      return;
    }
  }

  async #startContinuation(current: QuotaPause): Promise<void> {
    const { threadId } = await this.appServer.resumeThread(current.threadId);
    this.store.activateManagedThread(threadId);
    const { turnId } = await this.appServer.startTurn(
      threadId,
      await this.continuationPrompt(),
    );
    if (!this.store.recordContinuationTurnStarted(
      current.taskId,
      threadId,
      turnId,
      this.clock.now(),
    )) return;
    this.#reconcileTurnStart(threadId, turnId);
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

function selectApplicableQuota(rateLimits: AccountRateLimits): QuotaPauseDetails {
  const buckets = rateLimitBuckets(rateLimits);
  const reached = buckets.filter((candidate) =>
    candidate.snapshot.rateLimitReachedType !== null
  );
  const exhausted = buckets.filter((candidate) =>
    [candidate.snapshot.primary, candidate.snapshot.secondary]
      .some((window) => window !== null && window.usedPercent >= 100)
  );
  const candidates = reached.length > 0 ? reached : exhausted;
  const selected = candidates
    .map((candidate) => ({
      ...candidate,
      resetAt: latestReset(candidate.snapshot),
    }))
    .sort((left, right) => {
      if (!left.resetAt && !right.resetAt) return 0;
      if (!left.resetAt) return -1;
      if (!right.resetAt) return 1;
      return right.resetAt.getTime() - left.resetAt.getTime();
    })[0];
  return {
    limitId: selected?.id,
    limitType: selected?.snapshot.rateLimitReachedType ?? undefined,
    resetAt: selected?.resetAt,
  };
}

function emptyRateLimits(): AccountRateLimits {
  return { rateLimits: null, rateLimitsByLimitId: null };
}

function quotaIsAvailable(
  rateLimits: AccountRateLimits,
  limitId: string | undefined,
): boolean {
  const bucket = quotaBucket(rateLimits, limitId)?.snapshot;
  if (!bucket || bucket.rateLimitReachedType !== null) return false;
  return [bucket.primary, bucket.secondary].every(
    (window) => window === null || window.usedPercent < 100,
  );
}

function quotaForLimit(
  rateLimits: AccountRateLimits,
  limitId: string | undefined,
): QuotaPauseDetails {
  const selected = quotaBucket(rateLimits, limitId);
  return {
    limitId: selected?.id,
    limitType: selected?.snapshot.rateLimitReachedType ?? undefined,
    resetAt: selected ? latestReset(selected.snapshot) : undefined,
  };
}

function rateLimitBuckets(
  rateLimits: AccountRateLimits,
): Array<{ id: string | undefined; snapshot: RateLimitSnapshot }> {
  const buckets: Array<{ id: string | undefined; snapshot: RateLimitSnapshot }> =
    rateLimits.rateLimitsByLimitId
    ? Object.entries(rateLimits.rateLimitsByLimitId).map(([id, snapshot]) => ({
      id: snapshot.limitId ?? id,
      snapshot,
    }))
    : [];
  if (rateLimits.rateLimits) {
    const legacyId = rateLimits.rateLimits.limitId ?? undefined;
    if (!buckets.some((candidate) => candidate.id === legacyId)) {
      buckets.push({ id: legacyId, snapshot: rateLimits.rateLimits });
    }
  }
  return buckets;
}

function quotaBucket(
  rateLimits: AccountRateLimits,
  limitId: string | undefined,
): { id: string | undefined; snapshot: RateLimitSnapshot } | undefined {
  const buckets = rateLimitBuckets(rateLimits);
  if (limitId !== undefined) {
    return buckets.find((candidate) => candidate.id === limitId);
  }
  if (rateLimits.rateLimits) {
    return {
      id: rateLimits.rateLimits.limitId ?? undefined,
      snapshot: rateLimits.rateLimits,
    };
  }
  return buckets.length === 1 ? buckets[0] : undefined;
}

function latestReset(snapshot: RateLimitSnapshot): Date | undefined {
  const windows = [snapshot.primary, snapshot.secondary];
  const exhausted = windows.filter(
    (window) => window !== null && window.usedPercent >= 100,
  );
  const applicable = exhausted.length > 0 ? exhausted : windows;
  const resetsAt = applicable
    .map((window) => window?.resetsAt)
    .filter((value): value is number =>
      typeof value === "number" && Number.isSafeInteger(value) && value > 0
    )
    .sort((left, right) => right - left)[0];
  return resetsAt === undefined ? undefined : new Date(resetsAt * 1_000);
}

function nextQuotaCheck(quotaPause: QuotaPause, now: Date): Date {
  if (
    quotaPause.resetAt
    && quotaPause.resetAt.getTime() + QUOTA_RESET_SAFETY_MS > now.getTime()
  ) {
    return new Date(quotaPause.resetAt.getTime() + QUOTA_RESET_SAFETY_MS);
  }
  const backoff = Math.min(
    QUOTA_POLL_INITIAL_MS * 2 ** quotaPause.pollAttempt,
    QUOTA_POLL_MAX_MS,
  );
  return new Date(now.getTime() + backoff);
}

async function waitUntil(
  clock: Clock,
  until: Date,
  signal: AbortSignal,
): Promise<void> {
  if (clock.waitUntil) return clock.waitUntil(until, signal);
  while (!signal.aborted && clock.now() < until) {
    const delay = Math.min(until.getTime() - clock.now().getTime(), 2_147_483_647);
    await new Promise<void>((resolve, reject) => {
      const onAbort = (): void => {
        clearTimeout(timeout);
        reject(signal.reason);
      };
      const timeout = setTimeout(() => {
        signal.removeEventListener("abort", onAbort);
        resolve();
      }, delay);
      timeout.unref();
      signal.addEventListener("abort", onAbort, { once: true });
    });
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
