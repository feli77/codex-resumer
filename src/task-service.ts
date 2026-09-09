import { constants as fsConstants } from "node:fs";
import { access, realpath, stat } from "node:fs/promises";
import path from "node:path";

import { DEFAULT_CONTINUATION_PROMPT, type AccessMode } from "./config.js";
import { normalizeCutoffTime, type RunPolicy } from "./run-policy.js";
import { serializeOperationalError } from "./structured-error.js";
import type {
  AccountRateLimits,
  AppServerController,
  Clock,
  CompletedTurn,
  RateLimitSnapshot,
  StartedTurn,
  ThreadRecoverySnapshot,
  UnattendedRequest,
  UsageLimitExceeded,
} from "./daemon.js";
import {
  StateStore,
  type QueueSnapshot,
  type QueuedTask,
  type QuotaPause,
  type QuotaPauseDetails,
  type TransientRetry,
} from "./state-store.js";

const QUOTA_RESET_SAFETY_MS = 2_000;
const QUOTA_POLL_INITIAL_MS = 60_000;
const QUOTA_POLL_MAX_MS = 15 * 60_000;
const TRANSIENT_RETRY_INITIAL_MS = 1_000;

export class TaskService {
  #automaticAdvance: Promise<void> = Promise.resolve();
  #accessMode: AccessMode | undefined;
  #completionBeforeTurnStart: CompletedTurn | undefined;
  #quotaBeforeTurnStart: UsageLimitExceeded | undefined;
  #quotaRecovery: Promise<void> = Promise.resolve();
  #transientRetry: Promise<void> = Promise.resolve();
  #unattendedBeforeTurnStart: UnattendedRequest | undefined;
  #turnStartsInProgress = new Set<string>();
  #turnStartOperations = new Set<Promise<unknown>>();
  #unrecordedAcceptedTurns: StartedTurn[] = [];
  #startedBeforeTurnRecorded: StartedTurn[] = [];
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

  async start(
    runPolicy: RunPolicy,
    accessMode: AccessMode = "configured",
  ): Promise<{ state: "started" | "already-running" | "idle" | "paused" }> {
    const result = this.store.startQueueRun(runPolicy, accessMode, this.clock.now());
    if (result === "already-running") return { state: result };
    const next = await this.#continueQueueRun(runPolicy, accessMode);
    return { state: next === "already-running" ? "started" : next };
  }

  async restoreQueueRun(): Promise<void> {
    const queueRun = this.store.restoreQueueRun(this.clock.now());
    if (queueRun) {
      this.#accessMode = queueRun.accessMode;
      this.#scheduleCutoff(queueRun.runPolicy);
    }
    const recoveryTask = this.store.readRecoveryTask();
    if (
      recoveryTask
      && (!recoveryTask.managedThreadId || !recoveryTask.activeTurnId)
    ) {
      this.store.recordTaskNeedsAttention(
        recoveryTask.id,
        {
          code: "turn_reconciliation_uncertain",
          details: {
            threadId: recoveryTask.managedThreadId ?? null,
            turnId: recoveryTask.activeTurnId ?? null,
          },
          message: "The active Turn could not be confirmed after daemon restart.",
        },
        this.clock.now(),
      );
      return;
    }
    if (
      recoveryTask
      && recoveryTask.managedThreadId
      && recoveryTask.activeTurnId
    ) {
      let thread: ThreadRecoverySnapshot;
      try {
        thread = await this.appServer.readThreadForReconciliation(
          recoveryTask.managedThreadId,
        );
      } catch (error) {
        this.#recordReconciliationUnavailable(recoveryTask.id, error);
        return;
      }
      let turn = thread.turns.find(
        (candidate) => candidate.turnId === recoveryTask.activeTurnId,
      );
      if (
        recoveryTask.state === "running"
        && thread.status === "active"
        && turn?.status === "in_progress"
      ) {
        try {
          await this.appServer.resumeThread(recoveryTask.managedThreadId);
          thread = await this.appServer.readThreadForReconciliation(
            recoveryTask.managedThreadId,
          );
          turn = thread.turns.find(
            (candidate) => candidate.turnId === recoveryTask.activeTurnId,
          );
        } catch (error) {
          this.#recordReconciliationUnavailable(recoveryTask.id, error);
          return;
        }
        if (thread.status === "active" && turn?.status === "in_progress") return;
      }
      if (recoveryTask.state === "running" && turn?.status === "completed") {
        this.#finishTurn({
          status: "completed",
          threadId: recoveryTask.managedThreadId,
          turnId: recoveryTask.activeTurnId,
        });
        return;
      }
      if (recoveryTask.state === "running" && turn?.status === "interrupted") {
        this.#finishTurn({
          status: "interrupted",
          threadId: recoveryTask.managedThreadId,
          turnId: recoveryTask.activeTurnId,
        });
        return;
      }
      if (recoveryTask.state === "running" && turn?.status === "failed") {
        if (turn.error?.codexErrorInfo === "usageLimitExceeded") {
          this.#finishTurn({
            status: "failed",
            threadId: recoveryTask.managedThreadId,
            turnId: recoveryTask.activeTurnId,
            error: turn.error,
          });
        } else {
          this.store.recordTurnNeedsAttention(
            recoveryTask.managedThreadId,
            recoveryTask.activeTurnId,
            "failed",
            {
              code: "turn_failed",
              ...(turn.error?.codexErrorInfo === undefined
                ? {}
                : { details: { codexErrorInfo: turn.error.codexErrorInfo } }),
              message: turn.error?.message
                ?? "Codex Turn failed; user action is required.",
            },
            this.clock.now(),
          );
        }
        return;
      }
      if (
        recoveryTask.state === "waiting_for_quota"
        && turn?.status === "failed"
        && turn.error?.codexErrorInfo === "usageLimitExceeded"
      ) {
        const quotaPause = this.store.readWaitingQuotaPause();
        if (queueRun && quotaPause) this.#scheduleQuotaRecovery(quotaPause);
        return;
      }
      this.store.recordTaskNeedsAttention(
        recoveryTask.id,
        {
          code: "turn_reconciliation_uncertain",
          details: {
            threadStatus: thread.status,
            turnId: recoveryTask.activeTurnId,
          },
          message: "The active Turn could not be confirmed after daemon restart.",
        },
        this.clock.now(),
      );
      return;
    }
    if (!queueRun) return;
    const quotaPause = this.store.readWaitingQuotaPause();
    if (quotaPause) {
      this.#scheduleQuotaRecovery(quotaPause);
      return;
    }
    await this.#startNextTask().catch(() => undefined);
  }

  #recordReconciliationUnavailable(taskId: number, error: unknown): void {
    this.store.recordTaskNeedsAttention(
      taskId,
      {
        code: "turn_reconciliation_unavailable",
        details: { cause: serializeOperationalError(error) },
        message: "The active Turn could not be read after daemon restart.",
      },
      this.clock.now(),
    );
  }

  async #continueQueueRun(
    runPolicy: RunPolicy,
    accessMode: AccessMode,
  ): Promise<"started" | "already-running" | "idle" | "paused"> {
    this.#accessMode = accessMode;
    this.#scheduleCutoff(runPolicy);
    const quotaPause = this.store.readWaitingQuotaPause();
    if (quotaPause) {
      this.#scheduleQuotaRecovery(quotaPause);
      return "already-running";
    }
    return this.#startNextTask();
  }

  pause(): { state: "paused" } {
    this.store.pause(this.clock.now());
    return { state: "paused" };
  }

  async prepareStop(force: boolean): Promise<{ state: "stopping" }> {
    this.pause();
    await Promise.allSettled(this.#turnStartOperations);
    const activeTask = this.store.readRecoveryTask();
    if (
      !force
      && (
        activeTask?.state === "running"
        || this.#unrecordedAcceptedTurns.length > 0
      )
    ) {
      throw new Error(
        "An active Turn is still running. Let it finish or use `daemon stop --force` to interrupt it.",
      );
    }
    if (force) {
      const turnsToInterrupt = [...this.#unrecordedAcceptedTurns];
      if (
        activeTask?.state === "running"
        && activeTask.managedThreadId
        && activeTask.activeTurnId
      ) {
        turnsToInterrupt.push({
          threadId: activeTask.managedThreadId,
          turnId: activeTask.activeTurnId,
        });
      }
      let interruptError;
      for (const turn of turnsToInterrupt) {
        try {
          await this.appServer.interruptTurn(
            turn.threadId,
            turn.turnId,
          );
        } catch (error) {
          interruptError = serializeOperationalError(error);
        }
      }
      if (
        activeTask
        && (
          activeTask.state === "running"
          || this.#unrecordedAcceptedTurns.length > 0
        )
      ) {
        this.store.recordTaskNeedsAttention(
          activeTask.id,
          {
            code: "daemon_force_stop",
            ...(interruptError ? { details: { interruptError } } : {}),
            message: turnsToInterrupt.length > 0
              ? "The active Turn was interrupted by forced daemon stop."
              : "The active Turn could not be confirmed during forced daemon stop.",
          },
          this.clock.now(),
        );
      }
    }
    return { state: "stopping" };
  }

  async #startNextTask(): Promise<
    "started" | "already-running" | "idle" | "paused"
  > {
    const next = this.store.startNextTask(this.clock.now());
    if (next.kind !== "started") return next.kind;
    return this.#trackTurnStartOperation(this.#dispatchStartedTask(next.task));
  }

  async #dispatchStartedTask(task: QueuedTask): Promise<"started" | "paused"> {
    try {
      await accessibleWorkspace(task.workspace);
      if (!await this.dispatch(task)) return "paused";
      return "started";
    } catch (error) {
      this.store.releaseTaskBeforeDispatch(
        task.id,
        serializeOperationalError(error),
        this.clock.now(),
      );
      throw error;
    }
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

  async cancel(taskId: number): Promise<{ taskId: number }> {
    const cancellation = this.store.cancelTask(taskId, this.clock.now());
    if (cancellation.interrupt) {
      await this.appServer.interruptTurn(
        cancellation.interrupt.threadId,
        cancellation.interrupt.turnId,
      );
    }
    return { taskId };
  }

  retry(taskId: number, prompt: string): { taskId: number } {
    if (prompt.trim().length === 0) {
      throw new Error("New retry prompt must not be empty.");
    }
    this.store.retryNeedsAttentionTask(taskId, prompt);
    return { taskId };
  }

  complete(taskId: number): { taskId: number } {
    this.store.completeNeedsAttentionTask(taskId, this.clock.now());
    return { taskId };
  }

  handleTurnCompleted(turn: CompletedTurn): void {
    if (!this.store.hasActiveTurn(turn.threadId, turn.turnId)) {
      if (this.store.hasTurn(turn.threadId, turn.turnId)) {
        this.store.recordInactiveTurnCompletion(
          turn.threadId,
          turn.turnId,
          turn.status,
          this.clock.now(),
        );
        return;
      }
      this.#completionBeforeTurnStart = turn;
      return;
    }
    this.#finishTurn(turn);
  }

  handleUsageLimitExceeded(event: UsageLimitExceeded): void {
    if (!this.store.hasActiveTurn(event.threadId, event.turnId)) {
      if (this.store.hasTurn(event.threadId, event.turnId)) return;
      this.#quotaBeforeTurnStart = event;
      return;
    }
    this.#scheduleQuotaPause(event);
  }

  handleUnattendedRequest(request: UnattendedRequest): void {
    if (!this.store.recordUnattendedRequest(
      request.threadId,
      request.turnId,
      {
        code: "unattended_request",
        details: { requestType: request.kind },
        message: unattendedRequestMessage(request.kind),
      },
      this.clock.now(),
    )) {
      if (request.turnId === undefined) return;
      if (this.store.hasTurn(request.threadId, request.turnId)) return;
      this.#unattendedBeforeTurnStart = request;
    }
  }

  handleAppServerExit(error: Error): void {
    if (this.#stopping.signal.aborted) return;
    this.store.recordActiveTaskNeedsAttention(
      {
        code: "app_server_process_exit",
        message: error.message,
      },
      this.clock.now(),
    );
  }

  handleTurnStarted(turn: StartedTurn): void {
    if (this.store.hasActiveTurn(turn.threadId, turn.turnId)) return;
    if (this.#turnStartsInProgress.has(turn.threadId)) {
      this.#startedBeforeTurnRecorded.push(turn);
      return;
    }
    this.store.recordExternalThreadActivity(
      turn.threadId,
      turn.turnId,
      this.clock.now(),
    );
  }

  async dispatch(task: QueuedTask): Promise<boolean> {
    const { threadId } = task.managedThreadId
      ? await this.appServer.resumeThread(task.managedThreadId)
      : await this.appServer.startThread(task.workspace);
    if (task.managedThreadId) {
      this.store.activateManagedThread(threadId);
    } else {
      this.store.recordManagedThread(task, threadId, this.clock.now());
    }
    if (!this.store.canStartTurn(this.clock.now())) {
      this.store.releaseUndispatchedTask(task.id);
      return false;
    }
    await this.#startOwnedTurn(
      threadId,
      task.prompt,
      task.workspace,
      (turnId) => {
        this.store.recordTurnStarted(task.id, threadId, turnId, this.clock.now());
        return true;
      },
    );
    return true;
  }

  async #startOwnedTurn(
    threadId: string,
    prompt: string,
    workspace: string,
    recordStarted: (turnId: string) => boolean,
  ): Promise<void> {
    await this.#trackTurnStartOperation(
      this.#performOwnedTurnStart(
        threadId,
        prompt,
        workspace,
        recordStarted,
      ),
    );
  }

  async #trackTurnStartOperation<T>(operation: Promise<T>): Promise<T> {
    this.#turnStartOperations.add(operation);
    try {
      return await operation;
    } finally {
      this.#turnStartOperations.delete(operation);
    }
  }

  async #performOwnedTurnStart(
    threadId: string,
    prompt: string,
    workspace: string,
    recordStarted: (turnId: string) => boolean,
  ): Promise<void> {
    this.#turnStartsInProgress.add(threadId);
    try {
      const { turnId } = await this.appServer.startTurn(
        threadId,
        prompt,
        workspace,
        this.#requiredAccessMode(),
      );
      const acceptedTurn = { threadId, turnId };
      this.#unrecordedAcceptedTurns.push(acceptedTurn);
      if (!recordStarted(turnId)) return;
      this.#unrecordedAcceptedTurns = this.#unrecordedAcceptedTurns.filter(
        (candidate) => candidate !== acceptedTurn,
      );
      this.#reconcileTurnStart(threadId, turnId);
    } finally {
      this.#turnStartsInProgress.delete(threadId);
    }
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
      this.#finishTurn(completion);
    }
    const unattended = this.#unattendedBeforeTurnStart;
    if (unattended?.threadId === threadId && unattended.turnId === turnId) {
      this.#unattendedBeforeTurnStart = undefined;
      this.handleUnattendedRequest(unattended);
    }
    const earlyStarts = this.#startedBeforeTurnRecorded.filter(
      (started) => started.threadId === threadId,
    );
    this.#startedBeforeTurnRecorded = this.#startedBeforeTurnRecorded.filter(
      (started) => started.threadId !== threadId,
    );
    for (const started of earlyStarts) {
      if (started.turnId !== turnId) {
        this.store.recordExternalThreadActivity(
          started.threadId,
          started.turnId,
          this.clock.now(),
        );
      }
    }
  }

  #finishTurn(turn: CompletedTurn): void {
    if (turn.status === "completed") {
      if (this.store.completeTurn(turn.threadId, turn.turnId, this.clock.now())) {
        this.#scheduleAutomaticAdvance();
      }
      return;
    }
    if (turn.error?.codexErrorInfo === "usageLimitExceeded") {
      this.handleUsageLimitExceeded({ threadId: turn.threadId, turnId: turn.turnId });
      return;
    }
    if (isTransientFailure(turn.error?.codexErrorInfo)) {
      const retry = this.store.recordTransientFailure(
        turn.threadId,
        turn.turnId,
        this.clock.now(),
      );
      if (retry && retry !== "exhausted") {
        this.#scheduleTransientRetry(retry);
        return;
      }
    }
    this.store.recordTurnNeedsAttention(
      turn.threadId,
      turn.turnId,
      turn.status,
      {
        code: "turn_failed",
        ...(turn.error?.codexErrorInfo === undefined
          ? {}
          : { details: { codexErrorInfo: turn.error.codexErrorInfo } }),
        message: turn.error?.message
          ?? `Codex Turn ${turn.status}; user action is required.`,
      },
      this.clock.now(),
    );
  }

  #scheduleTransientRetry(retry: TransientRetry): void {
    this.#transientRetry = this.#transientRetry
      .then(async () => {
        await waitUntil(
          this.clock,
          new Date(
            this.clock.now().getTime()
              + TRANSIENT_RETRY_INITIAL_MS * 2 ** (retry.attempt - 1),
          ),
          this.#stopping.signal,
        );
        if (this.#stopping.signal.aborted || !this.store.isQueueRunning()) return;
        const { threadId } = await this.appServer.resumeThread(retry.threadId);
        this.store.activateManagedThread(threadId);
        const prompt = await this.continuationPrompt();
        if (!this.store.canStartTurn(this.clock.now())) return;
        await this.#startOwnedTurn(
          threadId,
          prompt,
          retry.workspace,
          (turnId) => this.store.recordTransientRetryStarted(
            retry.taskId,
            threadId,
            turnId,
            this.clock.now(),
          ),
        );
      })
      .catch((error: unknown) => {
        if (this.#stopping.signal.aborted) return;
        this.store.recordTaskNeedsAttention(
          retry.taskId,
          serializeOperationalError(error),
          this.clock.now(),
        );
      });
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
      .catch((error: unknown) => {
        if (this.#stopping.signal.aborted) return;
        this.store.recordQuotaRecoveryFailure(
          quotaPause.taskId,
          serializeOperationalError(error),
          this.clock.now(),
        );
      });
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
        const refreshed = current.limitId === undefined
          ? selectApplicableQuota(rateLimits)
          : quotaForLimit(rateLimits, current.limitId);
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
    const prompt = await this.continuationPrompt();
    if (!this.store.canStartTurn(this.clock.now())) return;
    await this.#startOwnedTurn(
      threadId,
      prompt,
      current.workspace,
      (turnId) => this.store.recordContinuationTurnStarted(
        current.taskId,
        threadId,
        turnId,
        this.clock.now(),
      ),
    );
  }

  #requiredAccessMode(): AccessMode {
    if (!this.#accessMode) throw new Error("Queue Run Access Mode is unavailable.");
    return this.#accessMode;
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

  #scheduleCutoff(runPolicy: RunPolicy): void {
    if (runPolicy.kind !== "cutoff_time") return;
    void waitUntil(
      this.clock,
      new Date(runPolicy.cutoffTime),
      this.#stopping.signal,
    ).then(() => {
      this.store.pauseAtCutoff(runPolicy.cutoffTime, this.clock.now());
    }).catch(() => undefined);
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

function isTransientFailure(codexErrorInfo: unknown): boolean {
  if (
    codexErrorInfo === "internalServerError"
    || codexErrorInfo === "rateLimitExceeded"
    || codexErrorInfo === "serverOverloaded"
  ) return true;
  if (!isRecord(codexErrorInfo)) return false;
  return "httpConnectionFailed" in codexErrorInfo
    || "responseStreamConnectionFailed" in codexErrorInfo
    || "responseStreamDisconnected" in codexErrorInfo
    || "responseTooManyFailedAttempts" in codexErrorInfo;
}

function unattendedRequestMessage(kind: UnattendedRequest["kind"]): string {
  switch (kind) {
    case "command_approval":
      return "Codex requested command approval; user action is required.";
    case "file_change_approval":
      return "Codex requested file-change approval; user action is required.";
    case "permission_request":
      return "Codex requested additional permissions; user action is required.";
    case "user_input":
      return "Codex requested user input; user action is required.";
    case "mcp_elicitation":
      return "An MCP server requested user input; user action is required.";
  }
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
    case "queue/start": {
      const runPolicy = parseRunPolicy(request.params, "queue/start");
      return taskService.start(runPolicy, parseAccessMode(request.params, "queue/start"));
    }
    case "queue/resume": {
      const runPolicy = parseRunPolicy(request.params, "queue/resume");
      return taskService.start(runPolicy, parseAccessMode(request.params, "queue/resume"));
    }
    case "queue/pause":
      return taskService.pause();
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
    case "task/retry": {
      if (
        !isRecord(request.params)
        || !isTaskId(request.params.taskId)
        || typeof request.params.prompt !== "string"
      ) {
        throw new Error("task/retry requires a Task ID and a new prompt.");
      }
      return taskService.retry(request.params.taskId, request.params.prompt);
    }
    case "task/complete": {
      if (!isRecord(request.params) || !isTaskId(request.params.taskId)) {
        throw new Error("task/complete requires a Task ID.");
      }
      return taskService.complete(request.params.taskId);
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

function parseRunPolicy(value: unknown, method: string): RunPolicy {
  if (!isRecord(value) || !isRecord(value.runPolicy)) {
    throw new Error(`${method} requires a Run Policy.`);
  }
  const runPolicy = value.runPolicy;
  if (runPolicy.kind === "until_idle") return { kind: "until_idle" };
  if (
    runPolicy.kind === "cutoff_time"
    && typeof runPolicy.cutoffTime === "string"
  ) {
    try {
      return {
        cutoffTime: normalizeCutoffTime(runPolicy.cutoffTime),
        kind: "cutoff_time",
      };
    } catch {
      // Report the daemon method consistently below.
    }
  }
  throw new Error(`${method} received an invalid Run Policy.`);
}

function parseAccessMode(value: unknown, method: string): AccessMode {
  if (
    !isRecord(value)
    || (value.accessMode !== "configured" && value.accessMode !== "full")
  ) {
    throw new Error(`${method} requires an Access Mode.`);
  }
  return value.accessMode;
}
