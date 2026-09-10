import { appendFileSync, chmodSync, closeSync, openSync } from "node:fs";

export interface EventLogRecord {
  errorSummary?: {
    code: string;
    message: string;
  };
  eventType: string;
  quotaResetAt?: string;
  stateTransition?: {
    from: string | null;
    to: string;
  };
  taskId?: number;
  threadId?: string;
  timestamp: string;
  turnId?: string;
}

interface DatabaseEventPayload {
  errorCode?: string | null;
  eventType: string;
  fromState?: string | null;
  quotaResetAt?: string | null;
  taskId?: number | null;
  threadId?: string | null;
  toState?: string | null;
  turnId?: string | null;
}

export class EventLog {
  constructor(
    private readonly eventLogPath: string,
    private readonly now: () => Date = () => new Date(),
  ) {
    closeSync(openSync(eventLogPath, "a", 0o600));
    chmodSync(eventLogPath, 0o600);
  }

  record(input: Omit<EventLogRecord, "timestamp">): void {
    const record: EventLogRecord = {
      timestamp: this.now().toISOString(),
      eventType: input.eventType,
      ...(input.taskId === undefined ? {} : { taskId: input.taskId }),
      ...(input.threadId === undefined ? {} : { threadId: input.threadId }),
      ...(input.turnId === undefined ? {} : { turnId: input.turnId }),
      ...(input.stateTransition === undefined
        ? {}
        : { stateTransition: input.stateTransition }),
      ...(input.quotaResetAt === undefined
        ? {}
        : { quotaResetAt: input.quotaResetAt }),
      ...(input.errorSummary === undefined
        ? {}
        : { errorSummary: input.errorSummary }),
    };
    appendFileSync(this.eventLogPath, `${JSON.stringify(record)}\n`, {
      encoding: "utf8",
      mode: 0o600,
    });
  }

  recordDatabaseEvent(serializedPayload: string): void {
    const payload: unknown = JSON.parse(serializedPayload);
    if (!isDatabaseEventPayload(payload)) {
      throw new Error("Invalid internal event-log payload");
    }
    this.record({
      eventType: payload.eventType,
      ...(payload.taskId == null ? {} : { taskId: payload.taskId }),
      ...(payload.threadId == null ? {} : { threadId: payload.threadId }),
      ...(payload.turnId == null ? {} : { turnId: payload.turnId }),
      ...(payload.toState == null
        ? {}
        : {
          stateTransition: {
            from: payload.fromState ?? null,
            to: payload.toState,
          },
        }),
      ...(payload.quotaResetAt == null
        ? {}
        : { quotaResetAt: payload.quotaResetAt }),
      ...(payload.errorCode == null
        ? {}
        : { errorSummary: sanitizedErrorSummary(payload.errorCode) }),
    });
  }
}

export function sanitizedErrorSummary(code: string): {
  code: string;
  message: string;
} {
  return {
    code,
    message: "An operational error was recorded; inspect Queue status for details.",
  };
}

function isDatabaseEventPayload(value: unknown): value is DatabaseEventPayload {
  if (!isRecord(value) || typeof value.eventType !== "string") return false;
  return optionalNumber(value.taskId)
    && optionalString(value.threadId)
    && optionalString(value.turnId)
    && optionalString(value.fromState)
    && optionalString(value.toState)
    && optionalString(value.quotaResetAt)
    && optionalString(value.errorCode);
}

function optionalNumber(value: unknown): boolean {
  return value == null || (typeof value === "number" && Number.isSafeInteger(value));
}

function optionalString(value: unknown): boolean {
  return value == null || typeof value === "string";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
