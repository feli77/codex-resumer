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
