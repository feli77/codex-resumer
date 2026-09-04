export type RunPolicy =
  | { kind: "until_idle" }
  | { cutoffTime: string; kind: "cutoff_time" };

export function normalizeCutoffTime(value: string): string {
  if (!/(?:Z|[+-]\d{2}:\d{2})$/i.test(value)) {
    throw new Error("Cutoff Time requires an explicit timezone.");
  }
  const cutoffTime = new Date(value);
  if (Number.isNaN(cutoffTime.getTime())) {
    throw new Error(`Invalid Cutoff Time: ${value}`);
  }
  return cutoffTime.toISOString();
}
