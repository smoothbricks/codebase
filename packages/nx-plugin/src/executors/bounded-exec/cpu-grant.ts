/**
 * The CPU grant a run's record names (verdict.ts keeps the record; this is what it says of the pool). The tokens are the
 * parallelism the record's peak was measured under, which the task's next run reads (peak-charge.ts) to charge that
 * peak in proportion to the parallelism it is lent. It is scheduling metadata: a record that names none, or names one
 * that is malformed, changes no verdict a passing run records.
 */
export interface CpuGrantRecord {
  readonly tokens: number;
  readonly want: number;
  readonly waitedMs: number;
}

/** What a record's `cpu` field holds: no grant, a grant, or something that is no grant. */
export function parseCpuGrant(value: unknown): CpuGrantRecord | undefined | 'malformed' {
  if (value === undefined) {
    return undefined;
  }
  if (typeof value !== 'object' || value === null) {
    return 'malformed';
  }
  const { tokens, want, waitedMs } = value as Record<string, unknown>;
  if (
    !(typeof tokens === 'number' && Number.isSafeInteger(tokens) && tokens >= 1) ||
    !(typeof want === 'number' && Number.isSafeInteger(want) && want >= 1) ||
    !(typeof waitedMs === 'number' && Number.isFinite(waitedMs) && waitedMs >= 0)
  ) {
    return 'malformed';
  }
  return { tokens, want, waitedMs };
}
