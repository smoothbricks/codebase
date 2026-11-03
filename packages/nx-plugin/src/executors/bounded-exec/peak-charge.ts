import { join } from 'node:path';
import { readRecord, VERDICT_FILE } from './verdict.js';

/** What the task's last record charges its next run: the peak and the parallelism it was measured under. */
export type LastPeak =
  | { ok: true; peakBytes: number | null; parallelism: number | null }
  | { ok: false; error: string };

/**
 * The peak the task's last record in `directory` names: its charge in its run's pool. Null when the task has no record
 * or its record no peak; a record that cannot be read is an error naming it, never a charge of nothing. It decides what a
 * pool charges a run, a value that differs between runs of one task key, so the key leaves it out (README, "Bounded
 * exec key").
 */
export async function readLastPeak(directory: string): Promise<LastPeak> {
  try {
    const record = await readRecord(directory);
    return { ok: true, peakBytes: record?.peakBytes ?? null, parallelism: record?.cpu?.tokens ?? null };
  } catch (error) {
    return {
      ok: false,
      error: `the task's last record ${join(directory, VERDICT_FILE)} cannot be read: ${error instanceof Error ? error.message : String(error)}`,
    };
  }
}
