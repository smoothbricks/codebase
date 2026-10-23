import type { RetryEvent, RetryShell } from '../retry.js';

/**
 * A retry shell that records waits and retry notices instead of waiting. Jitter is off (`random` always 1) so the
 * backoff ladder a test asserts is exact.
 */
export function recordingRetryShell(): { shell: RetryShell; sleeps: number[]; retries: RetryEvent[] } {
  const sleeps: number[] = [];
  const retries: RetryEvent[] = [];
  return {
    sleeps,
    retries,
    shell: {
      sleep: async (milliseconds) => {
        sleeps.push(milliseconds);
      },
      random: () => 1,
      retrying: (event) => {
        retries.push(event);
      },
    },
  };
}
