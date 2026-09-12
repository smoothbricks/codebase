export interface DispatchScheduler {
  /** Return an idempotent cancellation function. Do not run the callback inline. */
  schedule(callback: () => void): () => void;
}

export function microtaskScheduler(): DispatchScheduler {
  return {
    schedule(callback) {
      let cancelled = false;
      queueMicrotask(() => { if (!cancelled) callback(); });
      return () => { cancelled = true; };
    },
  };
}

/** One instance per scenario. A flush drains the same successor waves as production. */
export function manualScheduler() {
  const pending = new Set<() => void>();
  return {
    schedule(callback: () => void): () => void {
      pending.add(callback);
      return () => { pending.delete(callback); };
    },
    flush(): void {
      while (pending.size > 0) {
        const callback = pending.values().next().value;
        if (callback) { pending.delete(callback); callback(); }
      }
    },
    get pending(): number { return pending.size; },
  } satisfies DispatchScheduler & { flush(): void; readonly pending: number };
}
