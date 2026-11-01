/**
 * The admission decision of a CPU pool (`cpu-pool.ts`; specs/cowshed/05_gateway.md, "Without mbx, every Nx run budgets
 * itself"): which queued requests a pool grants, in FIFO order, from the tokens it has free and, for a run's own pool,
 * the run's memory. Pure: the pool reads its jobserver fifo for the free count and keeps the holders' charges, and this
 * decides.
 *
 * Every grant needs both. The head takes `min(want, free)` tokens once one is free, and only while the charges of the
 * leaves holding grants plus its own fit the run's memory; a head that cannot be granted holds up every request behind
 * it, and no request holds part of a grant while it waits for the rest. A leaf is charged its task's last recorded
 * memory peak, and nothing while its task has none. A leaf whose own charge alone exceeds the run's memory could never
 * fit, so it is granted once nothing else holds memory, and the grant says so.
 */

/** What a queued request asks: whole tokens, at least one, and its charge in bytes (0 for a task with no peak). */
export interface PoolAsk {
  readonly want: number;
  readonly memory: number;
}

/** The run's memory a pool charges against, and the bytes its holders are charged; null for a pool that charges none. */
export interface MemoryLedger {
  readonly capacity: number;
  readonly held: number;
}

/** One request's grant: the request, its tokens, and whether its charge alone exceeds the run's memory. */
export interface PoolGrant<Ask extends PoolAsk> {
  readonly request: Ask;
  readonly tokens: number;
  readonly oversized: boolean;
}

/**
 * The grants of the queue's first requests, in its order, and what the first request left waiting waits for: free
 * tokens (`cpu`) or the run's memory (`memory`). `waiting` is null when every request was granted.
 */
export interface Admission<Ask extends PoolAsk> {
  readonly grants: readonly PoolGrant<Ask>[];
  readonly waiting: 'cpu' | 'memory' | null;
}

export function admit<Ask extends PoolAsk>(
  queue: readonly Ask[],
  available: number,
  memory: MemoryLedger | null,
): Admission<Ask> {
  const grants: PoolGrant<Ask>[] = [];
  let free = available;
  let held = memory?.held ?? 0;
  for (const request of queue) {
    const oversized = memory !== null && request.memory > memory.capacity;
    if (memory !== null && held + request.memory > memory.capacity && !(oversized && held === 0)) {
      return { grants, waiting: 'memory' };
    }
    const tokens = Math.min(request.want, free);
    if (tokens === 0) {
      return { grants, waiting: 'cpu' };
    }
    free -= tokens;
    held += request.memory;
    grants.push({ request, tokens, oversized });
  }
  return { grants, waiting: null };
}
