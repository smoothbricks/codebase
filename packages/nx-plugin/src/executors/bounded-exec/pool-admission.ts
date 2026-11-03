/**
 * The admission decision of a CPU pool (`cpu-pool.ts`; specs/cowshed/05_gateway.md, "Without mbx, every Nx run budgets
 * itself"): which queued requests a pool grants, in FIFO order, from the tokens it has free and, for a run's own pool,
 * the run's memory. Pure: the pool reads its jobserver fifo for the free count and keeps the holders' charges, and this
 * decides.
 *
 * Every grant needs both. The head takes `min(want, free)` tokens once one is free, and only while the charges of the
 * leaves holding grants plus its own fit the run's memory; a head that cannot be granted holds up every request behind
 * it, and no request holds part of a grant while it waits for the rest. A leaf is charged its task's last recorded
 * memory peak, and nothing while its task has none. A runner's memory grows with the parallelism it is lent (a Vitest
 * worker is a process of its own), so a peak measured while the leaf was lent `parallelism` tokens is charged in
 * proportion to the tokens it is lent now when that is more, never less: the baseline a leaf holds however few workers
 * it has makes a smaller grant no cheaper. When its whole ask does not fit, the head is lent the most tokens whose
 * charge does. A leaf whose own charge alone exceeds the run's memory could never fit, so it is granted once nothing
 * else holds memory, on one token when its peak names the parallelism it was measured under, and the grant says so.
 *
 * A leaf the kernel killed for memory, run again (`alone`), is granted only while nothing holds a grant, ahead of the
 * leaves behind it, and while it runs nothing else is granted: a leaf that was killed beside others runs by itself.
 */

/**
 * What a queued request asks: whole tokens, at least one, its charge in bytes (0 for a task with no peak), and the
 * tokens that charge was measured under. Without them the charge is flat: it is the same however many tokens are lent.
 */
export interface PoolAsk {
  readonly want: number;
  readonly memory: number;
  readonly parallelism?: number | undefined;
  /** A leaf the kernel killed for memory, run again: granted while nothing else holds a grant, and alone. */
  readonly alone?: boolean | undefined;
}

/** Who holds a grant when the pool decides: how many leaves, and whether one of them runs alone. */
export interface Holding {
  readonly holders: number;
  readonly alone: boolean;
}

const NOTHING_HELD: Holding = { holders: 0, alone: false };
/** The run's memory a pool charges against, and the bytes its holders are charged; null for a pool that charges none. */
export interface MemoryLedger {
  readonly capacity: number;
  readonly held: number;
}

/**
 * One request's grant: the request, its tokens, the bytes it is charged at those tokens, and whether its charge alone
 * exceeds the run's memory.
 */
export interface PoolGrant<Ask extends PoolAsk> {
  readonly request: Ask;
  readonly tokens: number;
  readonly charge: number;
  readonly oversized: boolean;
}

/**
 * The grants of the queue's first requests, in its order, and what the first request left waiting waits for: free
 * tokens (`cpu`), the run's memory (`memory`) or the grants others hold (`alone`). `waiting` is null when every request was granted.
 */
export interface Admission<Ask extends PoolAsk> {
  readonly grants: readonly PoolGrant<Ask>[];
  readonly waiting: 'cpu' | 'memory' | 'alone' | null;
}

/** The bytes `ask` is charged when it is lent `tokens`: its peak, scaled up when that is more than it was measured under. */
export function chargeAt(ask: PoolAsk, tokens: number): number {
  const measured = ask.parallelism;
  return measured === undefined || tokens <= measured ? ask.memory : Math.ceil((ask.memory * tokens) / measured);
}

/** The most tokens, up to `most`, whose charge fits `room` bytes; none when even the peak itself does not. */
function tokensFitting(ask: PoolAsk, most: number, room: number): number {
  if (ask.memory > room) {
    return 0;
  }
  const measured = ask.parallelism;
  if (measured === undefined || ask.memory === 0 || most <= measured) {
    return most;
  }
  return Math.min(most, Math.floor((room * measured) / ask.memory));
}

export function admit<Ask extends PoolAsk>(
  queue: readonly Ask[],
  available: number,
  memory: MemoryLedger | null,
  holding: Holding = NOTHING_HELD,
): Admission<Ask> {
  const grants: PoolGrant<Ask>[] = [];
  if (holding.alone) {
    return { grants, waiting: queue.length === 0 ? null : 'alone' };
  }
  let free = available;
  let held = memory?.held ?? 0;
  for (const request of queue) {
    if (request.alone === true && holding.holders + grants.length > 0) {
      return { grants, waiting: 'alone' };
    }
    const oversized = memory !== null && request.memory > memory.capacity;
    const room = memory === null ? Number.POSITIVE_INFINITY : memory.capacity - held;
    if (request.memory > room && !(oversized && held === 0)) {
      return { grants, waiting: 'memory' };
    }
    const most = Math.min(request.want, free);
    if (most === 0) {
      return { grants, waiting: 'cpu' };
    }
    const tokens = oversized ? (request.parallelism === undefined ? most : 1) : tokensFitting(request, most, room);
    const charge = chargeAt(request, tokens);
    free -= tokens;
    held += charge;
    grants.push({ request, tokens, charge, oversized });
    if (request.alone === true) {
      return { grants, waiting: grants.length < queue.length ? 'alone' : null };
    }
  }
  return { grants, waiting: null };
}
