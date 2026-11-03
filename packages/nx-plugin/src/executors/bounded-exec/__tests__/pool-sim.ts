import { admit, type PoolAsk } from '../pool-admission.js';

/**
 * A run's leaves against its CPU pool, as a discrete-event simulation on a virtual clock (specs/cowshed/05_gateway.md,
 * "Without mbx, every Nx run budgets itself"). Pure: a leaf is numbers (its work, what it truly holds at a count of
 * workers, what its record says) and time jumps from one event to the next, so nothing is spawned or slept on and a whole
 * run replays in microseconds. The pool's decision is the real `admit`; what is modelled is everything around it:
 * Nx's `--parallel` slots, which a leaf holds while it waits in the pool's queue, the tokens a grant lends, and the
 * memory the granted leaves truly hold, which is what the kernel judges and the pool only estimates.
 *
 * A policy is a value over this one engine (`cpuOnly`, `today`, `scaled`, `cap`, `liveHeadroom`, `scaledRetry`), as the candidates of
 * `packages/cowshed/crates/cowshed-admission-sim` are, so every candidate is judged on the same worlds.
 *
 * The kernel is modelled too: while the leaves and what lies outside them hold more than the run's memory it kills the
 * largest, and a policy may run a killed leaf again once, alone. Leaves may reach their peak over `ramp` seconds, which is
 * what a decision that samples live memory sees.
 */

export const MiB = 1024 ** 2;
export const GiB = 1024 ** 3;

/** What a leaf's last run recorded: what it held at its peak, and the workers it was lent then. */
export interface SimRecord {
  readonly peak: number;
  readonly parallelism: number;
}

/**
 * One leaf. Lent `w` workers it holds `(base + perWorker × w) × growth` bytes at its peak and takes `work ÷ w` virtual
 * seconds: a `bun test` process has one worker and `perWorker` 0, a Vitest run one per token it is lent. `growth` is
 * how much more its suite holds now than when its record was made (1: nothing has changed). Its peak is held from its
 * start to its end, the worst case for a pool that charges peaks: every leaf at its peak at once.
 */
export interface SimLeaf {
  readonly name: string;
  readonly want: number;
  readonly work: number;
  readonly base: number;
  readonly perWorker: number;
  readonly growth: number;
  readonly record: SimRecord | null;
}

/** The run: its memory (the cgroup's limit), its cores (the pool's tokens), what is outside every leaf, and the leaves. */
export interface SimWorld {
  readonly capacity: number;
  readonly cores: number;
  /** Bytes held by what no leaf is charged for: Nx itself, the runner, a tmpfs. */
  readonly baseline: number;
  readonly leaves: readonly SimLeaf[];
  /** Seconds a granted leaf takes to reach its peak, from nothing; 0 (the default): it holds its peak from its start. */
  readonly ramp?: number;
}

/** What a leaf truly holds when it is lent `workers`. */
export function truthAt(leaf: SimLeaf, workers: number): number {
  return Math.round((leaf.base + leaf.perWorker * workers) * leaf.growth);
}

/** The record a run leaves when the leaf was lent `parallelism` workers and its suite held what it does at `growth` 1. */
export function recordedAt(leaf: SimLeaf, parallelism: number): SimRecord {
  return { peak: Math.round(leaf.base + leaf.perWorker * parallelism), parallelism };
}

/** How a run is admitted: its `--parallel` slots, whether its pool charges memory, and the ask a leaf sends. */
export interface Policy {
  readonly name: string;
  readonly slots: (cores: number) => number;
  readonly charges: boolean;
  /** The share of the run's memory the pool admits against; the leaves are judged against all of it. */
  readonly budget: number;
  /** Admit by what the run holds live, not by the charges of the leaves holding grants. */
  readonly live?: boolean;
  /** A leaf the kernel killed runs again once, alone, instead of failing the run. */
  readonly retry?: boolean;
  readonly ask: (leaf: SimLeaf) => PoolAsk;
}

/** Before memory admission (nx-plugin 0.7.5 and earlier): CPU tokens only, every target side by side. */
export const cpuOnly: Policy = {
  name: 'cpu tokens only',
  slots: (cores) => cores,
  charges: false,
  budget: 1,
  ask: (leaf) => ({ want: leaf.want, memory: 0 }),
};

/** The pool on SmoothBricks main: a leaf is charged its last recorded peak, whatever it is lent now. */
export const today: Policy = {
  name: 'charge the last peak (main)',
  slots: (cores) => cores,
  charges: true,
  budget: 1,
  ask: (leaf) => ({ want: leaf.want, memory: leaf.record?.peak ?? 0 }),
};

/** The pool with the parallelism its peak was measured under, charged in proportion to the workers lent. */
export const scaled: Policy = {
  name: 'charge the peak per worker',
  slots: (cores) => cores,
  charges: true,
  budget: 1,
  ask: (leaf) =>
    leaf.record === null
      ? { want: leaf.want, memory: 0 }
      : { want: leaf.want, memory: leaf.record.peak, parallelism: leaf.record.parallelism },
};

/**
 * The pool charging the peak per worker, with two sensitivities the pool has no such knob for: a leaf with no record is
 * charged `unrecorded` bytes instead of nothing, and the pool admits against `budget` of the run's memory instead of all of it.
 */
export const scaledWith = (unrecorded: number, budget: number): Policy => ({
  name: `charge the peak per worker, ${(unrecorded / GiB).toFixed(0)} GiB unrecorded, ${Math.round(budget * 100)}% budget`,
  slots: (cores) => cores,
  charges: true,
  budget,
  ask: (leaf) =>
    leaf.record === null
      ? { want: leaf.want, memory: unrecorded }
      : { want: leaf.want, memory: leaf.record.peak, parallelism: leaf.record.parallelism },
});

/** No memory accounting: Nx runs at most `slots` targets side by side (`--parallel=slots`). */
export const cap = (slots: number): Policy => ({
  name: `--parallel=${slots}`,
  slots: () => slots,
  charges: false,
  budget: 1,
  ask: (leaf) => ({ want: leaf.want, memory: 0 }),
});

/**
 * Live-headroom admission: a leaf is admitted while its charge, per worker, fits under the run's memory less what the run
 * holds when the pool decides (the cgroup's \`memory.current\`, or its anonymous and shared memory). Records give the
 * charge; the live reading catches what lies outside the leaves and what a suite holds that its record does not say. A
 * leaf granted earlier in the same decision is not yet resident, so it counts by its charge; one granted before it is
 * seen only as far as it has ramped. When nothing runs the head is admitted whatever it is charged.
 */
export const liveHeadroom: Policy = {
  name: 'live headroom',
  slots: (cores) => cores,
  charges: false,
  budget: 1,
  live: true,
  ask: scaled.ask,
};

/** The per-worker charge, and a leaf the kernel killed runs again once, alone, instead of failing the run. */
export const scaledRetry: Policy = { ...scaled, name: 'per worker + a killed leaf runs again alone', retry: true };

/** Live-headroom admission, and a leaf the kernel killed runs again once, alone. */
export const liveHeadroomRetry: Policy = {
  ...liveHeadroom,
  name: 'live headroom + a killed leaf runs again alone',
  retry: true,
};

/** What a run came to. */
export interface Outcome {
  /** The most bytes the leaves and the baseline held beyond the run's memory at any instant, before the kernel acted. */
  readonly overshoot: number;
  /** The most bytes they held at any instant, before the kernel acted. */
  readonly peak: number;
  /** Virtual seconds until the last leaf ended or was killed for good. */
  readonly makespan: number;
  /** A leaf waited for ever: the pool's queue held it while nothing ran. */
  readonly starved: boolean;
  /** Every grant went to the leaf that had waited longest, a leaf run again aside. */
  readonly fifo: boolean;
  /** Leaves the kernel killed for memory, a leaf killed again counted again. */
  readonly kills: number;
  /** Leaves that ended killed with no run left: the run fails. */
  readonly failed: number;
}

interface Queued extends PoolAsk {
  readonly leaf: SimLeaf;
  readonly order: number;
  /** A leaf the kernel killed, run again: granted ahead of everything, and only when nothing else runs. */
  readonly alone: boolean;
}

interface Running {
  readonly queued: Queued;
  readonly tokens: number;
  readonly charge: number;
  readonly truth: number;
  readonly start: number;
  readonly end: number;
}

/** Run `world` under `policy`. */
export function simulate(policy: Policy, world: SimWorld): Outcome {
  const ramp = world.ramp ?? 0;
  const pending = [...world.leaves];
  const queue: Queued[] = [];
  let running: Running[] = [];
  let now = 0;
  let free = world.cores;
  let held = 0;
  let slotsUsed = 0;
  let arrived = 0;
  let lastGranted = -1;
  let fifo = true;
  let peak = 0;
  let kills = 0;
  let failed = 0;
  const slots = policy.slots(world.cores);

  // What a running leaf holds now: its peak, reached in a straight line over `ramp` seconds from its start.
  const heldNow = (leaf: Running): number =>
    ramp === 0 ? leaf.truth : Math.round(leaf.truth * Math.min(1, (now - leaf.start) / ramp));
  const live = (): number => world.baseline + running.reduce((sum, leaf) => sum + heldNow(leaf), 0);

  // Nx starts a target while it has a slot, and the target holds its slot while it waits in the pool's queue.
  const start = (): void => {
    while (slotsUsed < slots) {
      const leaf = pending.shift();
      if (leaf === undefined) return;
      slotsUsed += 1;
      queue.push({ ...policy.ask(leaf), leaf, order: arrived, alone: false });
      arrived += 1;
    }
  };
  const grant = (request: Queued, tokens: number, charge: number): void => {
    if (!request.alone) {
      if (request.order < lastGranted) fifo = false;
      lastGranted = request.order;
    }
    free -= tokens;
    held += charge;
    running.push({
      queued: request,
      tokens,
      charge,
      truth: truthAt(request.leaf, tokens),
      start: now,
      end: now + request.leaf.work / tokens,
    });
  };
  const ledger = (retry: boolean): { capacity: number; held: number } | null =>
    policy.charges || retry ? { capacity: world.capacity * policy.budget, held: policy.charges ? held : 0 } : null;
  const drain = (): void => {
    // Who holds a grant, as the pool counts it: a leaf run again alone is admitted by the pool's own decision.
    const holding = { holders: running.length, alone: running.some((leaf) => leaf.queued.alone) };
    if (policy.live && queue[0]?.alone !== true && !holding.alone) {
      const resident = live();
      let granted = 0;
      let charged = 0;
      for (let next = queue[0]; next !== undefined && !next.alone; next = queue[0]) {
        const room = world.capacity - resident - charged;
        const alone = running.length === 0 && granted === 0;
        if (next.memory > room && !alone) return;
        const [fit] = admit([next], free, { capacity: next.memory > room ? world.capacity : room, held: 0 }).grants;
        if (fit === undefined) return;
        grant(next, fit.tokens, fit.charge);
        queue.shift();
        granted += 1;
        charged += fit.charge;
      }
      return;
    }
    const admission = admit(queue, free, ledger(queue[0]?.alone === true), holding);
    for (const { request, tokens, charge } of admission.grants) grant(request, tokens, charge);
    queue.splice(0, admission.grants.length);
  };
  // The kernel's judgement at this instant: while the leaves hold more than the run's memory it kills the largest.
  const settle = (): boolean => {
    let total = live();
    peak = Math.max(peak, total);
    let killed = false;
    while (total > world.capacity) {
      let victim: Running | undefined;
      for (const leaf of running) {
        if (victim === undefined || heldNow(leaf) > heldNow(victim)) victim = leaf;
      }
      if (victim === undefined) break;
      const dead = victim;
      running = running.filter((leaf) => leaf !== dead);
      free += dead.tokens;
      held -= dead.charge;
      kills += 1;
      killed = true;
      if (policy.retry && !dead.queued.alone) {
        queue.unshift({ ...dead.queued, alone: true });
      } else {
        failed += 1;
        slotsUsed -= 1;
      }
      total = live();
    }
    return killed;
  };
  const step = (): void => {
    do {
      start();
      drain();
    } while (settle());
  };

  step();
  while (running.length > 0) {
    // The next change: a leaf ends, or a leaf reaches its peak.
    const next = Math.min(
      ...running.flatMap((leaf) => [leaf.end, ...(leaf.start + ramp > now ? [leaf.start + ramp] : [])]),
    );
    now = next;
    const ended = running.filter((leaf) => leaf.end <= next);
    running = running.filter((leaf) => leaf.end > next);
    for (const leaf of ended) {
      free += leaf.tokens;
      held -= leaf.charge;
      slotsUsed -= 1;
    }
    step();
  }
  return {
    overshoot: Math.max(0, peak - world.capacity),
    peak,
    makespan: now,
    starved: queue.length > 0 || pending.length > 0,
    fifo,
    kills,
    failed,
  };
}

/** What a run's records are like: the share of leaves with none, how much more a suite holds than it did, the baseline. */
export interface Regime {
  readonly missing: number;
  readonly stale: number;
  readonly baseline: number;
}

/** Every leaf has a record, and it is what the leaf held when it was last lent as many workers as it records. */
export const EXACT: Regime = { missing: 0, stale: 1, baseline: 0 };

/** A small deterministic generator (mulberry32): the same seed is the same run on every host. */
function random(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let mixed = Math.imul(state ^ (state >>> 15), 1 | state);
    mixed = (mixed + Math.imul(mixed ^ (mixed >>> 7), 61 | mixed)) ^ mixed;
    return ((mixed ^ (mixed >>> 14)) >>> 0) / 4294967296;
  };
}

/**
 * A run shaped like the downstream repository's test step on its 32 vCPU, 32 GiB runner (CI runs 571 to 618, 2026-11-01
 * to 03): 125 single-process `bun test` leaves, most of 0.5 to 2 GiB and a third of 2.5 to 5.5 GiB (the failed leaves'
 * charges the logs name are 3.2, 3.4, 4.2 and 5.2 GB), and four Vitest runs that default to 31 workers. What a Vitest
 * worker holds is not in the logs: 0.1 to 0.4 GiB a worker over 0.5 to 1.5 GiB is an assumption, as are the work
 * figures. Each leaf's record was made when it was lent one of 1, 2, 4, 8, 22 or 31 workers.
 */
export function downstreamWorld(seed: number, regime: Regime, ramp = 0): SimWorld {
  const next = random(seed);
  const between = (low: number, high: number): number => low + next() * (high - low);
  const leaves: SimLeaf[] = [];
  const add = (leaf: Omit<SimLeaf, 'record' | 'growth'>): void => {
    const measuredAt = [1, 2, 4, 8, 22, 31].filter((workers) => workers <= leaf.want);
    const at = measuredAt[Math.floor(next() * measuredAt.length)] ?? 1;
    const bare = { ...leaf, growth: regime.stale === 1 ? 1 : between(1, regime.stale), record: null };
    leaves.push({ ...bare, record: next() < regime.missing ? null : recordedAt(bare, at) });
  };
  for (let at = 0; at < 125; at += 1) {
    const large = next() < 0.3;
    add({
      name: `bun-${at}`,
      want: 1,
      work: between(5, 150),
      base: Math.round((large ? between(2.5, 5.5) : between(0.5, 2)) * GiB),
      perWorker: 0,
    });
  }
  for (let at = 0; at < 4; at += 1) {
    add({
      name: `vitest-${at}`,
      want: 31,
      work: between(300, 1500),
      base: Math.round(between(0.5, 1.5) * GiB),
      perWorker: Math.round(between(0.1, 0.4) * GiB),
    });
  }
  // Nx starts targets in graph order, which is neither size nor kind: a fixed shuffle.
  for (let at = leaves.length - 1; at > 0; at -= 1) {
    const swap = Math.floor(next() * (at + 1));
    const held = leaves[at];
    const other = leaves[swap];
    if (held !== undefined && other !== undefined) {
      leaves[at] = other;
      leaves[swap] = held;
    }
  }
  return { capacity: 32 * GiB, cores: 32, baseline: regime.baseline, ramp, leaves };
}

/** What a policy came to over many runs. */
export interface Judgement {
  readonly policy: string;
  /** The share of runs in which the leaves held more than the run's memory at some instant, before the kernel acted. */
  readonly overCommitted: number;
  /** In those runs, the most they held beyond it, in GiB, on average. */
  readonly meanOvershootGiB: number;
  /** The share of runs in which the kernel killed a leaf. */
  readonly killed: number;
  /** The share of runs that ended with a leaf killed and not run again, or killed again: the run fails. */
  readonly failed: number;
  /** The most they held at any instant before the kernel acted, in GiB, on average over all runs. */
  readonly meanPeakGiB: number;
  /** Virtual seconds until the last leaf ended, on average. */
  readonly meanMakespan: number;
  readonly starved: number;
}

/** Judge `policy` on the runs `worlds` makes. */
export function judge(policy: Policy, worlds: readonly SimWorld[]): Judgement {
  const outcomes = worlds.map((world) => simulate(policy, world));
  const over = outcomes.filter((outcome) => outcome.overshoot > 0);
  const share = (count: number): number => count / outcomes.length;
  const mean = (values: readonly number[]): number =>
    values.length === 0 ? 0 : values.reduce((sum, value) => sum + value, 0) / values.length;
  return {
    policy: policy.name,
    overCommitted: share(over.length),
    meanOvershootGiB: mean(over.map((outcome) => outcome.overshoot / GiB)),
    killed: share(outcomes.filter((outcome) => outcome.kills > 0).length),
    failed: share(outcomes.filter((outcome) => outcome.failed > 0).length),
    meanPeakGiB: mean(outcomes.map((outcome) => outcome.peak / GiB)),
    meanMakespan: mean(outcomes.map((outcome) => outcome.makespan)),
    starved: outcomes.filter((outcome) => outcome.starved).length,
  };
}
