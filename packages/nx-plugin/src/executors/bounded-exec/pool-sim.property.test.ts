import { describe, expect, it } from 'bun:test';
import fc from 'fast-check';
import {
  cap,
  cpuOnly,
  downstreamWorld,
  EXACT,
  GiB,
  judge,
  liveHeadroom,
  liveHeadroomRetry,
  MiB,
  type Policy,
  type Regime,
  recordedAt,
  type SimLeaf,
  type SimWorld,
  scaled,
  scaledRetry,
  scaledWith,
  simulate,
  today,
} from './__tests__/pool-sim.js';
import { admit, type MemoryLedger, type PoolAsk } from './pool-admission.js';

/**
 * The run pool judged as a simulation (`__tests__/pool-sim.ts`): random runs on a virtual clock, the real `admit`
 * deciding, and a model of what each leaf truly holds, which the kernel judges and the pool can only estimate. What the
 * pool claims is that the leaves it has granted never hold more than the run's memory. That is a property of the pool
 * together with the records it is told, so each property names the records it assumes.
 */

const SEED = 20261103;

const capacities = fc.constantFrom(16 * GiB, 32 * GiB, 64 * GiB);

function leafArb(cores: number, regime: Regime): fc.Arbitrary<SimLeaf> {
  return fc
    .record({
      workers: fc.boolean(),
      baseMiB: fc.integer({ min: 300, max: 6144 }),
      perWorkerMiB: fc.integer({ min: 64, max: 1024 }),
      want: fc.integer({ min: 2, max: Math.max(2, cores - 1) }),
      work: fc.integer({ min: 5, max: 1500 }),
      recordAt: fc.integer({ min: 1, max: 32 }),
      roll: fc.double({ min: 0, max: 1, noNaN: true }),
      growth: regime.stale === 1 ? fc.constant(1) : fc.double({ min: 1, max: regime.stale, noNaN: true }),
    })
    .map((raw): SimLeaf => {
      // A `bun test` process is one worker of fixed size; a Vitest run is a worker per token, each its own process.
      const bare = {
        name: 'leaf',
        want: raw.workers ? raw.want : 1,
        work: raw.workers ? raw.work : Math.min(raw.work, 120),
        base: raw.baseMiB * MiB,
        perWorker: raw.workers ? raw.perWorkerMiB * MiB : 0,
        growth: raw.growth,
        record: null,
      };
      const measuredAt = Math.min(raw.recordAt, bare.want);
      return { ...bare, record: raw.roll < regime.missing ? null : recordedAt(bare, measuredAt) };
    });
}

function worldArb(regime: Regime): fc.Arbitrary<SimWorld> {
  return fc
    .tuple(capacities, fc.integer({ min: 4, max: 32 }))
    .chain(([capacity, cores]) =>
      fc
        .array(leafArb(cores, regime), { minLength: 1, maxLength: 40 })
        .map((leaves) => ({ capacity, cores, baseline: regime.baseline, leaves })),
    );
}

const GiBs = (bytes: number): string => `${(bytes / GiB).toFixed(1)} GiB`;

/** The pool's claim: the leaves it granted never held more than the run's memory. */
function holdsTheRun(policy: Policy, regime: Regime): fc.IPropertyWithHooks<[SimWorld]> {
  return fc.property(worldArb(regime), (world) => {
    const outcome = simulate(policy, world);
    expect(
      outcome.overshoot,
      `${policy.name}: ${world.leaves.length} leaves on ${world.cores} cores held ${GiBs(outcome.peak)} of ${GiBs(world.capacity)}`,
    ).toBe(0);
  });
}

describe('a run pool over the simulation', () => {
  it("is SmoothBricks main's decision when no peak names a parallelism", () => {
    // `today` is the admission with the asks main sends: the peak alone. The decision on those asks must be main's.
    const admitMain = <Ask extends { want: number; memory: number }>(
      queue: readonly Ask[],
      available: number,
      memory: MemoryLedger | null,
    ): { grants: { request: Ask; tokens: number; oversized: boolean }[]; waiting: 'cpu' | 'memory' | null } => {
      const grants: { request: Ask; tokens: number; oversized: boolean }[] = [];
      let free = available;
      let held = memory?.held ?? 0;
      for (const request of queue) {
        const oversized = memory !== null && request.memory > memory.capacity;
        if (memory !== null && held + request.memory > memory.capacity && !(oversized && held === 0)) {
          return { grants, waiting: 'memory' };
        }
        const tokens = Math.min(request.want, free);
        if (tokens === 0) return { grants, waiting: 'cpu' };
        free -= tokens;
        held += request.memory;
        grants.push({ request, tokens, oversized });
      }
      return { grants, waiting: null };
    };
    const ask = fc.record({ want: fc.integer({ min: 1, max: 8 }), memory: fc.integer({ min: 0, max: 40 }) });
    const ledger = fc.option(
      fc.record({ capacity: fc.integer({ min: 1, max: 40 }), held: fc.integer({ min: 0, max: 40 }) }),
      {
        nil: null,
      },
    );
    fc.assert(
      fc.property(
        fc.array(ask, { maxLength: 8 }),
        fc.integer({ min: 0, max: 16 }),
        ledger,
        (queue, available, memory) => {
          const ours = admit<PoolAsk>(queue, available, memory);
          const main = admitMain(queue, available, memory);
          expect(ours.waiting).toBe(main.waiting);
          expect(ours.grants.map(({ request, tokens, oversized }) => ({ request, tokens, oversized }))).toEqual(
            main.grants,
          );
        },
      ),
      { seed: SEED, numRuns: 500 },
    );
  });

  it("never holds the leaves past the run's memory when its records are exact, charging each peak per worker", () => {
    fc.assert(holdsTheRun(scaled, EXACT), { seed: SEED, numRuns: 200 });
  });

  it("holds the leaves past the run's memory when its records are exact but a leaf is lent more workers than its peak was measured on", () => {
    // SmoothBricks main charges the peak whatever the leaf is lent: a Vitest run recorded on one worker is charged for one.
    expect(() => fc.assert(holdsTheRun(today, EXACT), { seed: SEED, numRuns: 200 })).toThrow(/Counterexample/);
  });

  it('lends the Vitest leaf of CI run 621 what its record can carry: main 22 of 31 workers, the per-worker charge fewer', () => {
    // CI run 621, 2026-11-03, 32 GiB container: the pool held four `bun test` leaves charged 3.19, 4.20, 3.42 and 5.20 GB
    // when a downstream package's Vitest run, whose record is 3.42 GB, asked for 31 tokens and was lent 22. The log does not
    // say how many workers its record was measured on, so every count it may have been is tried.
    const held = 3_185_790_976 + 4_197_408_768 + 3_416_981_504 + 5_203_169_280;
    const lent = (parallelism: number | undefined): number =>
      admit<PoolAsk>([{ want: 31, memory: 3_420_061_696, parallelism }], 22, { capacity: 32 * GiB, held }).grants[0]
        ?.tokens ?? 0;
    expect(lent(undefined)).toBe(22);
    expect([1, 2, 4, 8, 22, 31].map(lent)).toEqual([5, 10, 21, 22, 22, 22]);
  });

  describe('on runs shaped like the downstream test step', () => {
    const worlds = (regime: Regime): SimWorld[] =>
      Array.from({ length: 40 }, (_, seed) => downstreamWorld(seed + 1, regime));
    const overCommitted = (policy: Policy, regime: Regime): number => judge(policy, worlds(regime)).overCommitted;

    it("reproduces the kills: CPU tokens alone hold the leaves past the run's memory in every run", () => {
      expect(overCommitted(cpuOnly, EXACT)).toBe(1);
    });

    it("holds the leaves past the run's memory in nearly every run under main's flat charge, and in none per worker", () => {
      expect(overCommitted(today, EXACT)).toBeGreaterThan(0.8);
      expect(overCommitted(scaled, EXACT)).toBe(0);
    });

    it('leaves a leaf with no record, a suite that has grown and what lies outside the leaves unprotected', () => {
      expect(overCommitted(scaled, { ...EXACT, missing: 0.3 })).toBe(1);
      expect(overCommitted(scaled, { ...EXACT, stale: 1.3 })).toBe(1);
      // The pool admits against all of the run's memory, so anything outside the leaves is held past it.
      expect(overCommitted(scaled, { ...EXACT, baseline: 2 * GiB })).toBe(1);
    });

    it('is closed in part by charging a leaf with no record and by admitting against less than all of the memory', () => {
      const guarded = scaledWith(4 * GiB, 0.9);
      expect(overCommitted(guarded, { ...EXACT, missing: 0.3 })).toBeLessThan(
        overCommitted(scaled, { ...EXACT, missing: 0.3 }),
      );
      expect(overCommitted(guarded, { ...EXACT, baseline: 2 * GiB })).toBeLessThan(
        overCommitted(scaled, { ...EXACT, baseline: 2 * GiB }),
      );
    });

    const ramped = (regime: Regime): SimWorld[] =>
      Array.from({ length: 40 }, (_, seed) => downstreamWorld(seed + 1, regime, 20));

    it('does not see a leaf that has not ramped: live-headroom admission alone holds the leaves past the run on exact records', () => {
      expect(judge(scaled, ramped(EXACT)).overCommitted).toBe(0);
      expect(judge(liveHeadroom, ramped(EXACT)).overCommitted).toBe(1);
    });

    it('sees what lies outside the leaves, when the leaves are resident at the instant it decides', () => {
      expect(judge(liveHeadroom, worlds({ ...EXACT, baseline: 2 * GiB })).overCommitted).toBe(0);
    });

    it('ends no run in failure when a killed leaf runs again alone, but the kernel still kills one in every run', () => {
      const exact = judge(scaled, ramped(EXACT)).meanMakespan;
      for (const regime of [
        { ...EXACT, missing: 0.3 },
        { ...EXACT, stale: 1.3 },
        { ...EXACT, baseline: 2 * GiB },
      ]) {
        const plain = judge(scaled, ramped(regime));
        expect(plain.failed).toBe(1);
        for (const retried of [judge(scaledRetry, ramped(regime)), judge(liveHeadroomRetry, ramped(regime))]) {
          expect(retried.killed).toBe(1);
          expect(retried.failed).toBe(0);
          // Alone means nothing else runs: the run takes much longer than on exact records.
          expect(retried.meanMakespan).toBeGreaterThan(1.3 * exact);
        }
      }
    });
  });

  describe('whatever the records say', () => {
    const regimes: Regime[] = [
      EXACT,
      { missing: 0.3, stale: 1, baseline: 0 },
      { missing: 0, stale: 1.5, baseline: 0 },
      { missing: 0, stale: 1, baseline: 2 * GiB },
    ];
    const policies = [cpuOnly, today, scaled, cap(4), cap(8), liveHeadroom, scaledRetry, liveHeadroomRetry];
    for (const policy of policies) {
      it(`${policy.name}: every leaf runs, in the order it asked`, () => {
        for (const regime of regimes) {
          fc.assert(
            fc.property(worldArb(regime), (world) => {
              const outcome = simulate(policy, world);
              expect(outcome.starved).toBe(false);
              expect(outcome.fifo).toBe(true);
            }),
            { seed: SEED, numRuns: 40 },
          );
        }
      });
    }
  });
});
