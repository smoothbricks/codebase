import { expect, it } from 'bun:test';
import fc from 'fast-check';
import {
  type Admission,
  admit,
  IdleBesideStarved,
  type MemoryLedger,
  type PoolAsk,
  type PoolGrant,
  place,
} from './pool-admission.js';

const GiB = 1024 ** 3;
/** A downstream CI runner's run: a 32 GiB cgroup. */
const RUN: MemoryLedger = { capacity: 32 * GiB, held: 0 };
const leaves = (count: number, ask: PoolAsk): PoolAsk[] => Array.from({ length: count }, () => ask);

it('grants a request only while CPU tokens are free and the charges of the holders and its own fit the run', () => {
  const cases = [
    {
      name: 'a pool that charges no memory (a sub-pool) grants on its tokens alone',
      queue: [
        { want: 4, memory: 40 * GiB },
        { want: 2, memory: 0 },
      ],
      available: 4,
      memory: null,
      admission: { grants: [{ tokens: 4, oversized: false }], waiting: 'cpu' },
    },
    {
      // The downstream runner: 32 bun test leaves of 4 GiB on 32 cores. Eight fit beside each other.
      name: 'grants in FIFO order only while the charges fit',
      queue: leaves(32, { want: 1, memory: 4 * GiB }),
      available: 32,
      memory: RUN,
      admission: {
        grants: Array.from({ length: 8 }, () => ({ tokens: 1, oversized: false })),
        waiting: 'memory',
      },
    },
    {
      name: "the holders' charges count: an exact fit is granted, a byte more waits",
      queue: [
        { want: 1, memory: 4 * GiB },
        { want: 1, memory: 1 },
      ],
      available: 8,
      memory: { capacity: 32 * GiB, held: 28 * GiB },
      admission: { grants: [{ tokens: 1, oversized: false }], waiting: 'memory' },
    },
    {
      name: 'a head that does not fit holds up a smaller request behind it',
      queue: [
        { want: 1, memory: 6 * GiB },
        { want: 1, memory: GiB },
      ],
      available: 8,
      memory: { capacity: 8 * GiB, held: 4 * GiB },
      admission: { grants: [], waiting: 'memory' },
    },
    {
      name: 'a task with no recorded peak is charged nothing, even beside holders that fill the run',
      queue: leaves(2, { want: 2, memory: 0 }),
      available: 4,
      memory: { capacity: 8 * GiB, held: 8 * GiB },
      admission: {
        grants: [
          { tokens: 2, oversized: false },
          { tokens: 2, oversized: false },
        ],
        waiting: null,
      },
    },
    {
      name: 'a leaf whose charge alone exceeds the run is granted when nothing holds memory, and holds the run',
      queue: [
        { want: 4, memory: 40 * GiB },
        { want: 1, memory: 0 },
      ],
      available: 8,
      memory: RUN,
      admission: { grants: [{ tokens: 4, oversized: true }], waiting: 'memory' },
    },
    {
      name: 'an oversized leaf waits while anything holds memory',
      queue: [{ want: 1, memory: 40 * GiB }],
      available: 8,
      memory: { capacity: 32 * GiB, held: 1 },
      admission: { grants: [], waiting: 'memory' },
    },
    {
      name: 'an oversized leaf waits for a request granted ahead of it in the same pass',
      queue: [
        { want: 1, memory: GiB },
        { want: 1, memory: 40 * GiB },
      ],
      available: 8,
      memory: RUN,
      admission: { grants: [{ tokens: 1, oversized: false }], waiting: 'memory' },
    },
    {
      name: 'CPU gates where memory fits: the head takes what is free, the next waits for a token',
      queue: [
        { want: 3, memory: GiB },
        { want: 2, memory: GiB },
        { want: 1, memory: GiB },
      ],
      available: 4,
      memory: RUN,
      admission: {
        grants: [
          { tokens: 3, oversized: false },
          { tokens: 1, oversized: false },
        ],
        waiting: 'cpu',
      },
    },
    {
      name: 'memory gates where CPU is free',
      queue: leaves(2, { want: 1, memory: 3 * GiB }),
      available: 8,
      memory: { capacity: 4 * GiB, held: 0 },
      admission: { grants: [{ tokens: 1, oversized: false }], waiting: 'memory' },
    },
    {
      name: 'no free token grants nothing, however much memory is free',
      queue: [{ want: 1, memory: 0 }],
      available: 0,
      memory: RUN,
      admission: { grants: [], waiting: 'cpu' },
    },
    {
      name: 'an empty queue waits for nothing',
      queue: [],
      available: 4,
      memory: RUN,
      admission: { grants: [], waiting: null },
    },
  ] satisfies {
    name: string;
    queue: PoolAsk[];
    available: number;
    memory: MemoryLedger | null;
    admission: { grants: Pick<PoolGrant<PoolAsk>, 'tokens' | 'oversized'>[]; waiting: Admission<PoolAsk>['waiting'] };
  }[];
  for (const check of cases) {
    const admission = admit(check.queue, check.available, check.memory);
    const grants = admission.grants.map(({ tokens, oversized }) => ({ tokens, oversized }));
    expect({ grants, waiting: admission.waiting }, check.name).toEqual(check.admission);
    // FIFO: the grants are the queue's first requests, in its order.
    expect(
      admission.grants.every((grant, at) => grant.request === check.queue[at]),
      check.name,
    ).toBe(true);
  }
});

it('sizes a grant to the memory it needs: a peak scales up with the parallelism granted beyond the parallelism it was measured under', () => {
  const cases = [
    {
      // A vitest leaf recorded on one worker (4 GiB), asking for 31 beside a 32 GiB run: 8 workers fit.
      name: 'a peak measured on one worker is charged per worker, and the grant is cut to what fits',
      queue: [{ want: 31, memory: 4 * GiB, parallelism: 1 }],
      available: 31,
      memory: { capacity: 32 * GiB, held: 0 },
      admission: { grants: [{ tokens: 8, charge: 32 * GiB, oversized: false }], waiting: null },
    },
    {
      name: 'a peak measured on 22 workers is the charge of 22 workers, and of fewer: it never scales down',
      queue: [{ want: 31, memory: 22 * GiB, parallelism: 22 }],
      available: 4,
      memory: { capacity: 32 * GiB, held: 0 },
      admission: { grants: [{ tokens: 4, charge: 22 * GiB, oversized: false }], waiting: null },
    },
    {
      name: 'the holders count: 8 GiB of room fits two workers of 4 GiB',
      queue: [{ want: 31, memory: 4 * GiB, parallelism: 1 }],
      available: 31,
      memory: { capacity: 32 * GiB, held: 24 * GiB },
      admission: { grants: [{ tokens: 2, charge: 8 * GiB, oversized: false }], waiting: null },
    },
    {
      name: 'one worker that does not fit waits for memory, whatever tokens are free',
      queue: [{ want: 31, memory: 4 * GiB, parallelism: 1 }],
      available: 31,
      memory: { capacity: 32 * GiB, held: 29 * GiB },
      admission: { grants: [], waiting: 'memory' },
    },
    {
      name: 'a charge with no measured parallelism is flat: the grant is never cut for it',
      queue: [{ want: 31, memory: 4 * GiB }],
      available: 31,
      memory: { capacity: 32 * GiB, held: 24 * GiB },
      admission: { grants: [{ tokens: 31, charge: 4 * GiB, oversized: false }], waiting: null },
    },
    {
      name: 'a leaf that exceeds the run on the workers it was measured on runs alone on one worker',
      queue: [{ want: 31, memory: 40 * GiB, parallelism: 8 }],
      available: 31,
      memory: { capacity: 32 * GiB, held: 0 },
      admission: { grants: [{ tokens: 1, charge: 40 * GiB, oversized: true }], waiting: null },
    },
    {
      name: 'the second leaf of a pass is charged beside the first, at the tokens it is granted',
      queue: [
        { want: 4, memory: 4 * GiB, parallelism: 1 },
        { want: 4, memory: 4 * GiB, parallelism: 1 },
      ],
      available: 8,
      memory: { capacity: 24 * GiB, held: 0 },
      admission: {
        grants: [
          { tokens: 4, charge: 16 * GiB, oversized: false },
          { tokens: 2, charge: 8 * GiB, oversized: false },
        ],
        waiting: null,
      },
    },
  ] satisfies {
    name: string;
    queue: PoolAsk[];
    available: number;
    memory: MemoryLedger;
    admission: {
      grants: Pick<PoolGrant<PoolAsk>, 'tokens' | 'charge' | 'oversized'>[];
      waiting: Admission<PoolAsk>['waiting'];
    };
  }[];
  for (const check of cases) {
    const admission = admit(check.queue, check.available, check.memory);
    const grants = admission.grants.map(({ tokens, charge, oversized }) => ({ tokens, charge, oversized }));
    expect({ grants, waiting: admission.waiting }, check.name).toEqual(check.admission);
  }
});

it('grants a leaf the kernel killed only while nothing holds a grant, ahead of the leaves behind it, and grants nothing beside it', () => {
  const again: PoolAsk = { want: 4, memory: 0, alone: true };
  const other: PoolAsk = { want: 1, memory: 0 };
  const cases = [
    {
      name: 'with nothing held it is granted, and the leaf behind it waits for it',
      queue: [again, other],
      held: { holders: 0, alone: false },
      grants: [4],
      waiting: 'alone',
    },
    {
      name: 'beside a holder it waits, and so does everything behind it',
      queue: [again, other],
      held: { holders: 1, alone: false },
      grants: [],
      waiting: 'alone',
    },
    {
      name: 'while a leaf runs alone nothing is granted',
      queue: [other],
      held: { holders: 1, alone: true },
      grants: [],
      waiting: 'alone',
    },
    {
      name: 'a leaf granted ahead of it in the same pass is a holder: it waits',
      queue: [other, again],
      held: { holders: 0, alone: false },
      grants: [1],
      waiting: 'alone',
    },
    {
      name: 'granted with no leaf behind it, nothing is left waiting',
      queue: [again],
      held: { holders: 0, alone: false },
      grants: [4],
      waiting: null,
    },
  ] satisfies {
    name: string;
    queue: PoolAsk[];
    held: { holders: number; alone: boolean };
    grants: number[];
    waiting: Admission<PoolAsk>['waiting'];
  }[];
  for (const check of cases) {
    const admission = admit(check.queue, 8, null, check.held);
    expect({ grants: admission.grants.map(({ tokens }) => tokens), waiting: admission.waiting }, check.name).toEqual({
      grants: check.grants,
      waiting: check.waiting,
    });
  }
});

//#region smoo/cowshed!n/a-case-starts-on-a-core
const NOTHING = { holders: 0, alone: false };
const lent = (left?: number): PoolAsk => ({ want: 1, memory: 0, lend: true, ...(left === undefined ? {} : { left }) });

it('places a lent request behind every leaf, the leaf with the most work left first', () => {
  const leaf: PoolAsk = { want: 4, memory: 0 };
  const again: PoolAsk = { want: 4, memory: 0, alone: true };
  const cases = [
    { name: 'a leaf joins the end of the leaves', queue: [leaf], ask: leaf, at: 1 },
    { name: 'a leaf goes ahead of every lent request', queue: [leaf, lent(9), lent(3)], ask: leaf, at: 1 },
    { name: 'a lent request goes behind every leaf', queue: [leaf, leaf], ask: lent(9), at: 2 },
    { name: 'more work left goes first', queue: [leaf, lent(9), lent(3)], ask: lent(5), at: 2 },
    { name: 'equal work left keeps arrival order', queue: [lent(5), lent(5)], ask: lent(5), at: 2 },
    { name: 'no named work left is none', queue: [lent(5)], ask: lent(), at: 1 },
    { name: 'a leaf run again alone goes ahead of the leaves', queue: [leaf, lent(9)], ask: again, at: 0 },
  ] satisfies { name: string; queue: PoolAsk[]; ask: PoolAsk; at: number }[];
  for (const check of cases) {
    expect(place(check.queue, check.ask), check.name).toBe(check.at);
  }
});

it('grants no lent request inside the settle time, and every leaf as before', () => {
  const queue: PoolAsk[] = [{ want: 2, memory: 0 }, lent(9)];
  const tokens = (admission: Admission<PoolAsk>) => ({
    grants: admission.grants.map((grant) => grant.tokens),
    waiting: admission.waiting,
  });
  expect(tokens(admit(queue, 8, null, NOTHING, true))).toEqual({ grants: [2, 1], waiting: null });
  expect(tokens(admit(queue, 8, null, NOTHING, false))).toEqual({ grants: [2], waiting: 'settle' });
});

it('lends only tokens no leaf waits for, to the most work left, over random arrivals', () => {
  const arrival = fc.record({
    lend: fc.boolean(),
    want: fc.integer({ min: 1, max: 8 }),
    left: fc.integer({ min: 0, max: 1000 }),
  });
  fc.assert(
    fc.property(
      fc.array(arrival, { maxLength: 24 }),
      fc.integer({ min: 0, max: 8 }),
      fc.boolean(),
      (arrivals, free, lendable) => {
        const queue: PoolAsk[] = [];
        for (const { lend, want, left } of arrivals) {
          const ask: PoolAsk = lend ? lent(left) : { want, memory: 0 };
          queue.splice(place(queue, ask), 0, ask);
        }
        const { grants } = admit(queue, free, null, NOTHING, lendable);
        const waiting = queue.slice(grants.length);
        const lentGrants = grants.filter((grant) => grant.request.lend === true);
        expect(grants.reduce((sum, grant) => sum + grant.tokens, 0)).toBeLessThanOrEqual(free);
        if (lentGrants.length === 0) return;
        expect(lendable).toBe(true);
        expect(waiting.filter((ask) => ask.lend !== true)).toEqual([]);
        const least = Math.min(...lentGrants.map((grant) => grant.request.left ?? 0));
        expect(Math.max(0, ...waiting.map((ask) => ask.left ?? 0))).toBeLessThanOrEqual(least);
      },
    ),
    { seed: 20261119 },
  );
});

it('counts the token-time idle beside a starved hold, and none beside holds that have what they asked', () => {
  const idle = new IdleBesideStarved(1_000);
  idle.observe(1_000, 3, false);
  // Ten seconds idle beside nobody starved count for nothing.
  idle.observe(11_000, 3, true);
  idle.observe(31_000, 1, true);
  idle.observe(36_000, 0, true);
  idle.observe(40_000, 2, false);
  expect(idle.tokenMs(50_000)).toBe(3 * 20_000 + 5_000);
  idle.observe(50_000, 2, true);
  expect(idle.tokenMs(51_000)).toBe(3 * 20_000 + 5_000 + 2_000);
});
//#endregion smoo/cowshed!n/a-case-starts-on-a-core
