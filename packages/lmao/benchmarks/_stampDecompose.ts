/**
 * Scratch instrument: frozen-clock decomposition of the js-heap row stamp.
 *
 * The JSC profile names `bigint [Unknown Executable]` under `nextTimestamp` as
 * ~26% of on-CPU on the log-row path, but a frame is not a mechanism: that one
 * label covers the `process.hrtime.bigint()` cell, the `+` offset add, the
 * `<=` monotonic compare, and the `BigInt64Array` store. This freezes them one
 * at a time — the same method `containium-realm-spans/benches/span_write.rs`
 * used to put the Rust clock read at 16.75 of 19.4 ns/row.
 *
 * Each variant does exactly what a log row does with the stamp: produce the
 * bigint and store it into the timestamp column. Nothing else from
 * `appendLogEntry` differs between variants, so the delta is the stamp.
 */

const CAPACITY = 64;
const column = new BigInt64Array(CAPACITY);

const anchor = process.hrtime.bigint();
const epochOffset = 1_700_000_000_000_000_000n - anchor;

/** Mutable stamp state, shaped like the TraceRoot fields it stands for. */
const state = { last: 0n, cache: anchor + epochOffset, reads: 16 };

type Variant = { readonly name: string; readonly step: (i: number) => void };

const variants: readonly Variant[] = [
  {
    // Exactly today's traceRoot.node.ts nextTimestamp, plus the column store.
    name: 'full (clock + offset add + monotonic guard + store)',
    step: (i) => {
      let t = epochOffset + process.hrtime.bigint();
      if (t <= state.last) t = state.last + 1n;
      state.last = t;
      column[i & (CAPACITY - 1)] = t;
    },
  },
  {
    name: 'no guard (clock + offset add + store)',
    step: (i) => {
      const t = epochOffset + process.hrtime.bigint();
      state.last = t;
      column[i & (CAPACITY - 1)] = t;
    },
  },
  {
    name: 'no offset add (clock + monotonic guard + store)',
    step: (i) => {
      let t = process.hrtime.bigint();
      if (t <= state.last) t = state.last + 1n;
      state.last = t;
      column[i & (CAPACITY - 1)] = t;
    },
  },
  {
    // Clock frozen: the offset add and guard survive, the syscall/cell does not.
    name: 'frozen clock (offset add + monotonic guard + store)',
    step: (i) => {
      let t = epochOffset + state.cache;
      if (t <= state.last) t = state.last + 1n;
      state.last = t;
      column[i & (CAPACITY - 1)] = t;
    },
  },
  {
    // The coarse-clock floor: one already-materialized bigint, one store.
    name: 'cached stamp (store only)',
    step: (i) => {
      column[i & (CAPACITY - 1)] = state.cache;
    },
  },
  {
    // The proposed design: cached stamp, refreshed every 16 rows.
    name: 'coarse-16 (cached stamp, refresh every 16 rows)',
    step: (i) => {
      const reads = state.reads;
      let t: bigint;
      if (reads === 0) {
        t = epochOffset + process.hrtime.bigint();
        state.cache = t;
        state.reads = 16;
      } else {
        state.reads = reads - 1;
        t = state.cache;
      }
      column[i & (CAPACITY - 1)] = t;
    },
  },
  {
    // The alternative the brief asks about: hold one bigint, derive per-row
    // offsets as numbers. A distinct stamp per row costs a BigInt cell per row.
    name: 'coarse-16 + numeric row offset (BigInt(n) per row)',
    step: (i) => {
      const reads = state.reads;
      let base: bigint;
      if (reads === 0) {
        base = epochOffset + process.hrtime.bigint();
        state.cache = base;
        state.reads = 16;
      } else {
        state.reads = reads - 1;
        base = state.cache;
      }
      column[i & (CAPACITY - 1)] = base + BigInt(16 - state.reads);
    },
  },
  {
    // Control: the store alone, no bigint produced at all, to price the store.
    name: 'control (constant store)',
    step: (i) => {
      column[i & (CAPACITY - 1)] = 1n;
    },
  },
];

const ROUNDS = 7;
const ITERATIONS = 400_000;

function price(step: (i: number) => void): number {
  for (let i = 0; i < 200_000; i++) step(i);
  let best = Number.POSITIVE_INFINITY;
  for (let round = 0; round < ROUNDS; round++) {
    const start = Bun.nanoseconds();
    for (let i = 0; i < ITERATIONS; i++) step(i);
    const ns = (Bun.nanoseconds() - start) / ITERATIONS;
    if (ns < best) best = ns;
  }
  return best;
}

const load = (await Bun.file('/dev/null')
  .text()
  .catch(() => '')) satisfies string;
void load;

// ABBA interleaving: forward pass then reverse pass, report the pair mean, so a
// monotonic drift in machine state cannot be read as a variant difference.
const forward = variants.map((v) => price(v.step));
const reverse = [...variants]
  .reverse()
  .map((v) => price(v.step))
  .reverse();

const loadavg = (await Bun.$`sysctl -n vm.loadavg`.text()).trim();
console.log(`stamp decomposition  rows=${ITERATIONS}x${ROUNDS} best-of  loadavg=${loadavg}`);
for (const [index, variant] of variants.entries()) {
  const a = forward[index] ?? 0;
  const b = reverse[index] ?? 0;
  const mean = (a + b) / 2;
  console.log(`  ${mean.toFixed(2).padStart(6)} ns/row  (A ${a.toFixed(2)} / B ${b.toFixed(2)})  ${variant.name}`);
}
console.log(`column checksum ${column.reduce((sum, v) => sum ^ v, 0n)}`);
