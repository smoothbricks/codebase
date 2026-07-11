/**
 * Transformer-output-shape benchmark — spec 01o §4 / lmao-rs optimization q.
 *
 * Compares the per-span cost of writing 6 tag fields (1 enum, 2 category
 * strings, 2 numbers, 1 boolean) through:
 *
 *  A. fluent-chain      — runtime fluent builder calls (no transformer)
 *  B. codegen-writer    — new Function() generated writer (lmao runtime today)
 *  C. inlined-direct    — direct TypedArray writes with compile-time-known
 *                         enum indices and eagerness (what tagChainInliner
 *                         emits; zero runtime codegen — CSP-safe)
 *  D. inlined-batched   — C, but numeric writes packed into one staging
 *                         Float64Array + one .set() (the "compile-time
 *                         batching" candidate for amortizing a wasm boundary)
 *
 * Wasm-boundary reference numbers (from packages/lmao/benchmarks/
 * wasm-boundary.bench.ts on this machine, Apple M5 Max): per-value export
 * call ~39-42 ns; JS TypedArray view write into wasm memory ~7.6 ns; plain
 * JS array/TypedArray write ~0.5-2 ns.
 *
 * Run: bun benchmarks/inline-vs-codegen.bench.ts
 */

import { bench, group, run } from 'mitata';

const CAP = 64;

// Columnar buffer matching the generated SpanBuffer field layout:
// per-field `{name}_nulls` bitmap view + `{name}_values` view/array.
function makeBuffer() {
  return {
    operation_nulls: new Uint8Array(1),
    operation_values: new Uint8Array(CAP), // enum indices
    userId_nulls: new Uint8Array(1),
    userId_values: [] as (string | undefined)[], // category: raw push, strings stay in JS
    region_nulls: new Uint8Array(1),
    region_values: [] as (string | undefined)[],
    latencyMs_nulls: new Uint8Array(1),
    latencyMs_values: new Float64Array(CAP),
    retries_nulls: new Uint8Array(1),
    retries_values: new Float64Array(CAP),
    cached_nulls: new Uint8Array(1),
    cached_values: new Uint8Array(1), // bit-packed bool
  };
}
type Buf = ReturnType<typeof makeBuffer>;

const OPERATIONS = ['DELETE', 'INSERT', 'SELECT', 'UPDATE'] as const; // sorted

// --- A: fluent builder (runtime fallback shape) -----------------------------
class FluentTag {
  constructor(private b: Buf) {}
  operation(v: string) {
    this.b.operation_nulls[0] |= 1;
    this.b.operation_values[0] = OPERATIONS.indexOf(v as (typeof OPERATIONS)[number]);
    return this;
  }
  userId(v: string) {
    this.b.userId_nulls[0] |= 1;
    this.b.userId_values[0] = v;
    return this;
  }
  region(v: string) {
    this.b.region_nulls[0] |= 1;
    this.b.region_values[0] = v;
    return this;
  }
  latencyMs(v: number) {
    this.b.latencyMs_nulls[0] |= 1;
    this.b.latencyMs_values[0] = v;
    return this;
  }
  retries(v: number) {
    this.b.retries_nulls[0] |= 1;
    this.b.retries_values[0] = v;
    return this;
  }
  cached(v: boolean) {
    this.b.cached_nulls[0] |= 1;
    if (v) this.b.cached_values[0] |= 1;
    else this.b.cached_values[0] &= ~1;
    return this;
  }
}

// --- B: runtime-codegen writer (new Function, lmao's current trick) ----------
// eslint-disable-next-line @typescript-eslint/no-implied-eval
const codegenWriter = new Function(
  'b',
  'op',
  'userId',
  'region',
  'latencyMs',
  'retries',
  'cached',
  `
  b.operation_nulls[0] |= 1;
  b.operation_values[0] = op === 'DELETE' ? 0 : op === 'INSERT' ? 1 : op === 'SELECT' ? 2 : 3;
  b.userId_nulls[0] |= 1;
  b.userId_values[0] = userId;
  b.region_nulls[0] |= 1;
  b.region_values[0] = region;
  b.latencyMs_nulls[0] |= 1;
  b.latencyMs_values[0] = latencyMs;
  b.retries_nulls[0] |= 1;
  b.retries_values[0] = retries;
  b.cached_nulls[0] |= 1;
  if (cached) b.cached_values[0] |= 1; else b.cached_values[0] &= ~1;
`,
) as (b: Buf, op: string, userId: string, region: string, latencyMs: number, retries: number, cached: boolean) => void;

// --- D: staging buffer for batched numeric writes ----------------------------
const staging = new Float64Array(3); // enumIdx, latencyMs, retries

const buf = makeBuffer();
let i = 0;

group('6 tag writes (enum + 2 category + 2 num + bool)', () => {
  bench('A fluent-chain (no transformer)', () => {
    new FluentTag(buf)
      .operation('SELECT')
      .userId(`u${i}`)
      .region('eu-west')
      .latencyMs(i)
      .retries(2)
      .cached((i & 1) === 0);
    i++;
  });

  bench('B codegen-writer (new Function)', () => {
    codegenWriter(buf, 'SELECT', `u${i}`, 'eu-west', i, 2, (i & 1) === 0);
    i++;
  });

  bench('C inlined-direct (transformer output)', () => {
    // Exactly what tagChainInliner emits: enum index folded to a constant
    // (literal 'SELECT' → 2), direct writes, bit ops for bool.
    buf.operation_nulls[0] |= 1;
    buf.operation_values[0] = 2;
    buf.userId_nulls[0] |= 1;
    buf.userId_values[0] = `u${i}`;
    buf.region_nulls[0] |= 1;
    buf.region_values[0] = 'eu-west';
    buf.latencyMs_nulls[0] |= 1;
    buf.latencyMs_values[0] = i;
    buf.retries_nulls[0] |= 1;
    buf.retries_values[0] = 2;
    buf.cached_nulls[0] |= 1;
    if ((i & 1) === 0) buf.cached_values[0] |= 1;
    else buf.cached_values[0] &= ~1;
    i++;
  });

  bench('D inlined-batched (staging + one set)', () => {
    staging[0] = 2;
    staging[1] = i;
    staging[2] = 2;
    buf.userId_values[0] = `u${i}`;
    buf.region_values[0] = 'eu-west';
    buf.operation_nulls[0] |= 1;
    buf.userId_nulls[0] |= 1;
    buf.region_nulls[0] |= 1;
    buf.latencyMs_nulls[0] |= 1;
    buf.retries_nulls[0] |= 1;
    buf.cached_nulls[0] |= 1;
    if ((i & 1) === 0) buf.cached_values[0] |= 1;
    else buf.cached_values[0] &= ~1;
    // One contiguous copy standing in for a single bulk boundary crossing.
    buf.latencyMs_values.set(staging.subarray(1, 2), 0);
    buf.retries_values.set(staging.subarray(2, 3), 0);
    buf.operation_values[0] = staging[0];
    i++;
  });
});

await run();
