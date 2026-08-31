/**
 * Scratch instrument: js-heap span arms, coarse row stamps against exact ones.
 *
 * Both clock models run in one process against the real span path, so the
 * comparison is same-machine/same-session by construction. The "exact" model is
 * the pre-coarsening `traceRoot.node.ts` primitive table, replicated here — the
 * same before-model pattern `timestamps.bench.ts` already uses — installed on a
 * subclass so each arm keeps one monomorphic hidden class rather than being
 * deoptimized by a per-instance property override.
 *
 * ## Statistic
 *
 * Contention can only ever ADD time, so a per-block MINIMUM rejects almost all
 * of it while a mean absorbs all of it. A minimum is only a floor if the block
 * is short relative to the spike arrival rate: five blocks of 72ms is five
 * samples of a 72ms window, i.e. the minimum of five means. Blocks are therefore
 * calibrated to ~`BLOCK_MILLIS` of wall time and there are `BLOCKS` of them, and
 * the floor is reported with the mean beside it — the gap between them IS the
 * contention, so it becomes a diagnostic instead of noise.
 *
 * Three independent guards on top:
 *
 * - **Floor convergence.** The floor over the first half of the blocks is
 *   printed next to the floor over all of them. If the floor keeps dropping as
 *   blocks are added, the blocks are still long enough to be eating spikes and
 *   the number is not yet a floor.
 * - **Null arm.** ABBA runs each model twice, so the two same-model floors are a
 *   null measurement of the instrument's own resolution. A signal at or inside
 *   the null spread is unquotable.
 * - **Sign agreement.** `--repeat` repetitions; no direction is printed unless
 *   every repetition agrees on the sign AND clears the null spread. Repetitions
 *   inside one invocation share JIT and cache state, so agreement is encouraging
 *   and disagreement is conclusive — confirm with a separate invocation.
 *
 * ABBA is kept because it cancels monotonic ordering drift, which a floor does
 * not; the two solve different problems. Blocks alternate model order so drift
 * within a repetition hits both models equally.
 *
 * Arms are reported separately for `logs-000` and a marginal-heavy count: the
 * clock is paid at both, and one aggregate number hides which moved. The
 * marginal arm stops at 50 rows because `SPAN_CAPACITY` is 64 and start plus
 * completion take two, so 62 is the ceiling before a second block chains in and
 * reports capacity growth as marshalling.
 *
 * ## The js-heap baseline has MOVED
 *
 * Every lane comparison and flat-class baseline taken from here on is against a
 * POST-COARSENING js-heap lane. Measured on this harness: `logs-000` 1250-1363
 * -> 954-1055 ns/span and `logs-050` 84.4 -> 58.8 ns/row. Any lane whose row
 * stamps are not coarsened — the per-span WASM lane, and the thread lane before
 * its own coarsening lands — will therefore show a WORSE ratio against js-heap
 * than it did before, by roughly 25 ns/row. That is expected and correct: the
 * denominator got faster. Do not read it as a regression in the other lane, and
 * do not compare a post-clock js-heap number against a pre-clock one.
 */

import os from 'node:os';
import { Nanoseconds } from '@smoothbricks/arrow-builder';
import { defineOpContext } from '../src/lib/defineOpContext.js';
import { JsBufferStrategy } from '../src/lib/JsBufferStrategy.js';
import { S } from '../src/lib/schema/builder.js';
import { defineLogSchema } from '../src/lib/schema/defineLogSchema.js';
import type { LogSchema } from '../src/lib/schema/LogSchema.js';
import { ENTRY_TYPE_SPAN_EXCEPTION, ENTRY_TYPE_SPAN_START } from '../src/lib/schema/systemSchema.js';
import type {
  SpanEndPrimitive,
  SpanStartPrimitive,
  TimestampAppendPrimitive,
  TracerLifecycleHooks,
} from '../src/lib/traceRoot.js';
import { createTraceRoot, TraceRoot } from '../src/lib/traceRoot.node.js';
import { TestTracer } from '../src/lib/tracers/TestTracer.js';

// --- the exact-stamp model that shipped before coarsening ---

function exactTimestamp(root: ExactTraceRoot): Nanoseconds {
  let timestamp = root._epochHrtimeOffset + process.hrtime.bigint();
  if (timestamp <= root._lastTimestampNanos) timestamp = root._lastTimestampNanos + 1n;
  root._lastTimestampNanos = timestamp;
  return Nanoseconds.unsafe(timestamp);
}

const exactAppendLogEntry: TimestampAppendPrimitive = (traceRoot, buffer, entryType) => {
  const entryTypes = buffer.entry_type;
  if (entryTypes === undefined) throw new TypeError('exact appender requires entry_type storage');
  const idx = buffer._writeIndex;
  buffer.timestamp[idx] = exactTimestamp(traceRoot as ExactTraceRoot);
  entryTypes[idx] = entryType;
  buffer._writeIndex = idx + 1;
  return idx;
};

const exactWriteSpanStart: SpanStartPrimitive = (traceRoot, buffer, spanName) => {
  const entryTypes = buffer.entry_type;
  if (entryTypes === undefined) throw new TypeError('exact span-start appender requires entry_type storage');
  buffer.timestamp[0] = exactTimestamp(traceRoot as ExactTraceRoot);
  entryTypes[0] = ENTRY_TYPE_SPAN_START;
  if (buffer.message_values) buffer.message(0, spanName);
  entryTypes[1] = ENTRY_TYPE_SPAN_EXCEPTION;
  buffer.timestamp[1] = 0n;
  buffer._writeIndex = 2;
};

const exactWriteSpanEnd: SpanEndPrimitive = (traceRoot, buffer, entryType) => {
  const entryTypes = buffer.entry_type;
  if (entryTypes === undefined) throw new TypeError('exact span-end appender requires entry_type storage');
  buffer.timestamp[1] = exactTimestamp(traceRoot as ExactTraceRoot);
  entryTypes[1] = entryType;
  buffer._sealStatsChain();
};

class ExactTraceRoot<T extends LogSchema = LogSchema> extends TraceRoot<T> {
  override readonly _appendLogEntry = exactAppendLogEntry;
  override readonly _writeSpanStart = exactWriteSpanStart;
  override readonly _writeSpanEnd = exactWriteSpanEnd;
}

function createExactTraceRoot<T extends LogSchema>(
  trace_id: string,
  tracer: TracerLifecycleHooks<T>,
): ExactTraceRoot<T> {
  const anchorHrtime = process.hrtime.bigint();
  const seed = createTraceRoot<T>(trace_id, tracer);
  return new ExactTraceRoot<T>(seed.trace_id, seed.anchorEpochNanos, Number(anchorHrtime), anchorHrtime, tracer);
}

// --- arms ---

const schema = defineLogSchema({ n: S.number() });
const context = defineOpContext({ logSchema: schema });
type Binding = typeof context;
type Schema = Binding['logBinding']['logSchema'];

const strategy = new JsBufferStrategy<Schema>();
const coarseTracer = new TestTracer(context, { bufferStrategy: strategy, createTraceRoot });
const exactTracer = new TestTracer(context, { bufferStrategy: strategy, createTraceRoot: createExactTraceRoot });

/** Wall time each timed block should occupy, so its minimum is not one lucky span. */
const BLOCK_MILLIS = 70;
const BLOCKS = 20;

function workFor(tracer: TestTracer<Binding>, logs: number): () => void {
  return () => {
    tracer.clear();
    tracer.trace_fn(0, 'c', {}, (span) => {
      for (let i = 0; i < logs; i++) span.log.info(`m${i}`).n(i);
      return span.ok(1);
    });
  };
}

/** One timed block: nanoseconds per span. */
function block(work: () => void, iterations: number): number {
  const start = Bun.nanoseconds();
  for (let i = 0; i < iterations; i++) work();
  return (Bun.nanoseconds() - start) / iterations;
}

interface Stat {
  readonly floor: number;
  readonly mean: number;
  /** Floor over the first half of the blocks, for the convergence check. */
  readonly halfFloor: number;
}

function summarize(samples: readonly number[]): Stat {
  const half = samples.slice(0, Math.ceil(samples.length / 2));
  return {
    floor: Math.min(...samples),
    mean: samples.reduce((sum, value) => sum + value, 0) / samples.length,
    halfFloor: Math.min(...half),
  };
}

interface ArmResult {
  readonly exact: readonly [Stat, Stat];
  readonly coarse: readonly [Stat, Stat];
  readonly iterations: number;
}

function runArm(logs: number): ArmResult {
  const exactWork = workFor(exactTracer, logs);
  const coarseWork = workFor(coarseTracer, logs);
  for (let i = 0; i < 3000; i++) {
    exactWork();
    coarseWork();
  }

  // Calibrate the block to BLOCK_MILLIS of wall time on the slower model.
  const probe = Math.max(block(exactWork, 2000), block(coarseWork, 2000));
  const iterations = Math.max(200, Math.round((BLOCK_MILLIS * 1_000_000) / probe));

  const exactA: number[] = [];
  const exactB: number[] = [];
  const coarseA: number[] = [];
  const coarseB: number[] = [];
  for (let i = 0; i < BLOCKS; i++) {
    // ABBA at block granularity: order flips every block, so ordering drift
    // inside the window lands on both models equally.
    if (i % 2 === 0) {
      exactA.push(block(exactWork, iterations));
      coarseA.push(block(coarseWork, iterations));
      coarseB.push(block(coarseWork, iterations));
      exactB.push(block(exactWork, iterations));
    } else {
      coarseB.push(block(coarseWork, iterations));
      exactB.push(block(exactWork, iterations));
      exactA.push(block(exactWork, iterations));
      coarseA.push(block(coarseWork, iterations));
    }
  }
  return {
    exact: [summarize(exactA), summarize(exactB)],
    coarse: [summarize(coarseA), summarize(coarseB)],
    iterations,
  };
}

// --- reporting ---

let repeat = 3;
for (let i = 2; i < Bun.argv.length; i++) {
  if (Bun.argv[i] === '--repeat') {
    repeat = Number.parseInt(Bun.argv[i + 1] ?? '', 10);
    if (!Number.isInteger(repeat) || repeat < 1) throw new Error('--repeat needs a positive integer');
    i++;
  }
}

const SWEEP = [0, 50] as const;
const deltasByArm: Record<number, number[]> = { 0: [], 50: [] };
const nullsByArm: Record<number, number[]> = { 0: [], 50: [] };

for (let repetition = 0; repetition < repeat; repetition++) {
  console.log(`\n=== repetition ${repetition + 1}/${repeat}  load1=${(os.loadavg()[0] ?? 0).toFixed(2)} ===`);
  for (const logs of SWEEP) {
    const result = runArm(logs);
    const [exactA, exactB] = result.exact;
    const [coarseA, coarseB] = result.coarse;
    const exactFloor = Math.min(exactA.floor, exactB.floor);
    const coarseFloor = Math.min(coarseA.floor, coarseB.floor);
    // Null arm: two floors of the SAME model. This is the instrument resolution.
    const nullSpread = Math.max(
      Math.abs(exactA.floor - exactB.floor) / Math.min(exactA.floor, exactB.floor),
      Math.abs(coarseA.floor - coarseB.floor) / Math.min(coarseA.floor, coarseB.floor),
    );
    const delta = (exactFloor - coarseFloor) / exactFloor;
    deltasByArm[logs].push(delta * 100);
    nullsByArm[logs].push(nullSpread * 100);

    const label = `logs-${String(logs).padStart(3, '0')}`;
    console.log(
      `${label}  ${result.iterations} spans/block x ${BLOCKS} blocks  load1=${(os.loadavg()[0] ?? 0).toFixed(2)}`,
    );
    for (const [name, a, b] of [
      ['exact ', exactA, exactB],
      ['coarse', coarseA, coarseB],
    ] as const) {
      console.log(
        `  ${name}  floor ${Math.min(a.floor, b.floor).toFixed(0).padStart(6)} ns/span` +
          `  (A ${a.floor.toFixed(0)} / B ${b.floor.toFixed(0)}, mean ${(((a.mean + b.mean) / 2) | 0).toString()},` +
          ` half-floor ${Math.min(a.halfFloor, b.halfFloor).toFixed(0)})`,
      );
    }
    if (logs > 0) {
      console.log(
        `  marginal  exact ${(exactFloor / logs).toFixed(2)} ns/row   coarse ${(coarseFloor / logs).toFixed(2)} ns/row` +
          `   saved ${((exactFloor - coarseFloor) / logs).toFixed(2)} ns/row`,
      );
    }
    console.log(
      `  delta ${(delta * 100).toFixed(1)}%  (${(exactFloor / coarseFloor).toFixed(3)}x)   null spread ${(nullSpread * 100).toFixed(1)}%`,
    );
  }
}

console.log('\n=== verdict ===');
for (const logs of SWEEP) {
  const deltas = deltasByArm[logs];
  const nulls = nullsByArm[logs];
  const worstNull = Math.max(...nulls);
  const label = `logs-${String(logs).padStart(3, '0')}`;
  const spread = `spread ${Math.min(...deltas).toFixed(1)}% .. ${Math.max(...deltas).toFixed(1)}%, worst null ${worstNull.toFixed(1)}%`;
  if (deltas.length < 2) {
    console.log(`${label}  DIRECTION UNRESOLVED — one repetition is not a result; use --repeat 3 or more.`);
    continue;
  }
  const positive = deltas.every((value) => value > 0);
  const negative = deltas.every((value) => value < 0);
  if (!positive && !negative) {
    console.log(`${label}  DIRECTION UNRESOLVED — repetitions disagree on the sign (${spread}).`);
    continue;
  }
  if (Math.min(...deltas.map(Math.abs)) <= worstNull) {
    console.log(`${label}  BOUNDED NOT SIGNED — |delta| is inside the instrument's own null spread (${spread}).`);
    continue;
  }
  console.log(
    `${label}  sign stable WITHIN this invocation across ${deltas.length} repetitions and clear of the null arm ` +
      `(${spread}) — NOT a resolved direction on its own; confirm with an independent invocation before quoting it.`,
  );
}
