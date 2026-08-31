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
 * Arms are reported separately for `logs-000` and a marginal-heavy count: the
 * clock is paid at both, and one aggregate number hides which moved. The
 * marginal arm stops at 50 rows because `SPAN_CAPACITY` is 64 and start plus
 * completion take two, so 62 is the ceiling before a second block chains in and
 * reports capacity growth as marshalling.
 *
 * ABBA-interleaved: position 1 pairs with 4 and 2 with 3, and the 1-minute load
 * average prints on every row. A loaded machine cannot resolve small deltas at
 * all — read nothing here below the load-average line.
 */

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

function arm(tracer: TestTracer<Binding>, logs: number, iterations: number): number {
  const work = (): void => {
    tracer.clear();
    tracer.trace_fn(0, 'c', {}, (span) => {
      for (let i = 0; i < logs; i++) span.log.info(`m${i}`).n(i);
      return span.ok(1);
    });
  };
  for (let i = 0; i < 2000; i++) work();
  let best = Number.POSITIVE_INFINITY;
  for (let round = 0; round < 5; round++) {
    const start = Bun.nanoseconds();
    for (let i = 0; i < iterations; i++) work();
    best = Math.min(best, (Bun.nanoseconds() - start) / iterations);
  }
  return best;
}

const loadavg = async (): Promise<string> => (await Bun.$`sysctl -n vm.loadavg`.text()).trim();

console.log('js-heap arms: exact row stamps vs coarse-16');
for (const logs of [0, 50]) {
  const iterations = logs === 0 ? 20_000 : 6_000;
  // ABBA: exact, coarse, coarse, exact.
  const exactA = arm(exactTracer, logs, iterations);
  const coarseA = arm(coarseTracer, logs, iterations);
  const coarseB = arm(coarseTracer, logs, iterations);
  const exactB = arm(exactTracer, logs, iterations);
  const exact = (exactA + exactB) / 2;
  const coarse = (coarseA + coarseB) / 2;
  const label = `logs-${String(logs).padStart(3, '0')}`;
  console.log(`${label}  loadavg=${await loadavg()}`);
  console.log(`  exact   ${exact.toFixed(0).padStart(6)} ns/span  (A ${exactA.toFixed(0)} / B ${exactB.toFixed(0)})`);
  console.log(
    `  coarse  ${coarse.toFixed(0).padStart(6)} ns/span  (A ${coarseA.toFixed(0)} / B ${coarseB.toFixed(0)})`,
  );
  if (logs > 0) {
    console.log(
      `  marginal: exact ${(exact / logs).toFixed(2)} ns/row  coarse ${(coarse / logs).toFixed(2)} ns/row  ` +
        `saved ${((exact - coarse) / logs).toFixed(2)} ns/row`,
    );
  }
  console.log(`  span delta ${(exact - coarse).toFixed(0)} ns  (${(exact / coarse).toFixed(3)}x)`);
}
