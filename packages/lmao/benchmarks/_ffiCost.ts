/**
 * Scratch instrument: prices the pieces the crossing-count instrument found.
 *
 * 3 crossings on an empty span cannot explain a 5.36 us floor unless a wasm
 * call costs ~1.8 us, so measure the raw call cost directly and price the
 * JS-side span setup separately.
 */

import { defineOpContext } from '../src/lib/defineOpContext.js';
import { S } from '../src/lib/schema/builder.js';
import { defineLogSchema } from '../src/lib/schema/defineLogSchema.js';
import { ThreadBufferStrategy } from '../src/lib/ThreadBufferStrategy.js';
import { createTraceRoot } from '../src/lib/traceRoot.node.js';
import { TestTracer } from '../src/lib/tracers/TestTracer.js';
import type { AnySpanBuffer } from '../src/lib/types.js';
import { requireThreadSpanView } from '../src/lib/wasm/threadSpanView.js';

const schema = defineLogSchema({ n: S.number() });
const context = defineOpContext({ logSchema: schema });
type Binding = typeof context;

const thread = await ThreadBufferStrategy.create<Binding['logBinding']['logSchema']>({ capacity: 64 });
const tracer = new TestTracer(context, { bufferStrategy: thread, createTraceRoot });
const runtime = thread.runtime;
const binding = thread.bindingFor(context.logBinding.logSchema);

function time(label: string, iterations: number, fn: () => void): void {
  for (let i = 0; i < Math.min(iterations, 2000); i++) fn();
  const start = Bun.nanoseconds();
  for (let i = 0; i < iterations; i++) fn();
  const ns = (Bun.nanoseconds() - start) / iterations;
  console.log(`${label.padEnd(42)} ${ns.toFixed(1)} ns`);
}

// 1. Raw ABI crossing cost, nothing else in the loop.
const payload = runtime.writeUtf8('probe');
time('raw ffi: intern (same string)', 200_000, () => {
  binding.intern(payload.ptr, payload.len);
});
time('raw ffi: rowCount export', 200_000, () => {
  runtime.rowCount(binding);
});
time('runtime.writeUtf8("m17")', 200_000, () => {
  runtime.writeUtf8('m17');
});
time('runtime.intern("m17")', 200_000, () => {
  runtime.intern(binding, 'm17');
});

// 2. Whole-span cost through the tracer, by log count.
const exercise = (logs: number) => (): void => {
  tracer.clear();
  tracer.trace_fn(0, 'cost', {}, (span) => {
    for (let i = 0; i < logs; i++) span.log.info(`m${i}`).n(i);
    return span.ok(1);
  });
};
time('tracer: empty span', 20_000, exercise(0));
time('tracer: span + 1 log', 20_000, exercise(1));
time('tracer: span + 33 logs', 5_000, exercise(33));
// 3. Phase split: how much of a span's wall time is view construction?
let createNs = 0;
let createCalls = 0;
const originalCreateSpanBuffer = thread.createSpanBuffer.bind(thread);
thread.createSpanBuffer = ((...args: Parameters<typeof originalCreateSpanBuffer>) => {
  const start = Bun.nanoseconds();
  const result = originalCreateSpanBuffer(...args);
  createNs += Bun.nanoseconds() - start;
  createCalls += 1;
  return result;
}) as typeof thread.createSpanBuffer;

const phase = (label: string, logs: number, iterations: number): void => {
  const work = exercise(logs);
  for (let i = 0; i < 500; i++) work();
  createNs = 0;
  createCalls = 0;
  const start = Bun.nanoseconds();
  for (let i = 0; i < iterations; i++) work();
  const totalNs = (Bun.nanoseconds() - start) / iterations;
  const create = createNs / iterations;
  console.log(
    `${label.padEnd(42)} total ${totalNs.toFixed(0)} ns | createSpanBuffer ${create.toFixed(0)} ns ` +
      `(${((create / totalNs) * 100).toFixed(0)}%, ${(createCalls / iterations).toFixed(1)} calls)`,
  );
};
phase('phase: empty span', 0, 20_000);
phase('phase: span + 33 logs', 33, 5_000);
