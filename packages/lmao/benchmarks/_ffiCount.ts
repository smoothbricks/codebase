/**
 * Scratch instrument: counts ABI crossings per span and per log row.
 *
 * Wraps the bound handle's methods plus the runtime scratch encoder so the
 * fixed floor (empty span) and the marginal cost (one log row) are separable.
 */

import { defineOpContext } from '../src/lib/defineOpContext.js';
import { S } from '../src/lib/schema/builder.js';
import { defineLogSchema } from '../src/lib/schema/defineLogSchema.js';
import { ThreadBufferStrategy } from '../src/lib/ThreadBufferStrategy.js';
import { createTraceRoot } from '../src/lib/traceRoot.node.js';
import { TestTracer } from '../src/lib/tracers/TestTracer.js';

const schema = defineLogSchema({ n: S.number() });
const context = defineOpContext({ logSchema: schema });
type Binding = typeof context;

const counts = new Map<string, number>();
const bump = (key: string): void => {
  counts.set(key, (counts.get(key) ?? 0) + 1);
};
const snapshot = (): Map<string, number> => new Map(counts);
const delta = (before: Map<string, number>): Array<[string, number]> => {
  const out: Array<[string, number]> = [];
  for (const [key, value] of counts) {
    const d = value - (before.get(key) ?? 0);
    if (d !== 0) out.push([key, d]);
  }
  out.sort((a, b) => b[1] - a[1]);
  return out;
};

const thread = await ThreadBufferStrategy.create<Binding['logBinding']['logSchema']>({ capacity: 64 });
const tracer = new TestTracer(context, { bufferStrategy: thread, createTraceRoot });
// Wrap every binding the strategy creates, plus the runtime scratch helpers.
const runtime = thread.runtime as unknown as Record<string, unknown>;
const originalCreate = runtime.createBinding as (...args: unknown[]) => Record<string, unknown>;
runtime.createBinding = (...args: unknown[]): Record<string, unknown> => {
  const created = originalCreate(...args);
  for (const key of Object.keys(created)) {
    const fn = created[key];
    if (typeof fn !== 'function') continue;
    const original = fn as (...a: unknown[]) => unknown;
    created[key] = (...a: unknown[]): unknown => {
      bump(`ffi:${key}`);
      return original(...a);
    };
  }
  return created;
};
const originalWrite = runtime.writeUtf8 as (text: string) => { ptr: number; len: number };
runtime.writeUtf8 = (text: string): { ptr: number; len: number } => {
  bump('encode:utf8');
  return originalWrite(text);
};
const originalIntern = runtime.intern as (b: unknown, text: string) => number;
runtime.intern = (b: unknown, text: string): number => {
  bump('encode:utf8');
  return originalIntern(b, text);
};

const exercise = (logs: number): void => {
  tracer.clear();
  tracer.trace_fn(0, 'count', {}, (span) => {
    for (let i = 0; i < logs; i++) span.log.info(`m${i}`).n(i);
    return span.ok(1);
  });
};

// Warm the schema binding and any lazy paths first.
exercise(0);
exercise(4);

const beforeEmpty = snapshot();
exercise(0);
const empty = delta(beforeEmpty);

const beforeOne = snapshot();
exercise(1);
const one = delta(beforeOne);

const beforeMany = snapshot();
exercise(33);
const many = delta(beforeMany);

const total = (rows: Array<[string, number]>): number =>
  rows.filter(([k]) => k.startsWith('ffi:')).reduce((a, [, v]) => a + v, 0);

console.log('--- empty span (0 logs) ---');
for (const [k, v] of empty) console.log(`  ${k}: ${v}`);
console.log(`  TOTAL ffi crossings: ${total(empty)}`);

console.log('--- span + 1 log ---');
for (const [k, v] of one) console.log(`  ${k}: ${v}`);
console.log(`  TOTAL ffi crossings: ${total(one)}`);

console.log('--- span + 33 logs ---');
for (const [k, v] of many) console.log(`  ${k}: ${v}`);
console.log(`  TOTAL ffi crossings: ${total(many)}`);

console.log(`--- marginal per log row: ${(total(many) - total(empty)) / 33} ffi crossings ---`);
