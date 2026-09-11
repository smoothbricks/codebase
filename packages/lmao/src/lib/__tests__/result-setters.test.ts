import { afterEach, describe, expect, it } from 'bun:test';
import { type MaterializerMode, setMaterializerModeOverride } from '@smoothbricks/arrow-builder';
import type { ErrResult, OkResult } from '../../index.js';
import { getResultWriterClass } from '../codegen/fixedPositionWriterGenerator.js';
import { defineOpContext } from '../defineOpContext.js';
import { Err, getResultClasses, Ok, type Result, SPAN_COMPLETION_OWNER_ERROR } from '../result.js';
import type { MessageLayoutFamily, MessagePhysicalLayout } from '../runtimeHint.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { createOverflowBuffer, createSpanBuffer, getSpanBufferClass } from '../spanBuffer.js';
import { TestTracer } from '../tracers/TestTracer.js';
import {
  createTestOpMetadata,
  createTestSchema,
  createTestSpanContext,
  createTestTraceRoot,
  createTestTracerOptions,
} from './test-helpers.js';

const schema = defineLogSchema({ status: S.number(), phase: S.enum(['start', 'done']) });

// Compile-only public API checks: no assertions or reflection may hide lost types.
function assertResultTypes(ok: OkResult<number, typeof schema>, err: ErrResult<string, typeof schema>): void {
  const success: Result<number, string> = ok.status(200).phase('done');
  const failure: Result<number, string> = err.with({ status: 404 }).phase('done');
  const mapped: string = ok.message('ok').map(String).status(200).value;
  const mappedError: number = err.line(7).mapErr(Number).status(404).error;
  const chained: string = ok.flatMap(() => err).phase('done').error;
  // @ts-expect-error - preserve the schema's numeric field type
  ok.status('200');
  // @ts-expect-error - preserve the schema's enum literal union
  err.phase('missing');
  // @ts-expect-error - unknown fields must not become callable
  ok.missing(1);
  // @ts-expect-error - a result is not a thenable
  ok.then(() => 1);
  void success;
  void failure;
  void mapped;
  void mappedError;
  void chained;
}
void assertResultTypes;

afterEach(() => setMaterializerModeOverride(undefined));

const modes: readonly MaterializerMode[] = ['compiled', 'closure'];
const families: readonly MessageLayoutFamily[] = ['mixed', 'dynamic-only', 'static-only'];
const physicals: readonly MessagePhysicalLayout[] = ['current', 'specialized', 'packed'];

describe('schema-bound result setters', () => {
  for (const mode of modes) {
    for (const family of families) {
      for (const physical of physicals) {
        it(`reuses ${mode} writer methods for ${family}/${physical} without a writer allocation`, () => {
          setMaterializerModeOverride(mode);
          const runtimeSchema = createTestSchema(schema.fields);
          const BufferClass = getSpanBufferClass(runtimeSchema, family, physical);
          const buffer = createSpanBuffer(runtimeSchema, createTestTraceRoot(), createTestOpMetadata(), 8, BufferClass);
          const state = createTestSpanContext(runtimeSchema, buffer);
          const WriterClass = getResultWriterClass(runtimeSchema, family, physical);
          // Bind the declared user schema, not the storage fixture's open-ended system fields.
          const classes = getResultClasses<typeof schema>(WriterClass);
          expect(getResultClasses(WriterClass)).toBe(classes);
          for (const key of ['status', 'phase', 'with', 'message', 'line', 'uint64_value']) {
            const writerMethod = Object.getOwnPropertyDescriptor(WriterClass.prototype, key);
            expect(Object.getOwnPropertyDescriptor(classes.OkClass.prototype, key)).toEqual(writerMethod);
            expect(Object.getOwnPropertyDescriptor(classes.ErrClass.prototype, key)).toEqual(writerMethod);
          }

          const ok = new classes.OkClass(42, state);
          expect(ok).toBeInstanceOf(Ok);
          expect(ok.with({ status: 201 }).phase('done').status(200).uint64_value(123n)).toBe(ok);
          expect(ok.message('finished').line(42)).toBe(ok);
          expect(ok.value).toBe(42);
          expect(ok.success).toBe(true);
          expect(Object.hasOwn(ok, '_writer')).toBe(false);
          expect(buffer.status_values[1]).toBe(200);
          expect(buffer.phase_values[1]).toBe(1);
          expect(buffer.uint64_value_values[1]).toBe(123n);
          expect(buffer.line_values[1]).toBe(42);
          expect(family === 'static-only' ? buffer._terminalMessage : buffer.message_values?.[1]).toBe('finished');

          const overflow = createOverflowBuffer(buffer);
          state._buffer = overflow;
          const mapped = ok.map(String).status(202);
          expect(mapped.value).toBe('42');
          expect(Object.getPrototypeOf(mapped)).toBe(Object.getPrototypeOf(ok));
          expect(Object.hasOwn(mapped, '_writer')).toBe(false);
          expect(() => mapped._assertOwner(state)).not.toThrow();
          expect(() => mapped._assertOwner({})).toThrow(SPAN_COMPLETION_OWNER_ERROR);
          expect(ok.mapErr(String)).toBe(ok);
          expect(buffer.status_values[1]).toBe(202);
          expect(overflow.getColumnIfAllocated('status')).toBeUndefined();

          const err = new classes.ErrClass('failed', state);
          expect(err).toBeInstanceOf(Err);
          expect(err.status(400).with({ phase: 'done' }).message('failed').line(43)).toBe(err);
          expect(err.success).toBe(false);
          expect(err.error).toBe('failed');
          expect(err.map(Number)).toBe(err);
          expect(err.flatMap(() => ok)).toBe(err);
          const mappedErr = err.mapErr((error) => error.length).status(500);
          expect(mappedErr.error).toBe(6);
          expect(Object.getPrototypeOf(mappedErr)).toBe(Object.getPrototypeOf(err));
          expect(Object.hasOwn(mappedErr, '_writer')).toBe(false);
          expect(() => mappedErr._assertOwner(state)).not.toThrow();
          expect(buffer.status_values[1]).toBe(500);
          const statusNulls = buffer.getNullsIfAllocated('status');
          if (!statusNulls) throw new Error('status null bitmap was not allocated');
          expect(statusNulls[0] & 1).toBe(0); // No result setter touched row 0.
          expect(statusNulls[0] & 2).toBe(2);
        });
      }
    }
  }

  it('does not reuse a result class when the selected writer materializer changes', () => {
    setMaterializerModeOverride('compiled');
    const compiled = getResultClasses(getResultWriterClass(schema));
    setMaterializerModeOverride('closure');
    const closure = getResultClasses(getResultWriterClass(schema));
    expect(closure).not.toBe(compiled);
    expect(closure.OkClass).not.toBe(compiled.OkClass);
    setMaterializerModeOverride('compiled');
    expect(getResultClasses(getResultWriterClass(schema))).toBe(compiled);
  });

  it('protects payloads, discriminants and promise assimilation with raw low-level schemas', async () => {
    const raw = createTestSchema({
      success: S.boolean(),
      value: S.text(),
      map: S.text(),
      // biome-ignore lint/suspicious/noThenProperty: Regression fixture deliberately exercises a raw then field.
      then: S.text(),
    });
    const buffer = createSpanBuffer(raw, createTestTraceRoot(), createTestOpMetadata(), 8);
    const state = createTestSpanContext(raw, buffer);
    const { OkClass, ErrClass } = getResultClasses<typeof raw>(getResultWriterClass(raw));
    const ok = new OkClass(7, state);
    const err = new ErrClass('failed', state);
    expect(ok.value).toBe(7);
    expect(err.success).toBe(false);
    expect(err.error).toBe('failed');
    expect(ok.map(String).value).toBe('7');
    expect('then' in ok).toBe(false);
    expect('then' in err).toBe(false);
    expect(await Promise.resolve(ok)).toBe(ok);
    expect(await Promise.resolve(err)).toBe(err);
    expect(() => ok._assertOwner(state)).not.toThrow();
  });

  it('preserves sync, async and child-span payload inference through the public API', async () => {
    const context = defineOpContext({ logSchema: schema });
    const sync = context.defineOp('result-setters-sync', (ctx) => ctx.ok(7).status(200).map(String));
    const asyncOp = context.defineOp('result-setters-async', async (ctx) =>
      ctx.err('failed').status(400).mapErr(String),
    );
    const tracer = new TestTracer(context, createTestTracerOptions());
    const syncResult = tracer.trace('sync', sync);
    expect(syncResult.value.toUpperCase()).toBe('7');
    const asyncResult = await tracer.trace('async', asyncOp);
    expect(asyncResult.error.toUpperCase()).toBe('FAILED');
    const parent = await tracer.trace('parent', async (ctx) => {
      const child = ctx.spanSync('child', (childCtx) => childCtx.ok(3).status(201));
      const next = await ctx.span('async-child', async (childCtx) => childCtx.ok(child.value.toFixed()).status(200));
      if (!next.success) return ctx.err(next.error).status(500);
      return ctx.ok(next.value.toUpperCase()).status(200);
    });
    if (!parent.success) throw new Error('Expected successful parent');
    expect(parent.value).toBe('3');
  });
});
