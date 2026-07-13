import { describe, expect, it } from 'bun:test';
import fc from 'fast-check';
import { createSpanLogger } from '../codegen/spanLoggerGenerator.js';
import { convertSpanTreeToArrowTable, convertToArrowTable } from '../convertToArrow.js';
import { defineOpContext } from '../defineOpContext.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import {
  ENTRY_TYPE_DEBUG,
  ENTRY_TYPE_ERROR,
  ENTRY_TYPE_INFO,
  ENTRY_TYPE_TRACE,
  ENTRY_TYPE_WARN,
} from '../schema/systemSchema.js';
import { createChildSpanBuffer, createSpanBuffer } from '../spanBuffer.js';
import { TestTracer } from '../tracers/TestTracer.js';
import type { SpanBuffer } from '../types.js';
import { createTestTraceRoot, createTestTracerOptions } from './test-helpers.js';

const schema = defineLogSchema({ marker: S.category() });
const opContext = defineOpContext({ logSchema: schema });

describe('runtime Op-local log template IDs', () => {
  it('normalizes structured compile metadata and shares one frozen empty table', () => {
    const plainA = opContext.defineOp('plain-a', (ctx) => ctx.ok(null));
    const plainB = opContext.defineOp('plain-b', (ctx) => ctx.ok(null), undefined, {
      runtimeHint: 17,
      logTemplateIds: [],
    });
    const inputTemplates = ['first', 'second'];
    const templated = opContext.defineOp('templated', (ctx) => ctx.ok(null), undefined, {
      runtimeHint: 23,
      logTemplateIds: inputTemplates,
    });

    inputTemplates[0] = 'mutated-after-definition';
    expect(plainA.metadata.logTemplateIds).toBe(plainB.metadata.logTemplateIds);
    expect(Object.isFrozen(plainA.metadata.logTemplateIds)).toBe(true);
    expect(plainB.runtimeHint).toBe(17);
    expect(templated.runtimeHint).toBe(23);
    expect(templated.metadata.logTemplateIds).toEqual(['first', 'second']);
    expect(Object.isFrozen(templated.metadata.logTemplateIds)).toBe(true);

    const grouped = opContext.defineOps(
      {
        existing: templated,
        inline: (ctx) => ctx.ok(null),
      },
      {
        inline: { runtimeHint: 31, logTemplateIds: ['inline-only'] },
      },
    );
    expect(grouped.existing).toBe(templated);
    expect(grouped.inline.runtimeHint).toBe(31);
    expect(grouped.inline.metadata.logTemplateIds).toEqual(['inline-only']);

    const overridden = opContext.defineOps(
      { existing: templated },
      { existing: { runtimeHint: 47, logTemplateIds: ['replacement'] } },
    );
    expect(overridden.existing).not.toBe(templated);
    expect(overridden.existing.runtimeHint).toBe(47);
    expect(overridden.existing.metadata.logTemplateIds).toEqual(['replacement']);
    expect(templated.metadata.logTemplateIds).toEqual(['first', 'second']);
  });

  it('stores mixed template and dynamic rows in separate lanes for all five levels', () => {
    const templates = ['info literal', 'debug literal', 'warn literal', 'error literal', 'trace literal'];
    const op = opContext.defineOp('levels', (ctx) => ctx.ok(null), undefined, {
      runtimeHint: 0,
      logTemplateIds: templates,
    });
    const buffer = createSpanBuffer(
      opContext.logBinding.logSchema,
      createTestTraceRoot('template-levels'),
      op.metadata,
      16,
    );
    buffer.message(0, 'span-start');
    buffer.message(1, 'span-end');
    const logger = createSpanLogger(opContext.logBinding.logSchema, buffer);

    logger._infoTemplate(1);
    logger._debugTemplate(2);
    logger._warnTemplate(3);
    logger._errorTemplate(4);
    logger._traceTemplate(5);
    logger.info('dynamic message');

    expect(buffer._messageTemplateIds).toBeInstanceOf(Uint16Array);
    if (!(buffer._messageTemplateIds instanceof Uint16Array)) {
      throw new Error('expected template-bearing buffer to allocate Uint16 storage');
    }
    expect(Array.from(buffer._messageTemplateIds.subarray(2, 8))).toEqual([1, 2, 3, 4, 5, 0]);
    expect(buffer.message_values[2]).toBeUndefined();
    expect(buffer.message_values[3]).toBeUndefined();
    expect(buffer.message_values[4]).toBeUndefined();
    expect(buffer.message_values[5]).toBeUndefined();
    expect(buffer.message_values[6]).toBeUndefined();
    expect(buffer.message_values[7]).toBe('dynamic message');
    expect(Array.from(buffer.entry_type.subarray(2, 7))).toEqual([
      ENTRY_TYPE_INFO,
      ENTRY_TYPE_DEBUG,
      ENTRY_TYPE_WARN,
      ENTRY_TYPE_ERROR,
      ENTRY_TYPE_TRACE,
    ]);

    const table = convertToArrowTable(buffer);
    const message = table.getChild('message');
    if (!message) throw new Error('Arrow table did not contain message column');
    expect(Array.from({ length: 6 }, (_, index) => message.get(index + 2))).toEqual([...templates, 'dynamic message']);
    expect(table.schema.fields.map((field) => field.name)).not.toContain('_messageTemplateIds');
    expect(table.schema.fields.map((field) => field.name)).not.toContain('message_template_ids');

    const baselineOp = opContext.defineOp('levels-baseline', (ctx) => ctx.ok(null));
    const baselineBuffer = createSpanBuffer(
      opContext.logBinding.logSchema,
      createTestTraceRoot('template-levels-baseline'),
      baselineOp.metadata,
      16,
    );
    baselineBuffer.message(0, 'span-start');
    baselineBuffer.message(1, 'span-end');
    const baselineLogger = createSpanLogger(opContext.logBinding.logSchema, baselineBuffer);
    baselineLogger.info(templates[0]);
    baselineLogger.debug(templates[1]);
    baselineLogger.warn(templates[2]);
    baselineLogger.error(templates[3]);
    baselineLogger.trace(templates[4]);
    baselineLogger.info('dynamic message');
    const baselineTable = convertToArrowTable(baselineBuffer);
    const baselineMessage = baselineTable.getChild('message');
    if (!baselineMessage) throw new Error('baseline Arrow table did not contain message column');
    expect(Array.from({ length: 6 }, (_, index) => baselineMessage.get(index + 2))).toEqual(
      Array.from({ length: 6 }, (_, index) => message.get(index + 2)),
    );
    expect(baselineTable.schema.fields.map((field) => field.name)).toEqual(
      table.schema.fields.map((field) => field.name),
    );
  });

  it('resolves arbitrary Op-local tables and mixed rows across overflow chains', () => {
    const scenario = fc.uniqueArray(fc.string({ maxLength: 24 }), { minLength: 1, maxLength: 20 }).chain((templates) =>
      fc
        .array(
          fc.oneof(
            fc.integer({ min: 1, max: templates.length }).map((id) => ({ id, message: '' })),
            fc.string({ maxLength: 24 }).map((message) => ({ id: 0, message })),
          ),
          { minLength: 1, maxLength: 24 },
        )
        .map((rows) => ({ templates, rows })),
    );

    fc.assert(
      fc.property(scenario, ({ templates, rows }) => {
        const op = opContext.defineOp('property', (ctx) => ctx.ok(null), undefined, {
          runtimeHint: 0,
          logTemplateIds: templates,
        });
        const buffer = createSpanBuffer(
          opContext.logBinding.logSchema,
          createTestTraceRoot('template-property'),
          op.metadata,
          8,
        );
        buffer.message(0, 'span-start');
        buffer.message(1, 'span-end');
        const logger = createSpanLogger(opContext.logBinding.logSchema, buffer);
        const expected: string[] = [];
        for (const row of rows) {
          if (row.id === 0) {
            logger.info(row.message);
            expected.push(row.message);
          } else {
            logger._infoTemplate(row.id);
            expected.push(templates[row.id - 1]);
          }
        }

        const table = convertToArrowTable(buffer);
        const message = table.getChild('message');
        if (!message) throw new Error('Arrow table did not contain message column');
        expect(Array.from({ length: rows.length }, (_, index) => message.get(index + 2))).toEqual(expected);
      }),
      { numRuns: 40 },
    );
  });

  it('throws on an invalid nonzero ID instead of silently reading dynamic storage', () => {
    const op = opContext.defineOp('invalid', (ctx) => ctx.ok(null), undefined, {
      runtimeHint: 0,
      logTemplateIds: ['only'],
    });
    const buffer = createSpanBuffer(
      opContext.logBinding.logSchema,
      createTestTraceRoot('template-invalid'),
      op.metadata,
      8,
    );
    buffer.message(0, 'span-start');
    buffer.message(1, 'span-end');
    const logger = createSpanLogger(opContext.logBinding.logSchema, buffer);
    logger._infoTemplate(2);

    expect(() => convertToArrowTable(buffer)).toThrow('Invalid message template ID 2 at row 2');
  });

  it('preserves template metadata and exact Arrow strings through child, prefix, and map views', () => {
    const libraryContext = defineOpContext({ logSchema: defineLogSchema({ value: S.text() }) });
    const libraryOps = libraryContext.defineOps(
      { work: (ctx) => ctx.ok(null) },
      { work: { runtimeHint: 0, logTemplateIds: ['library literal'] } },
    );
    const remappedLibrary = libraryOps.mapColumns({ value: 'renamed' }).prefix('lib');
    const remappedOp = remappedLibrary.work;
    expect(remappedOp.metadata.logTemplateIds).toBe(libraryOps.work.metadata.logTemplateIds);

    const appContext = defineOpContext({
      logSchema: defineLogSchema({ app: S.category() }),
      deps: { library: remappedLibrary },
    });
    const parentOp = appContext.defineOp('parent', (ctx) => ctx.ok(null));
    const parent = createSpanBuffer(
      appContext.logBinding.logSchema,
      createTestTraceRoot('template-remap'),
      parentOp.metadata,
      8,
    );
    parent.message(0, 'parent');
    parent._writeIndex = 1;

    const child = createChildSpanBuffer(parent, remappedOp.SpanBufferClass, parentOp.metadata, remappedOp.metadata, 8);
    child.message(0, 'child');
    child.message(1, 'done');
    const childLogger = createSpanLogger(libraryContext.logBinding.logSchema, child);
    childLogger._infoTemplate(1).value('payload');
    const remapDescriptor = remappedOp.remapDescriptor;
    if (!remapDescriptor) throw new Error('mapped and prefixed Op did not provide a remap descriptor');
    child._remapDescriptor = remapDescriptor;
    parent._children.push(child);

    const table = convertSpanTreeToArrowTable(parent);
    const messages = table.getChild('message');
    const mappedValue = table.getChild('lib_renamed');
    if (!messages || !mappedValue) throw new Error('Arrow table omitted remapped child columns');
    expect(Array.from({ length: table.numRows }, (_, row) => messages.get(row))).toContain('library literal');
    expect(Array.from({ length: table.numRows }, (_, row) => mappedValue.get(row))).toContain('payload');
  });

  it('leaves child span lifecycle hooks and Promise scheduling unchanged', async () => {
    const lifecycleContext = defineOpContext({ logSchema: schema });
    type LifecycleSchema = (typeof lifecycleContext)['logBinding']['logSchema'];
    class LifecycleTracer extends TestTracer<typeof lifecycleContext> {
      starts = 0;
      ends = 0;

      override onSpanStart(_buffer: SpanBuffer<LifecycleSchema>): void {
        this.starts++;
      }

      override onSpanEnd(_buffer: SpanBuffer<LifecycleSchema>): void {
        this.ends++;
      }
    }

    const child = lifecycleContext.defineOp('child', (ctx) => ctx.ok('child'), undefined, {
      runtimeHint: 0,
      logTemplateIds: ['unused but allocated'],
    });
    const parent = lifecycleContext.defineOp('parent', async (ctx) => {
      const pending = ctx.span('child-call', child);
      expect(pending).toBeInstanceOf(Promise);
      await pending;
      return ctx.ok('parent');
    });
    const tracer = new LifecycleTracer(lifecycleContext, createTestTracerOptions());

    await tracer.trace('root', parent);
    expect(tracer.starts).toBe(1);
    expect(tracer.ends).toBe(1);
    expect(tracer.rootBuffers).toHaveLength(1);
  });
});
