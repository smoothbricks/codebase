/**
 * Public integration contracts for immutable library remap descriptors.
 *
 * Mapped operations keep canonical child SpanBuffers in the logical tree. Arrow
 * conversion applies their immutable descriptors only during cold traversal.
 */

import { describe, expect, it } from 'bun:test';
// Must import test-helpers first to initialize timestamp implementation
import './test-helpers.js';
import type { Table } from '@uwdata/flechette';
import { convertSpanTreeToArrowTable } from '../convertToArrow.js';
import { defineOpContext } from '../defineOpContext.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { TestTracer } from '../tracers/TestTracer.js';
import type { AnySpanBuffer } from '../types.js';
import { createTestTracerOptions } from './test-helpers.js';

const ARROW_METADATA_FIELDS = [
  'timestamp',
  'trace_id',
  'thread_id',
  'span_id',
  'parent_thread_id',
  'parent_span_id',
  'entry_type',
  'package_name',
  'package_file',
  'git_sha',
  'message',
  'uint64_value',
] as const;

function extractRows(table: Table, columns: readonly string[]): Array<Record<string, unknown>> {
  return Array.from({ length: table.numRows }, (_, rowIndex) => {
    const row: Record<string, unknown> = {};
    for (const columnName of columns) {
      const column = table.getChild(columnName);
      if (!column) throw new Error(`Arrow table omitted '${columnName}'`);
      row[columnName] = column.get(rowIndex);
    }
    return row;
  });
}

function expectArrowSchema(table: Table, userFields: readonly string[]): void {
  expect(table.schema.fields.map((field) => field.name)).toEqual([...ARROW_METADATA_FIELDS, ...userFields]);
}

describe('Library remap descriptor integration', () => {
  it('composes nested prefixes for same-schema siblings without wrapping canonical child buffers', async () => {
    const librarySchema = defineLogSchema({
      value: S.number(),
      label: S.category(),
    });
    const libraryContext = defineOpContext({ logSchema: librarySchema });
    const childBuffers: AnySpanBuffer[] = [];
    const libraryOps = libraryContext.defineOps({
      recordNested: async (ctx) => {
        childBuffers.push(ctx.buffer);
        ctx.tag.value(11).label('nested');
        return ctx.ok(null);
      },
      recordSibling: async (ctx) => {
        childBuffers.push(ctx.buffer);
        ctx.tag.value(22).label('sibling');
        return ctx.ok(null);
      },
    });
    const nested = libraryOps.prefix('api').prefix('v1');
    const sibling = libraryOps.prefix('sibling');

    const appContext = defineOpContext({
      logSchema: defineLogSchema({ rootValue: S.number() }),
      deps: { nested, sibling },
    });
    let rootBuffer: AnySpanBuffer | undefined;
    const rootOp = appContext.defineOp('root-op', async (ctx) => {
      rootBuffer = ctx.buffer;
      ctx.tag.rootValue(1);
      await ctx.span('nested-child', ctx.deps.nested.recordNested);
      await ctx.span('sibling-child', ctx.deps.sibling.recordSibling);
      return ctx.ok(null);
    });

    const { trace } = new TestTracer(appContext, createTestTracerOptions());
    await trace('root-span', rootOp);

    if (!rootBuffer) throw new Error('root span did not expose its buffer');
    expect(childBuffers).toHaveLength(2);
    const nestedBuffer = childBuffers[0];
    const siblingBuffer = childBuffers[1];
    if (!nestedBuffer || !siblingBuffer) throw new Error('library child buffers were not captured');

    expect(rootBuffer._children).toHaveLength(2);
    expect(rootBuffer._children[0]).toBe(nestedBuffer);
    expect(rootBuffer._children[1]).toBe(siblingBuffer);
    expect(nestedBuffer._logSchema).toBe(libraryContext.logBinding.logSchema);
    expect(siblingBuffer._logSchema).toBe(libraryContext.logBinding.logSchema);
    expect(nestedBuffer.constructor).toBe(siblingBuffer.constructor);
    expect(nestedBuffer._remapDescriptor).toBe(nested.recordNested.callsitePlan.remapDescriptor ?? undefined);
    expect(siblingBuffer._remapDescriptor).toBe(sibling.recordSibling.callsitePlan.remapDescriptor ?? undefined);
    expect(nested.recordNested.callsitePlan.remapDescriptor?.sourceNames).toEqual({
      v1_api_value: 'value',
      v1_api_label: 'label',
    });
    expect(sibling.recordSibling.callsitePlan.remapDescriptor?.sourceNames).toEqual({
      sibling_value: 'value',
      sibling_label: 'label',
    });

    const table = convertSpanTreeToArrowTable(rootBuffer);
    expectArrowSchema(table, ['rootValue', 'v1_api_value', 'v1_api_label', 'sibling_value', 'sibling_label']);
    expect(table.numRows).toBe(6);
    expect(
      extractRows(table, [
        'entry_type',
        'message',
        'rootValue',
        'v1_api_value',
        'v1_api_label',
        'sibling_value',
        'sibling_label',
      ]),
    ).toEqual([
      {
        entry_type: 'span-start',
        message: 'root-span',
        rootValue: 1,
        v1_api_value: null,
        v1_api_label: null,
        sibling_value: null,
        sibling_label: null,
      },
      {
        entry_type: 'span-ok',
        message: null,
        rootValue: null,
        v1_api_value: null,
        v1_api_label: null,
        sibling_value: null,
        sibling_label: null,
      },
      {
        entry_type: 'span-start',
        message: 'nested-child',
        rootValue: null,
        v1_api_value: 11,
        v1_api_label: 'nested',
        sibling_value: null,
        sibling_label: null,
      },
      {
        entry_type: 'span-ok',
        message: null,
        rootValue: null,
        v1_api_value: null,
        v1_api_label: null,
        sibling_value: null,
        sibling_label: null,
      },
      {
        entry_type: 'span-start',
        message: 'sibling-child',
        rootValue: null,
        v1_api_value: null,
        v1_api_label: null,
        sibling_value: 22,
        sibling_label: 'sibling',
      },
      {
        entry_type: 'span-ok',
        message: null,
        rootValue: null,
        v1_api_value: null,
        v1_api_label: null,
        sibling_value: null,
        sibling_label: null,
      },
    ]);
  });

  it('keeps remapped values and nulls exact across an overflow chain', async () => {
    const librarySchema = defineLogSchema({ value: S.number() });
    const libraryContext = defineOpContext({ logSchema: librarySchema });
    let childBuffer: AnySpanBuffer | undefined;
    const libraryOps = libraryContext.defineOps({
      overflow: async (ctx) => {
        childBuffer = ctx.buffer;
        ctx.tag.value(100);
        for (let index = 0; index < 9; index++) {
          const entry = ctx.log.info(`event-${index}`);
          if (index % 2 === 0) entry.value(index);
        }
        return ctx.ok(null);
      },
    });
    const mapped = libraryOps.prefix('overflow');
    const originalCapacity = mapped.overflow.callsitePlan.SpanBufferClass.stats.capacity;
    mapped.overflow.callsitePlan.SpanBufferClass.stats.capacity = 8;

    const appContext = defineOpContext({
      logSchema: defineLogSchema({}),
      deps: { mapped },
    });
    let rootBuffer: AnySpanBuffer | undefined;
    const rootOp = appContext.defineOp('root-op', async (ctx) => {
      rootBuffer = ctx.buffer;
      await ctx.span('overflow-child', ctx.deps.mapped.overflow);
      return ctx.ok(null);
    });

    try {
      const { trace } = new TestTracer(appContext, createTestTracerOptions());
      await trace('root-span', rootOp);
    } finally {
      mapped.overflow.callsitePlan.SpanBufferClass.stats.capacity = originalCapacity;
    }

    if (!rootBuffer || !childBuffer) throw new Error('overflow trace did not expose its buffers');
    expect(rootBuffer._children[0]).toBe(childBuffer);
    expect(childBuffer._overflow).toBeDefined();
    expect(childBuffer._remapDescriptor).toBe(mapped.overflow.callsitePlan.remapDescriptor ?? undefined);

    const table = convertSpanTreeToArrowTable(rootBuffer);
    expectArrowSchema(table, ['overflow_value']);
    expect(table.numRows).toBe(13);
    expect(extractRows(table, ['entry_type', 'message', 'overflow_value'])).toEqual([
      { entry_type: 'span-start', message: 'root-span', overflow_value: null },
      { entry_type: 'span-ok', message: null, overflow_value: null },
      { entry_type: 'span-start', message: 'overflow-child', overflow_value: 100 },
      { entry_type: 'span-ok', message: null, overflow_value: null },
      { entry_type: 'info', message: 'event-0', overflow_value: 0 },
      { entry_type: 'info', message: 'event-1', overflow_value: null },
      { entry_type: 'info', message: 'event-2', overflow_value: 2 },
      { entry_type: 'info', message: 'event-3', overflow_value: null },
      { entry_type: 'info', message: 'event-4', overflow_value: 4 },
      { entry_type: 'info', message: 'event-5', overflow_value: null },
      { entry_type: 'info', message: 'event-6', overflow_value: 6 },
      { entry_type: 'info', message: 'event-7', overflow_value: null },
      { entry_type: 'info', message: 'event-8', overflow_value: 8 },
    ]);
  });

  it('preserves exact schema, preorder rows, nulls, and values through a depth-3 remapped tree', async () => {
    const treeSchema = defineLogSchema({
      outerValue: S.boolean(),
      middleValue: S.number(),
      leafValue: S.category(),
    });
    const leafContext = defineOpContext({ logSchema: treeSchema });
    const leafOps = leafContext.defineOps({
      run: async (ctx) => {
        ctx.tag.leafValue('leaf');
        return ctx.ok(null);
      },
    });
    const mappedLeaf = leafOps
      .mapColumns({ outerValue: null, middleValue: null, leafValue: 'leafValue' })
      .prefix('leaf');

    const middleContext = defineOpContext({ logSchema: treeSchema });
    const middleOps = middleContext.defineOps({
      run: async (ctx) => {
        ctx.tag.middleValue(2);
        await ctx.span('leaf-span', mappedLeaf.run);
        return ctx.ok(null);
      },
    });
    const mappedMiddle = middleOps
      .mapColumns({ outerValue: null, middleValue: 'middleValue', leafValue: null })
      .prefix('middle');

    const outerContext = defineOpContext({ logSchema: treeSchema });
    const outerOps = outerContext.defineOps({
      run: async (ctx) => {
        ctx.tag.outerValue(true);
        await ctx.span('middle-span', mappedMiddle.run);
        return ctx.ok(null);
      },
    });
    const mappedOuter = outerOps
      .mapColumns({ outerValue: 'outerValue', middleValue: null, leafValue: null })
      .prefix('outer');

    const appContext = defineOpContext({
      logSchema: defineLogSchema({ rootValue: S.category() }),
      deps: { outer: mappedOuter },
    });
    let rootBuffer: AnySpanBuffer | undefined;
    const rootOp = appContext.defineOp('root-op', async (ctx) => {
      rootBuffer = ctx.buffer;
      ctx.tag.rootValue('root');
      await ctx.span('outer-span', ctx.deps.outer.run);
      return ctx.ok(null);
    });

    const { trace } = new TestTracer(appContext, createTestTracerOptions());
    await trace('root-span', rootOp);

    if (!rootBuffer) throw new Error('depth-3 trace did not expose its root buffer');
    const table = convertSpanTreeToArrowTable(rootBuffer);
    expectArrowSchema(table, ['rootValue', 'outer_outerValue', 'middle_middleValue', 'leaf_leafValue']);
    expect(table.numRows).toBe(8);
    expect(
      extractRows(table, [
        'entry_type',
        'message',
        'rootValue',
        'outer_outerValue',
        'middle_middleValue',
        'leaf_leafValue',
      ]),
    ).toEqual([
      {
        entry_type: 'span-start',
        message: 'root-span',
        rootValue: 'root',
        outer_outerValue: null,
        middle_middleValue: null,
        leaf_leafValue: null,
      },
      {
        entry_type: 'span-ok',
        message: null,
        rootValue: null,
        outer_outerValue: null,
        middle_middleValue: null,
        leaf_leafValue: null,
      },
      {
        entry_type: 'span-start',
        message: 'outer-span',
        rootValue: null,
        outer_outerValue: true,
        middle_middleValue: null,
        leaf_leafValue: null,
      },
      {
        entry_type: 'span-ok',
        message: null,
        rootValue: null,
        outer_outerValue: null,
        middle_middleValue: null,
        leaf_leafValue: null,
      },
      {
        entry_type: 'span-start',
        message: 'middle-span',
        rootValue: null,
        outer_outerValue: null,
        middle_middleValue: 2,
        leaf_leafValue: null,
      },
      {
        entry_type: 'span-ok',
        message: null,
        rootValue: null,
        outer_outerValue: null,
        middle_middleValue: null,
        leaf_leafValue: null,
      },
      {
        entry_type: 'span-start',
        message: 'leaf-span',
        rootValue: null,
        outer_outerValue: null,
        middle_middleValue: null,
        leaf_leafValue: 'leaf',
      },
      {
        entry_type: 'span-ok',
        message: null,
        rootValue: null,
        outer_outerValue: null,
        middle_middleValue: null,
        leaf_leafValue: null,
      },
    ]);
  });
});
