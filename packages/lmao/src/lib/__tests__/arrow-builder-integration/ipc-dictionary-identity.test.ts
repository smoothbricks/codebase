/**
 * Flechette resolves IPC dictionary ids by instance identity of the dictionary
 * value type. Conversion must pair every column's field type and inner
 * dictionary column with one shared value-type instance, or tableToIPC throws
 * `BigInt(undefined)` in encodeDictionary. These tests serialize conversion
 * output DIRECTLY — the path real consumers (native query ingestion) use.
 *
 * Both conversion paths must retain populated system columns (error_code, line,
 * uint64_value, ...) derived from the maintained system schema, and omit
 * unpopulated ones instead of materializing empty columns.
 */
import { describe, expect, it } from 'bun:test';
import { tableFromIPC, tableToIPC } from '@uwdata/flechette';
import { convertSpanTreeToArrowTable, convertToArrowTable } from '../../convertToArrow.js';
import { defineOpContext } from '../../defineOpContext.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import { TestTracer } from '../../tracers/TestTracer.js';
import { createTestTracerOptions } from '../test-helpers.js';

const testOpContext = defineOpContext({
  logSchema: defineLogSchema({ query_cost: S.number() }),
});

describe('Arrow IPC dictionary identity', () => {
  it('serializes conversion output to an IPC stream directly and round-trips values', async () => {
    const tracer = new TestTracer(testOpContext, createTestTracerOptions());
    await tracer.trace('ipc-identity-root', (ctx) => {
      ctx.log.info('first read').query_cost(1.25);
      ctx.log.info('second read').query_cost(2.5);
      for (let index = 0; index < 50; index++) ctx.log.info('overflow read').query_cost(index);
      return ctx.ok(undefined);
    });
    const table = convertToArrowTable(tracer.rootBuffers[0]);
    expect(table.numRows).toBeGreaterThan(0);

    // Pre-fix this threw `Invalid argument type in ToBigInt operation` inside
    // flechette's encodeDictionary because the paired value-type instances
    // differed and the id lookup resolved to undefined.
    const bytes = tableToIPC(table, { format: 'stream' });
    if (!bytes) throw new Error('IPC stream serialization returned no bytes');
    expect(bytes.length).toBeGreaterThan(0);

    const roundTripped = tableFromIPC(bytes);
    const cost = roundTripped.getChild('query_cost');
    expect(cost).toBeDefined();
    // Log rows carry the costs; span lifecycle rows are null.
    const costs: unknown[] = [];
    for (let row = 0; row < roundTripped.numRows; row++) {
      const value = cost?.get(row);
      if (value !== null && value !== undefined) costs.push(value);
    }
    expect(costs.length).toBe(52);
    expect(costs[costs.length - 1]).toBe(49);
  });

  it('retains populated system columns and omits unpopulated ones', async () => {
    const tracer = new TestTracer(testOpContext, createTestTracerOptions());
    await tracer.trace('ipc-system-columns-root', (ctx) => {
      ctx.log.info('reading').query_cost(3.5);
      return ctx.err(Object.assign(new Error('read failed'), { code: 'READ_FAILED' }));
    });
    const table = convertToArrowTable(tracer.rootBuffers[0]);
    const bytes = tableToIPC(table, { format: 'stream' });
    if (!bytes) throw new Error('IPC stream serialization returned no bytes');
    const roundTripped = tableFromIPC(bytes);

    expect(roundTripped.names).toContain('error_code');
    const errorCodes = roundTripped.getChild('error_code');
    expect(errorCodes).toBeDefined();
    const values: unknown[] = [];
    for (let row = 0; row < roundTripped.numRows; row++) values.push(errorCodes?.get(row));
    expect(values).toContain('READ_FAILED');

    // Unpopulated lazy system lanes must not materialize as empty columns.
    expect(roundTripped.names).not.toContain('ff_value');
    expect(roundTripped.names).not.toContain('exception_stack');
    expect(roundTripped.names).not.toContain('line');
  });

  it('serializes tree conversion, retains populated system columns, and omits unpopulated ones', async () => {
    const wide = 18_446_744_073_709_551_615n;
    const tracer = new TestTracer(testOpContext, createTestTracerOptions());
    await tracer.trace('ipc-tree-system-columns-root', (ctx) => {
      ctx.log.info('reading').query_cost(3.5).uint64_value(wide);
      return ctx.err(Object.assign(new Error('read failed'), { code: 'READ_FAILED' }));
    });
    const table = convertSpanTreeToArrowTable(tracer.rootBuffers[0]);
    const bytes = tableToIPC(table, { format: 'stream' });
    if (!bytes) throw new Error('IPC stream serialization returned no bytes');
    const roundTripped = tableFromIPC(bytes);

    expect(roundTripped.names).toContain('error_code');
    expect(roundTripped.names).toContain('uint64_value');
    const errorCodes = roundTripped.getChild('error_code');
    expect(errorCodes).toBeDefined();
    const errorValues: unknown[] = [];
    for (let row = 0; row < roundTripped.numRows; row++) errorValues.push(errorCodes?.get(row));
    expect(errorValues).toContain('READ_FAILED');

    const uint64Values = roundTripped.getChild('uint64_value');
    if (!uint64Values) throw new Error('uint64_value column missing after tree IPC round-trip');
    const raw = uint64Values.data[0]?.values;
    if (!(raw instanceof BigUint64Array)) {
      throw new Error('uint64_value did not round-trip as BigUint64Array');
    }
    expect(Array.from(raw)).toContain(wide);

    expect(roundTripped.names).not.toContain('ff_value');
    expect(roundTripped.names).not.toContain('exception_stack');
    expect(roundTripped.names).not.toContain('line');
  });
  it('preserves distinct text values from parent and child buffers through IPC', async () => {
    const context = defineOpContext({ logSchema: defineLogSchema({ detail: S.text() }) });
    const tracer = new TestTracer(context, createTestTracerOptions());
    await tracer.trace('text-parent', async (ctx) => {
      ctx.log.info('parent-before').detail('parent-before');
      await ctx.span('text-child', (child) => {
        child.log.info('child-value').detail('child-value');
        return child.ok(undefined);
      });
      ctx.log.info('parent-after').detail('parent-after');
      return ctx.ok(undefined);
    });
    const bytes = tableToIPC(convertSpanTreeToArrowTable(tracer.rootBuffers[0]), { format: 'stream' });
    if (!bytes) throw new Error('IPC serialization returned no bytes');
    const table = tableFromIPC(bytes);
    const details = table.getChild('detail');
    if (!details) throw new Error('text column missing after IPC roundtrip');
    const values: string[] = [];
    for (let row = 0; row < table.numRows; row++) {
      const value = details.get(row);
      if (typeof value === 'string') values.push(value);
    }
    expect(values.sort()).toEqual(['child-value', 'parent-after', 'parent-before']);
  });
});
