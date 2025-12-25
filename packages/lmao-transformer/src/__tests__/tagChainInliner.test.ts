/**
 * Tests verifying transformer's tag chain inlining produces identical Arrow output
 *
 * Goal: Ensure the transformer's inlining optimizations don't change behavior.
 *
 * Strategy:
 * 1. Create buffers with identical capacity
 * 2. Write using the fluent API (non-transformed behavior) to buffer1
 * 3. Write using direct array access (simulating transformed behavior) to buffer2
 * 4. Convert both to Arrow tables
 * 5. Verify binary output is identical
 *
 * This validates that:
 * - Buffer setter methods produce same output as direct writes
 * - Enum index calculation matches runtime behavior
 * - Null bitmap handling is correct
 * - Eager vs lazy column handling is correct
 */

import { describe, expect, it } from 'bun:test';
import {
  convertToArrowTable,
  createSpanBuffer,
  createTagWriter,
  createTraceId,
  DEFAULT_METADATA,
  defineLogSchema,
  ENTRY_TYPE_SPAN_START,
  S,
  type SpanBuffer,
} from '@smoothbricks/lmao';
import * as arrow from 'apache-arrow';

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * System columns that vary between buffer creations and should be ignored in comparison.
 * These are identity-related columns that differ due to global span counter.
 */
const IGNORED_SYSTEM_COLUMNS = new Set([
  'span_id', // Global counter increments for each buffer
  'thread_id', // Same thread but can vary
]);

/**
 * Compare Arrow tables and return detailed diff info on mismatch.
 * Ignores system columns that vary between buffer creations.
 */
function compareArrowTablesDetailed(
  table1: arrow.Table,
  table2: arrow.Table,
  options: { ignoreSystemColumns?: boolean } = { ignoreSystemColumns: true },
): { equal: boolean; diff?: string } {
  // Compare row counts
  if (table1.numRows !== table2.numRows) {
    return {
      equal: false,
      diff: `Row count mismatch: ${table1.numRows} vs ${table2.numRows}`,
    };
  }

  // Compare schemas
  if (table1.schema.fields.length !== table2.schema.fields.length) {
    return {
      equal: false,
      diff: `Field count mismatch: ${table1.schema.fields.length} vs ${table2.schema.fields.length}`,
    };
  }

  // Compare each row's data
  for (let row = 0; row < table1.numRows; row++) {
    const row1 = table1.get(row)?.toJSON();
    const row2 = table2.get(row)?.toJSON();

    for (const field of table1.schema.fields) {
      // Skip system columns that vary between buffer creations
      if (options.ignoreSystemColumns && IGNORED_SYSTEM_COLUMNS.has(field.name)) {
        continue;
      }

      const val1 = row1?.[field.name];
      const val2 = row2?.[field.name];

      // Handle NaN comparison
      if (typeof val1 === 'number' && typeof val2 === 'number') {
        if (Number.isNaN(val1) && Number.isNaN(val2)) continue;
      }

      // Handle BigInt comparison
      if (typeof val1 === 'bigint' && typeof val2 === 'bigint') {
        if (val1 === val2) continue;
      }

      if (val1 !== val2) {
        return {
          equal: false,
          diff: `Row ${row}, field "${field.name}": ${JSON.stringify(val1)} vs ${JSON.stringify(val2)}`,
        };
      }
    }
  }

  return { equal: true };
}

/**
 * Get enum index using declaration order (matches runtime behavior in fixedPositionWriterGenerator.ts).
 *
 * The runtime creates a switch-case mapping where each value maps to its index in the array.
 * This is NOT sorted alphabetically - it preserves declaration order.
 */
function getEnumIndex(value: string, enumValues: readonly string[]): number {
  return enumValues.indexOf(value);
}

/**
 * Create a pair of test buffers with identical setup for comparison tests.
 * Uses DEFAULT_METADATA so convertToArrowTable has _opMetadata available.
 */
function createTestBufferPair<T extends ReturnType<typeof defineLogSchema>>(
  schema: T,
  traceIdSuffix: string,
  capacity?: number,
): { buffer1: SpanBuffer<T>; buffer2: SpanBuffer<T>; timestamp: bigint } {
  const traceId = createTraceId(`trace-${traceIdSuffix}`);
  const buffer1 = createSpanBuffer(schema, 'test-span', traceId, DEFAULT_METADATA, capacity);
  const buffer2 = createSpanBuffer(schema, 'test-span', traceId, DEFAULT_METADATA, capacity);

  // Setup: write system columns identically
  const timestamp = BigInt(Date.now()) * 1000000n;
  buffer1.timestamp[0] = timestamp;
  buffer1.entry_type[0] = ENTRY_TYPE_SPAN_START;
  buffer1._writeIndex = 1;

  buffer2.timestamp[0] = timestamp;
  buffer2.entry_type[0] = ENTRY_TYPE_SPAN_START;
  buffer2._writeIndex = 1;

  return { buffer1, buffer2, timestamp };
}

// ============================================================================
// Test Suite
// ============================================================================

describe('Tag Chain Inliner - Arrow Output Equivalence', () => {
  describe('literal values for all types', () => {
    const testSchema = defineLogSchema({
      operation: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE'] as const),
      userId: S.category(),
      description: S.text(),
      count: S.number(),
      enabled: S.boolean(),
    });

    it('enum literal produces identical output', () => {
      const { buffer1, buffer2 } = createTestBufferPair(testSchema, 'enum-test');

      // Fluent API (non-transformed)
      const tagWriter1 = createTagWriter(testSchema, buffer1);
      tagWriter1.operation('CREATE');

      // Direct write (simulating transformed)
      const enumIndex = getEnumIndex('CREATE', ['CREATE', 'READ', 'UPDATE', 'DELETE']);
      buffer2.operation(0, enumIndex);

      // Convert to Arrow
      const table1 = convertToArrowTable(buffer1);
      const table2 = convertToArrowTable(buffer2);

      // Verify data matches
      const result = compareArrowTablesDetailed(table1, table2);
      if (!result.equal) {
        console.error('Diff:', result.diff);
        console.error('Row1:', table1.get(0)?.toJSON());
        console.error('Row2:', table2.get(0)?.toJSON());
      }
      expect(result.equal).toBe(true);

      // Verify the enum value is correct
      expect(table1.get(0)?.toJSON().operation).toBe('CREATE');
      expect(table2.get(0)?.toJSON().operation).toBe('CREATE');
    });

    it('category (string) literal produces identical output', () => {
      const { buffer1, buffer2 } = createTestBufferPair(testSchema, 'category-test');

      // Fluent API
      const tagWriter1 = createTagWriter(testSchema, buffer1);
      tagWriter1.userId('user-123');

      // Direct write (category stores raw string)
      buffer2.userId(0, 'user-123');

      const table1 = convertToArrowTable(buffer1);
      const table2 = convertToArrowTable(buffer2);

      const result = compareArrowTablesDetailed(table1, table2);
      expect(result.equal).toBe(true);

      expect(table1.get(0)?.toJSON().userId).toBe('user-123');
      expect(table2.get(0)?.toJSON().userId).toBe('user-123');
    });

    it('text literal produces identical output', () => {
      const { buffer1, buffer2 } = createTestBufferPair(testSchema, 'text-test');

      // Fluent API
      const tagWriter1 = createTagWriter(testSchema, buffer1);
      tagWriter1.description('hello world');

      // Direct write (text stores raw string)
      buffer2.description(0, 'hello world');

      const table1 = convertToArrowTable(buffer1);
      const table2 = convertToArrowTable(buffer2);

      const result = compareArrowTablesDetailed(table1, table2);
      expect(result.equal).toBe(true);

      expect(table1.get(0)?.toJSON().description).toBe('hello world');
      expect(table2.get(0)?.toJSON().description).toBe('hello world');
    });

    it('number literal produces identical output', () => {
      const { buffer1, buffer2 } = createTestBufferPair(testSchema, 'number-test');

      // Fluent API
      const tagWriter1 = createTagWriter(testSchema, buffer1);
      tagWriter1.count(42);

      // Direct write (number uses Float64Array)
      buffer2.count(0, 42);

      const table1 = convertToArrowTable(buffer1);
      const table2 = convertToArrowTable(buffer2);

      const result = compareArrowTablesDetailed(table1, table2);
      expect(result.equal).toBe(true);

      expect(table1.get(0)?.toJSON().count).toBe(42);
      expect(table2.get(0)?.toJSON().count).toBe(42);
    });

    it('boolean true produces identical output', () => {
      const { buffer1, buffer2 } = createTestBufferPair(testSchema, 'bool-true-test');

      // Fluent API
      const tagWriter1 = createTagWriter(testSchema, buffer1);
      tagWriter1.enabled(true);

      // Direct write (boolean uses bit-packed Uint8Array)
      buffer2.enabled(0, true);

      const table1 = convertToArrowTable(buffer1);
      const table2 = convertToArrowTable(buffer2);

      const result = compareArrowTablesDetailed(table1, table2);
      expect(result.equal).toBe(true);

      expect(table1.get(0)?.toJSON().enabled).toBe(true);
      expect(table2.get(0)?.toJSON().enabled).toBe(true);
    });

    it('boolean false produces identical output', () => {
      const { buffer1, buffer2 } = createTestBufferPair(testSchema, 'bool-false-test');

      // Fluent API
      const tagWriter1 = createTagWriter(testSchema, buffer1);
      tagWriter1.enabled(false);

      // Direct write
      buffer2.enabled(0, false);

      const table1 = convertToArrowTable(buffer1);
      const table2 = convertToArrowTable(buffer2);

      const result = compareArrowTablesDetailed(table1, table2);
      expect(result.equal).toBe(true);

      expect(table1.get(0)?.toJSON().enabled).toBe(false);
      expect(table2.get(0)?.toJSON().enabled).toBe(false);
    });
  });

  describe('chained tag calls', () => {
    const testSchema = defineLogSchema({
      operation: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE'] as const),
      userId: S.category(),
      count: S.number(),
    });

    it('multiple chained calls produce identical output', () => {
      const { buffer1, buffer2 } = createTestBufferPair(testSchema, 'chain-test');

      // Fluent API - chained
      const tagWriter1 = createTagWriter(testSchema, buffer1);
      tagWriter1.operation('READ').userId('user-456').count(100);

      // Direct writes - what transformer generates
      const enumIndex = getEnumIndex('READ', ['CREATE', 'READ', 'UPDATE', 'DELETE']);
      buffer2.operation(0, enumIndex);
      buffer2.userId(0, 'user-456');
      buffer2.count(0, 100);

      const table1 = convertToArrowTable(buffer1);
      const table2 = convertToArrowTable(buffer2);

      const result = compareArrowTablesDetailed(table1, table2);
      expect(result.equal).toBe(true);

      const row1 = table1.get(0)?.toJSON();
      const row2 = table2.get(0)?.toJSON();

      expect(row1?.operation).toBe('READ');
      expect(row2?.operation).toBe('READ');
      expect(row1?.userId).toBe('user-456');
      expect(row2?.userId).toBe('user-456');
      expect(row1?.count).toBe(100);
      expect(row2?.count).toBe(100);
    });
  });

  describe('with() bulk setter', () => {
    const testSchema = defineLogSchema({
      operation: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE'] as const),
      userId: S.category(),
      count: S.number(),
    });

    it('with() bulk setter produces identical output to individual calls', () => {
      const { buffer1, buffer2 } = createTestBufferPair(testSchema, 'with-test');

      // Using with() - bulk setter
      const tagWriter1 = createTagWriter(testSchema, buffer1);
      tagWriter1.with({ operation: 'UPDATE', userId: 'user-789', count: 50 });

      // Individual calls (what with() does internally)
      const tagWriter2 = createTagWriter(testSchema, buffer2);
      tagWriter2.operation('UPDATE');
      tagWriter2.userId('user-789');
      tagWriter2.count(50);

      const table1 = convertToArrowTable(buffer1);
      const table2 = convertToArrowTable(buffer2);

      const result = compareArrowTablesDetailed(table1, table2);
      expect(result.equal).toBe(true);
    });

    it('mixed chain with fluent + with() produces correct output', () => {
      const { buffer1, buffer2 } = createTestBufferPair(testSchema, 'mixed-test');

      // Mixed: fluent + with() + fluent
      const tagWriter1 = createTagWriter(testSchema, buffer1);
      tagWriter1.operation('DELETE').with({ userId: 'user-mixed' }).count(25);

      // Equivalent individual calls
      const tagWriter2 = createTagWriter(testSchema, buffer2);
      tagWriter2.operation('DELETE');
      tagWriter2.userId('user-mixed');
      tagWriter2.count(25);

      const table1 = convertToArrowTable(buffer1);
      const table2 = convertToArrowTable(buffer2);

      const result = compareArrowTablesDetailed(table1, table2);
      expect(result.equal).toBe(true);
    });
  });

  describe('enum index calculation', () => {
    const testSchema = defineLogSchema({
      // Enum values NOT in alphabetical order
      status: S.enum(['PENDING', 'ACTIVE', 'COMPLETED', 'CANCELLED'] as const),
    });

    it('verifies enum index uses declaration order', () => {
      // Runtime uses declaration order (NOT alphabetically sorted):
      // PENDING=0, ACTIVE=1, COMPLETED=2, CANCELLED=3
      const enumValues = ['PENDING', 'ACTIVE', 'COMPLETED', 'CANCELLED'] as const;

      expect(getEnumIndex('PENDING', enumValues)).toBe(0);
      expect(getEnumIndex('ACTIVE', enumValues)).toBe(1);
      expect(getEnumIndex('COMPLETED', enumValues)).toBe(2);
      expect(getEnumIndex('CANCELLED', enumValues)).toBe(3);
    });

    it('all enum values produce correct Arrow output', () => {
      const enumValues = ['PENDING', 'ACTIVE', 'COMPLETED', 'CANCELLED'] as const;

      for (const value of enumValues) {
        const { buffer1, buffer2 } = createTestBufferPair(testSchema, `enum-${value}`);

        // Fluent API (user-facing, accepts string)
        const tagWriter1 = createTagWriter(testSchema, buffer1);
        tagWriter1.status(value);

        // Direct write with computed enum index (transformer-optimized, accepts index)
        const enumIndex = getEnumIndex(value, enumValues);
        buffer2.status(0, enumIndex);

        const table1 = convertToArrowTable(buffer1);
        const table2 = convertToArrowTable(buffer2);

        const result = compareArrowTablesDetailed(table1, table2);
        expect(result.equal).toBe(true);

        // Verify the correct string value in Arrow output
        expect(table1.get(0)?.toJSON().status).toBe(value);
        expect(table2.get(0)?.toJSON().status).toBe(value);
      }
    });
  });

  describe('eager columns', () => {
    const testSchema = defineLogSchema({
      lazyField: S.category(),
      eagerField: S.category().eager(),
    });

    it('eager column setter produces identical output to lazy column setter', () => {
      const { buffer1, buffer2 } = createTestBufferPair(testSchema, 'eager-test');

      // Using fluent API for both
      const tagWriter1 = createTagWriter(testSchema, buffer1);
      tagWriter1.lazyField('lazy-value');
      tagWriter1.eagerField('eager-value');

      // Direct writes
      buffer2.lazyField(0, 'lazy-value');
      buffer2.eagerField(0, 'eager-value');

      const table1 = convertToArrowTable(buffer1);
      const table2 = convertToArrowTable(buffer2);

      const result = compareArrowTablesDetailed(table1, table2);
      expect(result.equal).toBe(true);

      expect(table1.get(0)?.toJSON().lazyField).toBe('lazy-value');
      expect(table1.get(0)?.toJSON().eagerField).toBe('eager-value');
    });
  });

  describe('null/undefined handling', () => {
    const testSchema = defineLogSchema({
      nullableNumber: S.number(),
      nullableString: S.category(),
    });

    it('unset columns have default values', () => {
      const { buffer1, buffer2 } = createTestBufferPair(testSchema, 'null-test');

      // Don't write to any user columns
      // Unwritten columns get their type's default value (0 for number, null for category)
      // because the null bitmap is only allocated when values are written

      const table1 = convertToArrowTable(buffer1);
      const table2 = convertToArrowTable(buffer2);

      const result = compareArrowTablesDetailed(table1, table2);
      expect(result.equal).toBe(true);

      // Number defaults to 0 (Float64Array default), category defaults to null (no dictionary entry)
      expect(table1.get(0)?.toJSON().nullableNumber).toBe(0);
      expect(table1.get(0)?.toJSON().nullableString).toBe(null);
    });

    it('partial column writes preserve nulls in unwritten columns', () => {
      const { buffer1, buffer2 } = createTestBufferPair(testSchema, 'partial-test');

      // Write only one column
      const tagWriter1 = createTagWriter(testSchema, buffer1);
      tagWriter1.nullableNumber(42);

      buffer2.nullableNumber(0, 42);

      const table1 = convertToArrowTable(buffer1);
      const table2 = convertToArrowTable(buffer2);

      const result = compareArrowTablesDetailed(table1, table2);
      expect(result.equal).toBe(true);

      // Number column has value, string column is null
      expect(table1.get(0)?.toJSON().nullableNumber).toBe(42);
      expect(table1.get(0)?.toJSON().nullableString).toBe(null);
    });
  });

  describe('multiple rows', () => {
    const testSchema = defineLogSchema({
      value: S.number(),
      tag_val: S.category(),
    });

    it('multiple rows produce identical output', () => {
      const traceId = createTraceId('trace-multirow-test');
      const buffer1 = createSpanBuffer(testSchema, 'test-span', traceId, DEFAULT_METADATA, 16);
      const buffer2 = createSpanBuffer(testSchema, 'test-span', traceId, DEFAULT_METADATA, 16);

      const baseTimestamp = BigInt(Date.now()) * 1000000n;

      // Write 5 rows
      for (let i = 0; i < 5; i++) {
        const timestamp = baseTimestamp + BigInt(i * 1000000);

        buffer1.timestamp[i] = timestamp;
        buffer1.entry_type[i] = ENTRY_TYPE_SPAN_START;

        buffer2.timestamp[i] = timestamp;
        buffer2.entry_type[i] = ENTRY_TYPE_SPAN_START;
      }

      buffer1._writeIndex = 5;
      buffer2._writeIndex = 5;

      // Write tags for each row using fluent API
      const tagWriter1 = createTagWriter(testSchema, buffer1);

      // Row 0
      (tagWriter1 as unknown as { _pos: number })._pos = 0;
      tagWriter1.value(10).tag_val('first');

      // Row 1
      (tagWriter1 as unknown as { _pos: number })._pos = 1;
      tagWriter1.value(20).tag_val('second');

      // Row 2 - no tags (null)

      // Row 3
      (tagWriter1 as unknown as { _pos: number })._pos = 3;
      tagWriter1.value(40).tag_val('fourth');

      // Row 4
      (tagWriter1 as unknown as { _pos: number })._pos = 4;
      tagWriter1.value(50).tag_val('fifth');

      // Direct writes for buffer2
      buffer2.value(0, 10);
      buffer2.tag_val(0, 'first');

      buffer2.value(1, 20);
      buffer2.tag_val(1, 'second');

      // Row 2 - no writes

      buffer2.value(3, 40);
      buffer2.tag_val(3, 'fourth');

      buffer2.value(4, 50);
      buffer2.tag_val(4, 'fifth');

      const table1 = convertToArrowTable(buffer1);
      const table2 = convertToArrowTable(buffer2);

      expect(table1.numRows).toBe(5);
      expect(table2.numRows).toBe(5);

      const result = compareArrowTablesDetailed(table1, table2);
      expect(result.equal).toBe(true);

      // Verify row 2 is null
      expect(table1.get(2)?.toJSON().value).toBe(null);
      expect(table1.get(2)?.toJSON().tag_val).toBe(null);
    });
  });

  describe('IPC round-trip verification', () => {
    const testSchema = defineLogSchema({
      operation: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE'] as const),
      userId: S.category(),
      description: S.text(),
      count: S.number(),
      enabled: S.boolean(),
    });

    it('both fluent and direct writes produce valid IPC that round-trips correctly', () => {
      const { buffer1, buffer2 } = createTestBufferPair(testSchema, 'roundtrip-test');

      // Fluent API
      const tagWriter1 = createTagWriter(testSchema, buffer1);
      tagWriter1.operation('UPDATE').userId('user-roundtrip').description('test description').count(999).enabled(true);

      // Direct writes
      buffer2.operation(0, getEnumIndex('UPDATE', ['CREATE', 'READ', 'UPDATE', 'DELETE']));
      buffer2.userId(0, 'user-roundtrip');
      buffer2.description(0, 'test description');
      buffer2.count(0, 999);
      buffer2.enabled(0, true);

      const table1 = convertToArrowTable(buffer1);
      const table2 = convertToArrowTable(buffer2);

      // Round-trip both tables through IPC
      const ipc1 = arrow.tableToIPC(table1);
      const ipc2 = arrow.tableToIPC(table2);

      const restored1 = arrow.tableFromIPC(ipc1);
      const restored2 = arrow.tableFromIPC(ipc2);

      // Verify restored tables match original
      const result1 = compareArrowTablesDetailed(table1, restored1);
      const result2 = compareArrowTablesDetailed(table2, restored2);

      expect(result1.equal).toBe(true);
      expect(result2.equal).toBe(true);

      // Verify both restored tables match each other
      const crossResult = compareArrowTablesDetailed(restored1, restored2);
      expect(crossResult.equal).toBe(true);
    });
  });
});
