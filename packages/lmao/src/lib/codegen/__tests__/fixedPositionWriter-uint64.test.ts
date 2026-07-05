/**
 * Behavioral tests for the fluent `uint64_value(value)` writer on TagWriter (row 0)
 * and ResultWriter (row 1).
 *
 * `uint64_value` is a RESERVED system column (systemSchema), stored as a LAZY
 * BigUint64Array multiplexed by row. Both writers route through the buffer's own
 * lazy setter `buffer.uint64_value(pos, val)`, which allocates-then-writes and
 * flips the Arrow null bit (1 = valid, 0 = null).
 *
 * These tests defend the observable contract:
 *  - the value lands at the writer's fixed row (0 for tag, 1 for result),
 *  - the null bit is set for a written value and cleared for `null`,
 *  - rows 0 and 1 hold INDEPENDENT values (the load-bearing no-collision invariant,
 *    since span lifecycle writes never touch `uint64_value`),
 *  - allocation is lazy, chaining returns the writer, call order is irrelevant,
 *  - and large near-2^64 bigints round-trip without precision loss while `0n`
 *    is a distinct non-null zero (not "absent").
 *
 * `packages/lmao` is exempt from the traced-suite requirement, so this is a plain
 * bun:test file (matches ../../__tests__/arrow-builder-integration/lazy-columns.test.ts).
 */

import { describe, expect, it } from 'bun:test';
import { createResultWriter, createTagWriter, ENTRY_TYPE_SPAN_OK, ENTRY_TYPE_SPAN_START, S } from '@smoothbricks/lmao';
import { createTestOpMetadata, createTestSchema, createTestTraceRoot } from '../../__tests__/test-helpers.js';
import { createSpanBuffer } from '../../spanBuffer.js';
import type { AnySpanBuffer } from '../../types.js';

// ---------------------------------------------------------------------------
// Fixtures & readers
// ---------------------------------------------------------------------------

// Schema carries sibling user columns (batchId/count) so tests can prove that
// `uint64_value` neither interferes with nor is clobbered by other columns.
// One schema instance is shared between the buffer and its writers so their
// column sets are guaranteed identical.
function setup() {
  const schema = createTestSchema({ batchId: S.category(), count: S.number() });
  const buffer = createSpanBuffer(schema, createTestTraceRoot('t'), createTestOpMetadata(), 8);
  return { schema, buffer };
}

// Read the allocated `uint64_value` values, narrowed to BigUint64Array.
// Uses getColumnIfAllocated so it never triggers allocation as a side effect.
function uint64Column(buffer: AnySpanBuffer): BigUint64Array {
  const col = buffer.getColumnIfAllocated('uint64_value');
  if (!(col instanceof BigUint64Array)) {
    throw new Error('uint64_value column was not allocated as a BigUint64Array');
  }
  return col;
}

function uint64Nulls(buffer: AnySpanBuffer): Uint8Array {
  const nulls = buffer.getNullsIfAllocated('uint64_value');
  if (!(nulls instanceof Uint8Array)) {
    throw new Error('uint64_value null bitmap was not allocated');
  }
  return nulls;
}

// Arrow null bitmap: bit set (1) => valid/non-null; bit clear (0) => null/absent.
// Named because the bit-extraction formula is not self-explanatory inline and is
// asserted at many call sites in lockstep.
function isNonNull(nulls: Uint8Array, pos: number): boolean {
  return (nulls[pos >>> 3] & (1 << (pos & 7))) !== 0;
}

// Invoke a writer's uint64_value at runtime with a value TypeScript would reject
// (null), to exercise the setter's null-clearing branch without an unsafe `as` cast
// (the repo's eslint bans no-unsafe-type-assertion). Mirrors the Reflect pattern in
// arrow-builder's columnBufferSetters.test.ts.
function writeUint64Runtime(writer: object, value: bigint | null): void {
  const method = Reflect.get(writer, 'uint64_value');
  if (typeof method !== 'function') {
    throw new Error('writer is missing a uint64_value method');
  }
  Reflect.apply(method, writer, [value]);
}

// ---------------------------------------------------------------------------
// Row placement
// ---------------------------------------------------------------------------

describe('uint64_value row placement', () => {
  it('TagWriter writes to row 0 (span-start)', () => {
    const { schema, buffer } = setup();

    createTagWriter(schema, buffer).uint64_value(123n);

    const values = uint64Column(buffer);
    const nulls = uint64Nulls(buffer);
    expect(values[0]).toBe(123n);
    expect(isNonNull(nulls, 0)).toBe(true);
    // Row 1 must be untouched by a tag write.
    expect(isNonNull(nulls, 1)).toBe(false);
  });

  it('ResultWriter writes to row 1 (span-completion)', () => {
    const { schema, buffer } = setup();

    createResultWriter(schema, buffer, { ok: true }, false).uint64_value(456n);

    const values = uint64Column(buffer);
    const nulls = uint64Nulls(buffer);
    expect(values[1]).toBe(456n);
    expect(isNonNull(nulls, 1)).toBe(true);
    // Row 0 must be untouched by a result write.
    expect(isNonNull(nulls, 0)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// The load-bearing invariant: rows 0 and 1 never collide
// ---------------------------------------------------------------------------

describe('uint64_value cross-row independence (no collision)', () => {
  it('tag row 0 and result row 1 hold distinct values on one buffer', () => {
    const { schema, buffer } = setup();
    const rowZero = 111n;
    const rowOne = 222n;

    createTagWriter(schema, buffer).uint64_value(rowZero);
    createResultWriter(schema, buffer, { ok: true }, false).uint64_value(rowOne);

    const values = uint64Column(buffer);
    const nulls = uint64Nulls(buffer);
    // Neither write overwrote the other.
    expect(values[0]).toBe(rowZero);
    expect(values[1]).toBe(rowOne);
    expect(isNonNull(nulls, 0)).toBe(true);
    expect(isNonNull(nulls, 1)).toBe(true);
  });

  it('is order-independent: result-first then tag yields the same layout', () => {
    const { schema, buffer } = setup();
    const rowZero = 333n;
    const rowOne = 444n;

    // Reverse the write order relative to the previous case.
    createResultWriter(schema, buffer, { ok: true }, false).uint64_value(rowOne);
    createTagWriter(schema, buffer).uint64_value(rowZero);

    const values = uint64Column(buffer);
    expect(values[0]).toBe(rowZero);
    expect(values[1]).toBe(rowOne);
  });

  it('survives the span lifecycle: writeSpanStart/writeSpanEnd never touch uint64_value', () => {
    const { schema, buffer } = setup();
    const rowZero = 900n;
    const rowOne = 901n;

    const tag = createTagWriter(schema, buffer);
    const result = createResultWriter(schema, buffer, { ok: true }, false);

    // Reproduce the production sequence: lifecycle start, user tags, user result, lifecycle end.
    buffer._traceRoot.writeSpanStart(buffer, 'lifecycle-span');
    tag.uint64_value(rowZero);
    result.uint64_value(rowOne);
    buffer._traceRoot.writeSpanEnd(buffer, ENTRY_TYPE_SPAN_OK);

    // The lifecycle actually ran and wrote its OWN columns (entry_type)...
    expect(buffer.entry_type[0]).toBe(ENTRY_TYPE_SPAN_START);
    expect(buffer.entry_type[1]).toBe(ENTRY_TYPE_SPAN_OK);
    // ...without clobbering the user uint64 values in either row.
    const values = uint64Column(buffer);
    const nulls = uint64Nulls(buffer);
    expect(values[0]).toBe(rowZero);
    expect(values[1]).toBe(rowOne);
    expect(isNonNull(nulls, 0)).toBe(true);
    expect(isNonNull(nulls, 1)).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Lazy allocation
// ---------------------------------------------------------------------------

describe('uint64_value lazy allocation', () => {
  it('is unallocated until first write, then allocated', () => {
    const { schema, buffer } = setup();

    expect(buffer.getColumnIfAllocated('uint64_value')).toBeUndefined();
    expect(buffer.getNullsIfAllocated('uint64_value')).toBeUndefined();

    createTagWriter(schema, buffer).uint64_value(7n);

    expect(buffer.getColumnIfAllocated('uint64_value')).toBeDefined();
    expect(uint64Column(buffer)[0]).toBe(7n);
  });
});

// ---------------------------------------------------------------------------
// Chaining & call-order independence with a sibling column
// ---------------------------------------------------------------------------

describe('uint64_value chaining and sibling non-interference', () => {
  it('returns the writer for chaining', () => {
    const { schema, buffer } = setup();
    const tag = createTagWriter(schema, buffer);
    expect(tag.uint64_value(1n)).toBe(tag);
  });

  it('batchId(...).uint64_value(...) lands both values', () => {
    const { schema, buffer } = setup();

    createTagWriter(schema, buffer).batchId('batch-A').uint64_value(10n);

    expect(uint64Column(buffer)[0]).toBe(10n);
    const batch = buffer.getColumnIfAllocated('batchId');
    if (!Array.isArray(batch)) {
      throw new Error('batchId column was not allocated as an array');
    }
    expect(batch[0]).toBe('batch-A');
  });

  it('uint64_value(...).batchId(...) lands both values (reverse order)', () => {
    const { schema, buffer } = setup();

    createTagWriter(schema, buffer).uint64_value(20n).batchId('batch-B');

    expect(uint64Column(buffer)[0]).toBe(20n);
    const batch = buffer.getColumnIfAllocated('batchId');
    if (!Array.isArray(batch)) {
      throw new Error('batchId column was not allocated as an array');
    }
    expect(batch[0]).toBe('batch-B');
  });
});

// ---------------------------------------------------------------------------
// null clears the null bit
// ---------------------------------------------------------------------------

describe('uint64_value null handling', () => {
  it('writing null clears the null bit (value marked absent), not sets it', () => {
    const { schema, buffer } = setup();
    const tag = createTagWriter(schema, buffer);

    // First set a real value so the null bit is on...
    tag.uint64_value(99n);
    expect(isNonNull(uint64Nulls(buffer), 0)).toBe(true);

    // ...then clear it via the runtime null path.
    writeUint64Runtime(tag, null);
    expect(isNonNull(uint64Nulls(buffer), 0)).toBe(false);
  });

  it('null at row 1 clears row 1 only, leaving a non-null row 0 intact', () => {
    const { schema, buffer } = setup();
    const tag = createTagWriter(schema, buffer);
    const result = createResultWriter(schema, buffer, { ok: true }, false);

    tag.uint64_value(5n);
    result.uint64_value(6n);
    expect(isNonNull(uint64Nulls(buffer), 0)).toBe(true);
    expect(isNonNull(uint64Nulls(buffer), 1)).toBe(true);

    writeUint64Runtime(result, null);

    const nulls = uint64Nulls(buffer);
    expect(isNonNull(nulls, 1)).toBe(false); // row 1 cleared
    expect(isNonNull(nulls, 0)).toBe(true); // row 0 untouched
    expect(uint64Column(buffer)[0]).toBe(5n);
  });
});

// ---------------------------------------------------------------------------
// Boundary values
// ---------------------------------------------------------------------------

describe('uint64_value boundary values', () => {
  const cases: ReadonlyArray<{ name: string; value: bigint }> = [
    { name: 'max uint64 (2^64 - 1)', value: 2n ** 64n - 1n },
    { name: 'one below max', value: 2n ** 64n - 2n },
    { name: '2^63 (above JS safe-integer precision)', value: 2n ** 63n },
    { name: '2^53 + 1 (first value not exactly representable as a JS number)', value: 2n ** 53n + 1n },
    { name: 'one', value: 1n },
  ];

  for (const { name, value } of cases) {
    it(`round-trips ${name} exactly`, () => {
      const { schema, buffer } = setup();
      createTagWriter(schema, buffer).uint64_value(value);

      const values = uint64Column(buffer);
      expect(values[0]).toBe(value);
      expect(isNonNull(uint64Nulls(buffer), 0)).toBe(true);
    });
  }

  it('stores 0n as a non-null zero, distinct from absent', () => {
    const { schema, buffer } = setup();
    createTagWriter(schema, buffer).uint64_value(0n);

    const values = uint64Column(buffer);
    const nulls = uint64Nulls(buffer);
    expect(values[0]).toBe(0n);
    // The crux: 0n is a written value, so the null bit MUST be set (non-null),
    // distinguishing an explicit zero from an unwritten/absent row (row 1 here).
    expect(isNonNull(nulls, 0)).toBe(true);
    expect(isNonNull(nulls, 1)).toBe(false);
  });
});
