import { describe, it, expect } from 'bun:test';
import { generateAttributeColumns } from '../generateAttributeColumns.js';
import { createSchemaBuffer } from '../createSchemaBuffer.js';
import { getCacheAlignedCapacity } from '../capacity.js';
import { defineTagAttributes } from '../../schema/defineTagAttributes.js';
import { S } from '../../schema/builder.js';

describe('Buffer Integration', () => {
  it('generates attribute columns with proper names and types using defined schema', () => {
    const base = defineTagAttributes({
      userId: S.string(),
      isActive: S.boolean(),
      score: S.number(),
      category: S.union(['A', 'B']),
    });

    const aligned = 8;
    const cols = generateAttributeColumns(base as any, aligned) as Record<string, any>;

    // Keys
    expect(cols).toHaveProperty('attr_userId');
    expect(cols).toHaveProperty('attr_isActive');
    expect(cols).toHaveProperty('attr_score');
    expect(cols).toHaveProperty('attr_category');

    // Lengths
    Object.values(cols).forEach((arr: any) => {
      expect(arr.length).toBe(aligned);
    });

    // Types
    expect(cols.attr_userId).toBeInstanceOf(Uint32Array);
    expect(cols.attr_isActive).toBeInstanceOf(Uint8Array);
    expect(cols.attr_score).toBeInstanceOf(Float64Array);
    expect(cols.attr_category).toBeInstanceOf(Uint32Array);
  });

  it('creates a schema buffer with core and attribute columns using defined schema', () => {
    const base = defineTagAttributes({
      userId: S.string(),
      score: S.number(),
    });

    const capacity = 16;
    const buf = createSchemaBuffer(base as any, capacity) as any;

    // Core columns exist
    expect(buf).toHaveProperty('timestamps');
    expect(buf).toHaveProperty('operations');
    expect(buf).toHaveProperty('nullBitmap');
    // Attribute columns exist
    expect(buf).toHaveProperty('attr_userId');
    expect(buf).toHaveProperty('attr_score');

    // Lengths should be equal to the aligned capacity derived from capacity
    const aligned = getCacheAlignedCapacity(capacity, 1);
    expect(buf.timestamps.length).toBe(aligned);
    expect(buf.operations.length).toBe(aligned);
    expect(buf.nullBitmap.length).toBe(aligned);
    expect(buf.attr_userId.length).toBe(aligned);
    expect(buf.attr_score.length).toBe(aligned);
  });
});
