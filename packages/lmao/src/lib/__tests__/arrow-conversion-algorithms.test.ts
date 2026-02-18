import { describe, expect, it } from 'bun:test';
import { buildTextDictionary } from '../arrow/dictionaries.js';
import { concatenateNullBitmaps } from '../arrow/utils.js';
import type { AnySpanBuffer } from '../types.js';

function mockBuffer(writeIndex: number, column: unknown[] | undefined, nulls: Uint8Array | undefined): AnySpanBuffer {
  return {
    _writeIndex: writeIndex,
    getColumnIfAllocated(columnName: string): unknown {
      return columnName === 'field' ? column : undefined;
    },
    getNullsIfAllocated(columnName: string): Uint8Array | undefined {
      return columnName === 'field' ? nulls : undefined;
    },
  } as unknown as AnySpanBuffer;
}

describe('arrow conversion algorithms', () => {
  it('concatenateNullBitmaps handles non-byte-aligned buffer offsets', () => {
    const buffers: AnySpanBuffer[] = [
      mockBuffer(3, ['a', undefined, 'b'], new Uint8Array([0b00000101])),
      mockBuffer(5, ['c', 'd', undefined, 'e', undefined], new Uint8Array([0b00001011])),
    ];

    const { nullBitmap, nullCount } = concatenateNullBitmaps(buffers, 'field');

    expect(nullBitmap).toBeDefined();
    expect(nullBitmap?.[0]).toBe(0b01011101);
    expect(nullCount).toBe(3);
  });

  it('buildTextDictionary marks all rows null when text column is never allocated', () => {
    const buffers: AnySpanBuffer[] = [mockBuffer(2, undefined, undefined), mockBuffer(1, undefined, undefined)];

    const result = buildTextDictionary(buffers, 'field');

    expect(result).not.toBeNull();
    expect(result?.dictionary).toEqual([]);
    expect(result?.indices.length).toBe(3);
    expect(result?.nullCount).toBe(3);
    expect(result?.nullBitmap).toBeDefined();
    expect(result?.nullBitmap?.[0]).toBe(0);
  });
});
