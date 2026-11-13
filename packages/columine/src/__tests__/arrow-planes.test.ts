import { describe, expect, it } from 'bun:test';
import { ARROW_PLANES } from '../event-processor-abi.generated.js';

//#region smoo/columine!n/vocab-host-table
describe('arrow plane table', () => {
  // Tags are baked into the shipped wasm and every persisted fixture, so a gap
  // means a plane was deleted rather than appended, and a duplicate means two
  // planes decode as one.
  it('keeps tags a gapless block from zero', () => {
    const tags: number[] = ARROW_PLANES.map((plane) => plane.tag);
    expect([...tags].sort((a, b) => a - b)).toEqual(tags.map((_, index) => index));
  });

  it('gives every plane a distinct name on both sides', () => {
    expect(new Set(ARROW_PLANES.map((plane) => plane.kind)).size).toBe(ARROW_PLANES.length);
    expect(new Set(ARROW_PLANES.map((plane) => plane.variant)).size).toBe(ARROW_PLANES.length);
  });
});
//#endregion smoo/columine!n/vocab-host-table

//#region smoo/columine!n/vocab-host-table
/**
 * The carrier a width-and-signedness name REQUIRES, or `null` when the name
 * does not encode one (`binary`, `bool`, `decimal128`, `intervalDayTime`, …).
 *
 * This is the defect the table exists to prevent: `'u32'` carried by
 * `Int32Array` compiles and reads back sign-flipped, which is exactly why it
 * survived review and shipped.
 */
function requiredCarrier(kind: string): string | null {
  const numeric = /^(?<sign>[iuf])(?<width>8|16|32|64)$/.exec(kind);
  const sign = numeric?.groups?.sign;
  const width = numeric?.groups?.width;
  if (!sign || !width) {
    return null;
  }
  if (width === '64') {
    // No Int64Array exists; the eight-byte integer planes ride the BigInt arrays.
    return sign === 'f' ? 'Float64Array' : sign === 'i' ? 'BigInt64Array' : 'BigUint64Array';
  }
  if (sign === 'f') {
    // Float16Array is not in the language, so the half-width float plane is
    // carried as raw 16-bit words and converted by the reader.
    return width === '16' ? 'Uint16Array' : `Float${width}Array`;
  }
  return sign === 'i' ? `Int${width}Array` : `Uint${width}Array`;
}

describe('plane names agree with their carriers', () => {
  for (const plane of ARROW_PLANES) {
    const required = requiredCarrier(plane.kind);
    if (!required) {
      continue;
    }
    it(`carries '${plane.kind}' as ${required}`, () => {
      const carrier: string | null = plane.data;
      expect(carrier).toBe(required);
    });
  }
});
//#endregion smoo/columine!n/vocab-host-table

//#region smoo/columine!n/vocab-host-table
/**
 * The TypeScript name a Rust variant REQUIRES, or `null` when the variant name
 * carries no width and signedness (`Binary`, `Bool`, `Decimal128`, …).
 */
function requiredKind(variant: string): string | null {
  const parsed = /^(?<sign>U?Int|Float)(?<width>8|16|32|64)$/.exec(variant);
  const sign = parsed?.groups?.sign;
  const width = parsed?.groups?.width;
  if (!sign || !width) {
    return null;
  }
  return `${sign === 'UInt' ? 'u' : sign === 'Int' ? 'i' : 'f'}${width}`;
}

/**
 * Tag 1 is `ArrowType::Int32`, a signed plane, and the name `'u32'` for it
 * carries a `Uint32Array` that is internally consistent and reads back
 * sign-flipped. Binding the variant, the tag, the name and the carrier in one
 * generated row is half the guard; this is the half that rejects the
 * combination.
 */
describe('plane names agree across the ABI', () => {
  for (const plane of ARROW_PLANES) {
    const required = requiredKind(plane.variant);
    if (!required) {
      continue;
    }
    it(`names ${plane.variant} '${required}'`, () => {
      const kind: string = plane.kind;
      expect(kind).toBe(required);
    });
  }
});
//#endregion smoo/columine!n/vocab-host-table

//#region smoo/columine!n/vocab-host-table
describe('temporal planes', () => {
  // A Timestamp and a Duration are one little-endian i64 per row counting the
  // field's unit, so the host reads them through the eight-byte signed plane's
  // typed array.
  it('are carried by the typed array of the eight-byte signed plane', () => {
    const carriers = new Map(ARROW_PLANES.map((plane): [string, string | null] => [plane.kind, plane.data]));
    expect(carriers.get('i64')).toBe('BigInt64Array');
    expect(carriers.get('timestamp')).toBe(carriers.get('i64'));
    expect(carriers.get('duration')).toBe(carriers.get('i64'));
  });
});
//#endregion smoo/columine!n/vocab-host-table
