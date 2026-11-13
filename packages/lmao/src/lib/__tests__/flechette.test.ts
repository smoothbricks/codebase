import { describe, expect, it } from 'bun:test';
import { uint8, uint16, uint32 } from '@uwdata/flechette';

import { getArrowIndexTypeOr, hasTypeId } from '../arrow/flechette.js';
import { S } from '../schema/builder.js';

function enumOf(variantCount: number) {
  return S.enum(Array.from({ length: variantCount }, (_, ordinal) => `variant${ordinal}`));
}

describe('flechette helpers', () => {
  //#region smoo/lmao!n/schema-fields
  it('reads the enum dictionary index type its variant count declares', () => {
    // The fallback is never the declared width, so only the field can have produced the result.
    expect(getArrowIndexTypeOr(enumOf(2), uint32()).bitWidth).toBe(8);
    expect(getArrowIndexTypeOr(enumOf(256), uint32()).bitWidth).toBe(8);
    expect(getArrowIndexTypeOr(enumOf(257), uint32()).bitWidth).toBe(16);
    expect(getArrowIndexTypeOr(enumOf(65536), uint32()).bitWidth).toBe(16);
    expect(getArrowIndexTypeOr(enumOf(65537), uint8()).bitWidth).toBe(32);
    expect(getArrowIndexTypeOr(enumOf(257), uint32()).signed).toBe(false);
    expect(getArrowIndexTypeOr(S.optional(enumOf(257)), uint32()).bitWidth).toBe(16);
  });

  it('falls back when the value is not an enum field', () => {
    expect(getArrowIndexTypeOr({}, uint8()).bitWidth).toBe(8);
    expect(getArrowIndexTypeOr(undefined, uint16()).bitWidth).toBe(16);
    expect(getArrowIndexTypeOr(S.number(), uint8()).bitWidth).toBe(8);
    expect(getArrowIndexTypeOr(S.category(), uint16()).bitWidth).toBe(16);
  });

  it('rejects array inputs for object guards', () => {
    expect(hasTypeId([{ typeId: 1 }])).toBe(false);
    expect(hasTypeId({ typeId: 1 })).toBe(true);
    expect(getArrowIndexTypeOr([enumOf(257)], uint8()).bitWidth).toBe(8);
  });
  //#endregion smoo/lmao!n/schema-fields
});
