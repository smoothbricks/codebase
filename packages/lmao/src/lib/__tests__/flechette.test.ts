import { describe, expect, it } from 'bun:test';
import { uint8, uint16 } from '@uwdata/flechette';

import {
  getArrowIndexArrayConstructorOr,
  getArrowIndexTypeOr,
  hasArrowIndexMetadata,
  hasArrowIndexType,
  hasTypeId,
} from '../arrow/flechette.js';

describe('flechette helpers', () => {
  it('reads enum index metadata from schema objects', () => {
    const schema = {
      __arrow_index_type: uint16(),
      __index_array_ctor: Uint16Array,
    };

    expect(hasArrowIndexType(schema)).toBe(true);
    expect(hasArrowIndexMetadata(schema)).toBe(true);
    expect(getArrowIndexTypeOr(schema, uint8()).typeId).toBe(uint16().typeId);
    expect(getArrowIndexArrayConstructorOr(schema)).toBe(Uint16Array);
  });

  it('falls back when metadata is missing or incomplete', () => {
    const missingCtor = {
      __arrow_index_type: uint16(),
      __index_array_ctor: Array,
    };

    expect(hasArrowIndexType({})).toBe(false);
    expect(hasArrowIndexMetadata(missingCtor)).toBe(false);
    expect(getArrowIndexTypeOr({}, uint8()).typeId).toBe(uint8().typeId);
    expect(getArrowIndexArrayConstructorOr(missingCtor)).toBe(Uint8Array);
  });

  it('rejects array inputs for object metadata guards', () => {
    expect(hasTypeId([{ typeId: 1 }])).toBe(false);
    expect(hasArrowIndexType([{ __arrow_index_type: uint16() }])).toBe(false);
    expect(hasArrowIndexMetadata([{ __arrow_index_type: uint16(), __index_array_ctor: Uint16Array }])).toBe(false);
  });
});
