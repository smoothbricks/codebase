import { describe, expect, it } from 'bun:test';

import { parseReducerProgram } from '../reducer-bytecode.js';
import { MAGIC, PROGRAM_HASH_PREFIX, SlotType, SlotTypeFlag, StructFieldType, TtlStartOf } from '../types.js';

function buildProgram(initCode: number[], numSlots: number): Uint8Array {
  const headerSize = 14;
  const reduceCode = [0x00];
  const totalLen = PROGRAM_HASH_PREFIX + headerSize + initCode.length + reduceCode.length;
  const out = new Uint8Array(totalLen);
  const base = PROGRAM_HASH_PREFIX;

  out[base + 0] = MAGIC & 0xff;
  out[base + 1] = (MAGIC >> 8) & 0xff;
  out[base + 2] = (MAGIC >> 16) & 0xff;
  out[base + 3] = (MAGIC >> 24) & 0xff;
  out[base + 4] = 1;
  out[base + 5] = 0;
  out[base + 6] = numSlots;
  out[base + 7] = 0;
  out[base + 10] = initCode.length & 0xff;
  out[base + 11] = (initCode.length >> 8) & 0xff;
  out[base + 12] = reduceCode.length & 0xff;
  out[base + 13] = 0;

  out.set(initCode, base + headerSize);
  out.set(reduceCode, base + headerSize + initCode.length);
  return out;
}

function ttlBytes(ttlSeconds: number, graceSeconds: number, tsField: number, startOf: TtlStartOf): number[] {
  const bytes = new Uint8Array(10);
  const view = new DataView(bytes.buffer);
  view.setFloat32(0, ttlSeconds, true);
  view.setFloat32(4, graceSeconds, true);
  view.setUint8(8, tsField);
  view.setUint8(9, startOf);
  return [...bytes];
}

describe('parseReducerProgram', () => {
  it('decodes SLOT_DEF type nibble while preserving TTL metadata', () => {
    const typeFlags = SlotType.HASHMAP | SlotTypeFlag.HAS_TTL | SlotTypeFlag.HAS_EVICT_TRIGGER;
    const initCode = [0x10, 0x00, typeFlags, 0x20, 0x00, ...ttlBytes(90, 5, 3, TtlStartOf.MINUTE), 0x00];
    const program = parseReducerProgram(buildProgram(initCode, 1));

    expect(program.slotDefs[0].type).toBe(SlotType.HASHMAP);
    expect('storesTimestamps' in program.slotDefs[0] && program.slotDefs[0].storesTimestamps).toBe(true);
    expect('ttl' in program.slotDefs[0] && program.slotDefs[0].ttl).toEqual({
      ttlSeconds: 90,
      graceSeconds: 5,
      timestampFieldIndex: 3,
      startOf: TtlStartOf.MINUTE,
      hasEvictTrigger: true,
    });
  });

  it('decodes struct-map TTL payload and field types', () => {
    const typeFlags = SlotType.STRUCT_MAP | SlotTypeFlag.HAS_TTL;
    const initCode = [
      0x18,
      0x00,
      typeFlags,
      0x40,
      0x00,
      0x02,
      StructFieldType.UINT32,
      StructFieldType.INT64,
      ...ttlBytes(300, 0, 2, TtlStartOf.HOUR),
      0x00,
    ];

    const program = parseReducerProgram(buildProgram(initCode, 1));
    const slot = program.slotDefs[0];
    expect(slot.type).toBe(SlotType.STRUCT_MAP);
    if (slot.type !== SlotType.STRUCT_MAP) {
      throw new Error('invariant: expected struct-map slot');
    }
    expect(slot.fieldTypes).toEqual([StructFieldType.UINT32, StructFieldType.INT64]);
    expect(slot.ttl).toEqual({
      ttlSeconds: 300,
      graceSeconds: 0,
      timestampFieldIndex: 2,
      startOf: TtlStartOf.HOUR,
      hasEvictTrigger: false,
    });
  });

  it('keeps non-TTL slot decoding unchanged', () => {
    const initCode = [0x10, 0x00, SlotType.HASHSET, 0x10, 0x00, 0x00];
    const program = parseReducerProgram(buildProgram(initCode, 1));
    expect(program.slotDefs[0]).toEqual({ type: SlotType.HASHSET, capacity: 16 });
  });

  it('decodes HASHMAP no-timestamp metadata when no-timestamp bit is set', () => {
    const typeFlags = SlotType.HASHMAP | SlotTypeFlag.NO_HASHMAP_TIMESTAMPS;
    const initCode = [0x10, 0x00, typeFlags, 0x20, 0x00, 0x00];
    const program = parseReducerProgram(buildProgram(initCode, 1));

    expect(program.slotDefs[0]).toEqual({
      type: SlotType.HASHMAP,
      capacity: 32,
      storesTimestamps: false,
      ttl: undefined,
    });
  });

  it('decodes HASHMAP timestamp metadata when no-timestamp bit is unset', () => {
    const initCode = [0x10, 0x00, SlotType.HASHMAP, 0x20, 0x00, 0x00];
    const program = parseReducerProgram(buildProgram(initCode, 1));
    const slot = program.slotDefs[0];
    expect(slot.type).toBe(SlotType.HASHMAP);
    if (slot.type !== SlotType.HASHMAP) {
      throw new Error('invariant: expected hashmap slot');
    }
    expect(slot.storesTimestamps).toBe(true);
  });
});
