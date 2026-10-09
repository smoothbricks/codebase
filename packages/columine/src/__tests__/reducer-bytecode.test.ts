import { describe, expect, it } from 'bun:test';

import { appendIndex, readIndex } from '../operand.js';
import { encodeProgramHeader, parseReducerProgram } from '../reducer-bytecode.js';
import {
  AggType,
  HEADER_SIZE,
  Opcode,
  PROGRAM_HASH_PREFIX,
  PROGRAM_MAGIC,
  SlotType,
  SlotTypeFlag,
  StructFieldType,
  TtlStartOf,
} from '../types.js';

function buildProgram(initCode: number[], numSlots: number, magic = PROGRAM_MAGIC): Uint8Array {
  const reduceCode = [0x00];
  const totalLen = PROGRAM_HASH_PREFIX + HEADER_SIZE + initCode.length + reduceCode.length;
  const out = new Uint8Array(totalLen);
  const base = PROGRAM_HASH_PREFIX;
  out.set(
    encodeProgramHeader({
      magic,
      numSlots,
      numInputs: 0,
      initCodeLength: initCode.length,
      reduceCodeLength: reduceCode.length,
    }),
    base,
  );
  out.set(initCode, base + HEADER_SIZE);
  out.set(reduceCode, base + HEADER_SIZE + initCode.length);
  return out;
}

function ttlBytes(ttlSeconds: number, graceSeconds: number, tsColumn: number, startOf: TtlStartOf): number[] {
  const bytes = new Uint8Array(8);
  const view = new DataView(bytes.buffer);
  view.setFloat32(0, ttlSeconds, true);
  view.setFloat32(4, graceSeconds, true);
  const out = [...bytes];
  appendIndex(out, tsColumn);
  out.push(startOf);
  return out;
}

describe('index operands', () => {
  it('round-trips every length boundary at its encoded length', () => {
    for (const [value, length] of [
      [0, 1],
      [0x7f, 1],
      [0x80, 2],
      [0x3fff, 2],
      [0x4000, 3],
      [0x0fff_ffff, 4],
      [0x1000_0000, 5],
      [0xffff_ffff, 5],
    ]) {
      const out: number[] = [];
      appendIndex(out, value);
      expect(out).toHaveLength(length);
      expect(readIndex(Uint8Array.from(out), 0)).toEqual({ value, next: length });
    }
  });

  it('refuses truncated, overlong and out-of-range encodings', () => {
    expect(() => readIndex(Uint8Array.of(0x80), 0)).toThrow('truncated');
    expect(() => readIndex(Uint8Array.of(0x81, 0x00), 0)).toThrow('overlong');
    expect(() => readIndex(Uint8Array.of(0xff, 0xff, 0xff, 0xff, 0x1f), 0)).toThrow('exceeds u32');
    expect(() => appendIndex([], 2 ** 32)).toThrow(RangeError);
  });
});

describe('parseReducerProgram', () => {
  it('rejects foreign magics by default and admits them when explicitly accepted', () => {
    const foreignMagic = 0xdead_beef;
    const bytecode = buildProgram([Opcode.SLOT_DEF, 0, SlotType.HASHSET, 4, 0, Opcode.HALT], 1, foreignMagic);

    expect(() => parseReducerProgram(bytecode)).toThrow('Invalid program: bad magic');
    expect(parseReducerProgram(bytecode, 1024, [PROGRAM_MAGIC, foreignMagic]).slotDefs).toHaveLength(1);
  });
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

  it('refuses TTL on both struct-map declarations before decoding their metadata', () => {
    const structMap = [
      Opcode.SLOT_STRUCT_MAP,
      0,
      SlotType.STRUCT_MAP | SlotTypeFlag.HAS_TTL,
      0x40,
      0x00,
      0x02,
      StructFieldType.UINT32,
      StructFieldType.INT64,
      ...ttlBytes(300, 0, 2, TtlStartOf.HOUR),
      Opcode.HALT,
    ];
    expect(() => parseReducerProgram(buildProgram(structMap, 1))).toThrow('struct-map TTL is not supported');

    const structMap2 = [
      Opcode.SLOT_STRUCT_MAP2,
      0,
      SlotType.STRUCT_MAP2 | SlotTypeFlag.HAS_TTL,
      0x40,
      0x00,
      0x01,
      StructFieldType.UINT32,
      ...ttlBytes(300, 0, 2, TtlStartOf.HOUR),
      Opcode.HALT,
    ];
    expect(() => parseReducerProgram(buildProgram(structMap2, 1))).toThrow('struct-map2 TTL is not supported');

    // Refusal precedes metadata decoding: a TTL flag with no TTL bytes is still the kind refusal.
    const noMetadata = [
      Opcode.SLOT_STRUCT_MAP,
      0,
      SlotType.STRUCT_MAP | SlotTypeFlag.HAS_TTL,
      0x40,
      0x00,
      0x00,
      Opcode.HALT,
    ];
    expect(() => parseReducerProgram(buildProgram(noMetadata, 1))).toThrow('struct-map TTL is not supported');
  });

  it('admits VO struct-map fields and refuses the first byte past the closed field-kind set', () => {
    const init = [Opcode.SLOT_STRUCT_MAP, 0, SlotType.STRUCT_MAP, 4, 0, 2, StructFieldType.UINT32, StructFieldType.VO];
    const bytecode = buildProgram([...init, Opcode.HALT], 1);

    expect(StructFieldType.VO).toBe(10);
    expect(parseReducerProgram(bytecode).slotDefs[0]).toEqual({
      type: SlotType.STRUCT_MAP,
      capacity: 4,
      fieldTypes: [StructFieldType.UINT32, StructFieldType.VO],
    });
    bytecode[PROGRAM_HASH_PREFIX + HEADER_SIZE + init.length - 1] = StructFieldType.VO + 1;
    expect(() => parseReducerProgram(bytecode)).toThrow('Invalid program: unknown struct field type 11');
  });

  it('refuses VO descriptors outside STRUCT_MAP', () => {
    const structMap2 = [Opcode.SLOT_STRUCT_MAP2, 0, SlotType.STRUCT_MAP2, 4, 0, 1, StructFieldType.VO, Opcode.HALT];
    expect(() => parseReducerProgram(buildProgram(structMap2, 1))).toThrow(
      'Invalid program: struct-map2 VO field descriptors are not supported',
    );

    const orderedList = (...descriptor: number[]) =>
      buildProgram([Opcode.SLOT_ORDERED_LIST, 0, SlotType.ORDERED_LIST, 4, 0, ...descriptor, Opcode.HALT], 1);

    expect(() => parseReducerProgram(orderedList(StructFieldType.VO))).toThrow(
      'Invalid program: ordered-list VO field descriptors are not supported',
    );
    expect(() => parseReducerProgram(orderedList(0xff, 2, StructFieldType.UINT32, StructFieldType.VO))).toThrow(
      'Invalid program: ordered-list VO field descriptors are not supported',
    );
    expect(parseReducerProgram(orderedList(0xff, 1, StructFieldType.ARRAY_BOOL)).slotDefs[0]).toEqual({
      type: SlotType.ORDERED_LIST,
      capacity: 4,
      fieldTypes: [StructFieldType.ARRAY_BOOL],
    });
  });

  it('requires each specialized slot opcode to name its actual native kind', () => {
    const declarations: readonly (readonly [
      Opcode,
      SlotType.STRUCT_MAP | SlotType.STRUCT_MAP2 | SlotType.ORDERED_LIST,
    ])[] = [
      [Opcode.SLOT_STRUCT_MAP, SlotType.STRUCT_MAP],
      [Opcode.SLOT_STRUCT_MAP2, SlotType.STRUCT_MAP2],
      [Opcode.SLOT_ORDERED_LIST, SlotType.ORDERED_LIST],
    ];
    for (const [opcode, kind] of declarations) {
      const descriptor = opcode === Opcode.SLOT_ORDERED_LIST ? [StructFieldType.UINT32] : [1, StructFieldType.UINT32];
      const valid = [opcode, 0, kind, 4, 0, ...descriptor, Opcode.HALT];
      expect(parseReducerProgram(buildProgram(valid, 1)).slotDefs[0].type).toBe(kind);
      for (const other of [SlotType.HASHSET, SlotType.STRUCT_MAP, SlotType.ORDERED_LIST, SlotType.STRUCT_MAP2]) {
        if (other === kind) continue;
        const invalid = [...valid];
        invalid[2] = other;
        expect(() => parseReducerProgram(buildProgram(invalid, 1))).toThrow('declared slot kind disagree');
      }
    }
    expect(() =>
      parseReducerProgram(
        buildProgram(
          [
            Opcode.SLOT_ORDERED_LIST,
            0,
            SlotType.ORDERED_LIST | SlotTypeFlag.HAS_TTL,
            4,
            0,
            StructFieldType.UINT32,
            Opcode.HALT,
          ],
          1,
        ),
      ),
    ).toThrow('ordered-list TTL is not supported');
  });

  it('reads slot and timestamp-column indexes past one byte', () => {
    const numSlots = 300;
    const initCode: number[] = [];
    for (let slot = 0; slot < numSlots - 1; slot++) {
      initCode.push(Opcode.SLOT_DEF);
      appendIndex(initCode, slot);
      initCode.push(SlotType.HASHSET, 4, 0);
    }
    initCode.push(Opcode.SLOT_DEF);
    appendIndex(initCode, numSlots - 1);
    initCode.push(SlotType.HASHSET | SlotTypeFlag.HAS_TTL, 4, 0, ...ttlBytes(60, 0, 280, TtlStartOf.NONE), 0x00);

    const program = parseReducerProgram(buildProgram(initCode, numSlots));
    expect(program.numSlots).toBe(numSlots);
    expect(program.slotDefs[200]).toEqual({ type: SlotType.HASHSET, capacity: 4, ttl: undefined });
    const last = program.slotDefs[numSlots - 1];
    expect('ttl' in last && last.ttl?.timestampFieldIndex).toBe(280);
  });

  it('admits TTL only on the spec-defined HashMap, HashSet and Bitmap slot kinds', () => {
    for (const kind of [SlotType.AGGREGATE, SlotType.SCALAR, SlotType.CONDITION_TREE]) {
      const invalid = buildProgram(
        [Opcode.SLOT_DEF, 0, kind | SlotTypeFlag.HAS_TTL, 4, 0, ...ttlBytes(10, 0, 0, TtlStartOf.NONE), Opcode.HALT],
        1,
      );
      expect(() => parseReducerProgram(invalid)).toThrow('TTL is unsupported');
    }
    for (const kind of [SlotType.HASHMAP, SlotType.HASHSET, SlotType.BITMAP]) {
      const valid = buildProgram(
        [Opcode.SLOT_DEF, 0, kind | SlotTypeFlag.HAS_TTL, 4, 0, ...ttlBytes(10, 0, 0, TtlStartOf.NONE), Opcode.HALT],
        1,
      );
      const slot = parseReducerProgram(valid).slotDefs[0];
      expect<SlotType>(slot.type).toBe(kind);
      expect('ttl' in slot && slot.ttl?.ttlSeconds).toBe(10);
    }
  });

  it('decodes BITMAP TTL metadata and refuses malformed TTL tails like HASHSET', () => {
    const typeFlags = SlotType.BITMAP | SlotTypeFlag.HAS_TTL;
    const initCode = [Opcode.SLOT_DEF, 0, typeFlags, 0x40, 0x00, ...ttlBytes(90, 5, 3, TtlStartOf.MINUTE), Opcode.HALT];
    expect(parseReducerProgram(buildProgram(initCode, 1)).slotDefs[0]).toEqual({
      type: SlotType.BITMAP,
      capacity: 64,
      ttl: {
        ttlSeconds: 90,
        graceSeconds: 5,
        timestampFieldIndex: 3,
        startOf: TtlStartOf.MINUTE,
        hasEvictTrigger: false,
      },
    });

    const truncated = [Opcode.SLOT_DEF, 0, typeFlags, 0x40, 0x00, 0x00, 0x00, 0x00, Opcode.HALT];
    expect(() => parseReducerProgram(buildProgram(truncated, 1))).toThrow('truncated TTL metadata');

    const badStartOf = [
      Opcode.SLOT_DEF,
      0,
      typeFlags,
      0x40,
      0x00,
      ...ttlBytes(10, 0, 0, TtlStartOf.NONE).slice(0, -1),
      9,
    ];
    expect(() => parseReducerProgram(buildProgram(badStartOf, 1))).toThrow('unknown TTL startOf 9');
  });

  it('refuses a program of another format version', () => {
    const bytecode = buildProgram([0x10, 0x00, SlotType.HASHSET, 0x10, 0x00, 0x00], 1);
    bytecode[PROGRAM_HASH_PREFIX + 4] = 1;
    expect(() => parseReducerProgram(bytecode)).toThrow('Invalid program: unsupported version');
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

  it('decodes explicit U32, F64, and I64 scalar metadata', () => {
    const initCode = [
      Opcode.SLOT_DEF,
      0,
      SlotType.SCALAR,
      AggType.SCALAR_U32,
      0,
      Opcode.SLOT_DEF,
      1,
      SlotType.SCALAR,
      AggType.SCALAR_F64,
      0,
      Opcode.SLOT_DEF,
      2,
      SlotType.SCALAR,
      AggType.SCALAR_I64,
      0,
      Opcode.HALT,
    ];

    expect(parseReducerProgram(buildProgram(initCode, 3)).slotDefs).toEqual([
      { type: SlotType.SCALAR, aggType: AggType.SCALAR_U32 },
      { type: SlotType.SCALAR, aggType: AggType.SCALAR_F64 },
      { type: SlotType.SCALAR, aggType: AggType.SCALAR_I64 },
    ]);
  });

  it('rejects unknown scalar metadata', () => {
    const initCode = [Opcode.SLOT_DEF, 0, SlotType.SCALAR, 7, 0, Opcode.HALT];
    expect(() => parseReducerProgram(buildProgram(initCode, 1))).toThrow('unknown scalar type 7');
  });

  it('rejects missing, duplicate, and out-of-range slot definitions', () => {
    expect(() =>
      parseReducerProgram(buildProgram([Opcode.SLOT_DEF, 0, SlotType.HASHSET, 4, 0, Opcode.HALT], 2)),
    ).toThrow('missing slot definition 1');

    expect(() =>
      parseReducerProgram(
        buildProgram(
          [Opcode.SLOT_DEF, 0, SlotType.HASHSET, 4, 0, Opcode.SLOT_DEF, 0, SlotType.HASHSET, 4, 0, Opcode.HALT],
          1,
        ),
      ),
    ).toThrow('duplicate slot definition 0');

    expect(() =>
      parseReducerProgram(buildProgram([Opcode.SLOT_DEF, 1, SlotType.HASHSET, 4, 0, Opcode.HALT], 1)),
    ).toThrow('slot index 1 out of range');
  });

  it('distinguishes HALT from unknown and truncated init instructions', () => {
    expect(() => parseReducerProgram(buildProgram([Opcode.SLOT_DEF, 0, SlotType.HASHSET, 4, 0, 0xfe], 1))).toThrow(
      'unknown init opcode 254',
    );
    expect(() => parseReducerProgram(buildProgram([Opcode.SLOT_DEF, 0, SlotType.HASHSET], 1))).toThrow(
      'truncated SLOT_DEF operands',
    );
    expect(() => parseReducerProgram(buildProgram([Opcode.SLOT_DEF, 0, SlotType.HASHSET, 4, 0], 1))).toThrow(
      'init section missing HALT',
    );
  });
});
