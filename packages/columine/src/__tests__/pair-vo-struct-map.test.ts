import { beforeEach, describe, expect, it } from 'bun:test';
import { readFileSync } from 'node:fs';
import { encodeProgramHeader, parseReducerProgram } from '../reducer-bytecode.js';
import {
  type ColumineBackend,
  type ColumnInput,
  ErrorCode,
  HEADER_SIZE,
  Opcode,
  PROGRAM_HASH_PREFIX,
  PROGRAM_MAGIC,
  type ReducerProgram,
  SlotType,
  type StateHandle,
  StructFieldType,
  type StructMapArenaField,
} from '../types.js';
import { VoProgramAdmissionUnavailableError } from '../vo-program-metadata.js';
import { createColumineWasmBackend } from '../wasm-backend.js';

// Columine 04 §9: a text lane's frame is a leading value-object field of the row, written by the upsert's array
// operands, so a two-key record holds value-object fields. Columine 04 §2: growth compacts the arena.

// The dev wasm-opt output, as in vo-struct-map.test.ts: never gated on presence.
const WASM_PATH = new URL('../../.cache/wasm-dev/columine.wasm', import.meta.url);

const FRAME = 0;
const COUNT = 1;
const PAYLOAD = 2;
const FIELDS = [StructFieldType.VO, StructFieldType.UINT32, StructFieldType.VO] as const;

// Columns: 0 key1, 1 key2, 2 count, 3/4 the frame's CSR offsets and bytes, 5/6 the payload's.
const UPSERT = [
  Opcode.BATCH_STRUCT_MAP2_UPSERT_LAST_ARRAYS,
  0,
  0,
  1,
  1,
  2,
  COUNT,
  2,
  3,
  4,
  FRAME,
  5,
  6,
  PAYLOAD,
] as const;
const PREFIX_ERASE = [Opcode.BATCH_STRUCT_MAP2_REMOVE_PREFIX, 0, 0] as const;

function pairReducer(reduce: readonly number[], numInputs: number): Uint8Array {
  const init = [Opcode.SLOT_STRUCT_MAP2, 0, SlotType.STRUCT_MAP2, 4, 0, FIELDS.length, ...FIELDS, Opcode.HALT];
  const code = [...reduce, Opcode.HALT];
  const bytecode = new Uint8Array(PROGRAM_HASH_PREFIX + HEADER_SIZE + init.length + code.length);
  bytecode.set(
    encodeProgramHeader({
      magic: PROGRAM_MAGIC,
      numSlots: 1,
      numInputs,
      initCodeLength: init.length,
      reduceCodeLength: code.length,
    }),
    PROGRAM_HASH_PREFIX,
  );
  bytecode.set(init, PROGRAM_HASH_PREFIX + HEADER_SIZE);
  bytecode.set(code, PROGRAM_HASH_PREFIX + HEADER_SIZE + init.length);
  return bytecode;
}

type Row = readonly [key1: number, key2: number, frame: readonly number[], payload: readonly number[]];

function csr(parts: readonly (readonly number[])[]): [Uint32Array, Uint8Array] {
  const offsets = new Uint32Array(parts.length + 1);
  for (const [index, part] of parts.entries()) offsets[index + 1] = offsets[index] + part.length;
  return [offsets, Uint8Array.from(parts.flat())];
}

function pairBatch(rows: readonly Row[]): ColumnInput[] {
  const [frameOffsets, frames] = csr(rows.map(([, , frame]) => frame));
  const [payloadOffsets, payloads] = csr(rows.map(([, , , payload]) => payload));
  return [
    { data: Uint32Array.from(rows, ([key1]) => key1) },
    { data: Uint32Array.from(rows, ([, key2]) => key2) },
    { data: Uint32Array.from(rows, ([key1, key2]) => key1 * 1000 + key2) },
    { data: frameOffsets },
    { data: frames },
    { data: payloadOffsets },
    { data: payloads },
  ];
}

const text = (value: string): number[] => [...new TextEncoder().encode(value)];
const frameOf = (key1: number, key2: number): number[] => text(`frame:${key1}/${key2}`);
const payloadOf = (key1: number, key2: number): number[] =>
  Array.from({ length: (key1 * 7 + key2 * 3) % 23 }, (_, index) => (index + key2) & 0xff);

function contains(image: Uint8Array, needle: readonly number[]): boolean {
  for (let at = image.indexOf(needle[0]); at !== -1; at = image.indexOf(needle[0], at + 1)) {
    if (needle.every((byte, index) => image[at + index] === byte)) return true;
  }
  return false;
}

describe('STRUCT_MAP2 value-object fields through the built WASM VM', () => {
  let backend: ColumineBackend;
  let upsert: ReducerProgram;
  let erase: ReducerProgram;

  beforeEach(async () => {
    backend = await createColumineWasmBackend(readFileSync(WASM_PATH));
    upsert = await backend.loadProgram(pairReducer(UPSERT, 7));
    erase = await backend.loadProgram(pairReducer(PREFIX_ERASE, 1));
  });

  const field = (state: StateHandle, key1: number, key2: number, fieldIdx: number): StructMapArenaField =>
    backend.structMap2ArenaField(state, upsert, 0, key1, key2, fieldIdx);
  const bytes = (values: readonly number[]): StructMapArenaField => ({ kind: 'bytes', bytes: Uint8Array.from(values) });

  function expectHolds(state: StateHandle, rows: readonly Row[]): void {
    const model = new Map<string, Row>();
    for (const row of rows) model.set(`${row[0]}/${row[1]}`, row);
    const entries = backend.structMap2Entries(state, upsert, 0);
    if (!entries.ok) throw new Error(`structMap2Entries refused ${entries.error}`);
    expect(entries.value.map(({ key1, key2 }) => `${key1}/${key2}`).sort()).toEqual([...model.keys()].sort());
    for (const [key1, key2, frame, payload] of model.values()) {
      expect(field(state, key1, key2, FRAME)).toEqual(bytes(frame));
      expect(field(state, key1, key2, PAYLOAD)).toEqual(bytes(payload));
    }
  }

  it('parses a two-key map that declares value-object fields', () => {
    expect(parseReducerProgram(pairReducer(UPSERT, 7)).slotDefs[0]).toEqual({
      type: SlotType.STRUCT_MAP2,
      capacity: 4,
      fieldTypes: [...FIELDS],
    });
  });

  it('keeps one frame per key2 under a shared key1, the last write of a pair winning', () => {
    const state = backend.createState(upsert);
    const rows: Row[] = [
      [7, 0, text('op7/elem0'), [1, 2, 3]],
      [7, 1, text('op7/elem1'), []],
      [8, 0, text('op8/elem0'), [0, 255]],
      [0, 7, text('op0/elem7'), [9]],
      [7, 0, text('op7/elem0/rewritten'), [4, 5]],
    ];
    expect(backend.executeBatch(state, upsert, pairBatch(rows), rows.length)).toBe(ErrorCode.OK);
    expectHolds(state, rows);
    expect(field(state, 1, 7, FRAME)).toEqual({ kind: 'absent' });
    expect(field(state, 7, 0, COUNT)).toEqual({ kind: 'error', error: ErrorCode.INVALID_PROGRAM });
  });

  it('keeps every pair and frame when the rows and the arena grow', () => {
    const state = backend.createState(upsert);
    const rows: Row[] = [];
    for (let key1 = 1; key1 <= 6; key1++) {
      for (let key2 = 0; key2 < 8; key2++) rows.push([key1, key2, frameOf(key1, key2), payloadOf(key1, key2)]);
    }
    for (let key = 1; key <= 6; key++) {
      rows.push([9, key, frameOf(9, key), payloadOf(9, key)]);
      rows.push([key, 9, frameOf(key, 9), payloadOf(key, 9)]);
    }
    const sizeBefore = backend.serialize(state, upsert).byteLength;
    expect(backend.executeBatch(state, upsert, pairBatch(rows), rows.length)).toBe(ErrorCode.OK);
    expect(backend.serialize(state, upsert).byteLength).toBeGreaterThan(sizeBefore);
    expectHolds(state, rows);

    const large: Row = [3, 3, Array.from({ length: 4096 }, (_, index) => (index * 37) & 0xff), frameOf(3, 300)];
    expect(backend.executeBatch(state, upsert, pairBatch([large]), 1)).toBe(ErrorCode.OK);
    expectHolds(state, [...rows, large]);
  });

  it('rolls frames back through the undo log and the delta journal', () => {
    const state = backend.createState(upsert);
    const seed: Row[] = [
      [4, 0, text('seed 4/0'), [1, 0, 255]],
      [4, 1, text('seed 4/1'), [7, 7]],
    ];
    expect(backend.executeBatch(state, upsert, pairBatch(seed), seed.length)).toBe(ErrorCode.OK);
    const image = backend.serialize(state, upsert);
    const speculative: Row[] = [
      [4, 0, text('rewritten 4/0'), []],
      [4, 2, text('new 4/2'), [2]],
    ];

    expect(backend.undoEnable(state)).toStrictEqual({ ok: true, value: undefined });
    const checkpoint = backend.undoCheckpoint(state);
    expect(backend.executeBatch(state, upsert, pairBatch(speculative), 2)).toBe(ErrorCode.OK);
    expectHolds(state, [...seed, ...speculative]);
    backend.undoRollback(state, checkpoint);
    expect(backend.serialize(state, upsert)).toEqual(image);
    expectHolds(state, seed);

    const fromPos = backend.undoCheckpoint(state);
    expect(backend.executeBatchDelta(state, upsert, pairBatch(speculative), 2)).toBe(ErrorCode.OK);
    const exported = backend.deltaExportSegment(state, fromPos, backend.undoCheckpoint(state));
    if (!exported.ok) throw new Error(`unexpected delta export error ${exported.error}`);
    backend.deltaApplyRollbackSegment(state, exported.value.undoBytes, exported.value.entrySize);
    expectHolds(state, seed);
    backend.deltaApplyRollforwardSegment(state, exported.value.redoBytes, exported.value.entrySize);
    expectHolds(state, [...seed, ...speculative]);
  });

  it('drops every frame of an erased key1, and growth leaves none of its bytes in the image', () => {
    const state = backend.createState(upsert);
    const rows: Row[] = [
      [7, 0],
      [7, 1],
      [8, 0],
      [0, 7],
    ].map(([key1, key2]) => [key1, key2, frameOf(key1, key2), payloadOf(key1, key2)] as const);
    expect(backend.executeBatch(state, upsert, pairBatch(rows), rows.length)).toBe(ErrorCode.OK);
    expect(backend.executeBatch(state, erase, [{ data: Uint32Array.of(7) }], 1)).toBe(ErrorCode.OK);
    const kept = rows.filter(([key1]) => key1 !== 7);
    expectHolds(state, kept);
    expect(field(state, 7, 0, FRAME)).toEqual({ kind: 'absent' });
    expect(field(state, 7, 1, FRAME)).toEqual({ kind: 'absent' });

    // A frame past the arena grows it; growth compacts the live frames alone.
    const large: Row = [8, 1, Array.from({ length: 4096 }, (_, index) => (index * 37) & 0xff), []];
    expect(backend.executeBatch(state, upsert, pairBatch([large]), 1)).toBe(ErrorCode.OK);
    expectHolds(state, [...kept, large]);
    const image = backend.serialize(state, upsert);
    expect(contains(image, frameOf(7, 0))).toBe(false);
    expect(contains(image, frameOf(7, 1))).toBe(false);
    expect(contains(image, frameOf(8, 0))).toBe(true);
  });

  it('refuses value-object metadata without a capability, as for a one-key map', async () => {
    const metadata = {
      fields: [
        {
          slot: 0,
          fieldIdx: FRAME,
          schema: { root: 'text.Frame', frame: '00', rows: ['00'], profile: { records: [], attributes: [] } },
        },
      ],
      counts: [],
    };
    await expect(backend.loadProgram(pairReducer(UPSERT, 7), [PROGRAM_MAGIC], metadata)).rejects.toBeInstanceOf(
      VoProgramAdmissionUnavailableError,
    );
    // A two-key map's VO field is a VO declaration: explicitly empty metadata does not admit it either.
    await expect(
      backend.loadProgram(pairReducer(UPSERT, 7), [PROGRAM_MAGIC], { fields: [], counts: [] }),
    ).rejects.toBeInstanceOf(VoProgramAdmissionUnavailableError);
  });
});
