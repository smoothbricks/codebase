import { beforeEach, describe, expect, it } from 'bun:test';
import { readFileSync } from 'node:fs';
import { encodeProgramHeader } from '../reducer-bytecode.js';
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
  ValueType,
} from '../types.js';
import { VoProgramAdmissionUnavailableError } from '../vo-program-metadata.js';
import { createColumineWasmBackend } from '../wasm-backend.js';

// The shipped wasm-opt output, as in composite-struct-map.test.ts: never gated on presence.
const WASM_PATH = new URL('../../dist/columine.wasm', import.meta.url);

const CAPACITY = 4;
const VALUE_FIELD = 0;
const VO_FIELD = 1;

// STRUCT_MAP slot 0 rows: field 0 u32, field 1 VO. Columns: 0 keys, 1 u32 values,
// 2 CSR u32 offsets (rows + 1), 3 raw VO bytes.
const UPSERT_VO = [Opcode.BATCH_STRUCT_MAP_UPSERT_LAST, 0, 0, 1, 1, VALUE_FIELD, 1, 2, 3, VO_FIELD] as const;
const UPSERT_VALUE_ONLY = [Opcode.BATCH_STRUCT_MAP_UPSERT_LAST, 0, 0, 1, 1, VALUE_FIELD, 0] as const;

function voReducer(
  reduce: readonly number[],
  fields: readonly StructFieldType[] = [StructFieldType.UINT32, StructFieldType.VO],
): Uint8Array {
  const init = [Opcode.SLOT_STRUCT_MAP, 0, SlotType.STRUCT_MAP, CAPACITY, 0, fields.length, ...fields, Opcode.HALT];
  const code = [...reduce, Opcode.HALT];
  const bytecode = new Uint8Array(PROGRAM_HASH_PREFIX + HEADER_SIZE + init.length + code.length);
  bytecode.set(
    encodeProgramHeader({
      magic: PROGRAM_MAGIC,
      numSlots: 1,
      numInputs: 4,
      initCodeLength: init.length,
      reduceCodeLength: code.length,
    }),
    PROGRAM_HASH_PREFIX,
  );
  bytecode.set(init, PROGRAM_HASH_PREFIX + HEADER_SIZE);
  bytecode.set(code, PROGRAM_HASH_PREFIX + HEADER_SIZE + init.length);
  return bytecode;
}

/** One batch of `[key, value, voBytes]` rows as real program columns: raw VO bytes addressed by u32 CSR offsets. */
function voBatch(rows: readonly (readonly [number, number, readonly number[]])[]): ColumnInput[] {
  const offsets = new Uint32Array(rows.length + 1);
  for (const [row, [, , bytes]] of rows.entries()) offsets[row + 1] = offsets[row] + bytes.length;
  return [
    { data: Uint32Array.from(rows, ([key]) => key), type: ValueType.UINT32 },
    { data: Uint32Array.from(rows, ([, value]) => value), type: ValueType.UINT32 },
    { data: offsets, type: ValueType.UINT32 },
    { data: Uint8Array.from(rows.flatMap(([, , bytes]) => bytes)), type: ValueType.UINT32 },
  ];
}

function u32le(...words: readonly number[]): number[] {
  const out = new DataView(new ArrayBuffer(words.length * 4));
  for (const [index, word] of words.entries()) out.setUint32(index * 4, word, true);
  return [...new Uint8Array(out.buffer)];
}

/**
 * The one offset of `needle` in `image`. Corruption oracles locate state words
 * by the content the VM wrote there, never by a restated layout, and fail
 * loudly if that content is ambiguous.
 */
function uniqueIndexOf(image: Uint8Array, needle: readonly number[]): number {
  const hits: number[] = [];
  for (let at = image.indexOf(needle[0]); at !== -1; at = image.indexOf(needle[0], at + 1)) {
    if (needle.every((byte, index) => image[at + index] === byte)) hits.push(at);
  }
  expect(hits).toHaveLength(1);
  return hits[0];
}

/** A copy of `image` with the u32 LE word at `at` replaced. */
function withU32(image: Uint8Array, at: number, value: number): Uint8Array {
  const copy = image.slice();
  new DataView(copy.buffer).setUint32(at, value, true);
  return copy;
}

const EDGE_BYTES = [0x00, 0x01, 0xff] as const;
const MIXED_BYTES = [0xff, 0x00, 0x01, 0x80, 0x7f] as const;
const REPLACEMENT_BYTES = [0xff, 0xff, 0x00] as const;

describe('STRUCT_MAP VO fields through the built WASM VM', () => {
  // A fresh module instance per test: the native undo log is per instance, so
  // one test's enabled journal never leaks into another's batches.
  let backend: ColumineBackend;
  let upsert: ReducerProgram;
  let valueOnly: ReducerProgram;

  beforeEach(async () => {
    backend = await createColumineWasmBackend(readFileSync(WASM_PATH));
    upsert = await backend.loadProgram(voReducer(UPSERT_VO));
    valueOnly = await backend.loadProgram(voReducer(UPSERT_VALUE_ONLY));
  });

  const vo = (state: StateHandle, key: number) => backend.structMapArenaField(state, upsert, 0, key, VO_FIELD);
  const bytes = (values: readonly number[]): StructMapArenaField => ({ kind: 'bytes', bytes: Uint8Array.from(values) });

  it('stores exact bytes, replaces them, and distinguishes absent, unset, empty and non-arena fields', () => {
    const state = backend.createState(upsert);
    const first = voBatch([
      [1, 10, EDGE_BYTES],
      [2, 20, []],
      [3, 30, MIXED_BYTES],
    ]);
    const unsetRow = voBatch([[4, 40, []]]);
    const replacement = voBatch([[1, 11, REPLACEMENT_BYTES]]);
    const batches = [
      [upsert, first, 3],
      [valueOnly, unsetRow, 1],
      [upsert, replacement, 1],
    ] as const;

    expect(backend.executeBatch(state, upsert, first, 3)).toBe(ErrorCode.OK);
    expect(vo(state, 1)).toEqual(bytes(EDGE_BYTES));
    expect(vo(state, 2)).toEqual(bytes([]));
    expect(vo(state, 3)).toEqual(bytes(MIXED_BYTES));
    expect(vo(state, 99)).toEqual({ kind: 'absent' });

    expect(backend.executeBatch(state, valueOnly, unsetRow, 1)).toBe(ErrorCode.OK);
    expect(vo(state, 4)).toEqual({ kind: 'unset' });
    expect(backend.structMapArenaField(state, upsert, 0, 1, VALUE_FIELD)).toEqual({
      kind: 'error',
      error: ErrorCode.INVALID_PROGRAM,
    });

    const beforeReplacement = vo(state, 1);
    expect(backend.executeBatch(state, upsert, replacement, 1)).toBe(ErrorCode.OK);
    expect(vo(state, 1)).toEqual(bytes(REPLACEMENT_BYTES));
    expect(vo(state, 3)).toEqual(bytes(MIXED_BYTES));
    // A read is a value: the replacement did not reach bytes handed out before it.
    expect(beforeReplacement).toEqual(bytes(EDGE_BYTES));

    // Replaying the same batches from a fresh state reproduces the state image byte for byte.
    const replayed = backend.createState(upsert);
    for (const [program, columns, rows] of batches) {
      expect(backend.executeBatch(replayed, program, columns, rows)).toBe(ErrorCode.OK);
    }
    expect(backend.serialize(replayed, upsert)).toEqual(backend.serialize(state, upsert));
  });

  it('rolls VO bytes back through the undo log and delta journal', () => {
    const state = backend.createState(upsert);
    expect(
      backend.executeBatch(
        state,
        upsert,
        voBatch([
          [1, 10, EDGE_BYTES],
          [3, 30, MIXED_BYTES],
        ]),
        2,
      ),
    ).toBe(ErrorCode.OK);
    const speculative = voBatch([
      [1, 12, REPLACEMENT_BYTES],
      [5, 50, [0x01, 0x00, 0xff, 0x00]],
    ]);

    expect(backend.undoEnable(state)).toStrictEqual({ ok: true, value: undefined });
    const checkpoint = backend.undoCheckpoint(state);
    expect(backend.executeBatch(state, upsert, speculative, 2)).toBe(ErrorCode.OK);
    expect(vo(state, 1)).toEqual(bytes(REPLACEMENT_BYTES));
    expect(vo(state, 5)).toEqual(bytes([0x01, 0x00, 0xff, 0x00]));
    backend.undoRollback(state, checkpoint);
    expect(backend.undoHasOverflow()).toBe(false);
    expect(vo(state, 1)).toEqual(bytes(EDGE_BYTES));
    expect(vo(state, 3)).toEqual(bytes(MIXED_BYTES));
    expect(vo(state, 5)).toEqual({ kind: 'absent' });

    // The rollback left this window open, so the next segment nests in it with a savepoint, not a begin.
    const fromPos = backend.undoCheckpoint(state);
    expect(backend.executeBatchDelta(state, upsert, speculative, 2)).toBe(ErrorCode.OK);
    const exported = backend.deltaExportSegment(state, fromPos, backend.undoCheckpoint(state));
    if (!exported.ok) throw new Error(`unexpected delta export error ${exported.error}`);
    const segment = exported.value;
    expect(segment.overflow).toBe(false);
    expect(segment.undoBytes.byteLength).toBeGreaterThan(0);

    backend.deltaApplyRollbackSegment(state, segment.undoBytes, segment.entrySize);
    expect(vo(state, 1)).toEqual(bytes(EDGE_BYTES));
    expect(vo(state, 5)).toEqual({ kind: 'absent' });

    backend.deltaApplyRollforwardSegment(state, segment.redoBytes, segment.entrySize);
    expect(vo(state, 1)).toEqual(bytes(REPLACEMENT_BYTES));
    expect(vo(state, 3)).toEqual(bytes(MIXED_BYTES));
    expect(vo(state, 5)).toEqual(bytes([0x01, 0x00, 0xff, 0x00]));
  });

  it('keeps forks independent and relocates VO bytes exactly when the arena grows', () => {
    const state = backend.createState(upsert);
    expect(
      backend.executeBatch(
        state,
        upsert,
        voBatch([
          [1, 10, EDGE_BYTES],
          [2, 20, []],
          [3, 30, MIXED_BYTES],
        ]),
        3,
      ),
    ).toBe(ErrorCode.OK);
    const fork = backend.deserialize(upsert, backend.serialize(state, upsert));
    const sizeBeforeGrowth = backend.serialize(state, upsert).byteLength;

    // 4 KiB exceeds the initial arena (64 bytes per hash slot); 37 is odd, so every byte value occurs.
    const large = Array.from({ length: 4096 }, (_, index) => (index * 37) & 0xff);
    expect(backend.executeBatch(state, upsert, voBatch([[7, 70, large]]), 1)).toBe(ErrorCode.OK);
    expect(backend.serialize(state, upsert).byteLength).toBeGreaterThan(sizeBeforeGrowth);
    expect(vo(state, 7)).toEqual(bytes(large));
    expect(vo(state, 1)).toEqual(bytes(EDGE_BYTES));
    expect(vo(state, 2)).toEqual(bytes([]));
    expect(vo(state, 3)).toEqual(bytes(MIXED_BYTES));

    expect(vo(fork, 7)).toEqual({ kind: 'absent' });
    expect(backend.executeBatch(fork, upsert, voBatch([[1, 13, REPLACEMENT_BYTES]]), 1)).toBe(ErrorCode.OK);
    expect(vo(fork, 1)).toEqual(bytes(REPLACEMENT_BYTES));
    expect(vo(state, 1)).toEqual(bytes(EDGE_BYTES));
  });

  it('refuses malformed CSR sources by their exact uploaded lengths and leaves the image untouched', () => {
    const state = backend.createState(upsert);
    expect(backend.executeBatch(state, upsert, voBatch([[1, 10, EDGE_BYTES]]), 1)).toBe(ErrorCode.OK);
    const image = backend.serialize(state, upsert);
    const csr = (keys: readonly number[], offsets: readonly number[], values: readonly number[]): ColumnInput[] => [
      { data: Uint32Array.from(keys), type: ValueType.UINT32 },
      { data: Uint32Array.from(keys, (key) => key * 10), type: ValueType.UINT32 },
      { data: Uint32Array.from(offsets), type: ValueType.UINT32 },
      { data: Uint8Array.from(values), type: ValueType.UINT32 },
    ];
    const malformed = [
      // The offsets column ends before row 0's end boundary.
      [csr([9], [0], EDGE_BYTES), ErrorCode.COLUMN_UNDERRUN],
      // The values column ends one byte before row 0's end boundary.
      [csr([9], [0, 4], EDGE_BYTES), ErrorCode.COLUMN_UNDERRUN],
      // Row 1 runs backwards; row 0, a new key, must not survive the refused batch.
      [csr([9, 3], [0, 2, 1], EDGE_BYTES), ErrorCode.INVALID_CELL_VALUE],
      // A u32::MAX end boundary addresses far past the uploaded values column.
      [csr([9], [0, 0xffff_ffff], EDGE_BYTES), ErrorCode.COLUMN_UNDERRUN],
    ] as const;

    expect(backend.undoEnable(state)).toStrictEqual({ ok: true, value: undefined });
    for (const [columns, status] of malformed) {
      const rows = columns[0].data.length;
      expect(backend.executeBatch(state, upsert, columns, rows)).toBe(status);
      expect(backend.executeBatchDelta(state, upsert, columns, rows)).toBe(status);
      expect(backend.serialize(state, upsert)).toEqual(image);
      expect(vo(state, 9)).toEqual({ kind: 'absent' });
    }
    expect(vo(state, 1)).toEqual(bytes(EDGE_BYTES));
  });

  it('refuses corrupt stored references and arena headers as INVALID_STATE without mutating the image', () => {
    const distinct = Array.from({ length: 23 }, (_, index) => 0xa0 + index);
    const state = backend.createState(upsert);
    expect(
      backend.executeBatch(
        state,
        upsert,
        voBatch([
          [1, 10, EDGE_BYTES],
          [3, 30, distinct],
        ]),
        2,
      ),
    ).toBe(ErrorCode.OK);
    const image = backend.serialize(state, upsert);
    const used = EDGE_BYTES.length + distinct.length;

    // Key 3's in-row reference is the public [offset, length] record: its payload follows key 1's.
    const reference = uniqueIndexOf(image, u32le(EDGE_BYTES.length, distinct.length));
    // The arena header [capacity, used] precedes its data; the used word written by the VM pins the location.
    const data = uniqueIndexOf(image, [...EDGE_BYTES, ...distinct]);
    const header = data - 8;
    expect([...image.subarray(header + 4, data)]).toEqual(u32le(used));
    const capacity = new DataView(image.buffer).getUint32(header, true);
    expect(capacity).toBeGreaterThan(used);

    const corruptions = [
      // A reference ending one byte past arena used, still inside arena capacity.
      { image: withU32(image, reference + 4, distinct.length + 1), key: 3 },
      // A reference whose end overflows u32.
      { image: withU32(image, reference, 0xffff_fff0), key: 3 },
      // An arena header claiming more used bytes than its capacity.
      { image: withU32(image, header + 4, capacity + 1), key: 1 },
      // An arena capacity running past the state image yet inside WASM linear memory.
      { image: withU32(image, header, image.byteLength), key: 1 },
    ];
    for (const corruption of corruptions) {
      const corrupt = backend.deserialize(upsert, corruption.image);
      expect(backend.structMapArenaField(corrupt, upsert, 0, corruption.key, VO_FIELD)).toEqual({
        kind: 'error',
        error: ErrorCode.INVALID_STATE,
      });
      expect(backend.executeBatch(corrupt, upsert, voBatch([[corruption.key, 99, REPLACEMENT_BYTES]]), 1)).toBe(
        ErrorCode.INVALID_STATE,
      );
      expect(backend.serialize(corrupt, upsert)).toEqual(corruption.image);
    }
  });

  it('refuses programs whose committed field kinds disagree with the state', async () => {
    const arrayBool = await backend.loadProgram(
      voReducer(UPSERT_VO, [StructFieldType.UINT32, StructFieldType.ARRAY_BOOL]),
    );
    const bool = await backend.loadProgram(
      voReducer(UPSERT_VALUE_ONLY, [StructFieldType.UINT32, StructFieldType.BOOL]),
    );
    const row = voBatch([[1, 10, [0x01, 0x00, 0x01]]]);
    const voState = backend.createState(upsert);
    const arrayBoolState = backend.createState(arrayBool);
    expect(backend.executeBatch(voState, upsert, row, 1)).toBe(ErrorCode.OK);
    expect(backend.executeBatch(arrayBoolState, arrayBool, row, 1)).toBe(ErrorCode.OK);
    // ARRAY_BOOL keeps its arena bytes through the same checked reader.
    expect(backend.structMapArenaField(arrayBoolState, arrayBool, 0, 1, VO_FIELD)).toEqual(bytes([0x01, 0x00, 0x01]));
    const voImage = backend.serialize(voState, upsert);
    const arrayBoolImage = backend.serialize(arrayBoolState, arrayBool);

    expect(backend.executeBatch(voState, arrayBool, row, 1)).toBe(ErrorCode.INVALID_PROGRAM);
    expect(backend.executeBatch(voState, bool, row, 1)).toBe(ErrorCode.INVALID_PROGRAM);
    expect(backend.executeBatch(arrayBoolState, upsert, row, 1)).toBe(ErrorCode.INVALID_PROGRAM);
    expect(backend.serialize(voState, upsert)).toEqual(voImage);
    expect(backend.serialize(arrayBoolState, arrayBool)).toEqual(arrayBoolImage);
  });

  it('keeps the first VO bytes under 0x81 and appends nothing for skipped rows', async () => {
    const first = await backend.loadProgram(voReducer([Opcode.BATCH_STRUCT_MAP_UPSERT_FIRST, ...UPSERT_VO.slice(1)]));
    const state = backend.createState(first);
    const duplicate = voBatch([
      [1, 10, EDGE_BYTES],
      [1, 11, REPLACEMENT_BYTES],
    ]);
    expect(backend.executeBatch(state, first, duplicate, 2)).toBe(ErrorCode.OK);
    expect(backend.structMapArenaField(state, first, 0, 1, VO_FIELD)).toEqual(bytes(EDGE_BYTES));

    // The skipped duplicate wrote neither row nor arena bytes: the image equals a single-row run.
    const image = backend.serialize(state, first);
    const single = backend.createState(first);
    expect(backend.executeBatch(single, first, voBatch([[1, 10, EDGE_BYTES]]), 1)).toBe(ErrorCode.OK);
    expect(backend.serialize(single, first)).toEqual(image);

    expect(backend.executeBatch(state, first, voBatch([[1, 12, MIXED_BYTES]]), 1)).toBe(ErrorCode.OK);
    expect(backend.serialize(state, first)).toEqual(image);
  });

  it('refuses native admission metadata without a linked capability and keeps metadata-free VO storage', async () => {
    const metadata = {
      fields: [
        {
          slot: 0,
          fieldIdx: VO_FIELD,
          schema: { root: 'orders.Line', frame: '00', rows: ['00'], profile: { records: [], attributes: [] } },
        },
      ],
      counts: [],
    };
    await expect(backend.loadProgram(voReducer(UPSERT_VO), [PROGRAM_MAGIC], metadata)).rejects.toBeInstanceOf(
      VoProgramAdmissionUnavailableError,
    );
    await expect(backend.loadProgram(voReducer(UPSERT_VO), undefined, metadata)).rejects.toMatchObject({
      code: 'VO_ADMISSION_UNAVAILABLE',
    });
    // Without metadata the same bytes remain opaque storage on this backend.
    expect((await backend.loadProgram(voReducer(UPSERT_VO), [PROGRAM_MAGIC])).numSlots).toBe(1);
  });

  it('accepts explicitly empty metadata only for programs that declare no VO field', async () => {
    const empty = { fields: [], counts: [] };
    const link = {
      slot: 0,
      fieldIdx: VO_FIELD,
      schema: { root: 'orders.Line', frame: '00', rows: ['00'], profile: { records: [], attributes: [] } },
    };
    const withLink = { fields: [link], counts: [] };
    const withRole = {
      fields: [],
      counts: [{ instructionOffset: 0, countField: 0, histogramFields: [], maskFields: [] }],
    };
    const arrayBoolBytes = voReducer(UPSERT_VO, [StructFieldType.UINT32, StructFieldType.ARRAY_BOOL]);
    const scalarBytes = voReducer(UPSERT_VALUE_ONLY, [StructFieldType.UINT32, StructFieldType.UINT32]);

    for (const bytecode of [arrayBoolBytes, scalarBytes]) {
      // Empty metadata names nothing to admit: the program is exactly the metadata-free one.
      const loaded = await backend.loadProgram(bytecode, [PROGRAM_MAGIC], empty);
      expect(loaded).toEqual(await backend.loadProgram(bytecode, [PROGRAM_MAGIC]));
      expect(loaded.voProgramMetadata).toBeUndefined();
      // Any non-empty metadata is still refused with the typed error, never accepted unchecked.
      await expect(backend.loadProgram(bytecode, [PROGRAM_MAGIC], withLink)).rejects.toBeInstanceOf(
        VoProgramAdmissionUnavailableError,
      );
      await expect(backend.loadProgram(bytecode, undefined, withRole)).rejects.toMatchObject({
        code: 'VO_ADMISSION_UNAVAILABLE',
      });
      // The accepted magics still decide the program before its metadata is considered.
      await expect(backend.loadProgram(bytecode, [PROGRAM_MAGIC + 1], empty)).rejects.toThrow('bad magic');
    }

    // The accepted ordinary program is a real one: it runs on the shipped WASM VM.
    const arrayBool = await backend.loadProgram(arrayBoolBytes, [PROGRAM_MAGIC], empty);
    const state = backend.createState(arrayBool);
    const row = voBatch([[1, 10, [0x01, 0x00, 0x01]]]);
    expect(backend.executeBatch(state, arrayBool, row, 1)).toBe(ErrorCode.OK);
    expect(backend.structMapArenaField(state, arrayBool, 0, 1, VO_FIELD)).toEqual(bytes([0x01, 0x00, 0x01]));

    // A program that declares a VO field is refused empty metadata too, written or not by its reducer.
    for (const bytecode of [voReducer(UPSERT_VO), voReducer(UPSERT_VALUE_ONLY)]) {
      await expect(backend.loadProgram(bytecode, [PROGRAM_MAGIC], empty)).rejects.toBeInstanceOf(
        VoProgramAdmissionUnavailableError,
      );
      await expect(backend.loadProgram(bytecode, undefined, empty)).rejects.toMatchObject({
        code: 'VO_ADMISSION_UNAVAILABLE',
      });
    }
  });
});
