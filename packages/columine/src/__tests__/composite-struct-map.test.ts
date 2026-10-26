import { beforeAll, describe, expect, it } from 'bun:test';
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
  SlotType,
  StructFieldType,
  ValueType,
} from '../types.js';
import { createColumineWasmBackend } from '../wasm-backend.js';

// The wasm-opt output `cargo-wasm` declares as its Nx output, in this checkout's dev tree: the build a checkout's
// source authority names (packages/nx-plugin/README.md, "Cargo profiles"), which the loaders follow too; the shipped
// build is the production configuration's dist/columine.wasm. The pre-optimization intermediate under target/ is not
// an output: a cached build never restores it, so gating on its presence skipped these tests silently, and when they
// did run they exercised bytes no build declares.
const WASM_PATH = new URL('../../.cache/wasm-dev/columine.wasm', import.meta.url);

function reducer(
  reduce: readonly number[],
  fields: readonly StructFieldType[] = [StructFieldType.STRING, StructFieldType.UINT32],
  numInputs = 4,
): Uint8Array {
  const init = [Opcode.SLOT_STRUCT_MAP2, 0, SlotType.STRUCT_MAP2, 8, 0, fields.length, ...fields, Opcode.HALT];
  const code = [...reduce, Opcode.HALT];
  const bytecode = new Uint8Array(PROGRAM_HASH_PREFIX + HEADER_SIZE + init.length + code.length);
  const base = PROGRAM_HASH_PREFIX;
  bytecode.set(
    encodeProgramHeader({
      magic: PROGRAM_MAGIC,
      numSlots: 1,
      numInputs,
      initCodeLength: init.length,
      reduceCodeLength: code.length,
    }),
    base,
  );
  bytecode.set(init, base + HEADER_SIZE);
  bytecode.set(code, base + HEADER_SIZE + init.length);
  return bytecode;
}

function columns(...values: readonly Uint32Array[]): ColumnInput[] {
  return values.map((data) => ({ data, type: ValueType.UINT32 }));
}

describe('StructMap2 public WASM readers', () => {
  // One backend for the file: every test loads its own programs and states, so
  // nothing is shared across them but the module instance. `ColumineBackend`
  // exposes no disposal — the instance is reclaimed with the test process — so
  // there is nothing for an `afterAll` to release.
  let backend: ColumineBackend;

  beforeAll(async () => {
    backend = await createColumineWasmBackend(readFileSync(WASM_PATH));
  });

  it('reduces pair rows and restores checkpoint bytes with point lookup and iteration', async () => {
    const getRow = backend.structMap2GetRow.bind(backend);
    const entries = backend.structMap2Entries.bind(backend);
    const upsert = await backend.loadProgram(reducer([Opcode.BATCH_STRUCT_MAP2_UPSERT_LAST, 0, 0, 1, 2, 2, 0, 3, 1]));
    const remove = await backend.loadProgram(reducer([Opcode.BATCH_STRUCT_MAP2_REMOVE, 0, 0, 1]));
    const state = backend.createState(upsert);

    expect(
      backend.executeBatch(
        state,
        upsert,
        columns(
          new Uint32Array([7, 7]),
          new Uint32Array([40, 41]),
          new Uint32Array([100, 101]),
          new Uint32Array([1_000, 2_000]),
        ),
        2,
      ),
    ).toBe(ErrorCode.OK);

    const firstRead = getRow(state, upsert, 0, 7, 40);
    const secondRead = getRow(state, upsert, 0, 7, 41);
    if (!firstRead.ok) throw new Error(`unexpected structMap2GetRow error ${firstRead.error}`);
    if (!secondRead.ok) throw new Error(`unexpected structMap2GetRow error ${secondRead.error}`);
    const first = firstRead.value;
    const second = secondRead.value;
    expect(first).toBeDefined();
    expect(second).toBeDefined();
    expect(first?.rowOffset).not.toBe(second?.rowOffset);
    const pairsRead = entries(state, upsert, 0);
    if (!pairsRead.ok) throw new Error(`unexpected structMap2Entries error ${pairsRead.error}`);
    const pairs = pairsRead.value.map(({ key1, key2 }) => [key1, key2]);
    pairs.sort((left, right) => left[1] - right[1]);
    expect(pairs).toEqual([
      [7, 40],
      [7, 41],
    ]);

    const checkpoint = backend.serialize(state, upsert);
    expect(
      backend.executeBatch(
        state,
        upsert,
        columns(new Uint32Array([7]), new Uint32Array([40]), new Uint32Array([199]), new Uint32Array([9_000])),
        1,
      ),
    ).toBe(ErrorCode.OK);
    expect(backend.executeBatch(state, remove, columns(new Uint32Array([7]), new Uint32Array([41])), 1)).toBe(
      ErrorCode.OK,
    );
    expect(getRow(state, upsert, 0, 7, 41)).toStrictEqual({ ok: true, value: undefined });

    const restored = backend.deserialize(upsert, checkpoint);
    expect(getRow(restored, upsert, 0, 7, 40)).toStrictEqual({ ok: true, value: first });
    expect(getRow(restored, upsert, 0, 7, 41)).toStrictEqual({ ok: true, value: second });
    const restoredEntries = entries(restored, upsert, 0);
    if (!restoredEntries.ok) throw new Error(`unexpected structMap2Entries error ${restoredEntries.error}`);
    expect(restoredEntries.value).toHaveLength(2);
  });

  it('executes strict signed i64x2 max and min in the built WASM dispatcher', async () => {
    expect(Opcode.BATCH_STRUCT_MAP2_UPSERT_MAX_I64X2).toBe(0x87);
    expect(Opcode.BATCH_STRUCT_MAP2_UPSERT_MIN_I64X2).toBe(0x89);
    const getRow = backend.structMap2GetRow.bind(backend);
    // One pair sees a tie on (-5, 10) and then the greater (-4, i64::MIN): max keeps the last, min the first.
    for (const [op, payload, first, second] of [
      [Opcode.BATCH_STRUCT_MAP2_UPSERT_MAX_I64X2, 30, -4n, -(1n << 63n)],
      [Opcode.BATCH_STRUCT_MAP2_UPSERT_MIN_I64X2, 10, -5n, 10n],
    ] as const) {
      const program = await backend.loadProgram(
        reducer(
          [op, 0, 0, 1, 1, 2, 0, 3, 1, 4, 2],
          [StructFieldType.UINT32, StructFieldType.INT64, StructFieldType.INT64],
          5,
        ),
      );
      const state = backend.createState(program);
      const candidates: ColumnInput[] = [
        { data: new Uint32Array([7, 7, 7]), type: ValueType.UINT32 },
        { data: new Uint32Array([40, 40, 40]), type: ValueType.UINT32 },
        { data: new Uint32Array([10, 20, 30]), type: ValueType.UINT32 },
        { data: new BigInt64Array([-5n, -5n, -4n]), type: ValueType.UINT32 },
        { data: new BigInt64Array([10n, 10n, -(1n << 63n)]), type: ValueType.UINT32 },
      ];
      expect(backend.executeBatch(state, program, candidates, 3)).toBe(ErrorCode.OK);

      const rowRead = getRow(state, program, 0, 7, 40);
      if (!rowRead.ok) throw new Error(`unexpected structMap2GetRow error ${rowRead.error}`);
      const row = rowRead.value;
      if (!row) throw new Error('the strict signed i64x2 pick did not materialize its winner');
      const bytes = backend.serialize(state, program);
      const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
      const offset = row.rowOffset;
      expect(view.getUint32(offset + 1, true)).toBe(payload);
      expect(view.getBigInt64(offset + 5, true)).toBe(first);
      expect(view.getBigInt64(offset + 13, true)).toBe(second);
    }
  });
});
