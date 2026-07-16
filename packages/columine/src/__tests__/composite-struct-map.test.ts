import { describe, expect, it } from 'bun:test';
import { existsSync, readFileSync } from 'node:fs';
import {
  type ColumnInput,
  ErrorCode,
  HEADER_SIZE,
  MAGIC,
  Opcode,
  PROGRAM_HASH_PREFIX,
  SlotType,
  StructFieldType,
  ValueType,
} from '../types.js';
import { createColumineWasmBackend } from '../wasm-backend.js';

const WASM_PATH = new URL('../../target/wasm32-unknown-unknown/wasm-release/columine_wasm.wasm', import.meta.url);
const HAS_WASM = existsSync(WASM_PATH);

function reducer(reduce: readonly number[]): Uint8Array {
  const init = [
    Opcode.SLOT_STRUCT_MAP2,
    0,
    SlotType.STRUCT_MAP2,
    8,
    0,
    2,
    StructFieldType.STRING,
    StructFieldType.UINT32,
  ];
  const code = [...reduce, Opcode.HALT];
  const bytecode = new Uint8Array(PROGRAM_HASH_PREFIX + HEADER_SIZE + init.length + code.length);
  const base = PROGRAM_HASH_PREFIX;
  bytecode[base] = MAGIC & 0xff;
  bytecode[base + 1] = (MAGIC >>> 8) & 0xff;
  bytecode[base + 2] = (MAGIC >>> 16) & 0xff;
  bytecode[base + 3] = (MAGIC >>> 24) & 0xff;
  bytecode[base + 4] = 1;
  bytecode[base + 6] = 1;
  bytecode[base + 7] = 4;
  bytecode[base + 10] = init.length;
  bytecode[base + 12] = code.length;
  bytecode.set(init, base + HEADER_SIZE);
  bytecode.set(code, base + HEADER_SIZE + init.length);
  return bytecode;
}

function columns(...values: readonly Uint32Array[]): ColumnInput[] {
  return values.map((data) => ({ data, type: ValueType.UINT32 }));
}

describe('StructMap2 public WASM readers', () => {
  it.skipIf(!HAS_WASM)('reduces pair rows and restores checkpoint bytes with point lookup and iteration', async () => {
    const backend = await createColumineWasmBackend(readFileSync(WASM_PATH));
    const getRow = backend.structMap2GetRow;
    const entries = backend.structMap2Entries;
    if (!getRow || !entries) throw new Error('WASM backend omitted StructMap2 readers');
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

    const first = getRow(state, upsert, 0, 7, 40);
    const second = getRow(state, upsert, 0, 7, 41);
    expect(first).toBeDefined();
    expect(second).toBeDefined();
    expect(first?.rowOffset).not.toBe(second?.rowOffset);
    const pairs = entries(state, upsert, 0).map(({ key1, key2 }) => [key1, key2]);
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
    expect(getRow(state, upsert, 0, 7, 41)).toBeUndefined();

    const restored = backend.deserialize(upsert, checkpoint);
    expect(getRow(restored, upsert, 0, 7, 40)).toEqual(first);
    expect(getRow(restored, upsert, 0, 7, 41)).toEqual(second);
    expect(entries(restored, upsert, 0)).toHaveLength(2);
  });
});
