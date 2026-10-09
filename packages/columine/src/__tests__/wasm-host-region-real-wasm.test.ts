import { describe, expect, it } from 'bun:test';
import { tableFromArrays, tableFromIPC, tableToIPC, utf8 } from '@uwdata/flechette';
import { loadParseBackend } from '../parse-backend.js';
import type { CompactBatch, EncodedArrowSchema } from '../pipeline.js';
import { encodeProgramHeader } from '../reducer-bytecode.js';
import { ErrorCode, HEADER_SIZE, Opcode, PROGRAM_HASH_PREFIX, PROGRAM_MAGIC, SlotType, ValueType } from '../types.js';
import { createColumineWasmBackend } from '../wasm-backend.js';
import { WASM_PAGE_BYTES, WasmHostRegion, WasmMemoryContractError } from '../wasm-memory-contract.js';

// The host owns only pages above the module's linker-derived memory and skips
// pages native allocation appends. These run against the real built artifacts.
const WASM_PATH = new URL('../../dist/columine.wasm', import.meta.url);
const EP_WASM_PATH = new URL('../../dist/event_processor.wasm', import.meta.url);

async function instantiate(path: URL = WASM_PATH): Promise<WebAssembly.Memory> {
  const module = await WebAssembly.compile(await Bun.file(path).arrayBuffer());
  const instance = await WebAssembly.instantiate(module, {});
  const memory = instance.exports.memory;
  if (!(memory instanceof WebAssembly.Memory)) throw new Error('real wasm artifact exports no memory');
  return memory;
}

function leb(bytes: Uint8Array, at: number): [value: number, next: number] {
  let value = 0;
  let shift = 0;
  for (let pos = at; ; pos++) {
    const byte = bytes[pos];
    if (byte === undefined) throw new Error('truncated LEB128');
    value += (byte & 0x7f) * 2 ** shift;
    shift += 7;
    if ((byte & 0x80) === 0) return [value, pos + 1];
  }
}

/** End of every active data segment, read from the binary's data section. */
function activeDataEnds(wasm: Uint8Array): number[] {
  const ends: number[] = [];
  let at = 8;
  while (at < wasm.length) {
    const id = wasm[at];
    const [size, body] = leb(wasm, at + 1);
    if (id === 11) {
      let [count, pos] = leb(wasm, body);
      for (; count > 0; count--) {
        let flags: number;
        [flags, pos] = leb(wasm, pos);
        if (flags === 1) {
          let length: number;
          [length, pos] = leb(wasm, pos);
          pos += length;
          continue;
        }
        if (flags === 2) [, pos] = leb(wasm, pos);
        if (wasm[pos] !== 0x41) throw new Error('active data offset is not i32.const');
        let offset: number;
        [offset, pos] = leb(wasm, pos + 1);
        if (wasm[pos] !== 0x0b) throw new Error('active data offset expression is not one constant');
        let length: number;
        [length, pos] = leb(wasm, pos + 1);
        ends.push(offset + length);
        pos += length;
      }
    }
    at = body + size;
  }
  return ends;
}

function hashMapsProgram(maps: number, capacity: number): Uint8Array {
  const init: number[] = [];
  const reduce: number[] = [];
  for (let slot = 0; slot < maps; slot++) {
    init.push(Opcode.SLOT_DEF, slot, SlotType.HASHMAP, capacity & 0xff, capacity >> 8);
    reduce.push(Opcode.BATCH_MAP_UPSERT_LAST, slot, 0, 1);
  }
  const initCode = new Uint8Array([...init, Opcode.HALT]);
  const reduceCode = new Uint8Array([...reduce, Opcode.HALT]);
  const bytes = new Uint8Array(PROGRAM_HASH_PREFIX + HEADER_SIZE + initCode.length + reduceCode.length);
  bytes.set(
    encodeProgramHeader({
      magic: PROGRAM_MAGIC,
      numSlots: maps,
      numInputs: 2,
      initCodeLength: initCode.length,
      reduceCodeLength: reduceCode.length,
    }),
    PROGRAM_HASH_PREFIX,
  );
  bytes.set(initCode, PROGRAM_HASH_PREFIX + HEADER_SIZE);
  bytes.set(reduceCode, PROGRAM_HASH_PREFIX + HEADER_SIZE + initCode.length);
  return bytes;
}

function utf8Schema(): EncodedArrowSchema {
  const stream = tableToIPC(tableFromArrays({ text: [''] }, { types: { text: utf8() } }), { format: 'stream' });
  if (stream === null) throw new Error('Flechette did not return in-memory Arrow IPC bytes');
  const view = new DataView(stream.buffer, stream.byteOffset, stream.byteLength);
  return {
    schemaBytes: stream.slice(0, 8 + view.getUint32(4, true)),
    // UTF8 is compact kind tag 4; the field is nullable.
    fieldMetadata: new Uint8Array([4, 1, 0, 0]),
  };
}

function textBatch(schema: EncodedArrowSchema, rows: number, width: number): { batch: CompactBatch; texts: string[] } {
  const encoder = new TextEncoder();
  const texts = Array.from({ length: rows }, (_, row) => `${row}:`.padEnd(width, String.fromCharCode(97 + (row % 26))));
  const encoded = texts.map((text) => encoder.encode(text));
  const offsets = new Uint32Array(rows + 1);
  for (let row = 0; row < rows; row++) offsets[row + 1] = offsets[row] + encoded[row].byteLength;
  const data = new Uint8Array(offsets[rows]);
  for (let row = 0; row < rows; row++) data.set(encoded[row], offsets[row]);
  return { batch: { rowCount: rows, schema, columns: [{ kind: 'utf8', offsets, data }] }, texts };
}

const ARTIFACTS: ReadonlyArray<readonly [name: string, path: URL]> = [
  ['columine.wasm', WASM_PATH],
  ['event_processor.wasm', EP_WASM_PATH],
];

describe('host-owned WASM region on the real artifacts', () => {
  for (const [name, path] of ARTIFACTS) {
    it(`starts past every active data segment of the linker-derived memory, ${name}`, async () => {
      const wasm = new Uint8Array(await Bun.file(path).arrayBuffer());
      const ends = activeDataEnds(wasm);
      expect(ends.length).toBeGreaterThan(0);
      const memory = await instantiate(path);
      const region = new WasmHostRegion(memory);
      expect(region.offset).toBe(memory.buffer.byteLength);
      for (const end of ends) expect(end).toBeLessThanOrEqual(region.offset);
    });
  }

  it('encodes small, large, then small batches exactly after native allocation grew memory', async () => {
    const backend = await loadParseBackend(EP_WASM_PATH);
    try {
      const schema = utf8Schema();
      const small = textBatch(schema, 8, 32);
      const large = textBatch(schema, 40_000, 200);
      const first = backend.encode(small.batch);
      expect(tableFromIPC(first).toColumns().text).toEqual(small.texts);
      // Larger than everything staged before: its working set must not land on
      // pages native allocation appended after the first call.
      expect(tableFromIPC(backend.encode(large.batch)).toColumns().text).toEqual(large.texts);
      expect(backend.encode(small.batch)).toEqual(first);
    } finally {
      backend.dispose();
    }
  });

  it('moves past pages appended outside it and never grows over them', async () => {
    const memory = await instantiate();
    const region = new WasmHostRegion(memory);
    const first = region.reserve(3 * WASM_PAGE_BYTES);
    expect(first).toBe(region.offset);
    expect(memory.buffer.byteLength).toBe(first + 3 * WASM_PAGE_BYTES);

    // Owned pages are reused while they suffice.
    expect(region.reserve(WASM_PAGE_BYTES)).toBe(first);

    // Pages appended beyond the region, as native allocation appends them.
    const appended = memory.grow(1) * WASM_PAGE_BYTES;
    new Uint8Array(memory.buffer, appended, WASM_PAGE_BYTES).fill(0x5a);

    const moved = region.reserve(4 * WASM_PAGE_BYTES);
    expect(moved).toBeGreaterThanOrEqual(appended + WASM_PAGE_BYTES);
    expect(new Uint8Array(memory.buffer, appended, WASM_PAGE_BYTES).every((byte) => byte === 0x5a)).toBe(true);
    expect(memory.buffer.byteLength).toBeGreaterThanOrEqual(moved + 4 * WASM_PAGE_BYTES);
  });

  it('refuses past its cap without changing the region or memory', async () => {
    const memory = await instantiate();
    const pages = memory.buffer.byteLength / WASM_PAGE_BYTES;
    const region = new WasmHostRegion(memory, pages + 2);
    const start = region.offset;
    expect(() => region.reserve(3 * WASM_PAGE_BYTES)).toThrow(WasmMemoryContractError);
    expect(region.offset).toBe(start);
    expect(memory.buffer.byteLength).toBe(pages * WASM_PAGE_BYTES);
    expect(region.reserve(2 * WASM_PAGE_BYTES)).toBe(start);
  });

  it('initializes, reduces and reads a state larger than the module memory', async () => {
    const backend = await createColumineWasmBackend(await Bun.file(WASM_PATH).arrayBuffer());
    const maps = 5;
    const reducer = await backend.loadProgram(hashMapsProgram(maps, 32768));
    const state = backend.createState(reducer);
    const image = backend.serialize(state, reducer);
    // Larger than the artifact's whole initial memory: a host address inside it
    // would overlap the shadow stack or static data.
    expect(image.byteLength).toBeGreaterThan((await instantiate()).buffer.byteLength);

    const keys = new Uint32Array([1, 2, 3_000_000_000]);
    const values = new Uint32Array([10, 20, 0xffffffff]);
    expect(
      backend.executeBatch(
        state,
        reducer,
        [
          { data: keys, type: ValueType.UINT32 },
          { data: values, type: ValueType.UINT32 },
        ],
        keys.length,
      ),
    ).toBe(ErrorCode.OK);
    for (let slot = 0; slot < maps; slot++) {
      expect(backend.getMapSize(state, reducer, slot)).toBe(keys.length);
      for (let i = 0; i < keys.length; i++) {
        expect(backend.mapGet(state, reducer, slot, keys[i])).toStrictEqual({ ok: true, value: values[i] });
      }
      expect(backend.mapGet(state, reducer, slot, 3)).toStrictEqual({ ok: true, value: undefined });
    }

    const restored = backend.deserialize(reducer, backend.serialize(state, reducer));
    expect(backend.mapGet(restored, reducer, maps - 1, 2)).toStrictEqual({ ok: true, value: 20 });
  });
});
