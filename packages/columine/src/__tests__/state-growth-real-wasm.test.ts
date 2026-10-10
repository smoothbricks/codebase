import { describe, expect, it } from 'bun:test';
import { appendIndex } from '../operand.js';
import { encodeProgramHeader } from '../reducer-bytecode.js';
import {
  AggType,
  ErrorCode,
  HEADER_SIZE,
  NO_PARENT_TS_COL,
  Opcode,
  PROGRAM_HASH_PREFIX,
  PROGRAM_MAGIC,
  SlotType,
  SlotTypeFlag,
  StructFieldType,
  ValueType,
} from '../types.js';
import { loadColumineWasm, VmStateAdmissionError } from '../wasm-backend.js';
import { WasmHostRegion } from '../wasm-memory-contract.js';

const WASM_PATH = new URL('../../dist/columine.wasm', import.meta.url);

function program(init: readonly number[], reduce: readonly number[], numSlots: number, numInputs: number): Uint8Array {
  const initCode = new Uint8Array([...init, Opcode.HALT]);
  const reduceCode = new Uint8Array([...reduce, Opcode.HALT]);
  const bytes = new Uint8Array(PROGRAM_HASH_PREFIX + HEADER_SIZE + initCode.length + reduceCode.length);
  bytes.set(
    encodeProgramHeader({
      magic: PROGRAM_MAGIC,
      numSlots,
      numInputs,
      initCodeLength: initCode.length,
      reduceCodeLength: reduceCode.length,
    }),
    PROGRAM_HASH_PREFIX,
  );
  bytes.set(initCode, PROGRAM_HASH_PREFIX + HEADER_SIZE);
  bytes.set(reduceCode, PROGRAM_HASH_PREFIX + HEADER_SIZE + initCode.length);
  return bytes;
}

function invoke(exports: WebAssembly.Exports, name: string, ...args: number[]): number {
  const entry = exports[name];
  if (typeof entry !== 'function') throw new Error(`real wasm artifact is missing ${name}`);
  const result: unknown = entry(...args);
  if (typeof result !== 'number') throw new Error(`${name} did not return a native status`);
  return result;
}

function admittedSize(
  exports: WebAssembly.Exports,
  memory: WebAssembly.Memory,
  programPtr: number,
  programLength: number,
): number {
  const outputPtr = Math.ceil((programPtr + programLength) / 8) * 8;
  expect(invoke(exports, 'vm_calculate_state_size', programPtr, programLength, outputPtr)).toBe(ErrorCode.OK);
  return new DataView(memory.buffer).getUint32(outputPtr, true);
}

describe('real wasm state geometry and committed-boundary growth', () => {
  it('returns growth to the full-window owner and replays deltas only within the grown committed layout', async () => {
    const backend = await loadColumineWasm(WASM_PATH);
    const reducer = await backend.loadProgram(
      program([Opcode.SLOT_DEF, 0, SlotType.HASHMAP, 8, 0], [Opcode.BATCH_MAP_UPSERT_LAST, 0, 0, 1], 1, 2),
    );
    const state = backend.createState(reducer);
    expect(
      backend.executeBatch(
        state,
        reducer,
        [
          { data: new Uint32Array([7]), type: ValueType.UINT32 },
          { data: new Uint32Array([70]), type: ValueType.UINT32 },
        ],
        1,
      ),
    ).toBe(ErrorCode.OK);
    const original = backend.serialize(state, reducer);
    let committed = original;
    const keys = Uint32Array.from({ length: 160 }, (_, index) => index + 1);
    const values = Uint32Array.from(keys, (key) => key * 10);
    const columns = [
      { data: keys, type: ValueType.UINT32 },
      { data: values, type: ValueType.UINT32 },
    ];
    let checkpoint = 0;
    let growths = 0;
    for (;;) {
      expect(backend.undoEnable(state)).toStrictEqual({ ok: true, value: undefined });
      checkpoint = backend.undoCheckpoint(state);
      // This successful batch belongs to the same window as the later failure.
      expect(
        backend.executeBatchDelta(
          state,
          reducer,
          [
            { data: new Uint32Array([1000]), type: ValueType.UINT32 },
            { data: new Uint32Array([10000]), type: ValueType.UINT32 },
          ],
          1,
        ),
      ).toBe(ErrorCode.OK);
      const attemptCheckpoint = backend.undoCheckpoint(state);
      const beforeAttempt = backend.serialize(state, reducer);
      const result = backend.executeBatchDelta(state, reducer, columns, keys.length);
      if (result === ErrorCode.OK) break;
      expect(result).toBe(ErrorCode.NEEDS_GROWTH);
      expect(backend.undoCheckpoint(state)).toBe(attemptCheckpoint);
      expect(backend.serialize(state, reducer)).toEqual(beforeAttempt);
      expect(backend.growState(state, reducer)).toBe(ErrorCode.NEEDS_GROWTH);
      expect(backend.undoCheckpoint(state)).toBe(attemptCheckpoint);
      expect(backend.serialize(state, reducer)).toEqual(beforeAttempt);
      backend.undoRollback(state, checkpoint);
      expect(backend.serialize(state, reducer)).toEqual(committed);
      expect(backend.growState(state, reducer)).toBe(ErrorCode.NEEDS_GROWTH);
      backend.undoCommit(state, checkpoint);
      expect(backend.undoCheckpoint(state)).toBe(0);
      expect(backend.growState(state, reducer)).toBe(ErrorCode.OK);
      expect(backend.undoCheckpoint(state)).toBe(0);
      const grownCommitted = backend.serialize(state, reducer);
      expect(grownCommitted.byteLength).toBeGreaterThan(committed.byteLength);
      expect(backend.getMapSize(state, reducer, 0)).toBe(1);
      expect(backend.mapGet(state, reducer, 0, 7)).toStrictEqual({ ok: true, value: 70 });
      expect(backend.mapGet(state, reducer, 0, 1000)).toStrictEqual({ ok: true, value: undefined });
      committed = grownCommitted;
      growths++;
      // Retry both batches, not merely the one that requested growth.
    }
    expect(growths).toBeGreaterThan(1);
    const after = backend.serialize(state, reducer);
    expect(after.byteLength).toBeGreaterThan(original.byteLength);
    expect(backend.getMapSize(state, reducer, 0)).toBe(keys.length + 1);
    for (const key of keys) expect(backend.mapGet(state, reducer, 0, key)).toStrictEqual({ ok: true, value: key * 10 });
    expect(backend.mapGet(state, reducer, 0, 1000)).toStrictEqual({ ok: true, value: 10000 });
    const fork = backend.deserialize(reducer, after);
    // These calls replace the shared wasm upload with a different handle.
    expect(backend.mapGet(fork, reducer, 0, 160)).toStrictEqual({ ok: true, value: 1600 });
    const forkBefore = backend.serialize(fork, reducer);
    const exported = backend.deltaExportSegment(state, checkpoint, backend.undoCheckpoint(state));
    if (!exported.ok) throw new Error(`unexpected delta export error ${exported.error}`);
    const segment = exported.value;
    expect(segment.overflow).toBe(false);
    expect(segment.undoBytes.byteLength).toBeGreaterThan(0);
    for (let at = 0; at < segment.undoBytes.byteLength; at += segment.entrySize) {
      expect(segment.undoBytes[at]).toBe(15);
      expect(segment.redoBytes[at]).toBe(15);
    }
    expect(backend.mapGet(fork, reducer, 0, 1)).toStrictEqual({ ok: true, value: 10 });
    backend.undoRollback(state, checkpoint);
    expect(backend.serialize(state, reducer)).toEqual(committed);
    expect(backend.serialize(fork, reducer)).toEqual(forkBefore);
    backend.undoCommit(state, checkpoint);
    backend.deltaApplyRollforwardSegment(state, segment.redoBytes, segment.entrySize);
    expect(backend.serialize(state, reducer)).toEqual(after);
    expect(backend.serialize(fork, reducer)).toEqual(forkBefore);
    backend.deltaApplyRollbackSegment(state, segment.undoBytes, segment.entrySize);
    expect(backend.serialize(state, reducer)).toEqual(committed);
    expect(backend.serialize(fork, reducer)).toEqual(forkBefore);
  });

  it('preserves persisted bytes of an allocated auxiliary lane through update rollback and delta replay', async () => {
    const backend = await loadColumineWasm(WASM_PATH);
    const reducer = await backend.loadProgram(
      program([Opcode.SLOT_DEF, 0, SlotType.HASHMAP, 8, 0], [Opcode.BATCH_MAP_UPSERT_LAST, 0, 0, 1], 1, 2),
    );
    const state = backend.createState(reducer);
    expect(
      backend.executeBatch(
        state,
        reducer,
        [
          { data: new Uint32Array([7]), type: ValueType.UINT32 },
          { data: new Uint32Array([30]), type: ValueType.UINT32 },
        ],
        1,
      ),
    ).toBe(ErrorCode.OK);
    const beforeBegin = backend.serialize(state, reducer);
    expect(backend.undoEnable(state)).toStrictEqual({ ok: true, value: undefined });
    const checkpoint = backend.undoCheckpoint(state);
    expect(
      backend.executeBatchDelta(
        state,
        reducer,
        [
          { data: new Uint32Array([7]), type: ValueType.UINT32 },
          { data: new Uint32Array([70]), type: ValueType.UINT32 },
        ],
        1,
      ),
    ).toBe(ErrorCode.OK);
    const updated = backend.serialize(state, reducer);
    const exported = backend.deltaExportSegment(state, checkpoint, backend.undoCheckpoint(state));
    if (!exported.ok) throw new Error(`unexpected delta export error ${exported.error}`);
    const segment = exported.value;
    expect(segment.overflow).toBe(false);
    backend.undoRollback(state, checkpoint);
    expect(backend.mapGet(state, reducer, 0, 7)).toStrictEqual({ ok: true, value: 30 });
    expect(backend.serialize(state, reducer)).toEqual(beforeBegin);
    backend.undoCommit(state, checkpoint);
    backend.deltaApplyRollforwardSegment(state, segment.redoBytes, segment.entrySize);
    expect(backend.serialize(state, reducer)).toEqual(updated);
    backend.deltaApplyRollbackSegment(state, segment.undoBytes, segment.entrySize);
    expect(backend.serialize(state, reducer)).toEqual(beforeBegin);
  });

  it('keeps ordinary automatic growth and returns enabled non-delta growth without changing its image', async () => {
    const backend = await loadColumineWasm(WASM_PATH);
    const reducer = await backend.loadProgram(
      program([Opcode.SLOT_DEF, 0, SlotType.HASHMAP, 8, 0], [Opcode.BATCH_MAP_UPSERT_LAST, 0, 0, 1], 1, 2),
    );
    const state = backend.createState(reducer);
    const initial = backend.serialize(state, reducer);
    const keys = Uint32Array.from({ length: 160 }, (_, index) => index + 1);
    expect(
      backend.executeBatch(
        state,
        reducer,
        [
          { data: keys, type: ValueType.UINT32 },
          { data: Uint32Array.from(keys, (key) => key * 10), type: ValueType.UINT32 },
        ],
        keys.length,
      ),
    ).toBe(ErrorCode.OK);
    const committed = backend.serialize(state, reducer);
    expect(committed.byteLength).toBeGreaterThan(initial.byteLength);
    for (const key of keys) expect(backend.mapGet(state, reducer, 0, key)).toStrictEqual({ ok: true, value: key * 10 });
    expect(backend.undoEnable(state)).toStrictEqual({ ok: true, value: undefined });
    const checkpoint = backend.undoCheckpoint(state);
    const more = Uint32Array.from({ length: 440 }, (_, index) => index + 161);
    const columns = [
      { data: more, type: ValueType.UINT32 },
      { data: Uint32Array.from(more, (key) => key * 10), type: ValueType.UINT32 },
    ];
    expect(backend.executeBatch(state, reducer, columns, more.length)).toBe(ErrorCode.NEEDS_GROWTH);
    expect(backend.undoCheckpoint(state)).toBe(checkpoint);
    expect(backend.serialize(state, reducer)).toEqual(committed);
    expect(backend.growState(state, reducer)).toBe(ErrorCode.NEEDS_GROWTH);
    backend.undoRollback(state, checkpoint);
    backend.undoCommit(state, checkpoint);
    expect(backend.growState(state, reducer)).toBe(ErrorCode.OK);
    expect(backend.executeBatch(state, reducer, columns, more.length)).toBe(ErrorCode.OK);
    expect(backend.getMapSize(state, reducer, 0)).toBe(keys.length + more.length);
    for (const key of more) expect(backend.mapGet(state, reducer, 0, key)).toStrictEqual({ ok: true, value: key * 10 });
  });

  it('uploads the selected handle before rollback after another handle changed the shared image', async () => {
    const backend = await loadColumineWasm(WASM_PATH);
    const reducer = await backend.loadProgram(
      program([Opcode.SLOT_DEF, 0, SlotType.HASHMAP, 8, 0], [Opcode.BATCH_MAP_UPSERT_LAST, 0, 0, 1], 1, 2),
    );
    const selected = backend.createState(reducer);
    const untouched = backend.createState(reducer);
    const selectedBefore = backend.serialize(selected, reducer);
    expect(
      backend.executeBatch(
        untouched,
        reducer,
        [
          { data: new Uint32Array([99]), type: ValueType.UINT32 },
          { data: new Uint32Array([990]), type: ValueType.UINT32 },
        ],
        1,
      ),
    ).toBe(ErrorCode.OK);
    expect(backend.undoEnable(selected)).toStrictEqual({ ok: true, value: undefined });
    const checkpoint = backend.undoCheckpoint(selected);
    expect(
      backend.executeBatch(
        selected,
        reducer,
        [
          { data: new Uint32Array([7]), type: ValueType.UINT32 },
          { data: new Uint32Array([70]), type: ValueType.UINT32 },
        ],
        1,
      ),
    ).toBe(ErrorCode.OK);
    const untouchedBefore = backend.serialize(untouched, reducer);
    expect(backend.mapGet(untouched, reducer, 0, 7)).toStrictEqual({ ok: true, value: undefined });
    backend.undoRollback(selected, checkpoint);
    expect(backend.serialize(selected, reducer)).toEqual(selectedBefore);
    expect(backend.mapGet(selected, reducer, 0, 7)).toStrictEqual({ ok: true, value: undefined });
    expect(backend.serialize(untouched, reducer)).toEqual(untouchedBefore);
  });

  it('uses immutable absent results and refuses corrupt geometry instead of reporting absence', async () => {
    const backend = await loadColumineWasm(WASM_PATH);
    const reducer = await backend.loadProgram(
      program([Opcode.SLOT_STRUCT_MAP, 0, SlotType.STRUCT_MAP, 8, 0, 1, StructFieldType.ARRAY_U32], [], 1, 0),
    );
    const state = backend.createState(reducer);
    const absent = backend.structMapArenaField(state, reducer, 0, 7, 0);
    expect(absent.kind).toBe('absent');
    expect(Object.isFrozen(absent)).toBe(true);
    expect(backend.structMapArenaField(state, reducer, 0, 8, 0)).toBe(absent);
    // Corrupt slot geometry is refused where the image enters the VM, before
    // any reader could take it for absence.
    const corrupt = backend.serialize(state, reducer);
    new DataView(corrupt.buffer, corrupt.byteOffset, corrupt.byteLength).setUint32(36, 3, true);
    expect(() => backend.deserialize(reducer, corrupt)).toThrow(VmStateAdmissionError);
  });

  it('reuses immutable unset results without collapsing a present empty arena field', async () => {
    const backend = await loadColumineWasm(WASM_PATH);
    const scalarOnly = await backend.loadProgram(
      program(
        [Opcode.SLOT_STRUCT_MAP, 0, SlotType.STRUCT_MAP, 8, 0, 2, StructFieldType.UINT32, StructFieldType.ARRAY_U32],
        [Opcode.BATCH_STRUCT_MAP_UPSERT_LAST, 0, 0, 1, 1, 0, 0],
        1,
        2,
      ),
    );
    const state = backend.createState(scalarOnly);
    expect(
      backend.executeBatch(
        state,
        scalarOnly,
        [
          { data: new Uint32Array([7]), type: ValueType.UINT32 },
          { data: new Uint32Array([70]), type: ValueType.UINT32 },
        ],
        1,
      ),
    ).toBe(ErrorCode.OK);
    const unset = backend.structMapArenaField(state, scalarOnly, 0, 7, 1);
    expect(unset.kind).toBe('unset');
    expect(Object.isFrozen(unset)).toBe(true);
    expect(backend.structMapArenaField(state, scalarOnly, 0, 7, 1)).toBe(unset);
    const emptyArray = await backend.loadProgram(
      program(
        [Opcode.SLOT_STRUCT_MAP, 0, SlotType.STRUCT_MAP, 8, 0, 1, StructFieldType.ARRAY_U32],
        [Opcode.BATCH_STRUCT_MAP_UPSERT_LAST, 0, 0, 0, 1, 1, 2, 0],
        1,
        3,
      ),
    );
    const emptyState = backend.createState(emptyArray);
    expect(
      backend.executeBatch(
        emptyState,
        emptyArray,
        [
          { data: new Uint32Array([7]), type: ValueType.UINT32 },
          { data: new Uint32Array([0, 0]), type: ValueType.UINT32 },
          { data: new Uint32Array(), type: ValueType.UINT32 },
        ],
        1,
      ),
    ).toBe(ErrorCode.OK);
    expect(backend.structMapArenaField(emptyState, emptyArray, 0, 7, 0)).toEqual({
      kind: 'bytes',
      bytes: new Uint8Array(),
    });
  });

  it('bounds real wasm growth, arena reads, delta preflight and rollback outputs before mutation', async () => {
    const bytes = await Bun.file(WASM_PATH).arrayBuffer();
    const imported = new WebAssembly.Memory({ initial: 2 });
    const { instance } = await WebAssembly.instantiate(bytes, { env: { memory: imported } });
    const exported = instance.exports.memory;
    const memory = exported instanceof WebAssembly.Memory ? exported : imported;
    const exports = instance.exports;
    const region = new WasmHostRegion(memory);
    const bytecode = program(
      [Opcode.SLOT_STRUCT_MAP, 0, SlotType.STRUCT_MAP, 8, 0, 1, StructFieldType.ARRAY_U32],
      [],
      1,
      0,
    );
    // Span from base: program at +64 KiB, then 64 KiB of headroom past the program.
    const base = region.reserve(65536 + bytecode.length + 65536);
    const statePtr = base;
    const programPtr = base + 65536;
    new Uint8Array(memory.buffer).set(bytecode, programPtr);
    const size = admittedSize(exports, memory, programPtr, bytecode.length);
    expect(size).toBeGreaterThan(0);
    expect(invoke(exports, 'vm_init_state', statePtr, programPtr, bytecode.length)).toBe(ErrorCode.OK);
    const initial = new Uint8Array(memory.buffer, statePtr, size).slice();
    const outPtr = programPtr + 4096;
    expect(invoke(exports, 'vm_calculate_grown_state_size', statePtr, size, 0, outPtr)).toBe(ErrorCode.OK);
    const grownSize = new DataView(memory.buffer).getUint32(outPtr, true);
    expect(invoke(exports, 'vm_grow_state', statePtr, size, statePtr, grownSize, 0)).toBe(ErrorCode.INVALID_STATE);
    const newPtr = programPtr + 8192;
    expect(region.reserve(newPtr + grownSize - base)).toBe(base);
    expect(invoke(exports, 'vm_grow_state', statePtr, size, newPtr, grownSize - 1, 0)).toBe(ErrorCode.INVALID_STATE);
    expect(new Uint8Array(memory.buffer, statePtr, size)).toEqual(initial);
    expect(invoke(exports, 'vm_struct_map_arena_field', statePtr, 31, 0, 7, 0, outPtr)).toBe(ErrorCode.INVALID_STATE);
    expect(invoke(exports, 'vm_struct_map_arena_field', statePtr, size, 0, 7, 0, statePtr)).toBe(
      ErrorCode.INVALID_STATE,
    );
    expect(invoke(exports, 'vm_struct_map_arena_field', statePtr, size, 0, 7, 0, memory.buffer.byteLength - 4)).toBe(
      ErrorCode.INVALID_STATE,
    );
    expect(invoke(exports, 'vm_undo_enable', statePtr, size)).toBe(ErrorCode.OK);
    const untouchedGrowthDestination = new Uint8Array(memory.buffer, newPtr, grownSize).slice();
    expect(invoke(exports, 'vm_grow_state', statePtr, size, newPtr, grownSize, 0)).toBe(ErrorCode.NEEDS_GROWTH);
    expect(new Uint8Array(memory.buffer, newPtr, grownSize)).toEqual(untouchedGrowthDestination);
    expect(invoke(exports, 'vm_undo_rollback', statePtr, size, 0, statePtr)).toBe(ErrorCode.INVALID_STATE);
    expect(invoke(exports, 'vm_delta_required_state_size', memory.buffer.byteLength - 1, 24, 24, size, outPtr)).toBe(
      ErrorCode.INVALID_STATE,
    );
    expect(invoke(exports, 'vm_delta_apply_rollforward_segment', statePtr, size, statePtr, 24, 24, outPtr)).toBe(
      ErrorCode.INVALID_STATE,
    );
    expect(new Uint8Array(memory.buffer, statePtr, size)).toEqual(initial);
    expect(invoke(exports, 'vm_undo_rollback', statePtr, size, 0, outPtr)).toBe(ErrorCode.OK);
    expect(new DataView(memory.buffer).getUint32(outPtr, true)).toBe(size);
    const commit = exports.vm_undo_commit;
    if (typeof commit !== 'function') throw new Error('real wasm artifact is missing vm_undo_commit');
    commit(statePtr, 0);
    new Uint8Array(memory.buffer, newPtr, grownSize).fill(0);
    expect(invoke(exports, 'vm_grow_state', statePtr, size, newPtr, grownSize, 0)).toBe(ErrorCode.OK);
    expect(new Uint8Array(memory.buffer, statePtr, size)).toEqual(initial);
  });

  it('returns typed journal and TTL-eviction refusal while preserving serialized rollback and replay', async () => {
    const ttl = new Uint8Array(8);
    new DataView(ttl.buffer).setFloat32(0, 10, true);
    for (const delta of [false, true]) {
      const backend = await loadColumineWasm(WASM_PATH);
      const reducer = await backend.loadProgram(
        program(
          [Opcode.SLOT_DEF, 0, SlotType.HASHMAP | SlotTypeFlag.HAS_TTL, 8, 0, ...ttl, 2, 0],
          [Opcode.BATCH_MAP_UPSERT_LAST, 0, 0, 1],
          1,
          3,
        ),
      );
      const state = backend.createState(reducer);
      const execute = (values: Uint32Array) => {
        const columns = [
          { data: new Uint32Array(values.length).fill(7), type: ValueType.UINT32 },
          { data: values, type: ValueType.UINT32 },
          { data: new Float64Array(values.length).fill(1), type: ValueType.FLOAT64 },
        ];
        return delta
          ? backend.executeBatchDelta(state, reducer, columns, values.length)
          : backend.executeBatch(state, reducer, columns, values.length);
      };
      expect(execute(new Uint32Array([30]))).toBe(ErrorCode.OK);
      const committed = backend.serialize(state, reducer);
      expect(backend.undoEnable(state)).toStrictEqual({ ok: true, value: undefined });
      const checkpoint = backend.undoCheckpoint(state);
      expect(execute(new Uint32Array([70]))).toBe(ErrorCode.OK);
      const accepted = backend.serialize(state, reducer);
      const attemptCheckpoint = backend.undoCheckpoint(state);
      expect(attemptCheckpoint).toBeGreaterThan(checkpoint);

      // Every alternating value is a real mutation of the same bounded slot.
      // Exhaust the actual native journal; no fabricated entries or codec.
      const values = Uint32Array.from({ length: 20_000 }, (_, i) => (i % 2 === 0 ? 80 : 90));
      expect(execute(values)).toBe(ErrorCode.JOURNAL_OVERFLOW);
      expect(backend.undoHasOverflow()).toBe(true);
      expect(backend.undoCheckpoint(state)).toBe(attemptCheckpoint);
      expect(backend.serialize(state, reducer)).toEqual(accepted);
      expect(execute(new Uint32Array([100]))).toBe(ErrorCode.JOURNAL_OVERFLOW);
      expect(backend.serialize(state, reducer)).toEqual(accepted);
      expect(backend.evictExpired(state, reducer, 100, delta)).toEqual({
        ok: false,
        error: ErrorCode.JOURNAL_OVERFLOW,
      });
      expect(backend.serialize(state, reducer)).toEqual(accepted);
      expect(backend.undoCheckpoint(state)).toBe(attemptCheckpoint);
      const exported = backend.deltaExportSegment(state, checkpoint, attemptCheckpoint);
      if (!delta) {
        // Undo-only rows have no redo lane: a paired export is refused, never returned as empty.
        expect(exported).toStrictEqual({ ok: false, error: ErrorCode.DELTA_LANE_MISMATCH });
      }

      backend.undoRollback(state, checkpoint);
      expect(backend.serialize(state, reducer)).toEqual(committed);
      backend.undoCommit(state, checkpoint);
      if (delta) {
        if (!exported.ok) throw new Error(`unexpected delta export error ${exported.error}`);
        const segment = exported.value;
        expect(segment.overflow).toBe(true);
        expect(segment.redoBytes.byteLength).toBeGreaterThan(0);
        backend.deltaApplyRollforwardSegment(state, segment.redoBytes, segment.entrySize);
        expect(backend.serialize(state, reducer)).toEqual(accepted);
        backend.deltaApplyRollbackSegment(state, segment.undoBytes, segment.entrySize);
        expect(backend.serialize(state, reducer)).toEqual(committed);
      }
      expect(execute(new Uint32Array([110]))).toBe(ErrorCode.OK);
      expect(backend.mapGet(state, reducer, 0, 7)).toStrictEqual({ ok: true, value: 110 });
      expect(backend.evictExpired(state, reducer, 100, delta)).toMatchObject({ ok: true, count: 1 });
      expect(backend.mapGet(state, reducer, 0, 7)).toStrictEqual({ ok: true, value: undefined });
    }
  });

  it('refuses a paired export spanning an undo-only TTL eviction and replays exact paired images', async () => {
    const ttl = new Uint8Array(8);
    new DataView(ttl.buffer).setFloat32(0, 10, true);
    for (const pairedEviction of [false, true]) {
      const backend = await loadColumineWasm(WASM_PATH);
      const reducer = await backend.loadProgram(
        program(
          [Opcode.SLOT_DEF, 0, SlotType.HASHMAP | SlotTypeFlag.HAS_TTL, 8, 0, ...ttl, 2, 0],
          [Opcode.BATCH_MAP_UPSERT_LAST, 0, 0, 1],
          1,
          3,
        ),
      );
      const state = backend.createState(reducer);
      // One TTL row at key 7 with timestamp 1, which expires before now = 100.
      const insertColumns = [
        { data: new Uint32Array([7]), type: ValueType.UINT32 },
        { data: new Uint32Array([30]), type: ValueType.UINT32 },
        { data: new Float64Array([1]), type: ValueType.FLOAT64 },
      ];
      const updateColumns = [
        { data: new Uint32Array([7]), type: ValueType.UINT32 },
        { data: new Uint32Array([70]), type: ValueType.UINT32 },
        { data: new Float64Array([1]), type: ValueType.FLOAT64 },
      ];
      expect(backend.executeBatch(state, reducer, insertColumns, 1)).toBe(ErrorCode.OK);
      const beforeBegin = backend.serialize(state, reducer);
      expect(backend.undoEnable(state)).toStrictEqual({ ok: true, value: undefined });
      const checkpoint = backend.undoCheckpoint(state);
      expect(backend.executeBatchDelta(state, reducer, updateColumns, 1)).toBe(ErrorCode.OK);
      const middle = backend.undoCheckpoint(state);
      // The paired prefix is complete, so it exports exactly and is the earlier successful selection.
      const pairedPrefix = backend.deltaExportSegment(state, checkpoint, middle);
      if (!pairedPrefix.ok) throw new Error(`unexpected paired prefix export error ${pairedPrefix.error}`);
      expect(pairedPrefix.value.overflow).toBe(false);
      expect(pairedPrefix.value.undoBytes.byteLength).toBeGreaterThan(0);

      if (pairedEviction) {
        expect(backend.evictExpired(state, reducer, 100, true)).toMatchObject({ ok: true, count: 1 });
        const evicted = backend.serialize(state, reducer);
        const end = backend.undoCheckpoint(state);
        const full = backend.deltaExportSegment(state, checkpoint, end);
        if (!full.ok) throw new Error(`unexpected paired eviction export error ${full.error}`);
        expect(full.value.overflow).toBe(false);
        expect(full.value.redoBytes.byteLength).toBe(full.value.undoBytes.byteLength);

        backend.undoRollback(state, checkpoint);
        expect(backend.serialize(state, reducer)).toEqual(beforeBegin);
        backend.undoCommit(state, checkpoint);
        backend.deltaApplyRollforwardSegment(state, full.value.redoBytes, full.value.entrySize);
        expect(backend.serialize(state, reducer)).toEqual(evicted);
        backend.deltaApplyRollbackSegment(state, full.value.undoBytes, full.value.entrySize);
        expect(backend.serialize(state, reducer)).toEqual(beforeBegin);
      } else {
        expect(backend.evictExpired(state, reducer, 100, false)).toMatchObject({ ok: true, count: 1 });
        const end = backend.undoCheckpoint(state);
        // One undo-only row in the requested range refuses the whole paired export with a typed cause.
        expect(backend.deltaExportSegment(state, checkpoint, end)).toStrictEqual({
          ok: false,
          error: ErrorCode.DELTA_LANE_MISMATCH,
        });
        // The refusal leaves the earlier paired prefix exportable with byte-identical lanes.
        expect(backend.deltaExportSegment(state, checkpoint, middle)).toStrictEqual(pairedPrefix);
        backend.undoRollback(state, checkpoint);
        expect(backend.serialize(state, reducer)).toEqual(beforeBegin);
        backend.undoCommit(state, checkpoint);
      }
    }
  });

  it('refused raw paired export after undo-only eviction leaves cursor, lanes and out unchanged', async () => {
    const wasmBytes = await Bun.file(WASM_PATH).arrayBuffer();
    const imported = new WebAssembly.Memory({ initial: 2 });
    const { instance } = await WebAssembly.instantiate(wasmBytes, { env: { memory: imported } });
    const exportedMemory = instance.exports.memory;
    const memory = exportedMemory instanceof WebAssembly.Memory ? exportedMemory : imported;
    const exports = instance.exports;
    const region = new WasmHostRegion(memory);
    // Span from base: program at +64 KiB, tables and data after it, then 64 KiB of headroom past dataStart.
    const base = region.reserve(65536 + 4096 + 64 + 65536);
    const statePtr = base;
    const programPtr = base + 65536;
    const pointersPtr = programPtr + 4096;
    const lengthsPtr = pointersPtr + 32;
    const dataStart = lengthsPtr + 32;
    const outPtr = dataStart - 16;
    const ttl = new Uint8Array(8);
    new DataView(ttl.buffer).setFloat32(0, 10, true);
    const bytecode = program(
      [Opcode.SLOT_DEF, 0, SlotType.HASHMAP | SlotTypeFlag.HAS_TTL, 8, 0, ...ttl, 2, 0],
      [Opcode.BATCH_MAP_UPSERT_LAST, 0, 0, 1],
      1,
      3,
    );
    new Uint8Array(memory.buffer).set(bytecode, programPtr);
    const size = admittedSize(exports, memory, programPtr, bytecode.length);
    expect(size).toBeGreaterThan(0);
    new Uint8Array(memory.buffer, statePtr, size).fill(0);
    expect(invoke(exports, 'vm_init_state', statePtr, programPtr, bytecode.length)).toBe(ErrorCode.OK);

    // Columns follow the program's indexes (key, value, TTL timestamp); each starts on an 8-byte boundary.
    const execute = (delta: boolean, columns: readonly ArrayBufferView[], batchLen: number) => {
      const view = new DataView(memory.buffer);
      let at = dataStart;
      columns.forEach((column, i) => {
        at = Math.ceil(at / 8) * 8;
        new Uint8Array(memory.buffer).set(new Uint8Array(column.buffer, column.byteOffset, column.byteLength), at);
        view.setUint32(pointersPtr + i * 4, at, true);
        view.setUint32(lengthsPtr + i * 4, column.byteLength, true);
        at += column.byteLength;
      });
      return invoke(
        exports,
        delta ? 'vm_execute_batch_delta' : 'vm_execute_batch',
        statePtr,
        size,
        programPtr,
        bytecode.length,
        pointersPtr,
        lengthsPtr,
        columns.length,
        batchLen,
      );
    };
    expect(execute(false, [new Uint32Array([7]), new Uint32Array([30]), new Float64Array([1])], 1)).toBe(ErrorCode.OK);
    expect(invoke(exports, 'vm_undo_enable', statePtr, size)).toBe(ErrorCode.OK);
    const checkpoint = invoke(exports, 'vm_undo_checkpoint', statePtr);
    expect(execute(true, [new Uint32Array([7]), new Uint32Array([70]), new Float64Array([1])], 1)).toBe(ErrorCode.OK);
    const middle = invoke(exports, 'vm_undo_checkpoint', statePtr);
    expect(middle).toBeGreaterThan(checkpoint);

    // Every raw selection output: cursor pointers, length, lane bytes, diagnostics and the caller's out record.
    const selection = () => {
      const len = invoke(exports, 'vm_delta_export_len_bytes');
      const undoPtr = invoke(exports, 'vm_delta_export_undo_ptr');
      const redoPtr = invoke(exports, 'vm_delta_export_redo_ptr');
      return {
        undoPtr,
        redoPtr,
        len,
        entrySize: invoke(exports, 'vm_delta_export_entry_size'),
        overflow: invoke(exports, 'vm_delta_export_overflow'),
        undo: new Uint8Array(memory.buffer, undoPtr, len).slice(),
        redo: new Uint8Array(memory.buffer, redoPtr, len).slice(),
        out: new Uint8Array(memory.buffer, outPtr, 4).slice(),
      };
    };
    expect(invoke(exports, 'vm_delta_export_segment', statePtr, size, checkpoint, middle, outPtr)).toBe(ErrorCode.OK);
    expect(new DataView(memory.buffer).getUint32(outPtr, true)).toBe(middle - checkpoint);
    const prefix = selection();
    expect(prefix.len).toBeGreaterThan(0);
    expect(prefix.entrySize).toBe(24);
    expect(prefix.overflow).toBe(0);

    // Undo-only eviction on the ordinary lane (delta_mode 0) appends a record with no redo partner after `middle`.
    expect(invoke(exports, 'vm_evict_all_expired', statePtr, size, 100, 0)).toBe(ErrorCode.OK);
    expect(invoke(exports, 'vm_get_evicted_count')).toBe(1);
    const end = invoke(exports, 'vm_undo_checkpoint', statePtr);
    expect(end).toBeGreaterThan(middle);

    // Seed the out record so an untouched record is distinguishable from a written zero.
    new Uint8Array(memory.buffer, outPtr, 4).fill(0xa5);
    const beforeRefusal = selection();
    expect(beforeRefusal.undoPtr).toBe(prefix.undoPtr);
    expect(beforeRefusal.redoPtr).toBe(prefix.redoPtr);
    expect(beforeRefusal.len).toBe(prefix.len);
    expect(beforeRefusal.undo).toStrictEqual(prefix.undo);
    expect(beforeRefusal.redo).toStrictEqual(prefix.redo);
    expect(beforeRefusal.out).toStrictEqual(new Uint8Array([0xa5, 0xa5, 0xa5, 0xa5]));

    // The full range contains the undo-only record, so the native refusal is the typed DELTA_LANE_MISMATCH (13).
    expect(invoke(exports, 'vm_delta_export_segment', statePtr, size, checkpoint, end, outPtr)).toBe(
      ErrorCode.DELTA_LANE_MISMATCH,
    );
    expect(selection()).toStrictEqual(beforeRefusal);

    // Only after the comparison above: the earlier paired prefix still re-exports with identical lane bytes.
    expect(invoke(exports, 'vm_delta_export_segment', statePtr, size, checkpoint, middle, outPtr)).toBe(ErrorCode.OK);
    expect(new DataView(memory.buffer).getUint32(outPtr, true)).toBe(middle - checkpoint);
    const reexported = selection();
    expect(reexported.undo).toStrictEqual(prefix.undo);
    expect(reexported.redo).toStrictEqual(prefix.redo);
  });

  it('refuses malformed reader geometry distinctly from real absence and a legal maximum mapped value', async () => {
    const backend = await loadColumineWasm(WASM_PATH);
    const reducer = await backend.loadProgram(
      program(
        [
          Opcode.SLOT_DEF,
          0,
          SlotType.HASHMAP | SlotTypeFlag.NO_HASHMAP_TIMESTAMPS,
          4,
          0,
          Opcode.SLOT_DEF,
          1,
          SlotType.HASHSET,
          4,
          0,
          Opcode.SLOT_STRUCT_MAP2,
          2,
          SlotType.STRUCT_MAP2,
          4,
          0,
          1,
          StructFieldType.UINT32,
        ],
        [
          Opcode.BATCH_MAP_UPSERT_LAST,
          0,
          0,
          1,
          Opcode.BATCH_SET_INSERT,
          1,
          0,
          Opcode.BATCH_STRUCT_MAP2_UPSERT_LAST,
          2,
          0,
          2,
          1,
          3,
          0,
        ],
        3,
        4,
      ),
    );
    const state = backend.createState(reducer);
    expect(backend.mapGet(state, reducer, 0, 7)).toStrictEqual({ ok: true, value: undefined });
    expect(backend.setContains(state, reducer, 1, 7)).toStrictEqual({ ok: true, value: false });
    expect(backend.structMap2GetRow(state, reducer, 2, 7, 41)).toStrictEqual({ ok: true, value: undefined });
    expect(backend.structMap2Entries(state, reducer, 2)).toStrictEqual({ ok: true, value: [] });
    expect(
      backend.executeBatch(
        state,
        reducer,
        [
          { data: new Uint32Array([7]), type: ValueType.UINT32 },
          { data: new Uint32Array([0xffff_ffff]), type: ValueType.UINT32 },
          { data: new Uint32Array([41]), type: ValueType.UINT32 },
          { data: new Uint32Array([99]), type: ValueType.UINT32 },
        ],
        1,
      ),
    ).toBe(ErrorCode.OK);
    expect(backend.mapGet(state, reducer, 0, 7)).toStrictEqual({ ok: true, value: 0xffff_ffff });
    expect(backend.setContains(state, reducer, 1, 7)).toStrictEqual({ ok: true, value: true });
    const row = backend.structMap2GetRow(state, reducer, 2, 7, 41);
    if (!row.ok || row.value === undefined) throw new Error('the real composite control row must exist');
    expect(new DataView(backend.serialize(state, reducer).buffer).getUint32(row.value.rowOffset + 1, true)).toBe(99);
    expect(backend.structMap2Entries(state, reducer, 2)).toStrictEqual({ ok: true, value: [row.value] });
    const committed = backend.serialize(state, reducer);
    // Malformed reader geometry (a capacity of 3, an image one byte short) is
    // refused where the image enters the VM, never read as absence.
    for (const slot of [0, 1, 2]) {
      const malformed = committed.slice();
      new DataView(malformed.buffer).setUint32(32 + slot * 48 + 4, 3, true);
      expect(() => backend.deserialize(reducer, malformed)).toThrow(VmStateAdmissionError);
    }
    expect(() => backend.deserialize(reducer, committed.slice(0, -1))).toThrow(VmStateAdmissionError);
    expect(backend.mapGet(state, reducer, -1, 7)).toStrictEqual({ ok: false, error: ErrorCode.INVALID_SLOT });
    expect(backend.structMap2GetRow(state, reducer, 2, 7.5, 41)).toStrictEqual({
      ok: false,
      error: ErrorCode.INVALID_KEY,
    });

    const bitmapProgram = await backend.loadProgram(program([Opcode.SLOT_DEF, 0, SlotType.BITMAP, 16, 0], [], 1, 0));
    const bitmap = backend.createState(bitmapProgram);
    expect(backend.setContains(bitmap, bitmapProgram, 0, 7)).toStrictEqual({ ok: true, value: false });
    const badBitmap = backend.serialize(bitmap, bitmapProgram);
    new DataView(badBitmap.buffer).setUint32(36, 3, true);
    expect(() => backend.deserialize(bitmapProgram, badBitmap)).toThrow(VmStateAdmissionError);
  });

  it('propagates nested element journal refusal with an exact real-WASM accepted prefix', async () => {
    const wasmBytes = await Bun.file(WASM_PATH).arrayBuffer();
    for (const flat of [false, true]) {
      for (const delta of [false, true]) {
        const imported = new WebAssembly.Memory({ initial: 2 });
        const { instance } = await WebAssembly.instantiate(wasmBytes, { env: { memory: imported } });
        const exportedMemory = instance.exports.memory;
        const memory = exportedMemory instanceof WebAssembly.Memory ? exportedMemory : imported;
        const exports = instance.exports;
        const region = new WasmHostRegion(memory);
        // Span from base: program at +64 KiB, tables and data after it, then 70000 bytes of headroom past dataStart.
        const base = region.reserve(65536 + 4096 + 64 + 70000);
        const statePtr = base;
        const programPtr = base + 65536;
        const pointersPtr = programPtr + 4096;
        const lengthsPtr = pointersPtr + 32;
        const dataStart = lengthsPtr + 32;
        const outPtr = dataStart - 16;
        const init = [
          Opcode.SLOT_DEF,
          0,
          SlotType.HASHMAP | SlotTypeFlag.NO_HASHMAP_TIMESTAMPS,
          0,
          2,
          Opcode.SLOT_DEF,
          1,
          SlotType.AGGREGATE,
          AggType.COUNT,
          0,
        ];
        const typeId = 1001;
        const countBody = [Opcode.BATCH_AGG_COUNT, 1];
        const fillProgram = program(
          init,
          [
            Opcode.FOR_EACH,
            0,
            1,
            ...new Uint8Array(new Uint32Array([typeId]).buffer),
            countBody.length,
            0,
            ...countBody,
          ],
          2,
          1,
        );
        const rowBody = [Opcode.BATCH_MAP_UPSERT_LAST, 0, flat ? 2 : 1, flat ? 3 : 2];
        let body = rowBody;
        if (flat) {
          body = [Opcode.FLAT_MAP, 1];
          appendIndex(body, NO_PARENT_TS_COL);
          body.push(rowBody.length, 0, ...rowBody);
        }
        const nestedProgram = program(
          init,
          [Opcode.FOR_EACH, 0, 1, ...new Uint8Array(new Uint32Array([typeId]).buffer), body.length, 0, ...body],
          2,
          flat ? 4 : 3,
        );
        const execute = (bytecode: Uint8Array, columns: readonly Uint32Array[], batchLen: number) => {
          const end = dataStart + columns.reduce((sum, column) => sum + column.byteLength, 0) + 4096;
          expect(region.reserve(end - base)).toBe(base);
          new Uint8Array(memory.buffer).set(bytecode, programPtr);
          const view = new DataView(memory.buffer);
          let at = dataStart;
          columns.forEach((column, i) => {
            new Uint8Array(memory.buffer).set(new Uint8Array(column.buffer, column.byteOffset, column.byteLength), at);
            view.setUint32(pointersPtr + i * 4, at, true);
            view.setUint32(lengthsPtr + i * 4, column.byteLength, true);
            at += column.byteLength;
          });
          return invoke(
            exports,
            delta ? 'vm_execute_batch_delta' : 'vm_execute_batch',
            statePtr,
            size,
            programPtr,
            bytecode.length,
            pointersPtr,
            lengthsPtr,
            columns.length,
            batchLen,
          );
        };
        new Uint8Array(memory.buffer).set(fillProgram, programPtr);
        const size = admittedSize(exports, memory, programPtr, fillProgram.length);
        expect(size).toBeGreaterThan(0);
        new Uint8Array(memory.buffer, statePtr, size).fill(0);
        expect(invoke(exports, 'vm_init_state', statePtr, programPtr, fillProgram.length)).toBe(ErrorCode.OK);
        const initial = new Uint8Array(memory.buffer, statePtr, size).slice();
        expect(invoke(exports, 'vm_undo_enable', statePtr, size)).toBe(ErrorCode.OK);
        // A COUNT batch folds all its rows into one accumulated journal record (one batched fill measured checkpoint 3,
        // not 16288), so the filler is successive one-row executions; each success's journal progress is measured, never assumed.
        const oneRow = [new Uint32Array([typeId])];
        let checkpoint = invoke(exports, 'vm_undo_checkpoint', statePtr);
        while (checkpoint < 16384 - 96) {
          expect(execute(fillProgram, oneRow, 1)).toBe(ErrorCode.OK);
          const next = invoke(exports, 'vm_undo_checkpoint', statePtr);
          expect(next).toBeGreaterThan(checkpoint);
          expect(next).toBeLessThanOrEqual(16384 - 96);
          checkpoint = next;
        }
        expect(checkpoint).toBe(16384 - 96);
        const before = new Uint8Array(memory.buffer, statePtr, size).slice();
        const keys = Uint32Array.from({ length: 128 }, (_, i) => i + 1);
        const values = Uint32Array.from(keys, (key) => key * 10);
        const columns = flat
          ? [new Uint32Array([typeId]), new Uint32Array([0, 128]), keys, values]
          : [new Uint32Array(128).fill(typeId), keys, values];
        expect(execute(nestedProgram, columns, flat ? 1 : 128)).toBe(ErrorCode.JOURNAL_OVERFLOW);
        expect(invoke(exports, 'vm_undo_has_overflow')).toBe(1);
        const end = invoke(exports, 'vm_undo_checkpoint', statePtr);
        expect(end).toBeGreaterThan(checkpoint);
        const after = new Uint8Array(memory.buffer, statePtr, size).slice();
        const accepted = new DataView(after.buffer).getUint32(32 + 8, true);
        expect(accepted).toBeGreaterThan(0);
        expect(accepted).toBeLessThan(128);
        for (const key of keys) {
          expect(invoke(exports, 'vm_map_get', statePtr, size, 0, key, outPtr)).toBe(ErrorCode.OK);
          const record = new DataView(memory.buffer, outPtr, 8);
          expect(record.getUint32(0, true)).toBe(key <= accepted ? 1 : 0);
          if (key <= accepted) expect(record.getUint32(4, true)).toBe(key * 10);
        }
        const exported = {
          undo: new Uint8Array(),
          redo: new Uint8Array(),
        };
        if (delta) {
          expect(invoke(exports, 'vm_delta_export_segment', statePtr, size, checkpoint, end, outPtr)).toBe(
            ErrorCode.OK,
          );
          expect(new DataView(memory.buffer).getUint32(outPtr, true)).toBe(end - checkpoint);
          const bytes = invoke(exports, 'vm_delta_export_len_bytes');
          exported.undo = new Uint8Array(memory.buffer, invoke(exports, 'vm_delta_export_undo_ptr'), bytes).slice();
          exported.redo = new Uint8Array(memory.buffer, invoke(exports, 'vm_delta_export_redo_ptr'), bytes).slice();
        }
        expect(invoke(exports, 'vm_undo_rollback', statePtr, size, checkpoint, outPtr)).toBe(ErrorCode.OK);
        expect(new Uint8Array(memory.buffer, statePtr, size)).toEqual(before);
        expect(execute(nestedProgram, columns, flat ? 1 : 128)).toBe(ErrorCode.JOURNAL_OVERFLOW);
        expect(new Uint8Array(memory.buffer, statePtr, size)).toEqual(before);
        if (delta) {
          const segmentPtr = dataStart;
          new Uint8Array(memory.buffer).set(exported.redo, segmentPtr);
          expect(
            invoke(
              exports,
              'vm_delta_apply_rollforward_segment',
              statePtr,
              size,
              segmentPtr,
              exported.redo.length,
              24,
              outPtr,
            ),
          ).toBe(ErrorCode.OK);
          expect(new Uint8Array(memory.buffer, statePtr, size)).toEqual(after);
          new Uint8Array(memory.buffer).set(exported.undo, segmentPtr);
          expect(
            invoke(
              exports,
              'vm_delta_apply_rollback_segment',
              statePtr,
              size,
              segmentPtr,
              exported.undo.length,
              24,
              outPtr,
            ),
          ).toBe(ErrorCode.OK);
          expect(new Uint8Array(memory.buffer, statePtr, size)).toEqual(before);
        }
        expect(invoke(exports, 'vm_undo_rollback', statePtr, size, 0, outPtr)).toBe(ErrorCode.OK);
        expect(new Uint8Array(memory.buffer, statePtr, size)).toEqual(initial);
      }
    }
  });
});
