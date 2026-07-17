/**
 * WASM Backend - Standalone columine loader (reducer-only binary)
 *
 * Architecture:
 *   - ONE shared WASM instance per CPU (not per agent)
 *   - State lives in JS ArrayBuffer per agent instance (GC'd)
 *   - State copied INTO WASM memory before reduce
 *   - State copied OUT of WASM memory after reduce
 *   - Input columns copied into WASM memory for each batch
 *
 * Why a separate loader?
 *   When columine is used standalone (without axe-runtime), it loads its own
 *   smaller reducer-only WASM binary. When used via axe-runtime, the superset
 *   binary is injected via setBackend() and this loader is never called.
 */

import { parseReducerProgram } from './reducer-bytecode.js';
import type {
  ColumineBackend,
  ColumnInput,
  EvictedRow,
  ReducerProgram,
  ScalarValue,
  StateHandle,
  StructMap2RowRef,
} from './types.js';
import { AggType, ErrorCode, SlotType, SlotTypeFlag } from './types.js';

// =============================================================================
// Constants
// =============================================================================

const STATE_HEADER_SIZE = 32;
// Must match vm.zig SLOT_META_SIZE (48 bytes with TTL/eviction fields)
const SLOT_META_SIZE = 48;
const EVICTION_ENTRY_SIZE = 16;
const WASM_PAGE_SIZE = 64 * 1024;
const STATE_REGION_OFFSET = WASM_PAGE_SIZE;

// WASM returns u32 as signed i32, so 0xFFFFFFFF becomes -1
const EMPTY_KEY_SIGNED = -1;

// =============================================================================
// State Handle - wraps JS ArrayBuffer
// =============================================================================

interface WasmStateHandle extends StateHandle {
  /** The actual state data - owned by JS, GC'd when dropped */
  buffer: ArrayBuffer;
  /** Size in bytes */
  size: number;
}

// =============================================================================
// WASM Exports Interface (reducer-only, no RETE)
// =============================================================================

interface VmExports {
  vm_calculate_state_size(programPtr: number, programLen: number): number;
  vm_init_state(statePtr: number, programPtr: number, programLen: number): number;
  vm_reset_state(statePtr: number, programPtr: number, programLen: number): number;
  vm_get_needs_growth_slot(): number;
  vm_calculate_grown_state_size(oldStatePtr: number, programPtr: number, programLen: number, grownSlot: number): number;
  vm_grow_state(
    oldStatePtr: number,
    newStatePtr: number,
    programPtr: number,
    programLen: number,
    grownSlot: number,
  ): number;
  vm_execute_batch(
    stateBase: number,
    programPtr: number,
    programLen: number,
    colPtrsPtr: number,
    numCols: number,
    batchLen: number,
  ): number;
  vm_execute_batch_delta(
    stateBase: number,
    programPtr: number,
    programLen: number,
    colPtrsPtr: number,
    numCols: number,
    batchLen: number,
  ): number;
  vm_evict_all_expired(stateBase: number, now: number): number;
  vm_get_evicted_count(): number;
  vm_map_get(stateBase: number, slotOffset: number, capacity: number, key: number): number;
  vm_set_contains(stateBase: number, slotOffset: number, capacity: number, elem: number): number;
  vm_struct_map2_get_row_ptr(
    stateBase: number,
    slotOffset: number,
    capacity: number,
    numFields: number,
    rowSize: number,
    key1: number,
    key2: number,
  ): number;
  vm_struct_map2_iter_start(stateBase: number, slotOffset: number, capacity: number, numFields: number): number;
  vm_struct_map2_iter_next(
    stateBase: number,
    slotOffset: number,
    capacity: number,
    numFields: number,
    current: number,
  ): number;
  vm_struct_map2_iter_key1(stateBase: number, slotOffset: number, numFields: number, pos: number): number;
  vm_struct_map2_iter_key2(
    stateBase: number,
    slotOffset: number,
    capacity: number,
    numFields: number,
    pos: number,
  ): number;
  vm_undo_enable(stateBase: number, stateSize: number): void;
  vm_undo_checkpoint(stateBase: number): number;
  vm_undo_rollback(stateBase: number, checkpointPos: number): void;
  vm_undo_commit(stateBase: number, checkpointPos: number): void;
  vm_undo_has_overflow(): number;
  vm_delta_export_segment(stateBase: number, fromPos: number, toPos: number): number;
  vm_delta_export_undo_ptr(): number;
  vm_delta_export_redo_ptr(): number;
  vm_delta_export_len_bytes(): number;
  vm_delta_export_entry_size(): number;
  vm_delta_export_overflow(): number;
  vm_delta_apply_rollback_segment(
    stateBase: number,
    segmentPtr: number,
    segmentLenBytes: number,
    entrySize: number,
  ): void;
  vm_delta_apply_rollforward_segment(
    stateBase: number,
    segmentPtr: number,
    segmentLenBytes: number,
    entrySize: number,
  ): void;
}

// =============================================================================
// Shared WASM Instance
// =============================================================================

interface WasmInstance {
  memory: WebAssembly.Memory;
  exports: VmExports;
  stateRegionOffset: number;
}

function isWasmFunction<T extends (...args: never[]) => unknown>(value: unknown): value is T {
  return typeof value === 'function';
}

const VM_EXPORT_NAMES = [
  'vm_calculate_state_size',
  'vm_init_state',
  'vm_reset_state',
  'vm_get_needs_growth_slot',
  'vm_calculate_grown_state_size',
  'vm_grow_state',
  'vm_execute_batch',
  'vm_execute_batch_delta',
  'vm_evict_all_expired',
  'vm_get_evicted_count',
  'vm_map_get',
  'vm_set_contains',
  'vm_struct_map2_get_row_ptr',
  'vm_struct_map2_iter_start',
  'vm_struct_map2_iter_next',
  'vm_struct_map2_iter_key1',
  'vm_struct_map2_iter_key2',
  'vm_undo_enable',
  'vm_undo_checkpoint',
  'vm_undo_rollback',
  'vm_undo_commit',
  'vm_undo_has_overflow',
  'vm_delta_export_segment',
  'vm_delta_export_undo_ptr',
  'vm_delta_export_redo_ptr',
  'vm_delta_export_len_bytes',
  'vm_delta_export_entry_size',
  'vm_delta_export_overflow',
  'vm_delta_apply_rollback_segment',
  'vm_delta_apply_rollforward_segment',
] as const;

function hasVmExports(
  exports: WebAssembly.Instance['exports'],
): exports is WebAssembly.Instance['exports'] & VmExports {
  return VM_EXPORT_NAMES.every((name) => isWasmFunction(exports[name]));
}

function parseVmExports(exports: WebAssembly.Instance['exports']): VmExports {
  if (!hasVmExports(exports)) {
    const missing = VM_EXPORT_NAMES.find((name) => !isWasmFunction(exports[name]));
    throw new Error(`WASM module missing VM export: ${missing ?? 'unknown'}`);
  }
  return exports;
}

function getExportedMemory(exports: WebAssembly.Instance['exports']): WebAssembly.Memory | null {
  return exports.memory instanceof WebAssembly.Memory ? exports.memory : null;
}

function createWasmStateHandle(buffer: ArrayBuffer, size: number): WasmStateHandle {
  return {
    _brand: 'ColumineStateHandle',
    buffer,
    size,
  };
}

function assertWasmStateHandle(state: StateHandle): WasmStateHandle {
  if (!isWasmStateHandle(state)) {
    // invariant throw: backend received a foreign or corrupted state handle
    throw new Error('Invalid Columine state handle for WASM backend');
  }
  return state;
}

function isWasmStateHandle(state: StateHandle): state is WasmStateHandle {
  return (
    typeof state === 'object' &&
    state !== null &&
    'buffer' in state &&
    state.buffer instanceof ArrayBuffer &&
    'size' in state &&
    typeof state.size === 'number' &&
    Number.isInteger(state.size) &&
    state.size >= 0
  );
}

function align8(n: number): number {
  return Math.ceil(n / 8) * 8;
}

/**
 * Map a raw non-OK status from the WASM VM to its ErrorCode member.
 *
 * Exhaustive switch instead of an `as ErrorCode` narrowing (same pattern as
 * parse-backend's compactStatusCode): a status outside the enum means the TS
 * ErrorCode mirror and the WASM binary disagree — a contract violation, so it
 * throws a teaching error instead of manufacturing a bogus enum member.
 */
function vmErrorCode(status: number): ErrorCode {
  switch (status) {
    case ErrorCode.CAPACITY_EXCEEDED:
      return ErrorCode.CAPACITY_EXCEEDED;
    case ErrorCode.INVALID_PROGRAM:
      return ErrorCode.INVALID_PROGRAM;
    case ErrorCode.INVALID_SLOT:
      return ErrorCode.INVALID_SLOT;
    case ErrorCode.INVALID_STATE:
      return ErrorCode.INVALID_STATE;
    case ErrorCode.NEEDS_GROWTH:
      return ErrorCode.NEEDS_GROWTH;
    case ErrorCode.ARENA_OVERFLOW:
      return ErrorCode.ARENA_OVERFLOW;
    case ErrorCode.INVALID_KEY:
      return ErrorCode.INVALID_KEY;
    default:
      throw new Error(
        `WASM VM returned unknown status ${status}: the TypeScript ErrorCode enum is out of sync ` +
          'with the loaded columine.wasm binary. Rebuild the wasm artifact or update ErrorCode in types.ts.',
      );
  }
}

// =============================================================================
// WASM Backend Factory
// =============================================================================

/**
 * Create columine WASM backend with ONE shared instance.
 * Returns ColumineBackend (reducer-only, no RETE).
 *
 * @param wasmBytes - WASM module bytes
 * @param memoryPages - WASM memory size in 64KB pages (default: 256 = 16MB)
 */
export async function createColumineWasmBackend(wasmBytes: BufferSource, memoryPages = 256): Promise<ColumineBackend> {
  const importedMemory = new WebAssembly.Memory({ initial: memoryPages });
  const wasmModule = await WebAssembly.compile(wasmBytes);
  const instance = await WebAssembly.instantiate(wasmModule, { env: { memory: importedMemory } });
  const exports = parseVmExports(instance.exports);
  const memory = getExportedMemory(instance.exports) ?? importedMemory;
  const currentPages = memory.buffer.byteLength / WASM_PAGE_SIZE;
  if (currentPages < memoryPages) memory.grow(memoryPages - currentPages);

  const wasmInstance: WasmInstance = {
    memory,
    exports,
    stateRegionOffset: STATE_REGION_OFFSET,
  };

  const ensureMemory = (endExclusive: number): void => {
    if (!Number.isSafeInteger(endExclusive) || endExclusive < 0) {
      throw new RangeError(`Invalid WASM memory extent: ${endExclusive}`);
    }
    const missing = endExclusive - wasmInstance.memory.buffer.byteLength;
    if (missing > 0) wasmInstance.memory.grow(Math.ceil(missing / WASM_PAGE_SIZE));
  };

  const copyStateIn = (state: WasmStateHandle): number => {
    const statePtr = wasmInstance.stateRegionOffset;
    ensureMemory(statePtr + state.size);
    new Uint8Array(wasmInstance.memory.buffer).set(new Uint8Array(state.buffer), statePtr);
    return statePtr;
  };

  const prepareProgramAfterState = (stateSize: number, program: ReducerProgram): number => {
    const programPtr = align8(wasmInstance.stateRegionOffset + stateSize);
    ensureMemory(programPtr + program.bytecode.byteLength);
    new Uint8Array(wasmInstance.memory.buffer).set(program.bytecode, programPtr);
    return programPtr;
  };

  // Reusable scratch buffer for column pointer array in executeBatch.
  // Avoids per-batch allocation on the hot reduce path. Per-instance (not
  // module-level) so no state is shared across backend instances.
  let scratchColPtrs: Uint32Array | null = null;
  let scratchColPtrsWidth = 0;

  const prepareExecution = (
    state: WasmStateHandle,
    program: ReducerProgram,
    columns: ColumnInput[],
  ): { statePtr: number; programPtr: number; colPtrsPtr: number } => {
    const statePtr = wasmInstance.stateRegionOffset;
    const programPtr = align8(statePtr + state.size);
    let cursor = align8(programPtr + program.bytecode.byteLength);
    for (const column of columns) cursor = align8(cursor + column.data.byteLength);
    const colPtrsPtr = cursor;
    ensureMemory(colPtrsPtr + columns.length * Uint32Array.BYTES_PER_ELEMENT);

    const wasmU8 = new Uint8Array(wasmInstance.memory.buffer);
    const wasmU32 = new Uint32Array(wasmInstance.memory.buffer);
    wasmU8.set(new Uint8Array(state.buffer), statePtr);
    wasmU8.set(program.bytecode, programPtr);
    cursor = align8(programPtr + program.bytecode.byteLength);
    if (!scratchColPtrs || scratchColPtrsWidth < columns.length) {
      scratchColPtrs = new Uint32Array(columns.length);
      scratchColPtrsWidth = columns.length;
    }
    for (let i = 0; i < columns.length; i++) {
      const data = columns[i].data;
      const bytes = new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
      wasmU8.set(bytes, cursor);
      scratchColPtrs[i] = cursor;
      cursor = align8(cursor + bytes.byteLength);
    }
    for (let i = 0; i < columns.length; i++) {
      wasmU32[colPtrsPtr / 4 + i] = scratchColPtrs[i];
    }
    return { statePtr, programPtr, colPtrsPtr };
  };

  const growFromAuthority = (
    state: WasmStateHandle,
    program: ReducerProgram,
    statePtr: number,
    failedAttemptCheckpoint: number,
  ): number => {
    wasmInstance.exports.vm_undo_rollback(statePtr, failedAttemptCheckpoint);
    const grownSlot = wasmInstance.exports.vm_get_needs_growth_slot();
    if (grownSlot >= program.numSlots) return ErrorCode.INVALID_SLOT;

    ensureMemory(statePtr + state.size);
    new Uint8Array(wasmInstance.memory.buffer).set(new Uint8Array(state.buffer), statePtr);
    const sizeProbeProgramPtr = prepareProgramAfterState(state.size, program);
    const grownSize = wasmInstance.exports.vm_calculate_grown_state_size(
      statePtr,
      sizeProbeProgramPtr,
      program.bytecode.byteLength,
      grownSlot,
    );
    if (grownSize <= state.size) return ErrorCode.INVALID_STATE;

    const newStatePtr = align8(statePtr + state.size);
    const growthProgramPtr = align8(newStatePtr + grownSize);
    ensureMemory(growthProgramPtr + program.bytecode.byteLength);
    const wasmU8 = new Uint8Array(wasmInstance.memory.buffer);
    wasmU8.set(new Uint8Array(state.buffer), statePtr);
    wasmU8.fill(0, newStatePtr, newStatePtr + grownSize);
    wasmU8.set(program.bytecode, growthProgramPtr);
    const result = wasmInstance.exports.vm_grow_state(
      statePtr,
      newStatePtr,
      growthProgramPtr,
      program.bytecode.byteLength,
      grownSlot,
    );
    if (result !== ErrorCode.OK) return result;

    const grownBuffer = new ArrayBuffer(grownSize);
    new Uint8Array(grownBuffer).set(new Uint8Array(wasmInstance.memory.buffer, newStatePtr, grownSize));
    state.buffer = grownBuffer;
    state.size = grownSize;
    return ErrorCode.OK;
  };

  const executeWithGrowth = (
    stateHandle: StateHandle,
    program: ReducerProgram,
    columns: ColumnInput[],
    batchLen: number,
    delta: boolean,
  ): number => {
    const state = assertWasmStateHandle(stateHandle);
    for (;;) {
      const { statePtr, programPtr, colPtrsPtr } = prepareExecution(state, program, columns);
      const checkpoint = wasmInstance.exports.vm_undo_checkpoint(statePtr);
      const result = delta
        ? wasmInstance.exports.vm_execute_batch_delta(
            statePtr,
            programPtr,
            program.bytecode.length,
            colPtrsPtr,
            columns.length,
            batchLen,
          )
        : wasmInstance.exports.vm_execute_batch(
            statePtr,
            programPtr,
            program.bytecode.length,
            colPtrsPtr,
            columns.length,
            batchLen,
          );

      if (result === ErrorCode.NEEDS_GROWTH) {
        const growthResult = growFromAuthority(state, program, statePtr, checkpoint);
        if (growthResult !== ErrorCode.OK) return growthResult;
        continue;
      }
      if (result !== ErrorCode.OK) {
        wasmInstance.exports.vm_undo_rollback(statePtr, checkpoint);
        new Uint8Array(wasmInstance.memory.buffer).set(new Uint8Array(state.buffer), statePtr);
        return result;
      }

      new Uint8Array(state.buffer).set(new Uint8Array(wasmInstance.memory.buffer, statePtr, state.size));
      return ErrorCode.OK;
    }
  };

  const readScalarValue = (state: WasmStateHandle, program: ReducerProgram, slot: number): ScalarValue => {
    const slotDef = program.slotDefs[slot];
    if (!slotDef || slotDef.type !== SlotType.SCALAR) throw new RangeError(`Slot ${slot} is not a scalar slot`);
    const view = new DataView(state.buffer);
    const meta = STATE_HEADER_SIZE + slot * SLOT_META_SIZE;
    const dataOffset = view.getUint32(meta, true);
    const timestamp = view.getFloat64(dataOffset + 8, true);
    if (timestamp === Number.NEGATIVE_INFINITY) return { kind: 'empty' };
    switch (slotDef.aggType) {
      case AggType.SCALAR_U32:
        return { kind: 'u32', value: view.getUint32(dataOffset, true) };
      case AggType.SCALAR_F64:
        return { kind: 'f64', value: view.getFloat64(dataOffset, true) };
      case AggType.SCALAR_I64:
        return { kind: 'i64', value: view.getBigInt64(dataOffset, true) };
      default:
        throw new Error(`Invalid scalar subtype ${slotDef.aggType}`);
    }
  };

  const readEvictedRows = (state: WasmStateHandle, program: ReducerProgram): readonly EvictedRow[] => {
    const view = new DataView(state.buffer);
    const rows: EvictedRow[] = [];
    for (let slot = 0; slot < program.numSlots; slot++) {
      const meta = STATE_HEADER_SIZE + slot * SLOT_META_SIZE;
      if ((view.getUint8(meta + 12) & SlotTypeFlag.HAS_EVICT_TRIGGER) === 0) continue;
      const bufferOffset = view.getUint32(meta + 36, true);
      const count = view.getUint32(meta + 40, true);
      for (let i = 0; i < count; i++) {
        const entry = bufferOffset + i * EVICTION_ENTRY_SIZE;
        rows.push({
          slot,
          timestamp: view.getFloat64(entry, true),
          key: view.getUint32(entry + 8, true),
          value: view.getUint32(entry + 12, true),
        });
      }
    }
    return rows;
  };

  const backend: ColumineBackend = {
    backend: 'wasm',

    async loadProgram(bytecode: Uint8Array): Promise<ReducerProgram> {
      return parseReducerProgram(bytecode);
    },

    createState(program: ReducerProgram): StateHandle {
      const probePtr = wasmInstance.stateRegionOffset;
      ensureMemory(probePtr + program.bytecode.byteLength);
      new Uint8Array(wasmInstance.memory.buffer).set(program.bytecode, probePtr);
      const stateSize = wasmInstance.exports.vm_calculate_state_size(probePtr, program.bytecode.length);
      if (stateSize === 0) throw new Error('Invalid program: cannot calculate state size');

      const statePtr = wasmInstance.stateRegionOffset;
      const programPtr = align8(statePtr + stateSize);
      ensureMemory(programPtr + program.bytecode.byteLength);
      const wasmU8 = new Uint8Array(wasmInstance.memory.buffer);
      wasmU8.fill(0, statePtr, statePtr + stateSize);
      wasmU8.set(program.bytecode, programPtr);
      const initResult = wasmInstance.exports.vm_init_state(statePtr, programPtr, program.bytecode.length);
      if (initResult !== ErrorCode.OK) {
        throw new Error(`Failed to initialize state: error code ${initResult}`);
      }
      const buffer = new ArrayBuffer(stateSize);
      new Uint8Array(buffer).set(new Uint8Array(wasmInstance.memory.buffer, statePtr, stateSize));
      return createWasmStateHandle(buffer, stateSize);
    },

    resetState(stateHandle: StateHandle, program: ReducerProgram): void {
      const state = assertWasmStateHandle(stateHandle);
      const statePtr = copyStateIn(state);
      const programPtr = prepareProgramAfterState(state.size, program);
      const result = wasmInstance.exports.vm_reset_state(statePtr, programPtr, program.bytecode.length);
      if (result !== ErrorCode.OK) throw new Error(`Failed to reset state: error code ${result}`);
      new Uint8Array(state.buffer).set(new Uint8Array(wasmInstance.memory.buffer, statePtr, state.size));
    },

    executeBatch(state, program, columns, batchLen) {
      return executeWithGrowth(state, program, columns, batchLen, false);
    },

    executeBatchDelta(state, program, columns, batchLen) {
      return executeWithGrowth(state, program, columns, batchLen, true);
    },

    getMapSize(stateHandle, _program, slot) {
      const state = assertWasmStateHandle(stateHandle);
      return new DataView(state.buffer).getUint32(STATE_HEADER_SIZE + slot * SLOT_META_SIZE + 8, true);
    },

    getSetSize(stateHandle, _program, slot) {
      const state = assertWasmStateHandle(stateHandle);
      return new DataView(state.buffer).getUint32(STATE_HEADER_SIZE + slot * SLOT_META_SIZE + 8, true);
    },

    getAggregateValue(stateHandle, _program, slot) {
      const state = assertWasmStateHandle(stateHandle);
      const view = new DataView(state.buffer);
      const meta = STATE_HEADER_SIZE + slot * SLOT_META_SIZE;
      const dataOffset = view.getUint32(meta, true);
      if (view.getUint8(meta + 13) === 2) {
        return view.getUint32(dataOffset, true) + view.getUint32(dataOffset + 4, true) * 0x100000000;
      }
      return view.getFloat64(dataOffset, true);
    },

    getScalarValue(stateHandle, program, slot) {
      return readScalarValue(assertWasmStateHandle(stateHandle), program, slot);
    },

    mapGet(stateHandle, _program, slot, key) {
      const state = assertWasmStateHandle(stateHandle);
      const statePtr = copyStateIn(state);
      const view = new DataView(state.buffer);
      const meta = STATE_HEADER_SIZE + slot * SLOT_META_SIZE;
      const result = wasmInstance.exports.vm_map_get(
        statePtr,
        view.getUint32(meta, true),
        view.getUint32(meta + 4, true),
        key,
      );
      return result === EMPTY_KEY_SIGNED ? undefined : result;
    },

    setContains(stateHandle, _program, slot, elem) {
      const state = assertWasmStateHandle(stateHandle);
      const statePtr = copyStateIn(state);
      const view = new DataView(state.buffer);
      const meta = STATE_HEADER_SIZE + slot * SLOT_META_SIZE;
      return (
        wasmInstance.exports.vm_set_contains(
          statePtr,
          view.getUint32(meta, true),
          view.getUint32(meta + 4, true),
          elem,
        ) !== 0
      );
    },

    structMap2GetRow(stateHandle, _program, slot, key1, key2) {
      const state = assertWasmStateHandle(stateHandle);
      const statePtr = copyStateIn(state);
      const bytes = new Uint8Array(state.buffer);
      const view = new DataView(state.buffer);
      const meta = STATE_HEADER_SIZE + slot * SLOT_META_SIZE;
      const rowOffset = wasmInstance.exports.vm_struct_map2_get_row_ptr(
        statePtr,
        view.getUint32(meta, true),
        view.getUint32(meta + 4, true),
        bytes[meta + 13],
        view.getUint16(meta + 16, true),
        key1,
        key2,
      );
      if (rowOffset === EMPTY_KEY_SIGNED || rowOffset === 0xffffffff) return undefined;
      return { key1: key1 >>> 0, key2: key2 >>> 0, rowOffset: rowOffset >>> 0 };
    },

    structMap2Entries(stateHandle, _program, slot) {
      const state = assertWasmStateHandle(stateHandle);
      const statePtr = copyStateIn(state);
      const bytes = new Uint8Array(state.buffer);
      const view = new DataView(state.buffer);
      const meta = STATE_HEADER_SIZE + slot * SLOT_META_SIZE;
      const slotOffset = view.getUint32(meta, true);
      const capacity = view.getUint32(meta + 4, true);
      const numFields = bytes[meta + 13];
      const rowSize = view.getUint16(meta + 16, true);
      const rowsBase = slotOffset + align8(numFields) + capacity * 8;
      const rows: StructMap2RowRef[] = [];
      let pos = wasmInstance.exports.vm_struct_map2_iter_start(statePtr, slotOffset, capacity, numFields);
      while (pos < capacity) {
        rows.push({
          key1: wasmInstance.exports.vm_struct_map2_iter_key1(statePtr, slotOffset, numFields, pos) >>> 0,
          key2: wasmInstance.exports.vm_struct_map2_iter_key2(statePtr, slotOffset, capacity, numFields, pos) >>> 0,
          rowOffset: rowsBase + pos * rowSize,
        });
        pos = wasmInstance.exports.vm_struct_map2_iter_next(statePtr, slotOffset, capacity, numFields, pos);
      }
      return rows;
    },

    evictExpired(stateHandle, program, now) {
      const state = assertWasmStateHandle(stateHandle);
      const statePtr = copyStateIn(state);
      const status = wasmInstance.exports.vm_evict_all_expired(statePtr, now);
      if (status !== ErrorCode.OK) return { ok: false, error: vmErrorCode(status) };
      const count = wasmInstance.exports.vm_get_evicted_count();
      new Uint8Array(state.buffer).set(new Uint8Array(wasmInstance.memory.buffer, statePtr, state.size));
      return { ok: true, count, rows: readEvictedRows(state, program) };
    },

    serialize(stateHandle, _program) {
      return new Uint8Array(assertWasmStateHandle(stateHandle).buffer).slice();
    },

    deserialize(_program, data) {
      const buffer = new ArrayBuffer(data.length);
      new Uint8Array(buffer).set(data);
      return createWasmStateHandle(buffer, data.length);
    },

    undoEnable(stateHandle) {
      const state = assertWasmStateHandle(stateHandle);
      const statePtr = copyStateIn(state);
      wasmInstance.exports.vm_undo_enable(statePtr, state.size);
    },

    undoCheckpoint(stateHandle) {
      assertWasmStateHandle(stateHandle);
      return wasmInstance.exports.vm_undo_checkpoint(wasmInstance.stateRegionOffset);
    },

    undoRollback(stateHandle, checkpointPos) {
      const state = assertWasmStateHandle(stateHandle);
      const statePtr = wasmInstance.stateRegionOffset;
      wasmInstance.exports.vm_undo_rollback(statePtr, checkpointPos);
      new Uint8Array(state.buffer).set(new Uint8Array(wasmInstance.memory.buffer, statePtr, state.size));
    },

    undoCommit(stateHandle, checkpointPos) {
      assertWasmStateHandle(stateHandle);
      wasmInstance.exports.vm_undo_commit(wasmInstance.stateRegionOffset, checkpointPos);
    },

    undoHasOverflow() {
      return wasmInstance.exports.vm_undo_has_overflow() !== 0;
    },

    deltaExportSegment(stateHandle, fromPos, toPos) {
      const state = assertWasmStateHandle(stateHandle);
      const statePtr = copyStateIn(state);
      wasmInstance.exports.vm_delta_export_segment(statePtr, fromPos, toPos);
      const undoPtr = wasmInstance.exports.vm_delta_export_undo_ptr();
      const redoPtr = wasmInstance.exports.vm_delta_export_redo_ptr();
      const len = wasmInstance.exports.vm_delta_export_len_bytes();
      const entrySize = wasmInstance.exports.vm_delta_export_entry_size();
      const overflow = wasmInstance.exports.vm_delta_export_overflow() !== 0;
      return {
        undoBytes: new Uint8Array(wasmInstance.memory.buffer, undoPtr, len).slice(),
        redoBytes: new Uint8Array(wasmInstance.memory.buffer, redoPtr, len).slice(),
        entrySize,
        overflow,
      };
    },

    deltaApplyRollbackSegment(stateHandle, segment, entrySize) {
      const state = assertWasmStateHandle(stateHandle);
      const statePtr = copyStateIn(state);
      const segmentPtr = align8(statePtr + state.size);
      ensureMemory(segmentPtr + segment.byteLength);
      new Uint8Array(wasmInstance.memory.buffer).set(segment, segmentPtr);
      wasmInstance.exports.vm_delta_apply_rollback_segment(statePtr, segmentPtr, segment.byteLength, entrySize);
      new Uint8Array(state.buffer).set(new Uint8Array(wasmInstance.memory.buffer, statePtr, state.size));
    },

    deltaApplyRollforwardSegment(stateHandle, segment, entrySize) {
      const state = assertWasmStateHandle(stateHandle);
      const statePtr = copyStateIn(state);
      const segmentPtr = align8(statePtr + state.size);
      ensureMemory(segmentPtr + segment.byteLength);
      new Uint8Array(wasmInstance.memory.buffer).set(segment, segmentPtr);
      wasmInstance.exports.vm_delta_apply_rollforward_segment(statePtr, segmentPtr, segment.byteLength, entrySize);
      new Uint8Array(state.buffer).set(new Uint8Array(wasmInstance.memory.buffer, statePtr, state.size));
    },
  };

  return backend;
}

// =============================================================================
// Loader - finds and loads columine.wasm
// =============================================================================

/**
 * Load columine's reducer-only WASM backend.
 *
 * Searches for columine.wasm in default locations relative to this module.
 * For standalone columine usage (without axe-runtime).
 *
 * @param wasmPath - Optional explicit path to columine.wasm
 * @param memoryPages - WASM memory size in 64KB pages (default: 256 = 16MB)
 */
export async function loadColumineWasm(wasmPath?: string | URL, memoryPages?: number): Promise<ColumineBackend> {
  const wasmBytes = await loadWasmBytes(wasmPath, 'columine.wasm');
  if (!wasmBytes) {
    throw new Error(
      'Could not find columine.wasm. Provide an explicit path via loadColumineWasm(path), ' +
        'or ensure columine.wasm is in ./columine.wasm or ../dist/columine.wasm relative to this module.',
    );
  }
  return createColumineWasmBackend(wasmBytes, memoryPages);
}

/**
 * Helper to load WASM bytes from path or default locations.
 */
async function loadWasmBytes(
  customPath: string | URL | undefined,
  defaultFileName: string,
): Promise<ArrayBuffer | undefined> {
  if (customPath) {
    try {
      const response = await fetch(customPath);
      if (response.ok) {
        return await response.arrayBuffer();
      }
    } catch {
      // File not found or other error
    }
    return undefined;
  }

  // Try default locations
  const defaultPaths = [
    new URL(`./${defaultFileName}`, import.meta.url),
    new URL(`../dist/${defaultFileName}`, import.meta.url),
  ];

  for (const path of defaultPaths) {
    try {
      const response = await fetch(path);
      if (response.ok) {
        // Note: Bun's fetch can return ok=true for file:// URLs even if file doesn't exist
        // The actual error is thrown when reading the body
        return await response.arrayBuffer();
      }
    } catch {
      // Try next
    }
  }

  return undefined;
}
