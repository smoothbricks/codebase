/**
 * WASM Backend - Standalone columine loader (reducer-only binary)
 *
 * Architecture:
 *   - ONE WASM instance per backend value (shared by its state handles)
 *   - State lives in JS ArrayBuffer per agent instance (GC'd)
 *   - State copied INTO WASM memory before reduce
 *   - State copied OUT of WASM memory after reduce
 *   - Input columns copied into WASM memory for each batch
 *
 * Why a separate loader?
 *   Standalone callers load the smaller reducer-only binary here. A consumer
 *   with its own superset backend passes that value to createPipeline instead;
 *   there is no global backend registry or injection.
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
  StructMapArenaField,
  VoProgramMetadata,
} from './types.js';
import { AggType, ErrorCode, SlotType, SlotTypeFlag, StructFieldType } from './types.js';
import { VoProgramAdmissionUnavailableError } from './vo-program-metadata.js';
import { align8, loadWasmBytes, WasmHostRegion } from './wasm-memory-contract.js';

// =============================================================================
// Constants
// =============================================================================

// Raw-state layout. These are `columine_types::types` — STATE_HEADER_SIZE,
// SLOT_META_SIZE, the SlotMetaOffset fields this host indexes with, and
// size_of::<EvictionEntry>(). A layout bump there fails
// `crates/columine-types/tests/typescript_abi.rs` rather than silently
// misreading a state blob here.
const STATE_HEADER_SIZE = 32;
const SLOT_META_SIZE = 48;
const SLOT_META_TYPE_FLAGS_OFFSET = 12;
const SLOT_META_EVICTED_BUFFER_OFFSET = 36;
const SLOT_META_EVICTED_COUNT_OFFSET = 40;
const EVICTION_ENTRY_SIZE = 16;

// `vm_struct_map_arena_field` writes [state-relative offset:u32 LE, length:u32 LE]
// to an 8-byte scratch on OK. Only the two reserved offsets mean absent/unset;
// length zero at an ordinary offset is a PRESENT empty payload.
const ARENA_FIELD_RECORD_SIZE = 8;
const ARENA_FIELD_ABSENT = 0xffff_ffff;
const ARENA_FIELD_UNSET = 0xffff_fffe;
const ABSENT_ARENA_FIELD = Object.freeze<StructMapArenaField>({ kind: 'absent' });
const UNSET_ARENA_FIELD = Object.freeze<StructMapArenaField>({ kind: 'unset' });

export type VmProgramInvalidCause =
  | { readonly kind: 'MalformedProgram' }
  | { readonly kind: 'BitmapTtlSourceToUntrackedTarget'; readonly source: number; readonly target: number };

/** An actual native program-admission refusal, before state construction. */
export class VmProgramAdmissionError extends Error {
  readonly code = ErrorCode.INVALID_PROGRAM;

  constructor(readonly reason: VmProgramInvalidCause) {
    super(
      reason.kind === 'MalformedProgram'
        ? 'Invalid program: MalformedProgram'
        : `Invalid program: BitmapTtlSourceToUntrackedTarget (source ${reason.source}, target ${reason.target})`,
    );
    this.name = 'VmProgramAdmissionError';
  }
}

/** A host-supplied state image the native VM refused where it entered. */
export class VmStateAdmissionError extends Error {
  constructor(readonly code: ErrorCode) {
    super(`Invalid state image: ${ErrorCode[code] ?? code}`);
    this.name = 'VmStateAdmissionError';
  }
}

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
  vm_calculate_state_size(programPtr: number, programLen: number, outputPtr: number): number;
  vm_init_state(statePtr: number, programPtr: number, programLen: number): number;
  vm_reset_state(statePtr: number, programPtr: number, programLen: number): number;
  vm_validate_state(statePtr: number, stateLen: number): number;
  vm_get_needs_growth_slot(): number;
  vm_calculate_grown_state_size(oldStatePtr: number, oldStateLen: number, grownSlot: number, outPtr: number): number;
  vm_grow_state(
    oldStatePtr: number,
    oldStateLen: number,
    newStatePtr: number,
    newStateLen: number,
    grownSlot: number,
  ): number;
  vm_execute_batch(
    stateBase: number,
    stateLen: number,
    programPtr: number,
    programLen: number,
    colPtrsPtr: number,
    colLengthsPtr: number,
    numCols: number,
    batchLen: number,
  ): number;
  vm_execute_batch_delta(
    stateBase: number,
    stateLen: number,
    programPtr: number,
    programLen: number,
    colPtrsPtr: number,
    colLengthsPtr: number,
    numCols: number,
    batchLen: number,
  ): number;
  vm_evict_all_expired(stateBase: number, stateLen: number, now: number, deltaMode: number): number;
  vm_get_evicted_count(): number;
  vm_map_get(stateBase: number, stateLen: number, slot: number, key: number, outPtr: number): number;
  vm_set_contains(stateBase: number, stateLen: number, slot: number, elem: number, outPtr: number): number;
  vm_struct_map2_get_row_ptr(
    stateBase: number,
    stateLen: number,
    slot: number,
    key1: number,
    key2: number,
    outPtr: number,
  ): number;
  vm_struct_map2_iter_start(stateBase: number, stateLen: number, slot: number, outPtr: number): number;
  vm_struct_map2_iter_next(stateBase: number, stateLen: number, slot: number, current: number, outPtr: number): number;
  vm_struct_map2_iter_key1(stateBase: number, stateLen: number, slot: number, pos: number, outPtr: number): number;
  vm_struct_map2_iter_key2(stateBase: number, stateLen: number, slot: number, pos: number, outPtr: number): number;
  vm_struct_map2_iter_row_ptr(stateBase: number, stateLen: number, slot: number, pos: number, outPtr: number): number;
  vm_struct_map_arena_field(
    stateBase: number,
    stateLen: number,
    slot: number,
    key: number,
    fieldIdx: number,
    outPtr: number,
  ): number;
  vm_undo_enable(stateBase: number, stateSize: number): number;
  vm_undo_checkpoint(stateBase: number): number;
  vm_undo_rollback(stateBase: number, stateLen: number, checkpointPos: number, outPtr: number): number;
  vm_undo_commit(stateBase: number, checkpointPos: number): void;
  vm_undo_has_overflow(): number;
  vm_delta_export_segment(stateBase: number, stateLen: number, fromPos: number, toPos: number, outPtr: number): number;
  vm_delta_export_undo_ptr(): number;
  vm_delta_export_redo_ptr(): number;
  vm_delta_export_len_bytes(): number;
  vm_delta_export_entry_size(): number;
  vm_delta_export_overflow(): number;
  vm_delta_apply_rollback_segment(
    stateBase: number,
    stateLen: number,
    segmentPtr: number,
    segmentLenBytes: number,
    entrySize: number,
    outPtr: number,
  ): number;
  vm_delta_apply_rollforward_segment(
    stateBase: number,
    stateLen: number,
    segmentPtr: number,
    segmentLenBytes: number,
    entrySize: number,
    outPtr: number,
  ): number;
  vm_delta_required_state_size(
    segmentPtr: number,
    segmentLen: number,
    entrySize: number,
    initialSize: number,
    outPtr: number,
  ): number;
}

// =============================================================================
// Shared WASM Instance
// =============================================================================

interface WasmInstance {
  memory: WebAssembly.Memory;
  exports: VmExports;
  /** Every call's working set: the staged state image first, then its scratch. */
  region: WasmHostRegion;
}

function isWasmFunction<T extends (...args: never[]) => unknown>(value: unknown): value is T {
  return typeof value === 'function';
}

const VM_EXPORT_NAMES = [
  'vm_calculate_state_size',
  'vm_init_state',
  'vm_reset_state',
  'vm_validate_state',
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
  'vm_struct_map2_iter_row_ptr',
  'vm_struct_map_arena_field',
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
  'vm_delta_required_state_size',
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
    state.size >= STATE_HEADER_SIZE &&
    state.size === state.buffer.byteLength
  );
}

/**
 * Map a raw non-OK status from the WASM VM to its ErrorCode member.
 *
 * Exhaustive switch instead of an `as ErrorCode` narrowing (same pattern as
 * parse-backend's compactStatusCode): a status outside the enum means the TS
 * ErrorCode mirror and the WASM binary disagree — a contract violation, so it
 * throws a teaching error instead of manufacturing a bogus enum member.
 * Discriminants must match columine-types ErrorCode (`types.rs`).
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
    case ErrorCode.COLUMN_UNDERRUN:
      return ErrorCode.COLUMN_UNDERRUN;
    case ErrorCode.INVALID_CELL_VALUE:
      return ErrorCode.INVALID_CELL_VALUE;
    case ErrorCode.COUNT_OVERFLOW:
      return ErrorCode.COUNT_OVERFLOW;
    case ErrorCode.METADATA_CONFLICT:
      return ErrorCode.METADATA_CONFLICT;
    case ErrorCode.JOURNAL_OVERFLOW:
      return ErrorCode.JOURNAL_OVERFLOW;
    case ErrorCode.DELTA_LANE_MISMATCH:
      return ErrorCode.DELTA_LANE_MISMATCH;
    case ErrorCode.SPECULATION_ACTIVE:
      return ErrorCode.SPECULATION_ACTIVE;
    case ErrorCode.OUT_OF_MEMORY:
      return ErrorCode.OUT_OF_MEMORY;
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
 * The module's memory is sized by its linker for stack and static data. Host
 * bytes live only in a region above it, grown on demand; see `WasmHostRegion`.
 *
 * @param wasmBytes - WASM module bytes
 */
export async function createColumineWasmBackend(wasmBytes: BufferSource): Promise<ColumineBackend> {
  const wasmModule = await WebAssembly.compile(wasmBytes);
  const instance = await WebAssembly.instantiate(wasmModule, {});
  const exports = parseVmExports(instance.exports);
  const memory = getExportedMemory(instance.exports);
  if (memory === null) throw new Error('WASM module exports no memory');

  // Constructed before the first native call, so the region starts past every
  // byte the module owns at instantiation.
  const wasmInstance: WasmInstance = {
    memory,
    exports,
    region: new WasmHostRegion(memory),
  };

  // The journal belongs to the shared VM, not to a state handle. An external
  // speculative window must end before its committed image can be relocated.
  let undoEnabled = false;

  const calculateProgramSize = (bytecode: Uint8Array): number => {
    const programPtr = wasmInstance.region.reserve(align8(bytecode.byteLength) + 16);
    const outputPtr = align8(programPtr + bytecode.byteLength);
    new Uint8Array(wasmInstance.memory.buffer).set(bytecode, programPtr);
    const status = wasmInstance.exports.vm_calculate_state_size(programPtr, bytecode.byteLength, outputPtr);
    const output = new DataView(wasmInstance.memory.buffer, outputPtr, 16);
    if (status === ErrorCode.OK) {
      const size = output.getUint32(0, true);
      if (size < STATE_HEADER_SIZE) throw new Error(`WASM admission returned invalid state size ${size}`);
      return size;
    }
    if (status === ErrorCode.INVALID_PROGRAM) {
      switch (output.getUint32(4, true)) {
        case 1:
          throw new VmProgramAdmissionError({ kind: 'MalformedProgram' });
        case 2:
          throw new VmProgramAdmissionError({
            kind: 'BitmapTtlSourceToUntrackedTarget',
            source: output.getUint32(8, true),
            target: output.getUint32(12, true),
          });
        default:
          throw new Error('WASM admission returned an unknown program cause');
      }
    }
    throw new Error(`WASM admission refused its host-owned spans: ${vmErrorCode(status)}`);
  };

  // Reserve the call's complete working set, the state image then `scratchBytes`
  // after its 8-aligned end, and upload the authoritative host image at its
  // start. Every pointer the call passes derives from the returned offset.
  const stageState = (state: WasmStateHandle, scratchBytes: number): number => {
    const statePtr = wasmInstance.region.reserve(align8(state.size) + scratchBytes);
    new Uint8Array(wasmInstance.memory.buffer).set(new Uint8Array(state.buffer), statePtr);
    return statePtr;
  };

  const copyStateOut = (state: WasmStateHandle, statePtr: number, logicalSize: number, physicalSize: number): void => {
    if (!Number.isInteger(logicalSize) || logicalSize < STATE_HEADER_SIZE || logicalSize > physicalSize) {
      throw new Error(
        `WASM VM returned logical state extent ${logicalSize} outside the ${physicalSize}-byte allocation`,
      );
    }
    if (state.size !== logicalSize) state.buffer = new ArrayBuffer(logicalSize);
    state.size = logicalSize;
    new Uint8Array(state.buffer).set(new Uint8Array(wasmInstance.memory.buffer, statePtr, logicalSize));
  };

  const rollback = (state: WasmStateHandle, checkpoint: number): number => {
    const statePtr = stageState(state, 4);
    const outPtr = align8(statePtr + state.size);
    const status = wasmInstance.exports.vm_undo_rollback(statePtr, state.size, checkpoint, outPtr);
    if (status === ErrorCode.OK) {
      const logicalSize = new DataView(wasmInstance.memory.buffer).getUint32(outPtr, true);
      if (logicalSize !== state.size) {
        throw new Error(`WASM undo rollback changed the stable ${state.size}-byte state extent to ${logicalSize}`);
      }
      copyStateOut(state, statePtr, logicalSize, state.size);
    }
    return status;
  };

  // Columns are uploaded after the program, then two parallel u32 tables:
  // each column's pointer, then its exact uploaded byte length, so the VM
  // bounds every column read by its source rather than by linear memory.
  const prepareExecution = (
    state: WasmStateHandle,
    program: ReducerProgram,
    columns: ColumnInput[],
  ): { statePtr: number; programPtr: number; colPtrsPtr: number; colLengthsPtr: number } => {
    // Offsets within the working set; columns sit above the state and program.
    const programOffset = align8(state.size);
    let cursor = align8(programOffset + program.bytecode.byteLength);
    for (const column of columns) cursor = align8(cursor + column.data.byteLength);
    const colPtrsOffset = cursor;
    const colLengthsOffset = colPtrsOffset + columns.length * Uint32Array.BYTES_PER_ELEMENT;
    const workingSetEnd = colLengthsOffset + columns.length * Uint32Array.BYTES_PER_ELEMENT;
    const statePtr = stageState(state, workingSetEnd - programOffset);
    const programPtr = statePtr + programOffset;
    const colPtrsPtr = statePtr + colPtrsOffset;
    const colLengthsPtr = statePtr + colLengthsOffset;

    const wasmU8 = new Uint8Array(wasmInstance.memory.buffer);
    const wasmU32 = new Uint32Array(wasmInstance.memory.buffer);
    wasmU8.set(program.bytecode, programPtr);
    cursor = align8(programPtr + program.bytecode.byteLength);
    for (let i = 0; i < columns.length; i++) {
      const data = columns[i].data;
      wasmU8.set(new Uint8Array(data.buffer, data.byteOffset, data.byteLength), cursor);
      wasmU32[colPtrsPtr / 4 + i] = cursor;
      wasmU32[colLengthsPtr / 4 + i] = data.byteLength;
      cursor = align8(cursor + data.byteLength);
    }
    return { statePtr, programPtr, colPtrsPtr, colLengthsPtr };
  };

  const growFromAuthority = (state: WasmStateHandle, program: ReducerProgram): number => {
    if (undoEnabled) return ErrorCode.NEEDS_GROWTH;
    const grownSlot = wasmInstance.exports.vm_get_needs_growth_slot() >>> 0;
    if (grownSlot >= program.numSlots) return ErrorCode.INVALID_SLOT;
    const statePtr = stageState(state, 4);
    const outPtr = align8(statePtr + state.size);
    const sized = wasmInstance.exports.vm_calculate_grown_state_size(statePtr, state.size, grownSlot, outPtr);
    if (sized !== ErrorCode.OK) return sized;
    const grownSize = new DataView(wasmInstance.memory.buffer).getUint32(outPtr, true);
    if (grownSize <= state.size) return ErrorCode.INVALID_STATE;
    // The grown image follows the old one in the same working set. A larger
    // reservation may move the region, which carries no contents: re-upload.
    const grownBase = wasmInstance.region.reserve(align8(state.size) + grownSize);
    if (grownBase !== statePtr) {
      new Uint8Array(wasmInstance.memory.buffer).set(new Uint8Array(state.buffer), grownBase);
    }
    const newStatePtr = align8(grownBase + state.size);
    new Uint8Array(wasmInstance.memory.buffer, newStatePtr, grownSize).fill(0);
    const result = wasmInstance.exports.vm_grow_state(grownBase, state.size, newStatePtr, grownSize, grownSlot);
    if (result !== ErrorCode.OK) return result;
    copyStateOut(state, newStatePtr, grownSize, grownSize);
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
      const { statePtr, programPtr, colPtrsPtr, colLengthsPtr } = prepareExecution(state, program, columns);
      const attemptCheckpoint = wasmInstance.exports.vm_undo_checkpoint(statePtr);
      const result = delta
        ? wasmInstance.exports.vm_execute_batch_delta(
            statePtr,
            state.size,
            programPtr,
            program.bytecode.length,
            colPtrsPtr,
            colLengthsPtr,
            columns.length,
            batchLen,
          )
        : wasmInstance.exports.vm_execute_batch(
            statePtr,
            state.size,
            programPtr,
            program.bytecode.length,
            colPtrsPtr,
            colLengthsPtr,
            columns.length,
            batchLen,
          );

      if (result !== ErrorCode.OK) {
        // With undo enabled, the failed attempt's image exists only in WASM memory:
        // persist it, so the rollback below undoes exactly the mutations it logged.
        // Without it, the unmodified host handle is the committed authority.
        if (undoEnabled) copyStateOut(state, statePtr, state.size, state.size);
        const rolledBack = rollback(state, attemptCheckpoint);
        if (rolledBack !== ErrorCode.OK) return rolledBack;
        if (result !== ErrorCode.NEEDS_GROWTH || undoEnabled) return result;
        const growthResult = growFromAuthority(state, program);
        if (growthResult !== ErrorCode.OK) return growthResult;
        continue;
      }

      copyStateOut(state, statePtr, state.size, state.size);
      return ErrorCode.OK;
    }
  };

  const applySegment = (state: WasmStateHandle, segment: Uint8Array, entrySize: number, forward: boolean): void => {
    const statePtr = stageState(state, align8(segment.byteLength) + 4);
    const segmentPtr = align8(statePtr + state.size);
    const outPtr = align8(segmentPtr + segment.byteLength);
    new Uint8Array(wasmInstance.memory.buffer).set(segment, segmentPtr);
    const preflight = wasmInstance.exports.vm_delta_required_state_size(
      segmentPtr,
      segment.byteLength,
      entrySize,
      state.size,
      outPtr,
    );
    if (preflight !== ErrorCode.OK)
      throw new Error(`WASM delta preflight refused the segment: ${vmErrorCode(preflight)}`);
    const requiredSize = new DataView(wasmInstance.memory.buffer).getUint32(outPtr, true);
    if (requiredSize !== state.size) {
      throw new Error(
        `WASM delta preflight requires ${requiredSize} bytes outside the stable ${state.size}-byte state image`,
      );
    }
    const status = forward
      ? wasmInstance.exports.vm_delta_apply_rollforward_segment(
          statePtr,
          state.size,
          segmentPtr,
          segment.byteLength,
          entrySize,
          outPtr,
        )
      : wasmInstance.exports.vm_delta_apply_rollback_segment(
          statePtr,
          state.size,
          segmentPtr,
          segment.byteLength,
          entrySize,
          outPtr,
        );
    if (status !== ErrorCode.OK) throw new Error(`WASM delta application refused the segment: ${vmErrorCode(status)}`);
    const logicalSize = new DataView(wasmInstance.memory.buffer).getUint32(outPtr, true);
    if (logicalSize !== state.size) {
      throw new Error(`WASM delta application changed the stable ${state.size}-byte state extent to ${logicalSize}`);
    }
    copyStateOut(state, statePtr, logicalSize, state.size);
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
      if ((view.getUint8(meta + SLOT_META_TYPE_FLAGS_OFFSET) & SlotTypeFlag.HAS_EVICT_TRIGGER) === 0) continue;
      const bufferOffset = view.getUint32(meta + SLOT_META_EVICTED_BUFFER_OFFSET, true);
      const count = view.getUint32(meta + SLOT_META_EVICTED_COUNT_OFFSET, true);
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
    growState(stateHandle, program) {
      return growFromAuthority(assertWasmStateHandle(stateHandle), program);
    },

    backend: 'wasm',

    async loadProgram(
      bytecode: Uint8Array,
      acceptedMagics?: readonly number[],
      voProgramMetadata?: VoProgramMetadata,
    ): Promise<ReducerProgram> {
      const program = parseReducerProgram(bytecode, undefined, acceptedMagics);
      calculateProgramSize(program.bytecode);
      if (voProgramMetadata === undefined) return program;
      // This module links no native value-object admission capability. Explicitly empty metadata on
      // a program that declares no VO field names nothing to admit, exactly as the native program
      // owner treats an ordinary program. Any other metadata is refused with a typed error; it is
      // never accepted unchecked. Only STRUCT_MAP slots can declare a VO field: the parser refuses
      // VO descriptors in every other slot kind.
      const declaresVo = program.slotDefs.some(
        (slotDef) => slotDef.type === SlotType.STRUCT_MAP && slotDef.fieldTypes.includes(StructFieldType.VO),
      );
      if (voProgramMetadata.fields.length === 0 && voProgramMetadata.counts.length === 0 && !declaresVo) {
        return program;
      }
      throw new VoProgramAdmissionUnavailableError();
    },

    createState(program: ReducerProgram): StateHandle {
      const stateSize = calculateProgramSize(program.bytecode);
      const statePtr = wasmInstance.region.reserve(align8(stateSize) + program.bytecode.byteLength);
      const programPtr = align8(statePtr + stateSize);
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
      const statePtr = stageState(state, program.bytecode.byteLength);
      const programPtr = align8(statePtr + state.size);
      new Uint8Array(wasmInstance.memory.buffer).set(program.bytecode, programPtr);
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
      if (!Number.isInteger(slot) || slot < 0 || slot > 0xffffffff) return { ok: false, error: ErrorCode.INVALID_SLOT };
      if (!Number.isInteger(key) || key < 0 || key > 0xffffffff) return { ok: false, error: ErrorCode.INVALID_KEY };
      const state = assertWasmStateHandle(stateHandle);
      const statePtr = stageState(state, 8);
      const outPtr = align8(statePtr + state.size);
      const status = wasmInstance.exports.vm_map_get(statePtr, state.size, slot, key, outPtr);
      if (status !== ErrorCode.OK) return { ok: false, error: vmErrorCode(status) };
      const record = new DataView(wasmInstance.memory.buffer, outPtr, 8);
      const present = record.getUint32(0, true);
      if (present > 1) return { ok: false, error: ErrorCode.INVALID_STATE };
      return { ok: true, value: present === 0 ? undefined : record.getUint32(4, true) };
    },

    setContains(stateHandle, _program, slot, elem) {
      if (!Number.isInteger(slot) || slot < 0 || slot > 0xffffffff) return { ok: false, error: ErrorCode.INVALID_SLOT };
      if (!Number.isInteger(elem) || elem < 0 || elem > 0xffffffff) return { ok: false, error: ErrorCode.INVALID_KEY };
      const state = assertWasmStateHandle(stateHandle);
      const statePtr = stageState(state, 4);
      const outPtr = align8(statePtr + state.size);
      const status = wasmInstance.exports.vm_set_contains(statePtr, state.size, slot, elem, outPtr);
      if (status !== ErrorCode.OK) return { ok: false, error: vmErrorCode(status) };
      const value = new DataView(wasmInstance.memory.buffer).getUint32(outPtr, true);
      if (value > 1) return { ok: false, error: ErrorCode.INVALID_STATE };
      return { ok: true, value: value === 1 };
    },

    structMap2GetRow(stateHandle, _program, slot, key1, key2) {
      if (!Number.isInteger(slot) || slot < 0 || slot > 0xffffffff) return { ok: false, error: ErrorCode.INVALID_SLOT };
      if (
        !Number.isInteger(key1) ||
        key1 < 0 ||
        key1 > 0xffffffff ||
        !Number.isInteger(key2) ||
        key2 < 0 ||
        key2 > 0xffffffff
      )
        return { ok: false, error: ErrorCode.INVALID_KEY };
      const state = assertWasmStateHandle(stateHandle);
      const statePtr = stageState(state, 8);
      const outPtr = align8(statePtr + state.size);
      const status = wasmInstance.exports.vm_struct_map2_get_row_ptr(statePtr, state.size, slot, key1, key2, outPtr);
      if (status !== ErrorCode.OK) return { ok: false, error: vmErrorCode(status) };
      const record = new DataView(wasmInstance.memory.buffer, outPtr, 8);
      const present = record.getUint32(0, true);
      if (present > 1) return { ok: false, error: ErrorCode.INVALID_STATE };
      return {
        ok: true,
        value: present === 0 ? undefined : { key1, key2, rowOffset: record.getUint32(4, true) },
      };
    },

    structMap2Entries(stateHandle, _program, slot) {
      if (!Number.isInteger(slot) || slot < 0 || slot > 0xffffffff) return { ok: false, error: ErrorCode.INVALID_SLOT };
      const state = assertWasmStateHandle(stateHandle);
      const statePtr = stageState(state, 4);
      const outPtr = align8(statePtr + state.size);
      const startStatus = wasmInstance.exports.vm_struct_map2_iter_start(statePtr, state.size, slot, outPtr);
      if (startStatus !== ErrorCode.OK) return { ok: false, error: vmErrorCode(startStatus) };
      // The checked native start proved the metadata and complete row extent before host access.
      const view = new DataView(state.buffer);
      const meta = STATE_HEADER_SIZE + slot * SLOT_META_SIZE;
      const capacity = view.getUint32(meta + 4, true);
      const output = new DataView(wasmInstance.memory.buffer, outPtr, 4);
      const rows: StructMap2RowRef[] = [];
      let pos = output.getUint32(0, true);
      while (pos < capacity) {
        const key1Status = wasmInstance.exports.vm_struct_map2_iter_key1(statePtr, state.size, slot, pos, outPtr);
        if (key1Status !== ErrorCode.OK) return { ok: false, error: vmErrorCode(key1Status) };
        const key1 = output.getUint32(0, true);
        const key2Status = wasmInstance.exports.vm_struct_map2_iter_key2(statePtr, state.size, slot, pos, outPtr);
        if (key2Status !== ErrorCode.OK) return { ok: false, error: vmErrorCode(key2Status) };
        const key2 = output.getUint32(0, true);
        const rowStatus = wasmInstance.exports.vm_struct_map2_iter_row_ptr(statePtr, state.size, slot, pos, outPtr);
        if (rowStatus !== ErrorCode.OK) return { ok: false, error: vmErrorCode(rowStatus) };
        const rowOffset = output.getUint32(0, true);
        rows.push({ key1, key2, rowOffset });
        const nextStatus = wasmInstance.exports.vm_struct_map2_iter_next(statePtr, state.size, slot, pos, outPtr);
        if (nextStatus !== ErrorCode.OK) return { ok: false, error: vmErrorCode(nextStatus) };
        pos = output.getUint32(0, true);
      }
      return { ok: true, value: rows };
    },

    structMapArenaField(stateHandle, _program, slot, key, fieldIdx) {
      const state = assertWasmStateHandle(stateHandle);
      const statePtr = stageState(state, ARENA_FIELD_RECORD_SIZE);
      const outPtr = align8(statePtr + state.size);
      const status = wasmInstance.exports.vm_struct_map_arena_field(statePtr, state.size, slot, key, fieldIdx, outPtr);
      if (status !== ErrorCode.OK) return { kind: 'error', error: vmErrorCode(status) };
      const record = new DataView(wasmInstance.memory.buffer, outPtr, ARENA_FIELD_RECORD_SIZE);
      const offset = record.getUint32(0, true);
      const length = record.getUint32(4, true);
      if (length === 0 && offset === ARENA_FIELD_ABSENT) return ABSENT_ARENA_FIELD;
      if (length === 0 && offset === ARENA_FIELD_UNSET) return UNSET_ARENA_FIELD;
      if (offset + length > state.size) {
        // invariant throw: the VM validated this reference against the arena it lives in
        throw new Error(
          `WASM VM returned arena field [${offset}, ${offset + length}) outside the ${state.size}-byte state`,
        );
      }
      return { kind: 'bytes', bytes: new Uint8Array(state.buffer, offset, length).slice() };
    },

    evictExpired(stateHandle, program, now, delta) {
      const state = assertWasmStateHandle(stateHandle);
      const statePtr = stageState(state, 0);
      const attemptCheckpoint = wasmInstance.exports.vm_undo_checkpoint(statePtr);
      const status = wasmInstance.exports.vm_evict_all_expired(statePtr, state.size, now, delta ? 1 : 0);
      if (status !== ErrorCode.OK) {
        if (undoEnabled) copyStateOut(state, statePtr, state.size, state.size);
        const rolledBack = rollback(state, attemptCheckpoint);
        return { ok: false, error: vmErrorCode(rolledBack === ErrorCode.OK ? status : rolledBack) };
      }
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
      const state = createWasmStateHandle(buffer, data.length);
      // A restored image enters the VM here: its whole geometry is checked
      // once, so no later call trusts metadata the VM did not write.
      const status = wasmInstance.exports.vm_validate_state(stageState(state, 0), state.size);
      if (status !== ErrorCode.OK) throw new VmStateAdmissionError(vmErrorCode(status));
      return state;
    },

    undoEnable(stateHandle) {
      const state = assertWasmStateHandle(stateHandle);
      const statePtr = stageState(state, 0);
      const status = wasmInstance.exports.vm_undo_enable(statePtr, state.size);
      if (status !== ErrorCode.OK) return { ok: false, error: vmErrorCode(status) };
      undoEnabled = true;
      return { ok: true, value: undefined };
    },

    undoCheckpoint(stateHandle) {
      assertWasmStateHandle(stateHandle);
      return wasmInstance.exports.vm_undo_checkpoint(wasmInstance.region.offset);
    },

    undoRollback(stateHandle, checkpointPos) {
      const status = rollback(assertWasmStateHandle(stateHandle), checkpointPos);
      if (status !== ErrorCode.OK) throw new Error(`WASM undo rollback refused the checkpoint: ${vmErrorCode(status)}`);
    },

    undoCommit(stateHandle, checkpointPos) {
      assertWasmStateHandle(stateHandle);
      wasmInstance.exports.vm_undo_commit(wasmInstance.region.offset, checkpointPos);
      undoEnabled = false;
    },

    undoHasOverflow() {
      return wasmInstance.exports.vm_undo_has_overflow() !== 0;
    },

    deltaExportSegment(stateHandle, fromPos, toPos) {
      const state = assertWasmStateHandle(stateHandle);
      if (
        !Number.isInteger(fromPos) ||
        fromPos < 0 ||
        fromPos > 0xffffffff ||
        !Number.isInteger(toPos) ||
        toPos < fromPos ||
        toPos > 0xffffffff
      ) {
        return { ok: false, error: ErrorCode.INVALID_STATE };
      }
      const statePtr = stageState(state, 4);
      const outPtr = align8(statePtr + state.size);
      const status = wasmInstance.exports.vm_delta_export_segment(statePtr, state.size, fromPos, toPos, outPtr);
      if (status !== ErrorCode.OK) return { ok: false, error: vmErrorCode(status) };
      const undoPtr = wasmInstance.exports.vm_delta_export_undo_ptr();
      const redoPtr = wasmInstance.exports.vm_delta_export_redo_ptr();
      const len = wasmInstance.exports.vm_delta_export_len_bytes();
      const entrySize = wasmInstance.exports.vm_delta_export_entry_size();
      const overflow = wasmInstance.exports.vm_delta_export_overflow() !== 0;
      return {
        ok: true,
        value: {
          undoBytes: new Uint8Array(wasmInstance.memory.buffer, undoPtr, len).slice(),
          redoBytes: new Uint8Array(wasmInstance.memory.buffer, redoPtr, len).slice(),
          entrySize,
          overflow,
        },
      };
    },

    deltaApplyRollbackSegment(stateHandle, segment, entrySize) {
      applySegment(assertWasmStateHandle(stateHandle), segment, entrySize, false);
    },

    deltaApplyRollforwardSegment(stateHandle, segment, entrySize) {
      applySegment(assertWasmStateHandle(stateHandle), segment, entrySize, true);
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
 * For standalone columine usage.
 *
 * @param wasmPath - Optional explicit path to columine.wasm
 */
export async function loadColumineWasm(wasmPath?: string | URL): Promise<ColumineBackend> {
  const wasmBytes = await loadWasmBytes(wasmPath, 'columine.wasm');
  if (!wasmBytes) {
    throw new Error(
      'Could not find columine.wasm. Provide an explicit path via loadColumineWasm(path), ' +
        'or ensure columine.wasm is beside the built module or in ../dist/columine.wasm relative to source.',
    );
  }
  return createColumineWasmBackend(wasmBytes);
}
