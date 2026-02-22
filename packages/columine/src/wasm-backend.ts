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
 *   binary is injected via setBackend() and this loader is never called.
 */

import { parseReducerProgram } from './reducer-bytecode.js';
import type { ColumineBackend, ColumnInput, ReducerProgram, StateHandle } from './types.js';

// =============================================================================
// Constants
// =============================================================================

const STATE_HEADER_SIZE = 32;
// Must match vm.zig SLOT_META_SIZE (48 bytes with TTL/eviction fields)
const SLOT_META_SIZE = 48;

// WASM returns u32 as signed i32, so 0xFFFFFFFF becomes -1
const EMPTY_KEY_SIGNED = -1;

// Reusable scratch buffer for column pointer array in executeBatch.
// Avoids per-batch allocation on the hot reduce path.
let scratchColPtrs: Uint32Array | null = null;
let scratchColPtrsWidth = 0;

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
  // State management
  vm_calculate_state_size(programPtr: number, programLen: number): number;
  vm_init_state(statePtr: number, programPtr: number, programLen: number): number;
  vm_reset_state(statePtr: number, programPtr: number, programLen: number): number;

  // Execution (reducer only)
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

  // Lookups
  vm_map_get(stateBase: number, slotOffset: number, capacity: number, key: number): number;
  vm_set_contains(stateBase: number, slotOffset: number, capacity: number, elem: number): number;

  // Undo log operations (Phase 29)
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
  /** Offset in WASM memory where state is copied to */
  stateRegionOffset: number;
  /** Offset in WASM memory where input columns are copied to */
  inputRegionOffset: number;
}

function align8(n: number): number {
  return (n + 7) & ~7;
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
  // Create shared WASM memory for modules that import it
  const importedMemory = new WebAssembly.Memory({ initial: memoryPages });

  // Compile and instantiate WASM module ONCE
  const wasmModule = await WebAssembly.compile(wasmBytes);
  const instance = await WebAssembly.instantiate(wasmModule, {
    env: { memory: importedMemory },
  });

  const exports = instance.exports as unknown as VmExports;

  // Use exported memory if the module provides one,
  // otherwise fall back to the imported memory we created above
  const exportedMemory = (instance.exports as Record<string, unknown>).memory as WebAssembly.Memory | undefined;
  const memory = exportedMemory ?? importedMemory;

  // Ensure memory is large enough for the layout
  const currentPages = memory.buffer.byteLength / 65536;
  if (currentPages < memoryPages) {
    memory.grow(memoryPages - currentPages);
  }

  // Memory layout:
  // [0, 64KB) - reserved for WASM stack/heap
  // [64KB, 8MB) - state region (holds one agent's state during execution)
  // [8MB, 16MB) - input region (holds columns during execution)
  const wasmInstance: WasmInstance = {
    memory,
    exports,
    stateRegionOffset: 64 * 1024, // 64KB
    inputRegionOffset: 8 * 1024 * 1024, // 8MB
  };

  return {
    backend: 'wasm',

    async loadProgram(bytecode: Uint8Array): Promise<ReducerProgram> {
      return parseReducerProgram(bytecode);
    },

    createState(program: ReducerProgram): StateHandle {
      const wasmU8 = new Uint8Array(wasmInstance.memory.buffer);

      // Copy program to WASM memory for size calculation
      const programPtr = wasmInstance.inputRegionOffset;
      wasmU8.set(program.bytecode, programPtr);

      // Calculate state size using Zig
      const stateSize = wasmInstance.exports.vm_calculate_state_size(programPtr, program.bytecode.length);
      if (stateSize === 0) {
        throw new Error('Invalid program: cannot calculate state size');
      }

      // Initialize state in WASM memory
      const statePtr = wasmInstance.stateRegionOffset;
      const initResult = wasmInstance.exports.vm_init_state(statePtr, programPtr, program.bytecode.length);
      if (initResult !== 0) {
        throw new Error(`Failed to initialize state: error code ${initResult}`);
      }

      // Copy initialized state OUT to JS ArrayBuffer
      const buffer = new ArrayBuffer(stateSize);
      new Uint8Array(buffer).set(wasmU8.subarray(statePtr, statePtr + stateSize));

      return {
        _brand: 'ColumineStateHandle',
        buffer,
        size: stateSize,
      } as WasmStateHandle;
    },

    resetState(state: StateHandle, program: ReducerProgram): void {
      const s = state as WasmStateHandle;
      const wasmU8 = new Uint8Array(wasmInstance.memory.buffer);

      // Copy program to WASM memory
      const programPtr = wasmInstance.inputRegionOffset;
      wasmU8.set(program.bytecode, programPtr);

      // Reset state in WASM memory
      const statePtr = wasmInstance.stateRegionOffset;
      wasmInstance.exports.vm_reset_state(statePtr, programPtr, program.bytecode.length);

      // Copy reset state OUT to JS ArrayBuffer
      new Uint8Array(s.buffer).set(wasmU8.subarray(statePtr, statePtr + s.size));
    },

    executeBatch(state: StateHandle, program: ReducerProgram, columns: ColumnInput[], batchLen: number): number {
      const s = state as WasmStateHandle;
      const wasmU8 = new Uint8Array(wasmInstance.memory.buffer);
      const wasmU32 = new Uint32Array(wasmInstance.memory.buffer);

      // 1. Copy state INTO WASM memory
      const statePtr = wasmInstance.stateRegionOffset;
      wasmU8.set(new Uint8Array(s.buffer), statePtr);

      // 2. Copy program bytecode to input region
      const programPtr = wasmInstance.inputRegionOffset;
      wasmU8.set(program.bytecode, programPtr);

      // 3. Copy input columns after program
      let colDataOffset = programPtr + align8(program.bytecode.length);

      // Reuse scratch buffer for column pointers to avoid per-batch allocation.
      // Grows only when schema width increases (rare — typically once on first call).
      if (!scratchColPtrs || scratchColPtrsWidth < columns.length) {
        scratchColPtrs = new Uint32Array(columns.length);
        scratchColPtrsWidth = columns.length;
      }

      for (let i = 0; i < columns.length; i++) {
        const col = columns[i];
        const bytes = new Uint8Array(col.data.buffer, col.data.byteOffset, col.data.byteLength);
        wasmU8.set(bytes, colDataOffset);
        scratchColPtrs[i] = colDataOffset;
        colDataOffset += align8(bytes.length);
      }

      // 4. Write column pointers array
      const colPtrsPtr = colDataOffset;
      for (let i = 0; i < columns.length; i++) {
        wasmU32[(colPtrsPtr + i * 4) / 4] = scratchColPtrs[i];
      }

      // 5. Execute
      const result = wasmInstance.exports.vm_execute_batch(
        statePtr,
        programPtr,
        program.bytecode.length,
        colPtrsPtr,
        columns.length,
        batchLen,
      );

      // 6. Copy state OUT of WASM memory back to JS ArrayBuffer.
      // Re-create view because WASM memory may have grown (e.g., dynamic shadow
      // buffer allocation during undo overflow), which detaches the old ArrayBuffer.
      const freshU8 = new Uint8Array(wasmInstance.memory.buffer);
      new Uint8Array(s.buffer).set(freshU8.subarray(statePtr, statePtr + s.size));

      return result;
    },

    executeBatchDelta(state: StateHandle, program: ReducerProgram, columns: ColumnInput[], batchLen: number): number {
      const s = state as WasmStateHandle;
      const wasmU8 = new Uint8Array(wasmInstance.memory.buffer);
      const wasmU32 = new Uint32Array(wasmInstance.memory.buffer);

      const statePtr = wasmInstance.stateRegionOffset;
      wasmU8.set(new Uint8Array(s.buffer), statePtr);

      const programPtr = wasmInstance.inputRegionOffset;
      wasmU8.set(program.bytecode, programPtr);

      let colDataOffset = programPtr + align8(program.bytecode.length);

      if (!scratchColPtrs || scratchColPtrsWidth < columns.length) {
        scratchColPtrs = new Uint32Array(columns.length);
        scratchColPtrsWidth = columns.length;
      }

      for (let i = 0; i < columns.length; i++) {
        const col = columns[i];
        const bytes = new Uint8Array(col.data.buffer, col.data.byteOffset, col.data.byteLength);
        wasmU8.set(bytes, colDataOffset);
        scratchColPtrs[i] = colDataOffset;
        colDataOffset += align8(bytes.length);
      }

      const colPtrsPtr = colDataOffset;
      for (let i = 0; i < columns.length; i++) {
        wasmU32[(colPtrsPtr + i * 4) / 4] = scratchColPtrs[i];
      }

      const result = wasmInstance.exports.vm_execute_batch_delta(
        statePtr,
        programPtr,
        program.bytecode.length,
        colPtrsPtr,
        columns.length,
        batchLen,
      );

      const freshU8 = new Uint8Array(wasmInstance.memory.buffer);
      new Uint8Array(s.buffer).set(freshU8.subarray(statePtr, statePtr + s.size));

      return result;
    },

    getMapSize(state: StateHandle, _program: ReducerProgram, slot: number): number {
      const s = state as WasmStateHandle;
      const u32 = new Uint32Array(s.buffer);
      const metaIdx = (STATE_HEADER_SIZE + slot * SLOT_META_SIZE) / 4;
      return u32[metaIdx + 2]; // size field
    },

    getSetSize(state: StateHandle, _program: ReducerProgram, slot: number): number {
      const s = state as WasmStateHandle;
      const u32 = new Uint32Array(s.buffer);
      const metaIdx = (STATE_HEADER_SIZE + slot * SLOT_META_SIZE) / 4;
      return u32[metaIdx + 2];
    },

    getAggregateValue(state: StateHandle, _program: ReducerProgram, slot: number): number {
      const s = state as WasmStateHandle;
      const u8 = new Uint8Array(s.buffer);
      const u32 = new Uint32Array(s.buffer);
      const f64 = new Float64Array(s.buffer);
      const metaStart = STATE_HEADER_SIZE + slot * SLOT_META_SIZE;
      const dataOffset = u32[metaStart / 4];
      // aggType is a single byte at offset 13 within the slot meta (SlotMetaOffset.AGG_TYPE)
      const aggType = u8[metaStart + 13];

      if (aggType === 2) {
        // COUNT - return count (u64 at dataOffset + 8)
        const countLo = u32[(dataOffset + 8) / 4];
        const countHi = u32[(dataOffset + 12) / 4];
        return countLo + countHi * 0x100000000;
      }
      return f64[dataOffset / 8];
    },

    mapGet(state: StateHandle, _program: ReducerProgram, slot: number, key: number): number | undefined {
      const s = state as WasmStateHandle;
      const wasmU8 = new Uint8Array(wasmInstance.memory.buffer);

      // Copy state to WASM for lookup
      const statePtr = wasmInstance.stateRegionOffset;
      wasmU8.set(new Uint8Array(s.buffer), statePtr);

      // Get slot metadata from JS buffer
      const u32 = new Uint32Array(s.buffer);
      const metaIdx = (STATE_HEADER_SIZE + slot * SLOT_META_SIZE) / 4;
      const dataOffset = u32[metaIdx];
      const capacity = u32[metaIdx + 1];

      const result = wasmInstance.exports.vm_map_get(statePtr, dataOffset, capacity, key);
      return result === EMPTY_KEY_SIGNED ? undefined : result;
    },

    setContains(state: StateHandle, _program: ReducerProgram, slot: number, elem: number): boolean {
      const s = state as WasmStateHandle;
      const wasmU8 = new Uint8Array(wasmInstance.memory.buffer);

      // Copy state to WASM for lookup
      const statePtr = wasmInstance.stateRegionOffset;
      wasmU8.set(new Uint8Array(s.buffer), statePtr);

      // Get slot metadata from JS buffer
      const u32 = new Uint32Array(s.buffer);
      const metaIdx = (STATE_HEADER_SIZE + slot * SLOT_META_SIZE) / 4;
      const dataOffset = u32[metaIdx];
      const capacity = u32[metaIdx + 1];

      return wasmInstance.exports.vm_set_contains(statePtr, dataOffset, capacity, elem) !== 0;
    },

    serialize(state: StateHandle, _program: ReducerProgram): Uint8Array {
      const s = state as WasmStateHandle;
      // Return a copy of the JS ArrayBuffer
      return new Uint8Array(s.buffer).slice();
    },

    deserialize(_program: ReducerProgram, data: Uint8Array): StateHandle {
      // Create new ArrayBuffer with the data
      const buffer = new ArrayBuffer(data.length);
      new Uint8Array(buffer).set(data);

      return {
        _brand: 'ColumineStateHandle',
        buffer,
        size: data.length,
      } as WasmStateHandle;
    },

    // =========================================================================
    // Undo Log (Phase 29)
    // =========================================================================

    undoEnable(state: StateHandle): void {
      const s = state as WasmStateHandle;
      const wasmU8 = new Uint8Array(wasmInstance.memory.buffer);
      // Copy state INTO WASM so enable can save change flags
      const statePtr = wasmInstance.stateRegionOffset;
      wasmU8.set(new Uint8Array(s.buffer), statePtr);
      // Pass state size so Zig can snapshot into shadow buffer on undo log overflow
      wasmInstance.exports.vm_undo_enable(statePtr, s.size);
    },

    undoCheckpoint(_state: StateHandle): number {
      const statePtr = wasmInstance.stateRegionOffset;
      return wasmInstance.exports.vm_undo_checkpoint(statePtr);
    },

    undoRollback(state: StateHandle, checkpointPos: number): void {
      const s = state as WasmStateHandle;
      const statePtr = wasmInstance.stateRegionOffset;
      // State is already in WASM memory from the last executeBatch
      wasmInstance.exports.vm_undo_rollback(statePtr, checkpointPos);
      // Copy rolled-back state OUT of WASM to JS ArrayBuffer
      const wasmU8 = new Uint8Array(wasmInstance.memory.buffer);
      new Uint8Array(s.buffer).set(wasmU8.subarray(statePtr, statePtr + s.size));
    },

    undoCommit(_state: StateHandle, checkpointPos: number): void {
      const statePtr = wasmInstance.stateRegionOffset;
      wasmInstance.exports.vm_undo_commit(statePtr, checkpointPos);
    },

    undoHasOverflow(): boolean {
      return wasmInstance.exports.vm_undo_has_overflow() !== 0;
    },

    deltaExportSegment(state: StateHandle, fromPos: number, toPos: number) {
      const s = state as WasmStateHandle;
      const wasmU8 = new Uint8Array(wasmInstance.memory.buffer);
      const statePtr = wasmInstance.stateRegionOffset;
      wasmU8.set(new Uint8Array(s.buffer), statePtr);

      wasmInstance.exports.vm_delta_export_segment(statePtr, fromPos, toPos);
      const undoPtr = wasmInstance.exports.vm_delta_export_undo_ptr();
      const redoPtr = wasmInstance.exports.vm_delta_export_redo_ptr();
      const len = wasmInstance.exports.vm_delta_export_len_bytes();
      const entrySize = wasmInstance.exports.vm_delta_export_entry_size();
      const overflow = wasmInstance.exports.vm_delta_export_overflow() !== 0;
      const undoBytes = new Uint8Array(wasmInstance.memory.buffer, undoPtr, len).slice();
      const redoBytes = new Uint8Array(wasmInstance.memory.buffer, redoPtr, len).slice();
      return { undoBytes, redoBytes, entrySize, overflow };
    },

    deltaApplyRollbackSegment(state: StateHandle, segment: Uint8Array, entrySize: number): void {
      const s = state as WasmStateHandle;
      const wasmU8 = new Uint8Array(wasmInstance.memory.buffer);
      const statePtr = wasmInstance.stateRegionOffset;
      wasmU8.set(new Uint8Array(s.buffer), statePtr);

      const segmentPtr = wasmInstance.inputRegionOffset;
      wasmU8.set(segment, segmentPtr);
      wasmInstance.exports.vm_delta_apply_rollback_segment(statePtr, segmentPtr, segment.byteLength, entrySize);

      const freshU8 = new Uint8Array(wasmInstance.memory.buffer);
      new Uint8Array(s.buffer).set(freshU8.subarray(statePtr, statePtr + s.size));
    },

    deltaApplyRollforwardSegment(state: StateHandle, segment: Uint8Array, entrySize: number): void {
      const s = state as WasmStateHandle;
      const wasmU8 = new Uint8Array(wasmInstance.memory.buffer);
      const statePtr = wasmInstance.stateRegionOffset;
      wasmU8.set(new Uint8Array(s.buffer), statePtr);

      const segmentPtr = wasmInstance.inputRegionOffset;
      wasmU8.set(segment, segmentPtr);
      wasmInstance.exports.vm_delta_apply_rollforward_segment(statePtr, segmentPtr, segment.byteLength, entrySize);

      const freshU8 = new Uint8Array(wasmInstance.memory.buffer);
      new Uint8Array(s.buffer).set(freshU8.subarray(statePtr, statePtr + s.size));
    },
  } as ColumineBackend;
}

// =============================================================================
// Loader - finds and loads columine.wasm
// =============================================================================

/**
 * Load columine's reducer-only WASM backend.
 *
 * Searches for columine.wasm in default locations relative to this module.
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
