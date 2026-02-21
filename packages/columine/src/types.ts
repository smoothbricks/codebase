/**
 * @smoothbricks/columine - Types
 *
 * Generic columnar processing pipeline types.
 *
 * Four backend implementations, one interface:
 * - WASM (universal fallback, input columns copied)
 * - Bun FFI (zero-copy)
 * - Node.js NAPI (zero-copy)
 * - Expo Native Module (zero-copy, future)
 */

// =============================================================================
// Value Types
// =============================================================================

export enum ValueType {
  UINT32 = 3,
  FLOAT64 = 10,
}

export enum AggType {
  SUM = 1,
  COUNT = 2,
  MIN = 3,
  MAX = 4,
}

export enum SlotType {
  HASHMAP = 0,
  HASHSET = 1,
  AGGREGATE = 2,
  ARRAY = 3,
  CONDITION_TREE = 4,
}

// =============================================================================
// Program - compiled once per agent TYPE, immutable, shared
// =============================================================================

export interface ReducerProgram {
  readonly bytecode: Uint8Array;
  readonly numSlots: number;
  readonly numInputs: number;
  readonly slotDefs: readonly SlotDef[];
}

export type SlotDef =
  | { type: SlotType.HASHMAP; capacity: number }
  | { type: SlotType.HASHSET; capacity: number }
  | { type: SlotType.AGGREGATE; aggType: AggType }
  | { type: SlotType.CONDITION_TREE };

// =============================================================================
// State Handle - opaque reference to per-instance state
// =============================================================================

/**
 * Opaque handle to reducer state.
 *
 * Implementation varies by backend:
 * - WASM: WebAssembly.Memory + instance
 * - FFI: ArrayBuffer
 * - NAPI: ArrayBuffer
 */
export interface StateHandle {
  /** For debugging/inspection only */
  readonly _brand: 'ColumineStateHandle';
}

// =============================================================================
// Column Input - Arrow columns for batch processing
// =============================================================================

export interface ColumnInput {
  /** The actual data - Uint32Array or Float64Array */
  data: Uint32Array | Float64Array;
  /** Element type */
  type: ValueType;
}

// =============================================================================
// Error Codes
// =============================================================================

export enum ErrorCode {
  OK = 0,
  CAPACITY_EXCEEDED = 1,
  INVALID_PROGRAM = 2,
  INVALID_SLOT = 3,
  INVALID_STATE = 4,
  NEEDS_GROWTH = 5,
}

// =============================================================================
// Bytecode Constants
// =============================================================================

export const MAGIC = 0x314D4C43; // "CLM1"
export const HEADER_SIZE = 14;
/** Reserved bytes at program start for SHA-256 hash (content starts at offset 32) */
export const PROGRAM_HASH_PREFIX = 32;

export enum Opcode {
  HALT = 0x00,

  // Slot creation (init section) - must match vm.zig Opcode enum
  SLOT_DEF = 0x10, // slot, type_flags, cap_lo, cap_hi [aggType in cap_lo when type=AGGREGATE]

  // Batch HashMap ops
  BATCH_MAP_UPSERT_LATEST = 0x20,
  BATCH_MAP_UPSERT_FIRST = 0x21,
  BATCH_MAP_UPSERT_LAST = 0x22,
  BATCH_MAP_REMOVE = 0x23,
  // Max/Min pick strategies
  BATCH_MAP_UPSERT_MAX = 0x26,
  BATCH_MAP_UPSERT_MIN = 0x27,

  // Batch HashSet ops
  BATCH_SET_INSERT = 0x30,
  BATCH_SET_REMOVE = 0x31,

  // Batch Aggregate ops
  BATCH_AGG_SUM = 0x40,
  BATCH_AGG_COUNT = 0x41,
  BATCH_AGG_MIN = 0x42,
  BATCH_AGG_MAX = 0x43,
  // Columine's reducer opcodes end at 0x4F
}

// =============================================================================
// ColumineBackend Interface - implemented by all backends
// =============================================================================

/**
 * Backend interface for columine's columnar processing.
 *
 * executeBatchWithRete, but columine only uses the methods below.
 */
export interface ColumineBackend {
  readonly backend: string;

  // ===========================================================================
  // Program Loading
  // ===========================================================================

  /**
   * Parse and validate a program. Fills embedded hash if needed; caches by hash when available.
   * Call once per agent type.
   */
  loadProgram(bytecode: Uint8Array): Promise<ReducerProgram>;

  // ===========================================================================
  // State Management
  // ===========================================================================

  /**
   * Create state for a program.
   * Call ONCE per agent INSTANCE.
   * State is GC'd when handle is dropped.
   */
  createState(program: ReducerProgram): StateHandle;

  /**
   * Reset state to initial values.
   * Use for: testing, reprocessing from scratch, windowed reducers.
   */
  resetState(state: StateHandle, program: ReducerProgram): void;

  // ===========================================================================
  // Execution
  // ===========================================================================

  /**
   * Execute reduce bytecode for a batch of Arrow columns.
   *
   * @param state - State handle for this agent instance
   * @param program - Program (shared across instances of same type)
   * @param columns - Arrow columns from event batch
   * @param batchLen - Number of rows to process
   * @returns 0 = OK, 1 = CAPACITY_EXCEEDED, 2 = INVALID_PROGRAM
   */
  executeBatch(state: StateHandle, program: ReducerProgram, columns: ColumnInput[], batchLen: number): number;

  /**
   * Delta-enabled execution profile for fork journaling.
   * Emits mutation deltas for rollback/rollforward navigation.
   */
  executeBatchDelta?(state: StateHandle, program: ReducerProgram, columns: ColumnInput[], batchLen: number): number;

  // ===========================================================================
  // State Reading
  // ===========================================================================

  getMapSize(state: StateHandle, program: ReducerProgram, slot: number): number;
  getSetSize(state: StateHandle, program: ReducerProgram, slot: number): number;
  getAggregateValue(state: StateHandle, program: ReducerProgram, slot: number): number;

  mapGet(state: StateHandle, program: ReducerProgram, slot: number, key: number): number | undefined;
  setContains(state: StateHandle, program: ReducerProgram, slot: number, elem: number): boolean;

  // ===========================================================================
  // Serialization
  // ===========================================================================

  /**
   * Serialize state to bytes (for checkpoint).
   * Result can be gzip'd and stored in database.
   */
  serialize(state: StateHandle, program: ReducerProgram): Uint8Array;

  /**
   * Deserialize state from bytes.
   * Returns new state handle.
   */
  deserialize(program: ReducerProgram, data: Uint8Array): StateHandle;

  // ===========================================================================
  // Undo Log (Phase 29 - optional)
  // ===========================================================================

  /**
   * Enable undo logging. Call before speculative execution.
   * Saves change flags for rollback restoration.
   */
  undoEnable?(state: StateHandle): void;

  /**
   * Save current undo log position. Returns position as number.
   */
  undoCheckpoint?(state: StateHandle): number;

  /**
   * Rollback all mutations since the given checkpoint position.
   * After calling, the state buffer contains rolled-back state.
   */
  undoRollback?(state: StateHandle, checkpointPos: number): void;

  /**
   * Commit (discard) undo entries since the given checkpoint position.
   */
  undoCommit?(state: StateHandle, checkpointPos: number): void;

  /**
   * Check if undo log overflowed during speculation.
   * Used by UndoStage to report overflow status to callers for perf monitoring.
   */
  undoHasOverflow?(): boolean;

  /**
   * Export delta segment for mutations in [fromPos, toPos).
   */
  deltaExportSegment?(state: StateHandle, fromPos: number, toPos: number): DeltaSegmentExport;

  /**
   * Apply rollback deltas from a previously exported segment.
   */
  deltaApplyRollbackSegment?(state: StateHandle, segment: Uint8Array, entrySize: number): void;

  /**
   * Apply rollforward deltas from a previously exported segment.
   */
  deltaApplyRollforwardSegment?(state: StateHandle, segment: Uint8Array, entrySize: number): void;
}

export interface DeltaSegmentExport {
  undoBytes: Uint8Array;
  redoBytes: Uint8Array;
  entrySize: number;
  overflow: boolean;
}

/**
 * Backend variant with native undo log support.
 *
 * The base interface keeps undo methods optional so non-native backends can
 * still implement ColumineBackend, while this type captures the capability
 * contract when native undo is available.
 */
export interface UndoCapableColumineBackend extends ColumineBackend {
  undoEnable(state: StateHandle): void;
  undoCheckpoint(state: StateHandle): number;
  undoRollback(state: StateHandle, checkpointPos: number): void;
  undoCommit(state: StateHandle, checkpointPos: number): void;
  undoHasOverflow(): boolean;
}

export function isUndoCapableBackend(backend: ColumineBackend): backend is UndoCapableColumineBackend {
  return (
    typeof backend.undoEnable === 'function' &&
    typeof backend.undoCheckpoint === 'function' &&
    typeof backend.undoRollback === 'function' &&
    typeof backend.undoCommit === 'function' &&
    typeof backend.undoHasOverflow === 'function'
  );
}

export function assertUndoCapableBackend(backend: ColumineBackend): UndoCapableColumineBackend {
  if (!isUndoCapableBackend(backend)) {
    throw new Error('Backend does not support native undo operations');
  }
  return backend;
}
