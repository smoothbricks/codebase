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

/** Must match Zig AggType enum in vm.zig */
export enum AggType {
  SUM = 1,
  COUNT = 2,
  MIN = 3,
  MAX = 4,
  AVG = 5,
  SCALAR_U32 = 8,
  SCALAR_F64 = 9,
  SCALAR_I64 = 10,
  /** Lossless i64 sum for S.bigint()/S.i64()/S.timestamp() */
  SUM_I64 = 11,
  /** Lossless i64 min */
  MIN_I64 = 12,
  /** Lossless i64 max */
  MAX_I64 = 13,
}

/** Must match Zig SlotType enum in vm.zig */
export enum SlotType {
  HASHMAP = 0,
  HASHSET = 1,
  AGGREGATE = 2,
  ARRAY = 3,
  CONDITION_TREE = 4,
  SCALAR = 5,
  STRUCT_MAP = 6,
  ORDERED_LIST = 7,
  BITMAP = 8,
}

/**
 * Bit layout for SLOT_DEF/SLOT_STRUCT_MAP/SLOT_ORDERED_LIST `type_flags` byte.
 *
 * - bits 0-3: SlotType
 * - bit 4: has_ttl
 * - bit 5: has_evict_trigger
 * - bit 6: no_hashmap_timestamps (HASHMAP only; omit f64 timestamp side-array)
 * - bit 7: reserved
 */
export enum SlotTypeFlag {
  HAS_TTL = 0x10,
  HAS_EVICT_TRIGGER = 0x20,
  NO_HASHMAP_TIMESTAMPS = 0x40,
}

export enum StructFieldType {
  UINT32 = 0, // 4 bytes
  INT64 = 1, // 8 bytes
  FLOAT64 = 2, // 8 bytes
  BOOL = 3, // 1 byte (stored as u8)
  STRING = 4, // 4 bytes (interned u32)
  // Array types — in-row: 8 bytes (offset:u32 + length:u32 into per-slot arena)
  ARRAY_U32 = 5,
  ARRAY_I64 = 6,
  ARRAY_F64 = 7,
  ARRAY_STRING = 8,
  ARRAY_BOOL = 9,
}

/**
 * Comparison type for HashMap pick strategies (latest/max/min).
 * Determines how the 8-byte comparison column is interpreted in the VM.
 * Must match Zig CmpType enum in hashmap_ops.zig.
 */
export enum ComparisonType {
  /** Compare as unsigned 32-bit integers (string intern IDs, ordinals) */
  U32 = 0,
  /** Compare as 64-bit IEEE 754 floats (numeric fields, timestamps-as-f64) */
  F64 = 1,
  /** Compare as signed 64-bit integers (bigint timestamps) */
  I64 = 2,
}

export enum TtlStartOf {
  NONE = 0,
  SECOND = 1,
  MINUTE = 2,
  HOUR = 3,
  DAY = 4,
  WEEK = 5,
  MONTH = 6,
  QUARTER = 7,
  YEAR = 8,
}

export interface SlotTtlMetadata {
  readonly ttlSeconds: number;
  readonly graceSeconds: number;
  readonly timestampFieldIndex: number;
  readonly startOf: TtlStartOf;
  readonly hasEvictTrigger: boolean;
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
  | { type: SlotType.HASHMAP; capacity: number; storesTimestamps: boolean; ttl?: SlotTtlMetadata }
  | { type: SlotType.HASHSET; capacity: number; ttl?: SlotTtlMetadata }
  | { type: SlotType.BITMAP; capacity: number; ttl?: SlotTtlMetadata }
  | { type: SlotType.AGGREGATE; aggType: AggType }
  | { type: SlotType.SCALAR; aggType: AggType }
  | { type: SlotType.CONDITION_TREE }
  | { type: SlotType.STRUCT_MAP; capacity: number; fieldTypes: readonly StructFieldType[]; ttl?: SlotTtlMetadata }
  | {
      type: SlotType.ORDERED_LIST;
      capacity: number;
      elemType?: StructFieldType;
      fieldTypes?: readonly StructFieldType[];
    };

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
  BATCH_MAP_UPSERT_LATEST_IF = 0x28,
  BATCH_MAP_UPSERT_FIRST_IF = 0x29,
  BATCH_MAP_UPSERT_LAST_IF = 0x2a,
  BATCH_MAP_REMOVE_IF = 0x2b,
  BATCH_MAP_UPSERT_MAX_IF = 0x2c,
  BATCH_MAP_UPSERT_MIN_IF = 0x2d,

  // Batch HashSet ops
  BATCH_SET_INSERT = 0x30,
  BATCH_SET_REMOVE = 0x31,
  BATCH_SET_INSERT_IF = 0x33,

  // Batch bitmap ops (u32 ordinal membership)
  BATCH_BITMAP_ADD = 0x34,
  BATCH_BITMAP_REMOVE = 0x35,

  // Bitmap in-place set algebra (slot × slot)
  BATCH_BITMAP_AND = 0x36, // target_slot:u8, source_slot:u8
  BATCH_BITMAP_OR = 0x37,
  BATCH_BITMAP_ANDNOT = 0x38,
  BATCH_BITMAP_XOR = 0x39,

  // Bitmap in-place set algebra (slot × scratch)
  BATCH_BITMAP_AND_SCRATCH = 0x3a,
  BATCH_BITMAP_OR_SCRATCH = 0x3b,
  BATCH_BITMAP_ANDNOT_SCRATCH = 0x3c,
  BATCH_BITMAP_XOR_SCRATCH = 0x3d,

  // Batch Aggregate ops
  BATCH_AGG_SUM = 0x40,
  BATCH_AGG_COUNT = 0x41,
  BATCH_AGG_MIN = 0x42,
  BATCH_AGG_MAX = 0x43,
  BATCH_AGG_SUM_IF = 0x44,
  BATCH_AGG_COUNT_IF = 0x45,
  BATCH_AGG_MIN_IF = 0x46,
  BATCH_AGG_MAX_IF = 0x47,

  // i64 aggregate ops — lossless integer accumulation for S.bigint()/S.i64()/S.timestamp()
  BATCH_AGG_SUM_I64 = 0x49,
  BATCH_AGG_MIN_I64 = 0x4a,
  BATCH_AGG_MAX_I64 = 0x4b,

  // Struct map ops (0x18 init, 0x80 batch)
  SLOT_STRUCT_MAP = 0x18, // slot, type_flags, cap_lo, cap_hi, num_fields, [field_type × num_fields]
  BATCH_STRUCT_MAP_UPSERT_LAST = 0x80, // slot, key_col, num_vals, [val_col, field_idx] × num_vals, num_array_vals, [(offsets_col, values_col, field_idx) × num_array_vals]

  // Ordered list ops
  SLOT_ORDERED_LIST = 0x19, // slot, type_flags, cap_lo, cap_hi [, num_fields, field_type × num_fields]
  LIST_APPEND = 0x84, // slot, val_col
  LIST_APPEND_STRUCT = 0x85, // slot, num_vals, [(val_col, field_idx) × N]

  // Block-based reduce opcodes (body opcodes use same values as BATCH_* but process one element)
  FOR_EACH = 0xe0, // col, match_count, match_ids (u32 LE × match_count), body_len (u16 LE)
  /** @deprecated Use FOR_EACH */
  FOR_EACH_EVENT = 0xe0, // alias for backward compat
  FLAT_MAP = 0xe1, // offsets_col, parent_ts_col, inner_body_len (u16 LE: 2 bytes)
  // Columine's reducer opcodes end at 0x4F (except struct map at 0x80+, blocks at 0xE0+)
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
