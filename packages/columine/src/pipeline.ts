/**
 * Pipeline composition API for columine's four processing stages.
 *
 * Stages: Parse | Reduce | Compact | Undo
 *
 * Each stage is independently usable — you can use Reduce without Parse,
 * or Compact without Reduce. A consumer runtime composes columine's pipeline
 * with RETE as an additional stage.
 *
 *   const stages = createPipeline({ backend, parseBackend });
 *   const { arrowIpc, eventCount } = stages.parse.parse(json, config);
 *   stages.reduce.executeBatch(state, program, columns, batchLen);
 *   const output = stages.compact.encode(batch);
 *   const begun = stages.undo.checkpoint(state);
 *   if (!begun.ok) throw new Error(`undo begin refused: ${begun.error}`);
 *   stages.undo.rollback(state, begun.value);
 */

import type { CompactColumn } from './arrow-planes.js';
import type { ParseCompactBackend } from './parse-backend.js';
import type { ColumineBackend, ColumnInput, ReducerProgram, StateHandle, VmReadResult } from './types.js';

// =============================================================================
// Configuration Types
// =============================================================================

/** Schema input shared by Parse and Compact. */
export interface EncodedArrowSchema {
  /** One complete framed Arrow IPC Schema message. */
  readonly schemaBytes: Uint8Array;
  /**
   * Four bytes per field: `[tag u8, nullable u8, typeParam u16 LE]`.
   * `typeParam` is the FixedSizeBinary byte width for tag 19 and must be 0
   * for every other tag.
   */
  readonly fieldMetadata: Uint8Array;
}

export interface ParseConfig extends EncodedArrowSchema {
  /** Field names for JSON key matching (required for schemas with value.* fields). */
  readonly fieldNames?: readonly string[];
}

/**
 * A physical Arrow column accepted by Compact.
 *
 * Nested and dictionary-encoded types (List, LargeList, FixedSizeList, Struct,
 * Union, Map, RunEndEncoded, Dictionary) are excluded: the retained-metadata
 * contract encodes at most three buffers per field (`max_buffers =
 * MAX_SCHEMA_FIELDS * 3`). Nested types need a different layout contract, not
 * another enum arm.
 *
 * `kind` names the physical plane and each arm's typed array is the carrier
 * that plane's encoder reads — a kind/carrier disagreement cannot be
 * expressed. Date32/Time32 ride `i32`; Date64/Time64/Timestamp/Duration ride
 * `i64`. Interval does not: its elements are not scalars.
 *
 * `f16` carries raw IEEE-754 half bits in a `Uint16Array` (Float16Array is
 * not baseline). `intervalDayTime` is two `i32` values per row (days,
 * milliseconds), so `data.length === rowCount * 2`.
 */
export type { CompactColumn };

/** One complete record batch for Compact encoding. */
export interface CompactBatch {
  readonly rowCount: number;
  readonly schema: EncodedArrowSchema;
  readonly columns: readonly CompactColumn[];
}

export interface ParseResult {
  /** Arrow IPC record batch bytes */
  arrowIpc: Uint8Array;
  /** Number of events in the batch (after parse, no dedup) */
  eventCount: number;
}

/**
 * Opaque token returned by UndoStage.checkpoint().
 * Pass to rollback() or commit() to undo or finalize speculative changes.
 */
export interface UndoToken {
  readonly _brand: 'UndoToken';
}

// =============================================================================
// Stage Interfaces
// =============================================================================

/** Parse: JSON/msgpack bytes -> Arrow IPC record batch */
export interface ParseStage {
  readonly name: 'parse';
  /**
   * Parse JSON (or future msgpack) into Arrow IPC bytes.
   * Creates an EventProcessor handle, processes input, returns Arrow IPC.
   *
   * @param input - JSON string or UTF-8 bytes (JSON array of events)
   * @param config - Schema configuration for Arrow encoding
   * @returns Arrow IPC bytes and event count
   */
  parse(input: string | Uint8Array, config: ParseConfig): ParseResult;
}

/** Reduce: Arrow columns -> reduced state via bytecode VM */
export interface ReduceStage {
  readonly name: 'reduce';
  /**
   * Execute reducer bytecode against a batch of Arrow columns.
   * Delegates to ColumineBackend.executeBatch().
   *
   * @param state - Per-instance state handle
   * @param program - Compiled reducer program (shared across instances)
   * @param columns - Arrow columns from event batch
   * @param batchLen - Number of rows to process
   * @returns ErrorCode (0 = OK)
   */
  executeBatch(state: StateHandle, program: ReducerProgram, columns: ColumnInput[], batchLen: number): number;
}

/** Compact: typed Arrow columns -> one caller-owned Arrow IPC stream */
export interface CompactStage {
  readonly name: 'compact';
  /**
   * Encode one validated physical batch through the native event processor.
   * The returned exact-length byte array never aliases WASM memory.
   */
  encode(batch: CompactBatch): Uint8Array;
}

/** Undo: Roll back speculative state changes via undo log */
export interface UndoStage {
  readonly name: 'undo';
  /**
   * Begin an undo window, then take a checkpoint before speculative execution.
   * The savepoint is taken only after the native begin succeeds; a refused begin
   * (e.g. SPECULATION_ACTIVE) is returned as an error value and takes no savepoint.
   *
   * @param state - State handle to checkpoint
   * @returns Opaque undo token, or the native begin error
   */
  checkpoint(state: StateHandle): VmReadResult<UndoToken>;
  /**
   * Roll back all state mutations since the checkpoint token.
   * Replays the undo log in reverse to restore pre-speculation state.
   *
   * @param state - State handle to roll back
   * @param token - Token from a prior checkpoint() call
   * @returns 'ok' if every write in the window was journaled; 'overflow' if a prior write was refused,
   *   in which case the exact valid prefix was rolled back (no snapshot restore)
   */
  rollback(state: StateHandle, token: UndoToken): 'ok' | 'overflow';
  /**
   * Commit speculative changes — discards undo entries since the token.
   * After commit, the mutations become permanent and cannot be rolled back.
   *
   * @param state - State handle to commit
   * @param token - Token from a prior checkpoint() call
   */
  commit(state: StateHandle, token: UndoToken): void;
}

// =============================================================================
// Composed Pipeline
// =============================================================================

export interface ColumineStages {
  parse: ParseStage;
  reduce: ReduceStage;
  compact: CompactStage;
  undo: UndoStage;
}

// =============================================================================
// Stage Factory Implementations
// =============================================================================

/** Internal undo token containing the native journal position. */
interface InternalUndoToken extends UndoToken {
  position: number;
}

function assertInternalUndoToken(token: UndoToken): InternalUndoToken {
  if (typeof token !== 'object' || token === null) {
    // invariant throw: caller passed a non-pipeline token or corrupted token
    throw new Error('Invalid UndoToken: expected object token');
  }

  const position = 'position' in token ? token.position : undefined;
  if (typeof position !== 'number' || !Number.isInteger(position) || position < 0) {
    // invariant throw: caller passed a non-pipeline token or corrupted token
    throw new Error('Invalid UndoToken: missing or invalid checkpoint position');
  }

  return {
    _brand: 'UndoToken',
    position,
  };
}

function createReduceStage(backend: ColumineBackend): ReduceStage {
  return {
    name: 'reduce',
    executeBatch(state: StateHandle, program: ReducerProgram, columns: ColumnInput[], batchLen: number): number {
      return backend.executeBatch(state, program, columns, batchLen);
    },
  };
}

function createParseStage(parseBackend: ParseCompactBackend): ParseStage {
  return {
    name: 'parse',
    parse(input: string | Uint8Array, config: ParseConfig): ParseResult {
      return parseBackend.parse(input, config);
    },
  };
}

function createCompactStage(parseBackend: ParseCompactBackend): CompactStage {
  return {
    name: 'compact',
    encode(batch: CompactBatch): Uint8Array {
      return parseBackend.encode(batch);
    },
  };
}

//#region reducer-speculation-undo-log #undo-log #speculation
/** Create the mandatory native undo stage. */
function createUndoStage(backend: ColumineBackend): UndoStage {
  return {
    name: 'undo',

    checkpoint(state: StateHandle): VmReadResult<UndoToken> {
      const begun = backend.undoEnable(state);
      if (!begun.ok) return begun;
      const position = backend.undoCheckpoint(state);
      const token: InternalUndoToken = {
        _brand: 'UndoToken',
        position,
      };
      return { ok: true, value: token };
    },

    rollback(state: StateHandle, token: UndoToken): 'ok' | 'overflow' {
      const internal = assertInternalUndoToken(token);

      backend.undoRollback(state, internal.position);
      return backend.undoHasOverflow() ? 'overflow' : 'ok';
    },

    commit(state: StateHandle, token: UndoToken): void {
      const internal = assertInternalUndoToken(token);

      backend.undoCommit(state, internal.position);
    },
  };
}
//#endregion reducer-speculation-undo-log

// =============================================================================
// Pipeline Factory
// =============================================================================

//#region columine-package.pipeline #create-pipeline #four-stages #composable
export interface PipelineOptions {
  /** Concrete reducer backend owned by this pipeline instance */
  backend: ColumineBackend;
  /** Concrete Parse/Compact backend owned by this pipeline instance */
  parseBackend: ParseCompactBackend;
}

/**
 * Create a composed pipeline with all four stages.
 *
 * The Reduce and Undo stages use the concrete ColumineBackend supplied in
 * options. The Parse and Compact stages use the concrete ParseCompactBackend.
 *
 * @param options - Concrete reducer and Parse/Compact backends
 * @returns All four pipeline stages
 */
export function createPipeline(options: PipelineOptions): ColumineStages {
  const backend = options.backend;
  const parseBackend = options.parseBackend;

  return {
    parse: createParseStage(parseBackend),
    reduce: createReduceStage(backend),
    compact: createCompactStage(parseBackend),
    undo: createUndoStage(backend),
  };
}
//#endregion columine-package.pipeline
