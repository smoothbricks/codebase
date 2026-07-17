/**
 * Pipeline composition API for columine's four processing stages.
 *
 * Stages: Parse | Reduce | Compact | Undo
 *
 * Each stage is independently usable — you can use Reduce without Parse,
 * or Compact without Reduce. axe-runtime composes columine's pipeline
 * with RETE as an additional stage.
 *
 * Usage:
 *   const stages = createPipeline({ backend });
 *   const { arrowIpc, eventCount } = stages.parse.parse(json, config);
 *   stages.reduce.executeBatch(state, program, columns, batchLen);
 *   const output = stages.compact.encode(columns, schema);
 *   const token = stages.undo.checkpoint(state);
 *   stages.undo.rollback(state, token);
 */

import type { ParseCompactBackend } from './parse-backend.js';
import type { ColumineBackend, ColumnInput, ReducerProgram, StateHandle } from './types.js';

// =============================================================================
// Configuration Types
// =============================================================================

export interface ParseConfig {
  /** Pre-computed Arrow schema bytes (from extractSchemaMessage / Flechette) */
  schemaBytes: Uint8Array;
  /** Field metadata array — 4 bytes per field: [ArrowType, nullable, pad, pad] */
  fieldMetadata: Uint8Array;
  /** Field names for JSON key matching (required for schemas with value.* fields) */
  fieldNames?: string[];
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

/** Compact: Arrow columns -> Arrow IPC output bytes */
export interface CompactStage {
  readonly name: 'compact';
  /**
   * Encode Arrow columns into Arrow IPC bytes using the parse backend.
   * Produces a record batch from raw column data.
   *
   * @param columns - Column data to encode
   * @param schema - Pre-computed Arrow schema bytes
   * @returns Arrow IPC record batch bytes
   */
  encode(columns: ColumnInput[], schema: Uint8Array): Uint8Array;
}

/** Undo: Roll back speculative state changes via undo log */
export interface UndoStage {
  readonly name: 'undo';
  /**
   * Create a checkpoint before speculative execution.
   * The returned token captures the undo log position.
   *
   * @param state - State handle to checkpoint
   * @returns Opaque undo token
   */
  checkpoint(state: StateHandle): UndoToken;
  /**
   * Roll back all state mutations since the checkpoint token.
   * Replays the undo log in reverse to restore pre-speculation state.
   *
   * @param state - State handle to roll back
   * @param token - Token from a prior checkpoint() call
   * @returns 'ok' if undo log replay was sufficient, 'overflow' if shadow buffer was needed
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
    encode(columns: ColumnInput[], schema: Uint8Array): Uint8Array {
      return parseBackend.encode(columns, schema);
    },
  };
}

//#region axe!n/reducer-speculation-undo-log #undo-log #speculation
/** Create the mandatory native undo stage. */
function createUndoStage(backend: ColumineBackend): UndoStage {
  return {
    name: 'undo',

    checkpoint(state: StateHandle): UndoToken {
      backend.undoEnable(state);
      const position = backend.undoCheckpoint(state);
      const token: InternalUndoToken = {
        _brand: 'UndoToken',
        position,
      };
      return token;
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
//#endregion axe!n/reducer-speculation-undo-log

// =============================================================================
// Pipeline Factory
// =============================================================================

//#region axe!n/columine-package.pipeline #create-pipeline #four-stages #composable
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
//#endregion axe!n/columine-package.pipeline
