/**
 * Pipeline composition API for columine's four processing stages.
 *
 * Stages: Parse | Reduce | Compact | Undo
 *
 * Each stage is independently usable — you can use Reduce without Parse,
 * with RETE as an additional stage.
 *
 * Usage:
 *   const stages = await createPipeline();
 *   const { arrowIpc, eventCount } = stages.parse.parse(json, config);
 *   stages.reduce.executeBatch(state, program, columns, batchLen);
 *   const output = stages.compact.encode(columns, schema);
 *   const token = stages.undo.checkpoint(state);
 *   stages.undo.rollback(state, token);
 */

import { getBackend } from './backend.js';
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

/**
 * Internal undo token — stores the undo log position at checkpoint time.
 * The undo log itself lives in the native state buffer (managed by Zig).
 */
interface InternalUndoToken extends UndoToken {
  /** Undo log position from native checkpoint (lightweight) */
  position: number;
  /** Fallback snapshot — only used if undo overflow detected or no native undo */
  snapshot: Uint8Array | null;
}

function assertInternalUndoToken(token: UndoToken): InternalUndoToken {
  const internal = token as Partial<InternalUndoToken>;
  const position = internal.position;
  if (!Number.isInteger(position) || (position as number) < 0) {
    // invariant throw: caller passed a non-pipeline token or corrupted token
    throw new Error('Invalid UndoToken: missing or invalid checkpoint position');
  }
  if (internal.snapshot !== null && !(internal.snapshot instanceof Uint8Array) && internal.snapshot !== undefined) {
    // invariant throw: caller passed a non-pipeline token or corrupted token
    throw new Error('Invalid UndoToken: snapshot must be Uint8Array | null');
  }
  return {
    _brand: 'UndoToken',
    position: position as number,
    snapshot: internal.snapshot ?? null,
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

function createParseStage(parseBackend: ParseCompactBackend | null): ParseStage {
  return {
    name: 'parse',
    parse(input: string | Uint8Array, config: ParseConfig): ParseResult {
      if (!parseBackend) {
        throw new Error(
          'Parse stage requires a parse backend. ' +
            'Call createPipeline({ parseBackend }) or use createPipelineWithParse().',
        );
      }
      return parseBackend.parse(input, config);
    },
  };
}

function createCompactStage(parseBackend: ParseCompactBackend | null): CompactStage {
  return {
    name: 'compact',
    encode(columns: ColumnInput[], schema: Uint8Array): Uint8Array {
      if (!parseBackend) {
        throw new Error(
          'Compact stage requires a parse backend. ' +
            'Call createPipeline({ parseBackend }) or use createPipelineWithParse().',
        );
      }
      return parseBackend.encode(columns, schema);
    },
  };
}

/**
 * Create undo stage with native undo log.
 *
 * Native path (fast): Uses Zig undo log via WASM/FFI exports.
 * checkpoint() saves log position (O(1), no memcpy), rollback() replays in reverse.
 * Undo log overflow is handled lazily inside Zig via shadow buffer — only when
 * the log actually exceeds capacity does a memcpy occur.
 *
 * Fallback path: If backend doesn't support native undo, falls back to
 * full-state snapshot for checkpoint/rollback.
 */
function createUndoStage(backend: ColumineBackend): UndoStage {
  // Check once at construction — backend capabilities don't change
  const hasNativeUndo = typeof backend.undoEnable === 'function';

  return {
    name: 'undo',

    checkpoint(state: StateHandle): UndoToken {
      if (hasNativeUndo) {
        // Enable undo logging, save change flags, store state pointer for lazy overflow
        backend.undoEnable!(state);
        const position = backend.undoCheckpoint!(state);
        // No snapshot needed — overflow is handled lazily inside Zig
        return {
          _brand: 'UndoToken',
          position,
          snapshot: null,
        } as InternalUndoToken;
      }
      // Fallback path: full snapshot only (no native undo available)
      const snapshot = backend.serialize(state, {} as ReducerProgram);
      return {
        _brand: 'UndoToken',
        position: 0,
        snapshot,
      } as InternalUndoToken;
    },

    rollback(state: StateHandle, token: UndoToken): 'ok' | 'overflow' {
      const internal = assertInternalUndoToken(token);

      if (hasNativeUndo) {
        // Native rollback — Zig handles overflow internally via shadow buffer:
        // if overflow occurred, it restores shadow then replays log;
        // if no overflow, it just replays log
        backend.undoRollback!(state, internal.position);
        const overflowed = backend.undoHasOverflow!();
        return overflowed ? 'overflow' : 'ok';
      }

      // Fallback: restore from snapshot (no native undo)
      if (internal.snapshot) {
        const stateAny = state as StateHandle & { buffer?: ArrayBuffer; size?: number };
        if (stateAny.buffer && internal.snapshot.length > 0) {
          if (internal.snapshot.length > stateAny.buffer.byteLength) {
            // invariant throw: snapshot/token does not belong to this state handle
            throw new Error('Invalid UndoToken: snapshot size exceeds target state buffer');
          }
          new Uint8Array(stateAny.buffer).set(internal.snapshot);
        }
      }
      return 'ok';
    },

    commit(state: StateHandle, token: UndoToken): void {
      const internal = assertInternalUndoToken(token);

      if (hasNativeUndo) {
        backend.undoCommit!(state, internal.position);
      }

      // Discard snapshot in all cases — mutations are now permanent
      internal.snapshot = null;
    },
  };
}

// =============================================================================
// Pipeline Factory
// =============================================================================

export interface PipelineOptions {
  /** Parse/Compact backend — if not provided, parse() and encode() will throw */
  parseBackend?: ParseCompactBackend;
}

/**
 * Create a composed pipeline with all four stages.
 *
 * The Reduce and Undo stages always work (they use the injected ColumineBackend).
 * The Parse and Compact stages require a ParseCompactBackend — pass one via options,
 * or they will throw with a helpful error message.
 *
 * @param options - Optional parse backend for Parse/Compact stages
 * @returns All four pipeline stages
 */
export async function createPipeline(options?: PipelineOptions): Promise<ColumineStages> {
  const backend = await getBackend();
  const parseBackend = options?.parseBackend ?? null;

  return {
    parse: createParseStage(parseBackend),
    reduce: createReduceStage(backend),
    compact: createCompactStage(parseBackend),
    undo: createUndoStage(backend),
  };
}
