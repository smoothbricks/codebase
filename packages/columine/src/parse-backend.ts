/**
 * Parse/Compact Backend - Bridge to event_processor WASM/FFI exports.
 *
 * Provides the ParseCompactBackend interface that pipeline.ts uses
 * for ParseStage.parse() and CompactStage.encode().
 *
 * The event_processor is a separate WASM module from the reducer VM.
 * It handles JSON-to-Arrow-IPC conversion (Parse) and Arrow IPC encoding (Compact).
 */

import type { ParseConfig, ParseResult } from './pipeline.js';
import type { ColumnInput } from './types.js';
import { calculateRequiredWasmBytes, ensureWasmMemoryForWorkingSet, WASM_MAX_PAGES } from './wasm-memory-contract.js';

// =============================================================================
// ParseCompactBackend Interface
// =============================================================================

/**
 * Backend interface for Parse and Compact stages.
 *
 * Wraps event_processor WASM/FFI exports. Separate from ColumineBackend
 * because the event_processor is a different WASM module with different
 * exports (ep_create_with_schema, ep_create_log_entry, etc.).
 */
export interface ParseCompactBackend {
  readonly backend: string;

  /**
   * Parse JSON/msgpack bytes into Arrow IPC record batch.
   * Delegates to event_processor's ep_create_log_entry.
   */
  parse(input: string | Uint8Array, config: ParseConfig): ParseResult;

  /**
   * Encode Arrow columns into Arrow IPC bytes.
   * Uses event_processor's IPC writer for Arrow encoding.
   */
  encode(columns: ColumnInput[], schema: Uint8Array): Uint8Array;
}

// =============================================================================
// Event Processor WASM Exports Interface
// =============================================================================

/**
 * WASM exports from event_processor.wasm (columine's Parse+Compact binary).
 *
 * These match the `export fn` declarations in event_processor.zig.
 * Note: no policy parameter — columine has no dedup.
 */
export interface EventProcessorWasmExports {
  memory: WebAssembly.Memory;
  ep_version(): number;
  ep_create_with_schema(
    capacity: number,
    schemaPtr: number,
    schemaLen: number,
    fieldMetaPtr: number,
    fieldCount: number,
  ): number;
  ep_create_with_schema_and_names(
    capacity: number,
    schemaPtr: number,
    schemaLen: number,
    fieldMetaPtr: number,
    fieldCount: number,
    fieldNamesPtr: number,
    fieldNamesLen: number,
  ): number;
  ep_destroy(handle: number): void;
  ep_create_log_entry(
    handle: number,
    inputPtr: number,
    inputLen: number,
    format: number,
    outputPtr: number,
    outputLen: number,
  ): number;
}

function isWasmFunction<T extends (...args: never[]) => unknown>(value: unknown): value is T {
  return typeof value === 'function';
}

function parseEventProcessorWasmExports(exports: WebAssembly.Instance['exports']): EventProcessorWasmExports {
  const memory = exports.memory;
  const epVersion = exports.ep_version;
  const epCreateWithSchema = exports.ep_create_with_schema;
  const epCreateWithSchemaAndNames = exports.ep_create_with_schema_and_names;
  const epDestroy = exports.ep_destroy;
  const epCreateLogEntry = exports.ep_create_log_entry;

  if (
    !(memory instanceof WebAssembly.Memory) ||
    !isWasmFunction<EventProcessorWasmExports['ep_version']>(epVersion) ||
    !isWasmFunction<EventProcessorWasmExports['ep_create_with_schema']>(epCreateWithSchema) ||
    !isWasmFunction<EventProcessorWasmExports['ep_create_with_schema_and_names']>(epCreateWithSchemaAndNames) ||
    !isWasmFunction<EventProcessorWasmExports['ep_destroy']>(epDestroy) ||
    !isWasmFunction<EventProcessorWasmExports['ep_create_log_entry']>(epCreateLogEntry)
  ) {
    throw new Error('event_processor.wasm missing expected exports');
  }

  return {
    memory,
    ep_version: epVersion,
    ep_create_with_schema: epCreateWithSchema,
    ep_create_with_schema_and_names: epCreateWithSchemaAndNames,
    ep_destroy: epDestroy,
    ep_create_log_entry: epCreateLogEntry,
  };
}

// =============================================================================
// WASM Memory Layout Constants
// =============================================================================

const WASM_OUTPUT_HEADER_SIZE = 16;

// Keep dynamic working regions after Zig heap + safety headroom.
const WASM_HEAP_RESERVE = 9 * 1024 * 1024;

// Parse backend per-call planning limits.
const MIN_INPUT_BYTES = 4 * 1024;
const MIN_OUTPUT_BYTES = 64 * 1024;
const MIN_WORKSPACE_BYTES = 256 * 1024;
const FIXED_LAYOUT_OVERHEAD_BYTES = 64 * 1024;
const MAX_BATCH_INPUT_BYTES = 64 * 1024 * 1024;
const MAX_BATCH_OUTPUT_BYTES = 64 * 1024 * 1024;

const INPUT_FORMAT_JSON = 0;
const RESULT_OK = 0;

interface ParseMemoryLayout {
  readonly inputOffset: number;
  readonly inputLength: number;
  readonly outputOffset: number;
  readonly outputLength: number;
  readonly workspaceOffset: number;
  readonly workspaceLength: number;
  readonly schemaOffset: number;
  readonly fieldMetaOffset: number;
  readonly fieldNamesOffset: number;
  readonly regionsBytes: number;
  readonly requiredWorkingSetBytes: number;
}

function align8(n: number): number {
  return (n + 7) & ~7;
}

function formatBytes(bytes: number): string {
  return `${bytes} bytes (${(bytes / (1024 * 1024)).toFixed(2)} MiB)`;
}

function estimateOutputBytes(inputBytes: number): number {
  // Output buffer should scale with expected Arrow payload, but a single batch payload
  // is capped at 64MB, so over-allocating 2x input only inflates required memory.
  const estimated = inputBytes + 1024 * 1024;
  return Math.min(MAX_BATCH_OUTPUT_BYTES, Math.max(MIN_OUTPUT_BYTES, align8(estimated)));
}

function estimateWorkspaceBytes(inputBytes: number, outputBytes: number): number {
  // Workspace budget accounts for parser scratch + transient conversion buffers.
  const estimated = Math.ceil(inputBytes * 0.5 + outputBytes * 0.25);
  return Math.max(MIN_WORKSPACE_BYTES, align8(estimated));
}

function planParseMemoryLayout(
  inputLen: number,
  schemaLen: number,
  fieldMetaLen: number,
  fieldNamesLen: number,
): ParseMemoryLayout {
  if (inputLen > MAX_BATCH_INPUT_BYTES) {
    throw new Error(
      `Parse input ${formatBytes(inputLen)} exceeds max batch input ${formatBytes(MAX_BATCH_INPUT_BYTES)}`,
    );
  }

  const inputLength = Math.max(MIN_INPUT_BYTES, align8(inputLen));
  const outputLength = Math.max(WASM_OUTPUT_HEADER_SIZE, estimateOutputBytes(inputLen));
  const workspaceLength = estimateWorkspaceBytes(inputLen, outputLength);

  const schemaBytes = align8(schemaLen);
  const fieldMetaBytes = align8(fieldMetaLen);
  const fieldNamesBytes = align8(fieldNamesLen);

  const regionsBytes = schemaBytes + fieldMetaBytes + fieldNamesBytes + FIXED_LAYOUT_OVERHEAD_BYTES + WASM_HEAP_RESERVE;
  const requiredWorkingSetBytes = calculateRequiredWasmBytes({
    inputBytes: inputLength,
    outputBytes: outputLength,
    workspaceBytes: workspaceLength,
    regionsBytes,
  });

  let offset = WASM_HEAP_RESERVE;

  const inputOffset = offset;
  offset += inputLength;

  const outputOffset = offset;
  offset += outputLength;

  const workspaceOffset = offset;
  offset += workspaceLength;

  const schemaOffset = offset;
  offset += schemaBytes;

  const fieldMetaOffset = offset;
  offset += fieldMetaBytes;

  const fieldNamesOffset = offset;
  offset += fieldNamesBytes;

  return {
    inputOffset,
    inputLength,
    outputOffset,
    outputLength,
    workspaceOffset,
    workspaceLength,
    schemaOffset,
    fieldMetaOffset,
    fieldNamesOffset,
    regionsBytes,
    requiredWorkingSetBytes,
  };
}

// =============================================================================
// Field Name Encoding
// =============================================================================

/**
 * Encode field names as null-terminated concatenated string.
 * e.g., ["id", "type", "timestamp"] -> "id\0type\0timestamp\0"
 */
function encodeFieldNames(names: string[]): Uint8Array {
  const encoder = new TextEncoder();
  const parts = names.map((n) => encoder.encode(`${n}\0`));
  const totalLen = parts.reduce((sum, p) => sum + p.length, 0);
  const result = new Uint8Array(totalLen);
  let offset = 0;
  for (const part of parts) {
    result.set(part, offset);
    offset += part.length;
  }
  return result;
}

// =============================================================================
// WASM-backed ParseCompactBackend
// =============================================================================

/**
 * Create a ParseCompactBackend from event_processor WASM exports.
 *
 * Each call to parse() with a new ParseConfig creates an EventProcessor
 * handle in WASM. For repeated use with the same config, callers should
 * reuse the pipeline stages object.
 *
 * @param wasm - Instantiated event_processor WASM exports
 */
export function createParseCompactWasmBackend(wasm: EventProcessorWasmExports): ParseCompactBackend {
  // Cache EventProcessor handle between parse() calls with the same config.
  // The EP is stateful (schema + field names) so we only reuse when config matches.
  // Key is a hash of schemaBytes + fieldMetadata + fieldNames for identity comparison.
  let cachedHandle: { configKey: string; handle: number } | null = null;

  /** Build a stable config identity key from schema bytes, field metadata, and field names */
  function configKey(config: ParseConfig): string {
    // Fast identity: concatenate lengths + first/last bytes as a fingerprint.
    // Full equality would require comparing all bytes, but configs are typically
    // stable across calls (same schema = same agent type).
    let key = `s${config.schemaBytes.length}:m${config.fieldMetadata.length}`;
    if (config.schemaBytes.length > 0) {
      key += `:${config.schemaBytes[0]}-${config.schemaBytes[config.schemaBytes.length - 1]}`;
    }
    if (config.fieldMetadata.length > 0) {
      key += `:${config.fieldMetadata[0]}-${config.fieldMetadata[config.fieldMetadata.length - 1]}`;
    }
    if (config.fieldNames) {
      key += `:n${config.fieldNames.join(',')}`;
    }
    return key;
  }

  return {
    backend: 'event-processor-wasm',

    parse(input: string | Uint8Array, config: ParseConfig): ParseResult {
      const inputBytes = typeof input === 'string' ? new TextEncoder().encode(input) : input;
      const fieldNamesBuffer =
        config.fieldNames && config.fieldNames.length > 0 ? encodeFieldNames(config.fieldNames) : null;

      const layout = planParseMemoryLayout(
        inputBytes.length,
        config.schemaBytes.length,
        config.fieldMetadata.length,
        fieldNamesBuffer?.length ?? 0,
      );

      try {
        ensureWasmMemoryForWorkingSet(
          wasm.memory,
          {
            inputBytes: layout.inputLength,
            outputBytes: layout.outputLength,
            workspaceBytes: layout.workspaceLength,
            regionsBytes: layout.regionsBytes,
          },
          { maxPages: WASM_MAX_PAGES },
        );
      } catch (error) {
        if (error instanceof Error) {
          throw new Error(
            `Failed to size parse WASM memory (${formatBytes(layout.requiredWorkingSetBytes)}): ${error.message}`,
          );
        }
        throw error;
      }

      // Reuse cached handle if config matches, otherwise create new one
      const key = configKey(config);
      let handle: number;

      if (cachedHandle && cachedHandle.configKey === key) {
        handle = cachedHandle.handle;
      } else {
        // Destroy old cached handle if config changed
        if (cachedHandle) {
          wasm.ep_destroy(cachedHandle.handle);
          cachedHandle = null;
        }

        const memory = new Uint8Array(wasm.memory.buffer);
        memory.set(config.schemaBytes, layout.schemaOffset);
        memory.set(config.fieldMetadata, layout.fieldMetaOffset);
        if (fieldNamesBuffer) {
          memory.set(fieldNamesBuffer, layout.fieldNamesOffset);
        }

        const fieldCount = config.fieldMetadata.length / 4;

        if (fieldNamesBuffer) {
          handle = wasm.ep_create_with_schema_and_names(
            256, // default capacity
            layout.schemaOffset,
            config.schemaBytes.length,
            layout.fieldMetaOffset,
            fieldCount,
            layout.fieldNamesOffset,
            fieldNamesBuffer.length,
          );
        } else {
          handle = wasm.ep_create_with_schema(
            256,
            layout.schemaOffset,
            config.schemaBytes.length,
            layout.fieldMetaOffset,
            fieldCount,
          );
        }

        if (handle === 0) {
          throw new Error('Failed to create EventProcessor — ep_create_with_schema returned 0');
        }

        cachedHandle = { configKey: key, handle };
      }

      // Get fresh memory view (handle creation or memory.grow may have changed buffer)
      const memory = new Uint8Array(wasm.memory.buffer);
      memory.set(inputBytes, layout.inputOffset);

      const result = wasm.ep_create_log_entry(
        handle,
        layout.inputOffset,
        inputBytes.length,
        INPUT_FORMAT_JSON,
        layout.outputOffset,
        layout.outputLength,
      );

      if (result !== RESULT_OK) {
        // Handle may be corrupted — invalidate cache
        cachedHandle = null;
        throw new Error(`ep_create_log_entry failed with code ${result}`);
      }

      // Read result header from output buffer
      const view = new DataView(wasm.memory.buffer);
      const code = view.getUint32(layout.outputOffset, true);
      const arrowOffset = view.getUint32(layout.outputOffset + 4, true);
      const arrowLen = view.getUint32(layout.outputOffset + 8, true);
      const eventsProcessed = view.getUint32(layout.outputOffset + 12, true);

      if (code !== RESULT_OK) {
        throw new Error(`ep_create_log_entry returned error code ${code}`);
      }

      const arrowStart = layout.outputOffset + arrowOffset;
      const arrowEnd = arrowStart + arrowLen;
      const outputEnd = layout.outputOffset + layout.outputLength;
      if (arrowStart < layout.outputOffset || arrowEnd > outputEnd) {
        throw new Error(
          'ep_create_log_entry produced out-of-bounds output: ' +
            `offset=${arrowOffset}, len=${arrowLen}, outputLen=${layout.outputLength}`,
        );
      }

      // Copy Arrow IPC bytes out of WASM memory
      const freshMem = new Uint8Array(wasm.memory.buffer);
      const arrowIpc = new Uint8Array(arrowLen);
      arrowIpc.set(freshMem.subarray(arrowStart, arrowEnd));

      return { arrowIpc, eventCount: eventsProcessed };
    },

    encode(_columns: ColumnInput[], _schema: Uint8Array): Uint8Array {
      // TODO: Implement direct Arrow IPC encoding from ColumnInput[].
      // This requires building a RecordBatch from raw columns, which
      // the event_processor currently does internally during parse.
      // For now, the Compact stage is primarily used via the parse path.
      // Direct encode() will be needed for Phase 29 (speculation output).
      throw new Error(
        'CompactStage.encode() is not yet implemented. ' + 'Use ParseStage.parse() for JSON-to-Arrow-IPC conversion.',
      );
    },
  };
}

// =============================================================================
// Loader - finds and loads event_processor.wasm
// =============================================================================

/**
 * Load event_processor WASM and create a ParseCompactBackend.
 *
 * @param wasmPath - Optional explicit path to event_processor.wasm
 * @returns ParseCompactBackend wrapping the WASM instance
 */
export async function loadParseBackend(wasmPath?: string | URL): Promise<ParseCompactBackend> {
  const wasmBytes = await loadWasmBytes(wasmPath, 'event_processor.wasm');
  if (!wasmBytes) {
    throw new Error(
      'Could not find event_processor.wasm. Provide an explicit path via loadParseBackend(path), ' +
        'or ensure event_processor.wasm is in ./event_processor.wasm or ../dist/event_processor.wasm.',
    );
  }

  const wasmModule = await WebAssembly.compile(wasmBytes);
  // event_processor exports its own memory — no imports needed
  const instance = await WebAssembly.instantiate(wasmModule, {});
  const exports = parseEventProcessorWasmExports(instance.exports);

  return createParseCompactWasmBackend(exports);
}

/**
 * Load WASM bytes from path or default locations.
 * Follows the same pattern as wasm-backend.ts.
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
      // File not found
    }
    return undefined;
  }

  const defaultPaths = [
    new URL(`./${defaultFileName}`, import.meta.url),
    new URL(`../dist/${defaultFileName}`, import.meta.url),
  ];

  for (const path of defaultPaths) {
    try {
      const response = await fetch(path);
      if (response.ok) {
        return await response.arrayBuffer();
      }
    } catch {
      // Try next
    }
  }

  return undefined;
}
