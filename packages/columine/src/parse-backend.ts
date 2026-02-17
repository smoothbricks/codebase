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

// =============================================================================
// WASM Memory Layout Constants
// =============================================================================

// Working buffer offsets — must be after the Zig heap (8MB)
const WASM_INPUT_OFFSET = 9 * 1024 * 1024; // 9MB
const WASM_INPUT_SIZE = 2 * 1024 * 1024; // 2MB
const WASM_OUTPUT_OFFSET = WASM_INPUT_OFFSET + WASM_INPUT_SIZE; // 11MB
const WASM_OUTPUT_SIZE = 2 * 1024 * 1024; // 2MB
const WASM_SCHEMA_OFFSET = WASM_OUTPUT_OFFSET + WASM_OUTPUT_SIZE; // 13MB
const WASM_SCHEMA_SIZE = 64 * 1024; // 64KB
const WASM_FIELD_META_OFFSET = WASM_SCHEMA_OFFSET + WASM_SCHEMA_SIZE;
const WASM_FIELD_META_SIZE = 4 * 1024; // 4KB

const INPUT_FORMAT_JSON = 0;
const RESULT_OK = 0;
const textEncoder = new TextEncoder();

// =============================================================================
// Field Name Encoding
// =============================================================================

/**
 * Encode field names as null-terminated concatenated string.
 * e.g., ["id", "type", "timestamp"] -> "id\0type\0timestamp\0"
 */
function encodeFieldNames(names: string[]): Uint8Array {
  const parts = new Array<Uint8Array>(names.length);
  let totalLen = 0;
  for (let i = 0; i < names.length; i++) {
    const part = textEncoder.encode(names[i]);
    parts[i] = part;
    totalLen += part.length + 1;
  }
  const result = new Uint8Array(totalLen);
  let offset = 0;
  for (const part of parts) {
    result.set(part, offset);
    offset += part.length;
    result[offset] = 0;
    offset += 1;
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
  return {
    backend: 'event-processor-wasm',

    parse(input: string | Uint8Array, config: ParseConfig): ParseResult {
      // Create a new handle for each parse call — the EventProcessor is
      // stateful (tracks field names, schema) so we create per-config
      const memory = new Uint8Array(wasm.memory.buffer);

      // Write schema to WASM memory
      if (config.schemaBytes.length > WASM_SCHEMA_SIZE) {
        throw new Error(`Schema bytes too large: ${config.schemaBytes.length} > ${WASM_SCHEMA_SIZE}`);
      }
      if (config.fieldMetadata.length > WASM_FIELD_META_SIZE) {
        throw new Error(`Field metadata too large: ${config.fieldMetadata.length} > ${WASM_FIELD_META_SIZE}`);
      }
      if (config.fieldMetadata.length % 4 !== 0) {
        throw new Error(`Field metadata must be a multiple of 4 bytes, got ${config.fieldMetadata.length}`);
      }

      memory.set(config.schemaBytes, WASM_SCHEMA_OFFSET);
      memory.set(config.fieldMetadata, WASM_FIELD_META_OFFSET);

      const fieldCount = config.fieldMetadata.length / 4;

      let handle: number;
      if (config.fieldNames && config.fieldNames.length > 0) {
        const fieldNamesBuffer = encodeFieldNames(config.fieldNames);
        const fieldNamesOffset = WASM_FIELD_META_OFFSET + config.fieldMetadata.length;
        if (fieldNamesOffset + fieldNamesBuffer.length > memory.byteLength) {
          throw new Error(
            `Field names exceed available WASM memory: offset=${fieldNamesOffset}, len=${fieldNamesBuffer.length}, memory=${memory.byteLength}`,
          );
        }
        memory.set(fieldNamesBuffer, fieldNamesOffset);

        handle = wasm.ep_create_with_schema_and_names(
          256, // default capacity
          WASM_SCHEMA_OFFSET,
          config.schemaBytes.length,
          WASM_FIELD_META_OFFSET,
          fieldCount,
          fieldNamesOffset,
          fieldNamesBuffer.length,
        );
      } else {
        handle = wasm.ep_create_with_schema(
          256,
          WASM_SCHEMA_OFFSET,
          config.schemaBytes.length,
          WASM_FIELD_META_OFFSET,
          fieldCount,
        );
      }

      if (handle === 0) {
        throw new Error('Failed to create EventProcessor — ep_create_with_schema returned 0');
      }

      try {
        // Encode input
        const inputBytes = typeof input === 'string' ? textEncoder.encode(input) : input;

        if (inputBytes.length > WASM_INPUT_SIZE) {
          throw new Error(`Input too large: ${inputBytes.length} > ${WASM_INPUT_SIZE}`);
        }

        // Get fresh memory view (handle creation may have grown memory)
        const mem = new Uint8Array(wasm.memory.buffer);
        mem.set(inputBytes, WASM_INPUT_OFFSET);

        const result = wasm.ep_create_log_entry(
          handle,
          WASM_INPUT_OFFSET,
          inputBytes.length,
          INPUT_FORMAT_JSON,
          WASM_OUTPUT_OFFSET,
          WASM_OUTPUT_SIZE,
        );

        if (result !== RESULT_OK) {
          throw new Error(`ep_create_log_entry failed with code ${result}`);
        }

        // Read result header from output buffer
        const view = new DataView(wasm.memory.buffer);
        const code = view.getUint32(WASM_OUTPUT_OFFSET, true);
        const arrowOffset = view.getUint32(WASM_OUTPUT_OFFSET + 4, true);
        const arrowLen = view.getUint32(WASM_OUTPUT_OFFSET + 8, true);
        const eventsProcessed = view.getUint32(WASM_OUTPUT_OFFSET + 12, true);

        if (code !== RESULT_OK) {
          throw new Error(`ep_create_log_entry returned error code ${code}`);
        }

        // Copy Arrow IPC bytes out of WASM memory
        const freshMem = new Uint8Array(wasm.memory.buffer);
        const arrowIpc = new Uint8Array(arrowLen);
        arrowIpc.set(freshMem.subarray(WASM_OUTPUT_OFFSET + arrowOffset, WASM_OUTPUT_OFFSET + arrowOffset + arrowLen));

        return { arrowIpc, eventCount: eventsProcessed };
      } finally {
        wasm.ep_destroy(handle);
      }
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
  const exports = instance.exports as unknown as EventProcessorWasmExports;

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
