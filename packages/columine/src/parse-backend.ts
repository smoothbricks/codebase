/**
 * Parse/Compact bridge to the native event-processor WASM module.
 *
 * Parse forwards JSON input. Compact validates and packs one typed CPB1 batch;
 * Arrow IPC encoding remains entirely native.
 */

import type { CompactBatch, CompactColumn, EncodedArrowSchema, ParseConfig, ParseResult } from './pipeline.js';
import {
  calculateRequiredWasmBytes,
  ensureWasmMemoryForWorkingSet,
  WASM_MAX_BYTES,
  WASM_MAX_PAGES,
} from './wasm-memory-contract.js';

export interface ParseCompactBackend {
  readonly backend: string;
  parse(input: string | Uint8Array, config: ParseConfig): ParseResult;
  encode(batch: CompactBatch): Uint8Array;
  dispose(): void;
}

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
  ep_compact(handle: number, batchPtr: number, batchLen: number, outputPtr: number, outputLen: number): number;
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
  const epCompact = exports.ep_compact;

  if (
    !(memory instanceof WebAssembly.Memory) ||
    !isWasmFunction<EventProcessorWasmExports['ep_version']>(epVersion) ||
    !isWasmFunction<EventProcessorWasmExports['ep_create_with_schema']>(epCreateWithSchema) ||
    !isWasmFunction<EventProcessorWasmExports['ep_create_with_schema_and_names']>(epCreateWithSchemaAndNames) ||
    !isWasmFunction<EventProcessorWasmExports['ep_destroy']>(epDestroy) ||
    !isWasmFunction<EventProcessorWasmExports['ep_create_log_entry']>(epCreateLogEntry) ||
    !isWasmFunction<EventProcessorWasmExports['ep_compact']>(epCompact)
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
    ep_compact: epCompact,
  };
}

const WASM_OUTPUT_HEADER_SIZE = 32;
const WASM_HEAP_RESERVE = 9 * 1024 * 1024;
const MIN_INPUT_BYTES = 4 * 1024;
const MIN_OUTPUT_BYTES = 64 * 1024;
const MIN_WORKSPACE_BYTES = 256 * 1024;
const FIXED_LAYOUT_OVERHEAD_BYTES = 64 * 1024;
const MAX_BATCH_INPUT_BYTES = 64 * 1024 * 1024;
const MAX_BATCH_OUTPUT_BYTES = 64 * 1024 * 1024;
const MAX_EVENTS_PER_BATCH = 65_536;
const MAX_FIELDS = 256;
const MAX_VARIABLE_DATA_BYTES = 16 * 1024 * 1024;
const MIN_COMPACT_ARROW_CAPACITY = 4 * 1024;
const UINT32_MAX = 0xffff_ffff;
const INPUT_FORMAT_JSON = 0;
const RESULT_OK = 0;
const COMPACT_MAGIC = 0x3142_5043;
const COMPACT_VERSION = 1;
const COMPACT_HEADER_SIZE = 16;
const COMPACT_DESCRIPTOR_SIZE = 32;
const HOST_IS_LITTLE_ENDIAN = new Uint8Array(new Uint16Array([1]).buffer)[0] === 1;

const COMPACT_KIND_TAG = {
  null: 0,
  u32: 1,
  f64: 2,
  binary: 3,
  utf8: 4,
  bool: 5,
  i64: 6,
} as const satisfies Record<CompactColumn['kind'], number>;

const COMPACT_STATUS_CODE = {
  1: 'INVALID_HANDLE',
  2: 'PARSE_ERROR',
  3: 'ENCODE_ERROR',
  4: 'OUT_OF_MEMORY',
  5: 'INVALID_FORMAT',
  6: 'INVALID_INPUT',
  7: 'SCHEMA_MISMATCH',
} as const;

export type CompactEncodingErrorCode =
  | (typeof COMPACT_STATUS_CODE)[keyof typeof COMPACT_STATUS_CODE]
  | 'UNKNOWN_STATUS';

function compactStatusCode(status: number): CompactEncodingErrorCode {
  switch (status) {
    case 1:
      return COMPACT_STATUS_CODE[1];
    case 2:
      return COMPACT_STATUS_CODE[2];
    case 3:
      return COMPACT_STATUS_CODE[3];
    case 4:
      return COMPACT_STATUS_CODE[4];
    case 5:
      return COMPACT_STATUS_CODE[5];
    case 6:
      return COMPACT_STATUS_CODE[6];
    case 7:
      return COMPACT_STATUS_CODE[7];
    default:
      return 'UNKNOWN_STATUS';
  }
}

export interface CompactDiagnostic {
  readonly version: number;
  readonly stage: number;
  readonly detail: number;
  readonly expectedType: number;
  readonly actualType: number;
  readonly fieldIndex: number;
  readonly rowIndex: number;
}

export class CompactEncodingError extends Error {
  readonly code: CompactEncodingErrorCode;

  constructor(
    readonly status: number,
    readonly diagnostic: CompactDiagnostic | null,
    message?: string,
  ) {
    const code = compactStatusCode(status);
    const diagnosticMessage =
      diagnostic === null
        ? ''
        : ` (detail=${diagnostic.detail}, field=${diagnostic.fieldIndex}, row=${diagnostic.rowIndex}, ` +
          `expectedType=${diagnostic.expectedType}, actualType=${diagnostic.actualType})`;
    super(message ?? `ep_compact failed with ${code} (${status})${diagnosticMessage}`);
    this.name = 'CompactEncodingError';
    this.code = code;
  }
}

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

interface CompactBufferPlan {
  readonly source: Uint8Array;
  readonly offset: number;
}

interface CompactColumnPlan {
  readonly tag: number;
  readonly validity: CompactBufferPlan | null;
  readonly offsets: CompactBufferPlan | null;
  readonly data: CompactBufferPlan | null;
  readonly dataElementBytes: 1 | 4 | 8;
}

interface CompactMemoryLayout {
  readonly schemaOffset: number;
  readonly fieldMetaOffset: number;
  readonly batchOffset: number;
  readonly batchLength: number;
  readonly outputOffset: number;
  readonly outputLength: number;
  readonly arrowCapacity: number;
  readonly requiredWorkingSetBytes: number;
  readonly columns: readonly CompactColumnPlan[];
}

function checkedAdd(left: number, right: number, label: string): number {
  if (!Number.isSafeInteger(left) || !Number.isSafeInteger(right) || left < 0 || right < 0) {
    throw new RangeError(`${label} must use non-negative safe-integer byte counts`);
  }
  const result = left + right;
  if (!Number.isSafeInteger(result)) {
    throw new RangeError(`${label} exceeds JavaScript safe-integer arithmetic`);
  }
  return result;
}

function align8(value: number, label = 'aligned byte count'): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`${label} must be a non-negative safe integer`);
  }
  const remainder = value % 8;
  return remainder === 0 ? value : checkedAdd(value, 8 - remainder, label);
}

function formatBytes(bytes: number): string {
  return `${bytes} bytes (${(bytes / (1024 * 1024)).toFixed(2)} MiB)`;
}

function estimateOutputBytes(inputBytes: number): number {
  const estimated = checkedAdd(inputBytes, 1024 * 1024, 'parse output estimate');
  return Math.min(MAX_BATCH_OUTPUT_BYTES, Math.max(MIN_OUTPUT_BYTES, align8(estimated)));
}

function estimateWorkspaceBytes(inputBytes: number, outputBytes: number): number {
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
  const regionsBytes =
    checkedAdd(
      checkedAdd(checkedAdd(schemaBytes, fieldMetaBytes, 'parse regions'), fieldNamesBytes, 'parse regions'),
      FIXED_LAYOUT_OVERHEAD_BYTES,
      'parse regions',
    ) + WASM_HEAP_RESERVE;
  const requiredWorkingSetBytes = calculateRequiredWasmBytes({
    inputBytes: inputLength,
    outputBytes: outputLength,
    workspaceBytes: workspaceLength,
    regionsBytes,
  });

  const inputOffset = WASM_HEAP_RESERVE;
  const outputOffset = checkedAdd(inputOffset, inputLength, 'parse output offset');
  const workspaceOffset = checkedAdd(outputOffset, outputLength, 'parse workspace offset');
  const schemaOffset = checkedAdd(workspaceOffset, workspaceLength, 'parse schema offset');
  const fieldMetaOffset = checkedAdd(schemaOffset, schemaBytes, 'parse metadata offset');
  const fieldNamesOffset = checkedAdd(fieldMetaOffset, fieldMetaBytes, 'parse field names offset');

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

function encodeFieldNames(names: readonly string[]): Uint8Array {
  const encoder = new TextEncoder();
  const parts = names.map((name) => encoder.encode(`${name}\0`));
  let totalLength = 0;
  for (const part of parts) {
    totalLength = checkedAdd(totalLength, part.length, 'encoded field names');
  }
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const part of parts) {
    result.set(part, offset);
    offset += part.length;
  }
  return result;
}

function bytesEqual(left: Uint8Array, right: Uint8Array): boolean {
  if (left.length !== right.length) {
    return false;
  }
  for (let index = 0; index < left.length; index += 1) {
    if (left[index] !== right[index]) {
      return false;
    }
  }
  return true;
}

function typedArrayBytes(value: ArrayBufferView): Uint8Array {
  return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
}

function requiredArrayValue<T>(values: ArrayLike<T>, index: number, label: string): T {
  const value = values[index];
  if (value === undefined) {
    throw new RangeError(`${label} is missing index ${index}`);
  }
  return value;
}

function assertUint8Array(value: unknown, label: string): asserts value is Uint8Array {
  if (!(value instanceof Uint8Array)) {
    throw new TypeError(`${label} must be a Uint8Array`);
  }
}

function assertUint32Array(value: unknown, label: string): asserts value is Uint32Array {
  if (!(value instanceof Uint32Array)) {
    throw new TypeError(`${label} must be a Uint32Array`);
  }
}

function validateTrailingBits(bytes: Uint8Array, rowCount: number, label: string): void {
  const usedBits = rowCount % 8;
  if (usedBits === 0 || bytes.length === 0) {
    return;
  }
  const unusedMask = (0xff << usedBits) & 0xff;
  const lastByte = requiredArrayValue(bytes, bytes.length - 1, label);
  if ((lastByte & unusedMask) !== 0) {
    throw new RangeError(`${label} has nonzero unused high bits`);
  }
}

function validityBitIsSet(validity: Uint8Array, row: number): boolean {
  return (requiredArrayValue(validity, row >>> 3, 'validity bitmap') & (1 << (row & 7))) !== 0;
}

function validateValidity(
  validity: unknown,
  nullable: boolean,
  rowCount: number,
  fieldIndex: number,
): Uint8Array | undefined {
  if (!nullable && validity !== undefined) {
    throw new TypeError(`columns[${fieldIndex}].validity must be omitted for a non-nullable field`);
  }
  if (validity === undefined) {
    return undefined;
  }
  assertUint8Array(validity, `columns[${fieldIndex}].validity`);
  const expectedLength = Math.ceil(rowCount / 8);
  if (validity.byteLength !== expectedLength) {
    throw new RangeError(
      `columns[${fieldIndex}].validity must contain exactly ${expectedLength} bytes for ${rowCount} rows`,
    );
  }
  validateTrailingBits(validity, rowCount, `columns[${fieldIndex}].validity`);
  return validity;
}

function validateVariableColumn(
  column: Extract<CompactColumn, { kind: 'binary' | 'utf8' }>,
  validity: Uint8Array | undefined,
  rowCount: number,
  fieldIndex: number,
): void {
  assertUint32Array(column.offsets, `columns[${fieldIndex}].offsets`);
  assertUint8Array(column.data, `columns[${fieldIndex}].data`);
  if (column.offsets.length !== rowCount + 1) {
    throw new RangeError(`columns[${fieldIndex}].offsets must contain exactly rowCount + 1 entries`);
  }
  if (column.data.byteLength > MAX_VARIABLE_DATA_BYTES) {
    throw new RangeError(`columns[${fieldIndex}].data exceeds the ${MAX_VARIABLE_DATA_BYTES}-byte variable-data limit`);
  }
  if (column.offsets[0] !== 0) {
    throw new RangeError(`columns[${fieldIndex}].offsets must start at zero`);
  }

  const decoder = column.kind === 'utf8' ? new TextDecoder('utf-8', { fatal: true }) : null;
  let previous = 0;
  for (let row = 0; row < rowCount; row += 1) {
    const next = requiredArrayValue(column.offsets, row + 1, `columns[${fieldIndex}].offsets`);
    if (next < previous || next > column.data.byteLength) {
      throw new RangeError(`columns[${fieldIndex}].offsets must be monotonic and within data`);
    }
    const valid = validity === undefined || validityBitIsSet(validity, row);
    if (!valid && next !== previous) {
      throw new RangeError(`columns[${fieldIndex}] null row ${row} must have an empty value interval`);
    }
    if (valid && decoder !== null) {
      try {
        decoder.decode(column.data.subarray(previous, next));
      } catch {
        throw new TypeError(`columns[${fieldIndex}] row ${row} is not valid UTF-8`);
      }
    }
    previous = next;
  }
  if (previous !== column.data.byteLength) {
    throw new RangeError(`columns[${fieldIndex}] final offset must equal data.byteLength`);
  }
}

function validateCompactBatch(batch: CompactBatch): void {
  if (typeof batch !== 'object' || batch === null) {
    throw new TypeError('Compact batch must be an object');
  }
  if (!Number.isInteger(batch.rowCount) || batch.rowCount < 0 || batch.rowCount > MAX_EVENTS_PER_BATCH) {
    throw new RangeError(`rowCount must be an integer in 0..${MAX_EVENTS_PER_BATCH}`);
  }
  if (typeof batch.schema !== 'object' || batch.schema === null) {
    throw new TypeError('batch.schema must be an EncodedArrowSchema');
  }
  assertUint8Array(batch.schema.schemaBytes, 'batch.schema.schemaBytes');
  assertUint8Array(batch.schema.fieldMetadata, 'batch.schema.fieldMetadata');
  const receivedColumns: unknown = batch.columns;
  if (!Array.isArray(receivedColumns)) {
    throw new TypeError('batch.columns must be an array');
  }

  const metadata = batch.schema.fieldMetadata;
  if (metadata.length % 4 !== 0) {
    throw new RangeError('fieldMetadata length must be a multiple of four');
  }
  const fieldCount = metadata.length / 4;
  if (fieldCount > MAX_FIELDS) {
    throw new RangeError(`fieldMetadata contains more than ${MAX_FIELDS} fields`);
  }
  if (fieldCount !== batch.columns.length) {
    throw new RangeError(`fieldMetadata field count ${fieldCount} does not match ${batch.columns.length} columns`);
  }

  for (let fieldIndex = 0; fieldIndex < fieldCount; fieldIndex += 1) {
    const metadataOffset = fieldIndex * 4;
    const tag = requiredArrayValue(metadata, metadataOffset, 'fieldMetadata');
    const nullableByte = requiredArrayValue(metadata, metadataOffset + 1, 'fieldMetadata');
    if (tag > COMPACT_KIND_TAG.i64) {
      throw new TypeError(`fieldMetadata field ${fieldIndex} has unknown physical type ${tag}`);
    }
    if (nullableByte !== 0 && nullableByte !== 1) {
      throw new TypeError(`fieldMetadata field ${fieldIndex} nullable byte must be zero or one`);
    }
    if (metadata[metadataOffset + 2] !== 0 || metadata[metadataOffset + 3] !== 0) {
      throw new TypeError(`fieldMetadata field ${fieldIndex} padding bytes must be zero`);
    }

    const column = batch.columns[fieldIndex];
    if (typeof column !== 'object' || column === null || !('kind' in column)) {
      throw new TypeError(`columns[${fieldIndex}] must be a CompactColumn`);
    }
    const expectedTag = COMPACT_KIND_TAG[column.kind];
    if (expectedTag === undefined || expectedTag !== tag) {
      throw new TypeError(
        `columns[${fieldIndex}] kind ${String(column.kind)} does not match metadata physical type ${tag}`,
      );
    }

    const nullable = nullableByte === 1;
    if (column.kind === 'null') {
      if (!nullable) {
        throw new TypeError(`columns[${fieldIndex}] null field must be nullable`);
      }
      continue;
    }

    const validity = validateValidity(column.validity, nullable, batch.rowCount, fieldIndex);
    switch (column.kind) {
      case 'u32':
        if (!(column.data instanceof Uint32Array)) {
          throw new TypeError(`columns[${fieldIndex}].data must be a Uint32Array`);
        }
        if (column.data.length !== batch.rowCount) {
          throw new RangeError(`columns[${fieldIndex}].data must contain exactly rowCount values`);
        }
        break;
      case 'f64':
        if (!(column.data instanceof Float64Array)) {
          throw new TypeError(`columns[${fieldIndex}].data must be a Float64Array`);
        }
        if (column.data.length !== batch.rowCount) {
          throw new RangeError(`columns[${fieldIndex}].data must contain exactly rowCount values`);
        }
        break;
      case 'i64':
        if (!(column.data instanceof BigInt64Array)) {
          throw new TypeError(`columns[${fieldIndex}].data must be a BigInt64Array`);
        }
        if (column.data.length !== batch.rowCount) {
          throw new RangeError(`columns[${fieldIndex}].data must contain exactly rowCount values`);
        }
        break;
      case 'bool': {
        assertUint8Array(column.data, `columns[${fieldIndex}].data`);
        const expectedLength = Math.ceil(batch.rowCount / 8);
        if (column.data.byteLength !== expectedLength) {
          throw new RangeError(`columns[${fieldIndex}].data must contain exactly ${expectedLength} bitmap bytes`);
        }
        validateTrailingBits(column.data, batch.rowCount, `columns[${fieldIndex}].data`);
        break;
      }
      case 'binary':
      case 'utf8':
        validateVariableColumn(column, validity, batch.rowCount, fieldIndex);
        break;
    }
  }
}

function planCompactMemoryLayout(batch: CompactBatch): CompactMemoryLayout {
  let batchLength = checkedAdd(
    COMPACT_HEADER_SIZE,
    batch.columns.length * COMPACT_DESCRIPTOR_SIZE,
    'Compact descriptor table',
  );
  batchLength = align8(batchLength, 'Compact descriptor table');
  let bodyBytes = 0;
  let bufferCount = 0;
  const columns: CompactColumnPlan[] = [];

  function planBuffer(source: Uint8Array | null, label: string, preserveEmpty = false): CompactBufferPlan | null {
    if (source === null) {
      return null;
    }
    if (source.byteLength === 0) {
      return preserveEmpty ? { source, offset: 0 } : null;
    }
    batchLength = align8(batchLength, label);
    const plan = { source, offset: batchLength };
    batchLength = checkedAdd(batchLength, source.byteLength, label);
    bodyBytes = checkedAdd(bodyBytes, align8(source.byteLength, label), 'Compact Arrow body');
    return plan;
  }

  for (let index = 0; index < batch.columns.length; index += 1) {
    const column = requiredArrayValue(batch.columns, index, 'Compact columns');
    const validitySource = column.kind === 'null' ? null : (column.validity ?? null);
    const validity = planBuffer(validitySource, `columns[${index}].validity`, true);
    let offsets: CompactBufferPlan | null = null;
    let data: CompactBufferPlan | null = null;
    let dataElementBytes: 1 | 4 | 8 = 1;

    switch (column.kind) {
      case 'null':
        break;
      case 'u32':
        bufferCount += 2;
        dataElementBytes = 4;
        data = planBuffer(typedArrayBytes(column.data), `columns[${index}].data`);
        break;
      case 'f64':
      case 'i64':
        bufferCount += 2;
        dataElementBytes = 8;
        data = planBuffer(typedArrayBytes(column.data), `columns[${index}].data`);
        break;
      case 'bool':
        bufferCount += 2;
        data = planBuffer(column.data, `columns[${index}].data`);
        break;
      case 'binary':
      case 'utf8':
        bufferCount += 3;
        offsets = planBuffer(typedArrayBytes(column.offsets), `columns[${index}].offsets`);
        data = planBuffer(column.data, `columns[${index}].data`);
        break;
    }

    columns.push({
      tag: COMPACT_KIND_TAG[column.kind],
      validity,
      offsets,
      data,
      dataElementBytes,
    });
  }

  batchLength = align8(batchLength, 'Compact batch length');
  if (batchLength > MAX_BATCH_INPUT_BYTES) {
    throw new RangeError(
      `Compact packed batch ${formatBytes(batchLength)} exceeds ${formatBytes(MAX_BATCH_INPUT_BYTES)}`,
    );
  }
  if (batchLength > UINT32_MAX) {
    throw new RangeError('Compact packed batch exceeds the native u32 length limit');
  }

  const fieldCount = batch.columns.length;
  const metadataBytes = align8(
    checkedAdd(
      76,
      checkedAdd(4 + 16 * bufferCount, 4 + 16 * fieldCount, 'Compact record metadata'),
      'Compact record metadata',
    ),
    'Compact record metadata',
  );
  let exactArrowLength = checkedAdd(batch.schema.schemaBytes.byteLength, 8, 'Compact Arrow output');
  exactArrowLength = checkedAdd(exactArrowLength, metadataBytes, 'Compact Arrow output');
  exactArrowLength = checkedAdd(exactArrowLength, bodyBytes, 'Compact Arrow output');
  exactArrowLength = checkedAdd(exactArrowLength, 8, 'Compact Arrow output');
  if (exactArrowLength > MAX_BATCH_OUTPUT_BYTES) {
    throw new RangeError(
      `Compact Arrow output ${formatBytes(exactArrowLength)} exceeds ${formatBytes(MAX_BATCH_OUTPUT_BYTES)}`,
    );
  }
  const arrowCapacity = Math.max(MIN_COMPACT_ARROW_CAPACITY, exactArrowLength);
  const outputLength = checkedAdd(WASM_OUTPUT_HEADER_SIZE, arrowCapacity, 'Compact output buffer');

  const schemaOffset = WASM_HEAP_RESERVE;
  const fieldMetaOffset = align8(
    checkedAdd(schemaOffset, batch.schema.schemaBytes.byteLength, 'Compact metadata offset'),
  );
  const batchOffset = align8(
    checkedAdd(fieldMetaOffset, batch.schema.fieldMetadata.byteLength, 'Compact batch offset'),
  );
  const outputOffset = align8(checkedAdd(batchOffset, batchLength, 'Compact output offset'));
  const requiredWorkingSetBytes = checkedAdd(outputOffset, outputLength, 'Compact working set');
  if (requiredWorkingSetBytes > WASM_MAX_BYTES || outputOffset > UINT32_MAX || outputLength > UINT32_MAX) {
    throw new RangeError(`Compact working set ${formatBytes(requiredWorkingSetBytes)} exceeds native WASM bounds`);
  }

  return {
    schemaOffset,
    fieldMetaOffset,
    batchOffset,
    batchLength,
    outputOffset,
    outputLength,
    arrowCapacity,
    requiredWorkingSetBytes,
    columns,
  };
}

function copyLittleEndian(memory: Uint8Array, offset: number, source: Uint8Array, elementBytes: 1 | 4 | 8): void {
  if (HOST_IS_LITTLE_ENDIAN || elementBytes === 1) {
    memory.set(source, offset);
    return;
  }
  for (let sourceOffset = 0; sourceOffset < source.byteLength; sourceOffset += elementBytes) {
    for (let byte = 0; byte < elementBytes; byte += 1) {
      memory[offset + sourceOffset + byte] = requiredArrayValue(
        source,
        sourceOffset + elementBytes - byte - 1,
        'Compact source buffer',
      );
    }
  }
}

function writeCompactBatch(memoryBuffer: ArrayBuffer, layout: CompactMemoryLayout, batch: CompactBatch): void {
  const memory = new Uint8Array(memoryBuffer);
  const view = new DataView(memoryBuffer);
  view.setUint32(layout.batchOffset, COMPACT_MAGIC, true);
  view.setUint16(layout.batchOffset + 4, COMPACT_VERSION, true);
  view.setUint16(layout.batchOffset + 6, COMPACT_DESCRIPTOR_SIZE, true);
  view.setUint32(layout.batchOffset + 8, batch.rowCount, true);
  view.setUint32(layout.batchOffset + 12, batch.columns.length, true);

  for (let index = 0; index < layout.columns.length; index += 1) {
    const column = requiredArrayValue(layout.columns, index, 'Compact column layout');
    const descriptor = layout.batchOffset + COMPACT_HEADER_SIZE + index * COMPACT_DESCRIPTOR_SIZE;
    view.setUint8(descriptor, column.tag);
    view.setUint8(descriptor + 1, column.validity === null ? 0 : 1);
    view.setUint16(descriptor + 2, 0, true);
    view.setUint32(descriptor + 4, column.validity?.offset ?? 0, true);
    view.setUint32(descriptor + 8, column.validity?.source.byteLength ?? 0, true);
    view.setUint32(descriptor + 12, column.offsets?.offset ?? 0, true);
    view.setUint32(descriptor + 16, column.offsets?.source.byteLength ?? 0, true);
    view.setUint32(descriptor + 20, column.data?.offset ?? 0, true);
    view.setUint32(descriptor + 24, column.data?.source.byteLength ?? 0, true);
    view.setUint32(descriptor + 28, 0, true);

    if (column.validity !== null) {
      memory.set(column.validity.source, layout.batchOffset + column.validity.offset);
    }
    if (column.offsets !== null) {
      copyLittleEndian(memory, layout.batchOffset + column.offsets.offset, column.offsets.source, 4);
    }
    if (column.data !== null) {
      copyLittleEndian(memory, layout.batchOffset + column.data.offset, column.data.source, column.dataElementBytes);
    }
  }
}

function readCompactDiagnostic(view: DataView, outputOffset: number): CompactDiagnostic {
  return {
    version: view.getUint8(outputOffset + 20),
    stage: view.getUint8(outputOffset + 21),
    detail: view.getUint8(outputOffset + 22),
    expectedType: view.getUint8(outputOffset + 23),
    actualType: view.getUint8(outputOffset + 24),
    fieldIndex: view.getUint16(outputOffset + 26, true),
    rowIndex: view.getUint32(outputOffset + 28, true),
  };
}

export function createParseCompactWasmBackend(wasm: EventProcessorWasmExports): ParseCompactBackend {
  interface CachedHandle {
    readonly handle: number;
    readonly schemaBytes: Uint8Array;
    readonly fieldMetadata: Uint8Array;
    readonly fieldNames: Uint8Array | null;
  }

  let cachedHandle: CachedHandle | null = null;
  let disposed = false;

  function schemaMatches(cached: CachedHandle, schema: EncodedArrowSchema): boolean {
    return bytesEqual(cached.schemaBytes, schema.schemaBytes) && bytesEqual(cached.fieldMetadata, schema.fieldMetadata);
  }

  function configMatches(cached: CachedHandle, config: ParseConfig, fieldNames: Uint8Array | null): boolean {
    return (
      schemaMatches(cached, config) &&
      ((cached.fieldNames === null && fieldNames === null) ||
        (cached.fieldNames !== null && fieldNames !== null && bytesEqual(cached.fieldNames, fieldNames)))
    );
  }

  function destroyCachedHandle(): void {
    if (cachedHandle !== null) {
      wasm.ep_destroy(cachedHandle.handle);
      cachedHandle = null;
    }
  }

  function assertOpen(): void {
    if (disposed) {
      throw new Error('Parse/Compact backend has been disposed');
    }
  }

  function cacheHandle(handle: number, schema: EncodedArrowSchema, fieldNames: Uint8Array | null): number {
    cachedHandle = {
      handle,
      schemaBytes: schema.schemaBytes.slice(),
      fieldMetadata: schema.fieldMetadata.slice(),
      fieldNames: fieldNames?.slice() ?? null,
    };
    return handle;
  }

  return {
    backend: 'event-processor-wasm',

    parse(input: string | Uint8Array, config: ParseConfig): ParseResult {
      assertOpen();
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

      let handle: number;
      if (cachedHandle && configMatches(cachedHandle, config, fieldNamesBuffer)) {
        handle = cachedHandle.handle;
      } else {
        destroyCachedHandle();
        const memory = new Uint8Array(wasm.memory.buffer);
        memory.set(config.schemaBytes, layout.schemaOffset);
        memory.set(config.fieldMetadata, layout.fieldMetaOffset);
        if (fieldNamesBuffer) {
          memory.set(fieldNamesBuffer, layout.fieldNamesOffset);
        }
        const fieldCount = config.fieldMetadata.length / 4;
        handle = fieldNamesBuffer
          ? wasm.ep_create_with_schema_and_names(
              256,
              layout.schemaOffset,
              config.schemaBytes.length,
              layout.fieldMetaOffset,
              fieldCount,
              layout.fieldNamesOffset,
              fieldNamesBuffer.length,
            )
          : wasm.ep_create_with_schema(
              256,
              layout.schemaOffset,
              config.schemaBytes.length,
              layout.fieldMetaOffset,
              fieldCount,
            );
        if (handle === 0) {
          throw new Error('Failed to create EventProcessor — ep_create_with_schema returned 0');
        }
        cacheHandle(handle, config, fieldNamesBuffer);
      }

      const memory = new Uint8Array(wasm.memory.buffer);
      memory.set(inputBytes, layout.inputOffset);
      let result: number;
      try {
        result = wasm.ep_create_log_entry(
          handle,
          layout.inputOffset,
          inputBytes.length,
          INPUT_FORMAT_JSON,
          layout.outputOffset,
          layout.outputLength,
        );
      } catch (error) {
        destroyCachedHandle();
        throw error;
      }
      if (result !== RESULT_OK) {
        destroyCachedHandle();
        throw new Error(`ep_create_log_entry failed with code ${result}`);
      }

      const view = new DataView(wasm.memory.buffer);
      const code = view.getUint32(layout.outputOffset, true);
      const arrowOffset = view.getUint32(layout.outputOffset + 4, true);
      const arrowLen = view.getUint32(layout.outputOffset + 8, true);
      const eventsProcessed = view.getUint32(layout.outputOffset + 12, true);
      if (code !== RESULT_OK) {
        destroyCachedHandle();
        throw new Error(`ep_create_log_entry returned error code ${code}`);
      }

      const arrowStart = checkedAdd(layout.outputOffset, arrowOffset, 'parse Arrow start');
      const arrowEnd = checkedAdd(arrowStart, arrowLen, 'parse Arrow end');
      const outputEnd = checkedAdd(layout.outputOffset, layout.outputLength, 'parse output end');
      if (arrowStart < layout.outputOffset || arrowEnd > outputEnd) {
        destroyCachedHandle();
        throw new Error(
          'ep_create_log_entry produced out-of-bounds output: ' +
            `offset=${arrowOffset}, len=${arrowLen}, outputLen=${layout.outputLength}`,
        );
      }
      const arrowIpc = new Uint8Array(arrowLen);
      arrowIpc.set(new Uint8Array(wasm.memory.buffer, arrowStart, arrowLen));
      return { arrowIpc, eventCount: eventsProcessed };
    },

    encode(batch: CompactBatch): Uint8Array {
      assertOpen();
      validateCompactBatch(batch);
      const layout = planCompactMemoryLayout(batch);
      let outputLength = layout.outputLength;
      let arrowCapacity = layout.arrowCapacity;
      ensureWasmMemoryForWorkingSet(
        wasm.memory,
        {
          inputBytes: layout.batchLength,
          outputBytes: outputLength,
          workspaceBytes: 0,
          regionsBytes: layout.outputOffset - layout.batchLength,
        },
        { maxPages: WASM_MAX_PAGES },
      );

      let handle: number;
      if (cachedHandle && schemaMatches(cachedHandle, batch.schema)) {
        handle = cachedHandle.handle;
      } else {
        destroyCachedHandle();
        const memory = new Uint8Array(wasm.memory.buffer);
        memory.set(batch.schema.schemaBytes, layout.schemaOffset);
        memory.set(batch.schema.fieldMetadata, layout.fieldMetaOffset);
        handle = wasm.ep_create_with_schema(
          256,
          layout.schemaOffset,
          batch.schema.schemaBytes.length,
          layout.fieldMetaOffset,
          batch.columns.length,
        );
        if (handle === 0) {
          throw new CompactEncodingError(7, null, 'ep_compact schema handle creation failed');
        }
        cacheHandle(handle, batch.schema, null);
      }

      writeCompactBatch(wasm.memory.buffer, layout, batch);
      let retriedForCapacity = false;
      for (;;) {
        new Uint8Array(wasm.memory.buffer, layout.outputOffset, WASM_OUTPUT_HEADER_SIZE).fill(0);
        let result: number;
        try {
          result = wasm.ep_compact(handle, layout.batchOffset, layout.batchLength, layout.outputOffset, outputLength);
        } catch (error) {
          destroyCachedHandle();
          throw error;
        }

        const view = new DataView(wasm.memory.buffer);
        const headerStatus = view.getUint32(layout.outputOffset, true);
        if (result !== headerStatus) {
          destroyCachedHandle();
          throw new Error(`ep_compact returned status ${result} but wrote result-header status ${headerStatus}`);
        }

        const arrowOffset = view.getUint32(layout.outputOffset + 4, true);
        const arrowLen = view.getUint32(layout.outputOffset + 8, true);
        if (result !== RESULT_OK) {
          if (
            !retriedForCapacity &&
            result === 3 &&
            arrowOffset === WASM_OUTPUT_HEADER_SIZE &&
            arrowLen > arrowCapacity
          ) {
            if (arrowLen > MAX_BATCH_OUTPUT_BYTES) {
              throw new CompactEncodingError(
                result,
                readCompactDiagnostic(view, layout.outputOffset),
                `ep_compact requires ${formatBytes(arrowLen)}, exceeding ${formatBytes(MAX_BATCH_OUTPUT_BYTES)}`,
              );
            }
            retriedForCapacity = true;
            arrowCapacity = Math.max(MIN_COMPACT_ARROW_CAPACITY, arrowLen);
            outputLength = checkedAdd(WASM_OUTPUT_HEADER_SIZE, arrowCapacity, 'Compact retry output buffer');
            const requiredWorkingSetBytes = checkedAdd(layout.outputOffset, outputLength, 'Compact retry working set');
            if (requiredWorkingSetBytes > WASM_MAX_BYTES) {
              throw new CompactEncodingError(
                result,
                readCompactDiagnostic(view, layout.outputOffset),
                `ep_compact retry requires ${formatBytes(requiredWorkingSetBytes)}, exceeding native WASM bounds`,
              );
            }
            ensureWasmMemoryForWorkingSet(
              wasm.memory,
              {
                inputBytes: layout.batchLength,
                outputBytes: outputLength,
                workspaceBytes: 0,
                regionsBytes: layout.outputOffset - layout.batchLength,
              },
              { maxPages: WASM_MAX_PAGES },
            );
            continue;
          }
          throw new CompactEncodingError(result, readCompactDiagnostic(view, layout.outputOffset));
        }

        const rowsEncoded = view.getUint32(layout.outputOffset + 12, true);
        const duplicates = view.getUint32(layout.outputOffset + 16, true);
        const arrowStart = checkedAdd(layout.outputOffset, arrowOffset, 'Compact Arrow start');
        const arrowEnd = checkedAdd(arrowStart, arrowLen, 'Compact Arrow end');
        const outputEnd = checkedAdd(layout.outputOffset, outputLength, 'Compact output end');
        if (
          arrowOffset !== WASM_OUTPUT_HEADER_SIZE ||
          rowsEncoded !== batch.rowCount ||
          duplicates !== 0 ||
          arrowLen > arrowCapacity ||
          arrowEnd > outputEnd
        ) {
          destroyCachedHandle();
          throw new Error(
            'ep_compact produced an invalid success header: ' +
              `offset=${arrowOffset}, len=${arrowLen}, rows=${rowsEncoded}, duplicates=${duplicates}`,
          );
        }

        const arrowIpc = new Uint8Array(arrowLen);
        arrowIpc.set(new Uint8Array(wasm.memory.buffer, arrowStart, arrowLen));
        return arrowIpc;
      }
    },

    dispose(): void {
      if (disposed) {
        return;
      }
      disposed = true;
      destroyCachedHandle();
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
