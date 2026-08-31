/**
 * Parse/Compact bridge to the native event-processor WASM module.
 *
 * Parse forwards JSON input. Compact validates and packs one typed CPB1 batch;
 * Arrow IPC encoding remains entirely native.
 */

import type { CompactBatch, CompactColumn, EncodedArrowSchema, ParseConfig, ParseResult } from './pipeline.js';
import {
  align8,
  calculateRequiredWasmBytes,
  ensureWasmMemoryForWorkingSet,
  loadWasmBytes,
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
/**
 * Ceiling for `batch.rowCount`, a VALIDATION limit: the native
 * `MAX_EVENTS_PER_BATCH` clamp in columine-arrow. Nothing is allocated for it.
 */
const MAX_EVENTS_PER_BATCH = 65_536;

/**
 * Event capacity passed to `ep_create_*`, an ALLOCATION SIZE: the native
 * `WASM_EVENT_CAPACITY`. It cannot be `MAX_EVENTS_PER_BATCH` — the native
 * `DynamicColumns::new` allocates the whole column plane eagerly, at
 * `min(MAX_VARIABLE_DATA_BYTES, capacity * 128)` data bytes plus
 * `(capacity + 1) * 4` offset bytes per variable-width column. Measured
 * against the shipped artifact: one utf8 column at capacity 65536 grows the
 * wasm heap 8.06 MiB and the seven-field compact schema needs 26.06 MiB, where
 * the same schema at 256 needs 0.10 MiB; past roughly 32 variable-width
 * columns the eager plane exceeds the artifact's 256 MiB memory maximum and
 * the allocator traps. Rows beyond it are not lost: `encode` hands the native
 * compact path a caller-owned batch of up to `MAX_EVENTS_PER_BATCH` rows
 * without touching the parse plane.
 */
const EP_EVENT_CAPACITY = 256;

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

/**
 * Physical plane tags — the `ArrowType` enum in columine-arrow, pinned by
 * `parse_backend_abi.rs`. The key is the plane's name on this side of the ABI
 * and must describe the same plane the tag selects natively.
 */
const COMPACT_KIND_TAG = {
  null: 0,
  i32: 1,
  f64: 2,
  binary: 3,
  utf8: 4,
  bool: 5,
  i64: 6,
  i8: 7,
  i16: 8,
  u8: 9,
  u16: 10,
  u32: 11,
  u64: 12,
  f16: 13,
  f32: 14,
  decimal128: 15,
  decimal256: 16,
  largeBinary: 17,
  largeUtf8: 18,
  fixedSizeBinary: 19,
  intervalYearMonth: 20,
  intervalDayTime: 21,
  intervalMonthDayNano: 22,
} as const satisfies Record<CompactColumn['kind'], number>;

const COMPACT_MAX_KIND_TAG = Math.max(...Object.values(COMPACT_KIND_TAG));
const MAX_FIXED_SIZE_BINARY_WIDTH = Math.floor(MAX_VARIABLE_DATA_BYTES / MAX_EVENTS_PER_BATCH);

const COMPACT_STATUS_CODE = {
  1: 'INVALID_HANDLE',
  2: 'PARSE_ERROR',
  3: 'ENCODE_ERROR',
  4: 'OUT_OF_MEMORY',
  5: 'INVALID_FORMAT',
  6: 'INVALID_INPUT',
  7: 'SCHEMA_MISMATCH',
} as const;

/**
 * Highest handle the wasm handle table can hand out: `HANDLES` is 256 slots
 * and slot 0 is reserved. Every return value outside `1..EP_MAX_HANDLE` is an
 * `EP_CREATE_FAILURE` code.
 */
const EP_MAX_HANDLE = 255;

/**
 * `ep_create_*` failure codes — the `CreateFailure` enum in
 * columine-event-processor, pinned by `parse_backend_abi.rs`.
 *
 * Creation used to answer every distinct refusal with a bare `0`, so a schema
 * type mismatch read exactly like a capacity refusal, a null pointer, and an
 * exhausted handle table.
 */
const EP_CREATE_FAILURE = {
  2147483649: 'BAD_REQUEST',
  2147483650: 'CAPACITY',
  2147483651: 'SCHEMA_MESSAGE',
  2147483652: 'SCHEMA_TOO_MANY_FIELDS',
  2147483653: 'SCHEMA_FIELD_METADATA',
  2147483654: 'SCHEMA_FIELD_COUNT',
  2147483655: 'SCHEMA_TYPE_MISMATCH',
  2147483656: 'SCHEMA_NULLABILITY',
  2147483657: 'SCHEMA_FIELD_NAMES',
  2147483658: 'INIT',
  2147483659: 'HANDLES_EXHAUSTED',
} as const;

const EP_CREATE_FAILURE_DETAIL = {
  BAD_REQUEST: 'a null pointer or an overflowing field count reached the wasm export',
  CAPACITY: 'the requested event capacity is zero or above the instance ceiling',
  SCHEMA_MESSAGE: 'schemaBytes is not one continuation-prefixed Arrow IPC Schema message',
  SCHEMA_TOO_MANY_FIELDS: `the schema declares more than ${MAX_FIELDS} fields`,
  SCHEMA_FIELD_COUNT: 'schemaBytes and fieldMetadata declare different field counts',
  SCHEMA_FIELD_METADATA: 'a fieldMetadata entry is not a valid [tag, nullable, typeParam u16 LE] descriptor',
  SCHEMA_TYPE_MISMATCH: "a fieldMetadata physical tag disagrees with that field's logical Arrow type in schemaBytes",
  SCHEMA_NULLABILITY: 'a field nullability flag disagrees with schemaBytes, or a Null field is non-nullable',
  SCHEMA_FIELD_NAMES: 'the field-name blob is malformed or does not carry one name per field',
  INIT: 'retained-metadata or extraction-config limits refused the schema',
  HANDLES_EXHAUSTED: `all ${EP_MAX_HANDLE} EventProcessor handle slots are in use`,
} as const satisfies Record<(typeof EP_CREATE_FAILURE)[keyof typeof EP_CREATE_FAILURE], string>;

export type EventProcessorCreateErrorCode =
  | (typeof EP_CREATE_FAILURE)[keyof typeof EP_CREATE_FAILURE]
  | 'UNKNOWN_FAILURE';

function epCreateFailureCode(status: number): EventProcessorCreateErrorCode {
  switch (status) {
    case 0x8000_0001:
      return EP_CREATE_FAILURE[0x8000_0001];
    case 0x8000_0002:
      return EP_CREATE_FAILURE[0x8000_0002];
    case 0x8000_0003:
      return EP_CREATE_FAILURE[0x8000_0003];
    case 0x8000_0004:
      return EP_CREATE_FAILURE[0x8000_0004];
    case 0x8000_0005:
      return EP_CREATE_FAILURE[0x8000_0005];
    case 0x8000_0006:
      return EP_CREATE_FAILURE[0x8000_0006];
    case 0x8000_0007:
      return EP_CREATE_FAILURE[0x8000_0007];
    case 0x8000_0008:
      return EP_CREATE_FAILURE[0x8000_0008];
    case 0x8000_0009:
      return EP_CREATE_FAILURE[0x8000_0009];
    case 0x8000_000a:
      return EP_CREATE_FAILURE[0x8000_000a];
    case 0x8000_000b:
      return EP_CREATE_FAILURE[0x8000_000b];
    default:
      return 'UNKNOWN_FAILURE';
  }
}

/**
 * `ep_create_with_schema[_and_names]` produced no handle.
 *
 * Deliberately not a {@link CompactEncodingError}: a handle that was never
 * created cannot have produced a compact status or diagnostic, and reporting
 * creation failures as `SCHEMA_MISMATCH` (status 7) made a real regression
 * indistinguishable from the mismatch an encode is supposed to detect.
 */
export class EventProcessorCreateError extends Error {
  readonly code: EventProcessorCreateErrorCode;

  constructor(
    readonly status: number,
    context: string,
  ) {
    const code = epCreateFailureCode(status);
    const detail =
      code === 'UNKNOWN_FAILURE'
        ? 'the wasm export returned no known handle or failure code'
        : EP_CREATE_FAILURE_DETAIL[code];
    super(`${context}: ${code} (0x${status.toString(16)}) — ${detail}`);
    this.name = 'EventProcessorCreateError';
    this.code = code;
  }
}

/**
 * A handle, or the named reason there is none.
 *
 * `returned` arrives as the wasm i32 JavaScript sees, i.e. sign-extended:
 * every `CreateFailure` code has bit 31 set, so `0x8000_0007` reaches here as
 * `-2147483641`. Reinterpret as unsigned before decoding.
 */
function requireEpHandle(returned: number, context: string): number {
  const handle = returned >>> 0;
  if (handle >= 1 && handle <= EP_MAX_HANDLE) {
    return handle;
  }
  throw new EventProcessorCreateError(handle, context);
}

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
  readonly dataElementBytes: 1 | 2 | 4 | 8;
  readonly offsetElementBytes: 4 | 8;
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
  column: Extract<CompactColumn, { kind: 'binary' | 'utf8' | 'largeBinary' | 'largeUtf8' }>,
  validity: Uint8Array | undefined,
  rowCount: number,
  fieldIndex: number,
): void {
  const large = column.kind === 'largeBinary' || column.kind === 'largeUtf8';
  if (large) {
    if (!(column.offsets instanceof BigInt64Array)) {
      throw new TypeError(`columns[${fieldIndex}].offsets must be a BigInt64Array`);
    }
  } else {
    assertUint32Array(column.offsets, `columns[${fieldIndex}].offsets`);
  }
  const offsets = column.offsets;
  assertUint8Array(column.data, `columns[${fieldIndex}].data`);
  if (offsets.length !== rowCount + 1) {
    throw new RangeError(`columns[${fieldIndex}].offsets must contain exactly rowCount + 1 entries`);
  }
  if (column.data.byteLength > MAX_VARIABLE_DATA_BYTES) {
    throw new RangeError(`columns[${fieldIndex}].data exceeds the ${MAX_VARIABLE_DATA_BYTES}-byte variable-data limit`);
  }

  const offsetAt = (index: number): bigint => {
    if (offsets instanceof BigInt64Array) {
      return requiredArrayValue(offsets, index, `columns[${fieldIndex}].offsets`);
    }
    return BigInt(requiredArrayValue(offsets, index, `columns[${fieldIndex}].offsets`));
  };
  const dataLength = BigInt(column.data.byteLength);
  if (offsetAt(0) !== 0n) {
    throw new RangeError(`columns[${fieldIndex}].offsets must start at zero`);
  }

  const decoder =
    column.kind === 'utf8' || column.kind === 'largeUtf8' ? new TextDecoder('utf-8', { fatal: true }) : null;
  let previous = 0n;
  for (let row = 0; row < rowCount; row += 1) {
    const next = offsetAt(row + 1);
    if (next < previous || next > dataLength) {
      throw new RangeError(`columns[${fieldIndex}].offsets must be monotonic and within data`);
    }
    const valid = validity === undefined || validityBitIsSet(validity, row);
    if (!valid && next !== previous) {
      throw new RangeError(`columns[${fieldIndex}] null row ${row} must have an empty value interval`);
    }
    if (valid && decoder !== null) {
      try {
        decoder.decode(column.data.subarray(Number(previous), Number(next)));
      } catch {
        throw new TypeError(`columns[${fieldIndex}] row ${row} is not valid UTF-8`);
      }
    }
    previous = next;
  }
  if (previous !== dataLength) {
    throw new RangeError(`columns[${fieldIndex}] final offset must equal data.byteLength`);
  }
}

function validateFixedValues(
  data: ArrayBufferView & { readonly length: number },
  ctor: new (length: number) => ArrayBufferView,
  fieldIndex: number,
  expectedValues: number,
): void {
  if (!(data instanceof ctor)) {
    const article = ctor.name === 'Int32Array' || ctor.name === 'Int8Array' || ctor.name === 'Int16Array' ? 'an' : 'a';
    throw new TypeError(`columns[${fieldIndex}].data must be ${article} ${ctor.name}`);
  }
  if (data.length !== expectedValues) {
    throw new RangeError(`columns[${fieldIndex}].data must contain exactly ${expectedValues} values`);
  }
}

function validateFixedBytes(data: Uint8Array, fieldIndex: number, expectedBytes: number): void {
  if (data.byteLength !== expectedBytes) {
    throw new RangeError(`columns[${fieldIndex}].data must contain exactly ${expectedBytes} bytes`);
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
    const typeParam =
      requiredArrayValue(metadata, metadataOffset + 2, 'fieldMetadata') |
      (requiredArrayValue(metadata, metadataOffset + 3, 'fieldMetadata') << 8);
    if (tag > COMPACT_MAX_KIND_TAG) {
      throw new TypeError(
        `fieldMetadata field ${fieldIndex} has unknown physical type ${tag} (max ${COMPACT_MAX_KIND_TAG})`,
      );
    }
    if (nullableByte !== 0 && nullableByte !== 1) {
      throw new TypeError(`fieldMetadata field ${fieldIndex} nullable byte must be zero or one`);
    }
    if (tag === COMPACT_KIND_TAG.fixedSizeBinary) {
      if (typeParam < 1 || typeParam > MAX_FIXED_SIZE_BINARY_WIDTH) {
        throw new RangeError(
          `fieldMetadata field ${fieldIndex} FixedSizeBinary width must be in 1..${MAX_FIXED_SIZE_BINARY_WIDTH}`,
        );
      }
    } else if (typeParam !== 0) {
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
    const rows = batch.rowCount;
    switch (column.kind) {
      case 'i8':
        validateFixedValues(column.data, Int8Array, fieldIndex, rows);
        break;
      case 'u8':
        validateFixedValues(column.data, Uint8Array, fieldIndex, rows);
        break;
      case 'i16':
        validateFixedValues(column.data, Int16Array, fieldIndex, rows);
        break;
      case 'u16':
      case 'f16':
        validateFixedValues(column.data, Uint16Array, fieldIndex, rows);
        break;
      case 'i32':
      case 'intervalYearMonth':
        validateFixedValues(column.data, Int32Array, fieldIndex, rows);
        break;
      case 'u32':
        validateFixedValues(column.data, Uint32Array, fieldIndex, rows);
        break;
      case 'f32':
        validateFixedValues(column.data, Float32Array, fieldIndex, rows);
        break;
      case 'intervalDayTime':
        validateFixedValues(column.data, Int32Array, fieldIndex, rows * 2);
        break;
      case 'i64':
        validateFixedValues(column.data, BigInt64Array, fieldIndex, rows);
        break;
      case 'u64':
        validateFixedValues(column.data, BigUint64Array, fieldIndex, rows);
        break;
      case 'f64':
        validateFixedValues(column.data, Float64Array, fieldIndex, rows);
        break;
      case 'bool': {
        assertUint8Array(column.data, `columns[${fieldIndex}].data`);
        const expectedLength = Math.ceil(rows / 8);
        if (column.data.byteLength !== expectedLength) {
          throw new RangeError(`columns[${fieldIndex}].data must contain exactly ${expectedLength} bitmap bytes`);
        }
        validateTrailingBits(column.data, rows, `columns[${fieldIndex}].data`);
        break;
      }
      case 'decimal128':
        assertUint8Array(column.data, `columns[${fieldIndex}].data`);
        validateFixedBytes(column.data, fieldIndex, rows * 16);
        break;
      case 'decimal256':
        assertUint8Array(column.data, `columns[${fieldIndex}].data`);
        validateFixedBytes(column.data, fieldIndex, rows * 32);
        break;
      case 'intervalMonthDayNano':
        assertUint8Array(column.data, `columns[${fieldIndex}].data`);
        validateFixedBytes(column.data, fieldIndex, rows * 16);
        break;
      case 'fixedSizeBinary':
        assertUint8Array(column.data, `columns[${fieldIndex}].data`);
        validateFixedBytes(column.data, fieldIndex, rows * typeParam);
        break;
      case 'binary':
      case 'utf8':
      case 'largeBinary':
      case 'largeUtf8':
        validateVariableColumn(column, validity, rows, fieldIndex);
        break;
      default: {
        const _exhaustive: never = column;
        throw new TypeError(`columns[${fieldIndex}] has unhandled kind ${String((_exhaustive as CompactColumn).kind)}`);
      }
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
    let dataElementBytes: 1 | 2 | 4 | 8 = 1;
    let offsetElementBytes: 4 | 8 = 4;

    switch (column.kind) {
      case 'null':
        break;
      case 'i8':
      case 'u8':
        bufferCount += 2;
        data = planBuffer(typedArrayBytes(column.data), `columns[${index}].data`);
        break;
      case 'bool':
        bufferCount += 2;
        data = planBuffer(column.data, `columns[${index}].data`);
        break;
      case 'i16':
      case 'u16':
      case 'f16':
        bufferCount += 2;
        dataElementBytes = 2;
        data = planBuffer(typedArrayBytes(column.data), `columns[${index}].data`);
        break;
      case 'i32':
      case 'u32':
      case 'f32':
      case 'intervalYearMonth':
      case 'intervalDayTime':
        bufferCount += 2;
        dataElementBytes = 4;
        data = planBuffer(typedArrayBytes(column.data), `columns[${index}].data`);
        break;
      case 'i64':
      case 'u64':
      case 'f64':
        bufferCount += 2;
        dataElementBytes = 8;
        data = planBuffer(typedArrayBytes(column.data), `columns[${index}].data`);
        break;
      case 'decimal128':
      case 'decimal256':
      case 'fixedSizeBinary':
      case 'intervalMonthDayNano':
        bufferCount += 2;
        data = planBuffer(column.data, `columns[${index}].data`);
        break;
      case 'binary':
      case 'utf8':
        bufferCount += 3;
        offsets = planBuffer(typedArrayBytes(column.offsets), `columns[${index}].offsets`);
        data = planBuffer(column.data, `columns[${index}].data`);
        break;
      case 'largeBinary':
      case 'largeUtf8':
        bufferCount += 3;
        offsetElementBytes = 8;
        offsets = planBuffer(typedArrayBytes(column.offsets), `columns[${index}].offsets`);
        data = planBuffer(column.data, `columns[${index}].data`);
        break;
      default: {
        const _exhaustive: never = column;
        throw new TypeError(`unhandled CompactColumn kind ${String((_exhaustive as CompactColumn).kind)}`);
      }
    }

    columns.push({
      tag: COMPACT_KIND_TAG[column.kind],
      validity,
      offsets,
      data,
      dataElementBytes,
      offsetElementBytes,
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

function copyLittleEndian(memory: Uint8Array, offset: number, source: Uint8Array, elementBytes: 1 | 2 | 4 | 8): void {
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
      copyLittleEndian(
        memory,
        layout.batchOffset + column.offsets.offset,
        column.offsets.source,
        column.offsetElementBytes,
      );
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
        handle = requireEpHandle(
          fieldNamesBuffer
            ? wasm.ep_create_with_schema_and_names(
                EP_EVENT_CAPACITY,
                layout.schemaOffset,
                config.schemaBytes.length,
                layout.fieldMetaOffset,
                fieldCount,
                layout.fieldNamesOffset,
                fieldNamesBuffer.length,
              )
            : wasm.ep_create_with_schema(
                EP_EVENT_CAPACITY,
                layout.schemaOffset,
                config.schemaBytes.length,
                layout.fieldMetaOffset,
                fieldCount,
              ),
          'parse could not create an EventProcessor',
        );
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
        handle = requireEpHandle(
          wasm.ep_create_with_schema(
            EP_EVENT_CAPACITY,
            layout.schemaOffset,
            batch.schema.schemaBytes.length,
            layout.fieldMetaOffset,
            batch.columns.length,
          ),
          'encode could not create an EventProcessor',
        );
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
        'or ensure event_processor.wasm is beside the built module or in ../dist/event_processor.wasm relative to source.',
    );
  }

  const wasmModule = await WebAssembly.compile(wasmBytes);
  // event_processor exports its own memory — no imports needed
  const instance = await WebAssembly.instantiate(wasmModule, {});
  const exports = parseEventProcessorWasmExports(instance.exports);

  return createParseCompactWasmBackend(exports);
}
