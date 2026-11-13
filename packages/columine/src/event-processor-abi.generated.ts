// Generated from the Rust definitions of the event-processor ABI (crates/columine-arrow/src/schema.rs:
// ArrowType, SignalSchemaField, DictionarySlot, IndexWidth, the time units and the limits;
// crates/columine-event-processor/src/lib.rs and compact.rs: ResultCode, InputFormat, CreateFailure and the compact
// batch constants) by crates/columine-event-processor/tests/host_abi.rs; rerun that test with UPDATE_GENERATED=1
// instead of editing.

/**
 * Every Arrow physical plane, ascending by tag: the `ArrowType` table of columine-arrow. `data`, `offsets` and
 * `validity` name the typed array that carries each buffer of the plane, derived from its layout.
 */
export const ARROW_PLANES = [
  { kind: 'null', variant: 'Null', tag: 0, data: null, offsets: null, validity: null },
  { kind: 'i32', variant: 'Int32', tag: 1, data: 'Int32Array', offsets: null, validity: 'Uint8Array' },
  { kind: 'f64', variant: 'Float64', tag: 2, data: 'Float64Array', offsets: null, validity: 'Uint8Array' },
  { kind: 'binary', variant: 'Binary', tag: 3, data: 'Uint8Array', offsets: 'Uint32Array', validity: 'Uint8Array' },
  { kind: 'utf8', variant: 'Utf8', tag: 4, data: 'Uint8Array', offsets: 'Uint32Array', validity: 'Uint8Array' },
  { kind: 'bool', variant: 'Bool', tag: 5, data: 'Uint8Array', offsets: null, validity: 'Uint8Array' },
  { kind: 'i64', variant: 'Int64', tag: 6, data: 'BigInt64Array', offsets: null, validity: 'Uint8Array' },
  { kind: 'i8', variant: 'Int8', tag: 7, data: 'Int8Array', offsets: null, validity: 'Uint8Array' },
  { kind: 'i16', variant: 'Int16', tag: 8, data: 'Int16Array', offsets: null, validity: 'Uint8Array' },
  { kind: 'u8', variant: 'UInt8', tag: 9, data: 'Uint8Array', offsets: null, validity: 'Uint8Array' },
  { kind: 'u16', variant: 'UInt16', tag: 10, data: 'Uint16Array', offsets: null, validity: 'Uint8Array' },
  { kind: 'u32', variant: 'UInt32', tag: 11, data: 'Uint32Array', offsets: null, validity: 'Uint8Array' },
  { kind: 'u64', variant: 'UInt64', tag: 12, data: 'BigUint64Array', offsets: null, validity: 'Uint8Array' },
  { kind: 'f16', variant: 'Float16', tag: 13, data: 'Uint16Array', offsets: null, validity: 'Uint8Array' },
  { kind: 'f32', variant: 'Float32', tag: 14, data: 'Float32Array', offsets: null, validity: 'Uint8Array' },
  { kind: 'decimal128', variant: 'Decimal128', tag: 15, data: 'Uint8Array', offsets: null, validity: 'Uint8Array' },
  { kind: 'decimal256', variant: 'Decimal256', tag: 16, data: 'Uint8Array', offsets: null, validity: 'Uint8Array' },
  {
    kind: 'largeBinary',
    variant: 'LargeBinary',
    tag: 17,
    data: 'Uint8Array',
    offsets: 'BigInt64Array',
    validity: 'Uint8Array',
  },
  {
    kind: 'largeUtf8',
    variant: 'LargeUtf8',
    tag: 18,
    data: 'Uint8Array',
    offsets: 'BigInt64Array',
    validity: 'Uint8Array',
  },
  {
    kind: 'fixedSizeBinary',
    variant: 'FixedSizeBinary',
    tag: 19,
    data: 'Uint8Array',
    offsets: null,
    validity: 'Uint8Array',
  },
  {
    kind: 'intervalYearMonth',
    variant: 'IntervalYearMonth',
    tag: 20,
    data: 'Int32Array',
    offsets: null,
    validity: 'Uint8Array',
  },
  {
    kind: 'intervalDayTime',
    variant: 'IntervalDayTime',
    tag: 21,
    data: 'Int32Array',
    offsets: null,
    validity: 'Uint8Array',
  },
  {
    kind: 'intervalMonthDayNano',
    variant: 'IntervalMonthDayNano',
    tag: 22,
    data: 'Uint8Array',
    offsets: null,
    validity: 'Uint8Array',
  },
  { kind: 'timestamp', variant: 'Timestamp', tag: 23, data: 'BigInt64Array', offsets: null, validity: 'Uint8Array' },
  { kind: 'duration', variant: 'Duration', tag: 24, data: 'BigInt64Array', offsets: null, validity: 'Uint8Array' },
] as const;

/** Bytes of one field descriptor. */
export const FIELD_DESCRIPTOR_BYTES = 8;

/**
 * Byte offset of each field of a field descriptor, found by encoding probe descriptors with
 * `SignalSchemaField::to_wire`. `typeParam` and `dictionary` are little-endian u16; `reserved` is zero.
 */
export const FIELD_DESCRIPTOR = {
  plane: 0,
  nullable: 1,
  typeParam: 2,
  dictionary: 4,
  indexPlane: 6,
  reserved: 7,
} as const;

/** The `typeParam` of a Timestamp or Duration plane: the unit its values count. */
export const TIME_UNIT = { s: 0, ms: 1, us: 2, ns: 3 } as const;

/** The time zone every Timestamp column declares. */
export const TIMESTAMP_ZONE = 'UTC';

/**
 * The `dictionary` of a descriptor: `none` for a plain column, `stream` for the schema's stream dictionary,
 * and `firstSchema + k` for schema dictionary `k` of the creation call's dictionary table.
 */
export const DICTIONARY_SLOT = { none: 0, stream: 1, firstSchema: 2 } as const;

/**
 * The planes a dictionary-encoded field stores its indexes in: its `indexPlane`, and the plane of the
 * column a caller supplies for it.
 */
export const DICTIONARY_INDEX_PLANES = [9, 10, 11] as const;

/** Arrow field metadata key under which a column names the declared fields it holds. */
export const FIELD_IDENTITY_KEY = 'sb.fields';

/** Arrow field metadata key under which a Binary column names the encoding of its cells. */
export const CELL_ENCODING_KEY = 'sb.cell';

/** Physical Arrow fields in one flattened schema. */
export const MAX_SCHEMA_FIELDS = 512;

/** Ceiling for a batch's row count: a validation limit, nothing is allocated for it. */
export const MAX_EVENTS_PER_BATCH = 65536;

/** Variable-width data bytes one column holds. */
export const MAX_VALUE_BYTES = 16777216;

/** Longest Arrow IPC Schema message a handle accepts. */
export const MAX_SCHEMA_MESSAGE_BYTES = 262144;

/** Widest FixedSizeBinary field, in bytes per row. */
export const MAX_FIXED_SIZE_BINARY_WIDTH = 256;

/** Fewest Arrow output bytes the encoder is given, however short the stream. */
export const MIN_ARROW_OUTPUT_CAPACITY = 4096;

/** Event capacity of every `ep_create_*` handle: an allocation size, not the row ceiling. */
export const WASM_EVENT_CAPACITY = 256;

/** Bytes of the result header that precede the Arrow bytes of every output. */
export const RESULT_HEADER_SIZE = 32;

/**
 * The caller-built batch header (`CPB1`): its magic, version, header and descriptor sizes, and the
 * diagnostic stage its refusals report.
 */
export const COMPACT = {
  magic: 0x3142_5043,
  version: 1,
  headerSize: 16,
  descriptorSize: 32,
  diagnosticStage: 6,
} as const;

/** Status codes of the `ep_*` exports, which also head every result. */
export const RESULT_CODE = {
  OK: 0,
  INVALID_HANDLE: 1,
  PARSE_ERROR: 2,
  ENCODE_ERROR: 3,
  OUTPUT_TOO_SMALL: 4,
  INVALID_FORMAT: 5,
  INVALID_INPUT: 6,
  SCHEMA_MISMATCH: 7,
  MEMORY_EXHAUSTED: 8,
} as const;

/** Input formats `ep_create_log_entry` reads. */
export const INPUT_FORMAT = { JSON: 0, MSGPACK: 1, ARROW_PASSTHROUGH: 2, MSGPACK_STREAM: 3 } as const;

/**
 * Why an `ep_create_*` call produced no handle. A handle is a small positive integer, so any other return
 * value is one of these codes, read as unsigned.
 */
export const CREATE_FAILURE = {
  BAD_REQUEST: 0x8000_0001,
  CAPACITY: 0x8000_0002,
  SCHEMA_MESSAGE: 0x8000_0003,
  SCHEMA_TOO_MANY_FIELDS: 0x8000_0004,
  SCHEMA_FIELD_METADATA: 0x8000_0005,
  SCHEMA_FIELD_COUNT: 0x8000_0006,
  SCHEMA_TYPE_MISMATCH: 0x8000_0007,
  SCHEMA_NULLABILITY: 0x8000_0008,
  SCHEMA_FIELD_NAMES: 0x8000_0009,
  INIT: 0x8000_000a,
  HANDLES_EXHAUSTED: 0x8000_000b,
  MEMORY_EXHAUSTED: 0x8000_000c,
  SCHEMA_TOO_LARGE: 0x8000_000d,
  SCHEMA_DICTIONARY: 0x8000_000e,
  SCHEMA_FIELD_IDENTITY: 0x8000_000f,
} as const;
