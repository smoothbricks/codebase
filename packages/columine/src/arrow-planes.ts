/**
 * The plane table: every Arrow physical plane, declared ONCE.
 *
 * A *plane* is one memory layout — a value width, a signedness and a buffer
 * set. This file owns each plane's name, its frozen wire tag, and the typed
 * array that carries it, and both language surfaces are generated from it:
 * `compact-column.generated.ts` (the `CompactColumn` union and
 * `COMPACT_KIND_TAG`) and the `arrow_planes!` declarations in
 * `columine-arrow/src/schema.rs`.
 *
 * # Why one table
 *
 * The name, the tag and the carrier used to be stated separately in the union,
 * in `COMPACT_KIND_TAG`, in the Rust enum and in a Rust-side name mapping. That
 * is how tag 1 came to be called `'u32'` and carry `Uint32Array` while the plane
 * it selects is signed: no single edit was wrong, the copies simply disagreed.
 * Stated together, that combination cannot be written down.
 *
 * # Tags are frozen
 *
 * Tag values are the wire ABI baked into the shipped `dist/event_processor.wasm`
 * and into every persisted fixture. New planes APPEND; nothing renumbers.
 * Renaming a plane or moving its tag changes what a published discriminant
 * means for consumers, which is a breaking change even when the type surface
 * still compiles.
 *
 * Regenerate both surfaces with `UPDATE_GENERATED=1 nx test columine`.
 */
export interface ArrowPlane {
  /** Name on the TypeScript side of the ABI, and the `CompactColumn` discriminant. */
  readonly kind: string;
  /** `ArrowType` variant name on the Rust side. */
  readonly variant: string;
  /** Frozen wire tag. */
  readonly tag: number;
  /** Typed array carrying the values, or `null` for a plane with no values. */
  readonly data: string | null;
  /** Offsets carrier for variable-width planes. */
  readonly offsets: string | null;
  /** Validity bitmap carrier, absent only where every row is null. */
  readonly validity: string | null;
}

/** Every plane, ascending by tag; the tags form a gapless block from zero. */
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
] as const satisfies readonly ArrowPlane[];
