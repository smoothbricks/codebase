// @generated from arrow-planes.ts — DO NOT EDIT. Regenerate: `UPDATE_GENERATED=1 nx test columine`.

/**
 * One compact column: a physical plane plus its buffers.
 *
 * The discriminant, its wire tag and its carrier are declared together in
 * `arrow-planes.ts`, so a plane cannot be named on one side of the ABI and
 * carried by a different-signedness array on the other.
 */
export type CompactColumn =
  | { readonly kind: 'null' }
  | { readonly kind: 'i32'; readonly data: Int32Array; readonly validity?: Uint8Array }
  | { readonly kind: 'f64'; readonly data: Float64Array; readonly validity?: Uint8Array }
  | {
      readonly kind: 'binary';
      readonly offsets: Uint32Array;
      readonly data: Uint8Array;
      readonly validity?: Uint8Array;
    }
  | { readonly kind: 'utf8'; readonly offsets: Uint32Array; readonly data: Uint8Array; readonly validity?: Uint8Array }
  | { readonly kind: 'bool'; readonly data: Uint8Array; readonly validity?: Uint8Array }
  | { readonly kind: 'i64'; readonly data: BigInt64Array; readonly validity?: Uint8Array }
  | { readonly kind: 'i8'; readonly data: Int8Array; readonly validity?: Uint8Array }
  | { readonly kind: 'i16'; readonly data: Int16Array; readonly validity?: Uint8Array }
  | { readonly kind: 'u8'; readonly data: Uint8Array; readonly validity?: Uint8Array }
  | { readonly kind: 'u16'; readonly data: Uint16Array; readonly validity?: Uint8Array }
  | { readonly kind: 'u32'; readonly data: Uint32Array; readonly validity?: Uint8Array }
  | { readonly kind: 'u64'; readonly data: BigUint64Array; readonly validity?: Uint8Array }
  | { readonly kind: 'f16'; readonly data: Uint16Array; readonly validity?: Uint8Array }
  | { readonly kind: 'f32'; readonly data: Float32Array; readonly validity?: Uint8Array }
  | { readonly kind: 'decimal128'; readonly data: Uint8Array; readonly validity?: Uint8Array }
  | { readonly kind: 'decimal256'; readonly data: Uint8Array; readonly validity?: Uint8Array }
  | {
      readonly kind: 'largeBinary';
      readonly offsets: BigInt64Array;
      readonly data: Uint8Array;
      readonly validity?: Uint8Array;
    }
  | {
      readonly kind: 'largeUtf8';
      readonly offsets: BigInt64Array;
      readonly data: Uint8Array;
      readonly validity?: Uint8Array;
    }
  | { readonly kind: 'fixedSizeBinary'; readonly data: Uint8Array; readonly validity?: Uint8Array }
  | { readonly kind: 'intervalYearMonth'; readonly data: Int32Array; readonly validity?: Uint8Array }
  | { readonly kind: 'intervalDayTime'; readonly data: Int32Array; readonly validity?: Uint8Array }
  | { readonly kind: 'intervalMonthDayNano'; readonly data: Uint8Array; readonly validity?: Uint8Array };

/** Physical plane tags — the `ArrowType` enum in columine-arrow. */
export const COMPACT_KIND_TAG = {
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

/**
 * Highest valid plane tag, DERIVED. A bounds check naming one plane’s tag
 * silently rejected every plane appended after it.
 */
export const COMPACT_MAX_KIND_TAG = 22;
