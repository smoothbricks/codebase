/**
 * The physical column vocabulary: every Arrow column type a field can lower to.
 *
 * The set is closed. A field kind, shared or an extension's, names one of these as a function of its declaration
 * alone (`FieldKind.physical`), so a column's type never depends on the data a batch happens to hold.
 */

export type TimeUnit = 's' | 'ms' | 'us' | 'ns';

export type PhysicalType =
  | { readonly type: 'bool' }
  | { readonly type: 'int'; readonly bits: 8 | 16 | 32 | 64; readonly signed: boolean }
  | { readonly type: 'float64' }
  | { readonly type: 'utf8' }
  | { readonly type: 'binary' }
  | { readonly type: 'fixedBinary'; readonly width: number }
  | { readonly type: 'timestamp'; readonly unit: TimeUnit; readonly timezone: 'UTC' }
  | { readonly type: 'duration'; readonly unit: TimeUnit }
  /**
   * Dictionary of Utf8 values with an unsigned index. `schema`: the dictionary is the declaration's (an enum's
   * variants, index = ordinal). `stream`: the dictionary grows with the data and belongs to the stream.
   */
  | { readonly type: 'dictionary'; readonly indexBits: 8 | 16 | 32; readonly dictionary: 'schema' | 'stream' };

/** The value of one cell of a physical column, as a field kind's codec hands it over. */
export type PhysicalValue = boolean | number | bigint | string | Uint8Array;
