/**
 * The plane table's TypeScript types. Every Arrow physical plane is declared
 * ONCE, in Rust.
 *
 * A *plane* is one memory layout — a value width, a signedness and a buffer
 * set. The `arrow_planes!` invocation in `columine-arrow/src/schema.rs` owns
 * each plane's name and frozen wire tag; `columine-event-processor/tests/host_abi.rs`
 * renders `ARROW_PLANES` into `event-processor-abi.generated.ts`, deriving the
 * typed array that carries each of a plane's buffers from its layout. This
 * file states no plane: it derives the `CompactColumn` union and the tag
 * lookups from that generated table.
 *
 * # One row per plane
 *
 * A plane's name, tag and carriers are one generated row, so a plane named
 * `'u32'` that carries an `Int32Array`, or whose tag selects a signed plane,
 * cannot be written down.
 *
 * # Tags are frozen
 *
 * Tag values are the wire ABI baked into the shipped `dist/event_processor.wasm`
 * and into every persisted fixture. New planes APPEND; nothing renumbers.
 * Renaming a plane or moving its tag changes what a published discriminant
 * means for consumers, which is a breaking change even when the type surface
 * still compiles.
 *
 * After a change to the Rust table, rerun `host_abi.rs` with
 * `UPDATE_GENERATED=1` to regenerate the table.
 */
import { ARROW_PLANES } from './event-processor-abi.generated.js';

/** What the derived types read of one generated plane row: each carrier is one of {@link Carriers}. */
interface ArrowPlane {
  /** The `CompactColumn` discriminant. */
  readonly kind: string;
  /** `ArrowType` variant name on the Rust side. */
  readonly variant: string;
  /** Frozen wire tag. */
  readonly tag: number;
  /** Typed array carrying the values, or `null` for a plane with no values. */
  readonly data: keyof Carriers | null;
  /** Offsets carrier for variable-width planes. */
  readonly offsets: keyof Carriers | null;
  /** Validity bitmap carrier, absent only where every row is null. */
  readonly validity: keyof Carriers | null;
}

/** Carrier name -> the language's typed array. Ten entries, none plane-specific. */
interface Carriers {
  Int8Array: Int8Array;
  Uint8Array: Uint8Array;
  Int16Array: Int16Array;
  Uint16Array: Uint16Array;
  Int32Array: Int32Array;
  Uint32Array: Uint32Array;
  Float32Array: Float32Array;
  Float64Array: Float64Array;
  BigInt64Array: BigInt64Array;
  BigUint64Array: BigUint64Array;
}

/** Compile-time check that every generated row names carriers this file knows. */
type Checked<Rows extends readonly ArrowPlane[]> = Rows;

type Plane = Checked<typeof ARROW_PLANES>[number];

/** Collapses the intersection so each column renders as one flat object. */
type Flatten<T> = { [K in keyof T]: T[K] };

type Buffers<P extends Plane> = (P['offsets'] extends keyof Carriers
  ? { readonly offsets: Carriers[P['offsets']] }
  : // biome-ignore lint/complexity/noBannedTypes: an empty member contributes nothing to the intersection
    {}) &
  (P['data'] extends keyof Carriers
    ? { readonly data: Carriers[P['data']] }
    : // biome-ignore lint/complexity/noBannedTypes: as above
      {}) &
  (P['validity'] extends keyof Carriers
    ? { readonly validity?: Carriers[P['validity']] }
    : // biome-ignore lint/complexity/noBannedTypes: as above
      {});

/** Distributive over the plane union: `P` is a type parameter, so each row maps separately. */
type ColumnOf<P extends Plane> = P extends unknown ? Flatten<{ readonly kind: P['kind'] } & Buffers<P>> : never;

/**
 * One compact column: a physical plane plus its buffers.
 *
 * DERIVED from {@link ARROW_PLANES}, which is generated from the Rust plane
 * table, so a plane's name, tag and carrier cannot disagree — there is one
 * Rust row to edit and no second copy to forget.
 */
export type CompactColumn = ColumnOf<Plane>;

/**
 * Physical plane tags — the `ArrowType` enum in columine-arrow.
 *
 * Coverage is structural rather than checked: the same array supplies the union
 * members and these entries, so a plane cannot appear in one and not the other.
 */
const KIND_TAGS: ReadonlyMap<CompactColumn['kind'], number> = new Map(
  ARROW_PLANES.map((plane) => [plane.kind, plane.tag] as const),
);

/**
 * This plane's wire tag. Takes the exact discriminant rather than a string, so
 * a misspelled plane name is a compile error instead of an undefined lookup.
 */
export function compactKindTag(kind: CompactColumn['kind']): number {
  const tag = KIND_TAGS.get(kind);
  if (tag === undefined) {
    // invariant throw: the map is built from the table this type is derived from
    throw new Error(`plane ${kind} has no tag`);
  }
  return tag;
}

/**
 * Highest valid plane tag, DERIVED. A bounds check naming one plane's tag
 * silently rejected every plane appended after it.
 */
export const COMPACT_MAX_KIND_TAG: number = Math.max(...ARROW_PLANES.map((plane) => plane.tag));
