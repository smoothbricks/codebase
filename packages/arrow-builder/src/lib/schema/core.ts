/**
 * Schema core types — replaces @sury/sury dependency
 *
 * Provides phantom type carriers for compile-time type inference
 * and minimal runtime composition (optional, union, refine).
 * No runtime validation — that belongs in consumer packages (e.g. defineLogSchema).
 */

// Phantom symbols for type inference — never set at runtime
declare const phantomOutput: unique symbol;
declare const phantomInput: unique symbol;

/**
 * Phantom schema type for compile-time type inference.
 * Replaces Sury.Schema<Output, Input>.
 *
 * Base singletons (string, number, etc.) below are phantom type carriers only —
 * they carry no runtime metadata. Builder functions (S.number(), S.enum(), etc.
 * in builder.ts) create schema objects with __schema_type metadata for columnar
 * storage dispatch. The Output/Input type parameters exist only for TypeScript
 * inference and are never set at runtime.
 */
export interface Schema<Output = unknown, Input = Output> {
  readonly [phantomOutput]?: Output;
  readonly [phantomInput]?: Input;
}

/** Extract the Output type from a Schema */
export type Output<S> = S extends Schema<infer O, unknown> ? O : unknown;

/** Extract the Input type from a Schema */
export type Input<S> = S extends Schema<unknown, infer I> ? I : unknown;

// ---------------------------------------------------------------------------
// Base singleton schemas — plain objects with phantom types, no validation
// ---------------------------------------------------------------------------

export const string: Schema<string, string> = {} as Schema<string, string>;
export const number: Schema<number, number> = {} as Schema<number, number>;
export const boolean: Schema<boolean, boolean> = {} as Schema<boolean, boolean>;
export const bigint: Schema<bigint, bigint> = {} as Schema<bigint, bigint>;
export const unknown: Schema<unknown, unknown> = {} as Schema<unknown, unknown>;

// ---------------------------------------------------------------------------
// Composition functions — type-level wrappers for schema construction
// ---------------------------------------------------------------------------

/**
 * Create a refined schema with a validation callback.
 * The refiner function is stored on the object for runtime validation.
 */
export function refine<I, O>(
  _base: Schema<I>,
  refiner: (value: I, fail: { fail(msg: string): never }) => O,
): Schema<O, I> {
  return { __refiner: refiner } as Schema<O, I> & {
    __refiner: typeof refiner;
  };
}

/** Create an optional schema wrapper */
export function optional<T>(schema: Schema<T>): Schema<T | undefined, T | undefined> {
  return { __optional: true, __inner: schema } as Schema<T | undefined, T | undefined>;
}

/** Create a union schema wrapper */
export function union<T extends readonly [Schema, ...Schema[]]>(
  schemas: T,
): Schema<Output<T[number]>, Input<T[number]>> {
  return { __union: [...schemas] } as Schema<Output<T[number]>, Input<T[number]>>;
}
