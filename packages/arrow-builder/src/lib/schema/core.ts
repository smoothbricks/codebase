/**
 * Schema core types: the phantom carrier of a field's TypeScript value type, and the result of an operation a
 * correct caller can make fail.
 */

// Phantom symbols for type inference — never set at runtime
declare const phantomOutput: unique symbol;
declare const phantomInput: unique symbol;

/**
 * Phantom schema type for compile-time type inference. The Output/Input type parameters exist only for TypeScript
 * inference and are never set at runtime; a field node (`Field`, field.ts) is the runtime value.
 */
export interface Schema<Output = unknown, Input = Output> {
  // Boxed: an optional property of type `T | undefined` infers back as `T`, which would lose an optional field's
  // `undefined`.
  readonly [phantomOutput]?: { readonly type: Output };
  readonly [phantomInput]?: { readonly type: Input };
}

/** Extract the Output type from a Schema */
export type Output<S> = S extends Schema<infer O, unknown> ? O : unknown;

/** Extract the Input type from a Schema */
export type Input<S> = S extends Schema<unknown, infer I> ? I : unknown;

/** The outcome of a schema operation whose failure is a value: a refusal the caller reads, never an exception. */
export type SchemaResult<T, E> = { readonly ok: true; readonly value: T } | { readonly ok: false; readonly error: E };
