/**
 * Result types for type-safe error handling without exceptions.
 *
 * ## Classes
 * - `Ok<V, T>` - Success result with fluent tag API
 * - `Err<E, T>` - Error result with fluent tag API
 *
 * ## Error Factories
 * - `defineCodeError(code)` - Create callable error class with `code` on prototype
 *
 * ## Deferred Tag Application
 *
 * Ok and Err capture tag operations as closures via the `applyTags` property.
 * When span()/trace() completes, it calls `result.applyTags(buffer)` to apply
 * all captured tags to the correct buffer row 1.
 *
 * @example
 * ```typescript
 * // Define error codes as callable classes
 * const NOT_FOUND = defineCodeError('NOT_FOUND')<{ userId: string }>();
 * const VALIDATION_FAILED = defineCodeError('VALIDATION_FAILED')<{ field: string }>();
 *
 * // Use in ops - no 'new' required
 * return ctx.err(NOT_FOUND({ userId }));
 * return ctx.err(VALIDATION_FAILED({ field: 'email' })).message('Invalid email');
 *
 * // instanceof works for type narrowing
 * if (result.isErr(NOT_FOUND)) {
 *   console.log(result.error.userId); // typed!
 * }
 * ```
 */

import type { InferSchema, LogSchema } from './schema/types.js';
import type { SpanBuffer } from './types.js';

// =============================================================================
// TAGGED ERROR INTERFACE
// =============================================================================

/**
 * Interface for tagged errors that can be discriminated with isErr(Tag).
 * Used by Blocked, RetriesExhausted, and user-defined error classes.
 */
export interface TaggedError<Tag extends string = string> {
  readonly _tag: Tag;
}

/**
 * Constructor type for tagged errors.
 * Used with Result.isErr(Tag) to check error type via instanceof.
 */
export interface TaggedErrorConstructor<T extends TaggedError = TaggedError> {
  readonly _tag: T['_tag'];
  new (...args: never[]): T;
}

// =============================================================================
// APPLY TAGS FUNCTION TYPE
// =============================================================================

/**
 * Type for the deferred tag application closure.
 * Called by span()/trace() at span-end to apply captured tags to the buffer.
 */
export type ApplyTagsFn<T extends LogSchema> = (buffer: SpanBuffer<T>) => void;

type OkJson<V> = { ok: true; value: V };
type ErrJson<E> = { ok: false; error: E };

// =============================================================================
// OK CLASS
// =============================================================================

/**
 * Success result with fluent tag application.
 *
 * Created by `ctx.ok(value)`. Tags are captured as closures and applied when
 * span()/trace() completes, ensuring writes go to the correct buffer.
 *
 * @typeParam V - The type of the success value
 * @typeParam T - The log schema for buffer tag application
 *
 * @example
 * ```typescript
 * return ctx.ok(user);
 * return ctx.ok(user).with({ userId: user.id }).message('Created');
 * ```
 */
export class Ok<V, T extends LogSchema = LogSchema> {
  readonly value: V;

  /**
   * Closure chain for deferred tag application.
   * Applied by span()/trace() at span-end to write tags to buffer row 1.
   * undefined if no chained methods were called.
   */
  applyTags?: ApplyTagsFn<T>;

  constructor(value: V) {
    this.value = value;
  }

  /** Discriminant for type narrowing. */
  get success(): true {
    return true;
  }

  /** Type guard - always returns true for Ok. */
  isOk(): this is Ok<V, T> {
    return true;
  }

  /** Type guard - always returns false for Ok. */
  isErr(): false;
  isErr<Tag extends TaggedError>(_Tag: TaggedErrorConstructor<Tag>): false;
  isErr(_predicate?: unknown): false {
    return false;
  }

  /** Get the value (always present for Ok). */
  get maybeValue(): V {
    return this.value;
  }

  /** Get the error (always undefined for Ok). */
  get maybeError(): undefined {
    return undefined;
  }

  /** Transform the success value. */
  map<U>(fn: (value: V) => U): Ok<U, T> {
    const mapped = new Ok<U, T>(fn(this.value));
    mapped.applyTags = this.applyTags;
    return mapped;
  }

  /** No-op for Ok (error transformation). */
  mapErr<F>(_fn: (error: never) => F): Ok<V, T> {
    return this;
  }

  /** Transform the success value, potentially returning an error. */
  flatMap<U, F>(fn: (value: V) => Result<U, F>): Result<U, F> {
    return fn(this.value);
  }

  /** Get the value (always returns value for Ok). */
  unwrapOr<U>(_defaultValue: U): V {
    return this.value;
  }

  /** Get the value (always returns value for Ok). */
  unwrapOrElse<U>(_fn: (error: never) => U): V {
    return this.value;
  }

  /** Pattern match on the result. */
  match<U>(handlers: { ok: (value: V) => U; err: (error: never) => U }): U {
    return handlers.ok(this.value);
  }

  /**
   * Set multiple attributes on the span-end entry (row 1).
   * Deferred - applied when span()/trace() completes.
   *
   * @example ctx.ok(result).with({ userId: 'u1', operation: 'CREATE' })
   */
  with(attributes: Partial<InferSchema<T>>): this {
    const prev = this.applyTags;
    this.applyTags = (buffer) => {
      if (prev) prev(buffer);
      for (const [key, value] of Object.entries(attributes)) {
        if (value != null && typeof (buffer as Record<string, unknown>)[key] === 'function') {
          (buffer as Record<string, (pos: number, val: unknown) => void>)[key](1, value);
        }
      }
    };
    return this;
  }

  /**
   * Set result message on span-end entry (row 1).
   * Overwrites the default span name in the message column.
   *
   * @example ctx.ok(result).message('User created successfully')
   */
  message(text: string): this {
    const prev = this.applyTags;
    this.applyTags = (buffer) => {
      if (prev) prev(buffer);
      buffer.message(1, text);
    };
    return this;
  }

  /**
   * Set source code line number on span-end entry (row 1).
   * Typically injected by transformer.
   *
   * @example ctx.ok(result).line(42)
   */
  line(lineNumber: number): this {
    const prev = this.applyTags;
    this.applyTags = (buffer) => {
      if (prev) prev(buffer);
      buffer.line(1, lineNumber);
    };
    return this;
  }

  [Symbol.for('nodejs.util.inspect.custom')](): OkJson<V> {
    return { ok: true, value: this.value };
  }

  toJSON(): OkJson<V> {
    return { ok: true, value: this.value };
  }
}

// =============================================================================
// ERR CLASS
// =============================================================================

/**
 * Error result with fluent tag application.
 *
 * Created by `ctx.err(error)`. The error is stored directly (flat structure).
 * Supports fluent `.with()`, `.message()`, `.line()` for deferred tag application.
 *
 * @typeParam E - The type of the error
 * @typeParam T - The log schema for buffer tag application
 *
 * @example
 * ```typescript
 * const NOT_FOUND = defineCodeError('NOT_FOUND')<{ userId: string }>();
 * return ctx.err(NOT_FOUND({ userId }));
 * return ctx.err(NOT_FOUND({ userId })).message('User not found');
 *
 * // Tagged errors work too
 * return ctx.err(Blocked.service('payment-api'));
 * ```
 */
export class Err<E, T extends LogSchema = LogSchema> {
  readonly error: E;

  /**
   * Closure chain for deferred tag application.
   * Applied by span()/trace() at span-end to write tags to buffer row 1.
   * Note: error_code is written directly by span()/trace() from this.error.code,
   * not via this closure.
   */
  applyTags?: ApplyTagsFn<T>;

  constructor(error: E) {
    this.error = error;
  }

  /** Discriminant for type narrowing. */
  get success(): false {
    return false;
  }

  /** Type guard - always returns false for Err. */
  isOk(): false {
    return false;
  }

  /**
   * Check if this is an error result, optionally matching a tagged type or predicate.
   *
   * @example
   * ```typescript
   * if (result.isErr()) { ... }  // Always true for Err
   * if (result.isErr(Blocked)) { ... }  // Check instanceof Blocked
   * if (result.isErr(NOT_FOUND)) { ... }  // Check instanceof NOT_FOUND
   * if (result.isErr(e => e.code === 'X')) { ... }  // Predicate
   * ```
   */
  isErr(): this is Err<E, T>;
  isErr<Tag extends TaggedError>(Tag: TaggedErrorConstructor<Tag>): this is Err<Tag, T>;
  isErr<C extends abstract new (...args: never[]) => unknown>(Class: C): this is Err<InstanceType<C>, T>;
  isErr(predicate: (error: E) => boolean): boolean;
  isErr<Tag extends TaggedError>(
    tagOrPredicate?:
      | TaggedErrorConstructor<Tag>
      | (abstract new (
          ...args: never[]
        ) => unknown)
      | ((error: E) => boolean),
  ): boolean {
    if (tagOrPredicate === undefined) return true;

    // instanceof check for classes (including TaggedError and CodeError)
    if (typeof tagOrPredicate === 'function') {
      // Check if it's a predicate function (not a constructor)
      if (tagOrPredicate.prototype === undefined || tagOrPredicate.prototype === Function.prototype) {
        return (tagOrPredicate as (error: E) => boolean)(this.error);
      }
      // It's a constructor - use instanceof
      return this.error instanceof (tagOrPredicate as abstract new (...args: never[]) => unknown);
    }

    return false;
  }

  /** Get the value (always undefined for Err). */
  get maybeValue(): undefined {
    return undefined;
  }

  /** Get the error (always present for Err). */
  get maybeError(): E {
    return this.error;
  }

  /** No-op for Err (value transformation). */
  map<U>(_fn: (value: never) => U): Err<E, T> {
    return this;
  }

  /** Transform the error. */
  mapErr<F>(fn: (error: E) => F): Err<F, T> {
    const mapped = new Err<F, T>(fn(this.error));
    mapped.applyTags = this.applyTags as ApplyTagsFn<T> | undefined;
    return mapped;
  }

  /** No-op for Err (returns self). */
  flatMap<U, F>(_fn: (value: never) => Result<U, F>): Err<E, T> {
    return this;
  }

  /** Get the default value (error is ignored). */
  unwrapOr<U>(defaultValue: U): U {
    return defaultValue;
  }

  /** Compute a value from the error. */
  unwrapOrElse<U>(fn: (error: E) => U): U {
    return fn(this.error);
  }

  /** Pattern match on the result. */
  match<U>(handlers: { ok: (value: never) => U; err: (error: E) => U }): U {
    return handlers.err(this.error);
  }

  /**
   * Set multiple attributes on the span-end entry (row 1).
   * Deferred - applied when span()/trace() completes.
   *
   * @example ctx.err(error).with({ user_id: 'u1' })
   */
  with(attributes: Partial<InferSchema<T>>): this {
    const prev = this.applyTags;
    this.applyTags = (buffer) => {
      if (prev) prev(buffer);
      for (const [key, value] of Object.entries(attributes)) {
        if (value != null && typeof (buffer as Record<string, unknown>)[key] === 'function') {
          (buffer as Record<string, (pos: number, val: unknown) => void>)[key](1, value);
        }
      }
    };
    return this;
  }

  /**
   * Set result message on span-end entry (row 1).
   * Overwrites the default span name in the message column.
   *
   * @example ctx.err('ERROR', details).message('Operation failed')
   */
  message(text: string): this {
    const prev = this.applyTags;
    this.applyTags = (buffer) => {
      if (prev) prev(buffer);
      buffer.message(1, text);
    };
    return this;
  }

  /**
   * Set source code line number on span-end entry (row 1).
   * Typically injected by transformer.
   *
   * @example ctx.err('ERROR', details).line(42)
   */
  line(lineNumber: number): this {
    const prev = this.applyTags;
    this.applyTags = (buffer) => {
      if (prev) prev(buffer);
      buffer.line(1, lineNumber);
    };
    return this;
  }

  [Symbol.for('nodejs.util.inspect.custom')](): ErrJson<E> {
    return { ok: false, error: this.error };
  }

  toJSON(): ErrJson<E> {
    return { ok: false, error: this.error };
  }
}

// =============================================================================
// RESULT TYPE
// =============================================================================

/** Union type for Result - either Ok or Err. */
export type Result<V, E> = Ok<V> | Err<E>;

// =============================================================================
// CODE ERROR FACTORY
// =============================================================================

/**
 * Code error instance type - has `code` on prototype plus fields as own properties.
 */
export interface CodeErrorInstance<Code extends string, _Fields extends object> {
  readonly code: Code;
}

/**
 * Callable class type returned by defineCodeError.
 * Can be called without `new`, instanceof works.
 */
export interface CodeErrorClass<Code extends string, Fields extends object> {
  (fields: Fields): CodeErrorInstance<Code, Fields> & Fields;
  new (fields: Fields): CodeErrorInstance<Code, Fields> & Fields;
  readonly prototype: { readonly code: Code };
}

/**
 * Create a callable error class with `code` on prototype.
 *
 * The returned class can be called without `new` and supports `instanceof`.
 *
 * @param code - The error code string (becomes a literal type)
 * @returns A curried function that takes type parameter for fields
 *
 * @example
 * ```typescript
 * const NOT_FOUND = defineCodeError('NOT_FOUND')<{ userId: string }>();
 * const VALIDATION_FAILED = defineCodeError('VALIDATION_FAILED')<{ field: string }>();
 *
 * // Call without 'new'
 * const err = NOT_FOUND({ userId: 'u1' });
 * err.code;     // 'NOT_FOUND' (from prototype)
 * err.userId;   // 'u1' (own property)
 *
 * // instanceof works
 * err instanceof NOT_FOUND;  // true
 *
 * // Use with ctx.err()
 * return ctx.err(NOT_FOUND({ userId }));
 *
 * // Type narrowing with isErr()
 * if (result.isErr(NOT_FOUND)) {
 *   result.error.userId;  // typed!
 * }
 * ```
 */
export function defineCodeError<Code extends string>(code: Code) {
  return <Fields extends object = Record<string, never>>(): CodeErrorClass<Code, Fields> => {
    // Constructor function that works with or without 'new'
    const CodeError = function (
      this: (CodeErrorInstance<Code, Fields> & Fields) | undefined,
      fields: Fields,
    ): CodeErrorInstance<Code, Fields> & Fields {
      // Allow calling without 'new'
      if (!(this instanceof CodeError)) {
        return new CodeError(fields);
      }
      // Assign fields as own properties
      Object.assign(this, fields);
      return this;
    } as unknown as CodeErrorClass<Code, Fields>;

    // Put code on prototype so all instances share it
    (CodeError.prototype as { code: Code }).code = code;

    return CodeError;
  };
}

/**
 * Check if an error has a `code` property.
 * Used by writeSpanEnd to extract error_code for logging.
 */
export function hasErrorCode(error: unknown): error is { code: string } {
  return (
    error !== null &&
    typeof error === 'object' &&
    'code' in error &&
    typeof (error as { code: unknown }).code === 'string'
  );
}

// =============================================================================
// RESULT TYPE EXTRACTION UTILITIES
// =============================================================================

/**
 * Extract success type from a Result type.
 * Returns `never` if R is not a Result.
 */
export type ResultSuccess<R> = R extends Result<infer S, unknown> ? S : never;

/**
 * Extract success type from a Promise<Result> type.
 * Returns `never` if R is not a Promise<Result>.
 */
export type PromiseResultSuccess<R> = R extends Promise<Result<infer S, unknown>> ? S : never;

/**
 * Extract success type from either Result or Promise<Result>.
 * Useful for inferring the success type from Op return values.
 */
export type ExtractSuccess<R> = ResultSuccess<R> | PromiseResultSuccess<R>;

/**
 * Extract error type from a Result type.
 * Returns `never` if R is not a Result.
 */
export type ResultError<R> = R extends Result<unknown, infer E> ? E : never;

/**
 * Extract error type from a Promise<Result> type.
 * Returns `never` if R is not a Promise<Result>.
 */
export type PromiseResultError<R> = R extends Promise<Result<unknown, infer E>> ? E : never;

/**
 * Extract error type from either Result or Promise<Result>.
 * Useful for inferring the error type from Op return values.
 */
export type ExtractError<R> = ResultError<R> | PromiseResultError<R>;
