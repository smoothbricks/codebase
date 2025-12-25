/**
 * Result types and fluent result builders for ctx.ok()/ctx.err()
 *
 * Provides type-safe error handling without exceptions using discriminated unions.
 *
 * ## Deferred Tag Application
 *
 * FluentOk and FluentErr are buffer-agnostic - they capture tag operations as closures
 * via the `applyTags` property. When span()/trace() completes, it calls `result.applyTags(buffer)`
 * to apply all captured tags to the correct buffer.
 *
 * This ensures type safety - tags are always applied to the span that created the result,
 * preventing accidental writes to wrong buffers if a result is captured and used later.
 */

import type { InferSchema, LogSchema } from './schema/types.js';
import type { SpanBuffer } from './types.js';

/**
 * Discriminated union representing a successful operation result.
 *
 * Use with {@link Err} and {@link Result} for type-safe error handling
 * without exceptions. The `success: true` literal enables TypeScript narrowing.
 *
 * @typeParam V - The type of the success value
 *
 * @example
 * ```typescript
 * function parseUserId(input: string): Result<number, string> {
 *   const id = parseInt(input);
 *   if (isNaN(id)) {
 *     return { success: false, error: { code: 'INVALID_ID', details: 'Not a number' } };
 *   }
 *   return { success: true, value: id };
 * }
 * ```
 *
 * @see {@link SpanContext.ok} - Create success result with trace logging
 */
export type Ok<V> = { success: true; value: V };

/**
 * Discriminated union representing a failed operation result.
 *
 * Use with {@link Ok} and {@link Result} for type-safe error handling
 * without exceptions. The `success: false` literal enables TypeScript narrowing.
 *
 * @typeParam E - The type of the error details
 *
 * @see {@link SpanContext.err} - Create error result with trace logging
 */
export type Err<E> = { success: false; error: { code: string; details: E } };

/**
 * Union type for operation results - either success or error.
 *
 * Provides type-safe error handling without exceptions. Use with `ctx.ok()` and
 * `ctx.err()` methods in span contexts for automatic trace logging.
 *
 * @typeParam V - The type of the success value
 * @typeParam E - The type of the error details (defaults to unknown)
 *
 * @example
 * ```typescript
 * async function createUser(ctx: SpanContext, data: UserData): Promise<Result<User, ValidationError>> {
 *   if (!data.email) {
 *     return ctx.err('MISSING_EMAIL', { field: 'email' });
 *   }
 *   const user = await db.insert(data);
 *   return ctx.ok(user);
 * }
 *
 * // Type narrowing works automatically
 * const result = await createUser(ctx, data);
 * if (result.success) {
 *   console.log(result.value); // TypeScript knows this is User
 * } else {
 *   console.log(result.error.code); // 'MISSING_EMAIL'
 * }
 * ```
 */
export type Result<V, E = unknown> = Ok<V> | Err<E>;

/**
 * Type for the deferred tag application closure.
 * Called by span()/trace() at span-end to apply captured tags to the buffer.
 */
export type ApplyTagsFn<T extends LogSchema> = (buffer: SpanBuffer<T>) => void;

/**
 * Fluent success result - buffer-agnostic with deferred tag application.
 *
 * Created by ctx.ok(value). Tags are captured as closures and applied when
 * span()/trace() completes, ensuring writes go to the correct buffer.
 *
 * @example
 * ```typescript
 * // Basic usage
 * return ctx.ok(user);
 *
 * // With chained tags (applied at span-end)
 * return ctx.ok(user).message('User created').with({ userId: user.id });
 * ```
 */
export class FluentOk<V, T extends LogSchema = LogSchema> implements Ok<V> {
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

  /** Getter for Ok interface - derived from class identity */
  get success(): true {
    return true;
  }

  /** Clean output for console.log in Node.js */
  [Symbol.for('nodejs.util.inspect.custom')](): { success: true; value: V } {
    return { success: true, value: this.value };
  }

  /** Clean output for JSON.stringify */
  toJSON(): { success: true; value: V } {
    return { success: true, value: this.value };
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
}

/**
 * Fluent error result - buffer-agnostic with deferred tag application.
 *
 * Created by ctx.err(code, details). The error code is stored in the result
 * and written to buffer by span()/trace() at span-end (not via closure).
 *
 * @example
 * ```typescript
 * // Basic usage
 * return ctx.err('VALIDATION_FAILED', { field: 'email' });
 *
 * // With chained tags (applied at span-end)
 * return ctx.err('NOT_FOUND', null).message('User not found');
 * ```
 */
export class FluentErr<E, T extends LogSchema = LogSchema> implements Err<E> {
  readonly error: { code: string; details: E };

  /**
   * Closure chain for deferred tag application.
   * Applied by span()/trace() at span-end to write tags to buffer row 1.
   * Note: error_code is written directly by span()/trace() from this.error.code,
   * not via this closure.
   */
  applyTags?: ApplyTagsFn<T>;

  constructor(code: string, details: E) {
    this.error = { code, details };
  }

  /** Getter for Err interface - derived from class identity */
  get success(): false {
    return false;
  }

  /** Clean output for console.log in Node.js */
  [Symbol.for('nodejs.util.inspect.custom')](): { success: false; error: { code: string; details: E } } {
    return { success: false, error: this.error };
  }

  /** Clean output for JSON.stringify */
  toJSON(): { success: false; error: { code: string; details: E } } {
    return { success: false, error: this.error };
  }

  /**
   * Set multiple attributes on the span-end entry (row 1).
   * Deferred - applied when span()/trace() completes.
   *
   * @example ctx.err('ERROR', details).with({ userId: 'u1' })
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
}

/**
 * Union type for fluent result builders - either success or error with chaining support.
 *
 * @typeParam S - The type of the success value
 * @typeParam E - The type of the error details
 * @typeParam T - The log schema for the span buffer
 */
export type FluentResult<S, E, T extends LogSchema = LogSchema> = FluentOk<S, T> | FluentErr<E, T>;

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
