/**
 * Result types and fluent result builders for ctx.ok()/ctx.err()
 *
 * Provides type-safe error handling without exceptions using discriminated unions.
 */

import { createResultWriter, type ResultWriter } from './codegen/fixedPositionWriterGenerator.js';
import { ENTRY_TYPE_SPAN_ERR, ENTRY_TYPE_SPAN_OK } from './schema/systemSchema.js';
import type { InferSchema, LogSchema } from './schema/types.js';
import { getTimestampNanos } from './timestamp.js';
import type { SpanBuffer } from './types.js';

/**
 * Discriminated union representing a successful operation result.
 *
 * Use with {@link ErrorResult} and {@link Result} for type-safe error handling
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
export type SuccessResult<V> = { success: true; value: V };

/**
 * Discriminated union representing a failed operation result.
 *
 * Use with {@link SuccessResult} and {@link Result} for type-safe error handling
 * without exceptions. The `success: false` literal enables TypeScript narrowing.
 *
 * @typeParam E - The type of the error details
 *
 * @see {@link SpanContext.err} - Create error result with trace logging
 */
export type ErrorResult<E> = { success: false; error: { code: string; details: E } };

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
export type Result<V, E = unknown> = SuccessResult<V> | ErrorResult<E>;

/**
 * Fluent result builder for ctx.ok()/ctx.err()
 * Allows chaining attributes and message before returning result
 *
 * Per specs/01h_entry_types_and_logging_primitives.md:
 * - Writes span-ok or span-err entry to buffer
 * - Supports .with() for attributes and individual setters
 * - Returns final result after writing to buffer
 *
 * This class uses ResultWriter internally for attribute writing while
 * maintaining proper TypeScript type narrowing via SuccessResult interface.
 */
export class FluentSuccessResult<V, T extends LogSchema> implements SuccessResult<V> {
  readonly success = true as const;
  readonly value: V;
  /** @internal - hidden from console.log via custom inspect */
  #writer: ResultWriter<T>;

  constructor(buffer: SpanBuffer<T>, value: V, schema: T) {
    this.value = value;

    // Overwrite the pre-initialized span-exception with span-ok
    buffer.entry_type[1] = ENTRY_TYPE_SPAN_OK;

    // Write timestamp (nanoseconds since epoch)
    buffer.timestamp[1] = getTimestampNanos(buffer._traceRoot.anchorEpochNanos, buffer._traceRoot.anchorPerfNow);

    // Create ResultWriter for fluent attribute setting (writes to position 1)
    this.#writer = createResultWriter(schema, buffer, value, false);

    // Note: writeIndex is NOT incremented - row 1 is reserved, events start at row 2
  }

  /** Clean output for console.log in Node.js */
  [Symbol.for('nodejs.util.inspect.custom')](): { success: true; value: V } {
    return { success: this.success, value: this.value };
  }

  /** Clean output for JSON.stringify */
  toJSON(): { success: true; value: V } {
    return { success: this.success, value: this.value };
  }

  /**
   * Set multiple attributes on the result entry
   * Example: ctx.ok(result).with({ userId: 'u1', operation: 'CREATE' })
   */
  with(attributes: Partial<InferSchema<T>>): this {
    this.#writer.with(attributes);
    return this;
  }

  /**
   * Set a message on the result entry
   * Example: ctx.ok(result).message('User created successfully')
   */
  message(text: string): this {
    // Use the unified message column via the writer's message setter
    // ResultWriter generates a message() method from the systemSchema
    const writer = this.#writer as ResultWriter<T, V, never> & { message?: (v: string) => unknown };
    if (typeof writer.message === 'function') {
      writer.message(text);
    }
    return this;
  }

  /**
   * Set the source code line number for this result entry
   * Example: ctx.ok(result).line(42)
   */
  line(lineNumber: number): this {
    // Use the writer's setter if available, otherwise fallback
    const writer = this.#writer as ResultWriter<T, V, never> & { lineNumber?: (v: number) => unknown };
    if (typeof writer.lineNumber === 'function') {
      writer.lineNumber(lineNumber);
    }
    return this;
  }
}

/**
 * Fluent error result with chaining support
 *
 * Uses ResultWriter internally for attribute writing while
 * maintaining proper TypeScript type narrowing via ErrorResult interface.
 */
export class FluentErrorResult<E, T extends LogSchema> implements ErrorResult<E> {
  readonly success = false as const;
  readonly error: { code: string; details: E };
  /** @internal - hidden from console.log via custom inspect */
  #writer: ResultWriter<T>;

  constructor(buffer: SpanBuffer<T>, code: string, details: E, schema: T) {
    this.error = { code, details };

    // Overwrite the pre-initialized span-exception with span-err
    buffer.entry_type[1] = ENTRY_TYPE_SPAN_ERR;

    // Write timestamp (nanoseconds since epoch)
    buffer.timestamp[1] = getTimestampNanos(buffer._traceRoot.anchorEpochNanos, buffer._traceRoot.anchorPerfNow);

    // Create ResultWriter for fluent attribute setting (writes to position 1)
    this.#writer = createResultWriter(schema, buffer, details, true);

    // Write error code using the writer if available
    const writer = this.#writer as ResultWriter<T, never, E> & { errorCode?: (v: string) => unknown };
    if (typeof writer.errorCode === 'function') {
      writer.errorCode(code);
    }

    // Note: writeIndex is NOT incremented - row 1 is reserved, events start at row 2
  }

  /** Clean output for console.log in Node.js */
  [Symbol.for('nodejs.util.inspect.custom')](): { success: false; error: { code: string; details: E } } {
    return { success: this.success, error: this.error };
  }

  /** Clean output for JSON.stringify */
  toJSON(): { success: false; error: { code: string; details: E } } {
    return { success: this.success, error: this.error };
  }

  /**
   * Set multiple attributes on the result entry
   * Example: ctx.err('ERROR', details).with({ userId: 'u1' })
   */
  with(attributes: Partial<InferSchema<T>>): this {
    this.#writer.with(attributes);
    return this;
  }

  /**
   * Set a message on the result entry
   * Example: ctx.err('ERROR', details).message('Operation failed')
   */
  message(text: string): this {
    // Use the unified message column via the writer's message setter
    // ResultWriter generates a message() method from the systemSchema
    const writer = this.#writer as ResultWriter<T, never, E> & { message?: (v: string) => unknown };
    if (typeof writer.message === 'function') {
      writer.message(text);
    }
    return this;
  }

  /**
   * Set the source code line number for this result entry
   * Example: ctx.err('ERROR', details).line(42)
   */
  line(lineNumber: number): this {
    // Use the writer's setter if available
    const writer = this.#writer as ResultWriter<T, never, E> & { lineNumber?: (v: number) => unknown };
    if (typeof writer.lineNumber === 'function') {
      writer.lineNumber(lineNumber);
    }
    return this;
  }
}
