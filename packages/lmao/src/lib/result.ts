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
 * ## Result Row Writer
 *
 * Span start reserves row 1 for the eventual span-end entry, so fluent result
 * methods can write directly to that fixed row instead of capturing deferred
 * closure chains.
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

import {
  createFixedFieldPlans,
  installFixedWriterMethods,
  installResultSystemMethods,
  type ResultWriter,
  type WriterState,
} from './codegen/fixedPositionWriterGenerator.js';
import type { SchemaEnumLookupDescriptor } from './enumMetadata.js';
import type { MessageLayoutFamily } from './runtimeHint.js';
import type { InferSchema, LogSchema } from './schema/types.js';

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
  readonly prototype: T;
  readonly [Symbol.hasInstance]: (value: unknown) => boolean;
}

type OkJson<V> = { ok: true; value: V };
type ErrJson<E> = { ok: false; error: E };

type ErrPredicate<E> = (error: E) => boolean;
type ErrClassMatcher = abstract new (...args: never[]) => unknown;
type ErrMatcher<E> = TaggedErrorConstructor<TaggedError> | ErrClassMatcher | ErrPredicate<E>;

type BoundResultWriter<T extends LogSchema, R, E> = ResultWriter<T, R, E>;

function ensureResultWriter<T extends LogSchema, R, E>(
  writer: BoundResultWriter<T, R, E> | undefined,
  state: WriterState | undefined,
): BoundResultWriter<T, R, E> | undefined {
  if (writer || !state) return writer;
  return new state._physicalLayoutPlan.ResultWriterClass<T, R, E>(state);
}

function isErrPredicate<E>(value: ErrMatcher<E>): value is ErrPredicate<E> {
  const prototype = Reflect.get(value, 'prototype');
  return prototype === undefined || prototype === Function.prototype;
}

function isInstanceofMatcher<E>(value: ErrMatcher<E>): value is TaggedErrorConstructor<TaggedError> | ErrClassMatcher {
  return typeof value === 'function' && !isErrPredicate(value);
}

function createCodeErrorValue<Code extends string, Fields extends object>(
  prototype: object,
  fields: Fields,
): CodeErrorInstance<Code, Fields> & Fields {
  return Object.assign(Object.create(prototype), fields);
}

// =============================================================================
// OK CLASS
// =============================================================================

//#region smoo/lmao!n/lmao-entry-fluentok
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

  /** protected, not private: getResultClasses() subclasses Ok per schema to install row-1 fluent setters. */
  protected readonly _state: WriterState | undefined;
  private declare _writer: BoundResultWriter<T, V, never> | undefined;

  constructor(value: V, state?: WriterState) {
    this.value = value;
    this._state = state;
  }

  private _resultWriter(): BoundResultWriter<T, V, never> | undefined {
    const writer = ensureResultWriter<T, V, never>(this._writer, this._state);
    if (writer) this._writer = writer;
    return writer;
  }

  /** Discriminant for type narrowing. */
  get success(): true {
    return true;
  }

  /** Tag discriminant for tagged result interop — matches the { _tag: 'ok' } protocol. */
  get _tag(): 'ok' {
    return 'ok';
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
    return new Ok<U, T>(fn(this.value), this._state);
  }

  /** No-op for Ok (error transformation). */
  mapErr<F>(_fn: (error: never) => F): Ok<V, T> {
    return this;
  }

  /** Transform the success value, potentially returning an error. */
  flatMap<U, F>(fn: (value: V) => Result<U, F, T>): Result<U, F, T> {
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
    this._resultWriter()?.with(attributes);
    return this;
  }

  /**
   * Set result message on span-end entry (row 1).
   * Overwrites the default span name in the message column.
   *
   * @example ctx.ok(result).message('User created successfully')
   */
  message(text: string): this {
    this._resultWriter()?.message(text);
    return this;
  }

  /**
   * Set source code line number on span-end entry (row 1).
   * Typically injected by transformer.
   *
   * @example ctx.ok(result).line(42)
   */
  line(lineNumber: number): this {
    this._resultWriter()?.line(lineNumber);
    return this;
  }

  [Symbol.for('nodejs.util.inspect.custom')](): OkJson<V> {
    return { ok: true, value: this.value };
  }

  toJSON(): OkJson<V> {
    return { ok: true, value: this.value };
  }
}
//#endregion smoo/lmao!n/lmao-entry-fluentok

// =============================================================================
// ERR CLASS
// =============================================================================

//#region smoo/lmao!n/lmao-entry-fluenterr
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

  /** protected, not private: getResultClasses() subclasses Err per schema to install row-1 fluent setters. */
  protected readonly _state: WriterState | undefined;
  private declare _writer: BoundResultWriter<T, never, E> | undefined;

  constructor(error: E, state?: WriterState) {
    this.error = error;
    this._state = state;
  }

  private _resultWriter(): BoundResultWriter<T, never, E> | undefined {
    const writer = ensureResultWriter<T, never, E>(this._writer, this._state);
    if (writer) this._writer = writer;
    return writer;
  }

  /** Discriminant for type narrowing. */
  get success(): false {
    return false;
  }

  /** Tag discriminant for tagged result interop — matches the { _tag: 'err' } protocol. */
  get _tag(): 'err' {
    return 'err';
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
    tagOrPredicate?: TaggedErrorConstructor<Tag> | ErrClassMatcher | ErrPredicate<E>,
  ): boolean {
    if (tagOrPredicate === undefined) return true;

    // instanceof check for classes (including TaggedError and CodeError)
    if (isErrPredicate(tagOrPredicate)) {
      return tagOrPredicate(this.error);
    }

    if (isInstanceofMatcher(tagOrPredicate)) {
      return this.error instanceof tagOrPredicate;
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
    return new Err<F, T>(fn(this.error), this._state);
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
    this._resultWriter()?.with(attributes);
    return this;
  }

  /**
   * Set result message on span-end entry (row 1).
   * Overwrites the default span name in the message column.
   *
   * @example ctx.err('ERROR', details).message('Operation failed')
   */
  message(text: string): this {
    this._resultWriter()?.message(text);
    return this;
  }

  /**
   * Set source code line number on span-end entry (row 1).
   * Typically injected by transformer.
   *
   * @example ctx.err('ERROR', details).line(42)
   */
  line(lineNumber: number): this {
    this._resultWriter()?.line(lineNumber);
    return this;
  }

  [Symbol.for('nodejs.util.inspect.custom')](): ErrJson<E> {
    return { ok: false, error: this.error };
  }

  toJSON(): ErrJson<E> {
    return { ok: false, error: this.error };
  }
}
//#endregion smoo/lmao!n/lmao-entry-fluenterr

// =============================================================================
// RESULT TYPE
// =============================================================================

/** Union type for Result - either Ok or Err. */
export type Result<V, E, T extends LogSchema = LogSchema> = Ok<V, T> | Err<E, T>;

// =============================================================================
// ROW-1 FLUENT SETTERS (ctx.ok(v).status(200), mirroring ctx.tag on row 0)
// =============================================================================

//#region smoo/lmao!n/lmao-entry-result-row-setters
/**
 * `Ok`, widened with the schema's row-1 fluent setters (mirrors `TagWriter` on row 0):
 * `ctx.ok(v).status(200).with({ userId })` and `ctx.ok(v).with({ userId }).status(200)`
 * both type-check and stay on this type. Self-referencing type alias, inlined (not
 * routed through a shared `FieldSetters<T, Self>` helper) — same shape `TagWriter<T>`
 * uses in fixedPositionWriterGenerator.ts; routing the mapped type through a separate
 * named generic makes the alias circularly reference itself instead of resolving.
 *
 * @example ctx.ok(cart.value).status(200)
 */
export type OkResult<V, T extends LogSchema = LogSchema> = Ok<V, T> & {
  [K in keyof InferSchema<T>]: (value: InferSchema<T>[K]) => OkResult<V, T>;
};

/** `Err`, widened with the schema's row-1 fluent setters. Returned by a schema-bound `ctx.err()`. See `OkResult` for why this is inlined. */
export type ErrResult<E, T extends LogSchema = LogSchema> = Err<E, T> & {
  [K in keyof InferSchema<T>]: (value: InferSchema<T>[K]) => ErrResult<E, T>;
};

/**
 * Generic construct signatures (`V`/`E` scoped to the call, not the type) so each
 * `new OkClass(value, state)` preserves its own value's type — mirrors
 * `ResultWriterConstructor` in fixedPositionWriterGenerator.ts, same reason.
 */
export type OkClassConstructor<T extends LogSchema> = new <V>(value: V, state: WriterState) => OkResult<V, T>;
export type ErrClassConstructor<T extends LogSchema> = new <E>(error: E, state: WriterState) => ErrResult<E, T>;

/** One schema's row-1-aware Ok/Err class pair, cached per schema in `getResultClasses`. */
export interface ResultClasses<T extends LogSchema> {
  readonly OkClass: OkClassConstructor<T>;
  readonly ErrClass: ErrClassConstructor<T>;
}

/**
 * Narrow `unknown` to a schema-bound constructor after `installFixedWriterMethods`
 * installs the row-1 setters at runtime — the same bridge `isTagWriterConstructor` and
 * `isResultWriterConstructor` use in fixedPositionWriterGenerator.ts for the identical
 * problem: the installed members are invisible to the class's own static shape, so no
 * assignment from the freshly-declared class to the richer public type can check out
 * structurally. `typeof value === 'function'` is the real, load-bearing runtime check;
 * the type predicate just gives the compiler the widened type once it holds. Callers
 * must hold the value as `unknown` first (see `getResultClasses` below) — narrowing a
 * concretely-typed class reference straight to an unrelated target type doesn't apply
 * the same way it does starting from `unknown`.
 */
function isOkClassConstructor<T extends LogSchema>(value: unknown): value is OkClassConstructor<T> {
  return typeof value === 'function';
}
function isErrClassConstructor<T extends LogSchema>(value: unknown): value is ErrClassConstructor<T> {
  return typeof value === 'function';
}

/** Cache key only needs messageLayoutFamily: it's the only input to the installers below besides schema/enumLookup. */
const resultClassCache = new WeakMap<LogSchema, Map<MessageLayoutFamily, ResultClasses<LogSchema>>>();

/**
 * Get or create the schema-bound `Ok`/`Err` subclasses that carry row-1 fluent setters.
 *
 * WHY `state` is mandatory on these constructors (unlike the base `Ok`/`Err`
 * constructors, where `state` is optional for standalone use like `new Ok(null)`): the
 * installed setters write straight through `this._state._spanBuffer` with no optional
 * chaining, mirroring how `installFixedWriterMethods` already writes TagWriter's row 0.
 * That's safe because these classes are only ever constructed by `SpanContext.ok`/
 * `.err()`, which always has a real span to write to — requiring `state` here makes
 * that invariant a type error instead of a runtime one if some other call site ever
 * tries to construct one directly.
 *
 * Standalone results built via `new Ok(value)` (e.g. `cloudflare/classSplit.ts`'s
 * shared singleton) stay on the base `Ok`/`Err` classes and never see these setters —
 * exactly matching the API surface those two constructions have (`.map()`/`.mapErr()`/
 * `.flatMap()` also stay on the base classes, for the same reason).
 */
export function getResultClasses<T extends LogSchema>(
  schema: T,
  messageLayoutFamily: MessageLayoutFamily,
  enumLookup: SchemaEnumLookupDescriptor,
): ResultClasses<T> {
  let familyClasses = resultClassCache.get(schema);
  let cached: ResultClasses<LogSchema> | undefined = familyClasses?.get(messageLayoutFamily);

  if (!cached) {
    class SchemaOk<V> extends Ok<V, T> {}
    class SchemaErr<E> extends Err<E, T> {}

    const plans = createFixedFieldPlans(schema, enumLookup);
    for (const prototype of [SchemaOk.prototype, SchemaErr.prototype]) {
      installFixedWriterMethods(prototype, plans, 1);
      installResultSystemMethods(prototype, messageLayoutFamily);
    }

    // Erase to `unknown` before narrowing — see the WHY on isOkClassConstructor above.
    const okCtor: unknown = SchemaOk;
    const errCtor: unknown = SchemaErr;
    if (!isOkClassConstructor<LogSchema>(okCtor) || !isErrClassConstructor<LogSchema>(errCtor)) {
      throw new Error('Failed to generate schema-bound Ok/Err constructors');
    }

    cached = { OkClass: okCtor, ErrClass: errCtor };
    familyClasses ??= new Map();
    familyClasses.set(messageLayoutFamily, cached);
    resultClassCache.set(schema, familyClasses);
  }

  const cachedOkCtor: unknown = cached.OkClass;
  const cachedErrCtor: unknown = cached.ErrClass;
  if (!isOkClassConstructor<T>(cachedOkCtor) || !isErrClassConstructor<T>(cachedErrCtor)) {
    throw new Error('Invalid cached Ok/Err constructor pair');
  }

  return { OkClass: cachedOkCtor, ErrClass: cachedErrCtor };
}
//#endregion smoo/lmao!n/lmao-entry-result-row-setters

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
    const CodeError: CodeErrorClass<Code, Fields> = function (
      this: (CodeErrorInstance<Code, Fields> & Fields) | undefined,
      fields: Fields,
    ): CodeErrorInstance<Code, Fields> & Fields {
      // Allow calling without 'new'
      if (!(this instanceof CodeError)) {
        return createCodeErrorValue<Code, Fields>(CodeError.prototype, fields);
      }
      // Assign fields as own properties
      Object.assign(this, fields);
      return this;
    };

    // Put code on prototype so all instances share it
    Object.defineProperty(CodeError.prototype, 'code', {
      value: code,
      writable: false,
      enumerable: true,
      configurable: false,
    });

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
export type ResultSuccess<R> = R extends Result<infer S, unknown, LogSchema> ? S : never;

/**
 * Extract success type from a Promise<Result> type.
 * Returns `never` if R is not a Promise<Result>.
 */
export type PromiseResultSuccess<R> = R extends Promise<Result<infer S, unknown, LogSchema>> ? S : never;

/**
 * Extract success type from either Result or Promise<Result>.
 * Useful for inferring the success type from Op return values.
 */
export type ExtractSuccess<R> = ResultSuccess<R> | PromiseResultSuccess<R>;

/**
 * Extract error type from a Result type.
 * Returns `never` if R is not a Result.
 */
export type ResultError<R> = R extends Result<unknown, infer E, LogSchema> ? E : never;

/**
 * Extract error type from a Promise<Result> type.
 * Returns `never` if R is not a Promise<Result>.
 */
export type PromiseResultError<R> = R extends Promise<Result<unknown, infer E, LogSchema>> ? E : never;

/**
 * Extract error type from either Result or Promise<Result>.
 * Useful for inferring the error type from Op return values.
 */
export type ExtractError<R> = ResultError<R> | PromiseResultError<R>;
