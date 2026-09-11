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

import type { ResultWriterConstructor, WriterState } from './codegen/fixedPositionWriterGenerator.js';
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

/**
 * Ok/Err's lazy fallback only consumes these commands, never the writer's fluent
 * return type. Retaining the mapped ResultWriter here would leak its string index
 * signature through a private field and make concrete results invariant in T.
 */
interface BoundResultWriter<T extends LogSchema> {
  with(attributes: Partial<InferSchema<T>>): void;
  message(text: string): void;
  line(lineNumber: number): void;
}

function ensureResultWriter<T extends LogSchema>(
  writer: BoundResultWriter<T> | undefined,
  state: WriterState | undefined,
): BoundResultWriter<T> | undefined {
  if (writer || !state) return writer;
  return new state._physicalLayoutPlan.ResultWriterClass<T>(state);
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

/**
 * Deliberate failure for returning a completion that the executing span did not
 * create: a standalone or foreign Ok/Err. Shared by every ownership envelope so
 * the message cannot drift between runtime paths.
 */
export const SPAN_COMPLETION_OWNER_ERROR = 'Span callback must return its own ctx.ok() or ctx.err() result';

// =============================================================================
// OK CLASS
// =============================================================================

//#region smoo/lmao!n/lmao-entry-fluentok
/**
 * Success result with fluent writes to the creating span's reserved row 1.
 *
 * Ownership is runtime identity of the captured writer state, not a TypeScript
 * per-invocation brand. Value/schema inference stays ordinary; a foreign or
 * standalone result cannot complete a span. This is not a linear-use contract.
 */
export class Ok<V, T extends LogSchema = LogSchema> {
  readonly value: V;

  /** protected, not private: getResultClasses() subclasses Ok per schema to install row-1 fluent setters. */
  protected readonly _state: WriterState | undefined;
  private declare _writer: BoundResultWriter<T> | undefined;

  constructor(value: V, state?: WriterState) {
    this.value = value;
    this._state = state;
  }

  /** @internal A completion belongs to the exact context that created it. */
  _assertOwner(owner: object): void {
    if (this._state !== owner) {
      // Invariant: forwarding a foreign completion is a broken traced call graph.
      throw new TypeError(SPAN_COMPLETION_OWNER_ERROR);
    }
  }

  private _resultWriter(): BoundResultWriter<T> | undefined {
    const writer = ensureResultWriter<T>(this._writer, this._state);
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

  /** Transform the success value. Ownership follows the original result. */
  map<U>(fn: (value: V) => U): Ok<U, T> {
    return new Ok<U, T>(fn(this.value), this._state);
  }

  /** No-op for Ok (error transformation). */
  mapErr<F>(_fn: (error: never) => F): Ok<V, T> {
    return this;
  }

  /**
   * Transform the success value, potentially returning an error.
   * Ownership follows the callback's result: chaining a same-span
   * `ctx.ok()` stays returnable; a foreign result stays rejected.
   */
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
   * Writes immediately to the creating span's reserved completion row.
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
 * Error result with fluent writes to the creating span's reserved row 1.
 * The captured writer state is its runtime owner, just as for Ok.
 */
export class Err<E, T extends LogSchema = LogSchema> {
  readonly error: E;

  /** protected, not private: getResultClasses() subclasses Err per schema to install row-1 fluent setters. */
  protected readonly _state: WriterState | undefined;
  private declare _writer: BoundResultWriter<T> | undefined;

  constructor(error: E, state?: WriterState) {
    this.error = error;
    this._state = state;
  }

  /** @internal A completion belongs to the exact context that created it. */
  _assertOwner(owner: object): void {
    if (this._state !== owner) {
      // Invariant: forwarding a foreign completion is a broken traced call graph.
      throw new TypeError(SPAN_COMPLETION_OWNER_ERROR);
    }
  }

  private _resultWriter(): BoundResultWriter<T> | undefined {
    const writer = ensureResultWriter<T>(this._writer, this._state);
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

  /** Transform the error. Ownership follows the original result. */
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
   * Writes immediately to the creating span's reserved completion row.
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

/** Union type for Result. Invocation ownership is checked at runtime. */
export type Result<V, E, T extends LogSchema = LogSchema> = Ok<V, T> | Err<E, T>;

/**
 * Schema-brand-wildcard Result for conditional inference positions (`infer S` / `infer E`)
 * where the LogSchema brand must not participate in constraint solving. Same rationale as
 * AnyResult: the `any` keeps variance open so per-op success/error types flow through.
 */
// biome-ignore lint/suspicious/noExplicitAny: inference wildcard by design; see doc above.
export type InferResult<S, E> = Result<S, E, any>;

/**
 * Wildcard Result for inference positions (Op/trace/span result kinds) where the
 * schema brand must not participate in constraint solving. The `any`s are the point:
 * they keep variance open so per-op success/error types flow through untouched.
 */
// biome-ignore lint/suspicious/noExplicitAny: inference wildcard by design; see doc above.
export type AnyResult = Result<any, any, any>;

// =============================================================================
// ROW-1 FLUENT SETTERS (ctx.ok(v).status(200), mirroring ctx.tag on row 0)
// =============================================================================

//#region smoo/lmao!n/lmao-entry-result-row-setters
/** Result members and JavaScript protocols must never become schema setters. */
type ResultMember = keyof Ok<unknown> | keyof Err<unknown> | keyof Object | 'then' | `_${string}`;

/**
 * A schema-bound success result. The base Ok intersection retains result identity
 * and inference; the additional overloads retain the fluent surface after chaining.
 *
 * WHY exclude a broad string key: an unspecified LogSchema is not a promise that
 * every string is a setter. Such an index signature also collides with Ok's own
 * members and makes concrete schemas invariant when an Op returns a loose Result.
 */
export type OkResult<V, T extends LogSchema = LogSchema> = {
  with(attributes: Partial<InferSchema<T>>): OkResult<V, T>;
  message(text: string): OkResult<V, T>;
  line(lineNumber: number): OkResult<V, T>;
  uint64_value(value: bigint): OkResult<V, T>;
  map<U>(fn: (value: V) => U): OkResult<U, T>;
  mapErr<F>(fn: (error: never) => F): OkResult<V, T>;
  flatMap<R extends AnyResult>(fn: (value: V) => R): R;
} & Ok<V, T> & {
    [K in keyof InferSchema<T> as string extends K ? never : K extends ResultMember ? never : K]: (
      value: InferSchema<T>[K],
    ) => OkResult<V, T>;
  };

/** A schema-bound error result, with the same row-1 fluent contract as OkResult. */
export type ErrResult<E, T extends LogSchema = LogSchema> = {
  with(attributes: Partial<InferSchema<T>>): ErrResult<E, T>;
  message(text: string): ErrResult<E, T>;
  line(lineNumber: number): ErrResult<E, T>;
  uint64_value(value: bigint): ErrResult<E, T>;
  map<U>(fn: (value: never) => U): ErrResult<E, T>;
  mapErr<F>(fn: (error: E) => F): ErrResult<F, T>;
  flatMap<U, F>(fn: (value: never) => Result<U, F>): ErrResult<E, T>;
} & Err<E, T> & {
    [K in keyof InferSchema<T> as string extends K ? never : K extends ResultMember ? never : K]: (
      value: InferSchema<T>[K],
    ) => ErrResult<E, T>;
  };

/** Payload generics belong to each construction, not to the cached class pair. */
export type OkClassConstructor<T extends LogSchema> = new <V>(value: V, state: WriterState) => OkResult<V, T>;
export type ErrClassConstructor<T extends LogSchema> = new <E>(error: E, state: WriterState) => ErrResult<E, T>;

export interface ResultClasses<T extends LogSchema> {
  readonly OkClass: OkClassConstructor<T>;
  readonly ErrClass: ErrClassConstructor<T>;
}

/**
 * Bridge the dynamically installed schema API to its mapped type. Only class pairs
 * assembled below enter this cache; prototype checks verify the underlying Result
 * identity, while the writer constructor supplies the schema-specific methods.
 */
function isResultClasses<T extends LogSchema>(value: unknown): value is ResultClasses<T> {
  if (typeof value !== 'object' || value === null) return false;
  const ok = Reflect.get(value, 'OkClass');
  const err = Reflect.get(value, 'ErrClass');
  return (
    typeof ok === 'function' &&
    ok.prototype instanceof Ok &&
    typeof err === 'function' &&
    err.prototype instanceof Err
  );
}

const resultClassCache = new WeakMap<ResultWriterConstructor, unknown>();

/**
 * Reuse the plan's already-specialized writer methods, not a second materializer.
 * The constructor identity includes schema, enum binding, materializer mode, and
 * message layout. Cache hits return the same pair without another allocation.
 *
 * Every installed method uses the same captured _state as Ok/Err and returns its
 * receiver. Copying descriptors preserves compiled direct writes on JIT runtimes
 * and the no-eval closure path on workerd, without allocating a ResultWriter.
 *
 * State is mandatory for these subclasses. Standalone new Ok/Err values retain
 * their original no-op fluent behavior; mapped span results retain their owner
 * and the schema-bound subclass instead of silently losing the fluent surface.
 */
export function getResultClasses<T extends LogSchema>(WriterClass: ResultWriterConstructor): ResultClasses<T> {
  let classes = resultClassCache.get(WriterClass);
  if (!classes) {
    class SchemaOk<V> extends Ok<V, T> {
      protected declare readonly _state: WriterState;

      constructor(value: V, state: WriterState) {
        super(value, state);
      }

      override map<U>(fn: (value: V) => U): SchemaOk<U> {
        return new SchemaOk(fn(this.value), this._state);
      }
    }

    class SchemaErr<E> extends Err<E, T> {
      protected declare readonly _state: WriterState;

      constructor(error: E, state: WriterState) {
        super(error, state);
      }

      override mapErr<F>(fn: (error: E) => F): SchemaErr<F> {
        return new SchemaErr(fn(this.error), this._state);
      }
    }

    const descriptors = Object.getOwnPropertyDescriptors(WriterClass.prototype);
    for (const name of Object.keys(descriptors)) {
      // Raw low-level LogSchemas can contain names rejected by defineLogSchema.
      // Never overwrite payloads, ownership checks, or JavaScript protocols.
      // Existing bulk/system methods deliberately replace the lazy writer path.
      if (
        name === 'constructor' ||
        name === 'then' ||
        name === 'value' ||
        name === 'error' ||
        name.startsWith('_') ||
        ((name in Ok.prototype || name in Err.prototype) &&
          name !== 'with' &&
          name !== 'message' &&
          name !== 'line')
      ) {
        delete descriptors[name];
      }
    }
    Object.defineProperties(SchemaOk.prototype, descriptors);
    Object.defineProperties(SchemaErr.prototype, descriptors);
    classes = { OkClass: SchemaOk, ErrClass: SchemaErr };
    resultClassCache.set(WriterClass, classes);
  }

  if (!isResultClasses<T>(classes)) {
    throw new TypeError('Invalid schema-bound Ok/Err constructor pair');
  }
  return classes;
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
  return error !== null && typeof error === 'object' && 'code' in error && typeof error.code === 'string';
}

// =============================================================================
// RESULT TYPE EXTRACTION UTILITIES
// =============================================================================

/**
 * Extract success type from a Result type.
 * Returns `never` if R is not a Result.
 */
export type ResultSuccess<R> = R extends InferResult<infer S, unknown> ? S : never;

/**
 * Extract success type from a Promise<Result> type.
 * Returns `never` if R is not a Promise<Result>.
 */
export type PromiseResultSuccess<R> = R extends Promise<InferResult<infer S, unknown>> ? S : never;

/**
 * Extract success type from either Result or Promise<Result>.
 * Useful for inferring the success type from Op return values.
 */
export type ExtractSuccess<R> = ResultSuccess<R> | PromiseResultSuccess<R>;

/**
 * Extract error type from a Result type.
 * Returns `never` if R is not a Result.
 */
export type ResultError<R> = R extends InferResult<unknown, infer E> ? E : never;

/**
 * Extract error type from a Promise<Result> type.
 * Returns `never` if R is not a Promise<Result>.
 */
export type PromiseResultError<R> = R extends Promise<InferResult<unknown, infer E>> ? E : never;

/**
 * Extract error type from either Result or Promise<Result>.
 * Useful for inferring the error type from Op return values.
 */
export type ExtractError<R> = ResultError<R> | PromiseResultError<R>;
