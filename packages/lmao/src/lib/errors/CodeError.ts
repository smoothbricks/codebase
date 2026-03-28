/**
 * CodeError - typed error codes with data payloads.
 *
 * Use the `Code()` helper to define typed error codes, then use them with `ctx.err()`.
 * The error code becomes the `_tag` for discrimination with `result.isErr(CODE)`.
 *
 * @example
 * ```typescript
 * import { Code } from '@smoothbricks/lmao/errors/CodeError';
 *
 * // Define typed error codes
 * const NOT_FOUND = Code<{ id: string }>('NOT_FOUND');
 * const VALIDATION_ERROR = Code<{ field: string; reason: string }>('VALIDATION_ERROR');
 * const TIMEOUT = Code('TIMEOUT'); // No data payload
 *
 * // Use in Ops
 * const fetchUser = defineOp('fetchUser', async (ctx, id: string) => {
 *   const user = await db.find(id);
 *   if (!user) {
 *     return ctx.err(NOT_FOUND({ id }));
 *   }
 *   return ctx.ok(user);
 * });
 *
 * // Discriminate errors
 * const result = await trace('fetch', fetchUser, 'abc');
 * if (result.isErr(NOT_FOUND)) {
 *   console.log(`User ${result.error.data.id} not found`);
 * }
 * ```
 */

import type { TaggedError, TaggedErrorConstructor } from '../result.js';

type CodeErrorJson<C extends string, D> = { code: C; data: D };

/**
 * Error instance created by a Code constructor.
 *
 * @typeParam C - The error code string literal type (also serves as _tag)
 * @typeParam D - The data payload type
 */
export class CodeError<C extends string, D = void> extends Error implements TaggedError<C> {
  /**
   * The error code - also serves as the tag for discrimination.
   */
  readonly code: C;

  /**
   * The typed data payload.
   */
  readonly data: D;

  constructor(code: C, data: D) {
    super(code);
    this.name = 'CodeError';
    this.code = code;
    this.data = data;
  }

  /**
   * Tag for discrimination with isErr(Tag).
   * Returns the error code.
   */
  get _tag(): C {
    return this.code;
  }

  [Symbol.for('nodejs.util.inspect.custom')](): CodeErrorJson<C, D> {
    return { code: this.code, data: this.data };
  }

  toJSON(): CodeErrorJson<C, D> {
    return { code: this.code, data: this.data };
  }
}

/**
 * Constructor function for a specific error code.
 * Has static `_tag` for use with `isErr(CODE)`.
 */
export interface CodeConstructor<C extends string, D = void> extends TaggedErrorConstructor<CodeError<C, D>> {
  (data: D): CodeError<C, D>;
  readonly _tag: C;
}

/**
 * Constructor function for error codes with no data.
 */
export interface CodeConstructorVoid<C extends string> extends TaggedErrorConstructor<CodeError<C, void>> {
  (): CodeError<C, void>;
  readonly _tag: C;
}

type CodeFactoryReturn<C extends string, D> = [unknown] extends [D]
  ? CodeConstructor<C, D>
  : undefined extends D
    ? CodeConstructorVoid<C>
    : CodeConstructor<C, D>;

/**
 * Define a typed error code.
 *
 * @param code - The error code string (becomes _tag for discrimination)
 * @returns A constructor function that creates CodeError instances
 *
 * @example
 * ```typescript
 * // With typed data
 * const NOT_FOUND = Code<{ id: string }>('NOT_FOUND');
 * ctx.err(NOT_FOUND({ id: 'abc' }));
 *
 * // Without data
 * const TIMEOUT = Code('TIMEOUT');
 * ctx.err(TIMEOUT());
 *
 * // Discrimination
 * if (result.isErr(NOT_FOUND)) {
 *   console.log(result.error.data.id); // TypeScript knows the type
 * }
 * ```
 */
export function Code<C extends string>(code: C): CodeConstructorVoid<C>;
export function Code<D, C extends string = string>(code: C): CodeFactoryReturn<C, D>;
export function Code(code: string) {
  function createCodeError(data?: unknown) {
    return new CodeError(code, data);
  }

  Object.defineProperty(createCodeError, '_tag', { value: code, writable: false, enumerable: true });
  return createCodeError;
}
