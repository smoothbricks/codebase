/**
 * @smoothbricks/validation
 *
 * Eval-free shared runtime narrowing and assertions.
 * Zero runtime dependencies (only tslib). Safe for all runtimes
 * (Bun, Node, browser, Expo, AWS, Cloudflare).
 *
 * @example
 * ```typescript
 * import { isRecord, assertDefined } from '@smoothbricks/validation';
 *
 * if (isRecord(data)) {
 *   console.log(data.someKey);  // narrowed to Record<string, unknown>
 * }
 *
 * assertDefined(user, 'User must exist');  // throws if null/undefined
 *
 * console.log('validated');
 * ```
 *
 * @packageDocumentation
 */

export { assertDefined, assertNever, assertRecord, expectDefined } from './assert.js';
export {
  hasOwn,
  hasOwnBigInt,
  hasOwnBoolean,
  hasOwnNumber,
  hasOwnString,
  isBigInt,
  isBoolean,
  isNumber,
  isPlainObject,
  isRecord,
  isString,
} from './guards.js';
