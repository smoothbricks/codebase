/**
 * @smoothbricks/validation
 *
 * Eval-free shared runtime narrowing, assertions, and JSON boundary parsing.
 * Zero runtime dependencies (only tslib). Safe for all runtimes
 * (Bun, Node, browser, Expo, AWS, Cloudflare).
 *
 * @example
 * ```typescript
 * import { isRecord, assertDefined, parseJsonRecord } from '@smoothbricks/validation';
 *
 * if (isRecord(data)) {
 *   console.log(data.someKey);  // narrowed to Record<string, unknown>
 * }
 *
 * assertDefined(user, 'User must exist');  // throws if null/undefined
 *
 * const result = parseJsonRecord(raw);
 * if (result.ok) {
 *   console.log(result.value);  // Record<string, unknown>
 * }
 * ```
 *
 * @packageDocumentation
 */

export { assertDefined, assertNever, assertRecord } from './assert.js';
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
export type { JsonParseResult } from './json.js';
export { parseJsonArray, parseJsonRecord, safeJsonParse } from './json.js';
