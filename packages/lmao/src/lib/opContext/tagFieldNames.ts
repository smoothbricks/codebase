/**
 * Field names a tag context cannot carry.
 *
 * A log schema's fields become setters on `ctx.tag`, on log entries and on `Ok` / `Err` results. A field named
 * like a member of those objects would shadow it, so the op context that builds them refuses the name. This is a
 * rule of the tag layer alone: a field schema that nothing tags through may use any name.
 */

/**
 * Reserved method names that cannot be used as attribute names
 *
 * These names are reserved to prevent conflicts or confusion:
 * - Technically conflicting: Names that would shadow actual methods/properties
 *   on TagWriter, SpanLogger, FluentLogEntry, or result objects
 * - Confusing: Names that would create confusing code patterns even though
 *   they don't technically conflict (e.g., ctx.tag.tag() or ctx.scope.scope)
 */
const RESERVED_NAMES = new Set([
  // Technically conflicting - methods on TagWriter/FluentLogEntry/result objects
  'then', // Prevent Promise/await from treating results as thenables
  'constructor', // Reserved by JavaScript classes and generated writers
  'toString', // Object protocol members must not become field setters
  'toLocaleString',
  'valueOf',
  'hasOwnProperty',
  'isPrototypeOf',
  'propertyIsEnumerable',
  'with', // Used on TagWriter, FluentLogEntry, Ok, Err
  'message', // Used on Ok, Err
  'line', // Used on Ok, Err, FluentLogEntry (also a system column)

  // Technically conflicting - Ok/Err instance properties and prototype members. Ok/Err
  // install row-1 fluent setters per schema field (mirrors TagWriter on row 0), so a
  // field sharing one of these names would shadow the real member instead of adding a
  // setter. `success` is the sharp edge: it's a getter on the Ok/Err prototype, so a
  // same-named field would silently overwrite it with a function - always truthy,
  // treating every error as a success.
  'success', // Ok.success / Err.success getter (ok/err discriminant)
  'value', // Ok.value instance property (the success payload)
  'map', // Ok.map / Err.map
  'mapErr', // Ok.mapErr / Err.mapErr
  'flatMap', // Ok.flatMap / Err.flatMap
  'match', // Ok.match / Err.match
  'isOk', // Ok.isOk / Err.isOk
  'isErr', // Ok.isErr / Err.isErr
  'maybeValue', // Ok.maybeValue / Err.maybeValue
  'maybeError', // Ok.maybeError / Err.maybeError
  'unwrapOr', // Ok.unwrapOr / Err.unwrapOr
  'unwrapOrElse', // Ok.unwrapOrElse / Err.unwrapOrElse
  'toJSON', // Ok.toJSON / Err.toJSON

  // Technically conflicting - log level methods on SpanLogger
  'info', // ctx.log.info()
  'debug', // ctx.log.debug()
  'warn', // ctx.log.warn()
  'error', // ctx.log.error() -- also Err.error instance property (the error payload)
  'trace', // ctx.log.trace()

  // Internal SpanLogger methods (prefixed with _ but included for completeness)
  '_nextRow', // Internal row advancement
  '_buffer', // Internal buffer reference
  '_writeIndex', // Internal write position
  '_getNextBuffer', // Internal buffer chaining
  '_setScope', // Internal scope setter
  '_prefillScopedAttributes', // Internal scope prefill

  // Internal FF logging methods (called by evaluator, not on public type)
  'ffAccess', // Feature flag access logging
  'ffUsage', // Feature flag usage logging

  // Confusing - properties on SpanContext (not technically conflicting)
  'tag', // ctx.tag is TagWriter; ctx.tag.tag() would be confusing
  'scope', // ctx.scope is readonly; ctx.scope.scope would be confusing

  // Confusing - methods on SpanContext (not technically conflicting)
  'ok', // ctx.ok() is method; ctx.tag.ok() or ctx.ok(result).ok() would be confusing
  'err', // ctx.err() is method; ctx.tag.err() or ctx.err(code, details).err() would be confusing
  'span', // ctx.span() is method; ctx.tag.span() would be confusing
]);

/**
 * Assert that the fields a tag context is built from carry no reserved name and none starting with `_`
 * (reserved for system properties).
 *
 * @throws Error if any field name is reserved or starts with _
 */
export function assertTagFieldNames(fieldNames: readonly string[]): void {
  for (const name of fieldNames) {
    if (name.startsWith('_')) {
      throw new Error(`Field name '${name}' cannot start with '_' - this prefix is reserved for system properties`);
    }
    if (RESERVED_NAMES.has(name)) {
      throw new Error(
        `Attribute name '${name}' is reserved and cannot be used. ` +
          `Reserved names: ${Array.from(RESERVED_NAMES).join(', ')}`,
      );
    }
  }
}
