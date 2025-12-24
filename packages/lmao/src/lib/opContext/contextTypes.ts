/**
 * Context Type Utilities for Op-Centric API
 *
 * Type-level utilities for handling user context property patterns:
 * - Required at createTrace (null sentinel)
 * - Optional (undefined sentinel)
 * - Has default (non-null, non-undefined value)
 *
 * Also includes reserved property validation to prevent conflicts with SpanContext.
 */

// =============================================================================
// RESERVED PROPERTY NAMES
// =============================================================================

/**
 * Reserved property names that cannot be used in user context.
 * These are properties that exist on SpanContext and would cause conflicts.
 */
export const RESERVED_CONTEXT_PROPS = [
  // Core SpanContext properties
  'buffer',
  'tag',
  'log',
  'scope',
  'setScope',
  'ok',
  'err',
  'span',
  'ff',
  'deps',
  // Internal properties
  '_buffer',
  '_schema',
  '_spanLogger',
] as const;

/**
 * Union type of all reserved context property names.
 */
export type ReservedContextProp = (typeof RESERVED_CONTEXT_PROPS)[number];

// =============================================================================
// CONTEXT TYPE UTILITIES
// =============================================================================

/**
 * Context property patterns:
 *
 * 1. Required at createTrace (null sentinel):
 *    `env: null as Env` -> must provide at createTrace, type is Env in SpanContext
 *
 * 2. Optional (undefined sentinel):
 *    `userId: undefined as string | undefined` -> optional at createTrace, may be undefined in SpanContext
 *
 * 3. Has default (non-null, non-undefined value):
 *    `config: { retryCount: 3 }` -> can override at createTrace, uses default if not provided
 */

/**
 * Extract keys where value is exactly `null` (required at trace creation).
 * These MUST be provided at createTrace().
 */
export type NullKeys<T> = {
  [K in keyof T]: [T[K]] extends [null] ? K : null extends T[K] ? (undefined extends T[K] ? never : K) : never;
}[keyof T];

/**
 * Extract keys where value is `undefined` or includes `undefined` (optional).
 * These are optional at createTrace() and may be undefined in SpanContext.
 */
export type UndefinedKeys<T> = {
  [K in keyof T]: undefined extends T[K] ? K : never;
}[keyof T];

/**
 * Extract keys with concrete values (neither null nor undefined).
 * These have defaults and are optional to override at createTrace().
 */
export type DefaultKeys<T> = {
  [K in keyof T]: null extends T[K] ? never : undefined extends T[K] ? never : K;
}[keyof T];

/**
 * Transform user context for SpanContext.
 * - Null properties become their non-null type (filled at createTrace)
 * - Undefined properties stay as potentially undefined
 * - Default properties stay as-is
 */
export type ResolvedContext<UserCtx> = {
  readonly [K in keyof UserCtx]: null extends UserCtx[K]
    ? undefined extends UserCtx[K]
      ? UserCtx[K] // null | undefined | T -> keep as is (weird case)
      : Exclude<UserCtx[K], null> // null | T -> T (null is sentinel, gets filled)
    : UserCtx[K]; // no null -> keep as is (including undefined if present)
};

/**
 * What MUST be provided at createTrace() - only null sentinel properties.
 */
export type RequiredContextParams<UserCtx> = {
  [K in NullKeys<UserCtx>]: Exclude<UserCtx[K], null>;
};

/**
 * What CAN be provided at createTrace() - optional and default properties.
 */
export type OptionalContextParams<UserCtx> = {
  [K in UndefinedKeys<UserCtx> | DefaultKeys<UserCtx>]?: undefined extends UserCtx[K]
    ? UserCtx[K] // Keep undefined in type for optional props
    : UserCtx[K]; // Default props
};

/**
 * Combined context params for createTrace().
 * - Null props are REQUIRED (must provide)
 * - Undefined props are OPTIONAL (can provide or omit)
 * - Default props are OPTIONAL (can override or use default)
 */
export type TraceContextParams<UserCtx> = RequiredContextParams<UserCtx> & OptionalContextParams<UserCtx>;

/**
 * Validate that user context doesn't use reserved property names.
 * Returns `never` with descriptive error if validation fails.
 */
export type ValidateUserContext<UserCtx> = keyof UserCtx extends infer K
  ? K extends ReservedContextProp
    ? { __error: `'${K & string}' is a reserved SpanContext property and cannot be used in ctx` }
    : UserCtx
  : UserCtx;
