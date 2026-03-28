/**
 * Op-Centric API for LMAO - Main Entry Point
 *
 * This is the factory function that creates the Op-centric API.
 * All types are exported from './opContext/index.js'
 *
 * @example
 * ```typescript
 * import { defineOpContext, S, defineLogSchema } from '@smoothbricks/lmao';
 *
 * const { defineOp, defineOps } = defineOpContext({
 *   logSchema: defineLogSchema({ userId: S.category() }),
 *   ctx: { env: null as Env },
 * });
 *
 * export const myHandler = defineOp('handler', async (ctx, req: Request) => {
 *   ctx.tag.userId('user-123');
 *   return ctx.ok({ status: 'ok' });
 * });
 * ```
 */

import {
  type AnyOpGroup,
  type AnyOpGroupPublic,
  createDefineOp,
  createDefineOps,
  createOpGroup,
  type DepsConfig,
  type EffectiveSchema,
  type FeatureFlagSchema,
  isMappedOpGroup,
  type OpContext,
  type OpContextConfig,
  type OpContextFactory,
  RESERVED_CONTEXT_PROPS,
  type ValidateUserContext,
} from './opContext/index.js';
import { LogSchema } from './schema/LogSchema.js';
import { mergeWithSystemSchema, SYSTEM_SCHEMA_FIELD_NAMES } from './schema/systemSchema.js';
import type { SchemaFields } from './schema/types.js';

const EMPTY_FLAGS = Object.freeze({}) satisfies Record<string, never>;
const EMPTY_DEPS = Object.freeze({}) satisfies Record<string, never>;
const EMPTY_USER_CTX = Object.freeze({}) satisfies Record<string, never>;

// =============================================================================
// EFFECTIVE SCHEMA COMPUTATION
// =============================================================================

// isMappedOpGroup is imported from opContext/index.js

/**
 * Check if a field name is a system field (original or prefixed).
 *
 * System fields can appear as:
 * - Original names: 'message', 'line', 'error_code', etc.
 * - Prefixed names: 'http_message', 'db_line', 'auth_error_code', etc.
 *
 * @param fieldName - The field name to check
 * @returns True if the field is a system field
 */
function isSystemField(fieldName: string): boolean {
  // Check direct match first
  if (SYSTEM_SCHEMA_FIELD_NAMES.has(fieldName)) {
    return true;
  }
  // Check if it's a prefixed system field (e.g., 'http_message' -> 'message')
  const underscoreIdx = fieldName.indexOf('_');
  if (underscoreIdx !== -1) {
    const suffix = fieldName.slice(underscoreIdx + 1);
    if (SYSTEM_SCHEMA_FIELD_NAMES.has(suffix)) {
      return true;
    }
  }
  return false;
}

/**
 * Filter out system schema fields from a schema.
 *
 * System fields (message, line, error_code, etc.) are merged into every logSchema
 * by defineOpContext, but they should NOT be contributed when a dep is wired.
 * Each app gets its own system fields via mergeWithSystemSchema.
 *
 * This function handles both original and prefixed system field names.
 *
 * @param fields - Schema fields to filter
 * @returns Schema fields without system fields
 */
function filterOutSystemFields(fields: SchemaFields): SchemaFields {
  const result: SchemaFields = {};
  for (const key of Object.keys(fields)) {
    if (!isSystemField(key)) {
      result[key] = fields[key];
    }
  }
  return result;
}

function isInternalOpGroup(dep: AnyOpGroupPublic): dep is AnyOpGroupPublic & AnyOpGroup {
  return '_logSchema' in dep;
}

/**
 * Get the schema fields contributed by a dep (OpGroup or MappedOpGroup).
 *
 * - For OpGroup: returns original schema fields (no transformation), excluding system fields
 * - For MappedOpGroup: returns contributedSchema (after prefix/mapping)
 *
 * System fields (message, line, error_code, etc.) are always filtered out because
 * each app gets its own system fields via mergeWithSystemSchema - deps should NOT
 * contribute system fields.
 *
 * The returned object maps field names to their schema definitions.
 *
 * @param dep - An OpGroup or MappedOpGroup (with internal properties _logSchema, _contributedSchema)
 * @returns The schema fields this dep contributes (user fields only, no system fields)
 */
function getContributedSchemaFromDep(dep: AnyOpGroupPublic): SchemaFields {
  if (!isInternalOpGroup(dep)) {
    throw new Error('Invalid op dependency: expected an OpGroup created by defineOps()');
  }

  if (isMappedOpGroup(dep)) {
    // MappedOpGroup has _contributedSchema with mapped field names
    // This already excludes null-mapped columns (they were dropped in buildContributedSchema)
    // Filter out system fields in case the original schema included them
    return filterOutSystemFields(dep._contributedSchema);
  }
  // OpGroup contributes its original schema fields, excluding system fields
  return filterOutSystemFields(dep._logSchema.fields);
}

/**
 * Compute the effective schema by merging app schema with all dep contributions.
 *
 * The effective schema is the union of:
 * 1. App's own schema fields
 * 2. All dep contributed fields (after prefix/mapping)
 *
 * Throws if multiple deps contribute the same field name (conflict detection).
 *
 * @param appFields - The app's own schema fields
 * @param deps - The deps config (name -> OpGroup | MappedOpGroup)
 * @returns Combined schema fields for the effective schema
 * @throws Error if deps contribute conflicting field names
 */
function computeEffectiveSchema<T extends SchemaFields>(appFields: T): T;
function computeEffectiveSchema<T extends SchemaFields, Deps extends DepsConfig>(
  appFields: T,
  deps: Deps,
): EffectiveSchema<T, Deps>;
function computeEffectiveSchema(appFields: SchemaFields, deps?: DepsConfig): SchemaFields {
  if (!deps || Object.keys(deps).length === 0) {
    return appFields;
  }

  // Start with app fields
  const effective: SchemaFields = { ...appFields };

  // Track which dep contributed each field for conflict detection
  const fieldSource: Record<string, string> = {};
  for (const fieldName of Object.keys(appFields)) {
    fieldSource[fieldName] = 'app';
  }

  // Add each dep's contributed fields
  for (const [depName, dep] of Object.entries(deps)) {
    // Cast to AnyOpGroup to access internal properties (_logSchema, _contributedSchema)
    // These exist at runtime but are hidden from public TypeScript interfaces
    const contributed = getContributedSchemaFromDep(dep);

    for (const [fieldName, fieldDef] of Object.entries(contributed)) {
      // Check for conflict
      if (fieldName in effective) {
        const existingSource = fieldSource[fieldName];
        throw new Error(
          `Schema conflict: field '${fieldName}' contributed by dep '${depName}' ` +
            `conflicts with field from '${existingSource}'. ` +
            'Use .prefix() or .mapColumns() to resolve the conflict.',
        );
      }

      effective[fieldName] = fieldDef;
      fieldSource[fieldName] = depName;
    }
  }

  return effective;
}

function buildOpContextFactory<
  EffectiveFields extends SchemaFields,
  FF extends FeatureFlagSchema,
  Deps extends DepsConfig,
  UserCtx extends Record<string, unknown>,
>(
  effectiveFields: EffectiveFields,
  flags: FF,
  deps: Deps,
  ctxDefaults: UserCtx,
): OpContextFactory<LogSchema<EffectiveFields>, FF, Deps, UserCtx> {
  // Merge effective schema with system schema (message, line, error_code, etc.)
  // System columns are always available on SpanBuffer regardless of user schema.
  // Type parameter stays on the effective user/dependency schema because callers should infer
  // only the public columns, while the runtime object still carries the merged system fields.
  const mergedSchema = new LogSchema<EffectiveFields>(mergeWithSystemSchema(effectiveFields));

  // Create a LogBinding for ops to use.
  // NOTE: Stats are NO LONGER on LogBinding - they are on SpanBufferClass.stats (static property).
  // This avoids sync bugs from having two stat objects and reduces per-instance memory.
  // See agent-todo/opgroup-refactor.md lines 58-70, 525-547 for rationale.
  const logBinding = {
    logSchema: mergedSchema,
    remappedViewClass: undefined,
  };

  // Create the defineOp and defineOps functions bound to this config.
  // CRITICAL: Use mergedSchema (with system columns), not just the app schema, because
  // runtime SpanBuffer generation needs the full row shape even though public factory.logSchema
  // intentionally exposes only user/dependency columns.
  const defineOp = createDefineOp<OpContext<typeof mergedSchema, FF, Deps, UserCtx>>({
    logSchema: mergedSchema,
    flags,
  });

  const defineOps = createDefineOps<OpContext<typeof mergedSchema, FF, Deps, UserCtx>>(
    { logSchema: mergedSchema, flags },
    createOpGroup,
  );

  // Create effective schema LogSchema instance for factory.logSchema.
  // This contains app fields + all dep contributed fields (user fields only, no system).
  const effectiveLogSchema = new LogSchema(effectiveFields);

  return {
    logSchema: effectiveLogSchema,
    flags,
    logBinding,
    ctxDefaults,
    deps,
    defineOp,
    defineOps,
  };
}

/**
 * Define an Op context factory
 *
 * Creates a factory for defining Ops that share the same schema, deps, and context.
 * This is the main entry point for the Op-centric API.
 *
 * @param config - Context configuration
 * @returns OpContextFactory with defineOp, defineOps
 *
 * @example
 * ```typescript
 * import { defineOpContext } from '@smoothbricks/lmao';
 *
 * const { defineOp, defineOps } = defineOpContext({
 *   logSchema: myLogSchema,
 *   deps: {
 *     http: httpOps.prefix('http'),
 *     auth: authOps.prefix('auth'),
 *   },
 *   flags: myFeatureFlags,
 *   ctx: {
 *     env: null as CFWorkerEnv,     // Must provide at createTrace()
 *     config: { retryCount: 3 },     // Has default
 *   },
 * });
 *
 * // Define ops
 * export const myHandler = defineOp('handler', async (ctx, req: Request) => {
 *   ctx.tag.method(req.method);
 *   return ctx.ok({ status: 'ok' });
 * });
 *
 * // Or batch define and export as OpGroup
 * export const myOps = defineOps({
 *   myHandler,
 *   healthCheck: async (ctx) => ctx.ok({ healthy: true }),
 * });
 * ```
 */
export function defineOpContext<T extends SchemaFields>(
  config: OpContextConfig<T, Record<string, never>, Record<string, never>, Record<string, never>>,
): OpContextFactory<LogSchema<T>, Record<string, never>, Record<string, never>, Record<string, never>>;
export function defineOpContext<T extends SchemaFields, const FF extends FeatureFlagSchema>(
  config: OpContextConfig<T, FF, Record<string, never>, Record<string, never>> & { flags: FF },
): OpContextFactory<LogSchema<T>, FF, Record<string, never>, Record<string, never>>;
export function defineOpContext<T extends SchemaFields, UserCtx extends Record<string, unknown>>(
  config: OpContextConfig<T, Record<string, never>, Record<string, never>, UserCtx> & {
    ctx: ValidateUserContext<UserCtx>;
  },
): OpContextFactory<LogSchema<T>, Record<string, never>, Record<string, never>, UserCtx>;
export function defineOpContext<
  T extends SchemaFields,
  const FF extends FeatureFlagSchema,
  UserCtx extends Record<string, unknown>,
>(
  config: OpContextConfig<T, FF, Record<string, never>, UserCtx> & { flags: FF; ctx: ValidateUserContext<UserCtx> },
): OpContextFactory<LogSchema<T>, FF, Record<string, never>, UserCtx>;
export function defineOpContext<T extends SchemaFields, Deps extends DepsConfig>(
  config: OpContextConfig<T, Record<string, never>, Deps, Record<string, never>> & { deps: Deps },
): OpContextFactory<LogSchema<EffectiveSchema<T, Deps>>, Record<string, never>, Deps, Record<string, never>>;
export function defineOpContext<T extends SchemaFields, const FF extends FeatureFlagSchema, Deps extends DepsConfig>(
  config: OpContextConfig<T, FF, Deps, Record<string, never>> & { flags: FF; deps: Deps },
): OpContextFactory<LogSchema<EffectiveSchema<T, Deps>>, FF, Deps, Record<string, never>>;
export function defineOpContext<
  T extends SchemaFields,
  Deps extends DepsConfig,
  UserCtx extends Record<string, unknown>,
>(
  config: OpContextConfig<T, Record<string, never>, Deps, UserCtx> & { deps: Deps; ctx: ValidateUserContext<UserCtx> },
): OpContextFactory<LogSchema<EffectiveSchema<T, Deps>>, Record<string, never>, Deps, UserCtx>;
export function defineOpContext<
  T extends SchemaFields,
  const FF extends FeatureFlagSchema,
  Deps extends DepsConfig,
  UserCtx extends Record<string, unknown>,
>(
  config: OpContextConfig<T, FF, Deps, UserCtx> & { flags: FF; deps: Deps; ctx: ValidateUserContext<UserCtx> },
): OpContextFactory<LogSchema<EffectiveSchema<T, Deps>>, FF, Deps, UserCtx>;
export function defineOpContext<
  T extends SchemaFields,
  const FF extends FeatureFlagSchema = Record<string, never>,
  Deps extends DepsConfig = Record<string, never>,
  UserCtx extends Record<string, unknown> = Record<string, never>,
>(config: OpContextConfig<T, FF, Deps, UserCtx>) {
  // Runtime validation: check for reserved property names in user context
  if (config.ctx) {
    const userKeys = Object.keys(config.ctx);
    for (const key of userKeys) {
      if (RESERVED_CONTEXT_PROPS.some((reservedProp) => reservedProp === key)) {
        throw new Error(
          `Cannot use '${key}' in ctx - it is a reserved SpanContext property. ` +
            `Reserved: ${RESERVED_CONTEXT_PROPS.join(', ')}`,
        );
      }
      // Check for underscore prefix (reserved for internal use)
      if (key.startsWith('_')) {
        throw new Error(`Cannot use '${key}' in ctx - properties starting with '_' are reserved for internal use.`);
      }
    }
  }

  // Compute effective schema by combining app schema with dep contributions.
  // This merges app fields + all dep contributed fields (after prefix/mapping).
  if (config.deps === undefined) {
    const effectiveFields = computeEffectiveSchema(config.logSchema.fields);

    if (config.flags === undefined) {
      if (config.ctx === undefined) {
        return buildOpContextFactory(effectiveFields, EMPTY_FLAGS, EMPTY_DEPS, EMPTY_USER_CTX);
      }

      return buildOpContextFactory(effectiveFields, EMPTY_FLAGS, EMPTY_DEPS, config.ctx);
    }

    if (config.ctx === undefined) {
      return buildOpContextFactory(effectiveFields, config.flags, EMPTY_DEPS, EMPTY_USER_CTX);
    }

    return buildOpContextFactory(effectiveFields, config.flags, EMPTY_DEPS, config.ctx);
  }

  const effectiveFields = computeEffectiveSchema(config.logSchema.fields, config.deps);

  if (config.flags === undefined) {
    if (config.ctx === undefined) {
      return buildOpContextFactory(effectiveFields, EMPTY_FLAGS, config.deps, EMPTY_USER_CTX);
    }

    return buildOpContextFactory(effectiveFields, EMPTY_FLAGS, config.deps, config.ctx);
  }

  if (config.ctx === undefined) {
    return buildOpContextFactory(effectiveFields, config.flags, config.deps, EMPTY_USER_CTX);
  }

  return buildOpContextFactory(effectiveFields, config.flags, config.deps, config.ctx);
}

// Re-export everything from opContext for convenience
export * from './opContext/index.js';

// Re-export schema utilities
export { S } from './schema/builder.js';
export { defineFeatureFlags } from './schema/defineFeatureFlags.js';
export { defineLogSchema } from './schema/defineLogSchema.js';
export { LogSchema } from './schema/LogSchema.js';
