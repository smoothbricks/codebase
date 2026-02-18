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
  opContextType,
  RESERVED_CONTEXT_PROPS,
} from './opContext/index.js';
import { LogSchema } from './schema/LogSchema.js';
import { mergeWithSystemSchema, SYSTEM_SCHEMA_FIELD_NAMES } from './schema/systemSchema.js';
import type { SchemaFields } from './schema/types.js';
import { EMPTY_SCOPE } from './spanBuffer.js';
import type { LogBinding } from './types.js';

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
  const result: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(fields)) {
    if (!isSystemField(key)) {
      result[key] = value;
    }
  }
  return result as SchemaFields;
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
function getContributedSchemaFromDep(dep: AnyOpGroup): SchemaFields {
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
function computeEffectiveSchema(appFields: SchemaFields, deps: DepsConfig | undefined): SchemaFields {
  if (!deps || Object.keys(deps).length === 0) {
    return appFields;
  }

  // Start with app fields
  const effective: Record<string, unknown> = { ...appFields };

  // Track which dep contributed each field for conflict detection
  const fieldSource: Record<string, string> = {};
  for (const fieldName of Object.keys(appFields)) {
    fieldSource[fieldName] = 'app';
  }

  // Add each dep's contributed fields
  for (const [depName, dep] of Object.entries(deps)) {
    // Cast to AnyOpGroup to access internal properties (_logSchema, _contributedSchema)
    // These exist at runtime but are hidden from public TypeScript interfaces
    const contributed = getContributedSchemaFromDep(dep as unknown as AnyOpGroup);

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

  return effective as SchemaFields;
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
export function defineOpContext<
  T extends SchemaFields,
  const FF extends FeatureFlagSchema = Record<string, never>,
  Deps extends DepsConfig = Record<string, never>,
  UserCtx extends Record<string, unknown> = Record<string, never>,
>(config: OpContextConfig<T, FF, Deps, UserCtx>): OpContextFactory<LogSchema<T>, FF, Deps, UserCtx> {
  // Runtime validation: check for reserved property names in user context
  if (config.ctx) {
    const userKeys = Object.keys(config.ctx);
    for (const key of userKeys) {
      if ((RESERVED_CONTEXT_PROPS as readonly string[]).includes(key)) {
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

  // Compute effective schema by combining app schema with dep contributions
  // This merges app fields + all dep contributed fields (after prefix/mapping)
  const effectiveFields = computeEffectiveSchema(config.logSchema.fields, config.deps);

  // Merge effective schema with system schema (message, line, error_code, etc.)
  // System columns are always available on SpanBuffer regardless of user schema
  const mergedFields = mergeWithSystemSchema(effectiveFields);
  const mergedSchema = new LogSchema(mergedFields);

  // Create a LogBinding for ops to use.
  // NOTE: Stats are NO LONGER on LogBinding - they are on SpanBufferClass.stats (static property).
  // This avoids sync bugs from having two stat objects and reduces per-instance memory.
  // See agent-todo/opgroup-refactor.md lines 58-70, 525-547 for rationale.
  const logBinding: LogBinding<LogSchema<T>> = {
    logSchema: mergedSchema as unknown as LogSchema<T>,
    remappedViewClass: undefined, // Will be set if prefixing is applied
  };

  // Create the defineOp and defineOps functions bound to this config
  // CRITICAL: Use mergedSchema (with system columns), not config.logSchema
  // Cast via unknown because mergedSchema includes SystemSchemaFieldTypes which
  // doesn't overlap with T directly, but is correct at runtime.
  const defineOp = createDefineOp<OpContext<LogSchema<T>, FF, Deps, UserCtx>>({
    logSchema: mergedSchema as unknown as LogSchema<T>,
    flags: config.flags as FF,
  });

  const defineOps = createDefineOps<OpContext<LogSchema<T>, FF, Deps, UserCtx>>(
    { logSchema: mergedSchema as unknown as LogSchema<T>, flags: config.flags as FF },
    createOpGroup,
  );

  // Create effective schema LogSchema instance for factory.logSchema
  // This contains app fields + all dep contributed fields (user fields only, no system)
  const effectiveLogSchema = new LogSchema(effectiveFields);

  // Return the factory with phantom type property
  // Note: logSchema is the effective schema (app + deps), not just app's schema
  // OpContextFactory expects LogSchema<EffectiveSchema<T, Deps>>,
  // which is computed at runtime above.
  return {
    [opContextType]: undefined as unknown as OpContext<LogSchema<T>, FF, Deps, UserCtx>,
    logSchema: effectiveLogSchema as unknown as LogSchema<EffectiveSchema<T, Deps>>,
    flags: config.flags as FF,
    logBinding,
    ctxDefaults: (config.ctx ?? EMPTY_SCOPE) as UserCtx,
    deps: (config.deps ?? EMPTY_SCOPE) as Deps,
    defineOp,
    defineOps,
  };
}

// Re-export everything from opContext for convenience
export * from './opContext/index.js';

// Re-export schema utilities
export { S } from './schema/builder.js';
export { defineFeatureFlags } from './schema/defineFeatureFlags.js';
export { defineLogSchema } from './schema/defineLogSchema.js';
export { LogSchema } from './schema/LogSchema.js';
