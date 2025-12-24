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

import { DEFAULT_BUFFER_CAPACITY } from '@smoothbricks/arrow-builder';
import {
  createDefineOp,
  createDefineOps,
  createOpGroup,
  type DepsConfig,
  type FeatureFlagSchema,
  type OpContext,
  type OpContextConfig,
  type OpContextFactory,
  opContextType,
  RESERVED_CONTEXT_PROPS,
} from './opContext/index.js';
import { LogSchema } from './schema/LogSchema.js';
import { mergeWithSystemSchema } from './schema/systemSchema.js';
import type { SchemaFields } from './schema/types.js';
import type { LogBinding } from './types.js';

// =============================================================================
// EFFECTIVE SCHEMA COMPUTATION
// =============================================================================

// NOTE: getContributedSchemaFromDep and computeEffectiveSchema were removed - not yet used.
// Will be re-added when dependency schema merging feature is implemented.
// See specs/01l_module_builder_pattern.md for details.

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
  Deps extends DepsConfig = {},
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

  // Merge user schema with system schema (message, line, error_code, etc.)
  // System columns are always available on SpanBuffer regardless of user schema
  const mergedFields = mergeWithSystemSchema(config.logSchema.fields);
  const mergedSchema = new LogSchema(mergedFields);

  // Create a LogBinding with initial capacity stats
  // The logBinding serves as the infrastructure for ops to write logs
  // Type is LogBinding<LogSchema<T>> to preserve schema type for Op type safety
  const logBinding: LogBinding<LogSchema<T>> = {
    logSchema: mergedSchema as LogSchema<T>,
    remappedViewClass: undefined, // Will be set if prefixing is applied
    sb_capacity: DEFAULT_BUFFER_CAPACITY, // Initial capacity (self-tuning will grow this)
    sb_totalWrites: 0,
    sb_overflowWrites: 0,
    sb_totalCreated: 0,
    sb_overflows: 0,
  };

  // Create the defineOp and defineOps functions bound to this config
  const defineOp = createDefineOp<OpContext<LogSchema<T>, FF, Deps, UserCtx>>({
    logSchema: config.logSchema,
    flags: config.flags as FF,
    logBinding,
  });

  const defineOps = createDefineOps<OpContext<LogSchema<T>, FF, Deps, UserCtx>>(
    { logSchema: config.logSchema, flags: config.flags as FF, logBinding },
    createOpGroup,
  );

  // Return the factory with phantom type property
  // Note: logSchema needs a cast because the effective schema
  // (which includes deps schemas) requires runtime computation.
  // OpContextFactory expects LogSchema<EffectiveSchema<T, Deps>>,
  // but at this point we only have T. The type is correct at the call site.
  return {
    [opContextType]: undefined as any as OpContext<LogSchema<T>, FF, Deps, UserCtx>,
    logSchema: config.logSchema as unknown as LogSchema<any>,
    flags: config.flags as FF,
    logBinding,
    ctxDefaults: (config.ctx ?? {}) as UserCtx,
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
