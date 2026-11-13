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
  type ComposedField,
  type ComposedPart,
  type CompositionRefusal,
  composeColumns,
  defineFields,
  storageLaneOf,
  typeText,
} from '@smoothbricks/arrow-builder';
import {
  type AnyOpGroup,
  type AnyOpGroupPublic,
  createDefineOp,
  createDefineOps,
  createOpGroup,
  type DepsConfig,
  type EffectiveSchema,
  type FeatureFlagSchema,
  type OpContext,
  type OpContextConfig,
  type OpContextFactory,
  RESERVED_CONTEXT_PROPS,
  type ValidateUserContext,
} from './opContext/index.js';
import { assertTagFieldNames } from './opContext/tagFieldNames.js';
import { LogSchema } from './schema/LogSchema.js';
import { mergeWithSystemSchema } from './schema/systemSchema.js';
import type { SchemaFields } from './schema/types.js';

const EMPTY_FLAGS = Object.freeze({}) satisfies Record<string, never>;
const EMPTY_DEPS = Object.freeze({}) satisfies Record<string, never>;
const EMPTY_USER_CTX = Object.freeze({}) satisfies Record<string, never>;

// =============================================================================
// EFFECTIVE SCHEMA COMPUTATION
// =============================================================================

function isInternalOpGroup(dep: AnyOpGroupPublic): dep is AnyOpGroupPublic & AnyOpGroup {
  return '_logSchema' in dep && '_columns' in dep;
}

/** Who brought a field into the composition, in the words of the wiring: the app, or a dep by its key. */
function contributor(field: ComposedField, depNames: readonly string[]): string {
  return field.part === 'composer' ? 'the app' : `dep '${depNames[field.part]}'`;
}

function describeConflict(refusal: CompositionRefusal, depNames: readonly string[]): string {
  const [a, b] = refusal.fields;
  const who = (field: ComposedField) => `field '${field.name}' of ${contributor(field, depNames)}`;
  if (refusal.code === 'two-types-one-column') {
    return (
      `Schema conflict: column '${refusal.column}' would hold two types: ` +
      `${who(a)} is ${refusal.types[0]}, ${who(b)} is ${refusal.types[1]}. ` +
      'A shared column needs equal field types; use .prefix() or .mapColumns() to give each its own column.'
    );
  }
  return (
    `Schema conflict: column '${refusal.column}' is reached by ${who(a)} and ${who(b)}, and not both were mapped there. ` +
    'Use .prefix() to keep them apart, or .mapColumns() on each dep to share the column.'
  );
}

/**
 * Compute the effective schema: the columns of the app's own fields and of every dep's fields under the names the
 * wiring gave them (01e "Column Mapping API").
 *
 * The composition is arrow-builder's: the app is the composer, each dep one part. Two fields share a column only
 * when each was mapped there by `.mapColumns()` (the app's own fields count as mapped) and their types are equal
 * (01e "Shared Columns Across Libraries").
 *
 * @param appSchema - The app's own log schema
 * @param deps - The deps config (name -> OpGroup | MappedOpGroup)
 * @returns Combined schema fields for the effective schema
 * @throws Error naming the column, both fields, who contributed them and, for a type conflict, both types
 */
function computeEffectiveSchema<T extends SchemaFields>(appSchema: LogSchema<T>): T;
function computeEffectiveSchema<T extends SchemaFields, Deps extends DepsConfig>(
  appSchema: LogSchema<T>,
  deps: Deps,
): EffectiveSchema<T, Deps>;
function computeEffectiveSchema(appSchema: LogSchema, deps?: DepsConfig): SchemaFields {
  const appFields = appSchema.fields;
  if (!deps || Object.keys(deps).length === 0) {
    return appFields;
  }

  const depNames = Object.keys(deps);
  const parts: ComposedPart[] = [];
  for (const depName of depNames) {
    const dep = deps[depName];
    if (!isInternalOpGroup(dep)) {
      throw new Error(`Invalid op dependency '${depName}': expected an OpGroup created by defineOps()`);
    }
    parts.push(dep._columns);
  }

  const composed = composeColumns(parts, { composer: defineFields(appSchema.library, appFields) });
  if (!composed.ok) {
    throw new Error(composed.error.map((refusal) => describeConflict(refusal, depNames)).join('\n'));
  }

  // App fields first, then each dep's columns in wiring order. A shared column is one field: the first that
  // names it; every later one has the same type.
  const effective: SchemaFields = { ...appFields };
  for (const part of parts) {
    const schema = 'columns' in part ? part.schema : part;
    for (const [name, field] of Object.entries(schema.fields)) {
      const column = 'columns' in part ? part.columns[name] : name;
      if (column !== null && column !== undefined && !(column in effective)) effective[column] = field;
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
  library: string,
  effectiveFields: EffectiveFields,
  flags: FF,
  deps: Deps,
  ctxDefaults: UserCtx,
): OpContextFactory<LogSchema<EffectiveFields>, FF, Deps, UserCtx> {
  // Merge effective schema with system schema (message, line, error_code, etc.)
  // System columns are always available on SpanBuffer regardless of user schema.
  // Type parameter stays on the effective user/dependency schema because callers should infer
  // only the public columns, while the runtime object still carries the merged system fields.
  const mergedSchema = new LogSchema<EffectiveFields>(mergeWithSystemSchema(effectiveFields), library);

  // Create a LogBinding for ops to use.
  // NOTE: Stats are NO LONGER on LogBinding - they are on SpanBufferClass.stats (static property).
  // This avoids sync bugs from having two stat objects and reduces per-instance memory.
  // See agent-todo/opgroup-refactor.md lines 58-70, 525-547 for rationale.
  const logBinding = {
    logSchema: mergedSchema,
    remapDescriptor: undefined,
  };

  // WHY: build the binding first so Ops created by defineOp carry it via _opContextBinding.
  // This enables downstream consumers (defineAgentOps, OpRegistry) to extract the LMAO schema.
  const opContextBinding = { logBinding, flags, ctxDefaults, deps };

  // Create the defineOp and defineOps functions bound to this config.
  // CRITICAL: Use mergedSchema (with system columns), not just the app schema, because
  // runtime SpanBuffer generation needs the full row shape even though public factory.logSchema
  // intentionally exposes only user/dependency columns.
  const defineOp = createDefineOp<OpContext<typeof mergedSchema, FF, Deps, UserCtx>>({
    logSchema: mergedSchema,
    flags,
    opContextBinding,
  });

  const defineOps = createDefineOps<OpContext<typeof mergedSchema, FF, Deps, UserCtx>>(
    { logSchema: mergedSchema, flags, opContextBinding },
    createOpGroup,
  );

  // Create effective schema LogSchema instance for factory.logSchema.
  // This contains app fields + all dep contributed fields (user fields only, no system).
  const effectiveLogSchema = new LogSchema(effectiveFields, library);

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

//#region smoo/lmao!n/opcontext-definition
/**
 * Define an Op context factory
 *
 * 01j "Module Definition", renamed: this is the shipped entry point (defineOpContext,
 * not defineModule().ctx().make()). Returns { defineOp, defineOps }; user context is the
 * `ctx` config field (runtime defaults, reserved-key-guarded — see the nested
 * n/schema-env-config region), deps carry their own prefix, and per-Op attribution is OpMetadata.
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
  //#region smoo/lmao!n/schema-env-config
  // Runtime validation: check for reserved property names in user context.
  // The `ctx` config object is the env/deployment-config surface from
  // 01a "Environment Variable Configuration" — a plain user object (env, services,
  // requestId, ...) spread into SpanContext, guarded only against reserved/internal keys.
  //#region smoo/lmao!n/schema-env-config.reserved
  // 01l "Reserved Names": ctx keys cannot collide with built-in SpanContext members
  // (buffer/tag/log/scope/setScope/ok/err/span/ff/deps) or start with `_` (internal).
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
  //#endregion smoo/lmao!n/schema-env-config.reserved
  //#endregion smoo/lmao!n/schema-env-config

  // 01a "Log Schema Definition": the log schema's fields become setters of this context's tag writer, log entries
  // and results, so a name that would shadow one of their members is refused here, where they are built.
  assertTagFieldNames(config.logSchema._columnNames);

  // 01a "Fields and kinds": a span buffer stores the lanes it has; a field of any other kind is refused here, by
  // name, before a buffer class is built for it.
  for (const [name, field] of config.logSchema._columns) {
    if (storageLaneOf(field) === undefined) {
      throw new Error(
        `Field '${name}' is ${typeText(field)}: span buffers store enum, category, text, f64, boolean, u64 and binary-column kinds`,
      );
    }
  }

  // Compute effective schema by combining app schema with dep contributions.
  // This merges app fields + all dep contributed fields (after prefix/mapping).
  if (config.deps === undefined) {
    const effectiveFields = computeEffectiveSchema(config.logSchema);

    if (config.flags === undefined) {
      if (config.ctx === undefined) {
        return buildOpContextFactory(
          config.logSchema.library,
          effectiveFields,
          EMPTY_FLAGS,
          EMPTY_DEPS,
          EMPTY_USER_CTX,
        );
      }

      return buildOpContextFactory(config.logSchema.library, effectiveFields, EMPTY_FLAGS, EMPTY_DEPS, config.ctx);
    }

    if (config.ctx === undefined) {
      return buildOpContextFactory(config.logSchema.library, effectiveFields, config.flags, EMPTY_DEPS, EMPTY_USER_CTX);
    }

    return buildOpContextFactory(config.logSchema.library, effectiveFields, config.flags, EMPTY_DEPS, config.ctx);
  }

  const effectiveFields = computeEffectiveSchema(config.logSchema, config.deps);

  if (config.flags === undefined) {
    if (config.ctx === undefined) {
      return buildOpContextFactory(config.logSchema.library, effectiveFields, EMPTY_FLAGS, config.deps, EMPTY_USER_CTX);
    }

    return buildOpContextFactory(config.logSchema.library, effectiveFields, EMPTY_FLAGS, config.deps, config.ctx);
  }

  if (config.ctx === undefined) {
    return buildOpContextFactory(config.logSchema.library, effectiveFields, config.flags, config.deps, EMPTY_USER_CTX);
  }

  return buildOpContextFactory(config.logSchema.library, effectiveFields, config.flags, config.deps, config.ctx);
}
//#endregion smoo/lmao!n/opcontext-definition

// Re-export everything from opContext for convenience
export * from './opContext/index.js';

// Re-export schema utilities
export { S } from './schema/builder.js';
export { defineFeatureFlags } from './schema/defineFeatureFlags.js';
export { defineLogSchema } from './schema/defineLogSchema.js';
export { LogSchema } from './schema/LogSchema.js';
