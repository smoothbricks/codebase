/**
 * Trace Creation for Op-Centric API
 *
 * Creates root trace contexts with proper validation and initialization.
 *
 * @module opContext/createTrace
 */

import { createTagWriter } from '../codegen/fixedPositionWriterGenerator.js';
import { createSpanLogger as createSpanLoggerFromGenerator } from '../codegen/spanLoggerGenerator.js';
import type { LogSchema } from '../schema/LogSchema.js';
import type { SchemaFields } from '../schema/types.js';
import { createOverflowBuffer, createSpanBuffer } from '../spanBuffer.js';
import { createSpanContextProto, type MutableSpanContext, writeSpanStart } from '../spanContext.js';
import { generateTraceId, type TraceId } from '../traceId.js';
import type { LogBinding, SpanBuffer } from '../types.js';
import type { NullKeys } from './contextTypes.js';
import { createOpMetadata } from './defineOp.js';
import type { FeatureFlagSchema } from './featureFlagTypes.js';
import type { DepsConfig } from './opGroupTypes.js';
import type { SpanContext } from './spanContextTypes.js';
import type { CreateTraceParams, OpContext, OpContextConfig } from './types.js';

// =============================================================================
// CREATE TRACE IMPLEMENTATION
// =============================================================================
//
// PHASE 6 NOTE: Internal types (SpanContextBase, InternalSpanContext) removed.
// Function now returns SpanContext<OpContext<...>> directly using the bundled type parameters.
// Implementation still uses old structure but type signature is updated.

/**
 * Create a root trace context with validation and initialization.
 *
 * Creates a fully functional SpanContext with:
 * - tag: TagWriter for span attributes
 * - log: SpanLogger for structured logging
 * - span: Child span creation function
 * - ok/err: Result helpers
 * - scope/setScope: Scoped attributes
 *
 * @param factoryConfig - Configuration from defineOpContext
 * @param logBinding - LogBinding containing merged schema and capacity stats
 * @param params - User-provided trace parameters
 * @returns Fully initialized SpanContext
 */
export function createTraceImpl<
  T extends SchemaFields,
  FF extends FeatureFlagSchema,
  Deps extends DepsConfig,
  UserCtx extends Record<string, unknown>,
>(
  factoryConfig: OpContextConfig<T, FF, Deps, UserCtx>,
  logBinding: LogBinding,
  params: CreateTraceParams<UserCtx>,
): SpanContext<OpContext<LogSchema<T>, FF, Deps, UserCtx>> {
  // ============================================================================
  // Step 1: Validate required params (null-sentinel properties)
  // ============================================================================
  const ctxDefaults = (factoryConfig.ctx ?? {}) as UserCtx;

  // Check that all null-sentinel keys are provided in params
  for (const key of Object.keys(ctxDefaults) as (keyof UserCtx)[]) {
    const defaultValue = ctxDefaults[key];
    // If the default value is exactly null, this is a required param
    if (defaultValue === null) {
      const providedValue = (params as Record<string, unknown>)[key as string];
      if (providedValue === null || providedValue === undefined) {
        throw new Error(
          "Required context parameter '" +
            String(key) +
            "' must be provided at createTrace(). " +
            'Properties with null values in ctx config are required.',
        );
      }
    }
  }

  // ============================================================================
  // Step 2: Merge context (defaults + provided params)
  // ============================================================================
  // Start with defaults, override with provided params
  // This creates the resolved user context with proper types
  const resolvedUserCtx: Record<string, unknown> = { ...ctxDefaults };

  // Override with provided params (excluding traceId which is not a user ctx property)
  for (const key of Object.keys(params) as (keyof typeof params)[]) {
    if (key !== 'traceId') {
      const value = params[key];
      if (value !== undefined) {
        resolvedUserCtx[key as string] = value;
      }
    }
  }

  // ============================================================================
  // Step 3: Generate trace ID
  // ============================================================================
  const traceId: TraceId = params.traceId ? (params.traceId as TraceId) : generateTraceId();

  // ============================================================================
  // Step 4: Create SpanBuffer using merged schema from logBinding
  // ============================================================================
  // IMPORTANT: Use logBinding.logSchema (merged schema) not factoryConfig.logSchema (user schema)
  // The merged schema includes system columns like message(), line(), etc.
  const mergedSchema = logBinding.logSchema;
  const spanName = 'root'; // Root span name

  const buffer = createSpanBuffer(mergedSchema, logBinding, spanName, traceId);

  // Initialize scope values to empty frozen object
  buffer._scopeValues = Object.freeze({});

  // Set _opMetadata for Arrow conversion (root spans use runtime metadata)
  buffer._opMetadata = createOpMetadata('root', 'createTrace', 'runtime', 0);

  // Write span-start entry (row 0) and pre-initialize span-end (row 1)
  writeSpanStart(buffer, spanName);

  // ============================================================================
  // Step 5: Create SpanContext using prototype inheritance
  // ============================================================================
  const spanContextProto = createSpanContextProto<OpContext<LogSchema<T>, FF, Deps, UserCtx>>(
    mergedSchema as LogSchema<T>,
    logBinding,
  );

  const ctx = Object.create(spanContextProto) as MutableSpanContext<OpContext<LogSchema<T>, FF, Deps, UserCtx>>;

  // Set instance properties
  ctx._buffer = buffer as SpanBuffer<LogSchema<T>>;
  ctx._schema = mergedSchema as LogSchema<T>;
  ctx._logBinding = logBinding;

  // Create tag writer and span logger
  // biome-ignore lint/suspicious/noExplicitAny: Schema type erasure for runtime codegen
  ctx.tag = createTagWriter(mergedSchema, buffer) as any;
  // biome-ignore lint/suspicious/noExplicitAny: Schema type erasure for runtime codegen
  const spanLogger = createSpanLoggerFromGenerator(mergedSchema, buffer, createOverflowBuffer);
  ctx.log = spanLogger as any;
  ctx._spanLogger = spanLogger as any;

  // Spread user context properties onto context
  for (const [key, value] of Object.entries(resolvedUserCtx)) {
    (ctx as Record<string, unknown>)[key] = value;
  }

  // Set up deps (empty for root created via createTrace)
  ctx.deps = {};

  // Set up feature flags if evaluator provided
  const flagEvaluator = factoryConfig.flagEvaluator;
  if (flagEvaluator) {
    // biome-ignore lint/suspicious/noExplicitAny: Type erasure needed for FlagEvaluator generics
    ctx.ff = (flagEvaluator as any).forContext(ctx);
  } else {
    // No-op ff if no evaluator
    // biome-ignore lint/suspicious/noExplicitAny: Type erasure for empty ff
    ctx.ff = {} as any;
  }

  return ctx as unknown as SpanContext<OpContext<LogSchema<T>, FF, Deps, UserCtx>>;
}

// =============================================================================
// TYPE HELPER FOR CHECKING REQUIRED KEYS
// =============================================================================

/**
 * Check if a key is a null-sentinel (required) key in the context config.
 * Used by validation logic.
 */
export function isRequiredContextKey<UserCtx extends Record<string, unknown>>(
  ctx: UserCtx,
  key: keyof UserCtx,
): key is NullKeys<UserCtx> {
  return ctx[key] === null;
}
