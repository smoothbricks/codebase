import * as S from '@sury/sury';
import { createEvaluatorClass } from '../codegen/evaluatorGenerator.js';
import type { OpContext } from '../opContext/types.js';
import type { SpanContext } from '../spanContext.js';
import type { FeatureFlagSchema } from './defineFeatureFlags.js';
import { ENTRY_TYPE_FF_ACCESS } from './systemSchema.js';

/**
 * Type guard to validate flag value matches schema
 */
function validateFlagValue<T>(value: unknown, schema: S.Schema<T, unknown>, defaultValue: T): T {
  try {
    return S.parseOrThrow(value, schema);
  } catch {
    return defaultValue;
  }
}

/**
 * Serializable flag value types
 */
export type FlagValue = string | number | boolean | null;

/**
 * SpanContext without 'ff' field - prevents infinite recursion in evaluators
 */
export type SpanContextWithoutFf<Ctx extends OpContext> = Omit<SpanContext<Ctx>, 'ff'>;

/**
 * Feature flag evaluator interface (backend implementation)
 *
 * This is implemented by the backend (database, LaunchDarkly, etc.)
 * and provides the actual flag values.
 *
 * ## Stateless Design - ctx is a parameter
 *
 * The evaluator receives `Omit<SpanContext, 'ff'>` as a PARAMETER (not stored state).
 * This allows the evaluator to:
 * - Be stateless (CAN be a singleton, but doesn't have to be)
 * - Access full context (ctx.log, ctx.span, ctx.scope, ctx.deps, user properties)
 * - Prevent infinite recursion (can't access ctx.ff which would call evaluator again)
 *
 * ## forContext() - Flexible Wrapper Creation
 *
 * The evaluator decides how to create the per-span wrapper:
 * - Return a new wrapper with `this` as the evaluator (most common - reuse evaluator)
 * - Return `this` if the evaluator itself implements the wrapper interface
 * - Return a completely different evaluator/wrapper (e.g., per-span caching)
 *
 * The wrapper (FeatureFlagEvaluator) holds a ctx reference to enable fluent getters.
 *
 * @template Ctx - OpContext bundle (contains flags via Ctx['flags'])
 */
export interface FlagEvaluator<Ctx extends OpContext = OpContext> {
  /**
   * Get a flag value synchronously (for cached/static flags)
   *
   * The evaluator receives ctx as a parameter (stateless design).
   * Can use ctx.log/ctx.span for tracing, ctx.scope for targeting.
   *
   * @param ctx - SpanContext without 'ff' (can log, create spans, access scope)
   * @param flag - Flag name to evaluate
   */
  getSync<K extends string>(ctx: SpanContextWithoutFf<Ctx>, flag: K): FlagValue;

  /**
   * Get a flag value asynchronously (for dynamic flags)
   *
   * The evaluator receives ctx as a parameter (stateless design).
   * Can use ctx.log/ctx.span for tracing external calls.
   *
   * @param ctx - SpanContext without 'ff' (can log, create spans, access scope)
   * @param flag - Flag name to evaluate
   */
  getAsync<K extends string>(ctx: SpanContextWithoutFf<Ctx>, flag: K): Promise<FlagValue>;

  /**
   * Factory method: Create per-span wrapper from span context.
   *
   * Returns a FeatureFlagEvaluator (per-span wrapper) with typed getters for each flag.
   *
   * ## Implementation Choices
   *
   * Common pattern: Return a new wrapper that holds `this` as the evaluator
   * ```typescript
   * forContext(ctx) {
   *   return new FeatureFlagEvaluator(schema, ctx, this); // Reuse evaluator, new wrapper
   * }
   * ```
   *
   * Alternative: Return `this` if evaluator implements the wrapper interface
   * ```typescript
   * forContext(ctx) {
   *   return this; // Evaluator itself is the wrapper (uncommon)
   * }
   * ```
   *
   * Alternative: Return different evaluator (e.g., per-span caching layer)
   * ```typescript
   * forContext(ctx) {
   *   return new CachedEvaluator(ctx, this); // Different wrapper with caching
   * }
   * ```
   *
   * The wrapper MUST store ctx because JS getters (`get darkMode()`) cannot receive parameters.
   *
   * Receives `Omit<SpanContext, 'ff'>` to prevent infinite recursion.
   */
  forContext(ctx: SpanContextWithoutFf<Ctx>): FeatureFlagEvaluator<Ctx> & InferFeatureFlagsWithContext<Ctx>;
}

/**
 * Column writers interface (to be implemented in buffer phase)
 *
 * Used for logging flag access and usage to span buffers.
 */
export interface FlagColumnWriters {
  writeEntryType(type: 'ff-access' | 'ff-usage'): void;
  writeFfName(name: string): void;
  writeFfValue(value: FlagValue): void;
  writeAction(action?: string): void;
  writeOutcome(outcome?: string): void;
  writeContextAttributes(context: Record<string, unknown>): void;
}

// ============================================================================
// Flag Context Types - returned when flag is enabled
// ============================================================================

/**
 * Track context for feature flag usage analytics
 */
export interface FlagTrackContext {
  action?: string;
  outcome?: string;
}

/**
 * Boolean flag context - returned when boolean flag is true
 */
export interface BooleanFlagContext {
  readonly value: true;
  track(context?: FlagTrackContext): void;
}

/**
 * Variant flag context - returned when variant flag is enabled
 */
export interface VariantFlagContext<V extends string> {
  readonly value: V;
  track(context?: FlagTrackContext): void;
}

/**
 * Config flag context - returned when config flag is enabled
 * Spreads the config object and adds track() method
 */
export type ConfigFlagContext<T extends Record<string, unknown>> = T & {
  track(context?: FlagTrackContext): void;
};

// ============================================================================
// Type Inference for Feature Flags
// ============================================================================

/**
 * Infer the flag context type for a single flag definition
 */
type InferFlagContextType<T> = T extends boolean
  ? BooleanFlagContext
  : T extends string
    ? VariantFlagContext<T>
    : T extends Record<string, unknown>
      ? ConfigFlagContext<T>
      : { value: T; track(context?: FlagTrackContext): void };

/**
 * Infer feature flag types from OpContext - returns FlagContext | undefined for each flag
 *
 * - Returns undefined when flag is false/disabled
 * - Returns FlagContext wrapper when flag is truthy/enabled
 */
export type InferFeatureFlagsWithContext<Ctx extends OpContext> = Ctx['flags'] extends FeatureFlagSchema
  ? {
      readonly [K in keyof Ctx['flags']]: InferFlagContextType<S.Output<Ctx['flags'][K]['schema']>> | undefined;
    }
  : {};

// ============================================================================
// Feature Flag Evaluator Class
// ============================================================================

/**
 * Cache for generated evaluator classes by schema reference
 * Using WeakMap so schemas can be garbage collected
 */
const evaluatorClassCache = new WeakMap<
  FeatureFlagSchema,
  new (
    spanContext: SpanContext<OpContext>,
    evaluator: FlagEvaluator,
  ) => FeatureFlagEvaluator<OpContext>
>();

/**
 * Get or create a generated evaluator class for a schema
 */
function getOrCreateEvaluatorClass<Ctx extends OpContext>(
  schema: Ctx['flags'],
): new (
  spanContext: SpanContext<Ctx>,
  evaluator: FlagEvaluator<Ctx>,
) => FeatureFlagEvaluator<Ctx> & InferFeatureFlagsWithContext<Ctx> {
  // Check cache first
  let GeneratedClass = evaluatorClassCache.get(schema);

  if (!GeneratedClass) {
    // Generate the class using new Function()
    GeneratedClass = createEvaluatorClass(schema, validateFlagValue, ENTRY_TYPE_FF_ACCESS) as unknown as new (
      spanContext: SpanContext<OpContext>,
      evaluator: FlagEvaluator,
    ) => FeatureFlagEvaluator<OpContext>;

    // Cache the generated class
    evaluatorClassCache.set(schema, GeneratedClass);
  }

  return GeneratedClass as unknown as new (
    spanContext: SpanContext<Ctx>,
    evaluator: FlagEvaluator<Ctx>,
  ) => FeatureFlagEvaluator<Ctx> & InferFeatureFlagsWithContext<Ctx>;
}

/**
 * Feature flag evaluator with undefined/truthy semantics (PER-SPAN WRAPPER)
 *
 * ## Architecture: Stateless Evaluator + Per-Span Wrapper
 *
 * ```
 * ┌─────────────────────────────────────────────────────────────┐
 * │ FlagEvaluator (backend - CAN be singleton)                  │
 * │ - getSync(ctx, flag) → FlagValue                            │
 * │ - getAsync(ctx, flag) → Promise<FlagValue>                  │
 * │ - forContext(ctx) → new FeatureFlagEvaluator(ctx, this)     │
 * └─────────────────────────────────────────────────────────────┘
 *                          ▲
 *                          │ reference (can be reused or new)
 *                          │
 * ┌─────────────────────────────────────────────────────────────┐
 * │ FeatureFlagEvaluator (PER-SPAN - holds ctx reference)       │
 * │ #spanContext = ctx                                          │
 * │ #evaluator = backend evaluator                              │
 * │                                                             │
 * │ get darkMode() {                                            │
 * │   return this.#getFlag('darkMode');                         │
 * │   // calls this.#evaluator.getSync(this.#spanContext, 'x')  │
 * │ }                                                           │
 * │                                                             │
 * │ forContext(childCtx) {                                      │
 * │   // Reuse same evaluator, new wrapper with childCtx        │
 * │   return new FeatureFlagEvaluator(childCtx, this.#evaluator)│
 * │ }                                                           │
 * └─────────────────────────────────────────────────────────────┘
 * ```
 *
 * ## Why Per-Span Wrapper?
 *
 * JS getters (`get darkMode()`) cannot receive parameters. To provide the fluent
 * API `ctx.ff.darkMode`, we need to store the ctx reference in the wrapper instance.
 *
 * The backend evaluator (FlagEvaluator) receives ctx as a parameter, so it CAN be
 * stateless/singleton. The wrapper is created per-span to bind ctx to getters.
 *
 * ## Behavior
 *
 * - Returns undefined when flag is false/disabled
 * - Returns FlagContext wrapper when flag is truthy/enabled
 * - First access logs ff-access entry
 * - Subsequent access in same span returns cached value (no duplicate log)
 * - track() always logs ff-usage entry
 *
 * ## V8 Optimization Benefits
 *
 * - Stable hidden class (all instances have same shape)
 * - Monomorphic property access (getters are real properties)
 * - No Proxy trap overhead
 * - Inline caching works properly
 *
 * ## Generated Class Pattern
 *
 * The class constructor returns a generated class instance rather than this instance.
 * This is a valid JavaScript pattern for creating optimized instances with schema-specific properties.
 *
 * @template Ctx - OpContext bundle (contains flags via Ctx['flags'])
 */
export abstract class FeatureFlagEvaluator<Ctx extends OpContext = OpContext> {
  /**
   * Get async flag value
   * Returns undefined when false, FlagContext when truthy
   */
  abstract get(flag: string): Promise<unknown>;

  /** Track flag usage. Prefer using flag.track() for the fluent API. */
  abstract trackUsage<K extends keyof Ctx['flags']>(flag: K, context?: FlagTrackContext): void;

  /**
   * Create a new wrapper for a child span context.
   *
   * The implementation can reuse the same evaluator (singleton pattern),
   * create a new evaluator, or return `this` if the evaluator itself
   * implements the wrapper interface.
   */
  abstract forContext(ctx: SpanContext<Ctx>): FeatureFlagEvaluator<Ctx> & InferFeatureFlagsWithContext<Ctx>;
}

/**
 * Create a FeatureFlagEvaluator instance using the generated class
 */
export function createFeatureFlagEvaluator<Ctx extends OpContext>(
  schema: Ctx['flags'],
  spanContext: SpanContext<Ctx>,
  evaluator: FlagEvaluator<Ctx>,
): FeatureFlagEvaluator<Ctx> & InferFeatureFlagsWithContext<Ctx> {
  const GeneratedClass = getOrCreateEvaluatorClass<Ctx>(schema);
  return new GeneratedClass(spanContext, evaluator);
}

/**
 * Simple in-memory flag evaluator for testing
 *
 * Demonstrates the simplest evaluator pattern:
 * - getSync/getAsync receive ctx first, enabling evaluators to use ctx.log, ctx.scope
 * - forContext bootstraps the generated FeatureFlagEvaluator class (creates per-span wrapper)
 * - Generated class has typed getters (ff.darkMode) and calls this.getSync(ctx, flag)
 */
export class InMemoryFlagEvaluator<Ctx extends OpContext = OpContext> implements FlagEvaluator<Ctx> {
  private flags: Record<string, FlagValue> = {};
  private ffSchema: Ctx['flags'];

  constructor(ffSchema: Ctx['flags'], initialFlags: Record<string, FlagValue> = {}) {
    this.ffSchema = ffSchema;
    this.flags = initialFlags;
  }

  /**
   * Get flag value synchronously.
   * Simple evaluator ignores ctx - just returns stored value.
   * Advanced evaluators can use ctx.log, ctx.span, ctx.scope, etc.
   */
  getSync<K extends string>(_ctx: SpanContextWithoutFf<Ctx>, flag: K): FlagValue {
    return this.flags[flag] ?? null;
  }

  /**
   * Get flag value asynchronously.
   * Simple evaluator ignores ctx - just returns stored value.
   * Advanced evaluators can use ctx.log, ctx.span for tracing external calls.
   */
  async getAsync<K extends string>(_ctx: SpanContextWithoutFf<Ctx>, flag: K): Promise<FlagValue> {
    return this.flags[flag] ?? null;
  }

  setFlag(flag: string, value: FlagValue): void {
    this.flags[flag] = value;
  }

  /**
   * Create span-bound FeatureFlagEvaluator with typed getters.
   *
   * Returns a per-span wrapper that holds ctx reference and reuses this evaluator.
   * The generated class has typed getters (ff.darkMode) and calls this.getSync(ctx, flag).
   */
  forContext(ctx: SpanContextWithoutFf<Ctx>): FeatureFlagEvaluator<Ctx> & InferFeatureFlagsWithContext<Ctx> {
    return createFeatureFlagEvaluator(this.ffSchema, ctx as SpanContext<Ctx>, this);
  }
}
