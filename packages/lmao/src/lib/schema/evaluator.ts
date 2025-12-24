import * as S from '@sury/sury';
import { createEvaluatorClass } from '../codegen/evaluatorGenerator.js';
import type { SpanContext } from '../spanContext.js';
import type { FeatureFlagSchema } from './defineFeatureFlags.js';
import { ENTRY_TYPE_FF_ACCESS } from './systemSchema.js';
import type { LogSchema } from './types.js';

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
export type SpanContextWithoutFf<T extends LogSchema, FF extends FeatureFlagSchema, Env> = Omit<
  SpanContext<T, FF, Env>,
  'ff'
>;

/**
 * Feature flag evaluator interface
 *
 * This is implemented by the backend (database, LaunchDarkly, etc.)
 * and provides the actual flag values.
 *
 * The evaluator receives `Omit<SpanContext, 'ff'>` which provides:
 * - Full context access (ctx.log, ctx.span, ctx.scope, ctx.env, ctx.deps)
 * - Prevents infinite recursion (can't access ctx.ff which would call evaluator again)
 *
 * @template T - LogSchema defining the evaluation context (scope values)
 * @template FF - FeatureFlagSchema defining available flags
 * @template Env - Environment type from module
 */
export interface FlagEvaluator<
  T extends LogSchema = LogSchema,
  FF extends FeatureFlagSchema = FeatureFlagSchema,
  Env = Record<string, unknown>,
> {
  /**
   * Get a flag value synchronously (for cached/static flags)
   *
   * @param ctx - SpanContext without 'ff' (can log, create spans, access scope)
   * @param flag - Flag name to evaluate
   */
  getSync<K extends string>(ctx: SpanContextWithoutFf<T, FF, Env>, flag: K): FlagValue;

  /**
   * Get a flag value asynchronously (for dynamic flags)
   *
   * @param ctx - SpanContext without 'ff' (can log, create spans, access scope)
   * @param flag - Flag name to evaluate
   */
  getAsync<K extends string>(ctx: SpanContextWithoutFf<T, FF, Env>, flag: K): Promise<FlagValue>;

  /**
   * Create span-bound accessor from span context.
   * Returns a FeatureFlagEvaluator with typed getters for each flag.
   *
   * MUST create a new instance per span because JS getters (`get darkMode()`)
   * cannot receive parameters - the span context must be stored at construction.
   *
   * Receives `Omit<SpanContext, 'ff'>` to prevent infinite recursion.
   */
  forContext(
    ctx: SpanContextWithoutFf<T, FF, Env>,
  ): FeatureFlagEvaluator<FF, T, Env> & InferFeatureFlagsWithContext<FF>;
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
 * Infer feature flag types - returns FlagContext | undefined for each flag
 *
 * - Returns undefined when flag is false/disabled
 * - Returns FlagContext wrapper when flag is truthy/enabled
 */
export type InferFeatureFlagsWithContext<T extends FeatureFlagSchema> = {
  readonly [K in keyof T]: InferFlagContextType<S.Output<T[K]['schema']>> | undefined;
};

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
    spanContext: SpanContext<LogSchema, FeatureFlagSchema, unknown>,
    evaluator: FlagEvaluator<LogSchema, FeatureFlagSchema, unknown>,
  ) => FeatureFlagEvaluator<FeatureFlagSchema>
>();

/**
 * Get or create a generated evaluator class for a schema
 */
function getOrCreateEvaluatorClass<FF extends FeatureFlagSchema, T extends LogSchema = LogSchema, Env = unknown>(
  schema: FF,
): new (
  spanContext: SpanContext<T, FF, Env>,
  evaluator: FlagEvaluator<T, FF, Env>,
) => FeatureFlagEvaluator<FF> & InferFeatureFlagsWithContext<FF> {
  // Check cache first
  let GeneratedClass = evaluatorClassCache.get(schema);

  if (!GeneratedClass) {
    // Generate the class using new Function()
    GeneratedClass = createEvaluatorClass(schema, validateFlagValue, ENTRY_TYPE_FF_ACCESS) as unknown as new (
      spanContext: SpanContext<LogSchema, FeatureFlagSchema, unknown>,
      evaluator: FlagEvaluator<LogSchema, FeatureFlagSchema, unknown>,
    ) => FeatureFlagEvaluator<FeatureFlagSchema>;

    // Cache the generated class
    evaluatorClassCache.set(schema, GeneratedClass);
  }

  return GeneratedClass as unknown as new (
    spanContext: SpanContext<T, FF, Env>,
    evaluator: FlagEvaluator<T, FF, Env>,
  ) => FeatureFlagEvaluator<FF> & InferFeatureFlagsWithContext<FF>;
}

/**
 * Feature flag evaluator with undefined/truthy semantics
 *
 * - Returns undefined when flag is false/disabled
 * - Returns FlagContext wrapper when flag is truthy/enabled
 * - First access logs ff-access entry
 * - Subsequent access in same span returns cached value (no duplicate log)
 * - track() always logs ff-usage entry
 *
 * V8 Optimization benefits:
 * - Stable hidden class (all instances have same shape)
 * - Monomorphic property access (getters are real properties)
 * - No Proxy trap overhead
 * - Inline caching works properly
 *
 * Note: The class constructor returns a generated class instance rather than this instance.
 * This is a valid JavaScript pattern for creating optimized instances with schema-specific properties.
 */
export class FeatureFlagEvaluator<FF extends FeatureFlagSchema, T extends LogSchema = LogSchema, Env = unknown> {
  // These properties exist for type compatibility but actual implementation uses generated class
  protected evaluator!: FlagEvaluator<T, FF, Env>;

  constructor(schema: FF, spanContext: SpanContext<T, FF, Env>, evaluator: FlagEvaluator<T, FF, Env>) {
    // Get or create the generated class for this schema
    const GeneratedClass = getOrCreateEvaluatorClass<FF, T, Env>(schema);

    // Return generated class instance instead of this instance
    // This is a valid JavaScript pattern - constructors can return objects
    // biome-ignore lint/correctness/noConstructorReturn: Valid pattern for generated class instances
    return new GeneratedClass(spanContext, evaluator) as unknown as FeatureFlagEvaluator<FF, T, Env>;
  }

  // These methods exist for TypeScript type information only
  // The generated class handles all actual method calls

  /**
   * Get async flag value
   * Returns undefined when false, FlagContext when truthy
   */
  get(_flag: string): Promise<unknown> {
    throw new Error('Should not be called - handled by generated class');
  }

  /** Track flag usage. Prefer using flag.track() for the fluent API. */
  trackUsage<K extends keyof FF>(_flag: K, _context?: FlagTrackContext): void {
    throw new Error('Should not be called - handled by generated class');
  }

  forContext(_ctx: SpanContext<T, FF, Env>): FeatureFlagEvaluator<FF, T, Env> {
    throw new Error('Should not be called - handled by generated class');
  }
}

/**
 * Simple in-memory flag evaluator for testing
 *
 * Demonstrates the simplest evaluator pattern:
 * - getSync/getAsync receive ctx first, enabling evaluators to use ctx.log, ctx.scope
 * - forContext bootstraps the generated FeatureFlagEvaluator class (once per trace)
 * - Generated class has typed getters (ff.darkMode) and its own forContext() for child spans
 */
export class InMemoryFlagEvaluator<
  T extends LogSchema = LogSchema,
  FF extends FeatureFlagSchema = FeatureFlagSchema,
  Env = unknown,
> implements FlagEvaluator<T, FF, Env>
{
  private flags: Record<string, FlagValue> = {};
  private ffSchema: FF;

  constructor(ffSchema: FF, initialFlags: Record<string, FlagValue> = {}) {
    this.ffSchema = ffSchema;
    this.flags = initialFlags;
  }

  /**
   * Get flag value synchronously.
   * Simple evaluator ignores ctx - just returns stored value.
   * Advanced evaluators can use ctx.log, ctx.span, ctx.scope, etc.
   */
  getSync<K extends string>(_ctx: SpanContextWithoutFf<T, FF, Env>, flag: K): FlagValue {
    return this.flags[flag] ?? null;
  }

  /**
   * Get flag value asynchronously.
   * Simple evaluator ignores ctx - just returns stored value.
   * Advanced evaluators can use ctx.log, ctx.span for tracing external calls.
   */
  async getAsync<K extends string>(_ctx: SpanContextWithoutFf<T, FF, Env>, flag: K): Promise<FlagValue> {
    return this.flags[flag] ?? null;
  }

  setFlag(flag: string, value: FlagValue): void {
    this.flags[flag] = value;
  }

  /**
   * Create span-bound FeatureFlagEvaluator with typed getters.
   *
   * Called once at trace creation to bootstrap the generated evaluator class.
   * After that, child spans use the generated class's forContext() method
   * which efficiently creates new instances reusing the same evaluator.
   *
   * The generated class has typed getters (ff.darkMode) and calls
   * this.getSync(ctx, flag) for actual flag lookup.
   */
  forContext(
    ctx: SpanContextWithoutFf<T, FF, Env>,
  ): FeatureFlagEvaluator<FF, T, Env> & InferFeatureFlagsWithContext<FF> {
    // Pass ctx as SpanContext (ff will be set by caller after this returns)
    return new FeatureFlagEvaluator(this.ffSchema, ctx as SpanContext<T, FF, Env>, this) as FeatureFlagEvaluator<
      FF,
      T,
      Env
    > &
      InferFeatureFlagsWithContext<FF>;
  }
}
