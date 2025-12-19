import * as S from '@sury/sury';
import { createEvaluatorClass, type GeneratedEvaluatorState } from '../codegen/evaluatorGenerator.js';
import { getTimestampNanos } from '../timestamp.js';
import type { SpanBuffer } from '../types.js';
import type { EvaluationContext, FeatureFlagSchema } from './defineFeatureFlags.js';
import { ENTRY_TYPE_FF_ACCESS, ENTRY_TYPE_FF_USAGE } from './systemSchema.js';

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
 * Feature flag evaluator interface
 *
 * This is implemented by the backend (database, LaunchDarkly, etc.)
 * and provides the actual flag values.
 */
export interface FlagEvaluator {
  /**
   * Get a flag value synchronously (for cached/static flags)
   */
  getSync<K extends string>(flag: K, context: EvaluationContext): FlagValue;

  /**
   * Get a flag value asynchronously (for dynamic flags)
   */
  getAsync<K extends string>(flag: K, context: EvaluationContext): Promise<FlagValue>;
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
  writeContextAttributes(context: EvaluationContext): void;
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
    state: GeneratedEvaluatorState<FeatureFlagSchema>,
  ) => FeatureFlagEvaluator<FeatureFlagSchema>
>();

/**
 * Get or create a generated evaluator class for a schema
 */
function getOrCreateEvaluatorClass<T extends FeatureFlagSchema>(
  schema: T,
): new (
  state: GeneratedEvaluatorState<T>,
) => FeatureFlagEvaluator<T> & InferFeatureFlagsWithContext<T> {
  // Check cache first
  let GeneratedClass = evaluatorClassCache.get(schema);

  if (!GeneratedClass) {
    // Generate the class using new Function()
    GeneratedClass = createEvaluatorClass(
      schema,
      validateFlagValue,
      getTimestampNanos,
      ENTRY_TYPE_FF_ACCESS,
      ENTRY_TYPE_FF_USAGE,
    ) as unknown as new (
      state: GeneratedEvaluatorState<FeatureFlagSchema>,
    ) => FeatureFlagEvaluator<FeatureFlagSchema>;

    // Cache the generated class
    evaluatorClassCache.set(schema, GeneratedClass);
  }

  return GeneratedClass as unknown as new (
    state: GeneratedEvaluatorState<T>,
  ) => FeatureFlagEvaluator<T> & InferFeatureFlagsWithContext<T>;
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
export class FeatureFlagEvaluator<T extends FeatureFlagSchema> {
  // These properties exist for type compatibility but actual implementation uses generated class
  protected schema!: T;
  protected evaluationContext!: EvaluationContext;
  protected evaluator!: FlagEvaluator;
  protected buffer!: SpanBuffer | null;
  protected columnWriters?: FlagColumnWriters;

  constructor(
    schema: T,
    evaluationContext: EvaluationContext,
    evaluator: FlagEvaluator,
    bufferOrColumnWriters?: SpanBuffer | FlagColumnWriters | null,
  ) {
    // Determine if we got FlagColumnWriters or SpanBuffer
    let buffer: SpanBuffer | null = null;
    let columnWriters: FlagColumnWriters | undefined;

    if (bufferOrColumnWriters && 'writeEntryType' in bufferOrColumnWriters) {
      // It's FlagColumnWriters
      columnWriters = bufferOrColumnWriters as FlagColumnWriters;
    } else if (bufferOrColumnWriters) {
      // It's SpanBuffer
      buffer = bufferOrColumnWriters as SpanBuffer;
    }

    const state: GeneratedEvaluatorState<T> = {
      schema,
      evaluationContext,
      evaluator,
      buffer,
      columnWriters,
      accessedFlags: new Set(),
      flagCache: new Map(),
    };

    // Get or create the generated class for this schema
    const GeneratedClass = getOrCreateEvaluatorClass(schema);

    // Return generated class instance instead of this instance
    // This is a valid JavaScript pattern - constructors can return objects
    // biome-ignore lint/correctness/noConstructorReturn: Valid pattern for generated class instances
    return new GeneratedClass(state) as unknown as FeatureFlagEvaluator<T>;
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
  trackUsage<K extends keyof T>(_flag: K, _context?: FlagTrackContext): void {
    throw new Error('Should not be called - handled by generated class');
  }

  forContext(_additional: Partial<EvaluationContext>): FeatureFlagEvaluator<T> {
    throw new Error('Should not be called - handled by generated class');
  }

  withBuffer(_buffer: SpanBuffer): FeatureFlagEvaluator<T> {
    throw new Error('Should not be called - handled by generated class');
  }

  getContext(): EvaluationContext {
    throw new Error('Should not be called - handled by generated class');
  }
}

/**
 * Simple in-memory flag evaluator for testing
 */
export class InMemoryFlagEvaluator implements FlagEvaluator {
  private flags: Record<string, FlagValue> = {};

  constructor(initialFlags: Record<string, FlagValue> = {}) {
    this.flags = initialFlags;
  }

  getSync<K extends string>(flag: K, _context: EvaluationContext): FlagValue {
    return this.flags[flag] ?? null;
  }

  async getAsync<K extends string>(flag: K, _context: EvaluationContext): Promise<FlagValue> {
    return this.flags[flag] ?? null;
  }

  setFlag(flag: string, value: FlagValue): void {
    this.flags[flag] = value;
  }
}
