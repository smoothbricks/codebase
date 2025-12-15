import * as S from '@sury/sury';
import { categoryInterner, ENTRY_TYPE_FF_ACCESS, ENTRY_TYPE_FF_USAGE } from '../lmao.js';
import { getTimestampMicros } from '../timestamp.js';
import type { SpanBuffer } from '../types.js';
import type { EvaluationContext, FeatureFlagSchema } from './defineFeatureFlags.js';

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
// Internal Evaluator State
// ============================================================================

/**
 * Internal state for a FeatureFlagEvaluator instance
 * Kept separate from the Proxy to allow clean property access
 */
interface EvaluatorState<T extends FeatureFlagSchema> {
  schema: T;
  evaluationContext: EvaluationContext;
  evaluator: FlagEvaluator;
  buffer: SpanBuffer | null;
  anchorEpochMicros: number;
  anchorPerfNow: number;
  columnWriters?: FlagColumnWriters;
  accessedFlags: Set<string>;
  flagCache: Map<string, unknown>;
}

// ============================================================================
// Feature Flag Evaluator Factory
// ============================================================================

/**
 * Create a feature flag evaluator with undefined/truthy semantics
 *
 * - Returns undefined when flag is false/disabled
 * - Returns FlagContext wrapper when flag is truthy/enabled
 * - First access logs ff-access entry
 * - Subsequent access in same span returns cached value (no duplicate log)
 * - track() always logs ff-usage entry
 *
 * @returns Proxy object that intercepts flag access
 */
function createEvaluatorProxy<T extends FeatureFlagSchema>(
  state: EvaluatorState<T>,
): FeatureFlagEvaluator<T> & InferFeatureFlagsWithContext<T> {
  // Helper functions that use state

  function getFlag(flagName: string): unknown {
    // Check cache first (deduplication - subsequent access in same span)
    if (state.flagCache.has(flagName)) {
      return state.flagCache.get(flagName);
    }

    const definition = state.schema[flagName];

    // Evaluate flag
    const rawValue = state.evaluator.getSync(flagName, state.evaluationContext);
    const value = validateFlagValue(rawValue, definition.schema, definition.defaultValue);

    // Log access (only on first access in this span)
    if (!state.accessedFlags.has(flagName)) {
      logAccess(flagName, rawValue);
      state.accessedFlags.add(flagName);
    }

    // Return undefined for falsy values (false, 0, "", null, undefined)
    if (!value) {
      state.flagCache.set(flagName, undefined);
      return undefined;
    }

    // Wrap truthy values with track() method
    const wrapped = wrapValue(flagName, value);
    state.flagCache.set(flagName, wrapped);
    return wrapped;
  }

  function wrapValue(flagName: string, value: unknown): unknown {
    if (typeof value === 'boolean') {
      return {
        value: true as const,
        track(context?: FlagTrackContext) {
          logUsage(flagName, context);
        },
      } satisfies BooleanFlagContext;
    }

    if (typeof value === 'string') {
      return {
        value,
        track(context?: FlagTrackContext) {
          logUsage(flagName, context);
        },
      };
    }

    if (typeof value === 'object' && value !== null) {
      return {
        ...value,
        track(context?: FlagTrackContext) {
          logUsage(flagName, context);
        },
      };
    }

    return {
      value,
      track(context?: FlagTrackContext) {
        logUsage(flagName, context);
      },
    };
  }

  function logAccess(flagName: string, value: unknown): void {
    // Legacy API support
    if (state.columnWriters) {
      state.columnWriters.writeEntryType('ff-access');
      state.columnWriters.writeFfName(flagName);
      state.columnWriters.writeFfValue(value as FlagValue);
      state.columnWriters.writeContextAttributes(state.evaluationContext);
      return;
    }

    // New SpanBuffer API
    if (!state.buffer) return;

    const idx = state.buffer.writeIndex;

    state.buffer.operations[idx] = ENTRY_TYPE_FF_ACCESS;

    if (state.anchorEpochMicros && state.anchorPerfNow) {
      state.buffer.timestamps[idx] = getTimestampMicros(state.anchorEpochMicros, state.anchorPerfNow);
    } else {
      state.buffer.timestamps[idx] = Date.now() * 1000;
    }

    const ffNameColumn = state.buffer['attr_ffName' as keyof SpanBuffer];
    if (ffNameColumn && ffNameColumn instanceof Uint32Array) {
      ffNameColumn[idx] = categoryInterner.intern(flagName);
    }

    const ffValueColumn = state.buffer['attr_ffValue' as keyof SpanBuffer];
    if (ffValueColumn && ffValueColumn instanceof Uint32Array) {
      const strValue = value === null || value === undefined ? 'null' : String(value);
      ffValueColumn[idx] = categoryInterner.intern(strValue);
    }

    state.buffer.writeIndex++;
  }

  function logUsage(flagName: string, context?: FlagTrackContext): void {
    // Legacy API support
    if (state.columnWriters) {
      state.columnWriters.writeEntryType('ff-usage');
      state.columnWriters.writeFfName(flagName);
      state.columnWriters.writeAction(context?.action);
      state.columnWriters.writeOutcome(context?.outcome);
      state.columnWriters.writeContextAttributes(state.evaluationContext);
      return;
    }

    if (!state.buffer) return;

    const idx = state.buffer.writeIndex;

    state.buffer.operations[idx] = ENTRY_TYPE_FF_USAGE;

    if (state.anchorEpochMicros && state.anchorPerfNow) {
      state.buffer.timestamps[idx] = getTimestampMicros(state.anchorEpochMicros, state.anchorPerfNow);
    } else {
      state.buffer.timestamps[idx] = Date.now() * 1000;
    }

    const ffNameColumn = state.buffer['attr_ffName' as keyof SpanBuffer];
    if (ffNameColumn && ffNameColumn instanceof Uint32Array) {
      ffNameColumn[idx] = categoryInterner.intern(flagName);
    }

    if (context?.action) {
      const actionColumn = state.buffer['attr_action' as keyof SpanBuffer];
      if (actionColumn && actionColumn instanceof Uint32Array) {
        actionColumn[idx] = categoryInterner.intern(context.action);
        const nullBitmap = state.buffer.nullBitmaps['attr_action'];
        if (nullBitmap) {
          const byteIndex = Math.floor(idx / 8);
          const bitOffset = idx % 8;
          nullBitmap[byteIndex] |= 1 << bitOffset;
        }
      }
    }

    if (context?.outcome) {
      const outcomeColumn = state.buffer['attr_outcome' as keyof SpanBuffer];
      if (outcomeColumn && outcomeColumn instanceof Uint32Array) {
        outcomeColumn[idx] = categoryInterner.intern(context.outcome);
        const nullBitmap = state.buffer.nullBitmaps['attr_outcome'];
        if (nullBitmap) {
          const byteIndex = Math.floor(idx / 8);
          const bitOffset = idx % 8;
          nullBitmap[byteIndex] |= 1 << bitOffset;
        }
      }
    }

    state.buffer.writeIndex++;
  }

  // Async flag getter
  async function getAsync(flag: string): Promise<unknown> {
    const cacheKey = `async_${flag}`;
    if (state.flagCache.has(cacheKey)) {
      return state.flagCache.get(cacheKey);
    }

    const definition = state.schema[flag];
    const rawValue = await state.evaluator.getAsync(flag, state.evaluationContext);
    const value = validateFlagValue(rawValue, definition.schema, definition.defaultValue);

    if (!state.accessedFlags.has(flag)) {
      logAccess(flag, rawValue);
      state.accessedFlags.add(flag);
    }

    if (!value) {
      state.flagCache.set(cacheKey, undefined);
      return undefined;
    }

    const wrapped = wrapValue(flag, value);
    state.flagCache.set(cacheKey, wrapped);
    return wrapped;
  }

  /** @deprecated Use flag.track() instead for new undefined/truthy API */
  function trackUsage(flag: string, context?: FlagTrackContext): void {
    logUsage(flag, context);
  }

  function withContext(
    additional: Partial<EvaluationContext>,
  ): FeatureFlagEvaluator<T> & InferFeatureFlagsWithContext<T> {
    return createEvaluatorProxy({
      ...state,
      evaluationContext: { ...state.evaluationContext, ...additional },
      accessedFlags: new Set(),
      flagCache: new Map(),
    });
  }

  function withBuffer(buffer: SpanBuffer): FeatureFlagEvaluator<T> & InferFeatureFlagsWithContext<T> {
    return createEvaluatorProxy({
      ...state,
      buffer,
      accessedFlags: new Set(),
      flagCache: new Map(),
    });
  }

  function getContext(): EvaluationContext {
    return state.evaluationContext;
  }

  // Create base object with methods
  const baseObject = {
    get: getAsync,
    trackUsage,
    withContext,
    withBuffer,
    getContext,
    // Include properties from the class for type compatibility
    schema: state.schema,
    evaluationContext: state.evaluationContext,
    evaluator: state.evaluator,
    buffer: state.buffer,
  };

  // Create proxy to intercept flag access
  // Type the proxy properly using type assertion
  type ProxiedType = FeatureFlagEvaluator<T> & InferFeatureFlagsWithContext<T>;

  return new Proxy(baseObject, {
    get(_target, prop) {
      // Handle methods first
      if (prop === 'get') return getAsync;
      if (prop === 'trackUsage') return trackUsage;
      if (prop === 'withContext') return withContext;
      if (prop === 'withBuffer') return withBuffer;
      if (prop === 'getContext') return getContext;

      // Expose internal state for legacy compatibility
      if (prop === 'schema') return state.schema;
      if (prop === 'evaluationContext') return state.evaluationContext;
      if (prop === 'evaluator') return state.evaluator;
      if (prop === 'buffer') return state.buffer;
      if (prop === 'columnWriters') return state.columnWriters;

      // Handle Symbol properties (for instanceof checks, etc.)
      if (typeof prop === 'symbol') {
        return undefined;
      }

      // Handle flag access
      if (typeof prop === 'string' && prop in state.schema) {
        return getFlag(prop);
      }

      return undefined;
    },

    set(_target, prop, value) {
      if (prop === 'columnWriters') {
        state.columnWriters = value;
        return true;
      }
      if (prop === 'buffer') {
        state.buffer = value;
        return true;
      }
      return false;
    },
  }) as unknown as ProxiedType;
}

// ============================================================================
// Feature Flag Evaluator Class
// ============================================================================

/**
 * Feature flag evaluator with undefined/truthy semantics
 *
 * - Returns undefined when flag is false/disabled
 * - Returns FlagContext wrapper when flag is truthy/enabled
 * - First access logs ff-access entry
 * - Subsequent access in same span returns cached value (no duplicate log)
 * - track() always logs ff-usage entry
 *
 * Note: The class constructor returns a Proxy object rather than the instance itself.
 * This is a valid JavaScript pattern for creating "virtual" instances with custom behavior.
 */
export class FeatureFlagEvaluator<T extends FeatureFlagSchema> {
  // These properties exist for type compatibility but actual implementation uses Proxy
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
    anchorEpochMicros = 0,
    anchorPerfNow = 0,
  ) {
    // Determine if we got FlagColumnWriters (legacy) or SpanBuffer (new)
    let buffer: SpanBuffer | null = null;
    let columnWriters: FlagColumnWriters | undefined;

    if (bufferOrColumnWriters && 'writeEntryType' in bufferOrColumnWriters) {
      columnWriters = bufferOrColumnWriters;
    } else {
      buffer = bufferOrColumnWriters || null;
    }

    const state: EvaluatorState<T> = {
      schema,
      evaluationContext,
      evaluator,
      buffer,
      anchorEpochMicros,
      anchorPerfNow,
      columnWriters,
      accessedFlags: new Set(),
      flagCache: new Map(),
    };

    // Return proxy instead of this instance
    // This is a valid JavaScript pattern - constructors can return objects
    // biome-ignore lint/correctness/noConstructorReturn: Valid pattern for Proxy-based classes
    return createEvaluatorProxy(state) as unknown as FeatureFlagEvaluator<T>;
  }

  // These methods exist for TypeScript type information only
  // The proxy handles all actual method calls

  /**
   * Get async flag value
   * Returns undefined when false, FlagContext when truthy
   */
  get(_flag: string): Promise<unknown> {
    throw new Error('Should not be called - handled by proxy');
  }

  /** @deprecated Use flag.track() instead */
  trackUsage<K extends keyof T>(_flag: K, _context?: FlagTrackContext): void {
    throw new Error('Should not be called - handled by proxy');
  }

  withContext(_additional: Partial<EvaluationContext>): FeatureFlagEvaluator<T> {
    throw new Error('Should not be called - handled by proxy');
  }

  withBuffer(_buffer: SpanBuffer): FeatureFlagEvaluator<T> {
    throw new Error('Should not be called - handled by proxy');
  }

  getContext(): EvaluationContext {
    throw new Error('Should not be called - handled by proxy');
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
