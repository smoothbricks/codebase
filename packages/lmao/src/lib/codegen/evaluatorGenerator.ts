/**
 * Runtime code generation for FeatureFlagEvaluator classes
 *
 * Per specs/01g_trace_context_api_codegen.md:
 * - Uses new Function() to generate optimized classes at schema definition time (cold path)
 * - Getter properties for each flag enable V8 hidden class optimization
 * - No Proxy overhead - direct property access with inline caching
 *
 * V8 Optimization benefits:
 * - Stable hidden class (all instances have same shape)
 * - Monomorphic property access (getters are real properties)
 * - No Proxy trap overhead
 * - Inline caching works properly
 */

import type { EvaluationContext, FeatureFlagSchema } from '../schema/defineFeatureFlags.js';
import type {
  FlagColumnWriters,
  FlagEvaluator,
  FlagTrackContext,
  InferFeatureFlagsWithContext,
} from '../schema/evaluator.js';
import type { SpanBuffer } from '../types.js';

/**
 * Internal state for a FeatureFlagEvaluator instance
 * Kept separate to allow clean property access
 */
export interface GeneratedEvaluatorState<T extends FeatureFlagSchema> {
  schema: T;
  evaluationContext: EvaluationContext;
  evaluator: FlagEvaluator;
  buffer: SpanBuffer | null;
  columnWriters?: FlagColumnWriters;
  accessedFlags: Set<string>;
  flagCache: Map<string, unknown>;
}

/**
 * Base interface for generated evaluator classes
 */
export interface GeneratedEvaluatorBase<T extends FeatureFlagSchema> {
  get(flag: string): Promise<unknown>;
  trackUsage<K extends keyof T>(flag: K, context?: FlagTrackContext): void;
  forContext(additional: Partial<EvaluationContext>): GeneratedEvaluatorBase<T> & InferFeatureFlagsWithContext<T>;
  withBuffer(buffer: SpanBuffer): GeneratedEvaluatorBase<T> & InferFeatureFlagsWithContext<T>;
  getContext(): EvaluationContext;
  // Expose internal state for introspection
  readonly schema: T;
  readonly evaluationContext: EvaluationContext;
  readonly evaluator: FlagEvaluator;
  readonly buffer: SpanBuffer | null;
  columnWriters?: FlagColumnWriters;
}

/**
 * Cache for generated evaluator classes by schema reference
 */
const evaluatorClassCache = new WeakMap<
  FeatureFlagSchema,
  new (
    state: GeneratedEvaluatorState<FeatureFlagSchema>,
  ) => GeneratedEvaluatorBase<FeatureFlagSchema>
>();

/**
 * Generate getter code for a single flag
 * Creates a getter property that evaluates the flag on access
 */
function generateFlagGetter(flagName: string): string {
  return `
    get ${flagName}() {
      return this.#getFlag('${flagName}');
    }`;
}

/**
 * Generate complete FeatureFlagEvaluator class code
 * Returns executable JavaScript code as a string
 */
export function generateEvaluatorClass<T extends FeatureFlagSchema>(
  schema: T,
  className = 'GeneratedEvaluator',
): string {
  const flagNames = Object.keys(schema);

  // Generate getter for each flag
  const flagGetters = flagNames.map((name) => generateFlagGetter(name)).join('\n');

  const classCode = `
(function(validateFlagValue, getTimestampNanos, ENTRY_TYPE_FF_ACCESS, ENTRY_TYPE_FF_USAGE) {
  'use strict';

  class ${className} {
    #state;

    constructor(state) {
      this.#state = state;
    }

    // Expose internal state as getters for introspection
    get schema() { return this.#state.schema; }
    get evaluationContext() { return this.#state.evaluationContext; }
    get evaluator() { return this.#state.evaluator; }
    get buffer() { return this.#state.buffer; }
    get columnWriters() { return this.#state.columnWriters; }
    set columnWriters(value) { this.#state.columnWriters = value; }
    set buffer(value) { this.#state.buffer = value; }

    /**
     * Get a flag value synchronously with caching and logging
     * Returns undefined for falsy values, FlagContext wrapper for truthy values
     */
    #getFlag(flagName) {
      const state = this.#state;

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
        this.#logAccess(flagName, rawValue);
        state.accessedFlags.add(flagName);
      }

      // Return undefined for falsy values (false, 0, "", null, undefined)
      if (!value) {
        state.flagCache.set(flagName, undefined);
        return undefined;
      }

      // Wrap truthy values with track() method
      const wrapped = this.#wrapValue(flagName, value);
      state.flagCache.set(flagName, wrapped);
      return wrapped;
    }

    /**
     * Wrap a truthy flag value with a track() method
     */
    #wrapValue(flagName, value) {
      const self = this;

      if (typeof value === 'boolean') {
        return {
          value: true,
          track(context) {
            self.#logUsage(flagName, context);
          }
        };
      }

      if (typeof value === 'string') {
        return {
          value,
          track(context) {
            self.#logUsage(flagName, context);
          }
        };
      }

      if (typeof value === 'object' && value !== null) {
        return {
          ...value,
          track(context) {
            self.#logUsage(flagName, context);
          }
        };
      }

      return {
        value,
        track(context) {
          self.#logUsage(flagName, context);
        }
      };
    }

    /**
     * Log flag access to buffer or column writers
     */
    #logAccess(flagName, value) {
      const state = this.#state;

      // Support for FlagColumnWriters interface
      if (state.columnWriters) {
        state.columnWriters.writeEntryType('ff-access');
        state.columnWriters.writeFfName(flagName);
        state.columnWriters.writeFfValue(value);
        state.columnWriters.writeContextAttributes(state.evaluationContext);
        return;
      }

      // New SpanBuffer API
      if (!state.buffer) return;

      const idx = state.buffer.writeIndex;

      state.buffer.operations[idx] = ENTRY_TYPE_FF_ACCESS;
      state.buffer.timestamps[idx] = getTimestampNanos();

      // Write flag name (category column - stored as raw string)
      const ffNameColumn = state.buffer['attr_ffName_values'];
      if (ffNameColumn && Array.isArray(ffNameColumn)) {
        ffNameColumn[idx] = flagName;
      }

      // Write flag value (category column - stored as raw string)
      const ffValueColumn = state.buffer['attr_ffValue_values'];
      if (ffValueColumn && Array.isArray(ffValueColumn)) {
        const strValue = value === null || value === undefined ? 'null' : String(value);
        ffValueColumn[idx] = strValue;
      }

      state.buffer.writeIndex++;
    }

    /**
     * Log flag usage to buffer or column writers
     */
    #logUsage(flagName, context) {
      const state = this.#state;

      // Support for FlagColumnWriters interface
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
      state.buffer.timestamps[idx] = getTimestampNanos();

      // Write flag name (category column - stored as raw string)
      const ffNameColumn = state.buffer['attr_ffName_values'];
      if (ffNameColumn && Array.isArray(ffNameColumn)) {
        ffNameColumn[idx] = flagName;
      }

      if (context?.action) {
        const actionColumn = state.buffer['attr_action_values'];
        if (actionColumn && Array.isArray(actionColumn)) {
          actionColumn[idx] = context.action;
          const nullBitmap = state.buffer.attr_action_nulls;
          if (nullBitmap) {
            const byteIndex = Math.floor(idx / 8);
            const bitOffset = idx % 8;
            nullBitmap[byteIndex] |= 1 << bitOffset;
          }
        }
      }

      if (context?.outcome) {
        const outcomeColumn = state.buffer['attr_outcome_values'];
        if (outcomeColumn && Array.isArray(outcomeColumn)) {
          outcomeColumn[idx] = context.outcome;
          const nullBitmap = state.buffer.attr_outcome_nulls;
          if (nullBitmap) {
            const byteIndex = Math.floor(idx / 8);
            const bitOffset = idx % 8;
            nullBitmap[byteIndex] |= 1 << bitOffset;
          }
        }
      }

      state.buffer.writeIndex++;
    }

    /**
     * Get async flag value
     * Returns undefined when false, FlagContext when truthy
     */
    async get(flag) {
      const state = this.#state;
      const cacheKey = 'async_' + flag;

      if (state.flagCache.has(cacheKey)) {
        return state.flagCache.get(cacheKey);
      }

      const definition = state.schema[flag];
      const rawValue = await state.evaluator.getAsync(flag, state.evaluationContext);
      const value = validateFlagValue(rawValue, definition.schema, definition.defaultValue);

      if (!state.accessedFlags.has(flag)) {
        this.#logAccess(flag, rawValue);
        state.accessedFlags.add(flag);
      }

      if (!value) {
        state.flagCache.set(cacheKey, undefined);
        return undefined;
      }

      const wrapped = this.#wrapValue(flag, value);
      state.flagCache.set(cacheKey, wrapped);
      return wrapped;
    }

    /**
     * Track flag usage. Prefer using flag.track() for the fluent API.
     */
    trackUsage(flag, context) {
      this.#logUsage(flag, context);
    }

    /**
     * Create child evaluator with additional context
     */
    forContext(additional) {
      const state = this.#state;
      return new ${className}({
        ...state,
        evaluationContext: { ...state.evaluationContext, ...additional },
        accessedFlags: new Set(),
        flagCache: new Map(),
      });
    }

    /**
     * Create evaluator bound to a different buffer
     */
    withBuffer(buffer) {
      const state = this.#state;
      return new ${className}({
        ...state,
        buffer,
        accessedFlags: new Set(),
        flagCache: new Map(),
      });
    }

    /**
     * Get current evaluation context
     */
    getContext() {
      return this.#state.evaluationContext;
    }

    // Generated getters for each flag
    ${flagGetters}
  }

  return ${className};
})
`;

  return classCode;
}

/**
 * Create evaluator class constructor from schema
 * This is the cold-path function called at schema definition time
 *
 * @param schema - Feature flag schema
 * @param validateFlagValue - Validation function for flag values
 * @param getTimestampMicros - Timestamp function
 * @param ENTRY_TYPE_FF_ACCESS - Entry type constant for access
 * @param ENTRY_TYPE_FF_USAGE - Entry type constant for usage
 * @returns Constructor for generated evaluator class
 */
export function createEvaluatorClass<T extends FeatureFlagSchema>(
  schema: T,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  validateFlagValue: (value: unknown, schema: any, defaultValue: any) => any,
  getTimestampNanos: () => bigint,
  ENTRY_TYPE_FF_ACCESS: number,
  ENTRY_TYPE_FF_USAGE: number,
): new (
  state: GeneratedEvaluatorState<T>,
) => GeneratedEvaluatorBase<T> & InferFeatureFlagsWithContext<T> {
  // Check cache first
  const cached = evaluatorClassCache.get(schema);
  if (cached) {
    return cached as unknown as new (
      state: GeneratedEvaluatorState<T>,
    ) => GeneratedEvaluatorBase<T> & InferFeatureFlagsWithContext<T>;
  }

  const classCode = generateEvaluatorClass(schema).trim();

  // Use Function constructor to create the class factory
  // The generated code is an IIFE that takes dependencies as parameters
  // eslint-disable-next-line @typescript-eslint/no-implied-eval, no-new-func
  const classFactory = new Function(`return ${classCode}`)();

  // Call the factory with dependencies to get the actual class
  const GeneratedClass = classFactory(validateFlagValue, getTimestampNanos, ENTRY_TYPE_FF_ACCESS, ENTRY_TYPE_FF_USAGE);

  // Cache the generated class
  evaluatorClassCache.set(schema, GeneratedClass);

  return GeneratedClass as unknown as new (
    state: GeneratedEvaluatorState<T>,
  ) => GeneratedEvaluatorBase<T> & InferFeatureFlagsWithContext<T>;
}
