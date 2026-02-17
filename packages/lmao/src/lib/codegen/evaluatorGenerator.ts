/**
 * Runtime code generation for FeatureFlagEvaluator classes
 *
 * Per specs/lmao/01g_trace_context_api_codegen.md:
 * - Uses new Function() to generate optimized classes at schema definition time (cold path)
 * - Getter properties for each flag enable V8 hidden class optimization
 * - No Proxy overhead - direct property access with inline caching
 *
 * ## Why Each Span Gets Its Own Evaluator Instance
 *
 * The generated class stores `#spanContext` because JS getters (`get darkMode()`)
 * cannot receive parameters. This means `forContext()` MUST create a new instance
 * per span - we can't reuse instances across spans.
 *
 * This is intentional and acceptable because:
 * 1. Span creation is already a cold-path allocation
 * 2. The object is small (just references to ctx, evaluator, schema)
 * 3. V8 is very efficient at allocating small objects with stable hidden classes
 *
 * The `FlagEvaluator.getSync(ctx, flag)` receives ctx so advanced evaluators
 * (e.g., LaunchDarkly) can use ctx.log, ctx.scope for tracing/targeting.
 *
 * V8 Optimization benefits:
 * - Stable hidden class (all instances have same shape)
 * - Monomorphic property access (getters are real properties)
 * - No Proxy trap overhead
 * - Inline caching works properly
 */

import type { OpContext } from '../opContext/types.js';
import type { FeatureFlagSchema } from '../schema/defineFeatureFlags.js';
import type { FlagEvaluator, FlagTrackContext, InferFeatureFlagsWithContext } from '../schema/evaluator.js';
import type { Schema } from '../schema/types.js';
import type { SpanContext } from '../spanContext.js';

/**
 * Base interface for generated evaluator classes
 *
 * Takes OpContext as the single type parameter (matches SpanContext, Op, etc.)
 * Extracts Ctx['flags'] for flag-specific typing.
 */
export interface GeneratedEvaluatorBase<Ctx extends OpContext> {
  get(flag: string): Promise<unknown>;
  trackUsage<K extends keyof Ctx['flags']>(flag: K, context?: FlagTrackContext): void;
  forContext(ctx: SpanContext<Ctx>): GeneratedEvaluatorBase<Ctx> & InferFeatureFlagsWithContext<Ctx>;
  readonly evaluator: FlagEvaluator<Ctx>;
}

/**
 * Cache for generated evaluator classes by schema reference
 */
const evaluatorClassCache = new WeakMap<
  FeatureFlagSchema,
  new (
    spanContext: SpanContext<OpContext>,
    evaluator: FlagEvaluator<OpContext>,
  ) => GeneratedEvaluatorBase<OpContext>
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
 *
 * The schema is NOT embedded in the generated code because it contains Sury
 * schema objects with methods that cannot be JSON serialized. Instead, the
 * schema is passed as a closure parameter when the IIFE is called.
 */
export function generateEvaluatorClass<T extends FeatureFlagSchema>(
  schema: T,
  className = 'GeneratedEvaluator',
): string {
  const flagNames = Object.keys(schema);

  // Generate getter for each flag
  const flagGetters = flagNames.map((name) => generateFlagGetter(name)).join('\n');

  // Generated code: IIFE that takes dependencies as parameters and returns the class
  // Schema is passed as a closure parameter (contains Sury schema objects with methods)
  const classCode =
    `(function(validateFlagValue, ENTRY_TYPE_FF_ACCESS, SCHEMA) {
  'use strict';

  class ${className} {
    #spanContext;
    #evaluator;
    #schema;

    constructor(spanContext, evaluator) {
      this.#spanContext = spanContext;
      this.#evaluator = evaluator;
      // Schema passed as closure parameter at codegen time
      this.#schema = SCHEMA;
    }

    get evaluator() { return this.#evaluator; }
` +
    // Generated code: #hasLoggedAccess() - scans buffer chain backward to check if flag already logged
    // Walks chain forward via ._overflow, scans each buffer backward from writeIndex
    `
    #hasLoggedAccess(flagName) {
      let buf = this.#spanContext._buffer;
      
      while (buf) {
        // Use buffer._writeIndex which is _writeIndex + 1 (points past last written index)
        // This ensures we check all written entries including the most recent
        const limit = buf._writeIndex;
        for (let i = limit - 1; i >= 0; i--) {
          if (buf.entry_type[i] === ENTRY_TYPE_FF_ACCESS && 
              buf.message_values && buf.message_values[i] === flagName) {
            return true;
          }
        }
        buf = buf._overflow;
      }
      return false;
    }
` +
    // Generated code: #getFlag() - synchronous flag evaluation with logging
    // Passes full SpanContext (minus ff) to evaluator for logging/tracing capability,
    // returns undefined for falsy values, wraps truthy values with track() method
    `
    #getFlag(flagName) {
      const ctx = this.#spanContext;
      const definition = this.#schema[flagName];
      
      // Pass ctx first (consistency), then flag name
      // Evaluator receives Omit<SpanContext, 'ff'> to prevent infinite recursion
      const rawValue = this.#evaluator.getSync(ctx, flagName);
      const value = validateFlagValue(rawValue, definition.schema, definition.defaultValue);
      
      // Deduplicate: only log first access per span
      if (!this.#hasLoggedAccess(flagName)) {
        const log = ctx.log;
        log.ffAccess(flagName, rawValue);
      }
      
      if (!value) {
        return undefined;
      }
      
      const wrapped = this.#wrapValue(flagName, value);
      return wrapped;
    }
` +
    // Generated code: #wrapValue() - wraps truthy flag value with track() method
    // Returns object with value property and track() that calls #logUsage
    `
    #wrapValue(flagName, value) {
      const self = this;
      if (typeof value === 'boolean') {
        return {
          value: true,
          track(context) { self.#logUsage(flagName, context); }
        };
      }
      if (typeof value === 'string') {
        return {
          value,
          track(context) { self.#logUsage(flagName, context); }
        };
      }
      if (typeof value === 'object' && value !== null) {
        return {
          ...value,
          track(context) { self.#logUsage(flagName, context); }
        };
      }
      return {
        value,
        track(context) { self.#logUsage(flagName, context); }
      };
    }
` +
    // Generated code: #logUsage() - logs flag usage via logger
    `
    #logUsage(flagName, context) {
      const log = this.#spanContext.log;
      log.ffUsage(flagName, context);
    }
` +
    // Generated code: async get() - async flag evaluation
    // Returns undefined when false, FlagContext when truthy
    `
    async get(flag) {
      const ctx = this.#spanContext;
      const definition = this.#schema[flag];
      
      // Pass ctx first (consistency), then flag name
      // Evaluator receives Omit<SpanContext, 'ff'> to prevent infinite recursion
      const rawValue = await this.#evaluator.getAsync(ctx, flag);
      const value = validateFlagValue(rawValue, definition.schema, definition.defaultValue);
      
      // Deduplicate: only log first access per span
      if (!this.#hasLoggedAccess(flag)) {
        const log = ctx.log;
        log.ffAccess(flag, rawValue);
      }
      
      if (!value) {
        return undefined;
      }
      
      const wrapped = this.#wrapValue(flag, value);
      return wrapped;
    }
` +
    // Generated code: trackUsage() - manual usage tracking (prefer flag.track() fluent API)
    `
    trackUsage(flag, context) {
      this.#logUsage(flag, context);
    }
` +
    // Generated code: forContext() - creates child evaluator bound to span context
    `
    forContext(ctx) {
      return new ${className}(ctx, this.#evaluator);
    }
` +
    // Generated code: getters for each flag (generated from schema)
    flagGetters +
    `
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
 * @param ENTRY_TYPE_FF_ACCESS - Entry type constant for access
 * @returns Constructor for generated evaluator class
 */
export function createEvaluatorClass<Ctx extends OpContext>(
  schema: Ctx['flags'],
  validateFlagValue: (value: unknown, schema: Schema<unknown, unknown>, defaultValue: unknown) => unknown,
  ENTRY_TYPE_FF_ACCESS: number,
): new (
  spanContext: SpanContext<Ctx>,
  evaluator: FlagEvaluator<Ctx>,
) => GeneratedEvaluatorBase<Ctx> & InferFeatureFlagsWithContext<Ctx> {
  // Check cache first
  const cached = evaluatorClassCache.get(schema);
  if (cached) {
    return cached as unknown as new (
      spanContext: SpanContext<Ctx>,
      evaluator: FlagEvaluator<Ctx>,
    ) => GeneratedEvaluatorBase<Ctx> & InferFeatureFlagsWithContext<Ctx>;
  }

  const classCode = generateEvaluatorClass(schema).trim();

  // Use Function constructor to create the class factory
  // The generated code is an IIFE that takes dependencies as parameters
  // eslint-disable-next-line @typescript-eslint/no-implied-eval, no-new-func
  const classFactory = new Function(`return ${classCode}`)();

  // Call the factory with dependencies to get the actual class
  // Schema is passed as closure parameter (contains Sury objects, can't be JSON serialized)
  const GeneratedClass = classFactory(validateFlagValue, ENTRY_TYPE_FF_ACCESS, schema);

  // Cache the generated class
  evaluatorClassCache.set(schema, GeneratedClass);

  return GeneratedClass as unknown as new (
    spanContext: SpanContext<Ctx>,
    evaluator: FlagEvaluator<Ctx>,
  ) => GeneratedEvaluatorBase<Ctx> & InferFeatureFlagsWithContext<Ctx>;
}
