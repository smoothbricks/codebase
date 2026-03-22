/**
 * Worker-safe feature-flag evaluator generation.
 *
 * Getter descriptors still live on the prototype, but the class is assembled with
 * normal closures and `Object.defineProperties()` instead of `new Function(...)`.
 */

import type { OpContext } from '../opContext/types.js';
import type { FeatureFlagSchema } from '../schema/defineFeatureFlags.js';
import type { FlagEvaluator, FlagTrackContext, InferFeatureFlagsWithContext } from '../schema/evaluator.js';
import type { Schema } from '../schema/types.js';
import type { SpanContext, SpanLoggerInternal } from '../spanContext.js';
import type { AnySpanBuffer } from '../types.js';

/**
 * Base interface for generated evaluator classes.
 */
export interface GeneratedEvaluatorBase<Ctx extends OpContext> {
  get<K extends keyof InferFeatureFlagsWithContext<Ctx> & string>(
    flag: K,
  ): Promise<InferFeatureFlagsWithContext<Ctx>[K]>;
  get(flag: string): Promise<unknown>;
  forContext(ctx: SpanContext<Ctx>): GeneratedEvaluatorInstance<Ctx>;
  readonly evaluator: FlagEvaluator<Ctx>;
}

export type GeneratedEvaluatorInstance<Ctx extends OpContext> = GeneratedEvaluatorBase<Ctx> &
  InferFeatureFlagsWithContext<Ctx>;

type GeneratedEvaluatorConstructor<Ctx extends OpContext> = new (
  spanContext: SpanContext<Ctx>,
  evaluator: FlagEvaluator<Ctx>,
) => GeneratedEvaluatorBase<Ctx>;

type CachedConstructor = abstract new (...args: never[]) => object;

type ValidateFlagValue = (value: unknown, schema: Schema<unknown, unknown>, defaultValue: unknown) => unknown;

type FeatureFlagDefinition = FeatureFlagSchema[string];

type WorkerSafeEvaluatorInstance = WorkerSafeGeneratedEvaluator<OpContext>;

/**
 * Cache for generated evaluator classes by schema reference.
 */
const evaluatorClassCache = new WeakMap<FeatureFlagSchema, CachedConstructor>();

function createFlagGetter(flagName: string) {
  return function generatedFlagGetter(this: WorkerSafeEvaluatorInstance) {
    return this.getFlag(flagName);
  };
}

function buildGetterDescriptor(flagName: string): PropertyDescriptor {
  return {
    configurable: true,
    enumerable: false,
    get: createFlagGetter(flagName),
  };
}

function buildGetterDescriptors(flagNames: readonly string[]): PropertyDescriptorMap {
  const descriptors: PropertyDescriptorMap = {};

  for (const flagName of flagNames) {
    descriptors[flagName] = buildGetterDescriptor(flagName);
  }

  return descriptors;
}

export function typeGeneratedEvaluator<Ctx extends OpContext>(
  evaluator: GeneratedEvaluatorBase<Ctx>,
): GeneratedEvaluatorInstance<Ctx> {
  return evaluator as GeneratedEvaluatorInstance<Ctx>;
}

/**
 * Render a debug view of the worker-safe generated class.
 *
 * This exists for snapshots/tests only; runtime assembly happens without eval.
 */
export function generateEvaluatorClass<T extends FeatureFlagSchema>(
  schema: T,
  className = 'GeneratedEvaluator',
): string {
  const flagNames = Object.keys(schema);
  const getterEntries = flagNames
    .map(
      (flagName) => `  ${flagName}: {
    configurable: true,
    enumerable: false,
    get() {
      return this.getFlag('${flagName}');
    },
  }`,
    )
    .join(',\n');

  return `class ${className} extends WorkerSafeGeneratedEvaluator {
  constructor(spanContext, evaluator) {
    super(spanContext, evaluator, SCHEMA, validateFlagValue, ENTRY_TYPE_FF_ACCESS);
  }

  forContext(ctx) {
    return new ${className}(ctx, this.evaluator);
  }
}

Object.defineProperties(${className}.prototype, {
${getterEntries}
});`;
}

class WorkerSafeGeneratedEvaluator<Ctx extends OpContext> implements GeneratedEvaluatorBase<Ctx> {
  readonly #spanContext: SpanContext<Ctx>;
  readonly #evaluator: FlagEvaluator<Ctx>;
  readonly #schema: Ctx['flags'];
  readonly #validateFlagValue: ValidateFlagValue;
  readonly #entryTypeFfAccess: number;

  constructor(
    spanContext: SpanContext<Ctx>,
    evaluator: FlagEvaluator<Ctx>,
    schema: Ctx['flags'],
    validateFlagValue: ValidateFlagValue,
    entryTypeFfAccess: number,
  ) {
    this.#spanContext = spanContext;
    this.#evaluator = evaluator;
    this.#schema = schema;
    this.#validateFlagValue = validateFlagValue;
    this.#entryTypeFfAccess = entryTypeFfAccess;
  }

  get evaluator(): FlagEvaluator<Ctx> {
    return this.#evaluator;
  }

  protected hasLoggedAccess(flagName: string): boolean {
    let buf: AnySpanBuffer | undefined = this.#spanContext._buffer as AnySpanBuffer;

    while (buf) {
      const limit = buf._writeIndex;
      for (let i = limit - 1; i >= 0; i--) {
        if (buf.entry_type[i] === this.#entryTypeFfAccess && buf.message_values && buf.message_values[i] === flagName) {
          return true;
        }
      }
      buf = buf._overflow as AnySpanBuffer | undefined;
    }

    return false;
  }

  protected getFlag(flagName: string): unknown {
    const ctx = this.#spanContext;
    const definition = this.getDefinition(flagName);
    const rawValue = this.#evaluator.getSync(ctx, flagName);
    const value = this.#validateFlagValue(rawValue, definition.schema, definition.defaultValue);

    if (!this.hasLoggedAccess(flagName)) {
      this.getLogger().ffAccess(flagName, rawValue);
    }

    if (!value) {
      return undefined;
    }

    return this.wrapValue(flagName, value);
  }

  get<K extends keyof InferFeatureFlagsWithContext<Ctx> & string>(
    flag: K,
  ): Promise<InferFeatureFlagsWithContext<Ctx>[K]>;
  get(flag: string): Promise<unknown>;
  async get(flag: string): Promise<unknown> {
    const ctx = this.#spanContext;
    const definition = this.getDefinition(flag);
    const rawValue = await this.#evaluator.getAsync(ctx, flag);
    const value = this.#validateFlagValue(rawValue, definition.schema, definition.defaultValue);

    if (!this.hasLoggedAccess(flag)) {
      this.getLogger().ffAccess(flag, rawValue);
    }

    if (!value) {
      return undefined;
    }

    return this.wrapValue(flag, value);
  }

  forContext(_ctx: SpanContext<Ctx>): GeneratedEvaluatorInstance<Ctx> {
    throw new Error('forContext() must be implemented by the schema-specific evaluator class');
  }

  private getDefinition(flagName: string): FeatureFlagDefinition {
    const definition = this.#schema[flagName];
    if (!definition) {
      throw new Error(`Unknown feature flag: ${flagName}`);
    }
    return definition;
  }

  private wrapValue(flagName: string, value: unknown): unknown {
    const track = (context?: FlagTrackContext) => this.logUsage(flagName, context);

    if (typeof value === 'boolean') {
      return { value: true, track };
    }

    if (typeof value === 'string') {
      return { value, track };
    }

    if (typeof value === 'object' && value !== null) {
      return { ...value, track };
    }

    return { value, track };
  }

  private getLogger(): SpanLoggerInternal<Ctx['logSchema']> {
    return this.#spanContext.log as SpanLoggerInternal<Ctx['logSchema']>;
  }

  private logUsage(flagName: string, context?: FlagTrackContext) {
    return this.getLogger().ffUsage(flagName, context ? { ...context } : undefined);
  }
}

/**
 * Create evaluator class constructor from schema.
 * This is the cold-path function called at schema definition time.
 */
export function createEvaluatorClass<Ctx extends OpContext>(
  schema: Ctx['flags'],
  validateFlagValue: ValidateFlagValue,
  ENTRY_TYPE_FF_ACCESS: number,
): GeneratedEvaluatorConstructor<Ctx> {
  const cached = evaluatorClassCache.get(schema);
  if (cached) {
    return cached as GeneratedEvaluatorConstructor<Ctx>;
  }

  class GeneratedEvaluator extends WorkerSafeGeneratedEvaluator<Ctx> {
    constructor(spanContext: SpanContext<Ctx>, evaluator: FlagEvaluator<Ctx>) {
      super(spanContext, evaluator, schema, validateFlagValue, ENTRY_TYPE_FF_ACCESS);
    }

    override forContext(ctx: SpanContext<Ctx>): GeneratedEvaluatorInstance<Ctx> {
      return typeGeneratedEvaluator(new GeneratedEvaluator(ctx, this.evaluator));
    }
  }

  Object.defineProperties(GeneratedEvaluator.prototype, buildGetterDescriptors(Object.keys(schema)));
  evaluatorClassCache.set(schema, GeneratedEvaluator);

  return GeneratedEvaluator;
}
