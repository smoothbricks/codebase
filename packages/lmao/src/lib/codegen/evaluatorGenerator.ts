/**
 * Worker-safe feature-flag evaluator generation.
 *
 * Getter descriptors still live on the prototype, but the class is assembled with
 * normal closures and `Object.defineProperties()` instead of `new Function(...)`.
 */

import type { OpContext } from '../opContext/types.js';
import type { FeatureFlagSchema } from '../schema/defineFeatureFlags.js';
import type {
  FlagEvaluator,
  FlagTrackContext,
  InferFeatureFlagsWithContext,
  SpanContextWithoutFf,
} from '../schema/evaluator.js';
import type { Schema } from '../schema/types.js';
import type { SpanLoggerInternal } from '../spanContext.js';
import type { AnySpanBuffer } from '../types.js';

/**
 * Base interface for generated evaluator classes.
 */
export interface GeneratedEvaluatorBase<Ctx extends OpContext> {
  get<K extends keyof InferFeatureFlagsWithContext<Ctx> & string>(
    flag: K,
  ): Promise<InferFeatureFlagsWithContext<Ctx>[K]>;
  get(flag: string): Promise<unknown>;
  forContext(ctx: SpanContextWithoutFf<Ctx>): GeneratedEvaluatorInstance<Ctx>;
  readonly evaluator: FlagEvaluator<Ctx>;
}

export type GeneratedEvaluatorInstance<Ctx extends OpContext> = GeneratedEvaluatorBase<Ctx> &
  InferFeatureFlagsWithContext<Ctx>;

type GeneratedEvaluatorConstructor<Ctx extends OpContext> = new (
  spanContext: SpanContextWithoutFf<Ctx>,
  evaluator: FlagEvaluator<Ctx>,
) => GeneratedEvaluatorInstance<Ctx>;

type ValidateFlagValue = (value: unknown, schema: Schema<unknown, unknown>, defaultValue: unknown) => unknown;

type FeatureFlagDefinition = FeatureFlagSchema[string];

type WorkerSafeEvaluatorInstance = WorkerSafeGeneratedEvaluator<OpContext>;

type EvaluatorRuntimeContext<Ctx extends OpContext> = SpanContextWithoutFf<Ctx> & {
  _buffer: AnySpanBuffer;
  log: SpanLoggerInternal<Ctx['logSchema']>;
};

/**
 * Cache for generated evaluator classes by schema reference.
 */
const evaluatorClassCache = new WeakMap<FeatureFlagSchema, unknown>();

function isGeneratedEvaluatorConstructor<Ctx extends OpContext>(
  value: unknown,
): value is GeneratedEvaluatorConstructor<Ctx> {
  return typeof value === 'function';
}

function isGeneratedEvaluatorInstance<Ctx extends OpContext>(value: unknown): value is GeneratedEvaluatorInstance<Ctx> {
  return typeof value === 'object' && value !== null;
}

function isAnySpanBuffer(value: unknown): value is AnySpanBuffer {
  return typeof value === 'object' && value !== null && '_writeIndex' in value && 'entry_type' in value;
}

function isInternalSpanLogger<T extends OpContext['logSchema']>(value: unknown): value is SpanLoggerInternal<T> {
  return typeof value === 'object' && value !== null && 'ffAccess' in value && 'ffUsage' in value;
}

function getEvaluatorRuntimeContext<Ctx extends OpContext>(
  ctx: SpanContextWithoutFf<Ctx>,
): EvaluatorRuntimeContext<Ctx> {
  if (typeof ctx !== 'object' || ctx === null || !('_buffer' in ctx) || !('log' in ctx)) {
    throw new Error('Feature-flag evaluator requires an internal span context');
  }

  if (!isAnySpanBuffer(ctx._buffer) || !isInternalSpanLogger<Ctx['logSchema']>(ctx.log)) {
    throw new Error('Feature-flag evaluator requires runtime span buffer and logger internals');
  }

  return {
    ...ctx,
    _buffer: ctx._buffer,
    log: ctx.log,
  };
}

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
  readonly #spanContext: EvaluatorRuntimeContext<Ctx>;
  readonly #evaluator: FlagEvaluator<Ctx>;
  readonly #schema: Ctx['flags'];
  readonly #validateFlagValue: ValidateFlagValue;
  readonly #entryTypeFfAccess: number;

  constructor(
    spanContext: SpanContextWithoutFf<Ctx>,
    evaluator: FlagEvaluator<Ctx>,
    schema: Ctx['flags'],
    validateFlagValue: ValidateFlagValue,
    entryTypeFfAccess: number,
  ) {
    this.#spanContext = getEvaluatorRuntimeContext(spanContext);
    this.#evaluator = evaluator;
    this.#schema = schema;
    this.#validateFlagValue = validateFlagValue;
    this.#entryTypeFfAccess = entryTypeFfAccess;
  }

  get evaluator(): FlagEvaluator<Ctx> {
    return this.#evaluator;
  }

  protected hasLoggedAccess(flagName: string): boolean {
    let buf: AnySpanBuffer | undefined = this.#spanContext._buffer;

    while (buf) {
      const limit = buf._writeIndex;
      for (let i = limit - 1; i >= 0; i--) {
        if (buf.entry_type[i] === this.#entryTypeFfAccess && buf.message_values && buf.message_values[i] === flagName) {
          return true;
        }
      }
      buf = buf._overflow;
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

  forContext(_ctx: SpanContextWithoutFf<Ctx>): GeneratedEvaluatorInstance<Ctx> {
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
    return this.#spanContext.log;
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
  if (isGeneratedEvaluatorConstructor<Ctx>(cached)) {
    return cached;
  }

  class GeneratedEvaluator extends WorkerSafeGeneratedEvaluator<Ctx> {
    constructor(spanContext: SpanContextWithoutFf<Ctx>, evaluator: FlagEvaluator<Ctx>) {
      super(spanContext, evaluator, schema, validateFlagValue, ENTRY_TYPE_FF_ACCESS);
    }

    override forContext(ctx: SpanContextWithoutFf<Ctx>): GeneratedEvaluatorInstance<Ctx> {
      const nextEvaluator: unknown = new GeneratedEvaluator(ctx, this.evaluator);
      if (!isGeneratedEvaluatorInstance<Ctx>(nextEvaluator)) {
        throw new Error('Failed to create feature-flag evaluator instance');
      }
      return nextEvaluator;
    }
  }

  Object.defineProperties(GeneratedEvaluator.prototype, buildGetterDescriptors(Object.keys(schema)));
  const generatedEvaluatorClass: unknown = GeneratedEvaluator;
  evaluatorClassCache.set(schema, generatedEvaluatorClass);

  if (!isGeneratedEvaluatorConstructor<Ctx>(generatedEvaluatorClass)) {
    throw new Error('Failed to generate feature-flag evaluator constructor');
  }

  return generatedEvaluatorClass;
}
