import * as S from '@sury/sury';
import type {
  AsyncFlagKeys,
  EvaluationContext,
  FeatureFlagSchema,
  InferFeatureFlags,
  UsageContext,
} from './defineFeatureFlags.js';

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

/**
 * Feature flag evaluator with span-aware logging
 *
 * This class provides type-safe access to feature flags with automatic
 * analytics tracking. Sync flags are exposed as direct properties for
 * zero-overhead access, while async flags use methods.
 */
export class FeatureFlagEvaluator<T extends FeatureFlagSchema> {
  protected schema: T;
  protected context: EvaluationContext;
  protected evaluator: FlagEvaluator;
  protected columnWriters?: FlagColumnWriters;

  constructor(schema: T, context: EvaluationContext, evaluator: FlagEvaluator, columnWriters?: FlagColumnWriters) {
    this.schema = schema;
    this.context = context;
    this.evaluator = evaluator;
    this.columnWriters = columnWriters;

    // Initialize sync flags as direct properties
    this.initializeSyncFlags();
  }

  /**
   * Initialize sync flags as getter properties
   *
   * This allows sync flags to be accessed as direct properties:
   * ctx.ff.debugMode instead of ctx.ff.get('debugMode')
   */
  protected initializeSyncFlags(): void {
    for (const [key, definition] of Object.entries(this.schema)) {
      if (definition.evaluationType === 'sync') {
        Object.defineProperty(this, key, {
          get: () => {
            const rawValue = this.evaluator.getSync(key, this.context);
            const value = validateFlagValue(rawValue, definition.schema, definition.defaultValue);

            // Log flag access for analytics
            if (this.columnWriters) {
              this.columnWriters.writeEntryType('ff-access');
              this.columnWriters.writeFfName(key);
              this.columnWriters.writeFfValue(rawValue);
              this.columnWriters.writeContextAttributes(this.context);
            }

            return value;
          },
          enumerable: true,
          configurable: true,
        });
      }
    }
  }

  /**
   * Get async flag value
   *
   * Async flags must be accessed via this method:
   * await ctx.ff.get('userSpecificLimit')
   */
  async get<K extends AsyncFlagKeys<T>>(flag: K): Promise<InferFeatureFlags<T>[K]> {
    const definition = this.schema[flag];
    const rawValue = await this.evaluator.getAsync(flag as string, this.context);
    const value = validateFlagValue(rawValue, definition.schema, definition.defaultValue);

    // Log flag access for analytics
    if (this.columnWriters) {
      this.columnWriters.writeEntryType('ff-access');
      this.columnWriters.writeFfName(flag as string);
      this.columnWriters.writeFfValue(rawValue);
      this.columnWriters.writeContextAttributes(this.context);
    }

    return value as InferFeatureFlags<T>[K];
  }

  /**
   * Track feature flag usage for A/B testing analytics
   *
   * This logs when a flag actually affects behavior, not just when it's accessed.
   * For example, after performing an action enabled by a flag.
   */
  trackUsage<K extends keyof T>(flag: K, context?: UsageContext): void {
    if (this.columnWriters) {
      this.columnWriters.writeEntryType('ff-usage');
      this.columnWriters.writeFfName(flag as string);
      this.columnWriters.writeAction(context?.action);
      this.columnWriters.writeOutcome(context?.outcome);
      this.columnWriters.writeContextAttributes(this.context);
    }
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
