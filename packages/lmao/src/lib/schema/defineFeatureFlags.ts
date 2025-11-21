import type * as Sury from '@sury/sury';
import type { FeatureFlagDefinition } from './types.js';

/**
 * Feature flag schema with sync/async markers
 */
export interface FeatureFlagSchema {
  [key: string]: FeatureFlagDefinition<string | number | boolean>;
}

/**
 * Define feature flags with type-safe access patterns
 *
 * @param schema - Object mapping flag names to flag definitions
 * @returns Feature flag definition object for use with evaluator
 */
export function defineFeatureFlags<T extends FeatureFlagSchema>(schema: T): {
  schema: T;
  syncFlags: SyncFlagKeys<T>[];
  asyncFlags: AsyncFlagKeys<T>[];
  type: InferFeatureFlags<T>;
} {
  const syncFlags: string[] = [];
  const asyncFlags: string[] = [];

  for (const [key, definition] of Object.entries(schema)) {
    if (definition.evaluationType === 'sync') {
      syncFlags.push(key);
    } else {
      asyncFlags.push(key);
    }
  }

  return {
    schema,
    syncFlags: syncFlags as SyncFlagKeys<T>[],
    asyncFlags: asyncFlags as AsyncFlagKeys<T>[],
    type: undefined as unknown as InferFeatureFlags<T>,
  };
}

/**
 * Extract sync flag keys
 */
export type SyncFlagKeys<T extends FeatureFlagSchema> = {
  [K in keyof T]: T[K]['evaluationType'] extends 'sync' ? K : never;
}[keyof T];

/**
 * Extract async flag keys
 */
export type AsyncFlagKeys<T extends FeatureFlagSchema> = {
  [K in keyof T]: T[K]['evaluationType'] extends 'async' ? K : never;
}[keyof T];

/**
 * Infer TypeScript types from feature flag schema
 */
export type InferFeatureFlags<T extends FeatureFlagSchema> = {
  [K in keyof T]: Sury.Output<T[K]['schema']>;
};

/**
 * Evaluation context for feature flag decisions
 */
export interface EvaluationContext {
  userId?: string;
  requestId?: string;
  userPlan?: string;
  region?: string;
  [key: string]: string | number | boolean | undefined;
}

/**
 * Usage tracking context for A/B testing analytics
 */
export interface UsageContext {
  action?: string;
  outcome?: 'success' | 'failure';
  value?: number;
  metadata?: Record<string, string | number | boolean>;
}
