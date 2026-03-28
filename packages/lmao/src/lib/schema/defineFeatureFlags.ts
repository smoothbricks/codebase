import type { Output } from '@smoothbricks/arrow-builder';
import type { FeatureFlagDefinition } from './types.js';

/**
 * Feature flag schema with sync/async markers
 */
export interface FeatureFlagSchema {
  [key: string]: FeatureFlagDefinition<string | number | boolean>;
}

type DefinedFeatureFlags<T extends FeatureFlagSchema> = {
  schema: T;
  syncFlags: SyncFlagKeys<T>[];
  asyncFlags: AsyncFlagKeys<T>[];
  readonly type?: InferFeatureFlags<T>;
};

function isSyncFlagKey<T extends FeatureFlagSchema>(schema: T, key: keyof T): key is SyncFlagKeys<T> {
  return schema[key].evaluationType === 'sync';
}

function isAsyncFlagKey<T extends FeatureFlagSchema>(schema: T, key: keyof T): key is AsyncFlagKeys<T> {
  return schema[key].evaluationType === 'async';
}

/**
 * Define feature flags with type-safe access patterns
 *
 * @param schema - Object mapping flag names to flag definitions
 * @returns Feature flag definition object for use with evaluator
 */
export function defineFeatureFlags<T extends FeatureFlagSchema>(schema: T): DefinedFeatureFlags<T> {
  const syncFlags: SyncFlagKeys<T>[] = [];
  const asyncFlags: AsyncFlagKeys<T>[] = [];

  for (const key in schema) {
    if (isSyncFlagKey(schema, key)) {
      syncFlags.push(key);
    } else if (isAsyncFlagKey(schema, key)) {
      asyncFlags.push(key);
    }
  }

  return {
    schema,
    syncFlags,
    asyncFlags,
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
  [K in keyof T]: Output<T[K]['schema']>;
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
