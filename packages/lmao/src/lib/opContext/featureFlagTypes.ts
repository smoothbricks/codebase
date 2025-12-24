/**
 * Feature Flag Types for Op-Centric API
 *
 * This module contains type definitions for feature flags used in the Op-centric API.
 * Feature flags allow for runtime toggling of functionality with type-safe evaluation.
 *
 * Key concepts:
 * - FeatureFlagSchema: Defines what flags exist and their types
 * - BoundFeatureFlags: Typed accessor for flag values bound to a span context
 *
 * For the FlagEvaluator interface (getSync/getAsync/forContext), see:
 * - packages/lmao/src/lib/schema/evaluator.ts
 */

import type { FeatureFlagDefinition } from '../schema/types.js';

// =============================================================================
// FEATURE FLAG TYPES
// =============================================================================

/**
 * Feature flag schema - defines what flags are available and their types
 */
export interface FeatureFlagSchema {
  [key: string]: FeatureFlagDefinition<string | number | boolean>;
}

/**
 * Feature flag accessor bound to a span context
 * Provides typed getters for each defined flag
 *
 * Access patterns:
 * - Sync flags: `ctx.ff.myFlag` returns `{ value: T, track(): void } | undefined`
 * - Async flags: `await ctx.ff.get('myFlag')` returns `{ value: T, track(): void } | undefined`
 *
 * Returns `undefined` when flag is false/disabled.
 * Returns wrapper object when flag is truthy/enabled, with `.track()` for usage analytics.
 */
export type BoundFeatureFlags<FF extends FeatureFlagSchema> = {
  readonly [K in keyof FF]: FF[K] extends FeatureFlagDefinition<infer V, 'sync'>
    ? { value: V; track(): void } | undefined
    : FF[K] extends FeatureFlagDefinition<infer V, 'async'>
      ? { value: V; track(): void } | undefined
      : never;
} & {
  /** Get async flag value. Returns undefined when false, FlagContext when truthy. */
  get(flag: string): Promise<{ value: unknown; track(): void } | undefined>;
  /** Track flag usage. Prefer using flag.track() for the fluent API. */
  trackUsage<K extends keyof FF>(flag: K, context?: { action?: string; outcome?: string }): void;
};
