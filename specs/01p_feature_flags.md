# Feature Flag System

## Overview

The feature flag system provides type-safe, high-performance configuration and attribute management for dynamic
behavior. It integrates deeply with the trace logging system to provide:

1.  **Type Safety**: Full TypeScript inference for flag values
2.  **Performance**: Cached evaluation with zero-allocation hot paths
3.  **Analytics**: Automatic tracking of flag access and usage
4.  **Context Awareness**: Evaluation based on span context (user, region, etc.)

## Schema Definition

**Purpose**: Define feature flags with type-safe access and explicit analytics tracking.

### Defining Flags

```typescript
const featureFlags = defineFeatureFlags({
  // Boolean flags
  darkMode: S.boolean().default(false),
  advancedValidation: S.boolean().default(false),

  // Numeric flags (e.g., limits, thresholds)
  maxItems: S.number().default(100),
  rateLimit: S.number().default(1000),

  // String flags using enum (known variants at compile time)
  buttonColor: S.enum(['blue', 'green', 'red']).default('blue'),

  // String flags using category (repeated values, runtime interning)
  experimentGroup: S.category().default('control'),
});
```

### Flag Access: Returns Just the Value

Flag access returns the **primitive value directly** - no wrapper objects. This is optimal for V8 hidden classes and
provides the simplest mental model.

```typescript
// Access flag values - returns the primitive directly
const enabled = ctx.ff.darkMode; // boolean
const maxItems = ctx.ff.maxItems; // number
const color = ctx.ff.buttonColor; // 'blue' | 'green' | 'red'

// Natural JavaScript usage
if (ctx.ff.darkMode) {
  applyDarkTheme();
}

// Use numeric flags directly
const items = fetchItems({ limit: ctx.ff.maxItems });
```

### Tracking: Via ctx.ff.track() Method

Tracking is **separate from access**. Use `ctx.ff.track('flagName')` to record usage analytics. The `track()` method
returns a chainable API just like `ctx.tag` and `ctx.log.info()`.

```typescript
// Track flag usage with chainable attributes
ctx.ff.track('darkMode').variant(ctx.ff.darkMode);

// Typical pattern: check flag, then track with user-defined attributes
if (ctx.ff.darkMode) {
  ctx.ff.track('darkMode');
  applyDarkTheme();
}

// Track with multiple user-defined attributes (chainable)
ctx.ff.track('buttonColor').variant(ctx.ff.buttonColor);

// Track numeric flag usage with user-defined attributes
ctx.ff.track('maxItems').requested(100).returned(ctx.ff.maxItems);
```

### Track Uses Same Schema as Tag

The `track()` method returns a chainable API that writes to the **same columns** as `ctx.tag`. There is no separate
tracking schema - this keeps the Arrow table structure unified and avoids column name collisions.

```typescript
// Your logSchema (user-defined)
const logSchema = {
  variant: S.category(), // flag variant value
  userId: S.category(),
  duration: S.number(),
  requested: S.number(),
  returned: S.number(),
};
```

## Context Flow & Integration

### Scope Values for Flag Evaluation

Feature flag evaluation reads context from two sources:

1. **Scope Attributes** (`ctx.scope`): Column values set via `setScope()` - userId, region, etc.
2. **Context Properties** (`ctx.*`): Runtime context properties - userId, userPlan, etc.

The evaluator receives the full SpanContext (minus `ff`) and can access both:

```typescript
flagEvaluator: async (ctx, flag, defaultValue) => {
  // Access scope values (column data)
  const region = ctx.scope.region;

  // Access context properties (runtime context)
  const userId = ctx.userId;
  const userPlan = ctx.userPlan;

  return launchDarkly.variation(flag, defaultValue, { userId, userPlan, region });
};
```

### Context Flow Diagram

```
Request Boundary                    Middleware                         Flag Evaluation
─────────────────────────────────────────────────────────────────────────────────────────

trace('request', {                  ctx.setScope({ region })          ctx.ff.premiumFeatures
  userId,                                     │                                  │
  userPlan,                                   ▼                                  ▼
}, handleRequest)                ┌─────────────────────────┐         ┌─────────────────────┐
        │                        │ SpanContext             │         │ flagEvaluator(ctx)  │
        ▼                        │                         │         │                     │
┌─────────────────┐              │ ctx.userId (prop)       │────────▶│ ctx.userId          │
│ trace() creates │              │ ctx.scope.region (col)  │         │ ctx.scope.region    │
│ root SpanContext│              │                         │         │ ctx.log.debug(...)  │
│ userId: 'u123'  │──────────────│ ff: uses flagEvaluator  │         │ ctx.span(...)       │
│ userPlan: 'pro' │              │     (Omit<ctx, 'ff'>)   │         │                     │
└─────────────────┘              └─────────────────────────┘         └─────────────────────┘
```

**Why the evaluator receives `Omit<SpanContext, 'ff'>`:**

- **Prevents infinite recursion**: Evaluator can't call `ctx.ff` (which would call evaluator again)
- **Full context access**: Can read userId, userPlan, scope, env, deps - everything except ff
- **Can log and span**: Create child spans for external calls, log debug info
- **Type-safe**: TypeScript ensures you can't accidentally access `ctx.ff` in evaluator

## Evaluator Implementation

**Purpose**: Handle feature flag evaluation with full span context for logging, tracing, and analytics.

### Ctx-First Evaluator Pattern

The evaluator receives `Omit<SpanContext, 'ff'>` - full context access except `ff` itself. This enables:

1. **Logging** via `ctx.log.debug()`, `ctx.log.info()`, etc.
2. **Child spans** via `ctx.span()` for tracing external flag service calls
3. **Context properties** via `ctx.userId`, `ctx.userPlan`, etc.
4. **Scope values** via `ctx.scope.region`, etc.
5. **Environment** via `ctx.env` for environment-specific evaluation

### Evaluator Interface

```typescript
/**
 * SpanContext without 'ff' field - prevents infinite recursion in evaluators
 */
type SpanContextWithoutFf<T, FF, Env> = Omit<SpanContext<T, FF, Env>, 'ff'>;

/**
 * Feature flag evaluator interface.
 *
 * Receives SpanContext (minus 'ff') as FIRST parameter for consistency with ctx-first pattern.
 * This enables evaluators to log, create child spans, and access scope/env during evaluation.
 */
interface FlagEvaluator<T extends LogSchema, FF extends FeatureFlagSchema, Env> {
  /**
   * Get a flag value synchronously (for cached/static flags)
   * @param ctx - SpanContext without 'ff' (can log, create spans, access scope)
   * @param flag - Flag name to evaluate
   */
  getSync<K extends string>(ctx: SpanContextWithoutFf<T, FF, Env>, flag: K): FlagValue;

  /**
   * Get a flag value asynchronously (for dynamic flags)
   * @param ctx - SpanContext without 'ff' (can log, create spans, access scope)
   * @param flag - Flag name to evaluate
   */
  getAsync<K extends string>(ctx: SpanContextWithoutFf<T, FF, Env>, flag: K): Promise<FlagValue>;

  /**
   * Create span-bound accessor from span context.
   * Returns a FeatureFlagEvaluator with typed getters for each flag.
   *
   * MUST create a new instance per span because JS getters (`get darkMode()`)
   * cannot receive parameters - the span context must be stored at construction.
   *
   * Receives `Omit<SpanContext, 'ff'>` to prevent infinite recursion.
   */
  forContext(ctx: SpanContextWithoutFf<T, FF, Env>): FeatureFlagEvaluator<FF, T, Env>;
}
```

**Key Design: ctx-first for consistency**

The evaluator receives `ctx` as the first parameter in `getSync(ctx, flag)` and `getAsync(ctx, flag)`. This follows the
ctx-first pattern used throughout the codebase and enables:

- **Logging**: `ctx.log.debug('Evaluating flag')`
- **Child spans**: `await ctx.span('launchDarkly', async (child) => { ... })`
- **Scope access**: `ctx.scope.userId`, `ctx.scope.region`
- **Environment**: `ctx.env.LD_SDK_KEY`
- **Dependencies**: `ctx.deps.http.request(...)`

### Example: LaunchDarkly Integration

```typescript
import { FlagEvaluator, FeatureFlagEvaluator, SpanContextWithoutFf } from '@smoothbricks/lmao';

/**
 * LaunchDarkly evaluator implementing FlagEvaluator interface.
 *
 * Simple evaluators can often just return `this` from forContext since
 * getSync/getAsync receive the correct span's context on each call.
 */
class LaunchDarklyEvaluator implements FlagEvaluator<AppSchema, AppFlags, Env> {
  constructor(
    private ldClient: LaunchDarkly.LDClient,
    private schema: AppFlags
  ) {}

  /**
   * Sync evaluation - returns cached value or default
   */
  getSync(ctx: SpanContextWithoutFf<AppSchema, AppFlags, Env>, flag: string) {
    // Can log during evaluation!
    ctx.log.debug(`Sync eval: ${flag}`);

    // Use LaunchDarkly's sync variation (requires flag to be pre-cached)
    return this.ldClient.variationSync(
      flag,
      {
        key: ctx.scope.userId ?? 'anonymous',
        custom: { plan: ctx.scope.userPlan },
      },
      this.schema[flag]?.defaultValue
    );
  }

  /**
   * Async evaluation - can make network calls with full observability
   */
  async getAsync(ctx: SpanContextWithoutFf<AppSchema, AppFlags, Env>, flag: string) {
    // Can create child spans for external calls!
    return ctx.span('launchDarkly', async (child) => {
      child.tag.flag(flag);

      try {
        const value = await this.ldClient.variation(
          flag,
          {
            key: ctx.scope.userId ?? 'anonymous',
            custom: { plan: ctx.scope.userPlan, region: ctx.scope.region },
          },
          this.schema[flag]?.defaultValue
        );

        child.tag.value(String(value));
        return child.ok(value).value;
      } catch (error) {
        child.log.error('LaunchDarkly evaluation failed');
        return child.err('LD_ERROR').value ?? this.schema[flag]?.defaultValue;
      }
    });
  }

  /**
   * Create span-bound accessor. Simple evaluators can return `this` since
   * getSync/getAsync receive the correct ctx on each call.
   */
  forContext(ctx: SpanContextWithoutFf<AppSchema, AppFlags, Env>) {
    // Create wrapper that provides typed flag getters
    return new FeatureFlagEvaluator(this.schema, ctx as SpanContext, this);
  }
}

// Usage with defineOpContext and Tracer
const opContext = defineOpContext({
  logSchema: appSchema,
  flags: defineFeatureFlags({
    premiumFeatures: S.boolean().default(false).async(),
    maxRetries: S.number().default(3).sync(),
    experimentGroup: S.category().default('control').async(),
  }),
  ctx: {
    env: null as Env,
    userId: undefined as string | undefined,
    userPlan: undefined as 'free' | 'pro' | 'enterprise' | undefined,
  },
});

// Pass the evaluator to Tracer via TracerOptions
const { trace } = new TestTracer(opContext, {
  flagEvaluator: new LaunchDarklyEvaluator(ldClient, opContext.flags.schema),
});
```

### Why Each Span Gets Its Own Evaluator Instance

The `forContext()` method creates a **new evaluator instance per span**. This is required because:

1. **JS getters can't receive parameters**: `ctx.ff.darkMode` is a getter - it has no way to receive `ctx`
2. **Instance stores `#spanContext`**: The generated class stores the span context at construction time
3. **Per-span deduplication**: Each instance tracks which flags were accessed in THIS span (for ff-access deduplication)

This allocation is acceptable because:

- Span creation is already a cold-path allocation
- The object is small (just references to ctx, evaluator, schema)
- V8 is very efficient at allocating small objects with stable hidden classes
- The generated class is cached - only the instance is new

```typescript
// Generated class structure (simplified)
class GeneratedEvaluator {
  #spanContext; // Stored at construction - getters use this
  #evaluator; // Shared across all spans
  #schema; // Shared across all spans

  get darkMode() {
    // Getter has no parameters - must use stored #spanContext
    return this.#getFlag('darkMode');
  }

  #getFlag(flagName) {
    const ctx = this.#spanContext; // Uses stored context
    const value = this.#evaluator.getSync(ctx, flagName);
    // ... deduplication, logging, wrapping
  }

  forContext(ctx) {
    // Must create new instance - can't reuse this with different ctx
    return new GeneratedEvaluator(ctx, this.#evaluator);
  }
}
```

**Note**: `getSync(ctx, flag)` and `getAsync(ctx, flag)` receive ctx as a parameter so that advanced evaluators (e.g.,
LaunchDarkly) can use `ctx.log`, `ctx.scope`, `ctx.span()` during evaluation. The ctx passed to these methods comes from
the stored `#spanContext`.

### Why `ff` is Omitted from Evaluator Context

The evaluator receives `Omit<SpanContext, 'ff'>` to **prevent infinite recursion**:

```typescript
// ❌ BAD: If evaluator had access to ctx.ff, this would infinite loop
flagEvaluator: async (ctx, flag, defaultValue) => {
  if (ctx.ff.someOtherFlag) {  // This calls flagEvaluator again!
    // ...infinite recursion
  }
};

// ✅ GOOD: ctx.ff is omitted, TypeScript prevents this mistake
flagEvaluator: async (ctx, flag, defaultValue) => {
  // ctx.ff doesn't exist here - TypeScript error if you try to access it
  ctx.log.debug('Safe to log');
  return ctx.span('external', async () => { ... }); // Safe to create spans
};
```

### Evaluator Capabilities Summary

| Capability         | Available | Example                                    |
| ------------------ | --------- | ------------------------------------------ |
| Logging            | ✅        | `ctx.log.debug('Evaluating')`              |
| Child spans        | ✅        | `ctx.span('launchDarkly', async () => {})` |
| Context properties | ✅        | `ctx.userId`, `ctx.userPlan`               |
| Scope values       | ✅        | `ctx.scope.region`                         |
| Environment        | ✅        | `ctx.env.LD_SDK_KEY`                       |
| Dependencies       | ✅        | `ctx.deps.http.request(...)`               |
| Other flags        | ❌        | `ctx.ff` is omitted (prevents recursion)   |

### Access Deduplication

**One `ff-access` per span**: The first access to a flag logs an `ff-access` entry. Subsequent accesses in the same span
do not log duplicate entries.

**Implementation**: The evaluator scans the buffer chain backward to check if a flag was already logged.

**Why buffer scan instead of Set?**

- **No extra allocations**: SpanBuffers are small, scanning is cheap
- **Common case optimized**: Single buffer (overflow self-tunes to rare), just scan backward
- **Handles overflow**: Chain walk covers all buffers in the span

**Note**: `ff-usage` entries (via `track()`) are **always logged** - they track when flags influence code paths.

**scopeValues is Flat for Single Hidden Class**

```typescript
// GOOD: Flat structure - single hidden class
// scopeValues is a plain object with flat properties
buffer.scopeValues = {
  userId: 'user-123',
  region: 'us-east-1',
  userPlan: 'premium',
  // ... other flat properties
};
```

## Schema Integration Patterns

### InMemoryFlagEvaluator for Testing/Bootstrap

For tests or bootstrap scenarios, use the built-in `InMemoryFlagEvaluator`:

```typescript
import { InMemoryFlagEvaluator, defineFeatureFlags, S } from '@smoothbricks/lmao';

const flags = defineFeatureFlags({
  darkMode: S.boolean().default(false).sync(),
  maxItems: S.number().default(100).sync(),
});

// Create evaluator with initial flag values
const evaluator = new InMemoryFlagEvaluator(flags.schema, {
  darkMode: true, // Override default
  maxItems: 50, // Override default
});

const opContext = defineOpContext({
  logSchema: appSchema,
  flags,
  ctx: { env: null as Env },
});

// Pass the evaluator to Tracer via TracerOptions
const { trace } = new TestTracer(opContext, { flagEvaluator: evaluator });

// In tests, you can update flag values dynamically
evaluator.setFlag('darkMode', false);
```

**InMemoryFlagEvaluator implementation pattern:**

```typescript
class InMemoryFlagEvaluator<T, FF, Env> implements FlagEvaluator<T, FF, Env> {
  private flags: Record<string, FlagValue>;

  constructor(
    private schema: FF,
    initialFlags: Record<string, FlagValue> = {}
  ) {
    this.flags = initialFlags;
  }

  // Simple: ignores ctx, just returns stored value
  getSync(_ctx: SpanContextWithoutFf<T, FF, Env>, flag: string) {
    return this.flags[flag] ?? null;
  }

  // Simple: ignores ctx, just returns stored value
  async getAsync(_ctx: SpanContextWithoutFf<T, FF, Env>, flag: string) {
    return this.flags[flag] ?? null;
  }

  // Returns FeatureFlagEvaluator wrapper with typed getters
  forContext(ctx: SpanContextWithoutFf<T, FF, Env>) {
    return new FeatureFlagEvaluator(this.schema, ctx as SpanContext, this);
  }

  // Test helper to update flags at runtime
  setFlag(flag: string, value: FlagValue) {
    this.flags[flag] = value;
  }
}
```

### Op Integration with Feature Flags

```typescript
const fetchUser = defineOp('fetchUser', async (ctx, id: string) => {
  // Access flags - evaluator is called with ctx (minus ff)
  const useNewApi = await ctx.ff.useNewUserApi.get(); // async flag

  if (useNewApi) {
    // Track that this flag influenced behavior
    ctx.ff.useNewUserApi.track();

    return ctx.span('newApi', async (child) => {
      const user = await newUserService.fetch(id);
      return child.ok(user);
    });
  }

  // Original path
  const user = await legacyUserService.fetch(id);
  return ctx.ok(user);
});
```

### Request Handler Example

```typescript
// With Tracer from defineOpContext
const { trace } = new TestTracer(opContext);

export default {
  async fetch(req: Request, env: Env) {
    return trace(
      'request',
      {
        env,
        userId: req.headers.get('x-user-id') ?? undefined,
        userPlan: await getUserPlan(req), // Available to flagEvaluator
      },
      async (ctx) => {
        ctx.setScope({ request_id: crypto.randomUUID() });

        // Flag evaluation has access to ctx.userId, ctx.userPlan, ctx.scope.request_id
        if (await ctx.ff.maintenanceMode.get()) {
          return new Response('Service under maintenance', { status: 503 });
        }

        return handleRequest(ctx, req);
      }
    );
  },
};
```

## Performance Characteristics

- **First access**: Evaluator call + ff-access log (~0.1ms for sync, varies for async)
- **Subsequent access**: Cached value (~0.01ms, no log, no evaluator call)
- **track() call**: Direct buffer write + chainable methods (~0.05ms)
- **Analytics**: Deduped ff-access per span, explicit ff-usage via `track()`
- **V8 Optimized**: Returns primitives, no wrapper objects

## Complete Example

```typescript
import {
  defineOpContext,
  defineLogSchema,
  S,
  defineFeatureFlags,
  FlagEvaluator,
  FeatureFlagEvaluator,
  SpanContextWithoutFf,
} from '@smoothbricks/lmao';
import LaunchDarkly from 'launchdarkly-node-server-sdk';

// Initialize LaunchDarkly client
const ldClient = LaunchDarkly.init(process.env.LD_SDK_KEY!);
await ldClient.waitForInitialization();

// Define schema and flags
const appSchema = defineLogSchema({
  user_id: S.category(),
  flag: S.category(),
  value: S.category(),
});

const flags = defineFeatureFlags({
  premiumFeatures: S.boolean().default(false).async(),
  maxUploadSize: S.number().default(10).sync(),
  experimentGroup: S.category().default('control').async(),
});

type AppSchema = typeof appSchema;
type AppFlags = typeof flags.schema;

// Implement FlagEvaluator interface
class AppFlagEvaluator implements FlagEvaluator<AppSchema, AppFlags, Env> {
  constructor(private ldClient: LaunchDarkly.LDClient) {}

  getSync(ctx: SpanContextWithoutFf<AppSchema, AppFlags, Env>, flag: string) {
    // Sync: return cached value (LaunchDarkly caches after first async fetch)
    ctx.log.debug(`Sync eval: ${flag}`);
    return this.ldClient.variationSync(
      flag,
      {
        key: ctx.scope.userId ?? 'anonymous',
      },
      flags.schema[flag]?.defaultValue
    );
  }

  async getAsync(ctx: SpanContextWithoutFf<AppSchema, AppFlags, Env>, flag: string) {
    // Async: full tracing of external call
    return ctx.span('launchDarkly', async (child) => {
      child.tag.flag(flag);

      const value = await this.ldClient.variation(
        flag,
        {
          key: ctx.scope.userId ?? 'anonymous',
          custom: {
            plan: ctx.scope.userPlan,
            region: ctx.scope.region,
          },
        },
        flags.schema[flag]?.defaultValue
      );

      child.tag.value(String(value));
      return child.ok(value).value;
    });
  }

  forContext(ctx: SpanContextWithoutFf<AppSchema, AppFlags, Env>) {
    return new FeatureFlagEvaluator(flags.schema, ctx as SpanContext, this);
  }
}

// Create op context - flags are defined here, evaluator is passed to Tracer
const opContext = defineOpContext({
  logSchema: appSchema,
  flags,
  ctx: {
    env: null as Env,
    userId: undefined as string | undefined,
    userPlan: undefined as 'free' | 'pro' | 'enterprise' | undefined,
  },
});

const { defineOp } = opContext;

// Define ops that use feature flags
const processUpload = defineOp('processUpload', async (ctx, file: File) => {
  // Sync flag access - returns { value, track() } | undefined
  const maxSizeFlag = ctx.ff.maxUploadSize;
  const maxSize = maxSizeFlag?.value ?? 10;

  if (file.size > maxSize * 1024 * 1024) {
    return ctx.err('FILE_TOO_LARGE', { maxSize });
  }

  // Async flag access
  const premiumFlag = await ctx.ff.get('premiumFeatures');
  if (premiumFlag) {
    premiumFlag.track(); // Record that flag influenced behavior
    await processPremiumUpload(ctx, file);
  } else {
    await processStandardUpload(ctx, file);
  }

  return ctx.ok({ uploaded: true });
});

// Usage at request boundary - pass evaluator to Tracer via TracerOptions
const tracer = new TestTracer(opContext, {
  flagEvaluator: new AppFlagEvaluator(ldClient),
});

export default {
  async fetch(req: Request, env: Env) {
    return tracer.trace(
      'upload',
      { env, userId: req.headers.get('x-user-id') ?? undefined, userPlan: await getUserPlan(req) },
      processUpload,
      await req.blob()
    );
  },
};
```
