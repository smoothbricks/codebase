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

// track() returns chainable API with SAME methods as ctx.tag
// Equivalent to:
// 1. Write entry_type = FF_USAGE
// 2. Write message = 'darkMode'  (unified message column - flag name)
// 3. Write user-defined attributes (from log schema)
```

## Context Flow & Integration

### Relationship to Scope Attributes

Feature flags use two distinct but related concepts for context management:

1.  **Scope Attributes** (`ctx.scope`): For automatic inclusion in log entries
2.  **Evaluation Context**: For flag decision-making

These can overlap but serve different purposes:

| Concern            | Scope Attributes                          | Evaluation Context           |
| ------------------ | ----------------------------------------- | ---------------------------- |
| **Purpose**        | Logging context                           | Flag decision-making         |
| **Applied to**     | All entries automatically                 | Flag evaluation only         |
| **Set via**        | `ctx.scope.key = value`                   | `forContext({ ... })`        |
| **Typical values** | userId, requestId, region                 | userId, userPlan, experiment |
| **Can overlap**    | Yes - derive FF context from scope values |                              |

### Scope-to-FF Integration via forContext()

**Key Design**: Feature flag evaluation automatically reads from `scopeValues` - no separate evaluation context needed.
The `forContext()` method creates a span-bound evaluator that reads evaluation context directly from
`spanContext.buffer.scopeValues`.

**Pattern: Root evaluator → span-bound evaluator**

```typescript
// Root context has FlagEvaluator (backend singleton)
const rootCtx = appModule.traceContext({
  ff: backendEvaluator, // Root evaluator (FlagEvaluator)
  env,
  requestId: 'req-123',
});

// Set scope values - automatically used for FF evaluation
ctx.setScope({ userId: 'user-123', region: 'us-east-1' });

// When creating a span, evaluator.forContext() creates span-bound accessor
await ctx.span('processUser', async (childCtx) => {
  // childCtx.ff is a FeatureFlagEvaluator with typed getters
  // Reads evaluation context from childCtx.buffer.scopeValues
  const { premiumFeatures } = childCtx.ff; // Evaluated WITH userId from scope!

  // Log entries include scope values automatically
  childCtx.log.info('Processing'); // Auto-includes userId, region from scope

  if (premiumFeatures) {
    premiumFeatures.track({ action: 'feature_used' });
  }
});
```

**Why read from scopeValues?**

- **Single source of truth**: Scope values are the evaluation context
- **Automatic updates**: When `ctx.setScope()` is called, FF evaluation uses new values
- **No duplication**: No separate `evaluationContext` object to maintain
- **Zero allocation**: Direct property access, no copying

### Context Flow: How Scope Values Flow to FF Evaluation

```
Request Boundary                    Middleware                         Business Logic
─────────────────────────────────────────────────────────────────────────────────────────

module.traceContext()               ctx.setScope({ userId })          childCtx.ff.premiumFeatures
        │                                     │                                  │
        ▼                                     ▼                                  ▼
┌─────────────────┐              ┌─────────────────────────┐         ┌─────────────────────┐
│ TraceContext    │              │ SpanContext             │         │ FF Evaluation       │
│                 │              │                         │         │                     │
│ ff: FlagEval    │──forContext──│ ff: FeatureFlagEval     │────────▶│ reads scopeValues   │
│                 │  (spanCtx)    │ buffer.scopeValues      │         │ ✓ user targeting    │
│                 │              │ = { userId, region }    │         │ ✓ A/B tests         │
└─────────────────┘              └─────────────────────────┘         └─────────────────────┘
        │                                     │
        │                                     │
   Root evaluator                    Scope set, FF reads
   (backend singleton)               (after auth middleware)
```

**Pattern: Set scope, FF evaluation reads automatically**

```typescript
// Set scope at middleware - automatically used for FF evaluation AND logging
ctx.setScope({ userId: req.user?.id, region: req.region });

// When creating a span, evaluator.forContext() creates span-bound accessor
await ctx.span('processUser', async (childCtx) => {
  // childCtx.ff reads evaluation context from childCtx.buffer.scopeValues
  // Inherits userId, region from parent scope (immutable reference)
  const { premiumFeatures } = childCtx.ff; // Evaluated with userId + region!

  // Log entries include scope values automatically
  childCtx.log.info('Processing'); // Auto-includes userId, region from scope

  if (premiumFeatures) {
    premiumFeatures.track({ action: 'feature_used' });
  }
});
```

**Why scopeValues is the evaluation context?**

- **Single source**: Scope values ARE the evaluation context - no duplication
- **Automatic updates**: When `ctx.setScope()` is called, FF evaluation uses new values immediately
- **Zero allocation**: Direct property access, no copying or extraction
- **Inheritance**: Child spans inherit parent scope by reference (immutable, zero-cost)

## Evaluator Implementation

**Purpose**: Handle feature flag evaluation with full span context for logging, tracing, and analytics.

### Why a Singleton Evaluator?

The evaluator may need to:

1.  **Make network calls** to external flag services (LaunchDarkly, Split, Unleash)
2.  **Create child spans** to trace those network calls with timing/errors
3.  **Log debug info** during evaluation via `ctx.log`
4.  **Access ctx.env** for environment-specific evaluation

This requires full `SpanContext` access, not just a buffer reference. But we can't create the evaluator per-span
(expensive) or pass ctx to constructor (circular dependency: SpanContext has ff, ff needs SpanContext).

**Solution**: Singleton evaluator created at app startup, with `forContext(ctx)` method that returns a context-bound
accessor for each span.

### FeatureFlagEvaluator Implementation

```typescript
/**
 * Root evaluator (FlagEvaluator) - backend singleton created at module startup.
 * Holds backend client that caches flag values based on evaluation context.
 * Stateless - all per-span state lives in the FeatureFlagEvaluator returned by forContext().
 */
interface FlagEvaluator<T extends LogSchema, FF extends FeatureFlagSchema, Env> {
  /**
   * Get a flag value synchronously (for cached/static flags)
   */
  getSync<K extends string>(flag: K, context: Partial<InferSchema<T>>): FlagValue;

  /**
   * Get a flag value asynchronously (for dynamic flags)
   */
  getAsync<K extends string>(flag: K, context: Partial<InferSchema<T>>): Promise<FlagValue>;

  /**
   * Create span-bound accessor from span context.
   * Returns a FeatureFlagEvaluator with typed getters for each flag.
   * Receives fully typed SpanContext from the module.
   */
  forContext?(
    ctx: SpanContext<T, FF, Env>
  ): FeatureFlagEvaluator<FF, T, Env> & InferFeatureFlagsWithContext<FF>;
}

/**
 * Per-span evaluator (FeatureFlagEvaluator) returned by rootEvaluator.forContext(ctx).
 * Minimal state - just 2 private fields: #spanContext, #evaluator, #schema
 */
class GeneratedEvaluator<FF extends FeatureFlagSchema, T extends LogSchema, Env> {
  #spanContext: SpanContext<T, FF, Env>;
  #evaluator: FlagEvaluator<T, FF, Env>;
  #schema: FF;

  constructor(spanContext: SpanContext<T, FF, Env>, evaluator: FlagEvaluator<T, FF, Env>) {
    this.#spanContext = spanContext;
    this.#evaluator = evaluator;
    this.#schema = spanContext.buffer.module.ffSchema;
  }

  /**
   * Get flag value - reads evaluation context from scopeValues
   */
  get [K in keyof FF](): InferFlagType<FF[K]> | undefined {
    return this.#getFlag(K);
  }

  #getFlag(flagName: string): unknown {
    const config = this.schema[flagName];
    if (!config) {
      throw new Error(`Unknown flag: ${flagName}`);
    }

    // Read evaluation context from scopeValues
    const evaluationContext = this.#spanContext.buffer.scopeValues || {};

    // Always evaluate - backend caches based on evaluationContext
    const rawValue = this.#evaluator.getSync(flagName, evaluationContext);
    const value = validateFlagValue(rawValue, this.#schema[flagName]);

    // Deduplicate: only log first access per span (buffer chain scan)
    if (!this.#hasLoggedAccess(flagName)) {
      const log = this.#spanContext.log as SpanLoggerInternal<T>;
      log.ffAccess(flagName, rawValue); // Logger handles overflow automatically
    }

    return value ? this.#wrapValue(flagName, value) : undefined;
  }

  #hasLoggedAccess(flagName: string): boolean {
    const log = this.#spanContext.log;
    let buf = this.#spanContext._buffer; // Start from original buffer

    // Walk chain FORWARD via .next, scan each buffer BACKWARD from writeIndex
    while (buf) {
      const limit = buf === log._buffer ? log._writeIndex : buf.writeIndex;
      // Scan BACKWARD within each buffer (finds recent accesses faster)
      for (let i = limit - 1; i >= 0; i--) {
        if (
          buf._operations[i] === ENTRY_TYPE_FF_ACCESS &&
          buf._message_values[i] === flagName
        ) {
          return true;
        }
      }
      buf = buf.next; // Move to next buffer in chain
    }
    return false;
  }

  private createChainableTracker(idx: number): FlagTracker<Tag> {
    const buffer = this.ctx.buffer;
    const tracker = {} as FlagTracker<Tag>;

    // Generate methods from logSchema - SAME columns as ctx.tag
    for (const attrName of Object.keys(this.logSchema)) {
      const valuesName = `${attrName}_values`;
      (tracker as any)[attrName] = (value: unknown) => {
        if (buffer[valuesName]) {
          buffer[valuesName][idx] = serializeValue(value);
        }
        return tracker;
      };
    }

    return tracker;
  }
}
```

### Buffer Chain Scan for Deduplication

**One `ff-access` per span** (per spec): The first access to a flag logs an `ff-access` entry. Subsequent accesses in
the same span do not log duplicate entries.

**Implementation**: Instead of maintaining a `Set<string>` of accessed flags, the evaluator scans the buffer chain
backward to check if a flag was already logged.

**Why buffer scan instead of Set?**

- **No extra allocations**: SpanBuffers are small, scanning is cheap
- **Common case optimized**: Single buffer (overflow self-tunes to rare), just scan backward
- **Handles overflow**: Chain walk covers all buffers in the span
- **Backend caching**: Backend caches flag values based on `(flagName, scopeValues)` - no per-span value cache needed

**Note**: `ff-usage` entries are **always logged** (not deduplicated) - they track when flags influence code paths.

### V8 Optimization Considerations

The evaluator implementation is designed for V8's hidden class optimizations:

**Minimal State - Only 3 Private Fields**

```typescript
class GeneratedEvaluator {
  #spanContext; // Reference to SpanContext
  #evaluator; // Backend singleton (caches based on scopeValues)
  #schema; // Schema reference (for validation)

  // NO #flagCache - backend caches based on (flagName, scopeValues)
  // NO #accessedFlags - buffer chain scan for deduplication
}
```

**Creating Span-Bound Evaluators**

```typescript
// Root evaluator (created at module startup)
const rootEvaluator = new InMemoryFlagEvaluator();

// Each span gets a NEW evaluator instance (via forContext)
const spanEvaluator1 = rootEvaluator.forContext(spanCtx1);
const spanEvaluator2 = rootEvaluator.forContext(spanCtx2);

// Benefits:
// - Minimal state (just 2 references)
// - Stable hidden class (same shape for V8)
// - Backend caching (no per-span value cache)
// - Buffer scan for deduplication (no Set allocation)
```

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

### DefaultFlagValueClient: Bootstrap Evaluator

**Problem**: The external flag client (LaunchDarkly, Split, etc.) may need to make network calls to initialize and log
those calls, but logging needs `ctx`, which needs `ff`, which needs the client...

**Solution**: `DefaultFlagValueClient` that just returns schema defaults. No external dependencies, always available.

```typescript
/**
 * Returns default values from schema. No network calls, no dependencies.
 * Used for:
 * - Bootstrap/initialization before real client is ready
 * - Fallback if real client fails
 * - Tests that don't need a real flag service
 */
class DefaultFlagValueClient implements FlagEvaluationClient {
  constructor(private schema: FeatureFlagSchema) {}

  evaluate(flagName: string, ctx: SpanContext): boolean | number | string {
    const config = this.schema[flagName];
    if (!config) {
      throw new Error(`Unknown flag: ${flagName}`);
    }
    // Just return the default value from schema
    return config.defaultValue;
  }
}

// Always available - created synchronously, no async init needed
const defaultFfEvaluator = new FeatureFlagEvaluator(
  featureFlagSchema,
  logSchema,
  new DefaultFlagValueClient(featureFlagSchema)
);
```

### App Startup: Initialize Real Client with Tracing

```typescript
// Bootstrap context uses default evaluator - can log immediately
function createBootstrapContext(): SpanContext {
  const buffer = createSpanBuffer(...);
  const ctx: SpanContext = {
    traceId: generateTraceId(),
    requestId: 'bootstrap',
    // ...
    ff: defaultFfEvaluator.forContext(ctx), // Uses defaults - always works
    env: environmentConfig,
    // ...
  };
  return ctx;
}

// Initialize app WITH tracing - even the flag client init is traced!
async function initializeApp(): Promise<FeatureFlagEvaluator> {
  const bootstrapCtx = createBootstrapContext();

  // Trace the client initialization itself
  const ldClient = await bootstrapCtx.span('ff-client-init', async (ctx) => {
    ctx.tag.provider('launchdarkly');
    ctx.log.info('Connecting to LaunchDarkly');

    try {
      const client = new LaunchDarklyClient(process.env.LD_SDK_KEY);
      await client.waitForInitialization();

      ctx.tag.status('connected');
      ctx.log.info('LaunchDarkly ready');
      return ctx.ok(client);
    } catch (error) {
      ctx.tag.status('failed');
      ctx.log.error('LaunchDarkly connection failed');
      // Fall back to default client
      return ctx.ok(new DefaultFlagValueClient(featureFlagSchema));
    }
  });

  return new FeatureFlagEvaluator(featureFlagSchema, logSchema, ldClient.value);
}
```

### Trace Context Creation via Module

```typescript
// Create context at request boundary via module.traceContext()
// This provides type-safe Extra properties
const ctx = appModule.traceContext({
  ff: ffEvaluator,
  env: environmentConfig,
  requestId: 'req-123',
  userId: session?.userId,
});
```

### Op Integration

```typescript
export const processUserOp = op('processUser', async (ctx, user: User) => {
  // ctx.ff evaluates flags WITH userId and userPlan context from span creation
  const premiumEnabled = ctx.ff.premiumFeatures; // boolean

  if (premiumEnabled) {
    // This flag was evaluated knowing the user's plan
    await enablePremiumFeatures(user);
    ctx.ff.track('premiumFeatures');
  }
});
```

## Performance Characteristics

- **First access**: Proxy intercept + cache + ff-access log (~0.1ms)
- **Subsequent access**: Map lookup only (~0.01ms, no log)
- **track() call**: Direct buffer write + chainable methods (~0.05ms)
- **Analytics**: Deduped ff-access per span, explicit ff-usage via `ctx.ff.track()`
- **V8 Optimized**: Returns primitives, no wrapper objects, monomorphic call sites
