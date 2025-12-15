# Trace Schema System

## Overview

The trace schema system provides type-safe, high-performance configuration and attribute management for the trace
logging system. It handles three types of data:

1. **Tag Attributes** - Structured data logged to spans
2. **Feature Flags** - Dynamic behavior configuration with analytics
3. **Environment Variables** - Static deployment configuration

## Design Philosophy

**Key Insight**: Different types of configuration data have different access patterns, tracking needs, and performance
requirements. The system optimizes each type appropriately rather than forcing them into a single pattern.

| Type                      | Complexity          | Tracking | Performance        | Use Case              |
| ------------------------- | ------------------- | -------- | ------------------ | --------------------- |
| **Tag Attributes**        | Schema + validation | Always   | Columnar writes    | Structured span data  |
| **Feature Flags**         | Schema + evaluator  | Always   | Cached + analytics | A/B testing, rollouts |
| **Environment Variables** | Plain object        | Never    | Property access    | Infrastructure config |

## Tag Attribute Schema Definition

**Purpose**: Define structured data that can be logged to spans with type safety and automatic masking.

```typescript
// Base attributes available everywhere
const baseAttributes = defineTagAttributes({
  requestId: S.category(),
  userId: S.category().mask('hash'),
  timestamp: S.number(),

  // Feature flag operations use ff-access and ff-usage entry types
  // They write directly to span buffer via ctx.ff methods
  // Flag evaluation context stored in regular attribute columns
  // Flag name stored in unified `label` column (same as span name / log template)
  // Only ffValue is FF-specific (S.category for efficient storage of repeated values)
});
```

**Why This Design**:

- **Composable**: Base attributes can be extended for specific domains
- **Type-safe**: Full TypeScript inference for tag operations
- **Masking rules**: Sensitive data automatically masked during serialization
- **Columnar storage**: Schema drives efficient TypedArray column generation

## Feature Flag Schema Definition

**Purpose**: Define feature flags with type-safe access and explicit analytics tracking.

### Schema Definition

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
ctx.ff.track('darkMode').action('button-click').outcome('converted');

// Typical pattern: check flag, then track
if (ctx.ff.darkMode) {
  ctx.ff.track('darkMode').action('enabled-path');
  applyDarkTheme();
}

// Track with multiple attributes (chainable)
ctx.ff.track('buttonColor').variant(ctx.ff.buttonColor).action('clicked').outcome('checkout-started');

// Track numeric flag usage
ctx.ff.track('maxItems').action('pagination').requested(100).returned(ctx.ff.maxItems);
```

### Track Uses Same Schema as Tag

The `track()` method returns a chainable API that writes to the **same columns** as `ctx.tag`. There is no separate
tracking schema - this keeps the Arrow table structure unified and avoids column name collisions.

```typescript
// Your tag attributes schema
const tagAttributes = defineTagAttributes({
  action: S.category(), // 'button-click', 'page-view', 'api-call'
  outcome: S.category(), // 'converted', 'bounced', 'error'
  userId: S.category(),
  duration: S.number(),
});

// track() returns chainable API with SAME methods as ctx.tag
// Equivalent to:
// 1. Write entry_type = FF_USAGE
// 2. Write label = 'darkMode'  (unified label column - flag name)
// 3. Write attr_action = 'button-click'  (from tag schema)
// 4. Write attr_outcome = 'converted'    (from tag schema)
```

**System columns** (defined in systemSchema):

```typescript
// Core system columns
const systemSchema = defineTagAttributes({
  // Trace structure
  timestamp: S.number(), // Microseconds since epoch
  traceId: S.category(), // Request trace ID
  spanId: S.number(), // Span identifier
  parentSpanId: S.number(), // Parent span (nullable)
  entryType: S.enum([
    // Entry type enum
    'span-start',
    'span-ok',
    'span-err',
    'span-exception',
    'info',
    'debug',
    'warn',
    'error',
    'tag',
    'ff-access',
    'ff-usage',
  ]),
  module: S.category(), // Module name

  // UNIFIED LABEL COLUMN - span name, log message template, OR flag name
  label: S.category(), // See "The label System Column" below

  // Feature flag value column
  ffValue: S.category(), // Flag value - uses category for efficient storage (values repeat: true/false, 'blue'/'green', etc.)
});
```

### The `label` System Column

The `label` column is a **unified column** that serves different purposes based on entry type:

| Entry Type                                                   | What `label` Contains                                       |
| ------------------------------------------------------------ | ----------------------------------------------------------- |
| `span-start`, `span-ok`, `span-err`, `span-exception`, `tag` | **Span name** (e.g., `'create-user'`)                       |
| `info`, `debug`, `warn`, `error`                             | **Log message template** (e.g., `'User ${userId} created'`) |
| `ff-access`, `ff-usage`                                      | **Flag name** (e.g., `'darkMode'`, `'advancedValidation'`)  |

**Why unified?**

1. **Simpler schema**: One column instead of separate `span_name`, `message`, and `ffName` columns (most would be null)
2. **Better storage**: `S.category()` means string interning - templates/names stored once
3. **Efficient queries**: Find all logs matching a template pattern, or all accesses of a specific flag

**CRITICAL - Format Strings, NOT Interpolation**:

```typescript
// This:
ctx.log.info('User ${userId} processed ${count} items').userId('user-123').count(42);

// Stores:
// - label: 'User ${userId} processed ${count} items'  (template, interned)
// - attr_userId: 'user-123'                           (value, in typed column)
// - attr_count: 42                                    (value, in typed column)

// NOT:
// - label: 'User user-123 processed 42 items'  (interpolated - WRONG!)
```

The template string is stored verbatim. Values go in their typed attribute columns.

See **[Arrow Table Structure](./01f_arrow_table_structure.md)** for detailed examples and query patterns.

**All other attributes** come from your tag schema - same columns, same table structure.

### Deduplication: One ff-access per Span

```typescript
const enabled = ctx.ff.darkMode; // Logs ff-access entry
const enabled2 = ctx.ff.darkMode; // No log - cached for this span

// Tracking is always logged (not deduplicated)
ctx.ff.track('darkMode').action('path-a'); // Logs ff-usage entry
ctx.ff.track('darkMode').action('path-b'); // Logs another ff-usage
```

### Why This Design

- **V8 optimized**: No wrapper objects, no hidden class polymorphism
- **Simple mental model**: Flags return values, tracking is explicit
- **Chainable tracking**: Consistent with `ctx.tag` and `ctx.log.info()` APIs
- **Deduped access logging**: First access per span logs ff-access
- **Explicit usage tracking**: `track()` for A/B analytics, separate from access

### Evaluation Context and Child Spans

**Key Insight**: Feature flag evaluation often depends on context that isn't known at request creation time. A request
may start without a userId, then later identify the user. Child spans may operate in different contexts (e.g.,
processing a specific user's data in a batch job).

#### Evaluation Context Structure

The FF evaluator receives context for flag decisions. This context is **flat** for performance (no nested objects):

```typescript
// EvaluationContext - flat structure for performance
interface EvaluationContext {
  // Common context fields
  userId?: string;
  requestId?: string;
  userPlan?: string;
  region?: string;
  // Extensible with additional string/number/boolean fields
  [key: string]: string | number | boolean | undefined;
}
```

#### Context Changes in Child Spans

When creating a child span, the evaluation context may need to change:

```typescript
// Request-level: no specific user yet (batch job processing multiple users)
const requestCtx = createRequestContext({ requestId: 'req-123' });

// At this point, ctx.ff evaluates flags without userId context
const batchEnabled = requestCtx.ff.batchProcessing; // Evaluated without userId

// Later, processing a specific user
await ctx.span('processUser', { userId: 'user-456' }, async (childCtx) => {
  // childCtx.ff evaluates flags WITH userId context
  // The evaluator was created with additional context: { userId: 'user-456' }
  const premiumEnabled = childCtx.ff.premiumFeatures; // Evaluated with userId!

  if (premiumEnabled) {
    // This user has premium features enabled
    childCtx.ff.track('premiumFeatures').outcome('used');
  }
});
```

#### FeatureFlagEvaluator.withContext() Method

The evaluator provides a `withContext()` method to create child evaluators with additional context:

```typescript
interface FeatureFlagEvaluator<T extends FeatureFlagSchema, Tag extends TagAttributeSchema> {
  // Flag access - returns primitive values directly (boolean, number, string)
  readonly [K in keyof T]: InferFlagType<T[K]>;

  // Track flag usage - returns chainable API with SAME methods as ctx.tag
  // Writes to same columns, unified table structure
  track(flagName: keyof T): FlagTracker<Tag>;

  // Create child evaluator with additional/updated context
  // Returns a new evaluator instance with merged context
  withContext(additional: Partial<EvaluationContext>): FeatureFlagEvaluator<T, Tag>;

  // Get the current evaluation context (read-only)
  readonly evaluationContext: Readonly<EvaluationContext>;
}

// FlagTracker - chainable API using SAME schema as ctx.tag
// Methods are generated from tagSchema, same columns as ctx.tag
interface FlagTracker<T extends TagAttributeSchema> {
  // Same methods as ctx.tag - writes to same columns
  [K in keyof T]: (value: InferAttributeType<T[K]>) => FlagTracker<T>;
}
```

**Why `withContext()` on the evaluator (not RequestContext)**:

- **Encapsulation**: The evaluator owns its context and knows how to merge it
- **Immutability**: Returns a new evaluator, preserving the parent's context
- **Composability**: Can chain multiple context additions
- **Buffer binding**: Child evaluator is bound to child span's buffer separately

#### Context Flow Through Span Creation

When `ctx.span()` is called with additional context:

```typescript
// In task wrapper / span creation
function createChildSpan(parentCtx, spanName, additionalContext, fn) {
  // Create child buffer
  const childBuffer = createChildSpanBuffer(parentCtx.buffer, spanName);

  // Create child FF evaluator with:
  // 1. Additional context merged with parent context
  // 2. New buffer reference for logging
  const childFf = parentCtx.ff
    .withContext(additionalContext) // Merge context
    .withBuffer(childBuffer); // Bind to child buffer

  const childCtx = {
    ...parentCtx,
    ff: childFf,
    buffer: childBuffer,
    log: new SpanLogger(childBuffer),
  };

  return fn(childCtx);
}

// Usage patterns:

// Pattern 1: Additional context in span options
await ctx.span('processUser', { userId: 'user-456' }, async (childCtx) => {
  // childCtx.ff has userId in evaluation context
});

// Pattern 2: Context already on parent, just inherits
await ctx.span('validateInput', async (childCtx) => {
  // childCtx.ff inherits parent's evaluation context
});
```

#### RequestContext Structure (Flat)

The RequestContext remains flat for performance:

```typescript
interface RequestContext {
  // Trace identifiers
  traceId: string;
  requestId: string;

  // Time anchoring for relative timestamps
  anchorEpochMicros: number;
  anchorPerfNow: number;

  // Optional context that may be set later
  userId?: string;

  // Feature flag evaluator (bound to request-level buffer initially)
  ff: FeatureFlagEvaluator<FeatureFlags>;

  // Environment config (plain object, no tracking)
  env: EnvironmentConfig;
}
```

**Note**: No nested `timeAnchor: { epochMicros, perfNow }` - fields are flat for performance.

## Environment Variable Configuration

**Purpose**: Provide simple, fast access to deployment configuration without overhead.

```typescript
// Simple configuration object loaded at startup
const environmentConfig = {
  // Static values from process.env or config service
  awsRegion: process.env.AWS_REGION || 'us-east-1',
  nodeEnv: process.env.NODE_ENV || 'development',
  logLevel: process.env.LOG_LEVEL || 'info',

  // Sensitive values - real values for app use, masked only if logged
  databaseUrl: process.env.DATABASE_URL,
  apiKey: process.env.API_KEY,

  // Numeric values
  maxConnections: parseInt(process.env.MAX_CONNECTIONS) || 100,
  rateLimitPerMinute: parseInt(process.env.RATE_LIMIT) || 1000,
};

// No interface needed - just a plain object
type EnvironmentConfig = typeof environmentConfig;
```

**Why This Design**:

- **Zero overhead**: Just property access, no evaluator or tracking
- **Security**: Values only appear in traces if explicitly logged
- **Simplicity**: No schema validation needed for deployment config
- **Performance**: Fastest possible access pattern

## Feature Flag Evaluator Implementation

**Purpose**: Handle feature flag evaluation with full span context for logging, tracing, and analytics.

### Why a Singleton Evaluator?

The evaluator may need to:

1. **Make network calls** to external flag services (LaunchDarkly, Split, Unleash)
2. **Create child spans** to trace those network calls with timing/errors
3. **Log debug info** during evaluation via `ctx.log`
4. **Access ctx.env** for environment-specific evaluation

This requires full `SpanContext` access, not just a buffer reference. But we can't create the evaluator per-span
(expensive) or pass ctx to constructor (circular dependency: SpanContext has ff, ff needs SpanContext).

**Solution**: Singleton evaluator created at app startup, with `forContext(ctx)` method that returns a context-bound
accessor for each span.

```
App Startup                    Per-Request                     Per-Span
───────────────────────────────────────────────────────────────────────────

┌─────────────────────┐
│ FeatureFlagEvaluator│        ┌──────────────┐               ┌─────────────┐
│ (singleton)         │───────▶│ requestCtx   │──────────────▶│ spanCtx     │
│                     │        │              │               │             │
│ - schema            │        │ ff: accessor │               │ ff: accessor│
│ - externalClient    │        │     ▲        │               │     ▲       │
└─────────────────────┘        └─────│────────┘               └─────│───────┘
         │                           │                               │
         │    forContext(ctx) ───────┘                               │
         │    forContext(childCtx) ──────────────────────────────────┘
         │
         ▼
   Can call ctx.span(), ctx.log, ctx.tag inside evaluation!
```

### FeatureFlagEvaluator Implementation

```typescript
/**
 * Singleton evaluator created at app startup.
 * Holds schema and external evaluation client.
 * Stateless - all per-span state lives in the accessor returned by forContext().
 */
class FeatureFlagEvaluator<T extends FeatureFlagSchema, Tag extends TagAttributeSchema> {
  constructor(
    private schema: T,
    private tagSchema: Tag,
    private externalClient: FlagEvaluationClient
  ) {}

  /**
   * Create a context-bound flag accessor for a span.
   * Called when creating each span to get the `ff` property.
   *
   * @param ctx - The SpanContext to bind to (for logging, tracing, buffer access)
   * @returns FlagAccessor with property access for flags and track() method
   */
  forContext<FF extends FeatureFlagSchema, Env>(ctx: SpanContext<Tag, FF, Env>): FlagAccessor<T, Tag> {
    return new FlagAccessor(this.schema, this.tagSchema, this.externalClient, ctx);
  }
}

/**
 * Per-span accessor returned by evaluator.forContext(ctx).
 * Holds reference to ctx for logging/spanning, plus per-span caches.
 */
class FlagAccessor<T extends FeatureFlagSchema, Tag extends TagAttributeSchema> {
  // Per-span caches (fresh for each span)
  private accessedFlags = new Set<string>();
  private valueCache = new Map<string, boolean | number | string>();

  constructor(
    private schema: T,
    private tagSchema: Tag,
    private client: FlagEvaluationClient,
    private ctx: SpanContext<Tag, any, any>
  ) {
    // Return proxy for property access
    return new Proxy(this, {
      get: (target, prop: string) => {
        if (prop === 'track') return target.track.bind(target);
        if (prop in target) return target[prop as keyof typeof target];
        return target.getFlag(prop);
      },
    });
  }

  /**
   * Track flag usage with chainable attributes.
   * Returns same chainable API as ctx.tag - unified schema.
   */
  track(flagName: keyof T): FlagTracker<Tag> {
    const buffer = this.ctx.buffer;
    const idx = buffer.writeIndex++;

    buffer.timestamps[idx] = getTimestampMicros(ctx.anchorEpochMicros, ctx.anchorPerfNow);
    buffer.operations[idx] = ENTRY_TYPE_FF_USAGE;
    buffer.label[idx] = internString(String(flagName)); // Unified label column

    // Return chainable tracker using SAME schema as ctx.tag
    return this.createChainableTracker(idx);
  }

  private getFlag(flagName: string): boolean | number | string {
    // Return cached value if already accessed in this span
    if (this.valueCache.has(flagName)) {
      return this.valueCache.get(flagName)!;
    }

    const config = this.schema[flagName];
    if (!config) {
      throw new Error(`Unknown flag: ${flagName}`);
    }

    // Evaluate - may be sync or async depending on client
    // For async, the client can use ctx.span() to trace network calls!
    const value = this.client.evaluate(flagName, this.ctx);

    // Log ff-access entry (only on first access per span)
    if (!this.accessedFlags.has(flagName)) {
      this.logAccess(flagName, value);
      this.accessedFlags.add(flagName);
    }

    // Cache the primitive value
    this.valueCache.set(flagName, value);
    return value;
  }

  private logAccess(flagName: string, value: boolean | number | string): void {
    const buffer = this.ctx.buffer;
    const idx = buffer.writeIndex++;

    buffer.timestamps[idx] = getTimestampMicros(this.ctx.anchorEpochMicros, this.ctx.anchorPerfNow);
    buffer.operations[idx] = ENTRY_TYPE_FF_ACCESS;
    buffer.label[idx] = internString(flagName); // Unified label column
    buffer.attr_ffValue[idx] = internString(String(value)); // S.category for efficient storage
  }

  private createChainableTracker(idx: number): FlagTracker<Tag> {
    const buffer = this.ctx.buffer;
    const tracker = {} as FlagTracker<Tag>;

    // Generate methods from tagSchema - SAME columns as ctx.tag
    for (const attrName of Object.keys(this.tagSchema)) {
      const columnName = `attr_${attrName}`;
      (tracker as any)[attrName] = (value: unknown) => {
        if (buffer[columnName]) {
          buffer[columnName][idx] = serializeValue(value);
        }
        return tracker;
      };
    }

    return tracker;
  }
}

/**
 * External flag evaluation client interface.
 * Implementations may make network calls, use ctx for tracing.
 */
interface FlagEvaluationClient {
  /**
   * Evaluate a flag. Has access to full SpanContext for:
   * - Creating child spans for network calls
   * - Logging debug info
   * - Accessing ctx.env for environment config
   */
  evaluate(flagName: string, ctx: SpanContext): boolean | number | string;
}
```

### Example: Async Flag Client with Tracing

```typescript
// External client that traces its network calls
class LaunchDarklyClient implements FlagEvaluationClient {
  constructor(private ldClient: LDClient) {}

  evaluate(flagName: string, ctx: SpanContext): boolean | number | string {
    // For cached/sync evaluation - just return
    if (this.ldClient.isCached(flagName)) {
      return this.ldClient.getCached(flagName);
    }

    // For network fetch - create child span!
    return ctx.span('ld-fetch', async (childCtx) => {
      childCtx.tag.flagName(flagName);
      childCtx.tag.provider('launchdarkly');

      try {
        const value = await this.ldClient.variation(flagName, ctx.userId);
        childCtx.tag.flagValue(String(value));
        return childCtx.ok(value);
      } catch (error) {
        childCtx.log.error('Flag evaluation failed');
        return childCtx.err('LD_ERROR', error);
      }
    });
  }
}
```

### Usage in Span Creation

```typescript
// App startup - create singleton evaluator
const ffEvaluator = new FeatureFlagEvaluator(
  featureFlagSchema,
  tagAttributeSchema,
  new LaunchDarklyClient(ldClient),
);

// In task wrapper - bind to span context
function createSpanContext(parentCtx, buffer, ...): SpanContext {
  const ctx: SpanContext = {
    ...parentCtx,
    buffer,
    log: createSpanLogger(...),
    tag: createTagApi(...),
    // ff is bound to THIS context - can log, span, access buffer
    ff: ffEvaluator.forContext(ctx),
  };
  return ctx;
}

// In child span creation
async span(name, fn) {
  const childBuffer = createChildSpanBuffer(this.buffer);
  const childCtx: SpanContext = {
    ...this,
    buffer: childBuffer,
    log: createSpanLogger(childBuffer, ...),
    tag: createTagApi(childBuffer, ...),
    // Child gets fresh accessor with fresh caches, bound to childCtx
    ff: ffEvaluator.forContext(childCtx),
  };
  return fn(childCtx);
}
```

### Key Implementation Details

**Singleton Evaluator**: One evaluator instance created at app startup. Holds schema and external client. No per-span
instantiation cost.

**Context-Bound Accessor**: `evaluator.forContext(ctx)` returns a lightweight accessor holding:

- Reference to `ctx` (for logging, spanning, buffer access)
- Per-span caches (`accessedFlags`, `valueCache`)

**Full Context Access**: The accessor and external client have full `SpanContext`, so they can:

- Create child spans for network calls (`ctx.span('ld-fetch', ...)`)
- Log debug info (`ctx.log.debug(...)`)
- Access environment config (`ctx.env.flagServiceUrl`)

**Unified Schema**: The `track()` method uses the **same schema as `ctx.tag`**:

- Same column names (`attr_action`, `attr_outcome`, not `attr_ff_action`)
- Same Arrow table structure - no schema split
- Flag name stored in unified `label` column (consistent with span names and log templates)
- Only `ffValue` is FF-specific (S.category for efficient storage of repeated values like true/false)

**Per-Span Caching**: Each accessor has fresh caches:

- First access logs `ff-access`
- Subsequent accesses return cached primitive
- Cache naturally cleared when child span gets new accessor

**Why This Implementation**:

- **Singleton pattern**: No per-span evaluator instantiation cost
- **Full context access**: Can log, span, access env during evaluation
- **V8 optimized**: Returns primitives, no wrapper objects
- **Unified schema**: `track()` uses same columns as `ctx.tag`
- **Deduped logging**: Only first access per span logs ff-access
- **Network tracing**: External clients can create child spans for API calls

### Creating Child Span Accessors

When creating child spans, just call `evaluator.forContext(childCtx)`:

```typescript
// In span creation code
function createChildSpan(parentCtx, spanName, fn) {
  const childBuffer = createChildSpanBuffer(parentCtx.buffer, spanName);

  const childCtx = {
    ...parentCtx,
    buffer: childBuffer,
    log: new SpanLogger(childBuffer),
    tag: createTagApi(childBuffer),
    // Fresh accessor bound to childCtx - has fresh caches
    ff: ffEvaluator.forContext(childCtx),
  };

  return fn(childCtx);
}
```

The singleton `ffEvaluator` is available in the module scope (created at app startup).

### Deduplication Behavior Summary

| Operation                      | Logs Entry?      | Notes                            |
| ------------------------------ | ---------------- | -------------------------------- |
| First flag access in span      | Yes (ff-access)  | Value cached for span lifetime   |
| Subsequent access same flag    | No               | Returns cached primitive         |
| Access same flag in child span | Yes (ff-access)  | Child has fresh accessor/cache   |
| `ctx.ff.track('flag')` call    | Yes (ff-usage)   | Always logged, not deduplicated  |
| Async evaluation network call  | Yes (child span) | Client can trace with ctx.span() |

### Design Tradeoffs

**Pros**:

- Singleton evaluator: No per-span instantiation cost
- Full context access: Evaluation can log, create spans, access env
- V8 optimized: Returns primitives, no wrapper objects
- Unified schema: `track()` uses same columns as `ctx.tag`
- Consistent chainable API: `ctx.ff.track('flag').action('click').outcome('converted')`
- Deduped ff-access logging: Only first access per span logged
- Type-safe: TypeScript knows flag value types

**Cons**:

- Flag name repeated in `track()` call (but type-safe via keyof)
- Proxy overhead on every property access (mitigated by caching)
- Slightly more complex mental model (evaluator vs accessor)

## Schema Integration Patterns

### DefaultFlagValueClient: Bootstrap Evaluator

**Problem**: The external flag client (LaunchDarkly, Split, etc.) may need to:

1. Make network calls to initialize
2. Log those calls for observability
3. But logging needs `ctx`, which needs `ff`, which needs the client...

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
  tagAttributeSchema,
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
    anchorEpochMicros: Date.now() * 1000,
    anchorPerfNow: performance.now(),
    buffer,
    log: createSpanLogger(buffer, ...),
    tag: createTagApi(buffer, ...),
    ff: defaultFfEvaluator.forContext(ctx), // Uses defaults - always works
    env: environmentConfig,
    // ... ok, err, span methods
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

  // Create production evaluator with real (or fallback) client
  const ffEvaluator = new FeatureFlagEvaluator(
    featureFlagSchema,
    tagAttributeSchema,
    ldClient.value,
  );

  // Flush bootstrap traces
  await flushTraces(bootstrapCtx.buffer);

  return ffEvaluator;
}

// App entry point
const ffEvaluator = await initializeApp();
export { ffEvaluator };
```

### Why This Matters

Without `DefaultFlagValueClient`, you have a chicken-and-egg problem:

- Can't create `ctx.ff` without the client
- Can't trace client initialization without `ctx`
- App startup is a black box

With `DefaultFlagValueClient`:

- Bootstrap immediately with defaults
- Trace EVERYTHING including flag client init
- Graceful fallback if real client fails
- Tests don't need external services

### Request Context Creation

```typescript
// Create context at request boundary
// Note: ff is NOT set here - it's set per-span in task wrapper
function createRequestContext(params: { requestId: string; userId?: string }): RequestContext {
  const now = Date.now();

  return {
    // Trace identifiers
    traceId: generateTraceId(),
    requestId: params.requestId,
    userId: params.userId,

    // Time anchoring (FLAT - not nested in timeAnchor object)
    anchorEpochMicros: now * 1000,
    anchorPerfNow: performance.now(),

    // Environment config (plain object, no tracking)
    env: environmentConfig,

    // Note: ff is NOT here - added per-span via ffEvaluator.forContext(spanCtx)
  };
}

// Type definition - all fields flat at top level
interface RequestContext {
  traceId: string;
  requestId: string;
  userId?: string;

  // Time anchoring - FLAT for performance
  anchorEpochMicros: number;
  anchorPerfNow: number;

  // Environment config
  env: typeof environmentConfig;
}

// SpanContext extends RequestContext with span-specific properties
interface SpanContext extends RequestContext {
  buffer: SpanBuffer;
  log: SpanLogger;
  tag: TagAPI;
  ff: FlagAccessor; // Bound to this span via ffEvaluator.forContext(this)
  ok: <V>(value: V) => SuccessResult<V>;
  err: <E>(code: string, details: E) => ErrorResult<E>;
  span: (name: string, fn: (ctx: SpanContext) => Promise<any>) => Promise<any>;
}
```

### Task Integration

```typescript
// Module context with tag attributes
const { task } = createModuleContext({
  moduleMetadata: {
    gitSha: 'abc123...',
    filePath: 'src/services/user.ts',
    moduleName: 'UserService',
  },
  tagAttributes: dbAttributes, // Use DB-specific attributes
});

export const createUser = task('create-user', async (ctx, userData: UserData) => {
  // ctx has: tag, log, ok, err, span, ff (feature flags), env (environment)

  // Feature flag access - returns primitive value directly
  const advancedValidation = ctx.ff.advancedValidation; // boolean, logs ff-access

  if (advancedValidation) {
    const result = await performAdvancedValidation(userData);
    // Track usage with chainable API
    ctx.ff
      .track('advancedValidation')
      .action('validation')
      .outcome(result.success ? 'success' : 'failure');
  }

  // Environment access (just plain property access, no tracking)
  const region = ctx.env.awsRegion; // 'us-east-1'
  const maxConnections = ctx.env.maxConnections; // 100
  const dbUrl = ctx.env.databaseUrl; // Real postgres URL

  // Span attributes - set context data at span start (via ctx.tag)
  ctx.tag
    .requestId(ctx.requestId) // Sets bit 0, writes to attr_requestId column
    .userId(userData.id) // Sets bit 1, writes to attr_userId column
    .operation('INSERT'); // Sets bit 4, writes to attr_operation column

  // Or, object-based API for multiple attributes
  ctx.tag({ requestId: ctx.requestId, userId: userData.id, operation: 'INSERT' });

  // Masking only happens if you explicitly log environment values
  ctx.tag.region(region); // Safe to log
  // ctx.tag.databaseUrl(dbUrl);  // Would be masked by tag schema

  // Child spans create child SpanBuffers in tree structure
  // Child span inherits parent's FF evaluation context
  const validation = await ctx.span('validate-user', async (childCtx) => {
    // childCtx.ff is a NEW evaluator instance bound to childCtx's buffer
    // Same evaluation context as parent (no additional context needed here)
    childCtx.tag.query('SELECT COUNT(*) FROM users WHERE email = ?'); // Sets bit 5
    childCtx.tag.duration(12.5); // Sets bit 2

    if (existingUser) {
      return childCtx.err('USER_EXISTS').with({ email: userData.email });
    }
    return childCtx.ok({ valid: true });
  });

  if (!validation.success) {
    return ctx.err('VALIDATION_FAILED', validation.error);
  }

  const user = await db.createUser(userData);
  return ctx.ok(user);
});

// Example: Child span with ADDITIONAL evaluation context
export const processBatch = task('process-batch', async (ctx, users: User[]) => {
  // Parent span - no specific user context yet
  const batchEnabled = ctx.ff.batchProcessing; // Evaluated without userId

  for (const user of users) {
    // Child span with additional context - adds userId to evaluation
    await ctx.span(
      'process-user',
      { additionalContext: { userId: user.id, userPlan: user.plan } },
      async (childCtx) => {
        // childCtx.ff evaluates flags WITH userId and userPlan context!
        const premiumEnabled = childCtx.ff.premiumFeatures; // boolean

        if (premiumEnabled) {
          // This flag was evaluated knowing the user's plan
          await enablePremiumFeatures(user);
          childCtx.ff.track('premiumFeatures').outcome('enabled');
        }
      }
    );
  }

  return ctx.ok({ processed: users.length });
});
```

## Performance Characteristics

### Tag Attributes

- **Runtime**: <0.1ms per tag operation (TypedArray writes + bitmap)
- **Memory**: Columnar storage with null bitmaps
- **Type safety**: Zero runtime overhead (compile-time only)

### Feature Flags

- **First access**: Proxy intercept + cache + ff-access log (~0.1ms)
- **Subsequent access**: Map lookup only (~0.01ms, no log)
- **track() call**: Direct buffer write + chainable methods (~0.05ms)
- **Analytics**: Deduped ff-access per span, explicit ff-usage via `ctx.ff.track()`

### Environment Variables

- **Access**: Plain property lookup (zero overhead)
- **Security**: Values only in traces if explicitly logged
- **Masking**: Applied during background processing if logged

## Benefits

1. **Type Safety**: Full TypeScript inference across all three systems
2. **Performance Optimization**: Each system optimized for its access patterns
3. **Security by Default**: Sensitive data only appears in traces when explicitly logged
4. **Analytics Integration**: Feature flags automatically tracked for product decisions
5. **Composable Schemas**: Tag attributes can be extended and reused across modules
6. **Zero Configuration Overhead**: Environment variables are just plain objects

This design provides the right tool for each job - sophisticated analytics for feature flags, structured logging for tag
attributes, and zero-overhead access for environment configuration.
