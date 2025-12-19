# Trace Schema System

## Overview

The trace schema system provides type-safe, high-performance configuration and attribute management for the trace
logging system. It handles three types of data:

1. **Log Schema** - Structured data logged to spans
2. **Feature Flags** - Dynamic behavior configuration with analytics
3. **Environment Variables** - Static deployment configuration

> **Package Ownership**: Schema definitions live in `@smoothbricks/lmao`. Buffer storage implementation (TypedArrays,
> lazy columns, null bitmaps) lives in `@smoothbricks/arrow-builder`. See `00_package_architecture.md` for the complete
> separation of concerns.

## Design Philosophy

**Key Insight**: Different types of configuration data have different access patterns, tracking needs, and performance
requirements. The system optimizes each type appropriately rather than forcing them into a single pattern.

| Type                      | Complexity          | Tracking | Performance        | Use Case              |
| ------------------------- | ------------------- | -------- | ------------------ | --------------------- |
| **Log Schema**            | Schema + validation | Always   | Columnar writes    | Structured span data  |
| **Feature Flags**         | Schema + evaluator  | Always   | Cached + analytics | A/B testing, rollouts |
| **Environment Variables** | Plain object        | Never    | Property access    | Infrastructure config |

### String Type Performance Characteristics

LMAO provides three distinct string types with different storage strategies optimized for different access patterns.

**CRITICAL**: Strings are **NOT interned on the hot path**. CATEGORY and TEXT columns store raw JS strings in `string[]`
arrays during logging. Dictionary building and UTF-8 encoding happen only during cold-path Arrow conversion. This keeps
logging lightweight while conversion can be heavier.

| Type         | Hot Path Storage       | Cold Path (Arrow Conversion)      | Memory Growth     | Use Case                     |
| ------------ | ---------------------- | --------------------------------- | ----------------- | ---------------------------- |
| **ENUM**     | Uint8Array (1 byte)    | Zero work (pre-built dictionary)  | Bounded (fixed)   | Known compile-time values    |
| **CATEGORY** | string[] (raw strings) | Sort + dedupe → sorted dictionary | Per-flush bounded | Values that often repeat     |
| **TEXT**     | string[] (raw strings) | 2-pass conditional dictionary     | Per-flush bounded | Unique values, rarely repeat |

See
**[Buffer Performance Optimizations](./01b1_buffer_performance_optimizations.md#string-interning-and-utf-8-caching-architecture)**
for implementation details, including:

- Why hot-path interning was rejected
- SIEVE cache usage for UTF-8 encoding
- Dictionary building strategies per type
- Memory growth prevention mechanisms

### String Type Decision Matrix

```typescript
// ENUM: Known values at compile time (≤256 common, ≤65536 max)
entryType: S.enum(['span-start', 'span-ok', 'span-err']);
logLevel: S.enum(['debug', 'info', 'warn', 'error']);
httpMethod: S.enum(['GET', 'POST', 'PUT', 'DELETE', 'PATCH']);

// CATEGORY: Runtime values that repeat (limited cardinality)
userId: S.category(); // Users appear in multiple spans
action: S.category(); // Same actions repeat ('login', 'checkout')
region: S.category(); // Limited AWS regions ('us-east-1', etc.)
spanName: S.category(); // Same span names repeat across requests
tableName: S.category(); // Database tables are reused

// TEXT: Unique values (high cardinality)
errorMessage: S.text(); // Error messages are often unique
sqlQuery: S.text(); // SQL queries vary widely
stackTrace: S.text(); // Stack traces are unique per error
requestId: S.text(); // Request IDs are unique by design
uuid: S.text(); // UUIDs are unique by definition
```

## Log Schema Definition

**Purpose**: Define structured data that can be logged to spans with type safety and automatic masking.

```typescript
// Define module with logSchema
const httpModule = defineModule({
  metadata: { packageName: '@mycompany/http', packagePath: 'src/index.ts', gitSha: 'abc123' },
  logSchema: {
    requestId: S.category(),
    userId: S.category().mask('hash'),
    timestamp: S.number(),
    status: S.number(),
    method: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
    endpoint: S.category(),
    errorMessage: S.text(),
  },
  deps: { db: dbModule },
  ff: { premium: ff.boolean() },
})
  .ctx<{ env: WorkerEnv; requestId: string }>({
    env: null!,
    requestId: null!,
  })
  .make();
```

**Why This Design**:

- **Composable**: Base logSchema can be extended for specific domains
- **Type-safe**: Full TypeScript inference for tag operations
- **Masking rules**: Sensitive data automatically masked during serialization
- **Columnar storage**: Schema drives efficient TypedArray column generation

## Feature Flag Schema Definition

**Purpose**: Define feature flags with type-safe access and explicit analytics tracking.

See **[Feature Flags](./01p_feature_flags.md)** for the complete feature flag system specification, including:

- Schema definition syntax (`defineFeatureFlags`)
- Flag access patterns (returns primitives)
- Tracking API (`ctx.ff.track()`)
- Context integration and inheritance
- Evaluator implementation details

```typescript
const featureFlags = defineFeatureFlags({
  // Boolean flags
  darkMode: S.boolean().default(false),
  // Numeric flags
  maxItems: S.number().default(100),
  // String flags (enum or category)
  buttonColor: S.enum(['blue', 'green', 'red']).default('blue'),
  experimentGroup: S.category().default('control'),
});
```

### Key Concepts

- **Access returns value**: `ctx.ff.darkMode` returns `boolean` (not an object)
- **Explicit tracking**: `ctx.ff.track('darkMode')` logs usage for analytics
- **Unified schema**: Tracking uses the same attribute columns as `ctx.tag`
- **Zero overhead**: Values cached per-span, deduped logging

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

See **[Feature Flags](./01p_feature_flags.md#evaluator-implementation)** for details on:

- Singleton evaluator pattern (backend service)
- `forContext(ctx)` method for span-bound accessors
- Buffer chain scanning for deduped access logging
- Schema integration and bootstrap patterns

## Performance Characteristics

### Log Schema Attributes

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
5. **Composable Schemas**: Log schema can be extended and reused across modules
6. **Zero Configuration Overhead**: Environment variables are just plain objects

This design provides the right tool for each job - sophisticated analytics for feature flags, structured logging for log
schema attributes, and zero-overhead access for environment configuration.
