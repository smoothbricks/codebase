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

### String Type Performance Characteristics <a id="smoo/lmao!n/schema-string-types"></a>

LMAO provides three distinct string types with different storage strategies optimized for different access patterns.

#### Shipped representation

The shipped implementation does **not** intern strings on the warmed entry path. `CATEGORY` and `TEXT` columns store raw
JS strings in `string[]`; dictionary construction and UTF-8 encoding occur during Arrow conversion. This describes the
current Op-local implementation, not the clean-cutover target below.

| Type         | Shipped entry storage  | Shipped Arrow conversion            | Semantic intent                 |
| ------------ | ---------------------- | ----------------------------------- | ------------------------------- |
| **ENUM**     | Numeric typed index    | Predefined dictionary               | Closed compile-time value set   |
| **CATEGORY** | `string[]` raw strings | Sort/dedupe and encode              | Repeating runtime values        |
| **TEXT**     | `string[]` raw strings | Conditional/raw string construction | High-cardinality runtime values |

#### Target representation: schema physicalization

At startup, a canonical log schema is lowered to an immutable `PhysicalLayoutPlan`. The plan fixes column order,
physical widths and offsets, eager/lazy placement, null bitmaps, static message-index lanes, dynamic reference lanes,
and backend-specific factories. The runtime caches the plan and generated classes by **schema identity + physical layout
version + backend kind**. Prefix/remap information that changes physical writes is part of the key or an immutable
binding layered over the cached plan; it is never rediscovered per entry.

Schema lowering produces **static**, **structured**, or **dynamic** callsite classes. A `mixed` physical plan contains
both static-index and dynamic-reference lanes; `mixed` is not a callsite class.

The schema type continues to describe semantics, not whether a particular value happened to be observed before:

| Type         | Target warmed entry storage                                 | Target flush behavior                              |
| ------------ | ----------------------------------------------------------- | -------------------------------------------------- |
| **ENUM**     | Fixed-width numeric index                                   | Reuse the plan's closed dictionary                 |
| **CATEGORY** | Dynamic reference lane for runtime strings                  | Deduplicate/encode through reusable category state |
| **TEXT**     | Dynamic reference lane preserving raw/high-cardinality data | Encode without CATEGORY identity semantics         |

Compiler-known message vocabulary is separate from user `CATEGORY`/`TEXT` columns. Checker-proven literal operational
templates use `kindTag = 1` (`LOG_TEMPLATE`), and checker-proven literal span names use `kindTag = 2` (`SPAN_NAME`);
both store process-dense `Uint32` indices that are Arrow-ready. Non-literal span names and dynamic diagnostic messages
use tagged dynamic reference lanes and are encoded on the overflow/flush slow path. They MUST NOT enter the static lane
through warmed-path interning.

This division is normative:

- A dynamic `CATEGORY` remains dynamic even when it repeats. Deduplication is a flush representation choice.
- A dynamic `TEXT` remains dynamic and high-cardinality; it MUST NOT be promoted to a static template.
- The LMAO runtime installer MUST be imported and evaluated before module registration. The registry validates each
  typed-array fragment and copies/merges it into a runtime-owned immutable dictionary generation; module fragment arrays
  remain inputs, never borrowed Arrow storage. Its returned `VocabularyBinding` maps `binding[ordinal]` directly to a
  process-dense Arrow index.
- Dense indices are process-local and append-only by fragment registration order. A new immutable generation preserves
  the prior generation as an unchanged prefix and appends unseen decoded values, keeping old bindings prefix-valid.
  Stable IDs and decoded values are deterministic, but dense indices and dictionary order are not cross-process
  guarantees. Index `0` is valid; null is represented by the Arrow validity bitmap.
- The target warmed entry write performs fixed monomorphic stores and allocates nothing while capacity remains. Startup,
  span setup, overflow/slow path, and per-request flush have separate allocation budgets.
- Per-request flush reuses the `PhysicalLayoutPlan`, backend class, vocabulary binding, dictionary generation, scratch
  arenas, and Arrow-compatible views. The target is few or no allocations; an `ArrowLease` pins runtime-owned storage
  and the exact immutable dictionary generation until release, never a caller's mutable fragment arrays.

See
**[Buffer Performance Optimizations](./01b1_buffer_performance_optimizations.md#string-interning-and-utf-8-caching-architecture)**
for the shipped conversion path and optimization roadmap.

### String Type Decision Matrix <a id="smoo/lmao!n/schema-string-types.matrix"></a>

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

## Log Schema Definition <a id="smoo/lmao!n/schema-log-schema"></a>

**Purpose**: Define structured data that can be logged to spans with type safety and automatic masking.

```typescript
// Define a log schema, then wire it into an op context.
// (Implemented API — see "Implementation status" below. The illustrative
// `defineModule(...).ctx<Extra>().make()` form below predates the shipped
// `defineLogSchema` + `defineOpContext` surface.)
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

**Implementation status**: The schema DSL (`S.enum/category/text/number/boolean`, plus `S.binary/unknown/object` for
msgpack columns and `.mask(preset)` on category/text) is `packages/lmao/src/lib/schema/builder.ts`; `defineLogSchema()`
(validation + inference + reserved-name guard) is `packages/lmao/src/lib/schema/defineLogSchema.ts`. The shipped
authoring surface is **`defineLogSchema(...)` + `defineOpContext({ logSchema, deps, flags, ctx })` →
`{ defineOp, defineOps }`** (`packages/lmao/src/lib/defineOpContext.ts`), NOT `defineModule(...).ctx<Extra>().make()` as
drawn above — user context is the `ctx` config field (runtime defaults), not a type-only `.ctx<Extra>()` method. The
wholesale `defineModule` → `defineOpContext` rename across 01a/01g/01j is staged as the reconciliation node below.

## Feature Flag Schema Definition <a id="smoo/lmao!n/schema-feature-flags"></a>

**Purpose**: Define feature flags with type-safe access and explicit analytics tracking.

> **Implementation status**: `defineFeatureFlags()` is `packages/lmao/src/lib/schema/defineFeatureFlags.ts`; the
> sync/async flag builder (`.default(v).sync()/.async()`) is on the schema DSL in `schema/builder.ts`. The full
> evaluator + `ff-access`/`ff-usage` analytics are specified in [01p](./01p_feature_flags.md) and realized in
> `codegen/evaluatorGenerator.ts` / `schema/evaluator.ts`.

See **[Feature Flags](./01p_feature_flags.md)** for the complete feature flag system specification, including:

- Schema definition syntax (`defineFeatureFlags`)
- Flag access patterns (`FlagContext | undefined` wrappers)
- Tracking API (`flag.track()` returning fluent entry)
- Context integration and inheritance
- Evaluator implementation details

```typescript
const featureFlags = defineFeatureFlags({
  // Boolean flags
  darkMode: S.boolean().default(false).sync(),
  // Numeric flags
  maxItems: S.number().default(100).sync(),
  // String flags (enum or category)
  buttonColor: S.enum(['blue', 'green', 'red']).default('blue').sync(),
  experimentGroup: S.category().default('control').async(),
});
```

### Key Concepts

- **Access returns wrapper**: `ctx.ff.darkMode` returns `{ value, track() } | undefined`
- **Explicit tracking**: `flag.track(...)` logs usage and returns fluent row tagging
- **Unified event model**: Feature-flag analytics are encoded as system entry types (`ff-access`, `ff-usage`)
- **Access dedupe**: `ff-access` is deduped per span/flag, with explicit `ff-usage` rows for tracking

## Environment Variable Configuration <a id="smoo/lmao!n/schema-env-config"></a>

**Purpose**: Provide simple, fast access to deployment configuration without overhead.

> **Implementation status**: There is no dedicated env-config module — by design (a plain object is "just property
> access, no evaluator or tracking"). In the shipped API the user-supplied object reaches ops through the `ctx` config
> field of `defineOpContext` (`packages/lmao/src/lib/defineOpContext.ts`), spread into `SpanContext` as user-extensible
> properties. Masking on logged values is the schema's `.mask(preset)` (`schema/builder.ts`), applied at
> Arrow-conversion time, not by an env layer.

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

- **Access**: Generated getter/`get()` calls evaluator and logs deduped `ff-access`
- **Tracking**: `flag.track(...)` emits `ff-usage` and supports fluent `.with(...)`
- **Analytics**: Deduped `ff-access` per span + explicit `ff-usage` entries

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
