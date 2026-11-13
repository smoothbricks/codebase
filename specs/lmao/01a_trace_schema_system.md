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

#### Schema physicalization

At startup, a canonical log schema is lowered to an immutable `PhysicalLayoutPlan`. The plan fixes column order,
physical widths and offsets, eager/lazy placement, null bitmaps, static message-index lanes, dynamic reference lanes,
and backend-specific factories. The runtime caches the plan and generated classes by **schema identity + physical layout
version + backend kind**. Prefix/remap information that changes physical writes is part of the key or an immutable
binding layered over the cached plan; it is never rediscovered per entry.

Schema lowering produces **static**, **structured**, or **dynamic** callsite classes. A `mixed` physical plan contains
both static-index and dynamic-reference lanes; `mixed` is not a callsite class.

The schema type continues to describe semantics, not whether a particular value happened to be observed before:

| Type         | Warmed entry storage                                        | Flush behavior                                     |
| ------------ | ----------------------------------------------------------- | -------------------------------------------------- |
| **ENUM**     | Fixed-width numeric index                                   | Reuse the plan's closed dictionary                 |
| **CATEGORY** | Dynamic reference lane for runtime strings                  | Deduplicate/encode through reusable category state |
| **TEXT**     | Dynamic reference lane preserving raw/high-cardinality data | Encode without CATEGORY identity semantics         |

A dynamic reference lane is a plain JS array of string references (`string[]`, the column's `_values`). The warmed write
stores the reference the producer already holds; it does not intern, look up vocabulary, or encode UTF-8. Dictionary
building and UTF-8 encoding happen during flush, whose registered-root-tree unit is defined in
[01j](./01j_module_context_and_spanlogger_generation.md#physicalization-and-cache-lifecycle)
([01b1](./01b1_buffer_performance_optimizations.md#string-interning-and-utf-8-caching-architecture)).

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
  the prior generation as an unchanged prefix and appends unseen decoded values, keeping old bindings prefix-valid. Ids
  and decoded values are deterministic, but dense indices and dictionary order are not cross-process guarantees. Index
  `0` is valid; null is represented by the Arrow validity bitmap.
- The warmed entry write performs fixed monomorphic stores and allocates nothing while capacity remains. Startup, span
  setup, overflow/slow path, and flush have separate allocation budgets.
- Flush reuses the `PhysicalLayoutPlan`, backend class, vocabulary binding, dictionary generation, scratch arenas, and
  Arrow-compatible views, and makes few or no allocations; an `ArrowLease` pins runtime-owned storage and the exact
  immutable dictionary generation until release, never a caller's mutable fragment arrays.

See
**[Buffer Performance Optimizations](./01b1_buffer_performance_optimizations.md#string-interning-and-utf-8-caching-architecture)**
for the conversion path and its optimizations.

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
const httpSchema = defineLogSchema({
  requestId: S.category(),
  userId: S.category().mask('hash'),
  timestamp: S.number(),
  status: S.number(),
  method: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
  endpoint: S.category(),
  errorMessage: S.text(),
});

const httpContext = defineOpContext({
  logSchema: httpSchema,
  deps: { db: dbModule },
  flags: defineFeatureFlags({ premium: S.boolean().default(false).sync() }).schema,
  ctx: { env: null as WorkerEnv, requestId: null as string },
});

const { defineOp, defineOps } = httpContext;
```

**Why This Design**:

- **Composable**: Base logSchema can be extended for specific domains
- **Type-safe**: Full TypeScript inference for tag operations
- **Masking rules**: Sensitive data automatically masked during serialization
- **Columnar storage**: Schema drives efficient TypedArray column generation

The schema DSL is one field builder, `S`, owned by `@smoothbricks/arrow-builder`
(`packages/arrow-builder/src/lib/schema/builder.ts`) and extended by lmao with feature flags and its MessagePack kind
(`packages/lmao/src/lib/schema/builder.ts`). `defineLogSchema()` (validation + inference) is
`packages/lmao/src/lib/schema/defineLogSchema.ts`. The authoring surface is **`defineLogSchema(...)` +
`defineOpContext({ logSchema, deps, flags, ctx })` → `{ defineOp, defineOps }`**
(`packages/lmao/src/lib/defineOpContext.ts`). User context is the `ctx` config field (runtime defaults).

### Fields and kinds <a id="smoo/lmao!n/schema-fields"></a>

A field is frozen plain data: `kind` (what the value is), `params` (what the kind was declared with), `physical` (the
Arrow column type the two determine), `optional`, and `hints` (`eager`, `mask`: storage and output choices that are no
part of the type). A column's type follows from the declaration alone, never from the data of a batch.

| Builder                                                     | Value                       | Column                                                                                                                                      |
| ----------------------------------------------------------- | --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `S.boolean()`                                               | `boolean`                   | Bool                                                                                                                                        |
| `S.i8()` `S.i16()` `S.i32()` / `S.u8()` `S.u16()` `S.u32()` | `number`                    | Int8, Int16, Int32 / UInt8, UInt16, UInt32                                                                                                  |
| `S.i64()` / `S.u64()`                                       | `bigint`                    | Int64 / UInt64                                                                                                                              |
| `S.f64()`, `S.number()`                                     | `number`                    | Float64                                                                                                                                     |
| `S.enum(variants)`                                          | a variant                   | Dictionary of the declared variants; the cell is the variant's position; UInt8 index up to 256 variants, UInt16 up to 65,536, UInt32 beyond |
| `S.category()`                                              | `string`                    | Dictionary of the stream, UInt32 index                                                                                                      |
| `S.text()`                                                  | `string`                    | Utf8                                                                                                                                        |
| `S.binary()` / `S.fixedBinary(n)`                           | `Uint8Array`                | Binary / FixedSizeBinary(n)                                                                                                                 |
| `S.timestamp(unit)` / `S.duration(unit)`                    | the unit's brand            | Timestamp(unit, UTC) / Duration(unit); unit is `s`, `ms`, `us` or `ns`                                                                      |
| `S.optional(field)`                                         | the field's, or `undefined` | the field's column                                                                                                                          |

- **One empty state.** `S.optional(field)` is the same field with `optional` set: the same kind, the same column, and
  empty is a clear validity bit. An eager column has no validity bitmap, so an eager field cannot be optional.
- **An enum's variant list is append-only.** A variant's ordinal is its position; a variant no longer written keeps its
  place.
- **A kind is an extension point.** `defineFieldKind({ id, physical, digest, validate, encode, decode })` gives a
  package a kind of its own: its identity (`id` and the canonical text of its parameters), its column type (one of the
  types above), its validation (a refusal names the path, what was expected and what was observed) and its codec between
  the value and the column's cell. The shared kinds are defined through the same call, so validation and identity never
  distinguish an extension from a shared kind. lmao's `S.unknown()` and `S.object<T>()` are such a kind, `lmao.msgpack`:
  any value, MessagePack in a Binary column.
- **A field's type has an identity.** `typeText(field)` is `kind(digest(params))`; `typeDigest(field)` is the lowercase
  hex of the first 16 bytes of BLAKE3 over it. `optional` and the hints are outside it.
- **Span buffers store the lanes they have.** A log schema an op context is built from holds fields of the kinds a span
  buffer stores: `enum`, `category`, `text`, `f64`, `boolean`, `u64`, and any kind whose column is Binary.
  `defineOpContext` refuses a field of any other kind, naming the field and its type.
- **Reserved names belong to the tag layer.** A log schema's fields become setters on `ctx.tag`, on log entries and on
  `Ok` / `Err`, so `defineOpContext` refuses a field named like one of their members or starting with `_`. A field
  schema that nothing tags through may use any name.

### Field numbers <a id="smoo/lmao!n/schema-field-numbers"></a>

A library's fields are declared with `defineFields(library, fields)`. A field's durable identity is its library and its
number, and the number is the field's position in the library's published field list, plain data:
`{ library, fields: [{ name, type, retired? }] }` with `type` the field's type digest.

- The list is append-only: an entry is never removed, reordered or re-typed. A field no longer declared is marked
  `retired` and its name is never declared again. A rename edits `name` in place and changes no number.
- `checkPublishedFields` judges a schema against its list and never rewrites it; `publishFields` is the author's run,
  which appends new fields and marks retired ones. `schema.numbered(list)` gives the schema its numbers.
- A column records the fields it holds in its Arrow field metadata under `sb.fields`: a JSON array of
  `{ lib, n, name, type }`. A field without a number cannot be recorded.

`OpContextConfig` has no metadata key. Source attribution (`package_name`, `package_file`, `git_sha`, `line`) is per op:
the build-time transformer fills the optional `metadata` argument of `defineOp(name, fn, metadata)`, and `defineOp`
builds the op's `OpMetadata` from it (`packages/lmao/src/lib/opContext/defineOp.ts`). Without that argument, `defineOp`
derives `package_file`, `line`, and a package name from the call-site stack frame and records `git_sha` as `'runtime'`.

## Feature Flag Schema Definition <a id="smoo/lmao!n/schema-feature-flags"></a>

**Purpose**: Define feature flags with type-safe access and explicit analytics tracking.

`defineFeatureFlags()` is `packages/lmao/src/lib/schema/defineFeatureFlags.ts`; the sync/async flag builder
(`.default(v).sync()/.async()`) is on the schema DSL in `schema/builder.ts`. The evaluator is generated by
`codegen/evaluatorGenerator.ts` and implemented in `schema/evaluator.ts`.

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

There is no dedicated env-config module, by design: a plain object is "just property access, no evaluator or tracking".
The user-supplied object reaches ops through the `ctx` config field of `defineOpContext`
(`packages/lmao/src/lib/defineOpContext.ts`), spread into `SpanContext` as user-extensible properties. Masking on logged
values is the schema's `.mask(preset)` (`schema/builder.ts`), applied at Arrow-conversion time, not by an env layer.

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
