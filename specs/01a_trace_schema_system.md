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

#### 1. ENUM - Known Values at Compile Time

**Storage Strategy:**

- **Initialization (ONCE at startup)**:
  - Sort all enum values lexicographically
  - Allocate UTF-8 bytes for ALL values immediately
  - Build reverse Map<string, index> for O(1) lookup
  - Dictionary is IMMUTABLE after construction
- **Hot path**: Uint8Array write via Map lookup (zero allocation)
- **Cold path**: Zero work - dictionary already sorted and UTF-8 encoded

**Why Sorted Dictionary:**

- Enables binary search for "does value X exist?" queries
- Arrow/Parquet can skip dictionary entries not present in data
- ClickHouse can push down predicates more efficiently

**Code Generation:**

```typescript
// Schema definition
entryType: S.enum(['span-start', 'span-ok', 'span-err']);

// At construction time (ONCE):
// 1. Sort: ['span-err', 'span-ok', 'span-start']
// 2. Encode UTF-8 for each value
// 3. Build reverse map: { 'span-err': 0, 'span-ok': 1, 'span-start': 2 }

// Generated hot-path code (compile-time mapping)
function writeEntryType(buffer, idx, value) {
  // Map lookup, NOT switch - supports any enum size
  buffer.attr_entryType_values[idx] = ENTRY_TYPE_MAP.get(value);
}

// Arrow conversion (zero work - already done)
function toArrowDictionary() {
  return {
    indices: buffer.attr_entryType_values.slice(0, writeIndex),
    dictionary: PRE_ENCODED_UTF8_DICTIONARY, // Already sorted, already UTF-8
  };
}
```

**Memory Characteristics:**

- ✓ Bounded growth (fixed at schema definition)
- ✓ Zero allocations during hot path
- ✓ Pre-sorted dictionary enables binary search queries
- ✓ UTF-8 bytes allocated ONCE, reused for every Arrow flush

**Limits:**

- Max 256 values (Uint8Array: 0-255)
- Use Uint16Array for 256-65536 values
- Use Uint32Array for >65536 values (rare)

#### 2. CATEGORY - Values Often Repeat (LIMITED CARDINALITY)

**Storage Strategy:**

- **Hot path**: Store raw JS strings in `string[]` array (zero cost, just reference assignment). **NO interning.**
- **Cold path**: Build SORTED Arrow Dictionary, UTF-8 encode with SIEVE cache
- **Memory**: Per-flush bounded (strings cleared after Arrow conversion)

**Why No Hot-Path Interning:**

Interning (Map lookup → integer index) was considered but rejected:

- Map lookups add latency even at O(1)
- Global interner state is complex to manage
- Deduplication at flush time is fast enough for typical workloads

**Why Sorted Dictionary (Cold Path):**

- Enables binary search for "does value X exist?" queries
- Consistent ordering across flushes for efficient Parquet merging
- ClickHouse can push down predicates on dictionary values

**Hot Path (Zero Overhead):**

```typescript
// Category columns store raw JS strings - just like TEXT
// The ONLY difference is cold-path dictionary building strategy
class CategoryColumn {
  private strings: string[] = [];

  write(idx: number, value: string): void {
    // FASTEST possible: just store the reference
    // No Map lookup, no interning, no UTF-8 conversion
    this.strings[idx] = value;
  }
}
```

**Cold Path (Arrow Conversion with SIEVE Cache):**

During Arrow conversion, category columns build a sorted dictionary and use SIEVE-cached UTF-8 encoding. This is where
all the "heavy" work happens:

```typescript
// In convertToArrow.ts - cold path only
function buildSortedCategoryDictionary(buffers, columnName) {
  // 1. Collect all unique strings from all buffers (deduplication HERE)
  const uniqueStrings = new Set<string>();
  for (const buf of buffers) {
    const strings = buf[columnName];
    for (let i = 0; i < buf.writeIndex; i++) {
      if (strings[i] != null) uniqueStrings.add(strings[i]);
    }
  }

  // 2. Sort dictionary alphabetically (enables binary search queries)
  const dictionary = [...uniqueStrings].sort();

  // 3. Build string → index mapping for remapping values
  const stringToIndex = new Map(dictionary.map((s, i) => [s, i]));

  // 4. Build indices array (remap original strings to sorted indices)
  const indices = new Uint32Array(totalRows);
  // ... remap each string to its sorted index

  return { dictionary, indices };
}

// UTF-8 encoding uses global SIEVE cache (cold path only)
const { data, offsets } = globalUtf8Cache.encodeMany(dictionary);
```

**SIEVE Cache for UTF-8 Encoding:**

Uses SIEVE algorithm (NSDI'24) instead of LRU - simpler AND better:

```typescript
import { SieveCache } from '@neophi/sieve-cache';

class Utf8Cache {
  private cache: SieveCache<string, Uint8Array>;
  private encoder = new TextEncoder();

  encode(str: string): Uint8Array {
    const cached = this.cache.get(str);
    if (cached) return cached;

    const encoded = this.encoder.encode(str);
    this.cache.set(str, encoded);
    return encoded;
  }

  encodeMany(strings: string[]): { data: Uint8Array; offsets: Int32Array } {
    // Encode all strings (using cache), build concatenated buffer + offsets
    // ...
  }
}

// Global cache provides cross-conversion benefits
export const globalUtf8Cache = new Utf8Cache(4096);
```

**Why SIEVE over LRU:**

- ~9% lower miss ratio than LRU-K, ARC, and 2Q (NSDI'24 paper)
- Simpler implementation (no frequency counters or ghost queues)
- Single pointer scan vs LRU's linked list manipulation
- Better for web workloads with skewed access patterns

**Memory Growth Prevention:**

| Mechanism          | What it bounds                   | Default             |
| ------------------ | -------------------------------- | ------------------- |
| Per-flush clearing | String array cleared after flush | Every flush         |
| SIEVE cache size   | UTF-8 encoded bytes cache        | 4096 entries        |
| SIEVE eviction     | Automatic removal of cold values | On insert when full |

**Use Cases:**

```typescript
userId: S.category(); // ✓ Same users appear multiple times
action: S.category(); // ✓ 'login', 'logout', 'purchase' repeat
region: S.category(); // ✓ 'us-east-1', 'eu-west-1', limited set
spanName: S.category(); // ✓ Same span names repeat
spanName: S.category(); // ✓ Same span names repeat
```

**Anti-patterns (use TEXT instead):**

```typescript
requestId: S.category(); // ✗ Every request has unique ID - will thrash LRU
timestamp: S.category(); // ✗ Every log has unique timestamp
uuid: S.category(); // ✗ UUIDs are unique by definition
```

#### 3. TEXT - Unique Values, Rarely Repeat

**Storage Strategy:**

- **Hot path**: Store raw JS strings in `string[]` array (no interning, no UTF-8 conversion)
- **Cold path**: 2-pass Arrow conversion with conditional dictionary encoding
- **Memory**: Bounded per-flush (strings cleared after Arrow conversion)

**Why No Hot-Path Interning:** TEXT columns are designed for unique/high-cardinality values. But note that CATEGORY also
doesn't intern on the hot path - both store raw strings. The difference is only in cold-path behavior:

- TEXT: May skip dictionary encoding if it doesn't save space
- CATEGORY: Always builds a sorted dictionary (assumes values repeat)

**Hot Path (Zero Overhead):**

```typescript
class TextColumn {
  private strings: string[] = []; // Just JS string references

  write(idx: number, value: string): void {
    // FASTEST possible: just store the reference
    // No Map lookup, no UTF-8 conversion, no interning
    this.strings[idx] = value;
  }
}
```

**Cold Path (2-Pass Arrow Conversion):**

The cold path decides whether dictionary encoding saves space:

```typescript
interface ConversionResult {
  type: 'dictionary' | 'plain';
  data: Uint8Array; // UTF-8 bytes (dictionary values OR all values)
  indices?: Uint32Array; // Only if dictionary encoding
  offsets: Int32Array; // Arrow string offsets
}

function convertTextColumn(strings: string[]): ConversionResult {
  // ═══════════════════════════════════════════════════════════
  // PASS 1: Count occurrences and calculate sizes
  // ═══════════════════════════════════════════════════════════
  const occurrences = new Map<string, number>();
  let totalUtf8Bytes = 0;

  for (const str of strings) {
    occurrences.set(str, (occurrences.get(str) || 0) + 1);
    totalUtf8Bytes += utf8ByteLength(str);
  }

  const uniqueCount = occurrences.size;
  const uniqueUtf8Bytes = sumUtf8Lengths(occurrences.keys());

  // ═══════════════════════════════════════════════════════════
  // Calculate space savings from dictionary encoding
  // ═══════════════════════════════════════════════════════════
  // Plain:      totalUtf8Bytes + (strings.length + 1) * 4 [offsets]
  // Dictionary: uniqueUtf8Bytes + (uniqueCount + 1) * 4 [offsets]
  //             + strings.length * 4 [indices]

  const plainSize = totalUtf8Bytes + (strings.length + 1) * 4;
  const dictionarySize = uniqueUtf8Bytes + (uniqueCount + 1) * 4 + strings.length * 4;
  const spaceSavings = plainSize - dictionarySize;

  // ═══════════════════════════════════════════════════════════
  // PASS 2: Build Arrow column based on decision
  // ═══════════════════════════════════════════════════════════

  // Threshold: 128 bytes minimum savings to justify dictionary overhead
  // - Below threshold: complexity not worth it
  // - Above threshold: meaningful space reduction
  if (spaceSavings > 128) {
    // Dictionary encoding: sort for query optimization
    const sortedUnique = [...occurrences.keys()].sort();
    const stringToIndex = new Map(sortedUnique.map((s, i) => [s, i]));

    // Build indices array
    const indices = new Uint32Array(strings.length);
    for (let i = 0; i < strings.length; i++) {
      indices[i] = stringToIndex.get(strings[i])!;
    }

    // Encode dictionary values to UTF-8
    const { data, offsets } = encodeStringsToUtf8(sortedUnique);

    return { type: 'dictionary', data, indices, offsets };
  } else {
    // Plain UTF-8: no dictionary indirection
    // Note: NO sorting for plain columns (maintains insertion order)
    const { data, offsets } = encodeStringsToUtf8(strings);

    return { type: 'plain', data, offsets };
  }
}
```

**Space Savings Examples:**

| Scenario                         | Unique | Total | Plain Size | Dict Size | Savings | Decision      |
| -------------------------------- | ------ | ----- | ---------- | --------- | ------- | ------------- |
| 100 identical "error" strings    | 1      | 100   | 505 B      | 413 B     | 92 B    | Plain (< 128) |
| 1000 identical "error" strings   | 1      | 1000  | 5005 B     | 4013 B    | 992 B   | Dictionary    |
| 100 unique UUIDs (36 chars each) | 100    | 100   | 4004 B     | 4404 B    | -400 B  | Plain         |
| 100 strings, 10 unique           | 10     | 100   | 5005 B     | 905 B     | 4100 B  | Dictionary    |

**Memory Growth Prevention:**

TEXT columns are **bounded per-flush**:

```typescript
class TextColumn {
  private strings: string[] = [];

  // Called during Arrow conversion (cold path)
  flush(): ArrowColumn {
    const result = convertTextColumn(this.strings);

    // CRITICAL: Clear strings after flush
    // Memory is released, GC can collect
    this.strings = [];

    return result;
  }
}
```

**What happens during flush:**

1. All strings converted to Arrow format (dictionary or plain UTF-8)
2. String array cleared (`this.strings = []`)
3. JS GC can collect the original strings
4. Arrow data is serialized/sent (then Arrow buffers released)

**No Global Interner for Either Type:** Neither CATEGORY nor TEXT use a global interner on the hot path:

- Both store raw JS strings during logging
- Dictionary building happens per-flush in cold path
- Memory naturally bounded by flush interval
- SIEVE cache (for UTF-8 encoding) provides cross-flush benefit for repeated strings

**Use Cases:**

```typescript
errorMessage: S.text(); // ✓ Each error might be unique
sqlQuery: S.text(); // ✓ Parameterized queries vary widely
stackTrace: S.text(); // ✓ Unique per error location
requestBody: S.text(); // ✓ JSON payloads are unique
maskedUrl: S.text(); // ✓ URLs are mostly unique
requestId: S.text(); // ✓ Better than CATEGORY for unique IDs
```

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

### Performance and Memory Tradeoffs

**ENUM:**

- ✓ Zero hot path allocations
- ✓ Bounded memory (fixed at schema definition)
- ✓ 1 byte per value (Uint8Array)
- ✓ Fastest queries (pre-sorted dictionary, binary search)
- ✓ UTF-8 allocated ONCE at startup
- ✗ Must know all values at compile time

**CATEGORY:**

- ✓ Zero hot-path overhead (just store string reference)
- ✓ Bounded memory (per-flush, cleared after Arrow conversion)
- ✓ Smart UTF-8 caching in cold path (SIEVE cache)
- ✓ Sorted dictionary for query optimization
- ⚠ String storage in JS heap during logging (not TypedArray)
- ⚠ Dictionary rebuilt each flush (no cross-flush index stability)

**TEXT:**

- ✓ Zero hot path overhead (just store string reference)
- ✓ Bounded memory (per-flush, cleared after Arrow conversion)
- ✓ Optimal cold path (dictionary only if saves >128 bytes)
- ✓ No LRU thrashing for unique values
- ⚠ String storage in JS heap (not TypedArray)
- ⚠ All strings re-encoded to UTF-8 each flush

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
  // Flag name stored in unified `message` column (same as span name / log template)
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
// Your tag attributes schema (user-defined)
const tagAttributes = defineTagAttributes({
  variant: S.category(), // flag variant value
  userId: S.category(),
  duration: S.number(),
  requested: S.number(),
  returned: S.number(),
});

// track() returns chainable API with SAME methods as ctx.tag
// Equivalent to:
// 1. Write entry_type = FF_USAGE
// 2. Write message = 'darkMode'  (unified message column - flag name)
// 3. Write user-defined attributes (from tag schema)
```

**System columns** (defined in systemSchema):

```typescript
// Core system columns
// See 01b_columnar_buffer_architecture.md "Span Definition" for span ID design details
const systemSchema = defineTagAttributes({
  // Trace structure
  timestamp: S.number(), // Microseconds since epoch (Float64Array, SAFE until year 2255)
  traceId: S.category(), // Request trace ID

  // Span identification (separate columns for query flexibility)
  // A span represents a unit of work within a single thread of execution.
  threadId: S.bigint(), // 64-bit random, generated once per worker/process
  spanId: S.number(), // 32-bit counter, unit of work within thread
  parentThreadId: S.bigint().nullable(), // Parent span's thread (null for root spans)
  parentSpanId: S.number().nullable(), // Parent span's ID (null for root spans)

  entryType: S.enum([
    // Entry type enum (1-10, no gaps)
    'span-start', // 1
    'span-ok', // 2
    'span-err', // 3
    'span-exception', // 4
    'info', // 5
    'debug', // 6
    'warn', // 7
    'error', // 8
    'ff-access', // 9
    'ff-usage', // 10
  ]),
  packageName: S.category(), // npm package name from package.json
  packagePath: S.category(), // Path within package, relative to package.json

  // UNIFIED MESSAGE COLUMN - span name, log message template, exception message, result message, OR flag name
  message: S.category(), // See "The message System Column" below

  // Feature flag value column
  ffValue: S.category(), // Flag value - uses category for efficient storage (values repeat: true/false, 'blue'/'green', etc.)
});
```

### The `message` System Column

The `message` column is a **unified column** that serves different purposes based on entry type:

| Entry Type                          | What `message` Contains                                     |
| ----------------------------------- | ----------------------------------------------------------- |
| `span-start`, `span-ok`, `span-err` | **Span name** (e.g., `'create-user'`)                       |
| `span-exception`                    | **Exception message** (e.g., `'TypeError: x is not a fn'`)  |
| `info`, `debug`, `warn`, `error`    | **Log message template** (e.g., `'User ${userId} created'`) |
| `ff-access`, `ff-usage`             | **Flag name** (e.g., `'darkMode'`, `'advancedValidation'`)  |

**Why unified?**

1. **Simpler schema**: One column instead of separate `span_name`, `log_message`, `exception_message`, and `ff_name`
   columns (most would be null)
2. **Better storage**: `S.category()` means string interning - templates/names stored once
3. **Efficient queries**: Find all logs matching a template pattern, or all accesses of a specific flag

**CRITICAL - Format Strings, NOT Interpolation**:

```typescript
// This:
ctx.log.info('User ${userId} processed ${count} items').userId('user-123').count(42);

// Stores:
// - message: 'User ${userId} processed ${count} items'  (template, interned)
// - attr_userId: 'user-123'                             (value, in typed column)
// - attr_count: 42                                      (value, in typed column)

// NOT:
// - message: 'User user-123 processed 42 items'  (interpolated - WRONG!)
```

The template string is stored verbatim. Values go in their typed attribute columns.

See **[Arrow Table Structure](./01f_arrow_table_structure.md)** for detailed examples and query patterns.

**All other attributes** come from your tag schema - same columns, same table structure.

### Deduplication: One ff-access per Span

```typescript
const enabled = ctx.ff.darkMode; // Logs ff-access entry
const enabled2 = ctx.ff.darkMode; // No log - cached for this span

// Tracking is always logged (not deduplicated)
ctx.ff.track('darkMode'); // Logs ff-usage entry
ctx.ff.track('darkMode'); // Logs another ff-usage
```

### Relationship to Scope Attributes

Feature flags use two distinct but related concepts for context management:

1. **Scope Attributes** (`ctx.scope`): For automatic inclusion in log entries
2. **Evaluation Context**: For flag decision-making

These can overlap but serve different purposes:

| Concern            | Scope Attributes                          | Evaluation Context           |
| ------------------ | ----------------------------------------- | ---------------------------- |
| **Purpose**        | Logging context                           | Flag decision-making         |
| **Applied to**     | All entries automatically                 | Flag evaluation only         |
| **Set via**        | `ctx.scope.key = value`                   | `forContext({ ... })`        |
| **Typical values** | userId, requestId, region                 | userId, userPlan, experiment |
| **Can overlap**    | Yes - derive FF context from scope values |

### Scope-to-FF Integration via forContext()

**Key Design**: The `forContext()` method creates a NEW context with a NEW evaluator bound to scope values. This
enables:

- FF evaluators to target specific users/contexts
- Prototype-based context creation for V8 hidden-class optimization
- Clean scope propagation without dynamic property assignment

**Pattern: forContext() creates child context with scope-bound evaluator**

```typescript
// Root context has DefaultFlagEvaluator (no user context yet)
const rootCtx = createRequestContext({
  requestId: 'req-123',
  featureFlagSchema,
  evaluator,
  env,
});

// When scope.userId is set, create child context with FF re-evaluated
const userCtx = rootCtx.forContext({ userId: 'user-123' });
// userCtx.ff is now a NEW evaluator created via:
// rootCtx.ff.forContext({ userId: 'user-123' })

// The new evaluator can target that specific user
const { premiumFeatures } = userCtx.ff; // Evaluated WITH userId context!

// Log entries include scope values automatically
userCtx.log.info('Processing'); // Auto-includes userId from scope

if (premiumFeatures) {
  userCtx.ff.track('premiumFeatures');
}
```

**Why forContext() instead of mutating scope?**

- **Immutability**: Parent context unchanged, child gets new evaluator
- **V8 optimization**: Prototype-based context creation maintains stable hidden classes
- **Fresh cache**: Child evaluator has fresh cache (deduplication per context)
- **Clear ownership**: Each context owns its evaluator instance

### FeatureFlagEvaluator.forContext() Method Signature

The `forContext()` method is the primary API for creating child evaluators with additional context:

```typescript
interface FeatureFlagEvaluator<T extends FeatureFlagSchema> {
  // Flag access - returns primitive values directly (boolean, number, string)
  readonly [K in keyof T]: InferFlagType<T[K]> | undefined;

  // Track flag usage - returns chainable API with SAME methods as ctx.tag
  track(flagName: keyof T): FlagTracker<Tag>;

  /**
   * Create child evaluator with additional/updated evaluation context.
   * Returns a NEW evaluator instance with:
   * - Merged evaluation context (additional overrides parent)
   * - Fresh accessedFlags Set (deduplication per context)
   * - Fresh flagCache Map (no stale values from parent)
   * - Same backend evaluator reference (shared)
   * - Same schema reference (shared)
   */
  forContext(additional: Partial<EvaluationContext>): FeatureFlagEvaluator<T>;

  // Get the current evaluation context (read-only)
  readonly evaluationContext: Readonly<EvaluationContext>;
}
```

**Implementation (in evaluator.ts)**:

```typescript
function forContext(additional: Partial<EvaluationContext>): FeatureFlagEvaluator<T> {
  return createEvaluatorProxy({
    ...state,
    evaluationContext: { ...state.evaluationContext, ...additional },
    // Fresh caches for deduplication in new context
    accessedFlags: new Set(),
    flagCache: new Map(),
    // buffer inherited from parent (can be overridden with withBuffer)
  });
}
```

### V8 Hidden Class Optimization for Context Creation

When creating child contexts via `forContext()`, we use prototype-based creation to maintain V8 hidden class stability:

```typescript
// GOOD: Prototype-based context creation - stable hidden class
function forContext(additional: Partial<EvaluationContext>): SpanContext {
  // Create child with stable prototype chain
  const child = Object.create(this); // Inherits parent prototype

  // Only set properties that differ
  child.ff = this.ff.forContext(additional);
  child.scope = { ...this.scope, ...additional }; // Merged scope

  return child;
}

// BAD: Object spread - creates new hidden class each time
function forContextBad(additional: Partial<EvaluationContext>): SpanContext {
  return {
    ...this, // New hidden class!
    ff: this.ff.forContext(additional),
    scope: { ...this.scope, ...additional },
  };
}
```

**V8 Hidden Class Benefits**:

- **Monomorphic property access**: V8 can inline property lookups when objects share hidden classes
- **Inline cache hits**: Same hidden class = same inline cache, no polymorphic dispatch
- **Lower GC pressure**: Prototype chain avoids copying all properties
- **Stable shapes**: Child contexts share parent's hidden class

**Implementation Pattern**:

```typescript
class RequestContext {
  // Define all properties with stable types
  readonly traceId: string;
  readonly requestId: string;
  readonly ff: FeatureFlagEvaluator;
  readonly env: Env;
  readonly scope: EvaluationContext;

  // Prototype method for child creation
  forContext(additional: Partial<EvaluationContext>): RequestContext {
    // Create via prototype for V8 optimization
    const child = Object.create(Object.getPrototypeOf(this));

    // Copy OWN properties (not prototype methods)
    Object.assign(child, this);

    // Override scope and ff with new values
    child.scope = { ...this.scope, ...additional };
    child.ff = this.ff.forContext(additional);

    return child;
  }
}
```

### Context Flow: How Scope Values Flow to FF Evaluation

```
Request Boundary                    Middleware                         Business Logic
─────────────────────────────────────────────────────────────────────────────────────────

createRequestContext()              ctx.forContext({ userId })         childCtx.ff.premiumFeatures
        │                                     │                                  │
        ▼                                     ▼                                  ▼
┌─────────────────┐              ┌─────────────────────────┐         ┌─────────────────────┐
│ RequestContext  │              │ UserContext             │         │ FF Evaluation       │
│                 │              │                         │         │                     │
│ ff: evaluator   │──forContext──│ ff: evaluator.forCtx()  │────────▶│ context: { userId } │
│ scope: {}       │    ({ userId })  scope: { userId }     │         │ ✓ user targeting    │
│                 │              │                         │         │ ✓ A/B tests         │
└─────────────────┘              └─────────────────────────┘         └─────────────────────┘
        │                                     │
        │                                     │
   No user context                    User identified
   (batch job start)                  (after auth middleware)
```

**Pattern: Derive FF context from scope values in spans**

```typescript
// Set scope at middleware - automatically included in all logs
ctx.scope.userId = req.user?.id;
ctx.scope.region = req.region;

// Option 1: Use forContext() at context level (recommended)
const userCtx = ctx.forContext({ userId: ctx.scope.userId, userPlan: user.plan });
await userCtx.span('processUser', async (childCtx) => {
  const { premiumFeatures } = childCtx.ff; // Evaluated with userId + userPlan
  // ...
});

// Option 2: Pass additionalContext to span (alternative pattern)
await ctx.span(
  'processUser',
  {
    additionalContext: {
      userId: ctx.scope.userId, // Use scope value for FF evaluation
      userPlan: user.plan, // Add FF-specific context
    },
  },
  async (childCtx) => {
    const { premiumFeatures } = childCtx.ff; // Evaluated with full context

    // userId automatically included in log from scope
    childCtx.log.info('Processing'); // Auto-includes userId, region from scope

    if (premiumFeatures) {
      childCtx.ff.track('premiumFeatures');
    }
  }
);
```

**Why separate?**

- **Scope**: Set once, applied everywhere (DRY for logging)
- **EvaluationContext**: Computed per-context based on what's needed for flag decisions
- **Flexibility**: Not all scope values are relevant for FF evaluation, and vice versa
- **Performance**: FF context is flat for V8 optimization (see below)

### V8 Optimization Considerations

The evaluator implementation is designed for V8's hidden class optimizations:

**Evaluator Instances are Proxy Objects**

```typescript
// Each span gets a NEW evaluator instance (via forContext)
const parentEvaluator = ffEvaluator.forContext(parentCtx);
const childEvaluator = ffEvaluator.forContext(childCtx);

// These are DIFFERENT instances with:
// - Fresh per-span caches (accessedFlags, valueCache)
// - Stable hidden class (same shape for V8)
```

**Creating New Instances with forContext() / withBuffer()**

```typescript
// Each creates a NEW instance
const evaluator1 = ffEvaluator.forContext(ctx);
const evaluator2 = evaluator1.forContext({ userId: 'user-123' });
const evaluator3 = evaluator2.withBuffer(childBuffer);

// Benefits:
// - Fresh cache (no stale values)
// - Stable shape (V8 can optimize property access)
// - Immutability (parent evaluator unchanged)
```

**EvaluationContext is Flat for Single Hidden Class**

```typescript
// GOOD: Flat structure - single hidden class
interface EvaluationContext {
  userId?: string;
  userPlan?: string;
  region?: string;
  experimentGroup?: string;
  [key: string]: string | number | boolean | undefined;
}

// BAD: Nested objects - multiple hidden classes, slower property access
interface BadEvaluationContext {
  user: {
    // Hidden class 1
    id: string;
    plan: string;
  };
  request: {
    // Hidden class 2
    region: string;
  };
}
```

**Performance Benefits**

- **Flag access**: <0.1ms including proxy intercept, cache check, evaluation
- **Minimal GC pressure**: Evaluator instances are lightweight, caches are Map/Set
- **Monomorphic property access**: V8 can inline property lookups on flat context
- **Cache locality**: All context fields in single object for better memory access

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
    childCtx.ff.track('premiumFeatures');
  }
});
```

#### FeatureFlagEvaluator.forContext() Method

The evaluator provides a `forContext()` method to create child evaluators with additional context:

```typescript
interface FeatureFlagEvaluator<T extends FeatureFlagSchema, Tag extends TagAttributeSchema> {
  // Flag access - returns primitive values directly (boolean, number, string)
  readonly [K in keyof T]: InferFlagType<T[K]>;

  // Track flag usage - returns chainable API with SAME methods as ctx.tag
  // Writes to same columns, unified table structure
  track(flagName: keyof T): FlagTracker<Tag>;

  // Create child evaluator with additional/updated context
  // Returns a new evaluator instance with merged context
  forContext(additional: Partial<EvaluationContext>): FeatureFlagEvaluator<T, Tag>;

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

**Why `forContext()` on the evaluator (not RequestContext)**:

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
    .forContext(additionalContext) // Merge context
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
    buffer.message[idx] = String(flagName); // Unified message column

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
    buffer.message[idx] = flagName; // Unified message column
    buffer.attr_ffValue[idx] = String(value); // S.category - raw string, dict built in cold path
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

- Same column names (user-defined attributes like `attr_variant`, not `attr_ff_variant`)
- Same Arrow table structure - no schema split
- Flag name stored in unified `message` column (consistent with span names and log templates)
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
- Consistent chainable API: `ctx.ff.track('flag').variant(value)` (user-defined attributes)
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
    packageName: '@mycompany/user-service',
    packagePath: 'src/services/user.ts',
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
    ctx.ff.track('advancedValidation');
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
          childCtx.ff.track('premiumFeatures');
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
