# Buffer Code Generation and Extension <a id="smoo/lmao!n/buffer-codegen"></a>

> **Part of [Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**

This document covers the code generation mechanism that allows `lmao` to extend `arrow-builder`'s buffer classes with
span-specific functionality while maintaining V8 optimization.

## Overview

The system uses runtime code generation (`new Function()`) to create specialized buffer classes, with a closure-composed
materializer for runtimes where a class build may not run string codegen
([Materializer Selection](#smoo/lmao!n/buffer-codegen.materializer-selection)):

1. **arrow-builder** provides the extension interface and class generator
2. **lmao** provides span-specific code via callbacks
3. Generated classes have fixed hidden class shapes for V8 optimization

### Materializer Selection <a id="smoo/lmao!n/buffer-codegen.materializer-selection"></a>

arrow-builder's `ColumnBuffer`/`ColumnWriter` and lmao's `SpanLogger`/`ResultWriter`/`WasmSpanBuffer` classes are each
built by one of two materializers from the same schema-derived plan:

- **compiled**: renders class source and evaluates it with `new Function`;
- **closure**: installs per-field closures on one shared prototype per schema, with no source text. It produces
  byte-identical buffers (pinned by the parity suites) and rejects code-string extension fields that have no
  `closureInit`/`closureMethods` counterpart.

Classes are built lazily, per schema, on first use; on a server that first use is usually inside a request. The choice
therefore answers "may every context that builds a class run `new Function`", not "may module init run it". workerd
allows code generation while a worker's top-level modules evaluate and refuses it inside a request
(`EvalError: Code generation from strings disallowed for this context`), so a module-init probe alone answers wrongly
there.

`packages/arrow-builder/src/lib/buffer/codegenCapability.ts` decides once, at module init, and every build site reads
that single decision through `activeMaterializerMode()`:

1. workerd selects **closure** without probing. Either of two documented Workers runtime signals identifies it:
   `navigator.userAgent === 'Cloudflare-Workers'` (global_navigator, default since compatibility date 2022-03-21;
   miniflare reports the same) and the `WebSocketPair` global, which the Workers WebSockets API defines under every
   compatibility date and flag. `WebSocketPair` identifies the isolates that have no `navigator` (compatibility date
   before 2022-03-21, or `no_global_navigator`).
2. Otherwise a `new Function` probe selects **compiled** when it succeeds and **closure** when it fails (strict CSP).
   Runtimes that allow codegen at init allow it everywhere (bun, node, browsers).

The decision is fixed for the process, so one schema never has both class kinds live. Build sites do not catch
`EvalError`: a refusal on the compiled path is a selection bug and surfaces as one. `setMaterializerModeOverride` forces
a mode for parity suites (forcing `compiled` where codegen is unsupported throws), and class caches are keyed by mode.
`packages/arrow-builder/src/lib/buffer/__tests__/workerdRequestCodegen.test.ts` (ColumnBuffer/ColumnWriter) and
`packages/lmao/src/lib/__tests__/workerd-request-codegen.test.ts` (every class above) reproduce workerd's policy in a
fresh process — codegen allowed while modules evaluate, refused afterwards — and require each class to build and write,
both when `navigator` identifies workerd and when only `WebSocketPair` does. With neither signal the same process fails
with `EvalError` on its first class build, which shows that the refusal reaches the compiled path.

## ColumnBuffer Extension Mechanism <a id="smoo/lmao!n/buffer-codegen-extension-mechanism"></a>

**Purpose**: Enable arrow-builder to provide extensible buffer generation while allowing lmao to inject span-specific
functionality without tight coupling.

**PACKAGE BOUNDARY**: This mechanism is the key to maintaining clean separation:

- **arrow-builder** owns the `ColumnBufferExtension` interface and `getColumnBufferClass()` function
- **lmao** provides the extension with span-specific code (identity, tree structure, context)
- arrow-builder has NO knowledge of what the extension code does

### Extension Options Interface <a id="smoo/lmao!n/buffer-codegen.extension-options"></a>

```typescript
interface ColumnBufferExtension {
  /** Runtime-selected columns allocated eagerly while retaining nullable semantics. */
  preallocatedColumns?: readonly string[];

  /** Constructor parameters after `requestedCapacity`, e.g. `'stats, parent, isChained'`. */
  constructorParams?: string;

  /** Code added to the constructor after system columns are initialized. May declare properties on `this`. */
  constructorCode?: string;

  /** Code added at the top of the constructor body, before system column initialization. */
  constructorPreamble?: string;

  /** Methods added to the class body, as source text. */
  methods?: string;

  /** Code added before the class definition (module-level helpers and constants). */
  classPreamble?: string;

  /** Runtime values injected into the generated class closure; keys are variable names in the code strings. */
  dependencies?: Record<string, unknown>;

  /** No-eval counterpart of `constructorPreamble` (+ `constructorParams`) for the closure-composed materializer. */
  closureInit?: (self: Record<string, unknown>, requestedCapacity: number, ctorArgs: readonly unknown[]) => void;

  /** No-eval counterpart of `methods` (+ `classPreamble`) for the closure-composed materializer. */
  closureMethods?: (prototype: object) => void;
}
```

Properties are declared inside `constructorCode`, so they land in the fixed hidden-class shape directly.

### Usage Example (lmao extending arrow-builder)

```typescript
// PACKAGE: lmao (packages/lmao/src/lib/spanBuffer.ts)
// This code lives in lmao but calls arrow-builder's getColumnBufferClass()
const SpanBufferClass = getColumnBufferClass(schema, {
  constructorParams: 'stats, parent, isChained, callsiteMetadata, opMetadata, traceRoot, vocabularyGeneration',

  dependencies: { copyThreadIdTo, getThreadId, EMPTY_SCOPE, checkCapacityTuning },

  // spanId and threadId feed the identity bytes; see 01b5 §Constructor Implementation
  constructorPreamble: `
    const spanId = ++globalThis.globalSpanCounter;
    const threadId = isChained ? parent.thread_id : getThreadId();
    this._parent = isChained ? parent._parent : parent;
  `,

  methods: `
    isParentOf(other) {
      return this === other._parent;
    }
    isChildOf(other) {
      return this._parent === other;
    }
  `,
});
```

### Generated Class Structure

```typescript
// Generated by arrow-builder with lmao extensions
class GeneratedSpanBuffer {
  // --- Core ColumnBuffer properties (from arrow-builder) ---
  timestamp: BigInt64Array;
  entry_type: Uint8Array;
  _writeIndex: number;
  _capacity: number;

  // --- Lazy attribute columns (from schema) ---
  // userId_nulls, userId_values, etc.

  // --- Extension properties (from lmao) ---
  _system: ArrayBuffer; // timestamps, entry_type, and identity bytes
  _identity: Uint8Array; // threadId and spanId bytes (plus trace id on ROOT); CHAINED buffers share the parent's
  _parent: SpanBuffer | undefined;
  _traceRoot: ITraceRoot;
  _overflow: SpanBuffer | undefined;
  _callsiteMetadata: OpMetadata | undefined;
  _opMetadata: OpMetadata;
  _vocabularyGeneration: VocabularyGeneration;
  _nodeIndex: number; // Node in _traceRoot._topology; child links live in the topology arena
  _topologyGeneration: number;

  // Identity accessors (lmao)
  get span_id() {
    const b = this._identity;
    return (b[8] | (b[9] << 8) | (b[10] << 16) | (b[11] << 24)) >>> 0;
  }
  get thread_id() {
    return this._threadId;
  }
  get trace_id() {
    return this._traceRoot.trace_id;
  }

  constructor(
    requestedCapacity,
    stats,
    parent,
    isChained,
    callsiteMetadata,
    opMetadata,
    traceRoot,
    vocabularyGeneration
  ) {
    // Constructor body: span counter, identity, system columns (see 01b5 §Constructor Implementation)
  }

  // Extension methods (lmao)
  isParentOf(other) {
    return this === other._parent;
  }
}
```

### Lazy Column Getter Implementation <a id="smoo/lmao!n/buffer-codegen.generate-class"></a>

Attribute columns use lazy getters that allocate shared ArrayBuffers on first access:

```typescript
// Generated at module creation time (cold path)
// Example for userId attribute (category type, Uint32Array):

class GeneratedColumnBuffer {
  constructor(requestedCapacity) {
    // Initialize system columns (always allocated)
    this.timestamp = new BigInt64Array(alignedCapacity);
    this.entry_type = new Uint8Array(alignedCapacity);

    // Lazy columns initialized as undefined (not allocated yet)
    this._userId_nulls = undefined;
    this._userId_values = undefined;
  }

  // Lazy getter - allocates BOTH nulls and values on first access
  get userId_nulls() {
    if (this._userId_nulls === undefined) {
      // Allocate shared ArrayBuffer for nulls + values
      // Calculate aligned sizes for cache line optimization
      // Create TypedArray views into shared buffer
      // Set both _userId_nulls and _userId_values
    }
    return this._userId_nulls;
  }

  // Values getter triggers allocation via nulls getter
  get userId_values() {
    if (this._userId_values === undefined) this.userId_nulls; // Trigger allocation
    return this._userId_values;
  }

  // Setter method writes to TypedArrays and updates null bitmap
  userId(pos, val) {
    if (val == null) {
      // Clear null bit (mark as null)
    } else {
      // Set value and mark as valid in null bitmap
    }
    return this;
  }

  // Helpers to check allocation without triggering it
  getColumnIfAllocated(columnName) {
    return this[`_${columnName}_values`];
  }
}
```

**Key Design Points**:

1. **Shared ArrayBuffer**: Each column uses ONE ArrayBuffer for both nulls and values
2. **Cache Alignment**: Null bitmap end is aligned to value type's element size
3. **Direct Properties**: Uses `this._userId_nulls` and `this._userId_values` (not Symbols)
4. **Inline Allocation**: Allocation happens directly in the getter, not via separate allocator function
5. **Zero Indirection**: Direct property access (`buffer.userId_values[i]`)
6. **V8 Optimization**: Generated classes optimize well with hidden classes and inline caching

### V8 Hidden Class Optimization

The extension mechanism ensures a **fixed hidden class shape**:

1. All properties declared upfront in constructor
2. No dynamic property addition after construction
3. Methods on prototype (not instance)
4. Consistent property order across all instances

This enables V8 to:

- Create optimized hidden class once
- Use inline caching for property access
- Avoid dictionary mode fallback

### Benefits Summary

| Aspect               | Without Extension            | With Extension                |
| -------------------- | ---------------------------- | ----------------------------- |
| **Package Coupling** | lmao must fork arrow-builder | Clean extension via callbacks |
| **Hidden Class**     | May vary per use case        | Fixed shape, V8 optimized     |
| **Code Generation**  | Duplicate logic              | Single codegen with injection |
| **Maintenance**      | Two codebases to update      | Single source of truth        |

## Schema-Generated Buffer Extensions <a id="smoo/lmao!n/buffer-codegen-schema-generated"></a>

**Purpose**: Extend the base buffer with typed columns based on tag attribute schemas.

**PACKAGE**: This section describes **lmao**-level code that uses arrow-builder's extension mechanism. The factory
functions (`createSpanBuffer`, `getSpanBufferClass`) live in `packages/lmao/src/lib/spanBuffer.ts`.

**See Also**: [Trace Schema System](./01a_trace_schema_system.md) for how schemas are defined.

### Composed Buffer Example

```typescript
// Generated from composed schema (HTTP + DB + user attributes)
// Each attribute becomes DIRECT properties via lazy getters on buffer
interface ComposedSpanBuffer extends SpanBuffer {
  // HTTP library attributes (prefix prevents conflicts)
  http_status: Uint16Array;
  http_method: Uint8Array; // enum index
  http_url: Uint32Array; // string registry index

  // Database library attributes
  db_query: Uint32Array; // string registry index (masked SQL)
  db_duration: Float32Array;

  // User-defined attributes (no prefix for regular modules)
  user_id: Uint32Array; // hashed
  business_metric: Float64Array;
}
```

### createSpanBuffer Factory <a id="smoo/lmao!n/buffer-codegen-create-factory"></a>

`createSpanBuffer` (`packages/lmao/src/lib/spanBuffer.ts`) takes
`(schema, traceRoot, opMetadata, capacity?, plannedClass?)`. A compiler-planned class
(`opMetadata._physicalLayoutPlan.SpanBufferClass`) is used when present, and its capacity hint is validated when the
runtime hint is decoded. Direct calls resolve capacity with `resolveSpanBufferCapacity`, which applies
`MIN_ADAPTIVE_BUFFER_CAPACITY` (8) when no explicit capacity is given and rejects explicit capacities below 2. The
8-aligned capacity rounding lives in arrow-builder's `getAlignedCapacity`, invoked inside the generated constructor, not
in this factory. The root buffer is registered with its trace's topology (`traceRoot._topology.registerRoot`). The
factory counts the span in the class's `stats.spansCreated`; `createChildSpanBuffer` counts the same way and registers
the child with `_topology.registerChild`, and `createOverflowBuffer` never counts, because an overflow buffer continues
an existing span.

```typescript
/**
 * Creates a root SpanBuffer for a new trace and registers it with the trace topology.
 * Uses runtime-generated class with all span properties built in.
 */
export function createSpanBuffer<T extends LogSchema>(
  schema: T,
  traceRoot: ITraceRoot,
  opMetadata: OpMetadata,
  capacity?: number,
  plannedClass?: SpanBufferConstructor<T>
): SpanBuffer<T> {
  const metadataPlan = opMetadata._physicalLayoutPlan;
  const metadataSpanBufferClass = metadataPlan?.SpanBufferClass;
  if (metadataSpanBufferClass !== undefined && !isSpanBufferConstructorForSchema(metadataSpanBufferClass, schema)) {
    throw new TypeError('Planned SpanBuffer class does not match schema');
  }
  const plannedSpanBufferClass = plannedClass ?? metadataSpanBufferClass;
  const SpanBufferClass = plannedSpanBufferClass ?? getSpanBufferClass(schema);
  const stats = SpanBufferClass.stats;

  const actualCapacity =
    plannedSpanBufferClass === undefined
      ? resolveSpanBufferCapacity(capacity, stats.capacity)
      : (capacity ?? Math.max(MIN_ADAPTIVE_BUFFER_CAPACITY, stats.capacity));

  // Track non-chained buffer creation for capacity tuning
  stats.spansCreated++;

  // Create root buffer (no parent), then register its stable logical node.
  const buffer = new SpanBufferClass(
    actualCapacity,
    stats,
    undefined,
    false,
    opMetadata,
    opMetadata,
    traceRoot,
    metadataPlan?.vocabularyGeneration ?? getVocabularyGeneration()
  );
  traceRoot._topology.registerRoot(buffer);
  return buffer;
}
```

### getSpanBufferClass Generator <a id="smoo/lmao!n/buffer-codegen-get-class"></a>

`getSpanBufferClass` (`packages/lmao/src/lib/spanBuffer.ts`) generates one class per schema and cache key (materializer
mode, message layout family, physical layout, eager-column key), cached in a `WeakMap` keyed by schema. The
`constructorParams` are `stats, parent, isChained, callsiteMetadata, opMetadata, traceRoot, vocabularyGeneration`, and
the injected `dependencies` are `copyThreadIdTo`, `getThreadId`, `EMPTY_SCOPE`, and `checkCapacityTuning`. The span
counter is `globalThis.globalSpanCounter`, module-level and per worker. The full constructor body lives in
[01b5 §Constructor Implementation](./01b5_spanbuffer_memory_layout.md#smoo/lmao!n/spanbuffer-layout.constructor).

Key optimizations in the generated class:

- `globalThis.globalSpanCounter` is module-level (per-worker) and read in the constructor preamble
- All span properties set in constructor code via extension
- `thread_id` comes from module-level singleton
- No post-creation property assignment

```typescript
export function getSpanBufferClass<T extends LogSchema>(
  schema: T,
  messageLayoutFamily: MessageLayoutFamily = 'mixed',
  messagePhysicalLayout: MessagePhysicalLayout = 'current',
  eagerColumns: EagerColumnDescriptor = EMPTY_EAGER_COLUMNS
): SpanBufferConstructor<T> {
  const cacheKey = `${activeMaterializerMode()}:${messageLayoutFamily}:${messagePhysicalLayout}:${eagerColumns.key}`;
  let familyClasses = spanBufferClassCache.get(schema);
  const cached = familyClasses?.get(cacheKey);
  if (cached) return cached;

  const extension: ColumnBufferExtension = {
    preallocatedColumns: eagerColumns.names,
    constructorParams: 'stats, parent, isChained, callsiteMetadata, opMetadata, traceRoot, vocabularyGeneration',
    dependencies: { copyThreadIdTo, getThreadId, EMPTY_SCOPE, checkCapacityTuning },
    // constructorPreamble (pure JavaScript; full body in 01b5 §Constructor Implementation) sets the span counter,
    // thread id, system buffer sizing per layout, and identity per buffer kind (CHAINED shares parent._identity;
    // CHILD writes its span id; ROOT uses traceRoot._traceIdBytes), then assigns the hot properties first.
    // methods (pure JavaScript): span_id, thread_id, and trace_id getters; isParentOf; isChildOf; message(pos, val).
    closureInit: createSpanBufferClosureInit(messageLayoutFamily, messagePhysicalLayout),
    closureMethods: createSpanBufferClosureMethods(messageLayoutFamily),
  };

  // The storage schema partitions the source schema, message family, and physical layout.
  const storageSchema = getStorageSchema(schema, messageLayoutFamily, messagePhysicalLayout);
  const generatedClass = getColumnBufferClass(storageSchema, extension, storageSchema);
  if (!isGeneratedSpanBufferClass<T>(generatedClass)) {
    throw new TypeError('Generated column buffer is missing SpanBuffer methods');
  }

  const SpanBufferClass = createSpanBufferConstructor(
    schema,
    messageLayoutFamily,
    messagePhysicalLayout,
    generatedClass,
    eagerColumns
  );
  familyClasses ??= new Map();
  familyClasses.set(cacheKey, SpanBufferClass);
  spanBufferClassCache.set(schema, familyClasses);
  return SpanBufferClass;
}
```

### createOverflowBuffer (Overflow Chaining) <a id="smoo/lmao!n/buffer-codegen-overflow"></a>

`createOverflowBuffer` (`packages/lmao/src/lib/spanBuffer.ts`) creates the continuation buffer when the current buffer
fills. It reads capacity from the class's static `stats.capacity`, 8-aligned, and passes the original buffer as `parent`
so the chain shares identity. It copies `_callsiteMetadata`, `_opMetadata`, `_traceRoot`, and `_vocabularyGeneration`
from the original, links the chain through `buffer._overflow`, and registers the link with
`buffer._traceRoot._topology.adoptOverflow`.

```typescript
/**
 * Creates a continuation buffer when the current buffer overflows.
 * Chained buffers SHARE the identity reference from the first buffer (same logical span).
 */
export function createOverflowBuffer<T extends LogSchema>(buffer: SpanBuffer<T>): SpanBuffer<T> {
  const SpanBufferClass = getSpanBufferConstructorForBuffer(buffer);
  const stats = SpanBufferClass.stats;

  // Ensure capacity is multiple of 8 for byte-aligned null bitmaps
  const capacity = (stats.capacity + 7) & ~7;

  const nextBuffer = new SpanBufferClass(
    capacity,
    stats,
    buffer, // parent: used for identity sharing; _parent is set to buffer._parent in the constructor
    true, // isChained
    buffer._callsiteMetadata,
    buffer._opMetadata,
    buffer._traceRoot,
    buffer._vocabularyGeneration
  );

  buffer._overflow = nextBuffer;
  buffer._traceRoot._topology.adoptOverflow(buffer, nextBuffer);
  return nextBuffer;
}
```

## Design Rationale

**Why This Design**:

- **Zero indirection**: Direct property access faster than nested Record lookup
- **Lazy allocation**: Columns allocated on first access via lazy getters
- **Shared ArrayBuffer**: Each column's nulls and values share ONE ArrayBuffer
- **Per-span buffers**: Each span gets own buffer for sorted logs
- **No trace_id/span_id arrays**: Constant per buffer, stored in `_identity` bytes and read through getters
- **Type safety**: Schema drives column generation and TypeScript types
- **Conflict prevention**: Optional prefixes (e.g., `http_`, `db_`) prevent name conflicts

**Scope Values** (SEPARATE from buffer):

Scope values are stored in a **separate Scope class** (see
[01i_span_scope_attributes.md](./01i_span_scope_attributes.md)), NOT in buffer columns. The Scope class is generated via
`new Function()` and applied to buffer columns during writes or pre-fill operations.

## Related Documents

- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: Parent document, buffer overview
- **[SpanBuffer Memory Layout](./01b5_spanbuffer_memory_layout.md)**: Memory diagrams, system column layout
- **[Buffer Performance Optimizations](./01b1_buffer_performance_optimizations.md)**: V8 optimization patterns
- **[Trace Schema System](./01a_trace_schema_system.md)**: Schema definitions (S.enum, S.category, etc.)
- **[Module Context and SpanLogger Generation](./01j_module_context_and_spanlogger_generation.md)**: SpanLogger class
  generation
