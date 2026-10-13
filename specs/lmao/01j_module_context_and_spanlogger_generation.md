# Module Context and Op/SpanLogger Generation

## Overview

The op context system provides the foundation for creating ops with generated TagWriter and SpanLogger classes. It
handles:

1. **OpMetadata** - per-op configuration and source attribution, one per `defineOp`
2. **SpanBuffer** - per-span buffer with span name, line number, and op metadata reference
3. **Op class generation** for traced operations with proper type parameters
4. **TagWriter class generation** with typed attribute methods (for `tag`)
5. **SpanLogger class generation** with log methods (for `log`)
6. **Schema compilation** for both standard and library modules
7. **User-extensible context** via the `ctx` field of `defineOpContext()` for custom properties

This system performs code generation at build/startup time; warmed, in-capacity entry writes are allocation-free.

## Physicalization and Cache Lifecycle

All schema- and backend-dependent decisions move to startup.

Startup canonicalizes the schema and creates one immutable `PhysicalLayoutPlan` containing concrete columns, offsets,
widths, null bitmaps, eager/lazy decisions, static message-index lanes, dynamic reference lanes, and backend factories.
One plan serves both backends: it carries the JS SpanBuffer class and the WASM layout template, so JS and WASM spans of
one callsite share it. Plans are cached by **schema identity**, then by **SpanContext class**, then by a key of the
**physical layout version**, runtime hint, message layout family, message physical layout, context layout, vocabulary
generation, eager columns, and local message dictionary. The cache shares the plan and its buffer/writer/flush classes.
Library prefix/remap state is an immutable binding over that physical plan, never a name lookup performed by a warmed
callsite.

Generated APIs are specialized into **static**, **structured**, or **dynamic** callsite classes. After warmup a callsite
sees one stable receiver shape, one fixed entry kind, one fixed representation, and a fixed sequence of direct stores.
`mixed` is reserved for a physical buffer/layout containing both a static-index lane and a dynamic-reference lane; it is
not a callsite class. The warmed entry path MUST NOT enumerate fields, resolve schemas, compute property names, look up
vocabulary, choose a backend, render strings, or allocate. Capacity failure branches to an explicit overflow/slow-path
helper and must not pollute the in-capacity path with changing shapes.

Allocation claims are phase-specific:

| Phase                  | Contract                                                                                                          |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------- |
| **Startup**            | May allocate heavily to canonicalize schemas, register vocabulary, build plans/classes, and reserve arenas.       |
| **Span setup**         | Minimize and amortize allocations; do not describe it as the warmed entry path.                                   |
| **Warmed entry write** | Zero allocation and fixed monomorphic stores while capacity remains.                                              |
| **Overflow/slow path** | May allocate or grow explicitly; behavior and counters are predictable.                                           |
| **Flush**              | Few or no allocations by reusing layout/backend classes, scratch arenas, dictionary generations, and Arrow views. |

Flush returns an `ArrowLease` over runtime-owned Arrow-compatible buffers rather than transferring untracked reusable
storage. The lease holds the trace topology, the JS-heap chunks a table borrows, and the exact immutable vocabulary
dictionary generation until the exporter releases it; it never pins a module's caller-owned fragment arrays. This
permits near-zero-allocation trace→Arrow conversion without recycling memory still observed by a consumer.

The flush unit is every root span tree registered with the flush scheduler since the previous flush. Each tree is
converted to its own leased Arrow table, and the tables are merged and handed to the flush handler. A flush runs on a
time interval, a capacity threshold, idle detection, memory pressure, or an explicit `flush()`. WASM-backed columns are
copied into Arrow-owned storage, so a table never views WASM memory; the memory epoch is pinned only during the
synchronous conversion, and WASM views are re-derived after linear-memory growth
([WASM Memory Architecture](./01q_wasm_memory_architecture.md#smoo/lmao!n/wasm-mem.lifecycle)).

## Compiler-Emitted Vocabulary Registration

Compiled application modules and precompiled JS libraries self-register their own vocabulary fragment inputs. The
application bootstrap MUST import and evaluate the LMAO runtime installer before evaluating any registering module. The
runtime installer owns the callback stored at `globalThis[Symbol.for('@smoothbricks/lmao/vocabulary/register/v1')]`;
emitted modules call it but never install or replace it.

The runtime type contract is:

```typescript
interface VocabularyFragmentV1 {
  schemaVersion: 1;
  idAlgorithm: 'sha256-32-v1';
  contentHash: ContentHash;
  ids: Uint32Array;
  kindTags: Uint8Array;
  utf8: Uint8Array;
  offsets: Int32Array;
}

type VocabularyBinding = Uint32Array;
type RegisterVocabularyV1 = (fragment: VocabularyFragmentV1) => VocabularyBinding;
```

Compiler-emitted JavaScript is governed by the runtime contract above and need not typecheck as a TypeScript
declaration. The compiler emits the installer import before registration; ESM dependency evaluation installs the
callback before the transformed module body executes, and bundlers MUST retain both side effects:

```javascript
import '@smoothbricks/lmao/vocabulary/register/v1';

const REGISTER_VOCABULARY = Symbol.for('@smoothbricks/lmao/vocabulary/register/v1');
const binding = globalThis[REGISTER_VOCABULARY](fragment);
const denseMessageIndex = binding[ordinal]; // warmed direct typed-array load
```

For `N` records, `ids.length === kindTags.length === N` and `offsets.length === N + 1`. `offsets[0]` is zero,
`offsets[N]` is `utf8.byteLength`, and adjacent entries delimit one complete record. Each record uses this exact
little-endian grammar:

```text
u32le textLength
u8[textLength] textUtf8
u16le fieldCount
repeat fieldCount times:
  u16le nameLength
  u8[nameLength] nameUtf8
  u16le columnLength
  u8[columnLength] columnUtf8
```

For record `i`, `ids[i]` is the first 32 SHA-256 bits, interpreted big-endian, over `kindTags[i] || recordBytes`.
Registration follows the
[01o fragment contract](./01o_typescript_transformer.md#vocabulary-fragment-and-library-without-source-contract),
including idempotence and collision resolution. `contentHash` is lowercase SHA-256 hex over this exact canonical byte
stream:

```text
u8 schemaVersion
u16le algorithmLength | u8[algorithmLength] idAlgorithmUtf8
u32le ids.length | repeated(ids.length times: u32le id)
u32le kindTags.length | u8[kindTags.length] kindTags
u32le utf8.length | u8[utf8.length] utf8
u32le offsets.length | repeated(offsets.length times: i32le offset)
```

This canonical serialization, rather than host TypedArray byte order, defines fragment identity.

`kindTags[i] = 1` is `LOG_TEMPLATE`; `kindTags[i] = 2` is `SPAN_NAME`. Checker-proven literal span names register as
`SPAN_NAME`; non-literal span names remain dynamic references. Static templates and structured templates are registered;
dynamic strings are not.

The registry validates versions, lengths, offsets, record grammar, IDs, kind tags, and `contentHash`, then copies/merges
the fragment into a runtime-owned immutable dictionary generation. Caller-owned typed arrays are registration inputs,
not borrowed mutable Arrow storage. Dense Arrow indices are process-local and append-only in fragment registration
order. A new generation preserves the previous generation as an unchanged prefix and appends only unseen decoded values,
so old bindings remain prefix-valid. Ids, record bytes and decoded values are deterministic; dense indices and
dictionary ordering are not deterministic across processes. Dense index `0` is valid; null is represented only by the
Arrow validity bitmap.

The returned `VocabularyBinding` maps each fragment-local ordinal through direct `binding[ordinal]` indexing. Generated
code MUST NOT call a warmed `messageIndex()` helper. An `ArrowLease` pins the exact runtime-owned immutable dictionary
generation used by its Arrow output, never a registering module's arrays.

Libraries retain ownership of their emitted fragment declarations and registration calls. An application may prefix or
bind a library's schema columns, but MUST NOT copy, renumber, or synthesize that library's vocabulary. Duplicate
evaluation is idempotent for the same schema version, algorithm, content hash, and decoded fragment; invalid or
incompatible input fails during startup rather than falling back to hot-path string lookup. Detailed transformer
emission is specified elsewhere.

Template placeholders use `{fieldName}`; `{{` and `}}` encode literal braces. `{{fieldName}}` is not accepted as an
alias. A `LOG_TEMPLATE` record's `textUtf8` is the template exactly as written, escapes included; readers decode `{{`
and `}}` only when rendering. `info`/`warn`/`error` callsites bind literal templates and object-literal fields;
`debug`/`trace` may bind a literal template or use an existing raw dynamic string reference, but avoidable interpolation
remains invalid. Callsite classes are static, structured, or dynamic. A physical layout may be mixed when it carries
both static-index and dynamic-reference lanes.

## Context Hierarchy <a id="smoo/lmao!n/opcontext-hierarchy"></a>

```
OpMetadata (interface) - ONE per defineOp
├── name: string (op name for metrics)
├── package_name / package_file / git_sha: string
├── line: number (line of the defineOp call)
├── package_name_entry / package_file_entry / git_sha_entry: PreEncodedEntry (UTF-8 cached)
└── _physicalLayoutPlan → SpanBuffer class, eager columns, vocabulary generation, WASM layout

LogBinding - ONE per defineOpContext
├── logSchema (tag attribute definitions)
└── remapDescriptor? (prefixed or remapped libraries)

SpanBuffer class - ONE per schema and physical layout
└── stats: SpanBufferStats { capacity, totalWrites, spansCreated } (self-tuning; flushed as buffer-* rows)

SpanBuffer - ONE per span (internal interface)
├── _callsiteMetadata?: OpMetadata ← caller's metadata (where span() was invoked) - for row 0 metadata
├── _opMetadata: OpMetadata         ← op's metadata (what code is executing) - for rows 1+ metadata
├── message_values[0]: string       ← span name written by writeSpanStart()
├── line_values: Float64Array       ← line numbers per row (written directly, NOT stored as property)
├── _parent?: SpanBuffer            ← reference to parent (child spans walk this for trace_id)
├── _nodeIndex: number             ← node in _traceRoot._topology (child links live there)
├── trace_id (getter)               ← root stores it, children walk parent chain
└── columns, writeIndex, etc.

SpanContext (interface) - user-facing, what ops receive
├── tag: TagWriter<T>
├── log: SpanLogger<T>
├── setScope() / scope
├── ok() / err()
├── span()
├── buffer (getter)
├── ff: FeatureFlagEvaluator
└── deps: Deps
```

**Key Design: Dual Metadata References for Source Attribution**

Each SpanBuffer has TWO metadata references for different purposes:

- **`_callsiteMetadata`**: The caller's op metadata - where `span()` was invoked. Used for **row 0 (span-start)**
  metadata (`git_sha`, `package_name`, `package_file`).
- **`_opMetadata`**: The executing op's metadata - what code is executing. Used for **rows 1+ (span-ok/err/exception,
  logs)** metadata (`git_sha`, `package_name`, `package_file`).

This enables accurate source attribution: the span-start entry records WHERE the span was invoked from (callsite), while
subsequent entries record WHERE the code is actually executing (the Op's module).

**Key Design: lineNumber is NEVER a property on any object**

Line numbers flow directly from transformer injection to TypedArray writes:

```typescript
// Transformer output:
await span(42, 'fetch-user', userLib.fetchUser, userId);
//         ^^ lineNumber argument

// Inside span():
buffer.line_values[0] = 42; // Direct TypedArray write for row 0, that's it

// For logs:
log.info('Processing').line(55); // .line(N) writes to line_values[writeIndex]
```

The lineNumber is passed as an argument and written directly to `line_values` TypedArray at the appropriate row index.
There is NO intermediate storage of lineNumber on any context object.

## Design Rationale: From ctx to Destructured SpanContext

### Problem with ctx Parameter

An alternative approach would pass a monolithic `ctx` object:

```typescript
// Alternative approach (rejected): ctx drilling everywhere
const createUser = defineOp('createUser', async (ctx, userData) => {
  ctx.tag.userId(userData.id);
  ctx.log.info('Creating user');
  const validated = await ctx.span('validate', validateUser, userData); // Pass ctx to everything
  return validated.isOk() ? ctx.ok(validated.value) : ctx.err(validated.error);
});
```

### Solution: Destructured SpanContext

The chosen design uses ops with destructuring:

```typescript
// Destructure what you need
const createUser = defineOp('createUser', async ({ span, log, tag, ok, err }, userData: UserData) => {
  tag.userId(userData.id);
  log.info('Creating user');
  const validated = await span('validate', validateUser, userData); // span() handles context
  return validated.isOk() ? ok(validated.value) : err(validated.error);
});
```

**Benefits**:

- **No ctx drilling**: Just destructure what you need
- **Span name at call site**: `span('validate', ...)` - caller names it
- **Clean signatures**: `({ span, log, tag }, userData)` instead of `(ctx, userData)`
- **Deps destructuring**: `const { retry, auth } = deps`
- **User-extensible**: Add `env` or other properties via the `ctx` config field

## Module Definition with defineOpContext() <a id="smoo/lmao!n/opcontext-definition"></a>

**Purpose**: Set up module-level configuration shared across all ops in the module.

```typescript
// Define op context with logSchema, dependencies, feature flags, and user context
const { defineOp, defineOps } = defineOpContext({
  logSchema: {
    userId: S.category(),
    operation: S.enum(['CREATE', 'UPDATE', 'DELETE']),
  },
  deps: {
    db: dbOps.prefix('db'),
    cache: cacheOps.prefix('cache'),
  },
  flags: {
    advancedValidation: ff.boolean(),
  },
  ctx: {
    env: null as { dbTimeout: number }, // Must provide at createTrace()
  },
});
```

**Key Design: the `ctx` config field**

The `ctx` field specifies user-extensible context properties beyond the built-in `span`, `log`, `tag`, `deps`, and `ff`.
It holds runtime defaults and required values, validated against reserved keys. These properties are spread into
SpanContext and can be destructured by ops.

### Internal Implementation

```typescript
interface OpMetadata {
  // Source attribution, one per defineOp
  readonly name: string; // Op name for metrics
  readonly package_name: string;
  readonly package_file: string;
  readonly git_sha: string;
  readonly line: number; // Line of the defineOp call

  // Pre-encoded UTF-8 for Arrow dictionary building
  readonly package_name_entry: PreEncodedEntry;
  readonly package_file_entry: PreEncodedEntry;
  readonly git_sha_entry: PreEncodedEntry;

  // Startup-resolved physical layout: SpanBuffer class, eager columns, vocabulary generation, WASM layout
  readonly _physicalLayoutPlan?: OpPhysicalLayout;
}

// Self-tuning buffer stats: one object on each SpanBuffer class, shared by every buffer of that class
interface SpanBufferStats {
  capacity: number; // Rows per new SpanBuffer
  totalWrites: number; // Completed entry rows; lifecycle rows 0 and 1 are excluded
  spansCreated: number; // Non-overflow buffers created
}

function defineOpContext<T, FF, Deps, UserCtx>(
  config: OpContextConfig<T, FF, Deps, UserCtx>
): OpContextFactory<LogSchema<EffectiveSchema<T, Deps>>, FF, Deps, UserCtx> {
  // One LogBinding (schema + optional remap descriptor) shared by every op of this context.
  // defineOp(name, fn, metadata?) builds one OpMetadata and one frozen callsite plan per op.
  return { logBinding, logSchema, flags, ctxDefaults, deps, defineOp, defineOps };
}
```

## Op Class Generation <a id="smoo/lmao!n/op-class"></a>

The `defineOp()` factory creates Op instances that wrap user functions. Type parameters are ordered to match the
function signature `(ctx, ...args) => Result`:

```typescript
class Op<Ctx extends OpContext, Args extends unknown[], S, E, R extends AnyResult | Promise<AnyResult>> {
  readonly metadata: OpMetadata; // This op's source attribution and physical layout plan
  readonly callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>; // Frozen startup-resolved classes and operands
  readonly fn: (ctx: SpanContext<Ctx>, ...args: Args) => R;
}
```

An `Op` holds no span logic. The tracer runs root spans and the SpanContext class runs child spans; both read the op's
`callsitePlan`.

**Root span** (one per `trace()` call):

1. The tracer creates the `TraceRoot` (trace id, time anchors, tracer reference).
2. `bufferStrategy.createSpanBuffer(schema, traceRoot, plan.metadata, plan.capacityTier, plan.SpanBufferClass)` creates
   the root buffer.
3. `buffer._appenders.writeSpanStart(buffer, name)` writes row 0, including the span name.
4. `new plan.SpanContextClass(buffer, schema, plan, ctxDefaults, overrides, deps, flagEvaluator)` creates the context.

**Child span** (one per `span()` call, in `_spanPre`):

1. `bufferStrategy.createChildSpanBuffer(parent, parent._opMetadata, plan.metadata, plan.capacityTier, schema, plan.SpanBufferClass)`
   creates the child. The caller's op metadata becomes the child's `_callsiteMetadata` (row 0); the callee's becomes its
   `_opMetadata` (rows 1+).
2. The child buffer takes `plan.remapDescriptor`, which Arrow conversion reads for prefixed libraries.
3. `childBuffer._appenders.writeSpanStart(childBuffer, name)` writes row 0, and `childBuffer.line(0, line)` writes the
   call line directly to row 0.
4. The child SpanContext receives the parent's `deps` and a flag evaluator bound to the child.

**Completion** (root and child):

```typescript
try {
  const result = await op.fn(ctx, ...args);
  if (!(result instanceof Ok) && !(result instanceof Err)) throw new TypeError(SPAN_COMPLETION_OWNER_ERROR);
  result._assertOwner(ctx); // The result must come from this context's ok()/err()
  writeSpanEnd(buffer, result); // Row 1: span-ok or span-err, timestamp, error_code
  return result;
} catch (error) {
  ctx._spanException(buffer, error); // Row 1: span-exception, message, exception_stack
  throw error;
}
```

The completion row's attributes, message, and line were already written by the result's chained methods
([Entry Types](./01h_entry_types_and_logging_primitives.md#smoo/lmao!n/lmao-entry-row1-result-writes)).

## TagWriter Class Generation <a id="smoo/lmao!n/codegen-architecture"></a>

The TagWriter class provides typed attribute methods for `tag`.

### Standard Compilation

Startup builds the writer classes for a schema as part of its `PhysicalLayoutPlan`. One generator emits both fixed-row
writers: the TagWriter writes row 0 and the ResultWriter writes row 1. The row is a literal embedded in every method.

```typescript
function generateFixedPositionWriterClass(
  schema: LogSchema,
  position: number, // 0 for TagWriter, 1 for ResultWriter
  className: string,
  extension?: FixedPositionWriterExtension, // Extra methods, e.g. the ResultWriter's message()/line()
  eagerColumns?: readonly string[],
  enumLookup?: SchemaEnumLookupDescriptor
): string;

// Cached per schema and eager-column set
function getTagWriterClass<T extends LogSchema>(
  schema: T,
  eagerColumns?: readonly string[],
  enumLookup?: SchemaEnumLookupDescriptor
): TagWriterConstructor<T>;
```

### Library Compilation (Prefixed) <a id="smoo/lmao!n/codegen-prefixed-remap"></a>

A prefixed library reuses the writer classes of its own schema unchanged. `prefix('http')` builds an immutable
`RemapDescriptor` that maps each output column name to its canonical source name. Each prefixed op's callsite plan
carries the descriptor, the child buffer takes it at span setup, and Arrow conversion reads it to emit `http_status` for
the buffer's `status` column. Writers never see prefixed names, so a library's warmed writes are the same with or
without a prefix.

### TagWriter Generation Logic

A generated writer stores only the SpanContext's writer state. Each setter writes one column of its fixed row through
the buffer's column setter; enum fields encode through an encoder bound when the class is created:

```typescript
// Simplified output of generateFixedPositionWriterClass(schema, 0, 'GeneratedTagWriter')
class GeneratedTagWriter {
  constructor(state) {
    this._state = state; // The only instance field
  }

  // Object API: tag.with({ userId: '123', operation: 'INSERT' })
  with(attributes) {
    if ('userId' in attributes && attributes.userId !== null && attributes.userId !== undefined) {
      this._state._spanBuffer.userId(0, attributes.userId);
    }
    if ('operation' in attributes && attributes.operation !== null && attributes.operation !== undefined) {
      this._state._spanBuffer.operation(0, encodeEnum0(attributes.operation));
    }
    return this;
  }

  // Chainable API: tag.userId('123')
  userId(value) {
    this._state._spanBuffer.userId(0, value);
    return this;
  }

  operation(value) {
    this._state._spanBuffer.operation(0, encodeEnum0(value));
    return this;
  }
}
```

## Generated Code Examples

### Standard Module TagWriter

For `{ userId: S.category(), operation: S.enum(['INSERT', 'UPDATE', 'DELETE']) }` the generated TagWriter is the class
shown above.

### Library Module TagWriter (with prefix 'http')

For `{ status: S.number(), method: S.category() }` used as `httpOps.prefix('http')`, the TagWriter is the library's own
class. `tag.status(200)` writes the buffer's `status` column; Arrow conversion names it `http_status` from the buffer's
`RemapDescriptor`:

```typescript
class GeneratedTagWriter {
  constructor(state) {
    this._state = state;
  }

  status(value) {
    this._state._spanBuffer.status(0, value); // Canonical column; prefixed only at Arrow conversion
    return this;
  }

  method(value) {
    this._state._spanBuffer.method(0, value);
    return this;
  }
}
```

## SpanLogger Class Generation <a id="smoo/lmao!n/codegen-spanlogger"></a>

The SpanLogger handles log methods (`info`/`debug`/`warn`/`error`/`trace`):

```typescript
// Cached per schema, materializer mode, message layout family, physical layout, and eager columns
function createSpanLoggerClass<T extends LogSchema>(
  schema: T,
  messageLayoutFamily: MessageLayoutFamily = 'mixed',
  messagePhysicalLayout: MessagePhysicalLayout = 'current',
  eagerColumns: readonly string[] = [],
  enumLookup: SchemaEnumLookupDescriptor = resolveEnumLookupDescriptor(schema)
): SpanLoggerConstructor<T>;
```

Where `new Function` is allowed the class is compiled from generated source; where it is forbidden (for example workerd)
a closure materializer builds the same prototype. An instance stores only the SpanContext's writer state. Its members
are:

- `info`/`debug`/`warn`/`error`/`trace`: append one row and return the fluent entry; attribute setters on that entry
  write the row just appended
- `line(n)`: write the line number of the row just appended
- `scope` (getter): the buffer's current frozen `_scopeValues`
- `_setScope(attributes)`: replace `_scopeValues` with a new frozen merge; `null` removes a key and `undefined` is
  ignored. `ctx.setScope()` calls it.
- `_prefillScopedAttributesOn(buffer)`: the overflow slow-path helper. When a log write moves to a new overflow buffer,
  it fills the current scope into that buffer's rows from its `_writeIndex` to its capacity.

Scope values fill the other rows at Arrow conversion ([Span Scope Attributes](./01i_span_scope_attributes.md)).

## SpanContext Type Definition <a id="smoo/lmao!n/spancontext-type"></a>

The context destructured by op functions. One type parameter, `Ctx extends OpContext`, bundles the log schema, flags,
deps, and user context; user-defined fields (like `env` with CF Worker bindings) come from `Ctx['userCtx']`:

```typescript
type SpanContext<Ctx extends OpContext> = {
  // Access to underlying buffer (advanced use)
  buffer: SpanBuffer<Ctx['logSchema']>;

  // Span attributes (chainable)
  tag: TagWriter<Ctx['logSchema']>;

  // Log messages
  log: SpanLogger<Ctx['logSchema']>;

  // Feature flags (logs access to current span)
  ff: FeatureFlagEvaluator<Ctx> & InferFeatureFlagsWithContext<Ctx>;

  // Dependencies - can be destructured!
  deps: ResolvedDeps<Ctx['deps']>;

  // Result helpers
  ok<S>(value: S): OkResult<S, Ctx['logSchema']>;
  err<E>(error: E): ErrResult<E, Ctx['logSchema']>;

  // Child spans (see SpanFn below)
  span: SpanFn<Ctx>;
  spanSync: SpanSyncFn<Ctx>;

  // Scoped attributes (merge semantics, null clears) and their current values
  setScope(values: ScopeUpdate<Ctx['logSchema']> | null): void;
  scope: Readonly<Partial<InferSchema<Ctx['logSchema']>>>;
} & Ctx['userCtx']; // User-defined fields spread in (e.g., env, services)

// SpanFn: 4 overloads (2 with a transformer-injected line number, 2 without)
type SpanFn<Ctx extends OpContext> = {
  // Op with line: span(line, name, op, ...args)
  <S, E, Args extends unknown[]>(
    line: number,
    name: string,
    op: Op<Ctx, Args, S, E>,
    ...args: Args
  ): Promise<Result<S, E>>;

  // Inline closure with line: span(line, name, fn)
  <S, E>(
    line: number,
    name: string,
    fn: (ctx: SpanContext<Ctx>) => Result<S, E> | Promise<Result<S, E>>
  ): Promise<Result<S, E>>;

  // Op: span(name, op, ...args)
  <S, E, Args extends unknown[]>(name: string, op: Op<Ctx, Args, S, E>, ...args: Args): Promise<Result<S, E>>;

  // Inline closure: span(name, fn)
  <S, E>(name: string, fn: (ctx: SpanContext<Ctx>) => Result<S, E> | Promise<Result<S, E>>): Promise<Result<S, E>>;
};

// Functions can pick what they need
type LoggingContext<Ctx extends OpContext> = Pick<SpanContext<Ctx>, 'log'>;
type TaggingContext<Ctx extends OpContext> = Pick<SpanContext<Ctx>, 'tag'>;
type MinimalContext<Ctx extends OpContext> = Pick<SpanContext<Ctx>, 'span' | 'log' | 'tag'>;
```

The runtime `span` dispatcher forwards each call to a fixed-arity member (`span0` through `span8`, one per argument
count) so every call site stays monomorphic. These members, and the transformer-only `spanAuto0` through `spanAuto8`,
are internal; ops call `span`.

## Op Definition Pattern

### Basic Op

```typescript
const { defineOp } = defineOpContext({ logSchema: userSchema });

const createUser = defineOp('createUser', async ({ span, log, tag, ok, err }, userData: UserData) => {
  tag.userId(userData.id).operation('INSERT');
  log.info('Creating new user');

  const validated = await span('validate', validateUser, userData);
  if (validated.isErr()) {
    return err(validated.error); // Rebuild the child's outcome on this context
  }

  const saved = await span('save', saveUser, userData);
  return saved.isOk() ? ok(saved.value) : err(saved.error);
});
```

### Op with Dependencies

```typescript
const { defineOp } = defineOpContext({
  logSchema: httpSchema,
  deps: { retry: retryOps, auth: authOps }, // Each dep is an OpGroup
});

const GET = defineOp('GET', async ({ span, log, tag, deps, ok, err }, url: string) => {
  const { retry, auth } = deps; // Destructure deps!

  tag.method('GET').url(url);
  log.info('Making GET request');

  const token = await span('auth', auth.getToken);
  if (token.isErr()) return err(token.error);

  try {
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${token.value}` },
    });
    tag.status(res.status);
    return ok(res);
  } catch (e) {
    log.warn('Request failed, retrying');
    await span('retry', retry.attempt, 1);
    throw e;
  }
});
```

### Op with Feature Flags

```typescript
const processOrder = defineOp('processOrder', async ({ span, log, tag, deps, ff, ok }, order: Order) => {
  const { premiumProcessing, newPaymentFlow } = ff;

  tag.orderId(order.id);

  if (premiumProcessing) {
    await span('premium-validate', deps.premiumValidation.validate, order);
    premiumProcessing.track();
  }

  if (newPaymentFlow) {
    await span('new-payment', deps.newPayment.charge, order);
    newPaymentFlow.track();
  } else {
    await span('legacy-payment', deps.legacyPayment.charge, order);
  }

  return ok({ processed: true });
});
```

## Library Definition with defineOpContext

Libraries define their logSchema and dependencies:

The precompiled library emits and owns its versioned vocabulary registration fragment; application wiring consumes its
`VocabularyBinding` while continuing to own prefix/remap composition.

```typescript
// @my-company/http-tracing/src/index.ts
import { defineOpContext, S } from '@smoothbricks/lmao';

// Define library module
export const httpLib = defineOpContext({
  logSchema: {
    status: S.number(),
    method: S.enum(['GET', 'POST', 'PUT', 'DELETE', 'PATCH']),
    url: S.text().masked('url'),
    duration: S.number(),
  },
  deps: {
    retry: retryOps,
  },
});

// Get op factories
const { defineOp, defineOps } = httpLib;

// Define ops
export const request = defineOp('request', async ({ span, log, tag, deps, ok }, url: string, opts: RequestInit) => {
  const startTime = performance.now();

  tag.method(opts.method || 'GET').url(url);
  log.info('Making HTTP request');

  try {
    const response = await fetch(url, opts);
    tag.status(response.status).duration(performance.now() - startTime);

    if (!response.ok) {
      const { retry } = deps;
      await span('retry', retry.attempt, 1);
    }

    return ok(response);
  } catch (error) {
    tag.duration(performance.now() - startTime);
    log.error('Request failed');
    throw error;
  }
});

export const GET = defineOp('GET', async ({ span, ok, err }, url: string) => {
  const res = await span('request', request, url, { method: 'GET' });
  return res.isOk() ? ok(res.value) : err(res.error);
});

export const POST = defineOp('POST', async ({ span, ok, err }, url: string, body: unknown) => {
  const res = await span('request', request, url, {
    method: 'POST',
    body: JSON.stringify(body),
  });
  return res.isOk() ? ok(res.value) : err(res.error);
});

// Export the ops as an OpGroup so applications can wire them with a prefix
export const httpOps = defineOps({ request, GET, POST });
```

### Application Wiring

```typescript
// Application wires deps with prefixes: each dep carries its own prefix
const appContext = defineOpContext({
  logSchema: appSchema,
  deps: {
    http: httpOps.prefix('http'),
    retry: retryOps.prefix('http_retry'),
  },
});

// Invoke a dependency op through span()
const { trace } = new TestTracer(appContext);
const result = await trace('fetch-users', async (ctx) => {
  const users = await ctx.span('get-users', ctx.deps.http.GET, 'https://api.example.com/users');
  return users.isOk() ? ctx.ok(users.value) : ctx.err(users.error);
});
```

## ESLint Rule: No Capturing Fluent Interface <a id="smoo/lmao!n/eslint-no-capture-fluent"></a>

Because `tag` writes entries, users must not capture the fluent interface:

```typescript
// INVALID - capturing fluent interface
let tagRef = tag;
tagRef.userId(123); // Won't work as expected

// VALID - use directly
tag.userId(123).operation('INSERT');
```

An ESLint rule prevents capturing `tag` or `log` in variables.

## Performance Characteristics

### Build-Time vs Runtime Costs

- **Build/Startup**: Schema compilation and class generation (~1-5ms per module)
- **Runtime**: Warmed, in-capacity entry calls use fixed monomorphic stores with zero allocation
- **Memory**: The compiled TagWriter/SpanLogger classes and the `LogBinding` are shared by every op in an op context;
  `OpMetadata` is per op

### Generated Code Efficiency

```typescript
// Warmed method (fixed stores, zero allocation):
userId(value) {
  this._state._spanBuffer.userId(0, value); // Row 0 column setter
  return this;
}

// vs runtime mapping (overhead):
setAttribute(name, value) {
  const columnName = this.schema.getColumnName(name);  // Runtime lookup
  this.buffer[`write${columnName}`](value);           // Dynamic method
  return this;
}
```

### SpanBuffer Efficiency

- **Dual metadata references**: `_callsiteMetadata` for row 0's git_sha/package_name/package_file, `_opMetadata` for
  rows 1+
- **Direct lineNumber writes**: lineNumber passed as argument to span(), written directly to `line_values[0]` (NO
  intermediate object storage, NO lineNumber property on any context)
- **Span names**: `spanName` is interned during Arrow conversion; the stored value is a bound dense index
- **Reference to OpMetadata**: Each buffer references its op's `OpMetadata` rather than copying it; the compiled classes
  and the `LogBinding` are shared per op context

## Integration Points

This module context and op generation system integrates with:

- **[Context Flow and Op Wrappers](./01c_context_flow_and_op_wrappers.md)**: Provides SpanContext and span() mechanics
- **[Op Context Pattern](./01l_op_context_pattern.md)**: High-level API for defineOpContext, defineOp, and Tracer
- **[Trace Schema System](./01a_trace_schema_system.md)**: Consumes LogSchema definitions
- **[Library Integration Pattern](./01e_library_integration_pattern.md)**: RemapDescriptor for prefixed columns
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: Generated classes write to SpanBuffer
