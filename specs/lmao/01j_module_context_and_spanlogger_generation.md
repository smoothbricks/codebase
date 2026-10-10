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
A three-dimensional cache keyed by **schema identity + physical layout version + backend kind** shares the plan and its
buffer/writer/flush classes. Library prefix/remap state is an immutable binding over that physical plan (or part of the
cache key when it changes stores), never a name lookup performed by a warmed callsite.

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
| **Per-request flush**  | Few or no allocations by reusing layout/backend classes, scratch arenas, dictionary generations, and Arrow views. |

Flush returns an `ArrowLease` over runtime-owned Arrow-compatible buffers rather than transferring untracked reusable
storage. The lease pins the buffer arena and exact immutable vocabulary dictionary generation until the exporter
releases it; it never pins a module's caller-owned fragment arrays. This permits near-zero-allocation
request→trace→Arrow conversion without recycling memory still observed by a consumer.

The flush unit is one request. After linear-memory growth, WASM views are re-derived. Growth while a lease pins the
previous WASM memory epoch fails with an explicit error; it never invalidates pinned views silently.

## Compiler-Emitted Vocabulary Registration

Compiled application modules and precompiled JS libraries self-register their own vocabulary fragment inputs. The
application bootstrap MUST import and evaluate the LMAO runtime installer before evaluating any registering module. The
runtime installer owns the callback stored at `globalThis[Symbol.for('@smoothbricks/lmao/vocabulary/register/v1')]`;
emitted modules call it but never install or replace it.

The runtime type contract is:

```typescript
interface VocabularyFragmentV1 {
  schemaVersion: 1;
  idAlgorithm: 'sha256-24-v1';
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

For record `i`, `ids[i]` is the first 24 SHA-256 bits, interpreted big-endian, over `kindTags[i] || recordBytes`.
`contentHash` is lowercase SHA-256 hex over this exact canonical byte stream:

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
so old bindings remain prefix-valid. Stable IDs and decoded values are deterministic; dense indices and dictionary
ordering are not deterministic across processes. Dense index `0` is valid; null is represented only by the Arrow
validity bitmap.

The returned `VocabularyBinding` maps each fragment-local ordinal through direct `binding[ordinal]` indexing. Generated
code MUST NOT call a warmed `messageIndex()` helper. An `ArrowLease` pins the exact runtime-owned immutable dictionary
generation used by its Arrow output, never a registering module's arrays.

Libraries retain ownership of their emitted fragment declarations and registration calls. An application may prefix or
bind a library's schema columns, but MUST NOT copy, renumber, or synthesize that library's vocabulary. Duplicate
evaluation is idempotent for the same schema version, algorithm, content hash, and decoded fragment; invalid or
incompatible input fails during startup rather than falling back to hot-path string lookup. Detailed transformer
emission is specified elsewhere.

Template placeholders use `{field}`; `{{` and `}}` encode literal braces. `{{field}}` is not accepted as an alias.
`info`/`warn`/`error` callsites bind literal templates and object-literal fields; `debug`/`trace` may bind a literal
template or use an existing raw dynamic string reference, but avoidable interpolation remains invalid. Callsite classes
are static, structured, or dynamic. A physical layout may be mixed when it carries both static-index and
dynamic-reference lanes.

## Context Hierarchy <a id="smoo/lmao!n/opcontext-hierarchy"></a>

```
OpMetadata (class) - ONE per defineOp
├── package_name: string
├── package_file: string
├── git_sha: string
├── package_name_entry: PreEncodedEntry (UTF-8 cached)
├── package_file_entry: PreEncodedEntry (UTF-8 cached)
├── git_sha_entry: PreEncodedEntry (UTF-8 cached)
├── logSchema (tag attribute definitions)
├── sb_capacity: number (buffer capacity)
├── sb_totalWrites: number (total writes across all buffers)
├── sb_overflows: number (overflow write count)
└── sb_totalCreated: number (total buffers created)

SpanBuffer - ONE per span (internal interface)
├── _callsiteModule?: OpMetadata ← caller's metadata (where span() was invoked) - for row 0 metadata
├── _opMetadata: OpMetadata         ← op's metadata (what code is executing) - for rows 1+ metadata
├── message_values[0]: string       ← span name written by writeSpanStart()
├── line_values: Float64Array       ← line numbers per row (written directly, NOT stored as property)
├── _parent?: SpanBuffer            ← reference to parent (child spans walk this for trace_id)
├── _children: SpanBuffer[]         ← child spans
├── trace_id (getter)               ← root stores it, children walk parent chain
└── columns, writeIndex, etc.

SpanContext (interface) - user-facing, what ops receive
├── tag: TagWriter<T>
├── log: SpanLogger<T>
├── setScope()
├── ok() / err()
├── span()
├── buffer (getter)
├── ff: FeatureFlagEvaluator
└── deps: Deps
```

**Key Design: Dual Module References for Source Attribution**

Each SpanBuffer has TWO module references for different purposes:

- **`callsiteModule`**: The caller's module - where `span()` was invoked. Used for **row 0 (span-start)** metadata
  (`git_sha`, `package_name`, `package_file`).
- **`module`**: The Op's module - what code is executing. Used for **rows 1+ (span-ok/err/exception, logs)** metadata
  (`git_sha`, `package_name`, `package_file`).

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
  await ctx.span('validate', validateUser, userData); // Pass ctx to everything
});
```

### Solution: Destructured SpanContext

The chosen design uses ops with destructuring:

```typescript
// Destructure what you need
const createUser = defineOp('createUser', async ({ span, log, tag }, userData: UserData) => {
  tag.userId(userData.id);
  log.info('Creating user');
  await span('validate', validateUser, userData); // span() handles context
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
class OpMetadata {
  // Source attribution (strings), one per defineOp
  package_name: string;
  package_file: string;
  git_sha: string;

  // Pre-encoded UTF-8 for zero-copy writes
  package_name_entry: PreEncodedEntry;
  package_file_entry: PreEncodedEntry;
  git_sha_entry: PreEncodedEntry;

  // Log schema definition
  logSchema: LogSchema;

  // Generated classes
  TagWriter: TagWriterClass;
  SpanLogger: SpanLoggerClass;

  // Self-tuning buffer stats (flat properties, not nested object)
  sb_capacity: number = 64;
  sb_totalWrites: number = 0;
  sb_overflows: number = 0;
  sb_totalCreated: number = 0;
}

function defineOpContext<Schema, Deps, FF, Ctx>(config: { logSchema: Schema; deps?: Deps; flags?: FF; ctx?: Ctx }) {
  // Compile logSchema to generate TagWriter and SpanLogger classes
  const compiledTagOps = compileTagOperations(config.logSchema);

  return {
    deps: config.deps || {},
    flags: config.flags || {},
    ctx: config.ctx || {},

    // Op factory - creates Op instances with full context type
    defineOp: <Args extends unknown[], Result>(
      name: string,
      fn: (ctx: SpanContext<Schema, Deps, FF, Ctx>, ...args: Args) => Promise<Result>
    ): Op<SpanContext<Schema, Deps, FF, Ctx>, Args, Result> => new Op(name, compiledTagOps, fn),
  };
}
```

## Op Class Generation <a id="smoo/lmao!n/op-class"></a>

The `defineOp()` factory creates Op instances that wrap user functions. Type parameters are ordered to match the
function signature `(ctx, ...args) => Promise<Result>`:

```typescript
class Op<Ctx, Args extends unknown[], Result> {
  constructor(
    readonly name: string, // For Op metrics (invocations, errors, duration)
    private module: OpMetadata, // For git_sha/package_name/package_file attribution
    private fn: (ctx: Ctx, ...args: Args) => Promise<Result>
  ) {}

  /**
   * Internal invocation - called by span() or Tracer.trace()
   *
   * Root spans: Tracer._createRootContext() and _executeResultWithContext() (tracer.ts)
   * Child spans: SpanContext._spanPre() and span0-span8 methods (spanContext.ts)
   *
   * @param parentBuffer - Parent span's buffer (null for root - Tracer handles root)
   * @param callsiteModule - The CALLER's module (where span() was invoked)
   * @param spanName - Name decided by caller
   * @param lineNumber - Line number where span() was called (injected by transformer, passed directly)
   * @param args - User arguments to the op
   */
  async _invoke(
    parentBuffer: SpanBuffer | null,
    callsiteModule: OpMetadata,
    spanName: string,
    lineNumber: number,
    args: Args
  ): Promise<Result> {
    // 1. Create SpanBuffer with callsiteModule reference
    //    - callsiteModule: where span() was called (for row 0's git_sha/package_name/package_file)
    //    - this.module: the Op's module (for rows 1+ git_sha/package_name/package_file)
    //    - For root spans, Tracer creates TraceRoot FIRST with trace_id, anchors, tracer reference
    const buffer = parentBuffer
      ? createChildSpanBuffer(parentBuffer, callsiteModule, this.module, spanName)
      : createSpanBuffer(callsiteModule, this.module, spanName, traceRoot); // traceRoot created by Tracer

    // 2. Register with parent's _children (RemappedBufferView if prefixed)
    if (parentBuffer) {
      if (this.module.remappedViewClass) {
        // Module has prefix - wrap buffer in RemappedBufferView for parent's tree traversal
        const view = new this.module.remappedViewClass(buffer);
        parentBuffer._children.push(view);
      } else {
        // No prefix - push raw buffer directly
        parentBuffer._children.push(buffer);
      }
      buffer.parent = parentBuffer;
    }

    // 3. Write span-start (row 0)
    //    - Uses callsiteModule for git_sha/package_name/package_file
    //    - lineNumber written DIRECTLY to line_values[0] (NO intermediate object storage)
    buffer.line_values[0] = lineNumber; // Direct TypedArray write
    buffer.writeSpanStart(); // Writes timestamp, operation, uses callsiteModule for metadata

    // 4. Create SpanContext (satisfies Ctx constraint)
    //    span() captures the CURRENT module (this.module) as callsiteModule for child spans
    //    User context properties (env, requestId, etc.) come from Tracer's ctxDefaults + overrides
    const opCtx = {
      span: (childLineNumber, name, childOp, ...childArgs) =>
        childOp._invoke(buffer, this.module, name, childLineNumber, childArgs),
      log: new this.module.SpanLogger(buffer),
      tag: new this.module.TagWriter(buffer),
      deps: this.module.boundDeps,
      ff: flagEvaluator?.forContext(opCtx) ?? {},
      ok: (value) => ({ success: true, value }),
      err: (error) => ({ success: false, error }),
      scope: (attrs) => buffer.setScope(attrs),
      get buffer() {
        return buffer;
      },
      // User context properties spread in (e.g., env, requestId, userId)
      // These come from Tracer.ctxDefaults merged with trace() overrides
      ...userCtxProperties,
    } as Ctx;

    // 5. Execute with try/catch
    //    Rows 1+ use this.module for git_sha/package_name/package_file
    try {
      const result = await this.fn(opCtx, ...args);
      buffer.writeSpanOk(); // Row 1 - uses module metadata
      return result;
    } catch (error) {
      buffer.writeSpanException(error); // Row 1 - uses module metadata
      throw error;
    }
  }
}
```

## TagWriter Class Generation <a id="smoo/lmao!n/codegen-architecture"></a>

The TagWriter class provides typed attribute methods for `tag`:

### Standard Compilation

```typescript
function compileTagOperations(logSchema: LogSchema) {
  const attributeNames = Object.keys(logSchema);
  const TagWriter = generateFixedPositionWriterClass(attributeNames, attributeNames, logSchema);
  const SpanLogger = createSpanLoggerClass(attributeNames, logSchema);

  return { logSchema, TagWriter, SpanLogger };
}
```

### Library Compilation (Prefixed) <a id="smoo/lmao!n/codegen-prefixed-remap"></a>

```typescript
function compilePrefixedTagOperations(cleanSchema: LogSchema, prefix: string) {
  const cleanNames = Object.keys(cleanSchema);
  const prefixedNames = cleanNames.map((name) => `${prefix}_${name}`);
  const prefixedSchema = prefixSchema(cleanSchema, prefix);

  const LibraryTagWriter = generateFixedPositionWriterClass(cleanNames, prefixedNames, prefixedSchema);
  const LibrarySpanLogger = createSpanLoggerClass(cleanNames, prefixedSchema);

  return {
    logSchema: prefixedSchema,
    TagWriter: LibraryTagWriter,
    SpanLogger: LibrarySpanLogger,
  };
}
```

### TagWriter Generation Logic

```typescript
function generateFixedPositionWriterClass(methodNames: string[], columnNames: string[], schema: LogSchema) {
  // Generate individual attribute methods
  const attributeMethods = methodNames
    .map(
      (methodName, i) => `
    ${methodName}(value) {
      this.buffer.write${capitalize(columnNames[i])}(value);
      return this;
    }
  `
    )
    .join('\n');

  // Generate callable body for object API: tag({ status: 200 })
  const callableBody = methodNames
    .map(
      (methodName, i) => `
    if (attributes.${methodName} !== undefined) {
      this.buffer.write${capitalize(columnNames[i])}(attributes.${methodName});
    }
  `
    )
    .join('\n');

  return new Function(
    'BaseTagWriter',
    `
    return class extends BaseTagWriter {
      // Object-based API: tag({ status: 200, method: 'GET' })
      call(attributes) {
        ${callableBody}
        return this;
      }

      ${attributeMethods}
    }
  `
  )(BaseTagWriter);
}
```

## Generated Code Examples

### Standard Module TagWriter

```typescript
// Input: { userId: S.category(), operation: S.enum(['INSERT', 'UPDATE', 'DELETE']) }
// Generated TagWriter:
class StandardTagWriter extends BaseTagWriter {
  // Object API: tag({ userId: "123", operation: "INSERT" })
  call(attributes) {
    if (attributes.userId !== undefined) {
      this.buffer.writeUserId(attributes.userId);
    }
    if (attributes.operation !== undefined) {
      this.buffer.writeOperation(attributes.operation);
    }
    return this;
  }

  // Chainable API: tag.userId("123")
  userId(value) {
    this.buffer.writeUserId(value);
    return this;
  }

  operation(value) {
    this.buffer.writeOperation(value);
    return this;
  }
}
```

### Library Module TagWriter (with prefix 'http')

```typescript
// Input: { status: S.number(), method: S.category() }, prefix: 'http'
// Generated TagWriter:
class LibraryTagWriter extends BaseTagWriter {
  call(attributes) {
    if (attributes.status !== undefined) {
      this.buffer.writeHttpStatus(attributes.status); // Clean → prefixed
    }
    if (attributes.method !== undefined) {
      this.buffer.writeHttpMethod(attributes.method);
    }
    return this;
  }

  status(value) {
    this.buffer.writeHttpStatus(value); // Clean method → prefixed column
    return this;
  }

  method(value) {
    this.buffer.writeHttpMethod(value);
    return this;
  }
}
```

## SpanLogger Class Generation <a id="smoo/lmao!n/codegen-spanlogger"></a>

The SpanLogger handles log methods (info/debug/warn/error):

```typescript
function createSpanLoggerClass(attributeNames: string[], schema: LogSchema) {
  // Generate with() method body for attributes
  const withBody = attributeNames
    .map(
      (name) => `
    if (attributes.${name} !== undefined) {
      this.buffer.write${capitalize(name)}(attributes.${name});
    }
  `
    )
    .join('\n');

  return new Function(
    'BaseSpanLogger',
    `
    return class extends BaseSpanLogger {
      info(message) {
        this._writeLogEntry('info', message);
        return this;
      }

      debug(message) {
        this._writeLogEntry('debug', message);
        return this;
      }

      warn(message) {
        this._writeLogEntry('warn', message);
        return this;
      }

      error(message) {
        this._writeLogEntry('error', message);
        return this;
      }

      with(attributes) {
        ${withBody}
        return this;
      }

      scope(attributes) {
        this._prefillRemainingCapacity(attributes);
        return this;
      }
    }
  `
  )(BaseSpanLogger);
}
```

## SpanContext Type Definition <a id="smoo/lmao!n/spancontext-type"></a>

The context destructured by op functions. The `Extra` type parameter allows user-defined fields (like `env` with CF
Worker bindings):

```typescript
// Full SpanContext type - Extra is spread in for user extensibility
type SpanContext<Schema, Deps, FF, Extra = {}> = {
  // Invoke another op as child span (6 overloads, see span() docs)
  span: SpanFn<SpanContext<Schema, Deps, FF, Extra>>;

  // Log messages
  log: SpanLogger;

  // Span attributes (chainable)
  tag: TagWriter<Schema>;

  // Dependencies - can be destructured!
  deps: Deps;

  // Feature flags (logs access to current span)
  ff: FeatureFlagEvaluator<FF>;

  // Result helpers
  ok<T>(value: T): OkResult<T>;
  err<E>(error: E): ErrResult<E>;

  // Scoped attributes
  scope(attributes: Partial<Schema>): void;

  // Access to underlying buffer (advanced use)
  buffer: SpanBuffer;
} & Extra; // User-defined fields spread in (e.g., env, services)

// SpanFn type with 6 overloads (3 with line number from transformer, 3 without)
type SpanFn<CurrentCtx> = {
  // Op-only: span(name, op, ...args)
  <Ctx, Args extends unknown[], Result>(name: string, op: Op<Ctx, Args, Result>, ...args: Args): Promise<Result>;

  // Context override + Op: span(name, ctx, op, ...args)
  <Ctx, Args extends unknown[], Result>(
    name: string,
    ctx: Partial<Ctx>,
    op: Op<Ctx, Args, Result>,
    ...args: Args
  ): Promise<Result>;

  // Inline closure: span(name, fn)
  <Result>(name: string, fn: (ctx: CurrentCtx) => Promise<Result>): Promise<Result>;
};

// Functions can pick what they need
type LoggingContext = Pick<SpanContext<{}, {}, {}>, 'log'>;
type TaggingContext<S> = Pick<SpanContext<S, {}, {}>, 'tag'>;
type MinimalContext<S> = Pick<SpanContext<S, {}, {}>, 'span' | 'log' | 'tag'>;
```

## Op Definition Pattern

### Basic Op

```typescript
const { defineOp } = userModule;

const createUser = defineOp('createUser', async ({ span, log, tag }, userData: UserData) => {
  tag.userId(userData.id).operation('INSERT');
  log.info('Creating new user');

  const validated = await span('validate', validateUser, userData);
  if (!validated.success) {
    return { success: false, error: validated.error };
  }

  const user = await span('save', saveUser, userData);
  return { success: true, user };
});
```

### Op with Dependencies

```typescript
const { defineOp } = httpModule;

const GET = defineOp('GET', async ({ span, log, tag, deps }, url: string) => {
  const { retry, auth } = deps; // Destructure deps!

  tag.method('GET').url(url);
  log.info('Making GET request');

  const token = await span('auth', auth.getToken);

  try {
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${token}` },
    });
    tag.status(res.status);
    return res;
  } catch (e) {
    log.warn('Request failed, retrying');
    await span('retry', retry, 1);
    throw e;
  }
});
```

### Op with Feature Flags

```typescript
const processOrder = defineOp('processOrder', async ({ span, log, tag, deps, ff }, order: Order) => {
  const { premiumProcessing, newPaymentFlow } = ff;

  tag.orderId(order.id);

  if (premiumProcessing) {
    await span('premium-validate', deps.premiumValidation, order);
    premiumProcessing.track();
  }

  if (newPaymentFlow) {
    await span('new-payment', deps.newPayment, order);
    newPaymentFlow.track();
  } else {
    await span('legacy-payment', deps.legacyPayment, order);
  }

  return { success: true };
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
    retry: retryLib,
  },
});

// Get op factory
const { defineOp } = httpLib;

// Define ops
export const request = defineOp('request', async ({ span, log, tag, deps }, url: string, opts: RequestInit) => {
  const startTime = performance.now();

  tag.method(opts.method || 'GET').url(url);
  log.info('Making HTTP request');

  try {
    const response = await fetch(url, opts);
    tag.status(response.status).duration(performance.now() - startTime);

    if (!response.ok) {
      const { retry } = deps;
      await span('retry', retry, 1);
    }

    return response;
  } catch (error) {
    tag.duration(performance.now() - startTime);
    log.error('Request failed');
    throw error;
  }
});

export const GET = defineOp('GET', async ({ span }, url: string) => {
  return await span('request', request, url, { method: 'GET' });
});

export const POST = defineOp('POST', async ({ span }, url: string, body: unknown) => {
  return await span('request', request, url, {
    method: 'POST',
    body: JSON.stringify(body),
  });
});
```

### Application Wiring

```typescript
// Application wires deps with prefixes: each dep carries its own prefix
const appDeps = {
  http: httpLib.prefix('http'),
  retry: retryLib.prefix('http_retry'),
};

// Invoke via span()
const result = await httpRoot.span('fetch-users', GET, 'https://api.example.com/users');
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
- **Memory**: Shared OpMetadata across all ops in same module

### Generated Code Efficiency

```typescript
// Warmed method (fixed stores, zero allocation):
userId(value) {
  this.buffer.writeUserId(value);  // Direct method call
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

- **Dual module references**: `callsiteModule` for row 0's git_sha/package_name/package_file, `module` for rows 1+
- **Direct lineNumber writes**: lineNumber passed as argument to span(), written directly to `line_values[0]` (NO
  intermediate object storage, NO lineNumber property on any context)
- **Span names**: `spanName` is interned during Arrow conversion; the stored value is a bound dense index
- **Reference to OpMetadata**: References to shared OpMetadata (no duplication)

## Integration Points

This module context and op generation system integrates with:

- **[Context Flow and Op Wrappers](./01c_context_flow_and_op_wrappers.md)**: Provides SpanContext and span() mechanics
- **[Op Context Pattern](./01l_op_context_pattern.md)**: High-level API for defineOpContext, defineOp, and Tracer
- **[Trace Schema System](./01a_trace_schema_system.md)**: Consumes LogSchema definitions
- **[Library Integration Pattern](./01e_library_integration_pattern.md)**: RemappedBufferView for prefixed columns
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: Generated classes write to SpanBuffer
