# Module Context and Op/SpanLogger Generation

## Overview

The module context system provides the foundation for creating ops with generated TagAPI and SpanLogger classes. It
handles:

1. **ModuleContext** - module-level configuration shared across all ops
2. **SpanBuffer** - per-span buffer with span name, line number, and module reference
3. **Op class generation** for traced operations with proper type parameters
4. **TagAPI class generation** with typed attribute methods (for `tag`)
5. **SpanLogger class generation** with log methods (for `log`)
6. **Schema compilation** for both standard and library modules
7. **User-extensible context** via `.ctx<Extra>()` for custom properties

This system operates at build/startup time to generate efficient runtime code with zero overhead.

## Context Hierarchy

```
ModuleContext (class) - ONE per module definition
├── packageName: string
├── packagePath: string
├── gitSha: string
├── packageEntry: PreEncodedEntry (UTF-8 cached)
├── packagePathEntry: PreEncodedEntry (UTF-8 cached)
├── gitShaEntry: PreEncodedEntry (UTF-8 cached)
├── logSchema (tag attribute definitions)
├── sb_capacity: number (buffer capacity)
├── sb_totalWrites: number (total writes across all buffers)
├── sb_overflows: number (overflow write count)
└── sb_totalCreated: number (total buffers created)

SpanBuffer - ONE per span (internal interface)
├── _callsiteModule?: ModuleContext ← caller's module (where span() was invoked) - for row 0 metadata
├── _module: ModuleContext          ← op's module (what code is executing) - for rows 1+ metadata
├── _spanName: string               ← span name for this invocation
├── lineNumber_values: Int32Array  ← line numbers per row (written directly, NOT stored as property)
├── _parent?: SpanBuffer            ← reference to parent (child spans walk this for trace_id)
├── _children: SpanBuffer[]         ← child spans
├── trace_id (getter)               ← root stores it, children walk parent chain
└── columns, writeIndex, etc.

SpanContext (interface) - user-facing, what ops receive
├── tag: TagWriter<T>
├── log: SpanLogger<T>
├── scope()
├── ok() / err()
├── span()
├── buffer (getter)
├── ff: FeatureFlagEvaluator
└── deps: Deps
```

**Key Design: Dual Module References for Source Attribution**

Each SpanBuffer has TWO module references for different purposes:

- **`callsiteModule`**: The caller's module - where `span()` was invoked. Used for **row 0 (span-start)** metadata
  (`gitSha`, `packageName`, `packagePath`).
- **`module`**: The Op's module - what code is executing. Used for **rows 1+ (span-ok/err/exception, logs)** metadata
  (`gitSha`, `packageName`, `packagePath`).

This enables accurate source attribution: the span-start entry records WHERE the span was invoked from (callsite), while
subsequent entries record WHERE the code is actually executing (the Op's module).

**Key Design: lineNumber is NEVER a property on any object**

Line numbers flow directly from transformer injection to TypedArray writes:

```typescript
// Transformer output:
await span(42, 'fetch-user', userLib.fetchUser, userId);
//         ^^ lineNumber argument

// Inside span():
buffer.lineNumber_values[0] = 42; // Direct TypedArray write for row 0, that's it

// For logs:
log.info('Processing').line(55); // .line(N) writes to lineNumber_values[writeIndex]
```

The lineNumber is passed as an argument and written directly to `lineNumber_values` TypedArray at the appropriate row
index. There is NO intermediate storage of lineNumber on any context object.

## Design Rationale: From ctx to Destructured SpanContext

### Problem with ctx Parameter

An alternative approach would pass a monolithic `ctx` object:

```typescript
// Alternative approach (rejected): ctx drilling everywhere
const createUser = op(async (ctx, userData) => {
  ctx.tag.userId(userData.id);
  ctx.log.info('Creating user');
  await ctx.span('validate', validateUser, userData); // Pass ctx to everything
});
```

### Solution: Destructured SpanContext

The chosen design uses ops with destructuring:

```typescript
// Destructure what you need
const createUser = op(async ({ span, log, tag }, userData: UserData) => {
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
- **User-extensible**: Add `env` or other properties via `.ctx<Extra>()`

## Module Definition with defineModule()

**Purpose**: Set up module-level configuration shared across all ops in the module.

```typescript
// Define module with logSchema, dependencies, feature flags, and user context
const userModule = defineModule({
  metadata: {
    packageName: '@my-company/user-service',
    packagePath: 'src/user.ts',
  },
  logSchema: {
    userId: S.category(),
    operation: S.enum(['CREATE', 'UPDATE', 'DELETE']),
  },
  deps: {
    db: dbLib,
    cache: cacheLib,
  },
  ff: {
    advancedValidation: ff.boolean(),
  },
}).ctx<{ env: { dbTimeout: number } }>();

// Destructure op factory
const { op } = userModule;
```

**Key Design: `.ctx<Extra>()`**

The `.ctx<Extra>()` method specifies user-extensible context properties beyond the built-in `span`, `log`, `tag`,
`deps`, and `ff`. These properties are spread into SpanContext and can be destructured by ops.

### Internal Implementation

```typescript
interface ModuleMetadata {
  packageName: string;
  packagePath: string;
  gitSha?: string; // Optional - injected by transformer at build time
}

class ModuleContext {
  // Metadata (strings)
  packageName: string;
  packagePath: string;
  gitSha: string;

  // Pre-encoded UTF-8 for zero-copy writes
  packageEntry: PreEncodedEntry;
  packagePathEntry: PreEncodedEntry;
  gitShaEntry: PreEncodedEntry;

  // Log schema definition
  logSchema: LogSchema;

  // Generated classes
  TagAPI: TagAPIClass;
  SpanLogger: SpanLoggerClass;

  // Self-tuning buffer stats (flat properties, not nested object)
  sb_capacity: number = 64;
  sb_totalWrites: number = 0;
  sb_overflows: number = 0;
  sb_totalCreated: number = 0;
}

function defineModule<Schema, Deps, FF>(config: { metadata: ModuleMetadata; logSchema: Schema; deps?: Deps; ff?: FF }) {
  // Compile logSchema to generate TagAPI and SpanLogger classes
  const compiledTagOps = compileTagOperations(config.logSchema);

  // Create module context
  const moduleContext = new ModuleContext();
  moduleContext.packageName = config.metadata.packageName;
  moduleContext.packagePath = config.metadata.packagePath;
  moduleContext.gitSha = config.metadata.gitSha || '';
  moduleContext.logSchema = config.logSchema;
  moduleContext.TagAPI = compiledTagOps.TagAPI;
  moduleContext.SpanLogger = compiledTagOps.SpanLogger;

  // Pre-encode metadata for zero-copy writes
  const encoder = new TextEncoder();
  moduleContext.packageEntry = { utf8: encoder.encode(config.metadata.packageName) };
  moduleContext.packagePathEntry = { utf8: encoder.encode(config.metadata.packagePath) };
  moduleContext.gitShaEntry = { utf8: encoder.encode(config.metadata.gitSha || '') };

  return {
    metadata: config.metadata,
    logSchema: config.logSchema,
    deps: config.deps || {},
    ff: config.ff || {},

    // Add user-extensible context properties
    ctx<Extra>() {
      return {
        // Op factory - creates Op instances with full context type
        op: <Args extends unknown[], Result>(
          fn: (ctx: SpanContext<Schema, Deps, FF, Extra>, ...args: Args) => Promise<Result>
        ): Op<SpanContext<Schema, Deps, FF, Extra>, Args, Result> => new Op(moduleContext, fn),

        // Fluent API for composition
        prefix<P extends string>(p: P) {
          return createPrefixedModule(this, p);
        },

        use(wiredDeps: WiredDeps) {
          return createRootContext(this, wiredDeps);
        },
      };
    },
  };
}
```

## Op Class Generation

The `op()` factory creates Op instances that wrap user functions. Type parameters are ordered to match the function
signature `(ctx, ...args) => Promise<Result>`:

```typescript
class Op<Ctx, Args extends unknown[], Result> {
  constructor(
    readonly name: string, // For Op metrics (invocations, errors, duration)
    private module: ModuleContext, // For gitSha/packageName/packagePath attribution
    private fn: (ctx: Ctx, ...args: Args) => Promise<Result>
  ) {}

  /**
   * Internal invocation - called by span() or Tracer.trace()
   *
   * NOTE: This is pseudo-code for illustration. The actual implementation is in:
   * - Root spans: Tracer._createRootContext() and _executeWithContext() (tracer.ts)
   * - Child spans: SpanContext._spanPre() and span0-span8 methods (spanContext.ts)
   *
   * @param parentBuffer - Parent span's buffer (null for root - Tracer handles root)
   * @param callsiteModule - The CALLER's module (where span() was invoked)
   * @param spanName - Name decided by caller
   * @param lineNumber - Line number where span() was called (injected by transformer, passed directly)
   * @param args - User arguments to the op
   */
  async _invoke(
    parentBuffer: SpanBuffer | null,
    callsiteModule: ModuleContext,
    spanName: string,
    lineNumber: number,
    args: Args
  ): Promise<Result> {
    // 1. Create SpanBuffer with callsiteModule reference
    //    - callsiteModule: where span() was called (for row 0's gitSha/packageName/packagePath)
    //    - this.module: the Op's module (for rows 1+ gitSha/packageName/packagePath)
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
    //    - Uses callsiteModule for gitSha/packageName/packagePath
    //    - lineNumber written DIRECTLY to lineNumber_values[0] (NO intermediate object storage)
    buffer.lineNumber_values[0] = lineNumber; // Direct TypedArray write
    buffer.writeSpanStart(); // Writes timestamp, operation, uses callsiteModule for metadata

    // 4. Create SpanContext (satisfies Ctx constraint)
    //    span() captures the CURRENT module (this.module) as callsiteModule for child spans
    //    User context properties (env, requestId, etc.) come from Tracer's ctxDefaults + overrides
    const opCtx = {
      span: (childLineNumber, name, childOp, ...childArgs) =>
        childOp._invoke(buffer, this.module, name, childLineNumber, childArgs),
      log: new this.module.SpanLogger(buffer),
      tag: new this.module.TagAPI(buffer),
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
    //    Rows 1+ use this.module for gitSha/packageName/packagePath
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

## TagAPI Class Generation

The TagAPI class provides typed attribute methods for `tag`:

### Standard Compilation

```typescript
function compileTagOperations(logSchema: LogSchema) {
  const attributeNames = Object.keys(logSchema);
  const TagAPI = generateTagAPIClass(attributeNames, attributeNames, logSchema);
  const SpanLogger = generateSpanLoggerClass(attributeNames, logSchema);

  return { logSchema, TagAPI, SpanLogger };
}
```

### Library Compilation (Prefixed)

```typescript
function compilePrefixedTagOperations(cleanSchema: LogSchema, prefix: string) {
  const cleanNames = Object.keys(cleanSchema);
  const prefixedNames = cleanNames.map((name) => `${prefix}_${name}`);
  const prefixedSchema = createPrefixedSchema(cleanSchema, prefix);

  const LibraryTagAPI = generateTagAPIClass(cleanNames, prefixedNames, prefixedSchema);
  const LibrarySpanLogger = generateSpanLoggerClass(cleanNames, prefixedSchema);

  return {
    logSchema: prefixedSchema,
    TagAPI: LibraryTagAPI,
    SpanLogger: LibrarySpanLogger,
  };
}
```

### TagAPI Generation Logic

```typescript
function generateTagAPIClass(methodNames: string[], columnNames: string[], schema: LogSchema) {
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
    'BaseTagAPI',
    `
    return class extends BaseTagAPI {
      // Object-based API: tag({ status: 200, method: 'GET' })
      call(attributes) {
        ${callableBody}
        return this;
      }

      ${attributeMethods}
    }
  `
  )(BaseTagAPI);
}
```

## Generated Code Examples

### Standard Module TagAPI

```typescript
// Input: { userId: S.category(), operation: S.enum(['INSERT', 'UPDATE', 'DELETE']) }
// Generated TagAPI:
class StandardTagAPI extends BaseTagAPI {
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

### Library Module TagAPI (with prefix 'http')

```typescript
// Input: { status: S.number(), method: S.category() }, prefix: 'http'
// Generated TagAPI:
class LibraryTagAPI extends BaseTagAPI {
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

## SpanLogger Class Generation

The SpanLogger handles log methods (info/debug/warn/error):

```typescript
function generateSpanLoggerClass(attributeNames: string[], schema: LogSchema) {
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

## SpanContext Type Definition

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
  tag: TagAPI<Schema>;

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
const { op } = userModule;

const createUser = op(async ({ span, log, tag }, userData: UserData) => {
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
const { op } = httpModule;

const GET = op(async ({ span, log, tag, deps }, url: string) => {
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
const processOrder = op(async ({ span, log, tag, deps, ff }, order: Order) => {
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

## Library Definition with defineModule

Libraries define their logSchema and dependencies:

```typescript
// @my-company/http-tracing/src/index.ts
import { defineModule, S } from '@smoothbricks/lmao';

// Define library module
export const httpLib = defineModule({
  metadata: {
    packageName: '@my-company/http-tracing',
    packagePath: 'src/index.ts',
  },
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
const { op } = httpLib;

// Define ops
export const request = op(async ({ span, log, tag, deps }, url: string, opts: RequestInit) => {
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

export const GET = op(async ({ span }, url: string) => {
  return await span('request', request, url, { method: 'GET' });
});

export const POST = op(async ({ span }, url: string, body: unknown) => {
  return await span('request', request, url, {
    method: 'POST',
    body: JSON.stringify(body),
  });
});
```

### Application Wiring

```typescript
// Application wires deps with prefixes
const httpRoot = httpLib.prefix('http').use({
  retry: retryLib.prefix('http_retry').use(),
});

// Invoke via span()
const result = await httpRoot.span('fetch-users', GET, 'https://api.example.com/users');
```

## ESLint Rule: No Capturing Fluent Interface

Because `tag` writes entries, users must not capture the fluent interface:

```typescript
// INVALID - capturing fluent interface
let tagRef = tag;
tagRef.userId(123); // Won't work as expected

// VALID - use directly
tag.userId(123).operation('INSERT');
```

An ESLint rule should prevent capturing `tag` or `log` in variables.

## Performance Characteristics

### Build-Time vs Runtime Costs

- **Build/Startup**: Schema compilation and class generation (~1-5ms per module)
- **Runtime**: Zero overhead for method calls - all mapping pre-computed
- **Memory**: Shared ModuleContext across all ops in same module

### Generated Code Efficiency

```typescript
// Generated method (zero overhead):
userId(value) {
  this.buffer.writeUserId(value);  // Direct method call
  return this;
}

// vs hypothetical runtime mapping (overhead):
setAttribute(name, value) {
  const columnName = this.schema.getColumnName(name);  // Runtime lookup
  this.buffer[`write${columnName}`](value);           // Dynamic method
  return this;
}
```

### SpanBuffer Efficiency

- **Dual module references**: `callsiteModule` for row 0's gitSha/packageName/packagePath, `module` for rows 1+
- **Direct lineNumber writes**: lineNumber passed as argument to span(), written directly to `lineNumber_values[0]` (NO
  intermediate object storage, NO lineNumber property on any context)
- **Interned span names**: `spanName` is interned for dictionary encoding during Arrow conversion
- **Reference to ModuleContext**: References to shared module metadata (no duplication)

## Integration Points

This module context and op generation system integrates with:

- **[Context Flow and Op Wrappers](./01c_context_flow_and_op_wrappers.md)**: Provides SpanContext and span() mechanics
- **[Op Context Pattern](./01l_op_context_pattern.md)**: High-level API for defineOpContext, defineOp, and Tracer
- **[Trace Schema System](./01a_trace_schema_system.md)**: Consumes LogSchema definitions
- **[Library Integration Pattern](./01e_library_integration_pattern.md)**: RemappedBufferView for prefixed columns
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: Generated classes write to SpanBuffer
