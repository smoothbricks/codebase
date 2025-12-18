# Module Context and Op/SpanLogger Generation

## Overview

The module context system provides the foundation for creating ops with generated TagAPI and SpanLogger classes. It
handles:

1. **Module-level configuration** shared across all ops in the same module
2. **Op class generation** for traced operations with proper type parameters
3. **TagAPI class generation** with typed attribute methods (for `tag`)
4. **SpanLogger class generation** with log methods (for `log`)
5. **Schema compilation** for both standard and library modules
6. **User-extensible context** via `.ctx<Extra>()` for custom properties

This system operates at build/startup time to generate efficient runtime code with zero overhead.

## Design Rationale: From ctx to Destructured Op Context

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

### Solution: Destructured Op Context

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
// Define module with schema, dependencies, feature flags, and user context
const userModule = defineModule({
  metadata: {
    packageName: '@my-company/user-service',
    packagePath: 'src/user.ts',
  },
  schema: {
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
`deps`, and `ff`. These properties are spread into OpContext and can be destructured by ops.

### Internal Implementation

```typescript
interface ModuleMetadata {
  packageName: string;
  packagePath: string;
  gitSha?: string; // Optional - injected by transformer at build time
}

function defineModule<Schema, Deps, FF>(config: { metadata: ModuleMetadata; schema: Schema; deps?: Deps; ff?: FF }) {
  // Compile schema to generate TagAPI and SpanLogger classes
  const compiledTagOps = compileTagOperations(config.schema);

  // Create module context
  const moduleContext: ModuleContext = {
    metadata: config.metadata,

    // Compiled classes
    TagAPI: compiledTagOps.TagAPI,
    SpanLogger: compiledTagOps.SpanLogger,
    compiledSchema: compiledTagOps.schema,

    // Self-tuning capacity stats
    spanBufferCapacityStats: {
      currentCapacity: 64,
      totalWrites: 0,
      overflowWrites: 0,
      totalBuffersCreated: 0,
    },
  };

  return {
    metadata: config.metadata,
    schema: config.schema,
    deps: config.deps || {},
    ff: config.ff || {},

    // Add user-extensible context properties
    ctx<Extra>() {
      return {
        // Op factory - creates Op instances with full context type
        op: <Args extends unknown[], Result>(
          fn: (ctx: OpContext<Schema, Deps, FF, Extra>, ...args: Args) => Promise<Result>
        ): Op<OpContext<Schema, Deps, FF, Extra>, Args, Result> => new Op(moduleContext, fn),

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
    private module: ModuleContext,
    private fn: (ctx: Ctx, ...args: Args) => Promise<Result>
  ) {}

  /**
   * Internal invocation - called by span()
   */
  async _invoke(
    traceCtx: TraceContext,
    parentBuffer: SpanBuffer | null,
    spanName: string,
    args: Args
  ): Promise<Result> {
    // 1. Create SpanBuffer
    const buffer = createSpanBuffer(
      this.module.compiledSchema,
      this.module.metadata,
      traceCtx.traceId,
      traceCtx.threadId
    );

    // 2. Link to parent
    if (parentBuffer) {
      parentBuffer.children.push(buffer);
      buffer.parent = parentBuffer;
    }

    // 3. Write span-start
    buffer.writeSpanStart(spanName);

    // 4. Create OpContext (satisfies Ctx constraint)
    const opCtx = {
      span: (name, childOp, ...childArgs) => childOp._invoke(traceCtx, buffer, name, childArgs),
      log: new this.module.SpanLogger(buffer),
      tag: new this.module.TagAPI(buffer),
      deps: this.module.boundDeps,
      ff: traceCtx.ff.withBuffer(buffer),
      // Spread Extra properties from TraceContext (e.g., env, requestId, userId)
      ...extractExtraFromTraceContext(traceCtx),
    } as Ctx;

    // 5. Execute with try/catch
    try {
      const result = await this.fn(opCtx, ...args);
      buffer.writeSpanOk();
      return result;
    } catch (error) {
      buffer.writeSpanException(error);
      throw error;
    }
  }
}
```

## TagAPI Class Generation

The TagAPI class provides typed attribute methods for `tag`:

### Standard Compilation

```typescript
function compileTagOperations(tagAttributes: TagAttributeSchema) {
  const attributeNames = Object.keys(tagAttributes);
  const TagAPI = generateTagAPIClass(attributeNames, attributeNames, tagAttributes);
  const SpanLogger = generateSpanLoggerClass(attributeNames, tagAttributes);

  return {
    schema: tagAttributes,
    TagAPI,
    SpanLogger,
  };
}
```

### Library Compilation (Prefixed)

```typescript
function compilePrefixedTagOperations(cleanSchema: TagAttributeSchema, prefix: string) {
  const cleanNames = Object.keys(cleanSchema);
  const prefixedNames = cleanNames.map((name) => `${prefix}_${name}`);
  const prefixedSchema = createPrefixedSchema(cleanSchema, prefix);

  const LibraryTagAPI = generateTagAPIClass(cleanNames, prefixedNames, prefixedSchema);
  const LibrarySpanLogger = generateSpanLoggerClass(cleanNames, prefixedSchema);

  return {
    schema: prefixedSchema,
    TagAPI: LibraryTagAPI,
    SpanLogger: LibrarySpanLogger,
  };
}
```

### TagAPI Generation Logic

```typescript
function generateTagAPIClass(methodNames: string[], columnNames: string[], schema: TagAttributeSchema) {
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
function generateSpanLoggerClass(attributeNames: string[], schema: TagAttributeSchema) {
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

## OpContext Type Definition

The context destructured by op functions. The `Extra` type parameter allows user-defined fields (like `env` with CF
Worker bindings):

```typescript
// Full OpContext type - Extra is spread in for user extensibility
type OpContext<Schema, Deps, FF, Extra = {}> = {
  // Invoke another op as child span (6 overloads, see span() docs)
  span: SpanFn<OpContext<Schema, Deps, FF, Extra>>;

  // Log messages
  log: SpanLogger;

  // Span attributes (chainable)
  tag: TagAPI<Schema>;

  // Dependencies - can be destructured!
  deps: Deps;

  // Feature flags (logs access to current span)
  ff: FeatureFlagEvaluator<FF>;
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
type LoggingContext = Pick<OpContext<{}, {}, {}>, 'log'>;
type TaggingContext<S> = Pick<OpContext<S, {}, {}>, 'tag'>;
type MinimalContext<S> = Pick<OpContext<S, {}, {}>, 'span' | 'log' | 'tag'>;
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

Libraries define their schema and dependencies:

```typescript
// @my-company/http-tracing/src/index.ts
import { defineModule, S } from '@smoothbricks/lmao';

// Define library module
export const httpLib = defineModule({
  metadata: {
    packageName: '@my-company/http-tracing',
    packagePath: 'src/index.ts',
  },
  schema: {
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
- **Memory**: Shared module context across all ops in same module

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

## Integration Points

This module context and op generation system integrates with:

- **[Context Flow and Op Wrappers](./01c_context_flow_and_task_wrappers.md)**: Provides OpContext and span() mechanics
- **[Module Builder Pattern](./01l_module_builder_pattern.md)**: High-level API for defineModule + op()
- **[Trace Schema System](./01a_trace_schema_system.md)**: Consumes TagAttributeSchema definitions
- **[Library Integration Pattern](./01e_library_integration_pattern.md)**: RemappedBufferView for prefixed columns
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: Generated classes write to SpanBuffer
