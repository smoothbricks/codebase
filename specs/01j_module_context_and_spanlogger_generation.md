# Module Context and TagAPI/SpanLogger Generation

## Overview

The module context system provides the foundation for creating task wrappers with generated TagAPI and SpanLogger
classes. It handles:

1. **Module-level configuration** shared across all tasks in the same module
2. **TagAPI class generation** with typed attribute methods (for `ctx.tag`)
3. **SpanLogger class generation** with log methods (for `ctx.log`)
4. **Schema compilation** for both standard and library modules
5. **Factory patterns** for clean library integration

This system operates at build/startup time to generate efficient runtime code with zero overhead.

## Module Context Definition

**Purpose**: Set up module-level configuration that's shared across all tasks in the same module.

```typescript
// Create module context with tag attributes
const { task } = createModuleContext({
  // Transformer or build tool injects module metadata
  moduleMetadata: {
    gitSha: 'abc123...',
    filePath: 'src/services/user.ts',
    moduleName: 'UserService',
  },
  tagAttributes: dbAttributes, // Use DB-specific attributes
});

// High-level API for standard modules
function createModuleContext(config: { moduleMetadata: ModuleMetadata; tagAttributes: TagAttributeSchema }) {
  // Standard case: method names = column names
  const compiledTagOps = compileTagOperations(config.tagAttributes);

  return createModuleContextFromCompiled(config.moduleMetadata, compiledTagOps);
}

// Low-level API that takes pre-compiled tag operations
function createModuleContextFromCompiled(moduleMetadata: ModuleMetadata, compiledTagOps: CompiledTagOperations) {
  // Register module once, get shared reference
  const moduleContext: ModuleContext = {
    moduleId: registerModule(moduleMetadata),
    gitSha: moduleMetadata.gitSha,
    filePath: moduleMetadata.filePath,

    // Initialize self-tuning capacity stats
    spanBufferCapacityStats: {
      currentCapacity: 64, // Start with cache-friendly size
      totalWrites: 0,
      overflowWrites: 0,
      totalBuffersCreated: 0,
    },
  };

  return {
    task: createTaskWrapper(moduleContext, compiledTagOps),
  };
}
```

**Why This Design**:

- **Shared module context**: All tasks in the same module share metadata and capacity stats
- **Self-tuning**: Each module learns its optimal buffer capacity independently
- **Build tool integration**: Module metadata injected automatically
- **Type safety**: Full TypeScript inference maintained throughout

## TagAPI and SpanLogger Class Generation

The `compileTagOperations` function creates the appropriate TagAPI class (for `ctx.tag`) based on the module type:

### Standard Compilation

```typescript
// Standard compilation: method names = column names
function compileTagOperations(tagAttributes: TagAttributeSchema) {
  const attributeNames = Object.keys(tagAttributes);
  const TagAPI = generateTagAPIClass(attributeNames, attributeNames, tagAttributes);

  return {
    schema: tagAttributes,
    TagAPI,
  };
}
```

### Library Compilation

```typescript
// Library compilation: clean method names → prefixed column names
function compilePrefixedTagOperations(cleanSchema: TagAttributeSchema, prefix: string) {
  const cleanNames = Object.keys(cleanSchema);
  const prefixedNames = cleanNames.map((name) => `${prefix}_${name}`);
  const prefixedSchema = createPrefixedSchema(cleanSchema, prefix);

  const LibraryTagAPI = generateTagAPIClass(cleanNames, prefixedNames, prefixedSchema);

  return {
    schema: prefixedSchema,
    TagAPI: LibraryTagAPI,
  };
}
```

### Core Generation Logic

```typescript
function generateTagAPIClass(methodNames, columnNames, schema) {
  // Generate individual attribute methods for ctx.tag.attribute()
  const attributeMethods = methodNames
    .map(
      (methodName, i) => `
    ${methodName}(value) {
      this._writeTagEntry();
      this.buffer.write${capitalize(columnNames[i])}(value);
      return this;
    }
  `
    )
    .join('\n');

  // Generate callable body for ctx.tag({ ... }) object API
  const callableBody = methodNames
    .map(
      (methodName, i) => `
    if (attributes.${methodName} !== undefined) {
      this.buffer.write${capitalize(columnNames[i])}(attributes.${methodName});
    }
  `
    )
    .join('\n');

  // Create the complete TagAPI class
  return new Function(
    'BaseTagAPI',
    `
    return class extends BaseTagAPI {
      // Callable for object-based API: ctx.tag({ ... })
      call(attributes) {
        this._writeTagEntry();
        ${callableBody}
        return this;
      }

      ${attributeMethods}
    }
  `
  )(TagAPI);
}
```

**Why This Approach**:

- **Zero runtime overhead**: All mapping happens at class generation time
- **Cleaner API**: `ctx.tag.userId()` instead of `ctx.log.tag.userId()`
- **Type safety**: Both standard and library cases maintain full TypeScript inference
- **Dual API**: Both `ctx.tag({ ... })` and `ctx.tag.attr()` supported
- **Separation of concerns**: Standard modules use simple API, libraries handle their own prefixing

## Generated Code Examples

### Standard Module (no prefix)

```typescript
// Input: { user_id: S.category(), operation: S.enum(['INSERT', 'UPDATE', 'DELETE']) }
// Generated TagAPI class (for ctx.tag):
class StandardTagAPI extends BaseTagAPI {
  // Object-based API: ctx.tag({ user_id: "123", operation: "INSERT" })
  call(attributes) {
    this._writeTagEntry();
    if (attributes.user_id !== undefined) {
      this.buffer.writeUserId(attributes.user_id);
    }
    if (attributes.operation !== undefined) {
      this.buffer.writeOperation(attributes.operation);
    }
    return this;
  }

  // Chainable API: ctx.tag.user_id("123")
  user_id(value) {
    this._writeTagEntry();
    this.buffer.writeUserId(value);
    return this;
  }

  operation(value) {
    this._writeTagEntry();
    this.buffer.writeOperation(value);
    return this;
  }
}
```

As `ctx.tag` writes a new entry (and so does `ctx.log.info()` etc) returning a fluent interface, we need an ESLint rule
preventing users from capturing the fluent interface in a variable:

```typescript
let tag = ctx.tag; // INVALID capturing fluent interface
tag.user_id(123); // INVALID USE via captured reference
```

### Library Module (with prefix 'http')

```typescript
// Input: { status: S.number(), method: S.category() }, prefix: 'http'
// Generated TagAPI class:
class LibraryTagAPI extends BaseTagAPI {
  call(attributes) {
    this._writeTagEntry();
    if (attributes.status !== undefined) {
      this.buffer.writeHttpStatus(attributes.status); // Clean attr → prefixed column
    }
    if (attributes.method !== undefined) {
      this.buffer.writeHttpMethod(attributes.method);
    }
    return this;
  }

  status(value) {
    this._writeTagEntry();
    this.buffer.writeHttpStatus(value); // Clean method → prefixed column
    return this;
  }

  method(value) {
    this._writeTagEntry();
    this.buffer.writeHttpMethod(value);
    return this;
  }
}
```

As `ctx.tag` writes a new entry (and so does `ctx.log.info()` etc) returning a fluent interface, we need an ESLint rule
preventing users from capturing the fluent interface in a variable:

```typescript
let entry = ctx.log.info('entry'); // INVALID capturing fluent interface
let tag = ctx.tag; // INVALID
tag.user_id(123); // INVALID USE via captured reference
```

### Library Module (with prefix 'http')

```typescript
// Input: { status: S.number(), method: S.category() }, prefix: 'http'
// Generated class:
class LibrarySpanLogger extends BaseSpanLogger {
  get tag() {
    this._writeTagEntry();
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

  with(attributes) {
    if (attributes.status !== undefined) {
      this.buffer.writeHttpStatus(attributes.status); // Clean attr → prefixed column
    }
    if (attributes.method !== undefined) {
      this.buffer.writeHttpMethod(attributes.method);
    }
    return this;
  }
}
```

## TaskContext Type Definition

The generic `TaskContext<TSchema>` type that task functions receive:

```typescript
// Generic task context type - exported from the main library
type TaskContext<TSchema extends ValidAttributes<TSchema> = {}> = {
  // Span attributes - chainable setters (directly on ctx, not ctx.log.tag)
  tag: TagAPI<TSchema>;

  // Log messages - info/debug/warn/error with scope support
  log: SpanLogger<TSchema>;

  // Span completion methods
  ok: (data?: any) => FluentResult;
  err: (error: string) => FluentResult;

  // Feature flags (logs access to current span)
  ff: FeatureFlagEvaluator;

  // Environment variables (passed through)
  env: EnvironmentConfig;

  // Standard context properties
  requestId: string;
  userId?: string;
  traceId: string;

  // Child span creation
  span: <T>(name: string, fn: (ctx: TaskContext<TSchema>) => Promise<T>) => Promise<T>;
};

// Functions can duck-type by picking only what they need
type TagContext<TSchema> = Pick<TaskContext<TSchema>, 'tag'>;
type LoggingContext<TSchema> = Pick<TaskContext<TSchema>, 'log'>;
type FeatureFlagContext = Pick<TaskContext, 'ff'>;
type MinimalContext<TSchema> = Pick<TaskContext<TSchema>, 'tag' | 'log' | 'ok' | 'err'>;
```

**Design Benefits**:

- **Type safety**: Full TypeScript inference for all context properties
- **Cleaner API**: `ctx.tag` directly on context instead of `ctx.log.tag`
- **Flexibility**: Functions can pick only the context properties they need
- **Generic attributes**: `TSchema` provides typed access to tag attributes
- **OpenTelemetry alignment**: `ctx.tag` mirrors `Span.setAttribute()`

## Library Factory Pattern

For third-party libraries that need prefixed attributes and clean APIs:

```typescript
// Clean schema (no prefixes) - used for typing
const HTTP_SCHEMA = {
  status: S.number(),
  method: S.category(),
  url: S.text(),
  duration: S.number(),
};

// Define library-specific context type once
type Ctx = TaskContext<typeof HTTP_SCHEMA>;

// Define clean functions at top level - much cleaner typing
async function get(ctx: Ctx, url: string, options = {}) {
  const startTime = performance.now();

  ctx.tag.method('GET').url(url); // ✅ TypeScript knows these methods exist

  try {
    const response = await fetch(url, { method: 'GET', ...options });
    ctx.tag.status(response.status).duration(performance.now() - startTime);
    return ctx.ok(response);
  } catch (error) {
    ctx.tag.duration(performance.now() - startTime);
    return ctx.err('HTTP_ERROR');
  }
}

async function post(ctx: Ctx, url: string, data: any, options = {}) {
  const startTime = performance.now();

  ctx.tag.method('POST').url(url); // ✅ TypeScript knows these methods exist

  try {
    const response = await fetch(url, {
      method: 'POST',
      body: JSON.stringify(data),
      ...options,
    });
    ctx.tag.status(response.status).duration(performance.now() - startTime);
    return ctx.ok(response);
  } catch (error) {
    ctx.tag.duration(performance.now() - startTime);
    return ctx.err('HTTP_ERROR');
  }
}

// Module metadata
const HTTP_MODULE_METADATA = {
  gitSha: 'abc123...',
  filePath: '@my-company/http-tracing',
  moduleName: 'HttpTracing',
};

// Factory call with custom span names
export const createHttpLibrary = moduleContextFactory(
  'http', // prefix
  HTTP_MODULE_METADATA, // module metadata
  HTTP_SCHEMA, // clean schema
  {
    get: { fn: get, spanName: 'http-request' },
    post: { fn: post, spanName: 'http-request' },
    // Or different names:
    // get: { fn: get, spanName: 'http-get' },
    // post: { fn: post, spanName: 'http-post' }
  }
);

// Implementation of moduleContextFactory
function moduleContextFactory(
  prefix: string,
  moduleMetadata: ModuleMetadata,
  cleanSchema: TagAttributeSchema,
  functions: Record<string, { fn: Function; spanName: string }>
) {
  // Compile with prefix
  const compiledTagOps = compilePrefixedTagOperations(cleanSchema, prefix);

  // Create module context
  const { task } = createModuleContextFromCompiled(moduleMetadata, compiledTagOps);

  // Wrap each function with task wrapper using custom span names
  const wrappedFunctions = Object.fromEntries(
    Object.entries(functions).map(([name, { fn, spanName }]) => [
      name,
      task(spanName, fn), // Use library author's chosen span name
    ])
  );

  // Return wrapped functions + schema for composition
  return {
    ...wrappedFunctions,
    tagAttributes: compiledTagOps.schema, // Prefixed schema for user composition
  };
}
```

**Why This Pattern**:

- **Clean library APIs**: Libraries use `ctx.tag.status()` but write to `http_status` column
- **Actual operations**: Libraries return real HTTP functions, not just schemas
- **Custom span names**: Library authors control span naming conventions
- **Schema composition**: Prefixed schema available for user composition with other libraries

## Usage in Context Flow

This module context system integrates seamlessly with the runtime context flow:

```typescript
// Module setup (build/startup time)
const { task } = createModuleContext({
  moduleMetadata: {
    /* ... */
  },
  tagAttributes: dbAttributes,
});

// Runtime usage (request processing)
export const createUser = task('create-user', async (ctx, userData) => {
  // ctx.tag is an instance of the generated TagAPI class
  ctx.tag.userId(userData.id).operation('INSERT');

  // ctx.log handles log messages with full TypeScript support
  ctx.log.info('Creating user').with({ email: userData.email });

  return ctx.ok(user);
});
```

## Performance Characteristics

### Build-Time vs Runtime Costs

- **Build/Startup Time**: Schema compilation and class generation (~1-5ms per module)
- **Runtime**: Zero overhead for method calls - all mapping pre-computed
- **Memory**: Shared module context across all tasks in same module
- **Type Safety**: Full TypeScript inference with no runtime type checking

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
  this.buffer[`write${columnName}`](value);           // Dynamic method call
  return this;
}
```

The generated approach eliminates all runtime overhead for attribute mapping.

## Integration Points

This module context and SpanLogger generation system integrates with:

- **[Context Flow and Task Wrappers](./01c_context_flow_and_task_wrappers.md)**: Provides the generated SpanLogger
  classes used in task wrappers
- **[Trace Schema System](./01a_trace_schema_system.md)**: Consumes TagAttributeSchema definitions to generate
  appropriate SpanLogger classes
- **[Library Integration Pattern](./01e_library_integration_pattern.md)**: Uses the factory pattern for clean library
  APIs with prefixed columns
- **[Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**: Generated SpanLogger methods write to the
  columnar SpanBuffer structure
