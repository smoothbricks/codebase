# TypeScript Transformer

The LMAO TypeScript transformer performs compile-time optimizations that enable zero-overhead logging at runtime. It
transforms ergonomic user code into V8-optimized output.

## Design Philosophy

**User writes ergonomic code, transformer produces optimized code.**

The transformer enables two modes:

1. **With transformer**: Monomorphic call sites, direct method calls, zero runtime dispatch
2. **Without transformer**: Polymorphic fallback still works, just slower

This means the library is usable without the transformer (for quick prototyping, REPL usage, etc.) but production builds
should always use the transformer for optimal performance.

## Transformations

### 1. Line Number Injection

Injects source line numbers as the first argument to `span()` calls for source mapping without runtime stack parsing.

```typescript
// User writes:
await ctx.span('fetch-user', fetchUserOp, userId);

// Transformer outputs:
await ctx.span(42, 'fetch-user', fetchUserOp, userId);
```

The line number flows directly to TypedArray writes - it is NEVER stored as a property on any object.

### 2. Monomorphic span() Rewriting

**Problem**: `span()` has multiple overloads (Op invocation, inline closure). A single method with runtime argument
inspection is polymorphic - V8 can't optimize the call site.

**Solution**: Transform to direct monomorphic method calls.

#### Runtime API (SpanContext prototype)

```typescript
class SpanContext {
  // Monomorphic implementations - ctx is ALWAYS passed explicitly
  span_op(line: number, name: string, ctx: SpanContext, op: Op, ...args: unknown[]): Promise<R> {
    // ctx is the context to use (may be 'this', or overridden)
    // Creates child span buffer, invokes op with ctx
  }

  span_fn(line: number, name: string, ctx: SpanContext, fn: () => Promise<R>): Promise<R> {
    // Same pattern for inline closures
  }

  // Polymorphic dispatcher - fallback without transformer
  span(lineOrName: number | string, ...rest: unknown[]): Promise<R> {
    // Parse arguments to determine: line, name, override?, op|fn, args

    // Build the context to use:
    let ctx: SpanContext = this;
    if (hasOverride) {
      ctx = Object.assign(Object.create(this), override);
    }

    // Dispatch to monomorphic implementation:
    if (isOp) {
      return this.span_op(line, name, ctx, op, ...args);
    } else {
      return this.span_fn(line, name, ctx, fn);
    }
  }
}
```

**Key insight**: `span_op`/`span_fn` always receive the context explicitly. Both the transformer and the polymorphic
fallback end up calling the same monomorphic methods - the transformer just skips runtime argument parsing.

#### Transformer Behavior

The transformer statically analyzes `span()` calls to determine which variant is being used:

```typescript
// User writes:
await ctx.span('fetch-user', fetchUserOp, userId);
await ctx.span('compute', async () => heavyComputation());

// Transformer outputs (ctx passed explicitly):
await ctx.span_op(42, 'fetch-user', ctx, fetchUserOp, userId);
await ctx.span_fn(43, 'compute', ctx, async () => heavyComputation());
```

**Context override case**:

```typescript
// User writes:
await ctx.span('external-call', { env: prodEnv }, externalOp, url);

// Transformer outputs (override merged via Object.create):
await ctx.span_op(42, 'external-call', Object.assign(Object.create(ctx), { env: prodEnv }), externalOp, url);
```

**Detection logic**:

- Second argument (after name) is object literal → context override, next arg is op/fn
- Second argument is `Op` instance → `span_op` with `ctx` as context
- Second argument is arrow function or function expression → `span_fn` with `ctx` as context

### 3. Destructured Context Rewriting

**Problem**: When users destructure the context parameter, there's no `ctx` variable to call methods on:

```typescript
const myOp = op(async ({ span, log, tag }, userId) => {
  await span('fetch', fetchOp, userId); // span is destructured, no ctx reference
});
```

**Solution**: Transformer rewrites destructured parameters to preserve context reference:

```typescript
// User writes:
const myOp = op(async ({ span, log, tag }, userId) => {
  await span('fetch', fetchOp, userId);
  log.info('done');
});

// Transformer outputs:
const myOp = op(async (__ctx, userId) => {
  const { log, tag } = __ctx; // span removed - not needed anymore
  await __ctx.span_op(42, 'fetch', fetchOp, userId);
  log.info('done');
});
```

**Transformation steps**:

1. Detect `op()` calls with destructured first parameter
2. Replace destructured param with synthetic `__ctx` variable
3. Move remaining destructured properties to `const` declaration inside function body
4. Remove `span` from destructure (no longer needed)
5. Rewrite `span(...)` calls to `__ctx.span_op(line, name, __ctx, ...)` or `__ctx.span_fn(line, name, __ctx, ...)`
6. For override case: `span(name, {...}, op, args)` →
   `__ctx.span_op(line, name, Object.assign(Object.create(__ctx), {...}), op, args)`

**Edge case - span used as value**:

If `span` is passed as a value (not just called), keep the polymorphic version:

```typescript
// User writes:
const myOp = op(async ({ span }, callback) => {
  callback(span); // span passed as value, not called
});

// Transformer outputs (keeps span in destructure):
const myOp = op(async (__ctx) => {
  const { span } = __ctx; // Keep span - it's used as a value
  callback(span);
});
```

### 4. `with()` Bulk Setter Unrolling

Transforms object-based bulk setters into individual setter calls for V8 inline caching:

```typescript
// User writes:
ctx.tag.with({ userId: 'user-123', requestId: 'req-456' });

// Transformer outputs:
ctx.tag.userId('user-123').requestId('req-456');
```

**Benefits**:

- Eliminates object allocation for the `{...}` literal
- Eliminates `Object.keys()` iteration at runtime
- Each setter call is monomorphic with inline caching

### 5. Metadata Injection

Injects module metadata from build context:

```typescript
// User writes:
const myModule = defineModule({
  logSchema: { userId: S.category() },
  deps: {},
  ff: {},
});

// Transformer outputs:
const myModule = defineModule({
  metadata: {
    packageName: '@mycompany/my-package',
    packagePath: 'src/modules/my-module.ts',
    gitSha: 'abc123def',
  },
  logSchema: { userId: S.category() },
  deps: {},
  ff: {},
});
```

**Metadata sources**:

- `packageName`: From nearest `package.json`
- `packagePath`: Relative path from package root
- `gitSha`: From git HEAD (optional, CI environments)

### 6. Log Line Number Injection

Appends `.line(N)` to log calls for source mapping:

```typescript
// User writes:
log.info('Processing user');
log.warn('Rate limit approaching');

// Transformer outputs:
log.info('Processing user').line(42);
log.warn('Rate limit approaching').line(43);
```

## V8 Optimization Impact

| Transformation       | Without Transformer      | With Transformer         |
| -------------------- | ------------------------ | ------------------------ |
| `span()` dispatch    | Polymorphic (arg check)  | Monomorphic (direct)     |
| `with()` bulk setter | Object alloc + iteration | Direct setter calls      |
| Destructured context | Closure-bound `span`     | Direct `__ctx.span_op()` |
| Line numbers         | Not available            | Zero-cost injection      |
| Metadata             | Manual or missing        | Auto-injected            |

## Implementation Notes

### Detecting Op vs Function

The transformer uses TypeScript's type system to distinguish:

```typescript
// Op instance - has Op type or is result of op() call
await span('name', fetchUserOp, args);     // → span_op

// Function - arrow function or function expression
await span('name', async () => { ... });   // → span_fn
await span('name', async function() { });  // → span_fn
```

### Synthetic Variable Naming

The `__ctx` variable name is chosen to:

- Avoid collision with user variables (double underscore prefix)
- Be short for minimal code size impact
- Be recognizable in stack traces

If `__ctx` is already in scope (unlikely), transformer falls back to `__ctx$1`, `__ctx$2`, etc.

### Source Maps

All transformations preserve source map mappings so debuggers show original source locations.

## Fallback Behavior

Without the transformer, everything still works:

- `span()` polymorphically dispatches to `span_op`/`span_fn` at runtime
- `with()` iterates object keys at runtime
- No line numbers in traces
- Metadata must be provided manually

This enables:

- REPL/playground usage
- Quick prototyping
- Gradual adoption

## Related Specs

- [Entry Types and Logging Primitives](./01h_entry_types_and_logging_primitives.md) - `with()` bulk setter
- [Module Builder Pattern](./01l_module_builder_pattern.md) - Metadata injection, `span()` overloads
- [Context Flow](./01c_context_flow_and_task_wrappers.md) - Line number flow to TypedArrays
