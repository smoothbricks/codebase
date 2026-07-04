# TypeScript Transformer <a id="smoo/lmao!n/transformer"></a>

The LMAO TypeScript transformer performs compile-time optimizations that enable zero-overhead logging at runtime. It
transforms ergonomic user code into V8-optimized output.

> **Implementation status (system state).** The transformer ships in `packages/lmao-transformer/src/`: `transformer.ts`
> (`createLmaoTransformer` + the `visitor`) is the entry factory wired into the TS compile via
> `transformers: { before: [createLmaoTransformer()] }`; `tagChainInliner.ts` holds the `ctx.tag` chain inliner. Every
> transformation below is implemented and tested (`src/__tests__/transformer.test.ts`,
> `src/__tests__/tagChainInliner.test.ts`) **except** §3 Destructured Context Rewriting, which is not implemented (see
> that section). Several sections below were stale relative to the shipped output and have been trued up to the code —
> the running invariant is **the transformed output the tests assert**, not the original sketches.

## Design Philosophy <a id="smoo/lmao!n/transformer-philosophy"></a>

**User writes ergonomic code, transformer produces optimized code.**

The transformer enables two modes:

1. **With transformer**: Monomorphic call sites, direct method calls, zero runtime dispatch
2. **Without transformer**: Polymorphic fallback still works, just slower

This means the library is usable without the transformer (for quick prototyping, REPL usage, etc.) but production builds
should always use the transformer for optimal performance.

## Transformations <a id="smoo/lmao!n/transformer-transformations"></a>

### 1. Line Number Injection <a id="smoo/lmao!n/transformer-line-injection"></a>

Injects source line numbers as the first argument to `span()` calls for source mapping without runtime stack parsing.
The 1-based line number comes from `getLineNumber()` (`getLineAndCharacterOfPosition` + 1) and is reused by every
transformation below that needs a call-site line (span, log, result, task).

```typescript
// User writes:
await ctx.span('fetch-user', fetchUserOp, userId);

// Transformer outputs:
await ctx.span(42, 'fetch-user', fetchUserOp, userId);
```

The line number flows directly to TypedArray writes - it is NEVER stored as a property on any object.

### 2. Monomorphic span() Rewriting <a id="smoo/lmao!n/transformer-span-rewrite"></a>

**Problem**: `span()` has multiple overloads (Op invocation, inline closure) and a variadic argument list. A single
method with runtime argument inspection is polymorphic - V8 can't optimize the call site.

**Solution**: Rewrite to **arity-indexed monomorphic methods** `span0`, `span1`, … `span8` — one per count of trailing
args after the op/fn — so each call site lands on a fixed-shape method. This replaces the earlier `span_op`/`span_fn`
sketch; the shipped runtime exposes `span0`–`span8` on the SpanContext prototype (plus a `spanSync` sync sibling), and
the variadic `span()` dispatcher is the no-transformer fallback that routes to the same `spanN` methods
(`packages/lmao/src/lib/spanContext.ts`, node `smoo/lmao!n/context-flow-span-promise`).

#### Transformer Behavior

`tryTransformSpanCall` statically analyzes each `ctx.span('name', opOrFn, ...rest)` call. The method name is
`span${rest.length}`, and the transformer emits **6 fixed arguments before the op/fn**, so the runtime method never
inspects an Op object at the call site:

1. `line: number` — the call-site line (`getLineNumber`)
2. `name: string` — the span name (unchanged)
3. `childCtx: SpanContext` — `ctx._newCtx0()` (the child context, allocated explicitly)
4. `SpanBufferClass` — the span-buffer constructor
5. `remappedViewClass` — `RemappedViewConstructor | undefined`
6. `opMetadata` — the per-Op metadata

```typescript
// User writes:
await ctx.span('fetch-user', fetchUserOp, userId);
ctx.span('compute', async () => heavyComputation());

// Transformer outputs (Op: pull SpanBufferClass/remappedViewClass/metadata/fn off the op):
await ctx.span1(
  42,
  'fetch-user',
  ctx._newCtx0(),
  fetchUserOp.SpanBufferClass,
  fetchUserOp.remappedViewClass,
  fetchUserOp.metadata,
  fetchUserOp.fn,
  userId
);

// Transformer outputs (plain fn: take buffer class + metadata off ctx._buffer, remappedView = undefined):
ctx.span0(43, 'compute', ctx._newCtx0(), ctx._buffer.constructor, undefined, ctx._buffer._opMetadata, async () =>
  heavyComputation()
);
```

**Detection logic** (Op vs function, §"Detecting Op vs Function"):

- With a `TypeChecker`: the 2nd argument's type starts with/contains `Op` → Op path; else function path.
- Without a `TypeChecker`: a non-(arrow|function) literal is assumed to be an Op; arrow/function expressions are the fn
  path.
- Op path → `op.SpanBufferClass`, `op.remappedViewClass`, `op.metadata`, `op.fn`. Function path →
  `ctx._buffer.constructor`, `undefined`, `ctx._buffer._opMetadata`, and the function expression itself.

> **Implementation note — context override not handled.** The earlier `span('name', { env }, op, …)` override form
> (merging via `Object.assign(Object.create(ctx), …)`) is **not** implemented in `tryTransformSpanCall`; the function
> requires the 2nd arg to be the op/fn directly. If the override form is still wanted it is unimplemented work, tracked
> by node `smoo/lmao!n/transformer-span-override`.

### 3. Destructured Context Rewriting <a id="smoo/lmao!n/transformer-destructured-context"></a>

**Status: NOT IMPLEMENTED in the transformer.** The `__ctx` rewrite described here does not exist in
`packages/lmao-transformer/src/` — there is no `__ctx` synthesis and `tryTransformSpanCall` only rewrites
`ctx.span(...)` property-access calls, not a bare destructured `span(...)` call. Tracked as unimplemented work by node
`smoo/lmao!n/transformer-destructured-context`.

**The destructured case still works at runtime — without this optimization.** When a user destructures the context
(`const myOp = op(async ({ span, log, tag }, userId) => …)`), the destructured `span`/`log`/`tag` are real closures
bound to the context at codegen time (`packages/lmao/src/lib/spanContext.ts`, node
`smoo/lmao!n/codegen-destructured-context`). So `span('fetch', fetchOp, userId)` dispatches through the variadic
`span()` fallback (§"Fallback Behavior") and is correct, just not rewritten to a monomorphic `spanN` call. This section
describes a **future** compile-time optimization, not current behavior.

The originally-sketched approach was: detect `op()` calls with a destructured first param, replace it with a synthetic
`__ctx`, move the remaining destructured props to a `const { … } = __ctx` inside the body, and rewrite each `span(...)`
to `__ctx.spanN(line, name, __ctx._newCtx0(), …)` — keeping `span` in the destructure only when it is passed as a value
rather than called.

### 4. `with()` Bulk Setter Unrolling <a id="smoo/lmao!n/transformer-tag-chain-inline"></a>

The `ctx.tag` chain inliner (`tagChainInliner.ts`) transforms tag setter chains — including the `with()` bulk setter —
into **direct columnar buffer writes** (not chained setter calls). It only fires in **statement context** (an
`ExpressionStatement` whose expression is a `ctx.tag…` chain); a tag chain assigned to a variable is left alone.

```typescript
// User writes:
ctx.tag.with({ userId: 'user-123', requestId: 'req-456' });

// Transformer outputs (a block of direct null-bitmap + value-array writes per field):
{
  ctx._buffer.userId_nulls[0] |= 1;
  ctx._buffer.userId_values[0] = 'user-123';
  ctx._buffer.requestId_nulls[0] |= 1;
  ctx._buffer.requestId_values[0] = 'req-456';
}
```

The same lowering applies to plain setter chains (`ctx.tag.operation('SELECT').userId('user-123')`) and to single calls.
With a `TypeChecker` and a known tag schema, boolean fields use bit-packed `|= 1` / `&= ~1` writes and enum fields map
to their sorted-order index via a switch IIFE; non-literal arguments are wrapped in a
`const $$vN = expr; if ($$vN != null) { … }` null check (`generateFieldWriteStatements` and the `generate*Write`
helpers).

**Benefits**:

- Eliminates the object allocation for the `{...}` literal and any intermediate fluent-builder objects.
- Eliminates `Object.keys()` iteration at runtime — each field is a direct, monomorphic TypedArray write.
- Null/eager handling is resolved at compile time from the schema.

### 5. Metadata Injection <a id="smoo/lmao!n/transformer-metadata-inject"></a>

Injects module metadata from build context. `tryTransformDefineModuleCall` rewrites `defineModule({...})` (direct or
`x.defineModule({...})`) by prepending a `metadata` property to its first object-literal argument, and is a no-op when a
`metadata` property already exists:

```typescript
// User writes:
const myModule = defineModule({
  logSchema: { userId: S.category() },
  deps: {},
  ff: {},
});

// Transformer outputs (metadata prepended, snake_case keys, git_sha first):
const myModule = defineModule({
  metadata: {
    git_sha: 'abc123def',
    package_name: '@mycompany/my-package',
    package_file: 'src/modules/my-module.ts',
  },
  logSchema: { userId: S.category() },
  deps: {},
  ff: {},
});
```

**Metadata sources** (`findNearestPackage` + `getLastGitCommit`):

- `package_name`: `name` from the nearest `package.json` (walking up); `'unknown'` if none.
- `package_file`: path of the source file relative to that package dir (basename fallback).
- `git_sha`: `git log -1 --format=%H` for the file; `'unknown'` if git is unavailable.

> **Cross-reference / known divergence.** Spec `01l §metadata-injection` (node `smoo/lmao!n/metadata-injection`)
> describes injection at the **`defineOpContext`** call site with a `__metadata` key — that is the _consumer_ contract
> (`createOpMetadata` in `packages/lmao/src/lib/opContext/defineOp.ts` builds the per-Op `OpMetadata`). The transformer
> as shipped injects into **`defineModule`** with a plain `metadata` key and the three snake_case fields above. The
> snake_case key names match the runtime/Arrow contract (`smoo/lmao!n/arrow-table-module-id`); the `defineOpContext`
> injection path is not implemented by this transformer.

### 6. Log Line Number Injection <a id="smoo/lmao!n/transformer-log-line"></a>

Appends `.line(N)` to log calls (and to `ctx.ok()`/`ctx.err()` result calls) for source mapping:

```typescript
// User writes:
log.info('Processing user');
log.warn('Rate limit approaching');

// Transformer outputs:
log.info('Processing user').line(42);
log.warn('Rate limit approaching').line(43);
```

The transform finds the log method call inside a chain (`findLogCallInChain`), marks the chain processed, and inserts
`.line(N)` immediately after the log method — preserving any trailing fluent calls (`ctx.log.info('msg').userId('123')`
→ `ctx.log.info('msg').line(N).userId('123')`). It is a no-op if `.line(...)` is already present (`hasLineInChain`). The
recognised methods are `info`/`debug`/`warn`/`error`/`trace`.

The same `.line(N)` insertion applies to the **result chain** `ctx.ok(...)` / `ctx.err(...)` via
`tryTransformResultChain` (`findResultCallInChain`), preserving trailing `.with(...)` / `.message(...)` calls:

```typescript
// User writes:
return ctx.ok(user).with({ userId: user.id }).message('Created');

// Transformer outputs:
return ctx.ok(user).line(42).with({ userId: user.id }).message('Created');
```

### 7. `task()` Line Number Injection <a id="smoo/lmao!n/transformer-task-line"></a>

`tryTransformTaskCall` appends the call-site line number as a trailing argument to `task('name', fn)` calls — both the
property-access form (`module.task('name', fn)`) and the destructured-direct form (`task('name', fn)` from
`const { task } = createModuleContext(...)`). It fires only when there are exactly two arguments and the first is a
string literal (so an already-lined `task('name', fn, 99)` and a dynamic name like `task(getName(), fn)` are left
alone):

```typescript
// User writes:
module.task('processOrder', async (ctx) => {});

// Transformer outputs (line number appended as the 3rd argument):
module.task('processOrder', async (ctx) => {}, 42);
```

## V8 Optimization Impact <a id="smoo/lmao!n/transformer-v8-impact"></a>

| Transformation       | Without Transformer      | With Transformer                  |
| -------------------- | ------------------------ | --------------------------------- |
| `span()` dispatch    | Polymorphic (arg check)  | Monomorphic (`span0`–`span8`)     |
| `with()` bulk setter | Object alloc + iteration | Direct columnar buffer writes     |
| Destructured context | Closure-bound `span`     | Closure-bound `span` (not yet §3) |
| Line numbers         | Not available            | Zero-cost injection               |
| Metadata             | Manual or missing        | Auto-injected                     |

## Implementation Notes <a id="smoo/lmao!n/transformer-impl-notes"></a>

### Detecting Op vs Function <a id="smoo/lmao!n/transformer-detect-op"></a>

`tryTransformSpanCall` distinguishes Op from inline function to choose the buffer/metadata/fn arguments (it does **not**
pick a method name from this — the method is always `span${restCount}`):

```typescript
// With a TypeChecker: type name starts-with/contains 'Op' → Op path
await ctx.span('name', fetchUserOp, args);   // → op.SpanBufferClass / op.remappedViewClass / op.metadata / op.fn

// Function — arrow function or function expression → function path
ctx.span('name', async () => { ... });        // → ctx._buffer.constructor / undefined / ctx._buffer._opMetadata / fn
ctx.span('name', async function() { });       // → function path

// Without a TypeChecker: any non-(arrow|function) literal is assumed to be an Op.
```

### Synthetic Variable Naming <a id="smoo/lmao!n/transformer-synthetic-naming"></a>

The `__ctx` synthetic-variable scheme belongs to the **not-yet-implemented** §3 Destructured Context Rewriting; no
`__ctx` is emitted by the shipped transformer. The tag-chain inliner that _is_ shipped uses `$$vN` temporaries
(`generateVarName`) for non-literal tag values, chosen to avoid collision with user variables. (Recorded under the §3
work node `smoo/lmao!n/transformer-destructured-context`.)

### Source Maps <a id="smoo/lmao!n/transformer-source-maps"></a>

The transformations are AST rewrites via the TS `NodeFactory`, so positions map through TypeScript's own emit. The tag
inliner additionally **strips** inherited comments and source positions from its synthesized statements
(`clearComments`: `setSyntheticLeadingComments`/`setSyntheticTrailingComments`/`setSourceMapRange` cleared,
`setTextRange` reset) to prevent the generated buffer-write block from inheriting nearby source comments.

> **Implementation status.** A dedicated source-map preservation/verification pass is not separately implemented or
> tested; correctness relies on factory-based emit plus the tag-inliner comment scrub. Tracked by node
> `smoo/lmao!n/transformer-source-maps`.

## Fallback Behavior <a id="smoo/lmao!n/transformer-fallback"></a>

Without the transformer, everything still works:

- `span()` is the variadic dispatcher that detects `(line?, name, op|fn, ...args)` via `arguments.length` and routes to
  the same monomorphic `span0`–`span8` (`packages/lmao/src/lib/spanContext.ts`,
  `smoo/lmao!n/context-flow-span-promise`).
- `with()` / tag setters iterate at runtime through the generated fluent builders instead of the inlined buffer writes.
- No line numbers in traces (the runtime dev fallback can parse the stack — `extractMetadataFromStack`).
- Metadata must be provided manually (`DEFAULT_METADATA` is the un-injected sentinel).

This enables:

- REPL/playground usage
- Quick prototyping
- Gradual adoption

## Related Specs <a id="smoo/lmao!n/transformer-related"></a>

- [Entry Types and Logging Primitives](./01h_entry_types_and_logging_primitives.md) - `with()` bulk setter
- [Op Context Pattern](./01l_op_context_pattern.md) - Metadata injection, `span()` overloads
- [Context Flow](./01c_context_flow_and_op_wrappers.md) - Line number flow to TypedArrays
- [Trace Logging System](./01_trace_logging_system.md) - the transformer-overview sections (line-number injection,
  monomorphic call sites, context destructuring)
