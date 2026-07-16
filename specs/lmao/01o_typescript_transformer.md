# ttsc TypeScript Transformer <a id="smoo/lmao!n/transformer"></a>

The LMAO TypeScript transformer performs compile-time rewrites that remove runtime dispatch and setup where static proof
is available. The optimizations below describe emitted code and runtime contracts; they do not imply an unmeasured
speedup.

> **Implementation status (system state).** The sole compiler implementation is the native ttsc/tsgo plugin in
> `packages/lmao-ttsc/plugin/`. Bun build and runtime hosts load that plugin through the package's `@ttsc/unplugin`
> adapters; there is no parallel TypeScript compiler-API transformer. Destructured-context rewriting (§3),
> Promise-preserving fixed-arity span lowering, packed runtime hints, private `u16` log-template storage, and the
> existing line, metadata, tag, log, and result transformations are implemented in the native plugin and covered by its
> Go suite plus Bun integration. `spanAutoN` exists as an internal, explicit runtime seam, but automatic `ctx.span(...)`
> → `spanAutoN(...)` lowering is intentionally disabled because a synchronous return would change the public Promise
> API's observable microtask scheduling. The running invariant is **the transformed output the tests assert**, not
> earlier design sketches. The global vocabulary ABI in §6.2 is the clean-cutover target, not shipped behavior. The
> shipped Op-local IDs remain lexical `u16` values, not stable IDs or `VocabularyBinding` indices. No performance
> improvement is claimed for either representation without its phase-specific benchmark gate.

## Bun integration

Install `@smoothbricks/lmao-ttsc` as a direct dependency. ttsc discovers its native descriptor from the package's
`ttsc.plugin` field; consumers do not register a second transformer implementation.

For `Bun.build`, use the package's adapter export:

```typescript
import ttsc from '@smoothbricks/lmao-ttsc/bun';

await Bun.build({
  entrypoints: ['src/index.ts'],
  outdir: 'dist',
  plugins: [ttsc()],
});
```

For Bun's runtime loader (`bun run` and `bun test`), register the same adapter once in `bunfig.toml`:

```toml
[test]
preload = ["@smoothbricks/lmao-ttsc/bun-register"]
```

Both entrypoints read the nearest TypeScript project and apply its configured and direct-dependency ttsc plugins. The
`./bun-register` export is intentionally marked as a package side effect; the build adapter itself is side-effect free.

## Design Philosophy <a id="smoo/lmao!n/transformer-philosophy"></a>

**User writes ergonomic code, transformer produces optimized code.**

The transformer enables two modes:

1. **With transformer**: Monomorphic call sites, direct method calls, zero runtime dispatch
2. **Without transformer**: Polymorphic fallback still works, just slower

This means the library is usable without the transformer (for quick prototyping, REPL usage, etc.) but production builds
should always use the transformer for optimal performance.

## Transformations <a id="smoo/lmao!n/transformer-transformations"></a>

### 1. Line Number Injection <a id="smoo/lmao!n/transformer-line-injection"></a>

Injects the 1-based source line as the first runtime argument when a span call can be lowered without changing
evaluation or Promise semantics. The line comes from `getLineNumber()` (`getLineAndCharacterOfPosition` + 1) and is also
reused by log, result, and task rewrites.

```typescript
// User writes:
await ctx.span('fetch-user', fetchUserOp, userId);

// Stable, checker-proved Op:
await ctx.span1(
  42,
  'fetch-user',
  Object.create(ctx),
  fetchUserOp.SpanBufferClass,
  fetchUserOp.remappedViewClass,
  fetchUserOp.metadata,
  fetchUserOp.fn,
  userId,
  fetchUserOp.runtimeHint
);
```

A dynamic or unstable Op expression that cannot be expanded exactly once remains on `span()` unchanged, so it does not
receive compile-time span-line injection. The line number in lowered calls flows directly to TypedArray writes; it is
never stored as a property on the context.

### 2. Monomorphic span() Rewriting <a id="smoo/lmao!n/transformer-span-rewrite"></a>

`tryTransformSpanCall` conservatively lowers supported calls to the Promise-based `span0`–`span8` ABI. It requires at
most eight trailing arguments, a stable receiver (`identifier` or `this`), and either:

1. a checker-proved `Op` represented by a stable identifier, whose buffer class, remapped view, metadata, function, and
   runtime hint can each be read once; or
2. an inline arrow/function expression, using the receiver's buffer class and metadata and runtime hint `0`.

```typescript
// User writes:
await ctx.span('fetch-user', fetchUserOp, userId);
ctx.span('compute', async () => heavyComputation());

// Transformer outputs:
await ctx.span1(
  42,
  'fetch-user',
  Object.create(ctx),
  fetchUserOp.SpanBufferClass,
  fetchUserOp.remappedViewClass,
  fetchUserOp.metadata,
  fetchUserOp.fn,
  userId,
  fetchUserOp.runtimeHint
);
ctx.span0(
  43,
  'compute',
  Object.create(ctx),
  ctx._buffer.constructor,
  undefined,
  ctx._buffer._opMetadata,
  async () => heavyComputation(),
  0
);
```

The child is exactly `Object.create(receiver)`, not `_newCtx0()` and not a copied or merged object. Because `spanN`
remains Promise-based, the lowered call preserves the public `span()` scheduling contract.

#### Evaluation and safety bailouts

The transformer never duplicates an unstable receiver or Op expression. Calls such as
`getCtx().span('name', getOp(), value)` remain byte-shape calls to the public variadic `span()` dispatcher: receiver and
Op are each evaluated once in source order, and the returned Promise retains its normal microtask boundary. Other
bailouts include a missing checker for an Op value, unproved receiver/Op types, more than eight trailing arguments,
override form `span(name, overrides, op, ...)`, and any unsupported function expression. Every bailout preserves the
public runtime path.

Automatic `spanAutoN` lowering is **not** considered safe merely because a call is directly awaited or returned from an
`async` function. Although both `Result` and `Promise<Result>` are await-compatible, returning a synchronous `Result`
changes the number/order of Promise-assimilation microtasks. Exact observable scheduling takes precedence over removing
Promise allocation.

#### Internal `spanAutoN` seam

The runtime still exposes `spanAuto0`–`spanAuto8` as an internal/explicit ABI returning `Result | Promise<Result>`.
`_spanAutoPre` performs `Object.create(this)`, passes that exact inherited child to `_spanPre`, and invokes the Op once
on the initial path. A genuine `Ok`/`Err` terminal result with no retry requirement is finalized and returned
synchronously. All other values—including custom thenables—transfer to the async helper and are assimilated once; a
retryable error also transfers to that helper. The helper awaits the first value, applies the normal retry policy
(re-invoking only for an actual retry), records retry and terminal rows, and ends tracing exactly once. Throws and
rejections use the normal span-exception path. This ABI is not emitted automatically from public `span()`.

#### Packed runtime hint, structured compile metadata, and specialized setup

For checker-proved LMAO calls, the transformer computes a packed unsigned 32-bit runtime hint and emits it inside the
structured `OpCompileMetadata` ABI. `defineOp(...)` must have result type `Op<...>`/symbol `Op`, and `defineOps(...)`
must have result type `OpGroup<...>`/symbol `OpGroup`; the resolved call declaration must come from LMAO. Same-named
unrelated functions and calls without checker-backed LMAO declaration provenance receive no compile metadata.

| Bits     | Meaning                                                                        |
| -------- | ------------------------------------------------------------------------------ |
| `0..15`  | initial row capacity (`0` means adaptive/unspecified)                          |
| `16..22` | required `tag`, `log`, `ff`, nested-span, result, scope, and deps capabilities |
| `23`     | analysis-valid marker                                                          |
| `24..31` | reserved; any set bit invalidates the hint                                     |

Analysis starts capacity at two reserved rows (span start/tags and terminal result), then adds one for each statically
encountered direct `ctx.log.info/debug/warn/error/trace(...)`. A loop makes capacity unknown and encodes zero while
still retaining safely proven capability bits. Capacity overflow beyond `0xffff` also becomes unknown. At runtime, a
nonzero analyzed capacity is clamped to a minimum of two; zero uses the normal adaptive capacity.

Capability analysis is closed-world. It accepts only direct, recognized forms: calls through `tag`/known log methods,
`ff(...)` or `ff.flag(...)`, direct `span`/`spanSync`/`ok`/`err`/`setScope` calls, and property access through `scope`
or `deps`. It emits hint `0` if the first parameter is not a simple identifier, the context escapes or is used as a
value, access is computed/unknown, a nested function is entered, or another use cannot be proven safe. Consequently a
callback whose first parameter is destructured is conservatively hinted as `0` even when the separate §3 rewrite
succeeds.

`defineOp` is annotated only when no fourth argument already exists. Its fourth argument is
`{ runtimeHint, logTemplateIds }`; missing user metadata is represented by an inserted third argument `undefined`.
`defineOps` is annotated only when called with a single object literal; its second argument is a property-keyed map of
the same structured metadata. The map includes inline arrow functions, function expressions, and method declarations,
keyed by literal property name. Shorthand properties and property assignments containing an existing Op
identifier/expression are omitted rather than assigned empty metadata; an analyzable inline callback that fails
closed-world hint analysis keeps its own key with `runtimeHint: 0`. This object ABI replaces the earlier bare
packed-hint argument/map: the runtime normalizes both fields, installs `runtimeHint` on the Op, and copies the frozen
template table to `OpMetadata.logTemplateIds`.

Hint injection and destructured-context rewriting compose in one visitor step: the original callback is analyzed first,
then the hint-bearing call is passed to §3 rewriting. A call may therefore receive both changes; if §3 later bails, the
already-proved hint injection remains.

`_spanPre` trusts specialization only when the value is an in-range integer, bit 23 is set, and reserved bits are clear.
Otherwise it installs the full context surface. A valid hint constructs only the required own properties. Logger setup
is shared by `log`, `ff`, and scope; `ff` binds via `forContext(ctx)` to the **exact inherited child**, not a copied
context. Unused capabilities remain inherited or absent rather than being eagerly allocated, while `_buffer`, `_schema`,
and `_logBinding` are always rebound to the child span.

### 3. Destructured Context Rewriting <a id="smoo/lmao!n/transformer-destructured-context"></a>

**Status: implemented in the TypeScript transformer.** For arrow/function literals passed to `op`, `defineOp`,
`defineOps`, or `task`, a destructured first parameter containing `span` may be replaced with `__ctx`. Other
destructured properties are rebound as the first body statement, preserving aliases, defaults, and nested bindings.
Every bare `span(...)` call must support the same Promise-preserving §2 lowering; otherwise the whole function is left
unchanged.

```typescript
// User writes:
op(async ({ span, log }, userId) => {
  await span('fetch', fetchUserOp, userId);
});

// Stable, checker-proved Op:
op(async (__ctx, userId) => {
  const { log } = __ctx;
  await __ctx.span1(
    42,
    'fetch',
    Object.create(__ctx),
    fetchUserOp.SpanBufferClass,
    fetchUserOp.remappedViewClass,
    fetchUserOp.metadata,
    fetchUserOp.fn,
    userId,
    fetchUserOp.runtimeHint
  );
});
```

The whole function is left unchanged if `span` is passed or otherwise used as a value, is shadowed, a rest binding is
present, `__ctx` would collide, any bare span call cannot be lowered safely, or no `span` binding exists. This all-or-
nothing preflight prevents a mixed rewrite from changing destructuring semantics. Concise arrow bodies are converted to
a block with an explicit `return`.

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

#### Op-local `u16` log-template IDs

The same checker-backed prepass can replace an eligible literal log write with a private numeric write. Eligibility is
intentionally narrow: the enclosing Op must be an inline arrow, function expression, or method with a simple identifier
as its first parameter; the call must be direct `ctx.log.info/debug/warn/error/trace(message)` with exactly one
argument; `ctx` must be that first parameter; the `ctx.log` value must be checker-proved as LMAO `SpanLogger` or
`GeneratedSpanLogger`; and `message` must be a string literal or no-substitution template literal. Analysis never
crosses a nested function. Trailing fluent links, including an existing `.line(...)`, are preserved around the rewritten
inner call.

The compiler uses the literal's **cooked string value** (`node.text`/the tsgo AST equivalent), not source-token
spelling. It deduplicates equal cooked strings within each Op and assigns IDs in first lexical encounter order. ID `0`
is reserved for the dynamic/raw-message path; unique templates receive IDs `1..65535`, and
`OpMetadata.logTemplateIds[id - 1]` is the only lookup table. Once 65,535 unique strings have been assigned, later
unseen strings remain ordinary dynamic writes; previous duplicates continue to reuse their assigned ID. These IDs are
private to one Op and are neither stable across builds nor comparable between Ops.

```typescript
// User writes:
const load = defineOp('load', async (ctx) => {
  ctx.log.info('cache\nhit').userId('42');
  ctx.log.warn(`cache\nhit`);
  ctx.log.info(getMessage());
});

// Contractual shape (irrelevant printer details omitted):
defineOp('load', async (ctx) => {
  ctx.log._infoTemplate(1).line(42).userId('42');
  ctx.log._warnTemplate(1).line(43);
  ctx.log.info(getMessage()).line(44);
}, undefined, {
  runtimeHint: /* packed value */,
  logTemplateIds: ['cache\nhit'],
});
```

A dynamic expression, substitution template, multiple/zero arguments, alias or computed access, unrelated/shadowed
logger type, nested-function call, non-inline Op definition, missing checker proof, or saturated new template bails out
of ID encoding without changing logging behavior. It still uses the existing raw string column and ordinary line
transform where eligible.

At runtime, a template-bearing Op conditionally adds a zero-initialized `Uint16Array` lane to each JS `SpanBuffer`
system allocation; the wasm-backed buffer uses the same conditional `Uint16Array` contract. Ops with an empty table
allocate no lane. Static compiler output writes only the nonzero ID (both fluent and direct-inlined log paths); dynamic
logs leave the zero sentinel and write `message_values` exactly as before. Overflow buffers inherit the same Op metadata
and lane shape.

`resolveMessage(buffer, row)` is the cold/public boundary: `0` returns the raw row value, while nonzero `n` returns
`buffer._opMetadata.logTemplateIds[n - 1]` and throws on an invalid nonzero ID. Arrow conversion, SQLite insertion,
testing/fact extraction, Cloudflare rows, feature-flag evaluation, and stdio tracing all use this resolver. Therefore
public Arrow `message` values and other exported messages remain the exact cooked literal or dynamic runtime string; the
private ID lane is not an Arrow schema change. Span names and all non-eligible messages remain ordinary strings.

The TypeScript and tsgo implementations share these checker-proof, lexical-order, deduplication, saturation, structured
metadata, and bailout rules. Their parity tests establish emitted behavior and runtime/Arrow invariants; they are not
performance evidence. The hot-store change has no claimed speedup until a dedicated benchmark reports results.

#### 6.1 Shipped versus clean-cutover status

The preceding Op-local `u16` path is the **shipped compatibility state**. It recognizes only direct calls inside
eligible inline `defineOp`/`defineOps` callbacks, assigns IDs by lexical encounter, and reconstructs strings at the cold
boundary. It is not a global vocabulary, a stable trace identity, or an Arrow dictionary optimization.

The **clean-cutover target** below replaces that representation rather than adding a second permanent ID namespace. At
cutover, every compiler and runtime caller uses the versioned global registration ABI, the old Op-local table/lane is
removed, and no alias or dual-write shim remains. Until that cutover ships, target examples in §6.2 are normative design
requirements but not claims about current emitted JavaScript.

#### 6.2 Target: checker-semantic structured vocabulary ABI

##### Recognition and level policy

Recognition is semantic, not tied to the spelling `ctx.log`. The checker must resolve the invoked member and prove both
that its receiver is LMAO `SpanLogger`/`GeneratedSpanLogger` and that the method declaration has LMAO provenance. This
covers typed aliases, parameters, imported helper-owned logger values, and property paths in application and library
source. It excludes same-named methods on unrelated types, `any`/`unknown`, unproved unions, shadowed implementations,
and computed dynamic member names. Recognition applies to all typed LMAO logger callsites in the compilation, not only
calls lexically inside an inline Op.

The source contract classifies callsites independently of the physical buffer family:

- `static`: literal text with no fields;
- `structured`: literal text plus a second **object-literal** fields argument; and
- `dynamic`: a raw runtime string reference, permitted only for `debug`/`trace` and never registered.

Operational `info`/`warn`/`error` text must be a string literal or no-substitution template literal. Interpolation,
concatenation, dynamic text, spread properties, computed keys, getters, methods, and non-object field bags are compile
errors. A clean-cutover structured template uses `{fieldName}`; `{{` and `}}` escape literal braces. This **replaces**
the shipped `{{fieldName}}` placeholder spelling. Placeholder names and object-literal keys form the same duplicate-free
set, and each key resolves to a schema column represented canonically as `{name,column}`. Literal `debug`/`trace` calls
follow the same `static`/`structured` rules; a referenced runtime string stays `dynamic`, while avoidable
interpolation/concatenation is rejected.

The physical term `mixed` is reserved for a buffer family carrying both numeric vocabulary and dynamic raw-string lanes;
it is not a callsite class, record mode, or `kindTags` value. `kindTags` describes entry semantics only.

Checker-proven literal trace/span names are also registered records of kind `SPAN_NAME`. This applies wherever the
transformer already proves the LMAO span/trace API and literal name without changing evaluation or Promise semantics. A
dynamic span name remains on the ordinary dynamic string lane. Shipped code stores every span name as a string; span
name registration is target-only.

```typescript
// Source: the checker proves `log` and `ctx` are LMAO values.
function record(log: SpanLogger, ctx: OpContext, user: User, elapsedMs: number) {
  log.info('loaded {userId} in {elapsedMs}ms', {
    userId: user.id,
    elapsedMs,
  });
  log.info('literal braces: {{ok}}');
  log.debug(debugMessage); // allowed dynamic debug lane
  return ctx.span('load-user', loadUserOp, user.id); // registered SPAN_NAME
}

// Rejected:
log.warn(`loaded ${user.id}`); // LMAO_DYNAMIC_OPERATIONAL_TEXT
log.info('loaded {userId}', fields); // LMAO_FIELDS_NOT_OBJECT_LITERAL
log.trace(`state=${state}`); // LMAO_AVOIDABLE_INTERPOLATION
```

The compiler lowers the object literal; it is not materialized at runtime. The registration snippet below is emitted
JavaScript, not typed TypeScript. Its side-effect import guarantees that ESM evaluates the LMAO installer before this
module calls the Symbol callback:

```javascript
import '@smoothbricks/lmao/vocabulary/register/v1';

const REGISTER_VOCABULARY = Symbol.for('@smoothbricks/lmao/vocabulary/register/v1');
const $$vocabulary = globalThis[REGISTER_VOCABULARY]({
  schemaVersion: 1,
  idAlgorithm: 'sha256-24-v1',
  contentHash: $$contentHash,
  ids: new Uint32Array($$ids),
  kindTags: new Uint8Array([1, 1, 2]),
  utf8: new Uint8Array($$canonicalUtf8),
  offsets: new Int32Array($$recordOffsets),
});

// Warmed callsites: direct dense-index loads, fixed arity, no object or lookup dispatch.
const $$field0 = user.id;
const $$field1 = elapsedMs;
log._infoStructured2($$vocabulary[0], 42, $$field0, $$field1);
log._infoStatic0($$vocabulary[1], 43);
log.debug(debugMessage);
return ctx.spanVocabulary1(44, $$vocabulary[2], loadUserOp, user.id);
```

The target span-name seam shown above remains Promise-based exactly like `span1`; its numeric name argument is the dense
`SPAN_NAME` index. A dynamic name continues through public `span(name, ...)`. Private seam spelling may change, but
literal and dynamic names must decode identically and retain the public Promise/microtask schedule.

The private write seams may instead be compiler-emitted direct stores, but they preserve observable order and allocation
behavior: fixed monomorphic stores for registered warmed writes and no object, array, rest-argument, or interpolated-
string allocation. Dense index `0` is valid; nullable rows use a validity bitmap, never an index sentinel.

##### Vocabulary fragment and library-without-source contract

The runtime type contract is separate from the emitted JavaScript above:

```typescript
type VocabularyFragmentV1 = {
  readonly schemaVersion: 1;
  readonly idAlgorithm: 'sha256-24-v1';
  readonly contentHash: string;
  readonly ids: Uint32Array;
  readonly kindTags: Uint8Array;
  readonly utf8: Uint8Array;
  readonly offsets: Int32Array;
};
type VocabularyBinding = Uint32Array;
type RegisterVocabularyV1 = (fragment: VocabularyFragmentV1) => VocabularyBinding;
```

For `N = ids.length`, `kindTags.length` and the returned `VocabularyBinding.length` are exactly `N`, while
`offsets.length` is exactly `N + 1`. `offsets[0]` is zero, `offsets[N]` equals `utf8.length`, and offsets are monotonic
and in range. Record `i` is `utf8.subarray(offsets[i], offsets[i + 1])` with exact bytes:

```text
u32le textLen | text UTF8 | u16le fieldCount |
repeated(u16le nameLen | name UTF8 | u16le columnLen | column UTF8)
```

`kindTags[i]` is `1` (`LOG_TEMPLATE`) or `2` (`SPAN_NAME`); these are semantic entry kinds, not
`static`/`structured`/`dynamic` or physical `mixed`. `ids[i]` is the first 24 bits, interpreted big-endian, of
`SHA-256(kindTag || recordBytes)`. `contentHash` is lowercase SHA-256 hex of this canonical byte stream:

```text
u8 schemaVersion |
u16le algorithmLen | idAlgorithm UTF8 |
u32le ids.length | repeated(u32le id) |
u32le kindTags.length | kindTags bytes |
u32le utf8.length | utf8 bytes |
u32le offsets.length | repeated(i32le offset)
```

Thus numeric array elements have explicit little-endian serialization independent of host typed-array byte order. The
callback validates all lengths, integer ranges, tags, UTF-8, record grammar, IDs, and hash before publishing a binding.

Every emitted JavaScript library file or independently loaded chunk imports the LMAO registration installer as a
side-effect dependency, obtains the shared Symbol with `Symbol.for('@smoothbricks/lmao/vocabulary/register/v1')`, and
self-registers its typed-array fragment during module evaluation. ESM dependency order installs the callback first. The
chunk closes over the returned binding; consumers need neither its TypeScript source/checker graph nor a second
transform. Bundling must retain installer/registration side effects and pair each callsite with its binding; it may
merge byte-identical fragments or remove entries whose callsites are tree-shaken.

The callback validates and copies/merges inputs into a runtime-owned immutable dictionary generation. New fragments
append unseen values in registration order; dense indices are deliberately not sorted or canonical. Existing bindings
remain prefix-valid forever, and each immutable generation is exactly its predecessor plus appended values. Later
mutation of compiler-emitted arrays or later registration cannot mutate a live generation.

A strict optimized build diagnoses an unavailable/incompatible ABI, and direct loading of incompatible emitted code
fails at startup rather than guessing an index. There is no warmed registration check or method dispatch: a callsite
reads `$$vocabulary[fragmentLocalOrdinal]` directly. Every dense index, including zero, is valid; nullability uses the
row validity bitmap.

Registration contributes the vocabulary portion of the startup-versioned `PhysicalLayoutPlan`, whose offsets, widths,
alignment, and chunk policy are immutable once live. `VocabularyBinding` maps fragment-local ordinals to dense indices.
An `ArrowLease` pins the exact backing store, WASM epoch, chunks, and dictionary generation used by a batch until
explicit release after flush consumption.

Registration is idempotent for byte-identical fragments and deduplicates equal `(kindTag,id,recordBytes)` records.
Reusing an ID for different kind/record bytes, or `contentHash` for another canonical fragment, is fatal before the
fragment is writable. Stable IDs and content bytes—not dense indices—provide deterministic identity and semantic
checksums across registration orders. Cross-process dense-index stability is not promised; decoded values and schema
semantics remain deterministic.

##### Diagnostics, fallback, and evaluation order

Policy violations are diagnostics, not silent raw-message bailouts:

| Diagnostic                        | Condition                                                                    |
| --------------------------------- | ---------------------------------------------------------------------------- |
| `LMAO_DYNAMIC_OPERATIONAL_TEXT`   | dynamic/interpolated/concatenated `info`, `warn`, or `error` text            |
| `LMAO_AVOIDABLE_INTERPOLATION`    | interpolated/concatenated `debug` or `trace` text                            |
| `LMAO_FIELDS_NOT_OBJECT_LITERAL`  | fields are not a plain literal or use spread/computed/accessor/method syntax |
| `LMAO_PLACEHOLDER_MISMATCH`       | missing, extra, duplicate, malformed, or unresolved placeholder/field        |
| `LMAO_LOGGER_PROOF_REQUIRED`      | strict optimized build cannot prove an apparent LMAO logger call             |
| `LMAO_VOCABULARY_ID_COLLISION`    | one stable `u24` identity has different canonical bytes                      |
| `LMAO_VOCABULARY_ABI_UNAVAILABLE` | emitted chunk cannot use callback ABI version 1                              |

An unrelated or genuinely unproved logger call is left byte-shape unchanged; the transformer must not guess. A typed
LMAO call violating the level/template contract must not silently fall back, because that would make vocabulary coverage
depend on optimizer success. A supported structured call exceeding a private fixed-arity seam may use the correct
overflow/slow path, but remains registered and diagnostic-free.

The transform preserves JavaScript evaluation exactly. It evaluates the receiver first, then arguments left-to-right;
each field initializer is evaluated once in source property order. Generated temporaries precede direct stores only as
needed to retain that order. Throws, getters reached by field value expressions, and side effects occur at the same
point relative to the call as in source. Registration occurs at module evaluation and may not move callsite field
expressions to startup. These rules do not relax §2: public `span()` lowering remains Promise-based, and no vocabulary
optimization may select `spanAutoN` or change Promise-assimilation/microtask turns.

##### Authoritative benchmark gates

Promotion from the shipped Op-local representation to this clean cutover requires repo-standard Mitata scenarios to
compare `plugin-off`, `plugin-on/current`, and target variants with a position-balanced schedule in independent
order-reversed processes. Every run emits machine-readable raw samples, semantic checksums, p50/p95/p99/p99.9, and
allocation/GC metadata when the host exposes it. A checksum mismatch, collision, decode difference, or evaluation-order
difference is an unconditional fail.

Semantic acceptance covers `static` log templates, `structured` fields and brace escaping, dynamic debug/trace text,
literal `SPAN_NAME` registration, and dynamic span-name fallback. It verifies binding length/append-only prefix
stability, dense index zero with validity-based nulls, exact record bytes/hashes, decoded Arrow values, and unchanged
span Promise/microtask observations under at least two reversed registration orders.

Results are reported separately for `startup`, `span setup`, `warmed entry write`, `overflow/slow path`, and
`per-request flush`; one phase may not amortize or hide another. The warmed registered entry path must demonstrate zero
allocations, fixed monomorphic stores, and at least a 10% p50 improvement over `plugin-on/current` without a greater
than 3% regression at any reported tail percentile. Overflow must remain bounded and correct without a greater than 3%
p99.9 regression. Per-request flush must show few or no allocations and reuse the startup `PhysicalLayoutPlan`, dense
Arrow vocabulary, and `ArrowLease`; it must not regress a reported percentile by more than 3%. Startup and span setup
are explicit costs and must not regress by more than 3% unless the commit message names and justifies the trade. Failing
a gate leaves the target unpromoted and forbids a zero-overhead claim.

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

| Transformation                     | Without Transformer                   | With Transformer                                                                                                         |
| ---------------------------------- | ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| Stable proven Op span              | Public variadic Promise dispatcher    | Promise-based fixed `span0`–`span8` ABI                                                                                  |
| Dynamic/unstable Op span           | Public variadic Promise dispatcher    | Unchanged public dispatcher; single evaluation preserved                                                                 |
| Inline-function span               | Public variadic Promise dispatcher    | Promise-based fixed `span0`–`span8` ABI with direct `Object.create` child                                                |
| Explicit internal `spanAutoN`      | Not applicable                        | Sync `Ok`/`Err` terminal or Promise retry/thenable fallback; never automatic                                             |
| `with()` bulk setter               | Object allocation + iteration         | Direct columnar buffer writes                                                                                            |
| Destructured `span`                | Closure-bound public dispatcher       | `__ctx` plus safe Promise-based fixed-arity lowering, or whole-function bailout                                          |
| Op setup                           | Full context setup, adaptive capacity | Structured compile metadata with packed hint and Op-local templates when analysis is valid                               |
| Literal Op log message             | String stored per row                 | Shipped: private Op-local `u16`; target: stable message `u24` in a stable fragment, bound to a process-dense Arrow index |
| Structured operational log         | Runtime object/string work or raw API | Target: checker-proved literal template and fixed-arity field stores; not shipped                                        |
| Dynamic debug/trace message        | Raw string row                        | Preserved dynamic lane; avoidable interpolation is a target diagnostic                                                   |
| Dynamic/ineligible operational log | Raw string row                        | Shipped: raw fallback; target proved-LMAO policy violations are diagnostics                                              |
| Line numbers                       | Runtime fallback/absent               | Compile-time injection on lowered calls                                                                                  |
| Metadata                           | Manual or missing                     | Auto-injected                                                                                                            |

These are structural properties of emitted code. Performance claims require a benchmark for the target runtime and
workload.

## Implementation Notes <a id="smoo/lmao!n/transformer-impl-notes"></a>

### Detecting Op vs Function <a id="smoo/lmao!n/transformer-detect-op"></a>

The transformer uses a `TypeChecker` to prove the LMAO context receiver and `Op<...>` second argument. A proven Op is
lowered to Promise-based `spanN` only when the Op is a stable identifier, so extracting its fields cannot repeat a
dynamic expression. Arrow and function expressions can use the same legacy `spanN` path when their receiver is stable.
Everything else remains on `span()`; `spanAutoN` is never selected automatically.

### Synthetic Variable Naming <a id="smoo/lmao!n/transformer-synthetic-naming"></a>

Destructured-context rewriting emits `__ctx` only after proving that name does not occur in the function body. The
transform is skipped on collision. The tag-chain inliner uses `$$vN` temporaries (`generateVarName`) for non-literal tag
values.

### Source Maps <a id="smoo/lmao!n/transformer-source-maps"></a>

The native plugin constructs replacement AST nodes through tsgo's shim factory inside ttsc's emit transform. That keeps
all rewriting in the compiler's normal emit pipeline instead of text-splicing JavaScript. Synthesized direct-write
blocks do not inherit unrelated source comments.

> **Implementation status.** A dedicated source-map preservation/verification pass is not separately implemented or
> tested; correctness currently relies on ttsc's factory-based emit. Tracked by node
> `smoo/lmao!n/transformer-source-maps`.

## Fallback Behavior <a id="smoo/lmao!n/transformer-fallback"></a>

Without proof—or without the transformer—everything stays correct through the public API:

- `span()` always returns `Promise<Result<...>>`. It parses the optional line/override forms, resolves Op versus
  function, creates `_newCtx0()`/`_newCtx1()`, and dispatches to Promise-based `span0`–`span8`.
- Stable proven Ops and inline functions may lower directly to the same Promise-based `spanN` methods. Dynamic Op
  expressions remain on `span()` to preserve single evaluation and Promise scheduling.
- `spanAutoN` is an internal/explicit `Result | Promise<Result>` runtime seam and is not an automatic public-call
  replacement, even in direct await or async-return positions.
- `with()` and tag setters iterate through generated fluent builders rather than direct inlined writes.
- Runtime line/metadata fallbacks continue to apply when compile-time injection is absent.
- Literal log-template encoding is optional in the shipped Op-local implementation: ineligible/dynamic calls write the
  raw message, every public/Arrow read resolves to the same string, and malformed nonzero IDs fail loudly.
- In the clean-cutover target, proved structured calls use the registered writer. Unsupported unrelated calls retain the
  public raw API, while typed LMAO policy violations are compile diagnostics rather than runtime fallbacks. A
  precompiled library/chunk without source is correct through its own version-1 fragment registration.

## Related Specs <a id="smoo/lmao!n/transformer-related"></a>

- [Entry Types and Logging Primitives](./01h_entry_types_and_logging_primitives.md) - `with()` bulk setter
- [Op Context Pattern](./01l_op_context_pattern.md) - Metadata injection, `span()` overloads
- [Context Flow](./01c_context_flow_and_op_wrappers.md) - Line number flow to TypedArrays
- [Trace Logging System](./01_trace_logging_system.md) - the transformer-overview sections (line-number injection,
  monomorphic call sites, context destructuring)
