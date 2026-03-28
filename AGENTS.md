# AGENTS.md - AI Coding Assistant Guidelines for LMAO

`CLAUDE.md` is the short repo-memory sheet. This file is the detailed manual. Keep the core rules in both files aligned.

## Read First

- `CLAUDE.md` — short high-retention rule sheet
- `.claude/commands/test-results.md` — inspect `.trace-results.db` failures before re-running large suites
- `specs/lmao/00_package_architecture.md` — read first when changing `packages/lmao` or `packages/arrow-builder`

## Core Rules

- **Use Bun only.** Never use `npm`, `npx`, or `bunx` here. Use `bun`, `nx`, `biome`, etc. directly from PATH.
- **Fix everything you see.** Never wave off a failure as "pre-existing". If you touch casts, hacks, dead code, or
  broken tests, clean them up.
- **Search before implementing.** Reuse existing code and patterns before adding new helpers, APIs, or schema objects.
- **Avoid stupid allocations, especially on hot or frequently hit paths.** Do not excuse needless allocations just
  because a path is "cold" — startup, boundary, serverless, and browser initialization paths still matter. If a
  success/failure wrapper can be an immutable singleton or eliminated entirely, do that instead of allocating fresh
  `{ ok: true }` / tiny throwaway objects. Keep small result objects only when they are truly the right contract and not
  an avoidable churn point.
- **This is greenfield.** No backwards-compat layers, deprecated APIs, or dead code retention.

## Specs Rules

- **Specs are commitments, not suggestions.** Critique them when needed, but do not skip implementation by deferring
  work or adding JS workarounds.

## Errors, Tracing, And Testing

- **Known operational failures return `Err`/`Result`.** `throw` is only for invariants, impossible states, or programmer
  bugs.
- **No default `NoOpTracer`.** `createNoOpTracer()` is banned/removed. `NoOpTracer` may still exist in
  `@smoothbricks/lmao` for API proof, comparison, and overhead benchmarking, but it is not the normal repo pattern.
  Require tracing context from callers, use child spans, and use observable suite/test tracers in tests.
- **Direct `bun test` now loads the shared tracing preload from either the repo root or a package directory.** The root
  `bunfig.toml` and the package-level `bunfig.toml` files preload `test-trace-preload.ts`, so traced single-file runs no
  longer fail just because they start at the monorepo root. Use whichever invocation is clearest:
  `bun test packages/foo/src/...` from the repo root or `bun test src/...` inside `packages/foo`.
  `nx test <project>` remains valid because Nx runs from the package directory.
- **Package-local extra preloads still require the package `bunfig.toml`.** Example: `packages/lmao-expo/bunfig.toml`
  also preloads `src/test-preload.ts`, so run Expo proofs from `packages/lmao-expo` when that extra setup matters.
- **Lint before tests.** Use `nx lint <project>` before `bun test` or `nx test <project>`.
- **Nx cache is not flaky; config is.** If a task only passes with `--skip-nx-cache`, fix the target's inputs, outputs,
  dependency graph, or task wiring so cached and uncached runs agree.
- **Do not normalize cache bypasses.** `--skip-nx-cache` is for diagnosis only, not routine development or validation.

## Type Inference Rules

- **Inference quality is API quality.** Prefer types inferred from library definitions.
- **Zero casts in code and tests.** No `as any`, `as unknown as`, or type-erasing helpers. If a cast seems necessary,
  fix the source typing first.
- **Do not manually restate large inferred context/state shapes.** Only add a local type when it truly narrows the
  surface.

## 💡 DEVELOPMENT TOOLS NOTE

**For efficient code search and editing in this project, use Serena's tools when available.** Serena provides semantic
code understanding, symbol-based editing, and precise file operations that are optimized for TypeScript and complex
codebases.

### ts-morph MCP Tools (for cross-file refactoring)

When using `mcp-tsmorph_rename_symbol_by_tsmorph` or other ts-morph tools:

 **Use `tsconfig.lib.json`** NOT `tsconfig.json` - The root tsconfig uses project references with empty `include`, so
  ts-morph won't find any files. Always use the lib-specific tsconfig that has `include: ["src/**/*.ts"]`.
 **Example**:
  ```
  tsconfigPath: "/path/to/packages/lmao/tsconfig.lib.json"  ✅
  tsconfigPath: "/path/to/packages/lmao/tsconfig.json"      ❌ (files not found)
  ```

### Comby for Structural Search/Replace

**Docs**: https://comby.dev/docs/syntax-reference

Use comby for bulk structural code edits. Key syntax:

| Pattern        | Description                                                            |
| -------------- | ---------------------------------------------------------------------- |
| `:[var]`       | Match zero or more chars (lazy), stops at newline outside delimiters   |
| `:[[var]]`     | Match one or more alphanumeric + `_` (like `\w+`)                      |
| `:[var:e]`     | Match expression-like (handles balanced parens, e.g., `foo.bar(x, y)`) |
| `:[var.]`      | Match alphanumeric + punctuation (`.`, `;`, `-`)                       |
| `:[var\n]`     | Match up to and including newline                                      |
| `:[ var]`      | Match only whitespace (no newlines)                                    |
| `:[var~regex]` | Match arbitrary PCRE regex                                             |

**⚠️ CRITICAL: ALWAYS dry-run first with `-diff` before using `-in-place`!**

```bash
# Step 1: ALWAYS preview changes first (dry-run)
comby 'old_pattern' 'new_pattern' -matcher .ts -d src -diff

# Step 2: Only after verifying the diff, apply changes
comby 'old_pattern' 'new_pattern' -matcher .ts -d src -in-place
```

**Examples**:

```bash
# Rename destructured variable (preview first!)
comby 'const { :[a], createTrace } = :[rest]' 'const { :[a], logBinding } = :[rest]' -matcher .ts -d src -diff
# Then with -in-place after review

# Match function calls with args
comby 'createTrace(:[args])' 'tracer.trace(:[args])' -matcher .ts -d src -diff

# Match multiline with expression hole
comby 'new Tracer(:[factory:e], { sink: :[sink:e] })' 'new Tracer({ logBinding: :[factory:e].logBinding, sink: :[sink:e] })' -matcher .ts -d src -diff
```

**Tips**:

 **ALWAYS use `-diff` first** - never go straight to `-in-place`
 Use `-matcher .ts` for TypeScript (better than `-extensions ts`)
 Use `:[var:e]` for expressions that may contain parens/brackets
 Comby handles balanced delimiters automatically - `{:[x]}` matches balanced braces
 Patterns can match more broadly than expected - review the diff carefully!

### git-reword-commit (for rewriting commit messages)

Rewrite a commit message without affecting worktree or staged changes. Uses git-filter-repo API directly.

```bash
# Replace entire message
echo "feat: new message" | ./tooling/git-reword-commit abc123

# Prepend to existing message
cat msg.txt | ./tooling/git-reword-commit abc123 --prepend

# Append to existing message
echo "Co-authored-by: Someone" | ./tooling/git-reword-commit abc123 --append
```

**Note**: Message is read from stdin to preserve newlines and avoid shell escaping issues.

### ts-morph MCP Tools (for cross-file refactoring)

When using `mcp-tsmorph_rename_symbol_by_tsmorph` or other ts-morph tools:

 **Use `tsconfig.lib.json`** NOT `tsconfig.json` - The root tsconfig uses project references with empty `include`, so
  ts-morph won't find any files. Always use the lib-specific tsconfig that has `include: ["src/**/*.ts"]`.
 **Example**:
  ```
  tsconfigPath: "/path/to/packages/lmao/tsconfig.lib.json"  ✅
  tsconfigPath: "/path/to/packages/lmao/tsconfig.json"      ❌ (files not found)
  ```

### Nx targetDefaults and New Package Checklist

**`targetDefaults` in `nx.json` do NOT auto-create targets.** They only provide default configuration (executor,
options, dependsOn) for targets that already exist from another source (plugin inference, package.json script, or
explicit `"nx".targets` config). If a package doesn't declare the target, the targetDefault has no effect.

**When creating a new package**, add these stub entries to `package.json` `"nx".targets`:

```json
{
  "nx": {
    "targets": {
      "lint": {},
      "typecheck-tests": {
        "executor": "nx:run-commands",
        "options": {
          "command": "tsc --noEmit -p tsconfig.test.json",
          "cwd": "packages/<name>"
        },
        "dependsOn": ["build"]
      }
    }
  }
}
```

 `"lint": {}` — creates the target; `nx.json` targetDefault fills in biome executor + `dependsOn: ["typecheck-tests"]`
 `"typecheck-tests"` — explicit target because it has package-specific `cwd`; runs `tsc --noEmit` on test tsconfig

Also create `tsconfig.test.json` for the package (see existing packages for the pattern: `types: ["bun"]`,
`composite: false`, `noEmit: true`, includes test globs, references `tsconfig.lib.json`).

**After any tsconfig or dependency changes**, run `nx sync` to update project references. Verify with `nx sync:check`.

### Nx Cache Reliability Policy

If agents keep reaching for `--skip-nx-cache`, assume the workspace configuration is wrong until proven otherwise.

- Verify target `inputs`, `outputs`, `dependsOn`, and project graph edges rather than bypassing cache.
- Fix nondeterministic generators, missing output declarations, stale build artifacts, or implicit dependency gaps.
- When a cached run diverges from a fresh run, capture the exact target and repair the Nx config so both paths behave
  the same.
- Do not leave docs, scripts, or plans teaching `--skip-nx-cache` as a normal fix.

### Platform-Agnostic tsconfig Policy

Platform-agnostic packages use `"types": []` in `tsconfig.lib.json` to prevent accidentally depending on platform
globals (`Buffer`, `process`, `Bun`). Files that intentionally use platform APIs opt in with per-file triple-slash
directives:

```typescript
/// <reference types="node" />  // for node:* imports
/// <reference types="bun" />   // for Bun.* APIs (use "bun-types")
```

## ⚠️ GREENFIELD PROJECT - CRITICAL ANALYSIS APPROACH

**THIS IS A GREENFIELD PROJECT.** There is NO legacy code. There are NO existing users.

 **DO NOT** add backwards compatibility layers or deprecated API support
 **DO NOT** maintain old function signatures "just in case"
 **DO NOT** keep dead code around
 **CRITICALLY ANALYZE** specs vs implementation - specs evolve and may be outdated or incorrect
 If implementation diverges from spec → **EVALUATE WHICH IS CORRECT** and update accordingly
 **GENERATE TASKS FOR BOTH** implementation fixes AND spec updates when inconsistencies are found
 **QUESTION ASSUMPTIONS** - implementation may have discovered better approaches than spec requirements
 When in doubt, ask - don't blindly follow specs if implementation suggests better patterns

## ERROR HANDLING POLICY (RESULT VS THROW)

This repo uses a strict policy:

 **Known operational failures MUST return `Err`/`Result`** (validation failures, business-rule failures, transient
  errors, blocked state, missing optional dependencies, missing runtime data, retryable failures).
 **`throw` is ONLY for invariants / impossible states / programmer bugs** (type exhaustiveness failures, impossible
  internal state, broken build/runtime contracts).
 **No SQS exception to this rule**: known retry cases MUST be represented as structured failures and mapped to platform
  responses (e.g. Lambda SQS `batchItemFailures`), not thrown.

### Trace semantics

 `span-err`: expected operational error represented via `ctx.err(...)` / `Err`.
 `span-exception`: unexpected thrown exception (bug/invariant break).

### NoOpTracer policy

**`NoOpTracer` is not an approved default wiring pattern in this
codebase.**

 **Production code**: Requires real tracing. If code needs a spanContext, require it from the caller
 **Tests**: MUST use `TestTracer` or suite tracer that flushes to SQLite (per CLAUDE.md test tracing policy)
- **Benchmarking / comparison**: `NoOpTracer` may still exist in `@smoothbricks/lmao` for API proof or overhead
  measurement, but ordinary app/test wiring in this repo should not use it as the default pattern

**Red flags that indicate broken design:**

```typescript
// ❌ WRONG - Creating throwaway tracer anywhere
const tracer = new NoOpTracer();  // NEVER DO THIS

// ❌ WRONG - Optional spanContext
interface Config {
  spanContext?: TraceContext;  // Should be REQUIRED
}

// ✅ CORRECT - Require context, use child spans
async function dispatchQuery(ctx: TraceContext, ...): Promise<Result> {
  return ctx.span('query', async (childCtx) => {
    // childCtx is properly nested in trace tree
  });
}

// ✅ CORRECT - Tests use TestTracer or suite tracer
const tracer = new TestTracer(opContext);  // For test inspection
// OR use the package's test suite tracer that flushes to SQLite
```

Runtime code that lacks tracing context indicates a broken call graph. Every entry point (Lambda handler, HTTP handler,
DO fetch) starts a root trace; all subsequent operations use child spans.

### Agent implementation requirements

 If code currently throws for a known operational failure, migrate it to a typed `Err` with explicit error code.
 If a throw is retained, add a short comment explaining invariant intent, e.g.
  `// invariant throw: programmer/config bug`.
 Prefer type guards and exhaustive checks so impossible states are prevented at compile-time where feasible.

--

## 🔍 CRITICAL ANALYSIS APPROACH - SPECS EVOLVE BUT MUST BE IMPLEMENTED

**SPECS EVOLVE AND MAY BE WRONG** - The agent must actively critique and improve specifications. But "critique" means
"build the better version," not "skip building." Every spec'd capability must be implemented or explicitly replaced by a
superior alternative that is also implemented.

### Spec Consistency Review Agent Requirements

The agent should:

1. **REVIEW for INCONSISTENCIES** between spec and implementation
2. **CRITIQUE the specs themselves** - specs will evolve
3. **Evaluate if implementation approaches are better than spec requirements**
4. **Generate tasks for BOTH** implementation fixes AND spec updates when inconsistencies are found
5. **Question assumptions**: Are spec requirements still valid? Do they make sense?
6. **Check for outdated patterns**: Specs written early may not reflect current best practices
7. **Validate constraints**: Are spec limitations still necessary or were they premature optimizations?
8. **Consider real-world usage**: Does the spec match actual developer needs and workflows?

### When Implementation Diverges from Spec

 **Don't blindly fix implementation** - specs may be wrong or outdated
 **Evaluate trade-offs**: Performance, ergonomics, correctness, maintainability
 **Implementation may be right**: Greenfield projects often discover better patterns during coding
 **Update specs proactively**: If implementation proves superior, update the spec to match
 **Document rationale**: When diverging from spec, explain WHY in commit messages and spec updates

### Spec Quality Assessment

 **Question assumptions**: Are spec requirements still valid? Do they make sense?
 **Check for outdated patterns**: Specs written early may not reflect current best practices
 **Validate constraints**: Are spec limitations still necessary or were they premature optimizations?
 **Consider real-world usage**: Does the spec match actual developer needs and workflows?

### Task Generation Strategy

 **Implementation fixes**: When spec is clearly correct and implementation is wrong
 **Spec updates**: When implementation demonstrates better approach or spec is outdated
 **Hybrid tasks**: When both spec and implementation need refinement
 **Documentation updates**: Always update this file when discovering new patterns or insights

--

## ⚠️ REPO COLLABORATION - MULTIPLE AGENTS WORKING SIMULTANEOUSLY

**MULTIPLE AI AGENTS WORK IN THIS REPO** - coordinate carefully to avoid conflicts:

 **NEVER use `git checkout`** - This can reset other agents' work in progress
 **NEVER use `git reset`** - Same issue, destroys other agents' changes
 **Check git status first** - See what others are working on before making changes
 **Communicate changes** - If you modify shared interfaces/types, notify other agents
 **Avoid conflicting edits** - Work on different files when possible, coordinate on shared files

**If you accidentally use git checkout/reset:**

 IMMEDIATELY notify other agents what you changed
 They may need to reapply their work
 Use this as a lesson to check git status first next time

--

--

## 📚 BEFORE WRITING CODE, READ THESE SPECS:

### Package Architecture (Read FIRST!)

 **Package Architecture**: specs/lmao/00_package_architecture.md - Defines arrow-builder vs lmao responsibilities,
  dependency direction, what each package OWNS and MUST NOT know about

### Core System

 **System Overview**: specs/lmao/01_trace_logging_system.md - Architecture overview, hot/cold path design, **V8
  Optimization Patterns** (see also [V8 Optimization References](#v8-optimization-references) below)
 **Schema System**: specs/lmao/01a_trace_schema_system.md - S.enum/S.category/S.text, logSchema [**LMAO**]
 **Feature Flags**: specs/lmao/01p_feature_flags.md - Flag schema, evaluator, analytics [**LMAO**]
 **Context Flow**: specs/lmao/01c_context_flow_and_op_wrappers.md - TraceContext→Op→Span hierarchy, op() pattern
  [**LMAO**]
 **Buffer Architecture**: specs/lmao/01b_columnar_buffer_architecture.md - TypedArray columnar storage (NOT Arrow
  builders!) [**ARROW-BUILDER**]
 **TypeScript Transformer**: specs/lmao/01o_typescript_transformer.md - Compile-time V8 optimizations, span_op/span_fn
  monomorphic methods [**LMAO-TRANSFORMER**]

### Buffer System Details (All in @packages/arrow-builder)

 **Performance Opts**: specs/lmao/01b1_buffer_performance_optimizations.md - Cache alignment, string interning, enum
  optimization
 **Self-Tuning**: specs/lmao/01b2_buffer_self_tuning.md - Zero-config capacity management
 **High-Precision Timestamps**: specs/lmao/01b3_high_precision_timestamps.md - Nanosecond timestamps, BigInt64Array
 **Span Identity**: specs/lmao/01b4_span_identity.md - Span ID design, TraceId, distributed tracing
 **SpanBuffer Memory Layout**: specs/lmao/01b5_spanbuffer_memory_layout.md - Memory diagrams, column organization,
  SpanBuffer interface
 **Buffer Codegen Extension**: specs/lmao/01b6_buffer_codegen_extension.md - ColumnBufferExtension, lazy getters,
  schema-generated buffers
 **Arrow Table**: specs/lmao/01f_arrow_table_structure.md - Final queryable format & zero-copy conversion

### API & Code Generation (All in @packages/lmao)

 **Entry Types**: specs/lmao/01h_entry_types_and_logging_primitives.md - Unified entry type enum, fluent API
 **Context API Codegen**: specs/lmao/01g_trace_context_api_codegen.md - Runtime code generation for tag methods
 **Module Context**: specs/lmao/01j_module_context_and_spanlogger_generation.md - Op/SpanLogger class generation
 **Span Scope**: specs/lmao/01i_span_scope_attributes.md - Scoped attributes for zero-overhead propagation

### Integration & Output

 **Op Context Pattern**: specs/lmao/01l_op_context_pattern.md - `defineOpContext()`, `defineOp()`, and Tracer
  [**LMAO**]
 **Library Integration**: specs/lmao/01e_library_integration_pattern.md - RemappedBufferView for prefixing [**LMAO**]
 **AI Agent Integration**: specs/lmao/01d_ai_agent_integration.md - MCP server for AI trace querying [**LMAO**]

## 🏗️ PACKAGE ARCHITECTURE - TWO SIBLING PACKAGES:

> **See specs/lmao/00_package_architecture.md for complete details including WHY each decision was made.**

### @packages/arrow-builder - Low-Level Alternative to Apache Arrow

**Purpose**: Explicit, visible allocations for Arrow table construction (NOT Apache Arrow's hidden resizing!)

**Owns**:

 Cache-aligned TypedArray buffer creation (64-byte alignment)
 Lazy column storage pattern (nulls + values share ONE ArrayBuffer per column)
 Runtime class generation via `new Function()` for V8 optimization
 Schema extensibility via composition (NOT inheritance)
 Zero-copy Arrow conversion

**CRITICAL - Does NOT know about**:

 ❌ Logging/tracing concepts (spans, traces, contexts)
 ❌ Entry types (info, warn, error, span-start)
 ❌ Scope or scoped attributes
 ❌ System vs user column distinction
 ❌ Any `@smoothbricks/lmao` dependency

**Key Files**:

 `src/lib/buffer/types.ts` - ColumnBuffer interface
 `src/lib/buffer/columnBufferGenerator.ts` - new Function() codegen for lazy columns
 `src/lib/buffer/createColumnBuffer.ts` - Buffer factory
 `src/lib/schema-types.ts` - Generic schema types

### @packages/lmao - High-Level Logging/Runtime

**Purpose**: Developer ergonomics with zero-allocation hot path

**Owns**:

 Schema DSL (S.enum/category/text/number/boolean) (specs/lmao/01a)
 logSchema definitions with masking
 **System columns (timestamps, operations) - ALWAYS eager, never lazy**
 **Scope storage - plain object on buffer, NO codegen needed**
 SpanBuffer creation (extends ColumnBuffer with span metadata)
 SpanLogger/ctx API generation (specs/lmao/01g, 01j)
 Fluent logging (ctx.tag, ctx.log, ctx.ok, ctx.err) (specs/lmao/01h)
 Context propagation (traceContext→module→op→span) (specs/lmao/01c)
 Feature flag evaluation (specs/lmao/01a)
 Library integration & prefixing (specs/lmao/01e)

**Key Architectural Decisions**:

 System columns NEVER lazy (written every entry, zero conditionals)
 User attribute columns lazy by default (sparse data)
 Scope is a plain object (`buffer.scopeValues`) - filled at Arrow conversion via SIMD
 Direct properties on SpanBuffer (`$name_nulls` + `$name_values` for each schema field)

**Key Files**:

 `src/lib/schema/` - Schema builders, logSchema, feature flags
 `src/lib/codegen/spanLoggerGenerator.ts` - SpanLogger class generation (tag methods)
 `src/lib/spanBuffer.ts` - SpanBuffer factory (extends ColumnBuffer)
 `src/lib/lmao.ts` - Main integration, context creation
 `src/lib/types.ts` - SpanBuffer interfaces

**Relationship**: lmao depends on arrow-builder. arrow-builder MUST NOT depend on lmao.

## 🚫 CRITICAL RULES:

 **Hot Path**: TypedArray assignments ONLY in arrow-builder. No Arrow builders, no objects!
 **Package Imports**: lmao can import from `@smoothbricks/arrow-builder`. arrow-builder MUST NOT import from lmao!
 **DO NOT**: Use Apache Arrow builders in hot path - only TypedArray assignments per
  specs/lmao/01b_columnar_buffer_architecture.md
 **⚠️ SEARCH BEFORE IMPLEMENTING**: Before writing ANY new code, ALWAYS search for existing implementations in BOTH
  packages:
  - Use `grep` or `glob` to find similar functions/types/patterns
  - Check `packages/lmao/src/lib/` for high-level APIs
  - Check `packages/arrow-builder/src/lib/` for low-level buffer operations
  - Look for existing helper functions, types, and patterns
  - **DO NOT re-implement what already exists** - reuse existing code
  - **DO NOT create raw objects** - use `defineLogSchema()` and `S` schema builder
  - **Example**: Before creating a schema object like `{ __lmao_type: 'number' }`, search for `defineLogSchema` and use
    it properly

## ✅ TYPE-DRIVEN TEST ERGONOMICS WORKFLOW (MANDATORY)

Goal: **zero casts in tests** (`as any`, broad assertion casts, and non-null assertion workarounds) by making library
APIs naturally inferable for users.

When a test needs casts, follow this exact sequence:

1. **Remove casts in one test file first** and run typecheck/lint to expose real type errors.
2. **Interpret each error as API feedback** (missing discriminated union, weak generic, unknown boundary, etc.).
3. **Fix source/runtime/public typings first** (non-test code) so usage is inferred without helper hacks.
4. **Only after repeated patterns emerge**, add minimal shared helpers/guards in source for boundary decoding.
5. Re-run package lint/typecheck/tests; then remove now-unneeded test scaffolding.

Rules:

 Do not introduce test-only generic wrappers that hide poor API inference.
 Prefer runtime validators/type guards at boundary seams (`postMessage`, network payloads, storage reads) over casts.
 Treat integration tests as user experience tests: if test code must cast to call public APIs, API typing is not ready.
 Make **atomic commits per fix cluster** (e.g., boundary parser + its call sites, decide context typing upgrade, etc.).

### No Type Erasure Policy (MANDATORY)

These rules are grounded in recent repository direction: typed API inference, generic threading across runtime
chains, and sustained cast-removal refactors across the codebase.

Policy:

 Keep generic type parameters threaded end-to-end (input -> storage -> accessor return type).
 Do not erase specific generic types into broad singleton storage when those values are later returned to callers.
 If a helper accepts a generic context/tracer, exposed getters/handles must preserve that exact generic.
 Do not patch inference gaps with `as unknown as ...`, `as any`, or broad cast bridges in library code.
 If a cast seems required, treat it as a typing bug in source API design and fix types first.
 Back-compat `unknown` defaults may exist in core public generics, but helper/harness APIs must still preserve concrete
  inferred types at the call site.

Scenario paths write facts to VM state via the RETE bridge (`vm-rete-bridge.ts` runs JS `:then` callbacks, writes
results to VM state via `vm_rete_insert_fact`). The VM facts reader (`vm-facts-reader.ts`) reads directly from WASM
memory with zero-copy Uint32Array views.

- **Domain/agent tests:** Use Scenario with `defineRuleset` — rules fire automatically after reduce, facts are stored in
  VM state, `scenario.facts` returns a snapshot-backed reader.
  pattern as reducer parity: run both paths, compare with fast-check. See `rete-parity.test.ts`.

## 🎯 STRING TYPE SYSTEM (CRITICAL - See specs/lmao/01a_trace_schema_system.md):

Three distinct string types, each with different storage strategies:

### S.enum - Known Values (Uint8Array)

 **When**: All possible values known at compile time
 **Storage**: Uint8Array (1 byte) with compile-time mapping
 **Example**: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE']) → switch case mapping
 **Arrow**: Dictionary with pre-defined values
 **Use for**: Operations, HTTP methods, entry types, status enums

### S.category - Repeated Values (Dictionary Encoded)

 **When**: Values often repeat (limited cardinality)
 **Storage**: Raw strings in hot path, dictionary built in cold path
 **Example**: buffer.userId_values[idx] = userId (raw string stored, dict built at Arrow conversion)
 **Arrow**: Dictionary built dynamically from unique values
 **Use for**: userIds, sessionIds, moduleNames, spanNames, table names

### S.text - Unique Values (No Dictionary)

 **When**: Values rarely repeat
 **Storage**: Raw strings without interning
 **Example**: Error messages, stack traces, SQL queries after masking
 **Arrow**: Plain string column (no dictionary overhead)
 **Use for**: Unique error messages, URLs, request bodies, masked queries **IMPORTANT**: Never use generic "S.string" -
  always choose enum/category/text explicitly!

## Build/Test Commands (Use Bun, Never npm/npx)

### Build & Typecheck

 **Build**: `nx build <project>`
 **Typecheck**: `nx typecheck <project>` (typechecks source code)

### Linting (ALWAYS run before tests!)

 **Lint**: `nx lint <project>`
 **Lint Fix**: `nx lint:fix <project>`
 **⚠️ CRITICAL**: Agents MUST run `nx lint <project>` before running tests to catch type errors early

### Testing

 **Test Single**: `bun test path/to/file.test.ts`
 **Test Pattern**: `bun test -t "pattern"`
 **Test All**: `nx test <project>` (runs all tests for a package)
 **Note**: Tests no longer depend on typecheck-tests - linting handles that. Tests only depend on build.

**Repository requirement:** Every package test suite (except `packages/lmao`) MUST be LMAO-traced and MUST flush
traces through a SQLite sink (local `.trace-results.db` or worker D1 binding such as `TRACE_RESULTS`).
 Configure SQLite once in the package-local tracer module (`src/test-suite-tracer.ts` or vitest equivalent), not in
  individual tests.
 Preload/setup files must stay wiring-only (one call to setup helper + runner mock bridge when needed).
 If a runner-specific package needs extra wiring (e.g. Worker/Vitest bridge), put that logic in the runner harness
  module, not in setup files.

### Property-Based Testing with fast-check

**Prefer property-based tests by default** for buffer/overflow semantics, fork graphs,
and data integrity scenarios. The `fast-check` library is installed.

```typescript
import fc from 'fast-check';

// Example: Verify buffer overflow preserves all entries
fc.assert(
  fc.property(
    fc.integer({ min: 1, max: 200 }), // Generate test inputs
    (numEntries) => {
      // ... write numEntries to buffer ...
      const entries = collectEntries(buffer);
      expect(entries.length).toBe(numEntries); // Property must hold for ALL inputs
    }
  ),
  { numRuns: 100 }
);
```

**When to use property-based tests:**

 Buffer overflow and chaining (entry preservation, buffer count formulas)
 Data integrity across serialization/deserialization
 Mathematical invariants (e.g., `sb_overflows === bufferCount - 1`)
 Any scenario where "it works for N" should imply "it works for all N"
 Deep fork of-fork interleavings with append/switch/reset operations
 Rollback/rollforward reversibility across long randomized operation sequences

 **Policy:**

 Start with a small example test for readability, then add a property test that stress-tests the same invariant.
 For fork/undo logic, prefer randomized traces over manually enumerated branch cases.

### Boundary Validation And Contract Preservation (MANDATORY)

- At transport/storage/IPC boundaries, prefer Typia or `@smoothbricks/validation` over ad-hoc local validators,
  normalizers, or one-off guard helpers.
- If a boundary already has a meaningful public type (for example `Record<string, unknown>`), use that type instead of manually
  restating `Record<string, unknown>` unions across multiple files.
- Treat new helpers named like `normalizeX`, `coerceX`, `toRecord`, `isStringKeyRecord`, or similar as suspicious by
  default. Before adding one, search for an existing shared validator/helper and justify why the new helper is
  necessary.
- If you replace a hand-written boundary parser with Typia, preserve the surrounding WHY comment or add a better one
  that explains the invariant being protected.
- When a typed boundary change alters an error message, update the test expectation deliberately; do not preserve stale
  message assertions just because they existed first.

**Key properties to test:**

 **Preservation**: All N inputs produce exactly N outputs
 **Formulas**: Buffer count matches `1 + ceil((N - reservedRows) / capacity)`
 **Bounds**: Values stay within expected ranges
 **Consistency**: Related counters/metrics stay in sync

## SPANBUFFER PROPERTY NAMING: EXACT ARROW COLUMN MATCH

**CRITICAL**: SpanBuffer public properties MUST match Arrow table column names exactly (no camelCase conversions). This
ensures obvious data flow and enables users to define custom columns with consistent naming.

### Core Arrow Columns → SpanBuffer Properties (Exact Match)

 `trace_id` → `trace_id` getter
 `thread_id` → `thread_id` getter
 `span_id` → `span_id` getter
 `parent_thread_id` → `parent_thread_id` getter
 `parent_span_id` → `parent_span_id` getter
 `timestamp` → `timestamp` (direct TypedArray)
 `entry_type` → `entry_type` (direct TypedArray)
 `package_name` → `package_name` (dictionary encoded)
 `package_file` → `package_file` (dictionary encoded)
 `git_sha` → `git_sha` (dictionary encoded)

### System Schema Fields (snake_case)

 `message` (eager category)
 `line` (lazy number)
 `error_code` (lazy category)
 `exception_stack` (lazy text)
 `ff_value` (lazy category)
 `uint64_value` (lazy bigUint64)

### SpanContext Changes

 No `traceId` (access via `ctx.buffer.trace_id`)
 New `module` getter for direct access to module metadata
 `callee_package`, `callee_file`, `callee_line`, `callee_git_sha` getters (pull from `buffer.module` and
  `buffer.line(0)`)
 `SpanLogger` methods use `snake_case` (e.g., `error_code()`, `ff_value()`)

### Internal Properties (underscore prefix)

 `_system`, `_identity`, `_writeIndex`, `_capacity`, `_next`, `_hasParent`, `_children`, `_parent`, `_module`,
  `_spanName`, `_callsiteModule`, `_scopeValues`

### Implementation Details

 **Arrow columns**: Use exact underscore names (correspond 1:1 with Arrow table)
 **System fields**: `snake_case` (same as Arrow columns)
 **Internal**: `_` prefix for encapsulation (implementation details)

### Example

```typescript
// ✅ CORRECT - Arrow column names match exactly
buffer.trace_id; // Arrow column: trace_id
buffer.parent_span_id; // Arrow column: parent_span_id
buffer.timestamp; // Arrow column: timestamp

// ✅ CORRECT - System fields (snake_case)
buffer.message; // System field: message
buffer.line; // System field: line

// ✅ CORRECT - Internal (underscore)
buffer._children; // Internal tree structure
buffer._module; // Internal context
```

## Implementation Patterns (See specs/lmao/01h_entry_types_and_logging_primitives.md)

 **Schema Definition**: ALWAYS use `defineLogSchema()` with `S` builder:

  ```typescript
  // ✅ CORRECT
  const schema = defineLogSchema({
    userId: S.category(),
    operation: S.enum(['CREATE', 'READ']),
    errorMsg: S.text(),
    count: S.number(),
  });

  // ❌ WRONG - Never create raw objects
  const schema = { userId: { __lmao_type: 'category' } };
  ```

 **Buffer Creation**: arrow-builder provides TypedArray buffers, lmao wraps with logging API
 **Hot Path Writes**: Direct TypedArray assignment only
  - Enums: buffer.operation_values[idx] = OPERATION_MAP[value] (compile-time lookup)
  - Categories: buffer.userId_values[idx] = rawString (raw string, dict built in cold path)
  - Text: buffer.errorMsg_values[idx] = rawString (no interning)
 **Method Chaining**: Return this from tag methods for fluent API: .userId(id).requestId(req)
 **Per-Span Buffers**: Each span owns its columnar TypedArrays (Uint8Array, Float64Array, etc.)

## Entry Type System (See specs/lmao/01h_entry_types_and_logging_primitives.md)

Unified enum for ALL trace events:

 **Span lifecycle**: span-start, span-ok, span-err, span-exception
 **Logging**: info, debug, warn, error
 **Structured data**: tag
 **Feature flags**: ff-access, ff-usage Entry types use compile-time enum mapping to Uint8Array for 1-byte storage.

## Critical Performance Rules (See specs/lmao/01b1_buffer_performance_optimizations.md)

1. **Hot Path**: TypedArray writes ONLY. No Arrow builders, no objects, no console.log
2. **Cache Alignment**: 64-byte aligned TypedArrays (specs/lmao/01b_columnar_buffer_architecture.md)
3. **String Optimization**:
   - Enums: Compile-time switch-case mapping to Uint8 (1 byte)
   - Categories: Raw strings on hot path, dictionary built in cold path
   - Text: Raw strings without dictionary overhead
4. **Direct References**: SpanLogger holds buffer ref, no lookups
   (specs/lmao/01j_module_context_and_spanlogger_generation.md)
5. **Background Conversion**: Arrow Table creation in cold path ONLY (specs/lmao/01f_arrow_table_structure.md)

## Code Generation (See specs/lmao/01g_trace_context_api_codegen.md & 01j)

 **SpanLogger generation**: Runtime class generation with typed methods per schema
 **Attribute methods**: Each schema field gets a typed method on SpanLogger
 **Dual API**: Object-based (ctx.tag({ userId: "123" })) and property-based (ctx.tag.userId("123"))
 **Zero allocation**: Fluent methods return this, no intermediate objects

## Library Integration (See specs/lmao/01l_op_context_pattern.md & 01e)

 Libraries use `defineOpContext({ logSchema, deps, flags, ctx })` to define their op context
 Ops are defined via
  `const { defineOp, defineOps } = opContext; const myOp = defineOp('name', async (ctx, ...args) => {})`
 Ops receive full ctx and can destructure: `{ span, log, tag, deps, ff, env }` - take only what you need
 Span names at call site: `await ctx.span('contextual-name', someOp, args)` - caller names spans
 Deps can be destructured: `const { retry, auth } = ctx.deps`
 Prefix applied at use time: `httpOps.prefix('http')` for column prefixing
 RemappedBufferView maps prefixed names to unprefixed columns for Arrow conversion
 `ctx` properties require all keys enumerable for V8 hidden class optimization

## Tracer Usage Pattern

`Tracer` is an **abstract base class** with 5 lifecycle hooks. Use concrete implementations. `NoOpTracer` may exist for
API/benchmark comparison, but this repo's normal runtime/test wiring should use real tracing in production and
suite/test tracers in tests.

```typescript
import { TestTracer, StdioTracer, ArrayQueueTracer } from '@smoothbricks/lmao';

// defineOpContext returns OpContextFactory which extends OpContextBinding
const opContext = defineOpContext({
  logSchema: defineLogSchema({ userId: S.category() }),
  ctx: { env: null as Env }, // Required at trace time
});

// For tests that need buffer inspection:
const tracer = new TestTracer(opContext);
const { trace } = tracer;
await trace('fetch', fetchOp);
const table = convertSpanTreeToArrowTable(tracer.rootBuffers[0]);

// For tests that don't need buffer inspection, still use the suite tracer wiring
// that flushes to SQLite for trace-backed debugging.

// For development/debugging (colored console output):
const { trace } = new StdioTracer(opContext);

// For production batching:
const tracer = new ArrayQueueTracer(opContext);
// ... process requests ...
const batch = tracer.drain();
for (const buf of batch) {
  await sendToBackend(convertSpanTreeToArrowTable(buf));
}

// With feature flag evaluator:
const { trace } = new TestTracer(opContext, { flagEvaluator });

// trace() overloads - name is always required:
await trace('my-op', myOp); // invoke Op
await trace('handle', async (ctx) => ctx.ok('done')); // inline function

// With context overrides (flat object, not nested { ctx: {...} }):
await trace('my-op', { env: myEnv, requestId: 'req-123' }, myOp);
await trace('handle', { env: myEnv }, async (ctx) => ctx.ok('done'));

// With trace_id for distributed tracing:
await trace('my-op', { trace_id: incomingTraceId, env: myEnv }, myOp);
```

**Key Points:**

 **Tracer is abstract** - use `TestTracer`, `StdioTracer`, or `ArrayQueueTracer` here
 **Do not default to `NoOpTracer` in repo code** - if ordinary code seems to need it, the tracing API/call graph is
  wrong
 **Pass full opContext** - `new TestTracer(opContext)` not just logBinding
 **Always destructure** `{ trace, flush }` from concrete tracer instance
 `TestTracer.rootBuffers` - accumulated root buffers for inspection
 `TestTracer.statsSnapshots` - captured capacity tuning stats
 `ArrayQueueTracer.drain()` - consume and clear batched buffers
 `StdioTracer` - prints spans with color-coded trace IDs and indentation

**Lifecycle Hooks** (for custom Tracer implementations): | Hook | When Called | |------|-------------| |
`onTraceStart(rootBuffer)` | Before root fn execution | | `onTraceEnd(rootBuffer)` | After root fn completes (in
finally) | | `onSpanStart(childBuffer)` | Before child span fn execution | | `onSpanEnd(childBuffer)` | After child span
fn completes | | `onStatsWillResetFor(buffer)` | Before capacity tuning stats reset |

## Span Scope Attributes (See specs/lmao/01i_span_scope_attributes.md)

 Set scoped attributes: `ctx.setScope({ requestId, userId })` - merge semantics, `null` to clear
 Read scope: `ctx.scope.requestId` - readonly view
 Scope appears on ALL rows in Arrow output (default for all rows)
 **Direct writes win**: `tag.X()` wins on row 0, `ctx.ok().X()` wins on row 1, scope fills rows 2+
 **Immutable objects**: `setScope` creates NEW frozen object (never mutates)
 Child spans inherit parent scope by reference (safe because immutable - zero-cost!)
 **Snapshot semantics**: Child's scope is frozen at creation time (async safe, no race conditions)
 Columns filled via `TypedArray.fill()` at Arrow conversion (SIMD optimized)

## Self-Tuning Buffers (See specs/lmao/01b2_buffer_self_tuning.md)

 Per-module capacity learning
 Buffer chaining for overflow
 Zero configuration needed
 Adapts to workload patterns

## AI Agent Integration (See specs/lmao/01d_ai_agent_integration.md)

 MCP server for structured trace querying
 Tool-based interface for AI agents
 Context-efficient (detailed data only loaded on request)
 Works with Claude Desktop, Cursor, VS Code Copilot

## Arrow Table Output (See specs/lmao/01f_arrow_table_structure.md)

 Enum columns: Dictionary with compile-time values
 Category columns: Dictionary with runtime-built values
 Text columns: Plain strings without dictionary
 Zero-copy conversion from SpanBuffer to Arrow
 Optimized for ClickHouse/Parquet analytics

## ✅ IMPLEMENTED FEATURES (Search These First!)

### Core Schema System (@packages/lmao/src/lib/schema/)

 ✅ `S.enum()` - Compile-time known values with Uint8Array storage
 ✅ `S.category()` - Runtime string interning for repeated values
 ✅ `S.text()` - Raw strings for unique values
 ✅ `S.number()` - Float64Array storage
 ✅ `S.boolean()` - Uint8Array (0/1) storage
 ✅ `defineLogSchema()` - Schema definition with validation
 ✅ `defineFeatureFlags()` - Feature flag schema with sync/async evaluation
 ✅ Schema extension with `.extend()` method
 ✅ Masking transforms (hash, url, sql, email)

### Buffer System (@packages/arrow-builder/src/lib/buffer/)

 ✅ `createSpanBuffer()` - Cache-aligned TypedArray buffer creation
 ✅ `createChildSpanBuffer()` - Child span buffer with tree structure
 ✅ `createNextBuffer()` - Buffer chaining for overflow
 ✅ `createAttributeColumns()` - Schema-based column creation
 ✅ Null bitmap management (Arrow format)
 ✅ `convertToArrowTable()` - Zero-copy Arrow conversion
 ✅ `convertSpanTreeToArrowTable()` - Recursive tree conversion

### Code Generation (@packages/lmao/src/lib/codegen/)

 ✅ `generateSpanLoggerClass()` - Runtime class code generation
 ✅ `createSpanLoggerClass()` - Compile and cache SpanLogger classes
 ✅ Compile-time enum mapping via switch-case (V8 JIT-inlined)
 ✅ Prototype methods for zero-overhead tag writing
 ✅ Distinct entry types (info/debug/warn/error)

### Context & Integration (@packages/lmao/src/lib/)

 ✅ `createTraceContext()` - Root trace context with ff/env
 ✅ `createModuleContext()` - Module-level context with op wrapper
 ✅ `ctx.ok()` / `ctx.err()` - Fluent result API
 ✅ `ctx.span()` - Child span creation (polymorphic dispatcher)
 ✅ `ctx.span_op()` / `ctx.span_fn()` - Monomorphic span methods (for transformer)
 ✅ `ctx.tag` - Chainable tag API for span attributes
 ✅ `ctx.setScope()` - Set scope values (merge semantics, null to clear)
 ✅ `ctx.scope` - Read-only view of current scope
 ✅ Feature flag evaluation with analytics tracking
 ✅ `callsiteModule` on SpanBuffer for dual module attribution (row 0 vs rows 1+)

### Library Integration (@packages/lmao/src/lib/library.ts)

 ✅ `prefixSchema()` - Add prefix to all schema fields
 ✅ `generateRemappedBufferViewClass()` - Generate view for Arrow conversion
 ✅ `generateRemappedSpanLoggerClass()` - Generate SpanLogger with prefix mapping
 ✅ `defineModule().ctx<Extra>(defaults).make()` - Fluent module definition API

### Background Processing (@packages/lmao/src/lib/flushScheduler.ts)

 ✅ `FlushScheduler` - Adaptive background flushing
 ✅ Capacity-based flushing (80% threshold)
 ✅ Time-based flushing (10s max, 1s min intervals)
 ✅ Idle detection (5s timeout)
 ✅ Memory pressure detection (Node.js only)
 ✅ Manual flush with `flush()` method

### String Storage

 ✅ `Utf8Cache` (SIEVE-based) - Bounded UTF-8 encoding cache for Arrow conversion (cold path only)

**Note**: CATEGORY and TEXT columns store raw `string[]` on hot path. Dictionary building and UTF-8 encoding happen
during Arrow conversion (cold path). There is NO hot-path interning.

Module IDs and span names are accessed directly from `buf.module.package_name`, `buf.module.package_file`, and
`buf.spanName` during Arrow conversion.

**BEFORE IMPLEMENTING**: Search these modules first! Most functionality already exists.

## V8 Optimization References

When implementing performance-critical code, refer to these V8 optimization resources:

 **Primary Spec**: [V8 Optimization Patterns](specs/lmao/01_trace_logging_system.md#v8-optimization-patterns) -
  Complete guide to V8 optimization patterns used in LMAO
 **External References**:
  - [V8 Fast Properties Blog](https://v8.dev/blog/fast-properties) - Hidden class internals and property access
    optimization
  - [Web.dev V8 Performance Tips](https://web.dev/articles/speed-v8) - Best practices for V8 optimization
  - [V8 Hidden Classes and Inline Caching](https://richardartoul.github.io/jekyll/update/2015/04/26/hidden-classes.html) -
    Detailed explanation of hidden classes
 **Key Principle**: Objects with same properties in same order share hidden classes = optimized property access