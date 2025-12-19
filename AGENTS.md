# AGENTS.md - AI Coding Assistant Guidelines for LMAO

## ⚠️ GREENFIELD PROJECT - NO BACKWARDS COMPATIBILITY

**THIS IS A GREENFIELD PROJECT.** There is NO legacy code. There are NO existing users.

- **DO NOT** add backwards compatibility layers or deprecated API support
- **DO NOT** maintain old function signatures "just in case"
- **DO NOT** keep dead code around
- **ALWAYS** follow the specs exactly - specs are the source of truth (unless explicitly told otherwise)
- If implementation diverges from spec → **FIX THE IMPLEMENTATION**
- If tests don't match spec → **FIX THE TESTS**
- When in doubt, ask - don't add compatibility shims

---

## 📚 BEFORE WRITING CODE, READ THESE SPECS:

### Package Architecture (Read FIRST!)

- **Package Architecture**: specs/00_package_architecture.md - Defines arrow-builder vs lmao responsibilities,
  dependency direction, what each package OWNS and MUST NOT know about

### Core System

- **System Overview**: specs/01_trace_logging_system.md - Architecture overview, hot/cold path design, **V8 Optimization
  Patterns** (see also [V8 Optimization References](#v8-optimization-references) below)
- **Schema System**: specs/01a_trace_schema_system.md - S.enum/S.category/S.text, logSchema, feature flags [**LMAO**]
- **Context Flow**: specs/01c_context_flow_and_task_wrappers.md - TraceContext→Op→Span hierarchy, op() pattern
  [**LMAO**]
- **Buffer Architecture**: specs/01b_columnar_buffer_architecture.md - TypedArray columnar storage (NOT Arrow builders!)
  [**ARROW-BUILDER**]
- **TypeScript Transformer**: specs/01o_typescript_transformer.md - Compile-time V8 optimizations, span_op/span_fn
  monomorphic methods [**LMAO-TRANSFORMER**]

### Buffer System Details (All in @packages/arrow-builder)

- **Buffer Overview**: specs/01b_columnar_buffer_architecture_overview.md - High-level columnar storage concepts
- **Performance Opts**: specs/01b1_buffer_performance_optimizations.md - Cache alignment, string interning, enum
  optimization
- **Self-Tuning**: specs/01b2_buffer_self_tuning.md - Zero-config capacity management
- **Arrow Table**: specs/01f_arrow_table_structure.md - Final queryable format & zero-copy conversion

### API & Code Generation (All in @packages/lmao)

- **Entry Types**: specs/01h_entry_types_and_logging_primitives.md - Unified entry type enum, fluent API
- **Context API Codegen**: specs/01g_trace_context_api_codegen.md - Runtime code generation for tag methods
- **Module Context**: specs/01j_module_context_and_spanlogger_generation.md - Op/SpanLogger class generation
- **Span Scope**: specs/01i_span_scope_attributes.md - Scoped attributes for zero-overhead propagation

### Integration & Output

- **Module Builder Pattern**: specs/01l_module_builder_pattern.md - `defineModule()` + `op()` API [**LMAO**]
- **Library Integration**: specs/01e_library_integration_pattern.md - RemappedBufferView for prefixing [**LMAO**]
- **AI Agent Integration**: specs/01d_ai_agent_integration.md - MCP server for AI trace querying [**LMAO**]

## 🏗️ PACKAGE ARCHITECTURE - TWO SIBLING PACKAGES:

> **See specs/00_package_architecture.md for complete details including WHY each decision was made.**

### @packages/arrow-builder - Low-Level Alternative to apache-arrow

**Purpose**: Explicit, visible allocations for Arrow table construction (NOT Apache Arrow's hidden resizing!)

**Owns**:

- Cache-aligned TypedArray buffer creation (64-byte alignment)
- Lazy column storage pattern (nulls + values share ONE ArrayBuffer per column)
- Null bitmap management (Arrow format)
- Runtime class generation via `new Function()` for V8 optimization
- Schema extensibility via composition (NOT inheritance)
- Zero-copy Arrow conversion

**CRITICAL - Does NOT know about**:

- ❌ Logging/tracing concepts (spans, traces, contexts)
- ❌ Entry types (info, warn, error, span-start)
- ❌ The `attr_` prefix convention
- ❌ Scope or scoped attributes
- ❌ System vs user column distinction
- ❌ Any `@smoothbricks/lmao` dependency

**Key Files**:

- `src/lib/buffer/types.ts` - ColumnBuffer interface
- `src/lib/buffer/columnBufferGenerator.ts` - new Function() codegen for lazy columns
- `src/lib/buffer/createColumnBuffer.ts` - Buffer factory
- `src/lib/schema-types.ts` - Generic schema types

### @packages/lmao - High-Level Logging/Runtime

**Purpose**: Developer ergonomics with zero-allocation hot path

**Owns**:

- Schema DSL (S.enum/category/text/number/boolean) (specs/01a)
- logSchema definitions with masking and `attr_` prefix
- **System columns (timestamps, operations) - ALWAYS eager, never lazy**
- **Scope storage - plain object on buffer, NO codegen needed**
- SpanBuffer creation (extends ColumnBuffer with span metadata)
- SpanLogger/ctx API generation (specs/01g, 01j)
- Fluent logging (ctx.tag, ctx.log, ctx.ok, ctx.err) (specs/01h)
- Context propagation (traceContext→module→op→span) (specs/01c)
- Feature flag evaluation (specs/01a)
- Library integration & prefixing (specs/01e)

**Key Architectural Decisions**:

- System columns NEVER lazy (written every entry, zero conditionals)
- User attribute columns lazy by default (sparse data)
- Scope is a plain object (`buffer.scopeValues`) - filled at Arrow conversion via SIMD
- Direct properties on SpanBuffer (attr*$name_nulls + attr*$name_values)

**Key Files**:

- `src/lib/schema/` - Schema builders, logSchema, feature flags
- `src/lib/codegen/spanLoggerGenerator.ts` - SpanLogger class generation (tag/log methods)
- `src/lib/spanBuffer.ts` - SpanBuffer factory (extends ColumnBuffer)
- `src/lib/lmao.ts` - Main integration, context creation
- `src/lib/types.ts` - SpanBuffer, TaskContext interfaces

**Relationship**: lmao depends on arrow-builder. arrow-builder MUST NOT depend on lmao.

## 🚫 CRITICAL RULES:

- **Hot Path**: TypedArray assignments ONLY in arrow-builder. No Arrow builders, no objects!
- **Package Imports**: lmao can import from `@smoothbricks/arrow-builder`. arrow-builder MUST NOT import from lmao!
- **DO NOT**: Use Apache Arrow builders in hot path - only TypedArray assignments per
  specs/01b_columnar_buffer_architecture.md
- **⚠️ SEARCH BEFORE IMPLEMENTING**: Before writing ANY new code, ALWAYS search for existing implementations in BOTH
  packages:
  - Use `grep` or `glob` to find similar functions/types/patterns
  - Check `packages/lmao/src/lib/` for high-level APIs
  - Check `packages/arrow-builder/src/lib/` for low-level buffer operations
  - Look for existing helper functions, types, and patterns
  - **DO NOT re-implement what already exists** - reuse existing code
  - **DO NOT create raw objects** - use `defineLogSchema()` and `S` schema builder
  - **Example**: Before creating a schema object like `{ __lmao_type: 'number' }`, search for `defineLogSchema` and use
    it properly

## 🎯 STRING TYPE SYSTEM (CRITICAL - See specs/01a_trace_schema_system.md):

Three distinct string types, each with different storage strategies:

### S.enum - Known Values (Uint8Array)

- **When**: All possible values known at compile time
- **Storage**: Uint8Array (1 byte) with compile-time mapping
- **Example**: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE']) → switch case mapping
- **Arrow**: Dictionary with pre-defined values
- **Use for**: Operations, HTTP methods, entry types, status enums

### S.category - Repeated Values (Dictionary Encoded)

- **When**: Values often repeat (limited cardinality)
- **Storage**: Uint32Array indices with string interning
- **Example**: buffer.attr_userId[idx] = internString(userId)
- **Arrow**: Dictionary built dynamically from interned strings
- **Use for**: userIds, sessionIds, moduleNames, spanNames, table names

### S.text - Unique Values (No Dictionary)

- **When**: Values rarely repeat
- **Storage**: Raw strings without interning
- **Example**: Error messages, stack traces, SQL queries after masking
- **Arrow**: Plain string column (no dictionary overhead)
- **Use for**: Unique error messages, URLs, request bodies, masked queries **IMPORTANT**: Never use generic "S.string" -
  always choose enum/category/text explicitly!

## Build/Test Commands (Use Bun, Never npm/npx)

### Build & Typecheck

- **Build**: `nx build lmao` | **Typecheck**: `nx typecheck lmao` (typechecks source code)

### Linting (ALWAYS run before tests!)

- **Lint**: `nx lint lmao` - Runs biome check AND typechecks test files (typecheck-tests)
- **Lint Fix**: `nx lint:fix lmao` - Auto-formats code
- **⚠️ CRITICAL**: Agents MUST run `nx lint` before running tests to catch type errors early

### Testing

- **Test Single**: `bun test path/to/file.test.ts`
- **Test Pattern**: `bun test -t "pattern"`
- **Test All**: `nx test lmao` (runs all tests for a package)
- **Note**: Tests no longer depend on typecheck-tests - linting handles that. Tests only depend on build.

### Property-Based Testing with fast-check

**Prefer property-based tests** for buffer, overflow, and data integrity scenarios. The `fast-check` library is
installed.

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

- Buffer overflow and chaining (entry preservation, buffer count formulas)
- Data integrity across serialization/deserialization
- Mathematical invariants (e.g., `sb_overflows === bufferCount - 1`)
- Any scenario where "it works for N" should imply "it works for all N"

**Key properties to test:**

- **Preservation**: All N inputs produce exactly N outputs
- **Formulas**: Buffer count matches `1 + ceil((N - reservedRows) / capacity)`
- **Bounds**: Values stay within expected ranges
- **Consistency**: Related counters/metrics stay in sync

## Implementation Patterns (See specs/01h_entry_types_and_logging_primitives.md)

- **Schema Definition**: ALWAYS use `defineLogSchema()` with `S` builder:

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

- **Buffer Creation**: arrow-builder provides TypedArray buffers, lmao wraps with logging API
- **Hot Path Writes**: Direct TypedArray assignment only
  - Enums: buffer.attr_operation[idx] = OPERATION_MAP[value] (compile-time lookup)
  - Categories: buffer.attr_userId[idx] = internString(userId) (runtime interning)
  - Text: buffer.attr_errorMsg[idx] = rawString (no interning)
- **Method Chaining**: Return this from tag methods for fluent API: .userId(id).requestId(req)
- **Per-Span Buffers**: Each span owns its columnar TypedArrays (Uint8Array, Float64Array, etc.)

## Entry Type System (See specs/01h_entry_types_and_logging_primitives.md)

Unified enum for ALL trace events:

- **Span lifecycle**: span-start, span-ok, span-err, span-exception
- **Logging**: info, debug, warn, error
- **Structured data**: tag
- **Feature flags**: ff-access, ff-usage Entry types use compile-time enum mapping to Uint8Array for 1-byte storage.

## Critical Performance Rules (See specs/01b1_buffer_performance_optimizations.md)

1. **Hot Path**: TypedArray writes ONLY. No Arrow builders, no objects, no console.log
2. **Cache Alignment**: 64-byte aligned TypedArrays (specs/01b_columnar_buffer_architecture.md)
3. **String Optimization**:
   - Enums: Compile-time mapping to Uint8 (1 byte)
   - Categories: Runtime string interning to Uint32 (4 bytes)
   - Text: Raw strings without dictionary overhead
4. **Direct References**: SpanLogger holds buffer ref, no lookups
   (specs/01j_module_context_and_spanlogger_generation.md)
5. **Background Conversion**: Arrow Table creation in cold path ONLY (specs/01f_arrow_table_structure.md)

## Code Generation (See specs/01g_trace_context_api_codegen.md & 01j)

- **SpanLogger generation**: Runtime class generation with typed methods per schema
- **Attribute methods**: Each schema field gets a typed method on SpanLogger
- **Dual API**: Object-based (ctx.tag({ userId: "123" })) and property-based (ctx.tag.userId("123"))
- **Zero allocation**: Fluent methods return this, no intermediate objects

## Library Integration (See specs/01l_module_builder_pattern.md & 01e)

- Libraries use `defineModule({ metadata, logSchema, deps, ff }).ctx<Extra>(defaults).make()` to define their module
- Ops are defined via
  `const { op } = myModule; export const myOp = op('name', async ({ span, log, tag }, ...args) => {})`
- Ops destructure context: `{ span, log, tag, deps, ff, env }` - take only what you need
- Span names at call site: `await span('contextual-name', someOp, args)` - caller names spans
- Deps can be destructured: `const { retry, auth } = deps`
- Prefix applied at use time: `httpLib.prefix('http').use({ retry: retryLib.prefix('http_retry').use() })`
- RemappedBufferView maps prefixed names to unprefixed columns for Arrow conversion
- `.ctx<Extra>(defaults)` requires all keys enumerable for `new Function()` codegen (V8 hidden class optimization)

## Span Scope Attributes (See specs/01i_span_scope_attributes.md)

- Set scoped attributes: `ctx.setScope({ requestId, userId })` - merge semantics, `null` to clear
- Read scope: `ctx.scope.requestId` - readonly view
- Scope appears on ALL rows in Arrow output (default for all rows)
- **Direct writes win**: `tag.X()` wins on row 0, `ctx.ok().X()` wins on row 1, scope fills rows 2+
- **Immutable objects**: `setScope` creates NEW frozen object (never mutates)
- Child spans inherit parent scope by reference (safe because immutable - zero-cost!)
- **Snapshot semantics**: Child's scope is frozen at creation time (async safe, no race conditions)
- Columns filled via `TypedArray.fill()` at Arrow conversion (SIMD optimized)

## Self-Tuning Buffers (See specs/01b2_buffer_self_tuning.md)

- Per-module capacity learning
- Buffer chaining for overflow
- Zero configuration needed
- Adapts to workload patterns

## AI Agent Integration (See specs/01d_ai_agent_integration.md)

- MCP server for structured trace querying
- Tool-based interface for AI agents
- Context-efficient (detailed data only loaded on request)
- Works with Claude Desktop, Cursor, VS Code Copilot

## Arrow Table Output (See specs/01f_arrow_table_structure.md)

- Enum columns: Dictionary with compile-time values
- Category columns: Dictionary with runtime-built values
- Text columns: Plain strings without dictionary
- Zero-copy conversion from SpanBuffer to Arrow
- Optimized for ClickHouse/Parquet analytics

## ✅ IMPLEMENTED FEATURES (Search These First!)

### Core Schema System (@packages/lmao/src/lib/schema/)

- ✅ `S.enum()` - Compile-time known values with Uint8Array storage
- ✅ `S.category()` - Runtime string interning for repeated values
- ✅ `S.text()` - Raw strings for unique values
- ✅ `S.number()` - Float64Array storage
- ✅ `S.boolean()` - Uint8Array (0/1) storage
- ✅ `defineLogSchema()` - Schema definition with validation
- ✅ `defineFeatureFlags()` - Feature flag schema with sync/async evaluation
- ✅ Schema extension with `.extend()` method
- ✅ Masking transforms (hash, url, sql, email)

### Buffer System (@packages/arrow-builder/src/lib/buffer/)

- ✅ `createSpanBuffer()` - Cache-aligned TypedArray buffer creation
- ✅ `createChildSpanBuffer()` - Child span buffer with tree structure
- ✅ `createNextBuffer()` - Buffer chaining for overflow
- ✅ `createAttributeColumns()` - Schema-based column creation
- ✅ Null bitmap management (Arrow format)
- ✅ Self-tuning capacity with `sb_*` stats (sb_capacity, sb_totalWrites, sb_overflows, sb_totalCreated)
- ✅ `convertToArrowTable()` - Zero-copy Arrow conversion
- ✅ `convertSpanTreeToArrowTable()` - Recursive tree conversion

### Code Generation (@packages/lmao/src/lib/codegen/)

- ✅ `generateSpanLoggerClass()` - Runtime class code generation
- ✅ `createSpanLoggerClass()` - Compile and cache SpanLogger classes
- ✅ Compile-time enum mapping via switch-case (V8 JIT-inlined)
- ✅ Prototype methods for zero-overhead tag writing
- ✅ Distinct entry types (info/debug/warn/error)

### Context & Integration (@packages/lmao/src/lib/)

- ✅ `createTraceContext()` - Root trace context with ff/env
- ✅ `createModuleContext()` - Module-level context with op wrapper
- ✅ `ctx.ok()` / `ctx.err()` - Fluent result API
- ✅ `ctx.span()` - Child span creation (polymorphic dispatcher)
- ✅ `ctx.span_op()` / `ctx.span_fn()` - Monomorphic span methods (for transformer)
- ✅ `ctx.tag` - Chainable tag API for span attributes
- ✅ `ctx.setScope()` - Set scope values (merge semantics, null to clear)
- ✅ `ctx.scope` - Read-only view of current scope
- ✅ Feature flag evaluation with analytics tracking
- ✅ `callsiteModule` on SpanBuffer for dual module attribution (row 0 vs rows 1+)

### Library Integration (@packages/lmao/src/lib/library.ts)

- ✅ `prefixSchema()` - Add prefix to all schema fields
- ✅ `generateRemappedBufferViewClass()` - Generate view for Arrow conversion
- ✅ `generateRemappedSpanLoggerClass()` - Generate SpanLogger with prefix mapping
- ✅ `defineModule().ctx<Extra>(defaults).make()` - Fluent module definition API

### Background Processing (@packages/lmao/src/lib/flushScheduler.ts)

- ✅ `FlushScheduler` - Adaptive background flushing
- ✅ Capacity-based flushing (80% threshold)
- ✅ Time-based flushing (10s max, 1s min intervals)
- ✅ Idle detection (5s timeout)
- ✅ Memory pressure detection (Node.js only)
- ✅ Manual flush with `flush()` method

### String Storage

- ✅ `StringInterner` - Category string interning (exported from lmao.ts)
- ✅ `TextStringStorage` - Text storage without interning (exported from lmao.ts)
- ✅ `categoryInterner` - Global category interner
- ✅ `textStringStorage` - Global text storage
- ✅ `Utf8Cache` (SIEVE-based) - Bounded UTF-8 encoding cache for Arrow conversion

**Note**: Module IDs and span names are accessed directly from `buf.task.module.packageName`,
`buf.task.module.packagePath`, and `buf.task.spanName` during Arrow conversion - no separate interners needed.

**BEFORE IMPLEMENTING**: Search these modules first! Most functionality already exists.

## V8 Optimization References

When implementing performance-critical code, refer to these V8 optimization resources:

- **Primary Spec**: [V8 Optimization Patterns](specs/01_trace_logging_system.md#v8-optimization-patterns) - Complete
  guide to V8 optimization patterns used in LMAO
- **External References**:
  - [V8 Fast Properties Blog](https://v8.dev/blog/fast-properties) - Hidden class internals and property access
    optimization
  - [Web.dev V8 Performance Tips](https://web.dev/articles/speed-v8) - Best practices for V8 optimization
  - [V8 Hidden Classes and Inline Caching](https://richardartoul.github.io/jekyll/update/2015/04/26/hidden-classes.html) -
    Detailed explanation of hidden classes
- **Key Principle**: Objects with same properties in same order share hidden classes = optimized property access
