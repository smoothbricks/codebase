# AGENTS.md - AI Coding Assistant Guidelines for LMAO

## 📚 BEFORE WRITING CODE, READ THESE SPECS:

### Package Architecture (Read FIRST!)

- **Package Architecture**: specs/00_package_architecture.md - Defines arrow-builder vs lmao responsibilities,
  dependency direction, what each package OWNS and MUST NOT know about

### Core System

- **System Overview**: specs/01_trace_logging_system.md - Architecture overview & hot/cold path design
- **Schema System**: specs/01a_trace_schema_system.md - S.enum/S.category/S.text, tag attributes, feature flags
  [**LMAO**]
- **Context Flow**: specs/01c_context_flow_and_task_wrappers.md - Request→Module→Task→Span hierarchy [**LMAO**]
- **Buffer Architecture**: specs/01b_columnar_buffer_architecture.md - TypedArray columnar storage (NOT Arrow builders!)
  [**ARROW-BUILDER**]

### Buffer System Details (All in @packages/arrow-builder)

- **Buffer Overview**: specs/01b_columnar_buffer_architecture_overview.md - High-level columnar storage concepts
- **Performance Opts**: specs/01b1_buffer_performance_optimizations.md - Cache alignment, string interning, enum
  optimization
- **Self-Tuning**: specs/01b2_buffer_self_tuning.md - Zero-config capacity management
- **Arrow Table**: specs/01f_arrow_table_structure.md - Final queryable format & zero-copy conversion

### API & Code Generation (All in @packages/lmao)

- **Entry Types**: specs/01h_entry_types_and_logging_primitives.md - Unified entry type enum, fluent API
- **Context API Codegen**: specs/01g_trace_context_api_codegen.md - Runtime code generation for tag methods
- **Module Context**: specs/01j_module_context_and_spanlogger_generation.md - SpanLogger class generation
- **Span Scope**: specs/01i_span_scope_attributes.md - Scoped attributes for zero-overhead propagation

### Integration & Output

- **Library Pattern**: specs/01e_library_integration_pattern.md - Third-party library integration with prefixing
  [**LMAO**]
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
- Tag attribute definitions with masking and `attr_` prefix
- **System columns (timestamps, operations) - ALWAYS eager, never lazy**
- **Scope class generation - SEPARATE from buffer columns**
- SpanBuffer creation (extends ColumnBuffer with span metadata)
- SpanLogger/ctx API generation (specs/01g, 01j)
- Fluent logging (ctx.tag, ctx.log, ctx.ok, ctx.err) (specs/01h)
- Context propagation (request→module→task→span) (specs/01c)
- Feature flag evaluation (specs/01a)
- Library integration & prefixing (specs/01e)

**Key Architectural Decisions**:

- System columns NEVER lazy (written every entry, zero conditionals)
- User attribute columns lazy by default (sparse data)
- Scope is a SEPARATE generated class (NOT stored in lazy columns)
- Direct properties on SpanBuffer (attr*$name_nulls + attr*$name_values)

**Key Files**:

- `src/lib/schema/` - Schema builders, tag attributes, feature flags
- `src/lib/codegen/spanLoggerGenerator.ts` - SpanLogger class generation
- `src/lib/codegen/scopeGenerator.ts` - Scope class generation (SEPARATE from buffer)
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
  - **DO NOT create raw objects** - use `defineTagAttributes()` and `S` schema builder
  - **Example**: Before creating a schema object like `{ __lmao_type: 'number' }`, search for `defineTagAttributes` and
    use it properly

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

- **Build**: x build lmao | **Test Single**: bun test path/to/file.test.ts | **Test Pattern**: bun test -t "pattern"
- **Typecheck**: x typecheck lmao | **Format**: bun run format (auto-formats on git commit)

## Implementation Patterns (See specs/01h_entry_types_and_logging_primitives.md)

- **Schema Definition**: ALWAYS use `defineTagAttributes()` with `S` builder:

  ```typescript
  // ✅ CORRECT
  const schema = defineTagAttributes({
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

## Library Integration (See specs/01e_library_integration_pattern.md)

- Libraries define clean schemas without prefixes
- Prefixing happens at composition time
- Example: Library writes ctx.tag.status(200) → becomes http_status column
- Avoids naming conflicts across libraries

## Span Scope Attributes (See specs/01i_span_scope_attributes.md)

- Set scoped attributes: ctx.log.scope({ requestId, userId })
- Automatically propagates to all child entries and spans
- Zero runtime cost after initial scope setting
- Eliminates repetitive attribute setting

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
- ✅ `defineTagAttributes()` - Schema definition with validation
- ✅ `defineFeatureFlags()` - Feature flag schema with sync/async evaluation
- ✅ Schema extension with `.extend()` method
- ✅ Masking transforms (hash, url, sql, email)

### Buffer System (@packages/arrow-builder/src/lib/buffer/)

- ✅ `createSpanBuffer()` - Cache-aligned TypedArray buffer creation
- ✅ `createChildSpanBuffer()` - Child span buffer with tree structure
- ✅ `createNextBuffer()` - Buffer chaining for overflow
- ✅ `createAttributeColumns()` - Schema-based column creation
- ✅ Null bitmap management (Arrow format)
- ✅ Self-tuning capacity with overflow tracking
- ✅ `convertToArrowTable()` - Zero-copy Arrow conversion
- ✅ `convertSpanTreeToArrowTable()` - Recursive tree conversion

### Code Generation (@packages/lmao/src/lib/codegen/)

- ✅ `generateSpanLoggerClass()` - Runtime class code generation
- ✅ `createSpanLoggerClass()` - Compile and cache SpanLogger classes
- ✅ Compile-time enum mapping in generated code
- ✅ Prototype methods for zero-overhead tag writing
- ✅ Scoped attributes with `scope()` method
- ✅ Distinct entry types (info/debug/warn/error)

### Context & Integration (@packages/lmao/src/lib/)

- ✅ `createRequestContext()` - Request-level context with ff/env
- ✅ `createModuleContext()` - Module-level context with task wrapper
- ✅ `ctx.ok()` / `ctx.err()` - Fluent result API
- ✅ `ctx.span()` - Child span creation
- ✅ `ctx.tag` - Chainable tag API for span attributes
- ✅ `ctx.log.scope()` - Scoped attribute propagation
- ✅ Feature flag evaluation with analytics tracking

### Library Integration (@packages/lmao/src/lib/library.ts)

- ✅ `prefixSchema()` - Add prefix to all schema fields
- ✅ `createLibraryModule()` - Library module factory
- ✅ `moduleContextFactory()` - Compose libraries with prefixes
- ✅ `createHttpLibrary()` - Example HTTP library
- ✅ `createDatabaseLibrary()` - Example database library

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
- ✅ `moduleIdInterner` - Module ID interning
- ✅ `spanNameInterner` - Span name interning

**BEFORE IMPLEMENTING**: Search these modules first! Most functionality already exists.
