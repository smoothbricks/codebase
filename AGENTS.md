# AGENTS.md - AI Coding Assistant Guidelines for LMAO

## 📚 BEFORE WRITING CODE, READ THESE SPECS:
- **System Overview**: `specs/01_trace_logging_system.md` - Core architecture & hot/cold path design
- **Schema System**: `specs/01a_trace_schema_system.md` - Tag attributes, feature flags, env config patterns  
- **Context Flow**: `specs/01c_context_flow_and_task_wrappers.md` - Request→Module→Task→Span hierarchy
- **Buffer Architecture**: `specs/01b_columnar_buffer_architecture.md` - TypedArray columnar storage (NOT Arrow builders!)
- **Arrow Table Output**: `specs/01f_arrow_table_structure.md` - Final Arrow format for querying

## 🏗️ CRITICAL ARCHITECTURE SEPARATION:
- **@packages/arrow-builder**: Low-level TypedArray buffers in Arrow-compatible layout (cache-aligned, columnar)
- **@packages/lmao**: High-level logging API (ctx.log.tag, method chaining, etc.) - uses arrow-builder, NOT Arrow directly!
- **DO NOT**: Use Apache Arrow builders in hot path - only TypedArray assignments per specs/01b_columnar_buffer_architecture.md

## Build/Test Commands (Use Bun, Never npm/npx)
- **Build**: `nx build lmao` | **Test Single**: `bun test path/to/file.test.ts` | **Test Pattern**: `bun test -t "pattern"`
- **Typecheck**: `nx typecheck lmao` | **Format**: `bun run format` (auto-formats on git commit)

## Implementation Patterns (See specs/01h_entry_types_and_logging_primitives.md)
- **Buffer Creation**: arrow-builder provides TypedArray buffers, lmao wraps with logging API
- **Hot Path Writes**: Direct TypedArray assignment: `buffer.userId[writeIndex] = stringTable.intern(id)`
- **Method Chaining**: Return `this` from tag methods for fluent API: `.userId(id).requestId(req)`
- **Per-Span Buffers**: Each span owns its columnar TypedArrays (Uint8Array, Float64Array, etc.) 

## Critical Performance Rules (See specs/01b1_buffer_performance_optimizations.md)
1. **Hot Path**: TypedArray writes ONLY. No Arrow builders, no objects, no console.log
2. **Cache Alignment**: 64-byte aligned TypedArrays (see specs/01b_columnar_buffer_architecture.md sections on alignment)
3. **Direct References**: SpanLogger holds buffer ref, no lookups. See specs/01j_module_context_and_spanlogger_generation.md
4. **Background Conversion**: Arrow Table creation happens in cold path ONLY (specs/01f_arrow_table_structure.md)