# AGENTS.md - AI Coding Assistant Guidelines for LMAO
## 📚 BEFORE WRITING CODE, READ THESE SPECS:
### Core System (Read First)
- **System Overview**: specs/01_trace_logging_system.md - Architecture overview & hot/cold path design
- **Schema System**: specs/01a_trace_schema_system.md - S.enum/S.category/S.text, tag attributes, feature flags
- **Context Flow**: specs/01c_context_flow_and_task_wrappers.md - Request→Module→Task→Span hierarchy
- **Buffer Architecture**: specs/01b_columnar_buffer_architecture.md - TypedArray columnar storage (NOT Arrow builders!)
### Buffer System Details
- **Buffer Overview**: specs/01b_columnar_buffer_architecture_overview.md - High-level columnar storage concepts
- **Performance Opts**: specs/01b1_buffer_performance_optimizations.md - Cache alignment, string interning, enum optimization
- **Self-Tuning**: specs/01b2_buffer_self_tuning.md - Zero-config capacity management
### API & Code Generation
- **Entry Types**: specs/01h_entry_types_and_logging_primitives.md - Unified entry type enum, fluent API
- **Context API Codegen**: specs/01g_trace_context_api_codegen.md - Runtime code generation for tag methods
- **Module Context**: specs/01j_module_context_and_spanlogger_generation.md - SpanLogger class generation
- **Span Scope**: specs/01i_span_scope_attributes.md - Scoped attributes for zero-overhead propagation
### Integration & Output
- **Arrow Table**: specs/01f_arrow_table_structure.md - Final queryable format (enum/category/text dictionaries)
- **Library Pattern**: specs/01e_library_integration_pattern.md - Third-party library integration with prefixing
- **AI Agent Integration**: specs/01d_ai_agent_integration.md - MCP server for AI trace querying
## 🏗️ CRITICAL ARCHITECTURE SEPARATION:
- **@packages/arrow-builder**: Low-level TypedArray buffers in Arrow-compatible layout (cache-aligned, columnar)
- **@packages/lmao**: High-level logging API (ctx.log.tag, method chaining, etc.) - uses arrow-builder, NOT Arrow directly!
- **DO NOT**: Use Apache Arrow builders in hot path - only TypedArray assignments per specs/01b_columnar_buffer_architecture.md
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
- **Use for**: Unique error messages, URLs, request bodies, masked queries
**IMPORTANT**: Never use generic "S.string" - always choose enum/category/text explicitly!
## Build/Test Commands (Use Bun, Never npm/npx)
- **Build**: 
x build lmao | **Test Single**: bun test path/to/file.test.ts | **Test Pattern**: bun test -t "pattern"
- **Typecheck**: 
x typecheck lmao | **Format**: bun run format (auto-formats on git commit)
## Implementation Patterns (See specs/01h_entry_types_and_logging_primitives.md)
- **Buffer Creation**: arrow-builder provides TypedArray buffers, lmao wraps with logging API
- **Hot Path Writes**: Direct TypedArray assignment only
  - Enums: buffer.attr_operation[idx] = OPERATION_MAP[value] (compile-time lookup)
  - Categories: buffer.attr_userId[idx] = internString(userId) (runtime interning)
  - Text: buffer.attr_errorMsg[idx] = rawString (no interning)
- **Method Chaining**: Return 	his from tag methods for fluent API: .userId(id).requestId(req)
- **Per-Span Buffers**: Each span owns its columnar TypedArrays (Uint8Array, Float64Array, etc.)
## Entry Type System (See specs/01h_entry_types_and_logging_primitives.md)
Unified enum for ALL trace events:
- **Span lifecycle**: span-start, span-ok, span-err, span-exception
- **Logging**: info, debug, warn, error
- **Structured data**: 	ag
- **Feature flags**: f-access, f-usage
Entry types use compile-time enum mapping to Uint8Array for 1-byte storage.
## Critical Performance Rules (See specs/01b1_buffer_performance_optimizations.md)
1. **Hot Path**: TypedArray writes ONLY. No Arrow builders, no objects, no console.log
2. **Cache Alignment**: 64-byte aligned TypedArrays (specs/01b_columnar_buffer_architecture.md)
3. **String Optimization**: 
   - Enums: Compile-time mapping to Uint8 (1 byte)
   - Categories: Runtime string interning to Uint32 (4 bytes)
   - Text: Raw strings without dictionary overhead
4. **Direct References**: SpanLogger holds buffer ref, no lookups (specs/01j_module_context_and_spanlogger_generation.md)
5. **Background Conversion**: Arrow Table creation in cold path ONLY (specs/01f_arrow_table_structure.md)
## Code Generation (See specs/01g_trace_context_api_codegen.md & 01j)
- **SpanLogger generation**: Runtime class generation with typed methods per schema
- **Attribute methods**: Each schema field gets a typed method on SpanLogger
- **Dual API**: Object-based (ctx.tag({ userId: "123" })) and property-based (ctx.tag.userId("123"))
- **Zero allocation**: Fluent methods return 	his, no intermediate objects
## Library Integration (See specs/01e_library_integration_pattern.md)
- Libraries define clean schemas without prefixes
- Prefixing happens at composition time
- Example: Library writes ctx.tag.status(200) → becomes http_status column
- Avoids naming conflicts across libraries
## Span Scope Attributes (See specs/01i_span_scope_attributes.md)
- Set attributes at span level: ctx.scope({ requestId, userId })
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