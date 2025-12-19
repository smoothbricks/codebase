// Main integration API - GREENFIELD - NO BACKWARDS COMPAT

// =============================================================================
// Primary API: defineModule
// =============================================================================

// Re-export the interface separately (Module is both a type and a value concept)
export type { Module, Module as ModuleBuilder, ModuleMetadata, OpFunction as OpFn } from './lib/defineModule.js';
export { DefaultValueFlagEvaluator, defineModule, trackOverflowAndTune } from './lib/defineModule.js';
export { Op, OpBrand } from './lib/op.js';

// =============================================================================
// Result Types
// =============================================================================

export type { ErrorResult, Result, SuccessResult } from './lib/result.js';
export { FluentErrorResult, FluentSuccessResult } from './lib/result.js';

// =============================================================================
// TraceContext Types
// =============================================================================

export type { RootSpanFn, TraceContext, TraceContextSystem } from './lib/traceContext.js';
export { isTraceContext, TRACE_CONTEXT_MARKER, TraceContextProto } from './lib/traceContext.js';

// =============================================================================
// SpanContext Types
// =============================================================================

export type { FluentLogEntry, SpanContext, SpanFn, SpanLogger } from './lib/spanContext.js';
export { isSpanContext, SPAN_CONTEXT_MARKER } from './lib/spanContext.js';

// =============================================================================
// System Schema & Entry Type Constants
// =============================================================================

export {
  ENTRY_TYPE_BUFFER_CREATED,
  ENTRY_TYPE_BUFFER_OVERFLOW_WRITES,
  ENTRY_TYPE_BUFFER_OVERFLOWS,
  // Buffer metrics
  ENTRY_TYPE_BUFFER_WRITES,
  ENTRY_TYPE_DEBUG,
  ENTRY_TYPE_ERROR,
  // Feature flags
  ENTRY_TYPE_FF_ACCESS,
  ENTRY_TYPE_FF_USAGE,
  ENTRY_TYPE_INFO,
  // Lookup
  ENTRY_TYPE_NAMES,
  ENTRY_TYPE_OP_DURATION_ERR,
  ENTRY_TYPE_OP_DURATION_MAX,
  ENTRY_TYPE_OP_DURATION_MIN,
  ENTRY_TYPE_OP_DURATION_OK,
  ENTRY_TYPE_OP_DURATION_TOTAL,
  ENTRY_TYPE_OP_ERRORS,
  ENTRY_TYPE_OP_EXCEPTIONS,
  // Op metrics
  ENTRY_TYPE_OP_INVOCATIONS,
  // Period markers
  ENTRY_TYPE_PERIOD_START,
  ENTRY_TYPE_SPAN_ERR,
  ENTRY_TYPE_SPAN_EXCEPTION,
  ENTRY_TYPE_SPAN_OK,
  // Span lifecycle
  ENTRY_TYPE_SPAN_START,
  // Log levels (ordered by verbosity)
  ENTRY_TYPE_TRACE,
  ENTRY_TYPE_WARN,
  mergeWithSystemSchema,
  // Schema helpers
  systemSchema,
} from './lib/schema/systemSchema.js';

// =============================================================================
// Schema System
// =============================================================================

export * from './lib/schema/builder.js';
export * from './lib/schema/defineFeatureFlags.js';
export * from './lib/schema/defineLogSchema.js';
export * from './lib/schema/evaluator.js';
export * from './lib/schema/extend.js';
export * from './lib/schema/typeGuards.js';
export * from './lib/schema/types.js';

// =============================================================================
// Code Generation (for advanced use)
// =============================================================================

export type { ResultWriter, TagWriter } from './lib/codegen/fixedPositionWriterGenerator.js';
export { createResultWriter, createTagWriter } from './lib/codegen/fixedPositionWriterGenerator.js';
export type { GeneratedScope, ScopeClass } from './lib/codegen/scopeGenerator.js';
export { createScope, createScopeWithInheritance, generateScopeClass } from './lib/codegen/scopeGenerator.js';
export type { BaseSpanLogger } from './lib/codegen/spanLoggerGenerator.js';
export { createSpanLogger, createSpanLoggerClass } from './lib/codegen/spanLoggerGenerator.js';

// =============================================================================
// SpanBuffer & Arrow Conversion
// =============================================================================

export * from './lib/convertToArrow.js';
export * from './lib/spanBuffer.js';
export * from './lib/types.js';

// =============================================================================
// Flush Scheduler
// =============================================================================

export * from './lib/flushScheduler.js';

// =============================================================================
// Library Integration
// =============================================================================

export * from './lib/library.js';

// =============================================================================
// Thread ID Generation
// =============================================================================

export {
  _resetThreadId,
  copyThreadIdTo,
  getThreadId,
  THREAD_ID_BYTES,
  writeThreadIdToUint64Array,
} from './lib/threadId.js';

// =============================================================================
// Trace ID
// =============================================================================

export { createTraceId, generateTraceId, isValidTraceId, MAX_TRACE_ID_LENGTH, type TraceId } from './lib/traceId.js';

// =============================================================================
// UTF-8 Cache (for Arrow conversion)
// =============================================================================

export * from './lib/utf8Cache.js';
