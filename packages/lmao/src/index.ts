// Main integration API - GREENFIELD - NO BACKWARDS COMPAT

// =============================================================================
// Primary API: defineOpContext (Op-Centric API)
// =============================================================================

// Op-Centric API - the module system (includes schema utilities and types)
export * from './lib/defineOpContext.js';

// Op class - used internally by defineOp()
export { Op } from './lib/op.js';

// =============================================================================
// Result Types
// =============================================================================

export { Blocked, type BlockedConfig, type BlockedReason } from './lib/errors/Blocked.js';
export type { Result, TaggedError, TaggedErrorConstructor } from './lib/result.js';
export { Err, hasErrorCode, Ok } from './lib/result.js';

// Tagged Error Types - tree-shakable subpath exports:
// import { Blocked } from '@smoothbricks/lmao/errors/Blocked'
// import { RetriesExhausted } from '@smoothbricks/lmao/errors/RetriesExhausted'

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
// LogAPI - Simple logging interface for contexts without SpanBuffer access
// =============================================================================

export type { LogAPI } from './lib/logApi.js';
export { noopLogAPI } from './lib/logApi.js';

// =============================================================================
// System Schema & Entry Type Constants
// =============================================================================

export {
  // Buffer metrics (utilization-based capacity tuning)
  ENTRY_TYPE_BUFFER_CAPACITY,
  ENTRY_TYPE_BUFFER_SPANS,
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
// Schema System (additional exports not in defineOpContext)
// Note: defineOpContext already re-exports S, defineFeatureFlags, defineLogSchema, LogSchema
// =============================================================================

export * from './lib/schema/evaluator.js';
export * from './lib/schema/extend.js';
export * from './lib/schema/typeGuards.js';

// =============================================================================
// Code Generation (for advanced use)
// =============================================================================

export type { ResultWriter, TagWriter } from './lib/codegen/fixedPositionWriterGenerator.js';
export { createResultWriter, createTagWriter } from './lib/codegen/fixedPositionWriterGenerator.js';
export type { BaseSpanLogger, SpanLoggerImpl } from './lib/codegen/spanLoggerGenerator.js';
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
// Buffer Strategy
// =============================================================================

export type { BufferStrategy } from './lib/bufferStrategy.js';
export { JsBufferStrategy } from './lib/JsBufferStrategy.js';

// =============================================================================
// Tracer
// =============================================================================

export type { OpContextBinding } from './lib/opContext/types.js';
export { type TraceFn, type TraceOverrides, Tracer, type TracerOptions } from './lib/tracer.js';
export { ArrayQueueTracer } from './lib/tracers/ArrayQueueTracer.js';
// Concrete tracer implementations
export { NoOpTracer } from './lib/tracers/NoOpTracer.js';
export { StdioTracer } from './lib/tracers/StdioTracer.js';
export { type StatsSnapshot, TestTracer } from './lib/tracers/TestTracer.js';

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

export {
  createTraceId,
  extractSpanIdentity,
  generateTraceId,
  isValidTraceId,
  MAX_TRACE_ID_LENGTH,
  type SpanIdentity,
  type TraceId,
} from './lib/traceId.js';

// =============================================================================
// TraceRoot (Platform-specific)
// =============================================================================

export type { ITraceRoot, TraceRootFactory, TracerLifecycleHooks } from './lib/traceRoot.js';

// =============================================================================
// UTF-8 Cache (for Arrow conversion)
// =============================================================================

export * from './lib/utf8Cache.js';
