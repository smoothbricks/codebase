// Main integration API

// Fixed position writers (TagWriter writes to position 0, ResultWriter to position 1)
export type { ResultWriter, TagWriter } from './lib/codegen/fixedPositionWriterGenerator.js';
export { createResultWriter, createTagWriter } from './lib/codegen/fixedPositionWriterGenerator.js';
export type { GeneratedScope, ScopeClass } from './lib/codegen/scopeGenerator.js';
export { createScope, createScopeWithInheritance, generateScopeClass } from './lib/codegen/scopeGenerator.js';
// Code generation
export type { BaseSpanLogger } from './lib/codegen/spanLoggerGenerator.js';
export { createSpanLogger, createSpanLoggerClass } from './lib/codegen/spanLoggerGenerator.js';
// Arrow conversion (lmao-specific - uses SpanBuffer)
export * from './lib/convertToArrow.js';
// Flush scheduler
export * from './lib/flushScheduler.js';
// Library integration
export * from './lib/library.js';
export type { ErrorResult, Result, SuccessResult } from './lib/lmao.js';
export * from './lib/lmao.js';
export * from './lib/schema/builder.js';
export * from './lib/schema/defineFeatureFlags.js';
export * from './lib/schema/defineTagAttributes.js';
export * from './lib/schema/evaluator.js';
export * from './lib/schema/extend.js';
export * from './lib/schema/typeGuards.js';
// Schema system
export * from './lib/schema/types.js';
// SpanBuffer creation (lmao-specific buffer management)
export * from './lib/spanBuffer.js';
// Thread ID generation for distributed span identification
export {
  _resetThreadId,
  copyThreadIdTo,
  getThreadId,
  THREAD_ID_BYTES,
  writeThreadIdToUint64Array,
} from './lib/threadId.js';
// Trace ID (branded string type)
export { createTraceId, generateTraceId, isValidTraceId, MAX_TRACE_ID_LENGTH, type TraceId } from './lib/traceId.js';
// LMAO types (SpanBuffer, ModuleContext, TaskContext)
export * from './lib/types.js';
// UTF-8 cache (SIEVE-based for Arrow conversion)
export * from './lib/utf8Cache.js';
