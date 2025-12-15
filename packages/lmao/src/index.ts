// Main integration API

// Code generation (re-export BaseSpanLogger only, ChainableTagAPI already exported from lmao.js)
export type { BaseSpanLogger } from './lib/codegen/spanLoggerGenerator.js';
export { createSpanLoggerClass, generateSpanLoggerClass } from './lib/codegen/spanLoggerGenerator.js';
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
// LMAO types (SpanBuffer, ModuleContext, TaskContext)
export * from './lib/types.js';
