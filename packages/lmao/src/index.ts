// Main integration API
export * from './lib/lmao.js';
export type { Result, SuccessResult, ErrorResult } from './lib/lmao.js';

// Schema system
export * from './lib/schema/types.js';
export * from './lib/schema/builder.js';
export * from './lib/schema/defineTagAttributes.js';
export * from './lib/schema/defineFeatureFlags.js';
export * from './lib/schema/evaluator.js';
export * from './lib/schema/extend.js';
export * from './lib/schema/typeGuards.js';

// Library integration
export * from './lib/library.js';

// Code generation (re-export BaseSpanLogger only, ChainableTagAPI already exported from lmao.js)
export type { BaseSpanLogger } from './lib/codegen/spanLoggerGenerator.js';
export { createSpanLoggerClass, generateSpanLoggerClass } from './lib/codegen/spanLoggerGenerator.js';

// Flush scheduler
export * from './lib/flushScheduler.js';
