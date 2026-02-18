/// <reference types="node" />

/**
 * TraceRoot - Universal implementation with runtime platform detection.
 *
 * Selects between Node.js (nanosecond hrtime) and ES (performance.now()) based
 * on whether process.hrtime.bigint is available at runtime.
 *
 * Both modules are safe to import — platform-specific calls (process.hrtime.bigint(),
 * NAPI addon) happen inside the factory function, not at module scope.
 *
 * @module traceRoot.universal
 */

import { createTraceRoot as esFactory } from './traceRoot.es.js';
import type { TraceRootFactory } from './traceRoot.js';
import { createTraceRoot as nodeFactory } from './traceRoot.node.js';

// Node/Bun → nanosecond precision via hrtime.bigint()
// CF Workers/browsers → performance.now() microsecond precision
export const createTraceRoot: TraceRootFactory =
  typeof process !== 'undefined' && typeof process.hrtime?.bigint === 'function' ? nodeFactory : esFactory;
