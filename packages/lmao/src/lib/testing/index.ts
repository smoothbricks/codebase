/**
 * Testing utilities for LMAO trace-testing.
 *
 * This module provides strongly-typed trace facts and assertion helpers
 * based on Akkartik's "tracing tests" approach:
 * https://akkartik.name/post/tracing-tests
 *
 * Instead of testing return values directly, you:
 * 1. Execute code that emits trace facts
 * 2. Assert on the facts (WHAT happened, not HOW)
 *
 * @example
 * ```typescript
 * import { TestTracer } from '@smoothbricks/lmao';
 * import {
 *   createFactArray,
 *   spanStarted,
 *   spanOk,
 *   tagFact,
 * } from '@smoothbricks/lmao/testing';
 *
 * const tracer = new TestTracer(opContext);
 * await tracer.trace('my-op', myOp);
 *
 * const facts = extractFacts(tracer.rootBuffers[0]);
 *
 * // Assert on WHAT happened
 * expect(facts.has(spanOk('my-op'))).toBe(true);
 * expect(facts.hasInOrder([
 *   spanStarted('validate'),
 *   spanStarted('execute'),
 *   spanOk('execute'),
 *   spanOk('validate'),
 * ])).toBe(true);
 * expect(facts.has(tagFact('userId', '123'))).toBe(true);
 * ```
 *
 * @module testing
 */

// =============================================================================
// FACT TYPES AND BUILDERS
// =============================================================================

export {
  createFactArray,
  // FactArray
  type FactArray,
  // Namespaces
  type FactNamespace,
  // Feature flag facts
  type FFFact,
  ffFact,
  isFFFact,
  isLogFact,
  isMetricFact,
  isScopeFact,
  isSpanFact,
  isTagFact,
  // Log facts
  type LogFact,
  type LogLevel,
  logDebug,
  logError,
  logFact,
  logInfo,
  logWarn,
  // Metric facts
  type MetricFact,
  metricFact,
  // Parsing
  type ParsedFact,
  parseFact,
  type SchemaScopeFacts,
  // Schema-aware types
  type SchemaTagFacts,
  // Scope facts
  type ScopeFact,
  // Span facts
  type SpanFact,
  type SpanState,
  scopeFact,
  spanErr,
  spanException,
  spanOk,
  spanStarted,
  // Tag facts
  type TagFact,
  // Union type
  type TraceFact,
  tagFact,
} from './facts.js';

// =============================================================================
// TRACER RE-EXPORTS
// =============================================================================

export { JsBufferStrategy } from '../JsBufferStrategy.js';
export type { TraceRootFactory } from '../traceRoot.js';
// Universal createTraceRoot — picks Node (hrtime.bigint) or ES (performance.now) at runtime
export { createTraceRoot } from '../traceRoot.universal.js';
export { NoOpTracer } from '../tracers/NoOpTracer.js';
// Re-export TestTracer for convenience
export { type StatsSnapshot, TestTracer } from '../tracers/TestTracer.js';

// =============================================================================
// FACT EXTRACTION
// =============================================================================

export { type ExtractFactsOptions, extractFacts } from './extractFacts.js';

// =============================================================================
// SPAN QUERY API
// =============================================================================

export { type QueryableSpan, querySpan } from './queryable-span.js';
export { extractFactsFor, findAllSpans, findSpan, spanNames } from './span-query.js';

// =============================================================================
// SQLITE PERSISTENCE
// =============================================================================

export type { SyncSQLiteDatabase, SyncSQLiteStatement } from '../sqlite/sqlite-db.js';
export { type TraceSQLiteConfig, TraceSQLiteSink } from '../sqlite/sqlite-sink.js';
