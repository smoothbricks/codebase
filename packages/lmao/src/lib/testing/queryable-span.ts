/**
 * QueryableSpan - Ergonomic wrapper around SpanBuffer for test assertions.
 *
 * Provides a clean API for navigating the span tree and extracting facts
 * without needing to know SpanBuffer internals.
 *
 * @example
 * ```typescript
 * const q = querySpan(tracer.rootBuffers[0]);
 * expect(q.name).toBe('my-op');
 * expect(q.find('validate')?.facts().has(spanOk('validate'))).toBe(true);
 * expect(q.names()).toContain('save');
 * ```
 *
 * @module queryable-span
 */

import { resolveMessage } from '../resolveMessage.js';
import type { LogSchema } from '../schema/LogSchema.js';
import type { SpanBuffer } from '../types.js';
import { iterateSpanChildren, iterateSpanTree } from '../traceTopology.js';
import { type ExtractFactsOptions, extractFacts } from './extractFacts.js';
import type { FactArray } from './facts.js';

export interface QueryableSpan<T extends LogSchema = LogSchema> {
  /** Resolved span name. */
  readonly name: string;

  /** Extract all facts from this span and its children */
  facts(options?: ExtractFactsOptions): FactArray;

  /** Find first child span by name (depth-first) */
  find(name: string): QueryableSpan<T> | undefined;

  /** Find all spans matching name (depth-first) */
  findAll(name: string): QueryableSpan<T>[];

  /** Direct child spans */
  readonly children: QueryableSpan<T>[];

  /** All descendant span names (flat list, depth-first) */
  names(): string[];

  /** Raw SpanBuffer for advanced use */
  readonly buffer: SpanBuffer<T>;
}

/** Wrap a SpanBuffer as a QueryableSpan for ergonomic test assertions */
export function querySpan<T extends LogSchema>(buffer: SpanBuffer<T>): QueryableSpan<T> {
  return new QueryableSpanImpl(buffer);
}

class QueryableSpanImpl<T extends LogSchema> implements QueryableSpan<T> {
  constructor(readonly buffer: SpanBuffer<T>) {}

  get name(): string {
    return resolveMessage(this.buffer, 0) ?? '';
  }

  facts(options?: ExtractFactsOptions): FactArray {
    return extractFacts(this.buffer, options);
  }

  find(name: string): QueryableSpan<T> | undefined {
    for (const descendant of iterateDescendantSpans(this.buffer)) {
      if (resolveMessage(descendant, 0) === name) return new QueryableSpanImpl(descendant);
    }
    return undefined;
  }

  findAll(name: string): QueryableSpan<T>[] {
    const results: QueryableSpan<T>[] = [];
    for (const descendant of iterateDescendantSpans(this.buffer)) {
      if (resolveMessage(descendant, 0) === name) results.push(new QueryableSpanImpl(descendant));
    }
    return results;
  }

  get children(): QueryableSpan<T>[] {
    const result: QueryableSpan<T>[] = [];
    for (const child of iterateSpanChildren(this.buffer)) result.push(new QueryableSpanImpl(child));
    return result;
  }

  names(): string[] {
    const result: string[] = [];
    for (const descendant of iterateDescendantSpans(this.buffer)) {
      result.push(resolveMessage(descendant, 0) ?? '');
    }
    return result;
  }
}

function* iterateDescendantSpans<T extends LogSchema>(root: SpanBuffer<T>): Generator<SpanBuffer<T>> {
  let previousNodeIndex = root._nodeIndex;
  for (const buffer of iterateSpanTree(root)) {
    if (buffer._nodeIndex === previousNodeIndex) continue;
    previousNodeIndex = buffer._nodeIndex;
    yield buffer;
  }
}
