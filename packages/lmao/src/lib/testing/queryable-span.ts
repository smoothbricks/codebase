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

import type { LogSchema } from '../schema/LogSchema.js';
import type { SpanBuffer } from '../types.js';
import { type ExtractFactsOptions, extractFacts } from './extractFacts.js';
import type { FactArray } from './facts.js';

export interface QueryableSpan<T extends LogSchema = LogSchema> {
  /** Span name (from message_values[0]) */
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
    return this.buffer.message_values[0];
  }

  facts(options?: ExtractFactsOptions): FactArray {
    return extractFacts(this.buffer, options);
  }

  find(name: string): QueryableSpan<T> | undefined {
    return findInChildren(this.buffer._children as SpanBuffer<T>[], name);
  }

  findAll(name: string): QueryableSpan<T>[] {
    const results: QueryableSpan<T>[] = [];
    collectAll(this.buffer._children as SpanBuffer<T>[], name, results);
    return results;
  }

  get children(): QueryableSpan<T>[] {
    return this.buffer._children.map((child) => new QueryableSpanImpl(child));
  }

  names(): string[] {
    const result: string[] = [];
    collectNames(this.buffer._children as SpanBuffer<T>[], result);
    return result;
  }
}

function findInChildren<T extends LogSchema>(children: SpanBuffer<T>[], name: string): QueryableSpan<T> | undefined {
  for (const child of children) {
    if (child.message_values[0] === name) {
      return new QueryableSpanImpl(child);
    }
    const found = findInChildren(child._children as SpanBuffer<T>[], name);
    if (found) return found;
  }
  return undefined;
}

function collectAll<T extends LogSchema>(children: SpanBuffer<T>[], name: string, results: QueryableSpan<T>[]): void {
  for (const child of children) {
    if (child.message_values[0] === name) {
      results.push(new QueryableSpanImpl(child));
    }
    collectAll(child._children as SpanBuffer<T>[], name, results);
  }
}

function collectNames<T extends LogSchema>(children: SpanBuffer<T>[], result: string[]): void {
  for (const child of children) {
    result.push(child.message_values[0]);
    collectNames(child._children as SpanBuffer<T>[], result);
  }
}
