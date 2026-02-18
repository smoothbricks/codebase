/**
 * Standalone span query functions — tree-shakable alternative to QueryableSpan.
 *
 * These functions operate directly on SpanBuffer instances without wrapping.
 * Prefer these when you only need one or two operations and want minimal overhead.
 *
 * @module span-query
 */

import type { LogSchema } from '../schema/LogSchema.js';
import type { SpanBuffer } from '../types.js';
import { type ExtractFactsOptions, extractFacts } from './extractFacts.js';
import type { FactArray } from './facts.js';

/** Find first descendant span by name (depth-first) */
export function findSpan<T extends LogSchema>(root: SpanBuffer<T>, name: string): SpanBuffer<T> | undefined {
  for (const child of root._children) {
    const typed = child as SpanBuffer<T>;
    if (typed.message_values[0] === name) return typed;
    const found = findSpan(typed, name);
    if (found) return found;
  }
  return undefined;
}

/** Find all descendant spans matching name (depth-first) */
export function findAllSpans<T extends LogSchema>(root: SpanBuffer<T>, name: string): SpanBuffer<T>[] {
  const results: SpanBuffer<T>[] = [];
  collectSpans(root, name, results);
  return results;
}

/** Extract facts for a specific named span within a root buffer tree */
export function extractFactsFor<T extends LogSchema>(
  root: SpanBuffer<T>,
  spanName: string,
  options?: ExtractFactsOptions,
): FactArray | undefined {
  const span = findSpan(root, spanName);
  if (!span) return undefined;
  return extractFacts(span, options);
}

/** Collect all descendant span names (flat list, depth-first) */
export function spanNames<T extends LogSchema>(root: SpanBuffer<T>): string[] {
  const result: string[] = [];
  collectNames(root._children as SpanBuffer<T>[], result);
  return result;
}

function collectSpans<T extends LogSchema>(root: SpanBuffer<T>, name: string, results: SpanBuffer<T>[]): void {
  for (const child of root._children) {
    const typed = child as SpanBuffer<T>;
    if (typed.message_values[0] === name) results.push(typed);
    collectSpans(typed, name, results);
  }
}

function collectNames<T extends LogSchema>(children: SpanBuffer<T>[], result: string[]): void {
  for (const child of children) {
    const typed = child as SpanBuffer<T>;
    result.push(typed.message_values[0]);
    collectNames(typed._children as SpanBuffer<T>[], result);
  }
}
