/**
 * Standalone span query functions — tree-shakable alternative to QueryableSpan.
 *
 * These functions operate directly on SpanBuffer instances without wrapping.
 * Prefer these when you only need one or two operations and want minimal overhead.
 *
 * @module span-query
 */

import { resolveMessage } from '../resolveMessage.js';
import type { LogSchema } from '../schema/LogSchema.js';
import type { SpanBuffer } from '../types.js';
import { type ExtractFactsOptions, extractFacts } from './extractFacts.js';
import type { FactArray } from './facts.js';

/** Find first descendant span by name (depth-first) */
export function findSpan<T extends LogSchema>(root: SpanBuffer<T>, name: string): SpanBuffer<T> | undefined {
  for (const child of root._children) {
    if (resolveMessage(child, 0) === name) return child;
    const found = findSpan(child, name);
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
  collectNames(root._children, result);
  return result;
}

function collectSpans<T extends LogSchema>(root: SpanBuffer<T>, name: string, results: SpanBuffer<T>[]): void {
  for (const child of root._children) {
    if (resolveMessage(child, 0) === name) results.push(child);
    collectSpans(child, name, results);
  }
}

function collectNames<T extends LogSchema>(children: SpanBuffer<T>[], result: string[]): void {
  for (const child of children) {
    result.push(resolveMessage(child, 0) ?? '');
    collectNames(child._children, result);
  }
}
