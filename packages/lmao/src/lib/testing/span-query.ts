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
import { iterateSpanTree } from '../traceTopology.js';
import { type ExtractFactsOptions, extractFacts } from './extractFacts.js';
import type { FactArray } from './facts.js';

/** Find first descendant span by name (depth-first) */
export function findSpan<T extends LogSchema>(root: SpanBuffer<T>, name: string): SpanBuffer<T> | undefined {
  for (const descendant of iterateDescendantSpans(root)) {
    if (resolveMessage(descendant, 0) === name) return descendant;
  }
  return undefined;
}

/** Find all descendant spans matching name (depth-first) */
export function findAllSpans<T extends LogSchema>(root: SpanBuffer<T>, name: string): SpanBuffer<T>[] {
  const results: SpanBuffer<T>[] = [];
  for (const descendant of iterateDescendantSpans(root)) {
    if (resolveMessage(descendant, 0) === name) results.push(descendant);
  }
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
  for (const descendant of iterateDescendantSpans(root)) {
    result.push(resolveMessage(descendant, 0) ?? '');
  }
  return result;
}

function* iterateDescendantSpans<T extends LogSchema>(root: SpanBuffer<T>): Generator<SpanBuffer<T>> {
  let previousNodeIndex = root._nodeIndex;
  for (const buffer of iterateSpanTree(root)) {
    if (buffer._nodeIndex === previousNodeIndex) continue;
    previousNodeIndex = buffer._nodeIndex;
    yield buffer;
  }
}
