/**
 * LMAO-specific types for trace logging
 *
 * These types extend the generic ColumnBuffer from arrow-builder
 * with span-specific fields and tracing concepts.
 */

import type { BufferCapacityStats, ColumnBuffer } from '@smoothbricks/arrow-builder';
import type { TagAttributeSchema } from './schema/types.js';

/**
 * Module context shared across all tasks in same module
 */
export interface ModuleContext {
  moduleId: number;
  gitSha: string;
  filePath: string;

  // Tag attribute schema for this module
  tagAttributes: TagAttributeSchema;

  // Self-tuning capacity stats
  spanBufferCapacityStats: BufferCapacityStats;
}

/**
 * Task context combines module + task-specific data
 */
export interface TaskContext {
  module: ModuleContext;
  spanNameId: number;
  lineNumber: number;
}

/**
 * SpanBuffer - lmao-specific extension of ColumnBuffer
 *
 * Adds span tree structure and task context to the base ColumnBuffer.
 */
export interface SpanBuffer extends ColumnBuffer {
  // Tree structure (lmao-specific for span hierarchy)
  children: SpanBuffer[];
  parent?: SpanBuffer;

  // Distributed span ID components
  threadId: bigint; // 64-bit random, set from worker context at span creation
  spanId: number; // 32-bit incrementing counter within this process
  traceId: string; // Root trace ID (constant per span)

  // Reference to task context (lmao-specific)
  task: TaskContext;
}

// Re-export useful arrow-builder types for convenience
export type { BufferCapacityStats, ColumnBuffer } from '@smoothbricks/arrow-builder';
