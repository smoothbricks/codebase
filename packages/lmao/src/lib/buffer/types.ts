import * as arrow from 'apache-arrow';
import type { TagAttributeSchema } from '../schema/types.js';

/**
 * Arrow-based SpanBuffer for zero-copy columnar storage
 * 
 * This uses Apache Arrow's builder pattern for efficient memory management
 * and direct conversion to Arrow tables for Parquet serialization.
 */
export interface SpanBuffer {
  // Arrow builders for core columns
  timestampBuilder: arrow.Float64Builder;
  operationBuilder: arrow.Uint8Builder;
  
  // Null bitmap builder (managed by Arrow)
  // Arrow handles null tracking automatically per column
  
  // Attribute column builders (generated from schema)
  attributeBuilders: Record<string, arrow.Builder>;
  
  // Tree structure
  children: SpanBuffer[];
  parent?: SpanBuffer;
  
  // Buffer management
  writeIndex: number;
  capacity: number;
  next?: SpanBuffer;
  
  spanId: number;
  
  // Reference to task context
  task: TaskContext;
}

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
  spanBufferCapacityStats: {
    currentCapacity: number;
    totalWrites: number;
    overflowWrites: number;
    totalBuffersCreated: number;
  };
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
 * Arrow data type mapping for schema types
 */
export const ARROW_TYPE_MAP = {
  string: new arrow.Utf8(),
  number: new arrow.Float64(),
  boolean: new arrow.Bool(),
  integer: new arrow.Int32(),
} as const;
