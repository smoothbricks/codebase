import type { TagAttributeSchema } from '../schema-types.js';

/**
 * TypedArray-based ColumnBuffer for zero-copy columnar storage
 * 
 * Uses native TypeScript TypedArrays for efficient memory management
 * per specs/01b_columnar_buffer_architecture.md.
 * 
 * Arrow table conversion happens in cold path (background processing).
 * 
 * NOTE: This is a generic buffer structure that lmao will extend for span-specific use.
 * 
 * TODO (future optimization): Consider lazy column initialization with getters
 * to only allocate memory for columns actually used. See GitHub review comment.
 */
export interface ColumnBuffer {
  // Core columns - always present
  timestamps: Float64Array;    // Every operation appends timestamp
  operations: Uint8Array;      // Operation type: tag, ok, err, etc.
  
  // Null bitmaps - one Uint8Array per nullable column (Arrow format)
  // Each bitmap has length = Math.ceil(capacity / 8) bytes
  // Bit 0 = row 0, bit 1 = row 1, etc. within each byte
  nullBitmaps: Record<`attr_${string}`, Uint8Array>;
  
  // Attribute columns (generated from schema with attr_ prefix)
  // These are TypedArrays matching the schema field types
  [key: `attr_${string}`]: TypedArray;
  
  // Buffer management
  writeIndex: number;          // Current write position (0 to capacity-1)
  capacity: number;            // Logical capacity for bounds checking
  next?: ColumnBuffer;         // Chain to next buffer when overflow
}

/**
 * Generic TypedArray union type
 */
export type TypedArray = 
  | Uint8Array 
  | Uint16Array 
  | Uint32Array 
  | Int8Array 
  | Int16Array 
  | Int32Array 
  | Float32Array 
  | Float64Array;

/**
 * Capacity stats for buffer size tuning
 * Generic stats that any use case can build upon
 */
export interface BufferCapacityStats {
  currentCapacity: number;
  totalWrites: number;
  overflowWrites: number;
  totalBuffersCreated: number;
}

/**
 * Module context shared across all tasks in same module
 * 
 * NOTE: This is a lmao-specific concept but kept here for backward compatibility.
 * In future, lmao should extend this with its own type.
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
 * 
 * NOTE: This is a lmao-specific concept but kept here for backward compatibility.
 * In future, lmao should extend this with its own type.
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
 * Kept for backward compatibility with lmao package.
 */
export interface SpanBuffer extends ColumnBuffer {
  // Tree structure (lmao-specific for span hierarchy)
  children: SpanBuffer[];
  parent?: SpanBuffer;
  
  spanId: number;              // Incremental ID for this span
  traceId: string;             // Root trace ID (constant per span)
  
  // Reference to task context (lmao-specific)
  task: TaskContext;
}

/**
 * TypedArray constructor mapping for schema types
 * 
 * STRING TYPE SYSTEM (See specs/01a_trace_schema_system.md):
 * - enum: Uint8Array (1 byte) with compile-time dictionary
 * - category: Uint32Array for string interning indices
 * - text: Uint32Array for raw string storage indices (or raw strings in separate array)
 */
export const TYPED_ARRAY_MAP = {
  // String types - THREE DISTINCT TYPES
  enum: Uint8Array,           // Enum: compile-time mapped to Uint8Array indices (0-255)
  category: Uint32Array,      // Category: runtime string interning indices
  text: Uint32Array,          // Text: indices to string array (no interning)
  
  // Other primitive types
  number: Float64Array,       // Full precision numbers
  boolean: Uint8Array,        // 0 or 1
  integer: Int32Array,        // Signed 32-bit integers
} as const;

/**
 * Get TypedArray constructor for a schema type
 */
export type TypedArrayConstructor = 
  | Uint8ArrayConstructor
  | Uint16ArrayConstructor  
  | Uint32ArrayConstructor
  | Int8ArrayConstructor
  | Int16ArrayConstructor
  | Int32ArrayConstructor
  | Float32ArrayConstructor
  | Float64ArrayConstructor;
