import type { TagAttributeSchema } from '@smoothbricks/lmao';

/**
 * TypedArray-based SpanBuffer for zero-copy columnar storage
 * 
 * Uses native TypeScript TypedArrays for efficient memory management
 * per specs/01b_columnar_buffer_architecture.md.
 * 
 * Arrow table conversion happens in cold path (background processing).
 */
export interface SpanBuffer {
  // Core columns - always present
  timestamps: Float64Array;    // Every operation appends timestamp
  operations: Uint8Array;      // Operation type: tag, ok, err, etc.
  
  // Null bitmap - dynamically sized based on attribute count
  nullBitmap: Uint8Array | Uint16Array | Uint32Array;  // Bit flags for which attributes have values
  
  // Attribute columns (generated from schema with attr_ prefix)
  // These are TypedArrays matching the schema field types
  [key: `attr_${string}`]: TypedArray;
  
  // Tree structure
  children: SpanBuffer[];
  parent?: SpanBuffer;
  
  // Buffer management
  writeIndex: number;          // Current write position (0 to capacity-1)
  capacity: number;            // Logical capacity for bounds checking
  next?: SpanBuffer;           // Chain to next buffer when overflow
  
  spanId: number;              // Incremental ID for this SpanBuffer
  traceId: string;             // Root trace ID (constant per span)
  
  // Reference to task context
  task: TaskContext;
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
