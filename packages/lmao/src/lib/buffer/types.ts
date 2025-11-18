export interface SpanBuffer {
  // Core columns - always present
  timestamps: Float64Array;
  operations: Uint8Array;
  nullBitmap: Uint8Array | Uint16Array | Uint32Array;
  
  // Tree structure
  children: SpanBuffer[];
  parent?: SpanBuffer;
  
  // Buffer management
  writeIndex: number;
  capacity: number;
  next?: SpanBuffer;
  
  spanId: number;
}
