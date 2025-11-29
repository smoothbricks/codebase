/**
 * Zero-copy conversion from SpanBuffer to Apache Arrow tables
 * 
 * Per specs/01f_arrow_table_structure.md:
 * - Enum columns: Dictionary with compile-time values
 * - Category columns: Dictionary with runtime-built values
 * - Text columns: Plain strings without dictionary
 * - Zero-copy wrap TypedArrays as Arrow vectors
 */

import * as arrow from 'apache-arrow';
import type { SpanBuffer } from './types.js';

/**
 * String interner interface (matches lmao's implementation)
 */
export interface StringInterner {
  getStrings(): readonly string[];
  getString(idx: number): string | undefined;
}

/**
 * Convert SpanBuffer to Apache Arrow Table
 * 
 * This is a cold-path operation that happens in background processing.
 * It creates a queryable Arrow table from the hot-path columnar buffers.
 * 
 * @param buffer - SpanBuffer to convert
 * @param categoryInterner - String interner for category columns
 * @param textStorage - Text string storage (no interning)
 * @param moduleIdInterner - Module ID interner
 * @param spanNameInterner - Span name interner
 * @returns Apache Arrow Table
 */
export function convertToArrowTable(
  buffer: SpanBuffer,
  categoryInterner: StringInterner,
  textStorage: StringInterner,
  moduleIdInterner: StringInterner,
  spanNameInterner: StringInterner
): arrow.Table {
  // Collect all buffers in the chain
  const buffers: SpanBuffer[] = [];
  let currentBuffer: SpanBuffer | undefined = buffer;
  
  while (currentBuffer) {
    buffers.push(currentBuffer);
    currentBuffer = currentBuffer.next as SpanBuffer | undefined;
  }
  
  // Calculate total row count
  const totalRows = buffers.reduce((sum, buf) => sum + buf.writeIndex, 0);
  
  if (totalRows === 0) {
    // Return empty table if no rows
    return new arrow.Table();
  }
  
  // Build Arrow schema from buffer columns
  const schema = buffer.task.module.tagAttributes;
  const fields: arrow.Field[] = [];
  
  // Core system columns (always present)
  fields.push(arrow.Field.new({ name: 'timestamp', type: new arrow.TimestampNanosecond() }));
  fields.push(arrow.Field.new({ name: 'trace_id', type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()) }));
  fields.push(arrow.Field.new({ name: 'span_id', type: new arrow.Uint64() }));
  fields.push(arrow.Field.new({ name: 'parent_span_id', type: new arrow.Uint64(), nullable: true }));
  fields.push(arrow.Field.new({ name: 'entry_type', type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int8()) }));
  fields.push(arrow.Field.new({ name: 'module', type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()) }));
  fields.push(arrow.Field.new({ name: 'span_name', type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()) }));
  
  // Add attribute columns from schema
  for (const [fieldName, fieldSchema] of Object.entries(schema)) {
    const schemaWithMetadata = fieldSchema as {
      __lmao_type?: string;
      __lmao_enum_values?: readonly string[];
    };
    const lmaoType = schemaWithMetadata.__lmao_type;
    
    if (lmaoType === 'enum') {
      // Enum: Dictionary with compile-time values
      fields.push(
        arrow.Field.new({
          name: fieldName,
          type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint8()),
          nullable: true
        })
      );
    } else if (lmaoType === 'category') {
      // Category: Dictionary with runtime-built values
      fields.push(
        arrow.Field.new({
          name: fieldName,
          type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint32()),
          nullable: true
        })
      );
    } else if (lmaoType === 'text') {
      // Text: Plain string column (no dictionary)
      fields.push(
        arrow.Field.new({
          name: fieldName,
          type: new arrow.Utf8(),
          nullable: true
        })
      );
    } else if (lmaoType === 'number') {
      // Number: Float64
      fields.push(
        arrow.Field.new({
          name: fieldName,
          type: new arrow.Float64(),
          nullable: true
        })
      );
    } else if (lmaoType === 'boolean') {
      // Boolean: Bool type
      fields.push(
        arrow.Field.new({
          name: fieldName,
          type: new arrow.Bool(),
          nullable: true
        })
      );
    }
  }
  
  const arrowSchema = new arrow.Schema(fields);
  
  // Build vectors from buffers
  const vectors: arrow.Vector[] = [];
  
  // Timestamp vector (microseconds, not nanoseconds - Arrow uses microseconds for TimestampMicrosecond)
  const timestampBuilder = arrow.makeBuilder({
    type: new arrow.TimestampNanosecond(),
    nullValues: [null]
  });
  
  for (const buf of buffers) {
    for (let i = 0; i < buf.writeIndex; i++) {
      // Convert milliseconds to nanoseconds
      const timestampMs = buf.timestamps[i];
      const timestampNs = Math.floor(timestampMs * 1_000_000); // ms to ns
      timestampBuilder.append(timestampNs);
    }
  }
  vectors.push(timestampBuilder.finish().toVector());
  
  // Trace ID vector (dictionary encoded)
  const traceIdBuilder = arrow.makeBuilder({
    type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()),
    nullValues: [null]
  });
  
  for (const buf of buffers) {
    for (let i = 0; i < buf.writeIndex; i++) {
      traceIdBuilder.append(buf.traceId);
    }
  }
  vectors.push(traceIdBuilder.finish().toVector());
  
  // Span ID vector
  const spanIdBuilder = arrow.makeBuilder({
    type: new arrow.Uint64(),
    nullValues: [null]
  });
  
  for (const buf of buffers) {
    for (let i = 0; i < buf.writeIndex; i++) {
      spanIdBuilder.append(BigInt(buf.spanId));
    }
  }
  vectors.push(spanIdBuilder.finish().toVector());
  
  // Parent span ID vector (nullable)
  const parentSpanIdBuilder = arrow.makeBuilder({
    type: new arrow.Uint64(),
    nullValues: [null]
  });
  
  for (const buf of buffers) {
    for (let i = 0; i < buf.writeIndex; i++) {
      if (buf.parent) {
        parentSpanIdBuilder.append(BigInt(buf.parent.spanId));
      } else {
        parentSpanIdBuilder.append(null);
      }
    }
  }
  vectors.push(parentSpanIdBuilder.finish().toVector());
  
  // Entry type vector (dictionary with entry type names)
  const entryTypeNames = [
    'ff-access',      // 1
    'ff-usage',       // 2
    'tag',            // 3
    'message',        // 4
    'span-start',     // 5
    'span-ok',        // 6
    'span-err',       // 7
    'span-exception', // 8
    'info',           // 9
    'debug',          // 10
    'warn',           // 11
    'error'           // 12
  ];
  
  const entryTypeBuilder = arrow.makeBuilder({
    type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int8()),
    nullValues: [null]
  });
  
  for (const buf of buffers) {
    for (let i = 0; i < buf.writeIndex; i++) {
      const entryTypeCode = buf.operations[i];
      const entryTypeName = entryTypeNames[entryTypeCode - 1] || 'unknown';
      entryTypeBuilder.append(entryTypeName);
    }
  }
  vectors.push(entryTypeBuilder.finish().toVector());
  
  // Module vector (dictionary from moduleIdInterner)
  const moduleBuilder = arrow.makeBuilder({
    type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()),
    nullValues: [null]
  });
  
  for (const buf of buffers) {
    const moduleName = moduleIdInterner.getString(buf.task.module.moduleId);
    for (let i = 0; i < buf.writeIndex; i++) {
      moduleBuilder.append(moduleName || 'unknown');
    }
  }
  vectors.push(moduleBuilder.finish().toVector());
  
  // Span name vector (dictionary from spanNameInterner)
  const spanNameBuilder = arrow.makeBuilder({
    type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()),
    nullValues: [null]
  });
  
  for (const buf of buffers) {
    const spanName = spanNameInterner.getString(buf.task.spanNameId);
    for (let i = 0; i < buf.writeIndex; i++) {
      spanNameBuilder.append(spanName || 'unknown');
    }
  }
  vectors.push(spanNameBuilder.finish().toVector());
  
  // Attribute columns
  for (const [fieldName, fieldSchema] of Object.entries(schema)) {
    const schemaWithMetadata = fieldSchema as {
      __lmao_type?: string;
      __lmao_enum_values?: readonly string[];
    };
    const lmaoType = schemaWithMetadata.__lmao_type;
    const columnName = `attr_${fieldName}` as `attr_${string}`;
    
    if (lmaoType === 'enum') {
      // Enum: Dictionary with compile-time values
      const enumValues = schemaWithMetadata.__lmao_enum_values || [];
      const enumBuilder = arrow.makeBuilder({
        type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint8()),
        nullValues: [null]
      });
      
      for (const buf of buffers) {
        const column = buf[columnName];
        const nullBitmap = buf.nullBitmaps[columnName];
        
        if (column && column instanceof Uint8Array) {
          for (let i = 0; i < buf.writeIndex; i++) {
            const isNull = !isRowNonNull(nullBitmap, i);
            if (isNull) {
              enumBuilder.append(null);
            } else {
              const enumIndex = column[i];
              const enumValue = enumValues[enumIndex] || '';
              enumBuilder.append(enumValue);
            }
          }
        } else {
          // Column doesn't exist, append nulls
          for (let i = 0; i < buf.writeIndex; i++) {
            enumBuilder.append(null);
          }
        }
      }
      
      vectors.push(enumBuilder.finish().toVector());
    } else if (lmaoType === 'category') {
      // Category: Dictionary with runtime-built values
      const categoryBuilder = arrow.makeBuilder({
        type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint32()),
        nullValues: [null]
      });
      
      for (const buf of buffers) {
        const column = buf[columnName];
        const nullBitmap = buf.nullBitmaps[columnName];
        
        if (column && column instanceof Uint32Array) {
          for (let i = 0; i < buf.writeIndex; i++) {
            const isNull = !isRowNonNull(nullBitmap, i);
            if (isNull) {
              categoryBuilder.append(null);
            } else {
              const stringIndex = column[i];
              const stringValue = categoryInterner.getString(stringIndex) || '';
              categoryBuilder.append(stringValue);
            }
          }
        } else {
          // Column doesn't exist, append nulls
          for (let i = 0; i < buf.writeIndex; i++) {
            categoryBuilder.append(null);
          }
        }
      }
      
      vectors.push(categoryBuilder.finish().toVector());
    } else if (lmaoType === 'text') {
      // Text: Plain string (no dictionary)
      const textBuilder = arrow.makeBuilder({
        type: new arrow.Utf8(),
        nullValues: [null]
      });
      
      for (const buf of buffers) {
        const column = buf[columnName];
        const nullBitmap = buf.nullBitmaps[columnName];
        
        if (column && column instanceof Uint32Array) {
          for (let i = 0; i < buf.writeIndex; i++) {
            const isNull = !isRowNonNull(nullBitmap, i);
            if (isNull) {
              textBuilder.append(null);
            } else {
              const stringIndex = column[i];
              const stringValue = textStorage.getString(stringIndex) || '';
              textBuilder.append(stringValue);
            }
          }
        } else {
          // Column doesn't exist, append nulls
          for (let i = 0; i < buf.writeIndex; i++) {
            textBuilder.append(null);
          }
        }
      }
      
      vectors.push(textBuilder.finish().toVector());
    } else if (lmaoType === 'number') {
      // Number: Float64
      const numberBuilder = arrow.makeBuilder({
        type: new arrow.Float64(),
        nullValues: [null]
      });
      
      for (const buf of buffers) {
        const column = buf[columnName];
        const nullBitmap = buf.nullBitmaps[columnName];
        
        if (column && column instanceof Float64Array) {
          for (let i = 0; i < buf.writeIndex; i++) {
            const isNull = !isRowNonNull(nullBitmap, i);
            if (isNull) {
              numberBuilder.append(null);
            } else {
              numberBuilder.append(column[i]);
            }
          }
        } else {
          // Column doesn't exist, append nulls
          for (let i = 0; i < buf.writeIndex; i++) {
            numberBuilder.append(null);
          }
        }
      }
      
      vectors.push(numberBuilder.finish().toVector());
    } else if (lmaoType === 'boolean') {
      // Boolean: Bool
      const boolBuilder = arrow.makeBuilder({
        type: new arrow.Bool(),
        nullValues: [null]
      });
      
      for (const buf of buffers) {
        const column = buf[columnName];
        const nullBitmap = buf.nullBitmaps[columnName];
        
        if (column && column instanceof Uint8Array) {
          for (let i = 0; i < buf.writeIndex; i++) {
            const isNull = !isRowNonNull(nullBitmap, i);
            if (isNull) {
              boolBuilder.append(null);
            } else {
              boolBuilder.append(column[i] !== 0);
            }
          }
        } else {
          // Column doesn't exist, append nulls
          for (let i = 0; i < buf.writeIndex; i++) {
            boolBuilder.append(null);
          }
        }
      }
      
      vectors.push(boolBuilder.finish().toVector());
    }
  }
  
  // Create Arrow Table from schema and vectors
  const data = arrow.makeData({
    type: new arrow.Struct(arrowSchema.fields),
    length: totalRows,
    nullCount: 0,
    children: vectors.map(v => v.data[0])
  });
  
  const recordBatch = new arrow.RecordBatch(arrowSchema, data);
  return new arrow.Table([recordBatch]);
}

/**
 * Check if a row is non-null in the null bitmap
 * Arrow format: 1 = valid, 0 = null
 */
function isRowNonNull(nullBitmap: Uint8Array | undefined, rowIndex: number): boolean {
  if (!nullBitmap) return false;
  
  const byteIndex = Math.floor(rowIndex / 8);
  const bitOffset = rowIndex % 8;
  
  if (byteIndex >= nullBitmap.length) return false;
  
  return (nullBitmap[byteIndex] & (1 << bitOffset)) !== 0;
}

/**
 * Convert SpanBuffer tree to Arrow Table (includes child spans)
 * Recursively collects all spans in the tree
 */
export function convertSpanTreeToArrowTable(
  rootBuffer: SpanBuffer,
  categoryInterner: StringInterner,
  textStorage: StringInterner,
  moduleIdInterner: StringInterner,
  spanNameInterner: StringInterner
): arrow.Table {
  // Collect all buffers in tree (depth-first)
  const allBuffers: SpanBuffer[] = [];
  
  function collectBuffers(buffer: SpanBuffer): void {
    allBuffers.push(buffer);
    for (const child of buffer.children) {
      collectBuffers(child);
    }
  }
  
  collectBuffers(rootBuffer);
  
  // Convert all buffers to a single table
  // For simplicity, we'll convert each buffer separately and concatenate
  const tables: arrow.Table[] = [];
  
  for (const buffer of allBuffers) {
    const table = convertToArrowTable(
      buffer,
      categoryInterner,
      textStorage,
      moduleIdInterner,
      spanNameInterner
    );
    
    if (table.numRows > 0) {
      tables.push(table);
    }
  }
  
  if (tables.length === 0) {
    return new arrow.Table();
  }
  
  if (tables.length === 1) {
    return tables[0];
  }
  
  // Concatenate all tables by extracting their record batches
  const allBatches: arrow.RecordBatch[] = [];
  for (const table of tables) {
    for (let i = 0; i < table.batches.length; i++) {
      allBatches.push(table.batches[i]);
    }
  }
  return new arrow.Table(allBatches);
}
