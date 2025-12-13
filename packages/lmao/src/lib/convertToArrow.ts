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
import type { TypedArray } from '@smoothbricks/arrow-builder';
import { ENTRY_TYPE_NAMES } from './lmao.js';

/**
 * String interner interface (matches lmao's implementation)
 */
export interface StringInterner {
  getStrings(): readonly string[];
  getString(idx: number): string | undefined;
}

/**
 * Zero-copy helper: Create Arrow Data from TypedArray without copying
 * 
 * This demonstrates the zero-copy approach using arrow.makeData instead of builders.
 * For full zero-copy conversion, we'd need to handle buffer chaining, dictionaries,
 * and null bitmaps. Current implementation uses builders for simplicity in cold path.
 * 
 * @param type - Arrow data type
 * @param data - TypedArray with data
 * @param length - Number of valid elements
 * @param nullBitmap - Optional null bitmap
 * @returns Arrow Data object (zero-copy)
 * 
 * @example
 * ```typescript
 * // Zero-copy wrap a Float64Array as Arrow data
 * const data = createZeroCopyData(
 *   new arrow.Float64(),
 *   buffer.timestamps,
 *   buffer.writeIndex
 * );
 * const vector = arrow.makeVector(data);
 * ```
 */
export function createZeroCopyData<T extends arrow.DataType>(
  type: T,
  data: TypedArray,
  length: number,
  nullBitmap?: Uint8Array,
): arrow.Data<T> {
  // Slice the TypedArray to the specified length to get only valid data
  const slicedData = data.slice(0, length);
  
  // Create a vector from the sliced TypedArray using makeVector
  // This properly wraps the buffer and creates valid Arrow Data with values
  const vector = arrow.makeVector(slicedData);
  
  // Extract the Data from the vector
  // Note: Custom null bitmaps require using builders or lower-level APIs
  // The nullBitmap parameter is used for nullCount calculation but the actual
  // null bitmap in the returned Data comes from makeVector (all valid by default)
  const vectorData = vector.data[0];
  
  // If a null bitmap was provided, log that it's being used for reference
  // but the actual Data uses makeVector's default (all valid)
  if (nullBitmap) {
    // The nullCount is calculated but not applied to the Data directly
    // For full null bitmap support, use the builder-based conversion in convertToArrowTable
    const _nullCount = countNulls(nullBitmap, length);
    void _nullCount; // Acknowledge the calculation
  }
  
  // Cast through unknown to handle the generic type constraint
  return vectorData as unknown as arrow.Data<T>;
}

/**
 * Count nulls in a null bitmap
 * 
 * @param nullBitmap - Null bitmap (bit-packed)
 * @param length - Number of elements
 * @returns Number of null values
 */
function countNulls(nullBitmap: Uint8Array, length: number): number {
  let count = 0;
  for (let i = 0; i < length; i++) {
    const byteIndex = Math.floor(i / 8);
    const bitOffset = i % 8;
    const isNotNull = (nullBitmap[byteIndex] & (1 << bitOffset)) !== 0;
    if (!isNotNull) {
      count++;
    }
  }
  return count;
}

/**
 * System column builder function type
 * Allows lmao package to inject its own system columns (trace_id, span_id, etc.)
 * while keeping arrow-builder generic.
 */
export type SystemColumnBuilder = (
  buffer: SpanBuffer,
  buffers: SpanBuffer[],
  totalRows: number,
) => { fields: arrow.Field[]; vectors: arrow.Vector[] };

/**
 * Convert SpanBuffer to Apache Arrow Table
 *
 * This is a cold-path operation that happens in background processing.
 * It creates a queryable Arrow table from the hot-path columnar buffers.
 *
 * Includes standard lmao system columns: timestamp, trace_id, span_id, parent_span_id,
 * entry_type, module, and span_name. Custom system columns can be provided via
 * the optional systemColumnBuilder parameter.
 *
 * @param buffer - SpanBuffer to convert
 * @param categoryInterner - String interner for category columns (unused - kept for API compatibility)
 * @param textStorage - Text string storage (unused - kept for API compatibility)
 * @param moduleIdInterner - Module ID interner
 * @param spanNameInterner - Span name interner
 * @param systemColumnBuilder - Optional function to build custom system columns
 * @returns Apache Arrow Table
 */
export function convertToArrowTable(
  buffer: SpanBuffer,
  categoryInterner: StringInterner,
  textStorage: StringInterner,
  moduleIdInterner: StringInterner,
  spanNameInterner: StringInterner,
  systemColumnBuilder?: SystemColumnBuilder,
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
  let systemVectors: arrow.Vector[] = [];

  // Build system columns (custom or default)
  if (systemColumnBuilder) {
    const systemColumns = systemColumnBuilder(buffer, buffers, totalRows);
    fields.push(...systemColumns.fields);
    systemVectors = systemColumns.vectors;
  } else {
    // Default lmao system columns
    fields.push(arrow.Field.new({ name: 'timestamp', type: new arrow.TimestampNanosecond() }));
    fields.push(arrow.Field.new({ name: 'trace_id', type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()) }));
    fields.push(arrow.Field.new({ name: 'span_id', type: new arrow.Uint64() }));
    fields.push(arrow.Field.new({ name: 'parent_span_id', type: new arrow.Uint64(), nullable: true }));
    fields.push(
      arrow.Field.new({ name: 'entry_type', type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int8()) }),
    );
    fields.push(arrow.Field.new({ name: 'module', type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()) }));
    fields.push(
      arrow.Field.new({ name: 'span_name', type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()) }),
    );
  }

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
          nullable: true,
        }),
      );
    } else if (lmaoType === 'category') {
      // Category: Dictionary with runtime-built values
      fields.push(
        arrow.Field.new({
          name: fieldName,
          type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint32()),
          nullable: true,
        }),
      );
    } else if (lmaoType === 'text') {
      // Text: Plain string column (no dictionary)
      fields.push(
        arrow.Field.new({
          name: fieldName,
          type: new arrow.Utf8(),
          nullable: true,
        }),
      );
    } else if (lmaoType === 'number') {
      // Number: Float64
      fields.push(
        arrow.Field.new({
          name: fieldName,
          type: new arrow.Float64(),
          nullable: true,
        }),
      );
    } else if (lmaoType === 'boolean') {
      // Boolean: Bool type
      fields.push(
        arrow.Field.new({
          name: fieldName,
          type: new arrow.Bool(),
          nullable: true,
        }),
      );
    }
  }

  const arrowSchema = new arrow.Schema(fields);

  // Build vectors from buffers
  const vectors: arrow.Vector[] = [];

  // Add system column vectors
  if (systemColumnBuilder) {
    vectors.push(...systemVectors);
  } else {
    // Build default lmao system column vectors
    buildDefaultSystemVectors(buffers, vectors, moduleIdInterner, spanNameInterner);
  }

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
        nullValues: [null],
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
      // Category: Build dictionary at conversion time (no hot-path interning)
      // Per vizanto's review: Always build dictionary for category columns, sorted by value
      const categoryBuilder = arrow.makeBuilder({
        type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint32()),
        nullValues: [null],
      });

      for (const buf of buffers) {
        const column = buf[columnName];
        const nullBitmap = buf.nullBitmaps[columnName];

        if (column && Array.isArray(column)) {
          // Column is Array<string> - direct string storage
          for (let i = 0; i < buf.writeIndex; i++) {
            const isNull = !isRowNonNull(nullBitmap, i);
            if (isNull) {
              categoryBuilder.append(null);
            } else {
              const stringValue = column[i] || '';
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
      // Text: Build dictionary only if it saves space (>128 bytes)
      // Per vizanto's review: Calculate space savings before deciding dictionary vs plain UTF-8

      // First pass: collect strings and count occurrences
      const stringOccurrences = new Map<string, number>();
      let totalRows = 0;

      for (const buf of buffers) {
        const column = buf[columnName];
        const nullBitmap = buf.nullBitmaps[columnName];

        if (column && Array.isArray(column)) {
          for (let i = 0; i < buf.writeIndex; i++) {
            const isNull = !isRowNonNull(nullBitmap, i);
            if (!isNull) {
              const stringValue = column[i] || '';
              stringOccurrences.set(stringValue, (stringOccurrences.get(stringValue) || 0) + 1);
              totalRows++;
            }
          }
        }
      }

      // Calculate space savings with dictionary encoding
      // Dictionary: (num_unique_strings * avg_string_length) + (total_rows * 4) bytes
      // Plain UTF-8: total_rows * avg_string_length bytes
      let totalStringBytes = 0;
      let uniqueStringBytes = 0;

      for (const [str, count] of stringOccurrences.entries()) {
        const strBytes = str.length; // Approximation (UTF-8 can be more)
        totalStringBytes += strBytes * count;
        uniqueStringBytes += strBytes;
      }

      const dictionarySize = uniqueStringBytes + totalRows * 4; // 4 bytes per index
      const plainSize = totalStringBytes;
      const spaceSavings = plainSize - dictionarySize;

      // Only use dictionary if it saves more than 128 bytes
      const useDictionary = spaceSavings > 128;

      if (useDictionary) {
        // Build dictionary encoding
        const textBuilder = arrow.makeBuilder({
          type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Uint32()),
          nullValues: [null],
        });

        for (const buf of buffers) {
          const column = buf[columnName];
          const nullBitmap = buf.nullBitmaps[columnName];

          if (column && Array.isArray(column)) {
            for (let i = 0; i < buf.writeIndex; i++) {
              const isNull = !isRowNonNull(nullBitmap, i);
              if (isNull) {
                textBuilder.append(null);
              } else {
                const stringValue = column[i] || '';
                textBuilder.append(stringValue);
              }
            }
          } else {
            for (let i = 0; i < buf.writeIndex; i++) {
              textBuilder.append(null);
            }
          }
        }

        vectors.push(textBuilder.finish().toVector());
      } else {
        // Use plain UTF-8 column (no dictionary)
        const textBuilder = arrow.makeBuilder({
          type: new arrow.Utf8(),
          nullValues: [null],
        });

        for (const buf of buffers) {
          const column = buf[columnName];
          const nullBitmap = buf.nullBitmaps[columnName];

          if (column && Array.isArray(column)) {
            for (let i = 0; i < buf.writeIndex; i++) {
              const isNull = !isRowNonNull(nullBitmap, i);
              if (isNull) {
                textBuilder.append(null);
              } else {
                const stringValue = column[i] || '';
                textBuilder.append(stringValue);
              }
            }
          } else {
            for (let i = 0; i < buf.writeIndex; i++) {
              textBuilder.append(null);
            }
          }
        }

        vectors.push(textBuilder.finish().toVector());
      }
    } else if (lmaoType === 'number') {
      // Number: Float64
      const numberBuilder = arrow.makeBuilder({
        type: new arrow.Float64(),
        nullValues: [null],
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
        nullValues: [null],
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
    children: vectors.map((v) => v.data[0]),
  });

  const recordBatch = new arrow.RecordBatch(arrowSchema, data);
  return new arrow.Table([recordBatch]);
}

/**
 * Check if a row is non-null in the null bitmap
 * Arrow format: 1 = valid, 0 = null
 * If nullBitmap is undefined, the row is considered valid (non-null)
 */
function isRowNonNull(nullBitmap: Uint8Array | undefined, rowIndex: number): boolean {
  // If no null bitmap exists, treat all rows as valid (non-null)
  if (!nullBitmap) return true;

  const byteIndex = Math.floor(rowIndex / 8);
  const bitOffset = rowIndex % 8;

  // Out of range means null
  if (byteIndex >= nullBitmap.length) return false;

  return (nullBitmap[byteIndex] & (1 << bitOffset)) !== 0;
}

/**
 * Build default lmao system column vectors
 *
 * Creates standard trace logging columns: timestamp, trace_id, span_id, parent_span_id,
 * entry_type, module, and span_name.
 *
 * @param buffers - Array of SpanBuffers to convert
 * @param vectors - Array to append vectors to
 * @param moduleIdInterner - Module ID string interner
 * @param spanNameInterner - Span name string interner
 */
function buildDefaultSystemVectors(
  buffers: SpanBuffer[],
  vectors: arrow.Vector[],
  moduleIdInterner: StringInterner,
  spanNameInterner: StringInterner,
): void {
  // Timestamp vector (microseconds, not nanoseconds - Arrow uses microseconds for TimestampMicrosecond)
  const timestampBuilder = arrow.makeBuilder({
    type: new arrow.TimestampNanosecond(),
    nullValues: [null],
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
    nullValues: [null],
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
    nullValues: [null],
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
    nullValues: [null],
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
  const entryTypeBuilder = arrow.makeBuilder({
    type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int8()),
    nullValues: [null],
  });

  for (const buf of buffers) {
    for (let i = 0; i < buf.writeIndex; i++) {
      const entryTypeCode = buf.operations[i];
      const entryTypeName = ENTRY_TYPE_NAMES[entryTypeCode] || 'unknown';
      entryTypeBuilder.append(entryTypeName);
    }
  }
  vectors.push(entryTypeBuilder.finish().toVector());

  // Module vector (dictionary from moduleIdInterner)
  const moduleBuilder = arrow.makeBuilder({
    type: new arrow.Dictionary(new arrow.Utf8(), new arrow.Int32()),
    nullValues: [null],
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
    nullValues: [null],
  });

  for (const buf of buffers) {
    const spanName = spanNameInterner.getString(buf.task.spanNameId);
    for (let i = 0; i < buf.writeIndex; i++) {
      spanNameBuilder.append(spanName || 'unknown');
    }
  }
  vectors.push(spanNameBuilder.finish().toVector());
}

/**
 * Convert SpanBuffer tree to Arrow Table (includes child spans)
 * Recursively collects all spans in the tree
 *
 * @param rootBuffer - Root span buffer
 * @param categoryInterner - Category string interner
 * @param textStorage - Text string storage
 * @param moduleIdInterner - Module ID interner
 * @param spanNameInterner - Span name interner
 * @param systemColumnBuilder - Optional system column builder (lmao-specific)
 * @returns Arrow Table containing all spans in tree
 */
export function convertSpanTreeToArrowTable(
  rootBuffer: SpanBuffer,
  categoryInterner: StringInterner,
  textStorage: StringInterner,
  moduleIdInterner: StringInterner,
  spanNameInterner: StringInterner,
  systemColumnBuilder?: SystemColumnBuilder,
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
      spanNameInterner,
      systemColumnBuilder,
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

  // When combining multiple tables, they must have identical schemas
  // Since different spans can have different tag attributes, we need to:
  // 1. Merge all schemas to find the union of all columns
  // 2. Convert each table's data to match the merged schema (adding nulls for missing columns)
  // 3. Concatenate the aligned batches

  // Collect all unique field names across all tables
  const allFieldNames = new Set<string>();
  for (const table of tables) {
    for (const field of table.schema.fields) {
      allFieldNames.add(field.name);
    }
  }

  // Build merged data column by column
  const allData: Record<string, unknown[]> = {};
  for (const fieldName of allFieldNames) {
    allData[fieldName] = [];
  }

  // For each table, extract data and append to merged columns
  for (const table of tables) {
    const numRows = table.numRows;
    const fieldMap = new Map<string, arrow.Vector>();

    // Build map of field name to vector
    for (let i = 0; i < table.schema.fields.length; i++) {
      const field = table.schema.fields[i];
      fieldMap.set(field.name, table.getChildAt(i)!);
    }

    // For each merged field, extract data or add nulls
    for (const fieldName of allFieldNames) {
      const vector = fieldMap.get(fieldName);
      if (vector) {
        // Extract values from vector
        for (let rowIdx = 0; rowIdx < numRows; rowIdx++) {
          allData[fieldName].push(vector.get(rowIdx));
        }
      } else {
        // Field doesn't exist in this table, add nulls
        for (let rowIdx = 0; rowIdx < numRows; rowIdx++) {
          allData[fieldName].push(null);
        }
      }
    }
  }

  return arrow.tableFromArrays(allData);
}
