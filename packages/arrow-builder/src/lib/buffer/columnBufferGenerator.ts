/**
 * Runtime code generation for ColumnBuffer classes with zero-indirection column access.
 *
 * Per specs/01b1_buffer_performance_optimizations.md:
 * - Generate concrete class at module initialization time (cold path)
 * - Direct properties for each column (no lazy getters, no wrapper objects)
 * - attr_${name}_nulls and attr_${name}_values share ONE ArrayBuffer per column
 * - System columns (timestamps, operations) as direct Float64Array/Uint8Array
 * - Zero indirection in the hot path
 *
 * WHY Runtime Generation:
 * - V8 hidden class stability: All properties defined in constructor
 * - Monomorphic access: V8 knows exact types at each call site
 * - Inline caching: Property access optimized by V8
 * - Cache alignment: ArrayBuffers aligned to 64-byte boundaries
 */

import type { TagAttributeSchema } from '../schema-types.js';
import type { ColumnBuffer } from './types.js';

/**
 * Get TypedArray constructor name and byte size for a schema field
 */
function getTypedArrayInfo(
  schema: TagAttributeSchema,
  fieldName: string,
): { constructorName: string; bytesPerElement: number; isBitPacked: boolean } {
  const fieldSchema = schema[fieldName];
  const schemaWithMetadata = fieldSchema as import('../schema-types.js').SchemaWithMetadata;
  const schemaType = schemaWithMetadata?.__schema_type;

  // Handle three string types
  if (schemaType === 'enum') {
    const enumValues = schemaWithMetadata.__enum_values;
    const enumCount = enumValues?.length ?? 0;

    // Uint8Array can hold 0-255 indices (256 values total)
    if (enumCount === 0 || enumCount <= 256) {
      return { constructorName: 'Uint8Array', bytesPerElement: 1, isBitPacked: false };
    }
    if (enumCount <= 65536) {
      return { constructorName: 'Uint16Array', bytesPerElement: 2, isBitPacked: false };
    }
    return { constructorName: 'Uint32Array', bytesPerElement: 4, isBitPacked: false };
  }

  if (schemaType === 'category') {
    // Hot path: Store raw JavaScript strings (zero conversion cost)
    // Cold path: Arrow conversion builds sorted dictionary
    return { constructorName: 'Array', bytesPerElement: 0, isBitPacked: false };
  }

  if (schemaType === 'text') {
    // Hot path: Store raw JavaScript strings (zero conversion cost)
    // Cold path: Arrow conversion calculates if dictionary saves space
    return { constructorName: 'Array', bytesPerElement: 0, isBitPacked: false };
  }

  if (schemaType === 'number') {
    return { constructorName: 'Float64Array', bytesPerElement: 8, isBitPacked: false };
  }

  if (schemaType === 'boolean') {
    // Boolean: bit-packed storage (8 booleans per byte) for Arrow compatibility
    return { constructorName: 'Uint8Array', bytesPerElement: 1, isBitPacked: true };
  }

  // Default to Uint32Array
  return { constructorName: 'Uint32Array', bytesPerElement: 4, isBitPacked: false };
}

/**
 * Generate ColumnBuffer class code as a string
 *
 * Creates a concrete class with:
 * 1. EAGER system columns (timestamps, operations) - allocated in constructor
 * 2. LAZY user attribute columns (attr_X_nulls, attr_X_values) - getters that allocate on first access
 * 3. Shared ArrayBuffer per column (nulls and values use same buffer when allocated)
 * 4. Cache-aligned allocations
 *
 * WHY lazy for user columns:
 * - Many records only use a subset of schema attributes
 * - Lazy allocation saves memory for unused columns
 * - First access triggers allocation of shared ArrayBuffer for both _nulls and _values
 *
 * IMPORTANT: Per-instance storage via private symbol keys
 * Each instance stores its lazy-allocated arrays in private symbol-keyed properties.
 * This avoids the closure-sharing bug where all instances share the same arrays.
 */
export function generateColumnBufferClass(schema: TagAttributeSchema, className = 'GeneratedColumnBuffer'): string {
  const schemaFields = Object.keys(schema);

  // Generate constructor code for eager system columns
  const constructorCode: string[] = [];

  // System columns (ALWAYS EAGER - written on every entry)
  constructorCode.push('    // System columns (eager - written on every entry)');
  constructorCode.push('    const alignedCapacity = getCacheAlignedCapacity(requestedCapacity);');
  constructorCode.push('    this._alignedCapacity = alignedCapacity;');
  constructorCode.push('    this.timestamps = new Float64Array(alignedCapacity);');
  constructorCode.push('    this.operations = new Uint8Array(alignedCapacity);');
  constructorCode.push('');
  constructorCode.push('    // Buffer management');
  constructorCode.push('    this.writeIndex = 0;');
  constructorCode.push('    this.capacity = requestedCapacity;');
  constructorCode.push('    this.next = undefined;');

  // Generate symbol declarations and allocator functions for lazy columns
  const symbolDeclarations: string[] = [];
  const allocatorFunctions: string[] = [];
  const getterMethods: string[] = [];

  for (const fieldName of schemaFields) {
    const columnName = `attr_${fieldName}`;
    const { constructorName, bytesPerElement, isBitPacked } = getTypedArrayInfo(schema, fieldName);

    // Symbol declaration
    symbolDeclarations.push(`  const ${columnName}_sym = Symbol('${columnName}');`);
    symbolDeclarations.push(`  columnSymbols['${columnName}'] = ${columnName}_sym;`);

    if (isBitPacked) {
      // Bit-packed boolean allocator: 8 values per byte
      allocatorFunctions.push(`
  function allocate_${columnName}(self) {
    if (self[${columnName}_sym]) return self[${columnName}_sym];
    const capacity = self._alignedCapacity;
    const nullBitmapSize = Math.ceil(capacity / 8);
    const valuesBitmapSize = Math.ceil(capacity / 8);
    const totalSize = nullBitmapSize + valuesBitmapSize;
    const buffer = new ArrayBuffer(totalSize);
    const storage = {
      buffer: buffer,
      nulls: new Uint8Array(buffer, 0, nullBitmapSize),
      values: new Uint8Array(buffer, nullBitmapSize, valuesBitmapSize)
    };
    self[${columnName}_sym] = storage;
    return storage;
  }`);
    } else if (constructorName === 'Array') {
      // String array allocator for category/text columns
      // No ArrayBuffer needed - just JavaScript array
      allocatorFunctions.push(`
  function allocate_${columnName}(self) {
    if (self[${columnName}_sym]) return self[${columnName}_sym];
    const capacity = self._alignedCapacity;
    const nullBitmapSize = Math.ceil(capacity / 8);
    const nullBuffer = new ArrayBuffer(nullBitmapSize);
    const storage = {
      buffer: nullBuffer,
      nulls: new Uint8Array(nullBuffer, 0, nullBitmapSize),
      values: new Array(capacity)
    };
    self[${columnName}_sym] = storage;
    return storage;
  }`);
    } else {
      // Regular TypedArray allocator: one element per array slot
      allocatorFunctions.push(`
  function allocate_${columnName}(self) {
    if (self[${columnName}_sym]) return self[${columnName}_sym];
    const capacity = self._alignedCapacity;
    const nullBitmapSize = Math.ceil(capacity / 8);
    const alignedNullOffset = Math.ceil(nullBitmapSize / ${bytesPerElement}) * ${bytesPerElement};
    const totalSize = alignedNullOffset + capacity * ${bytesPerElement};
    const buffer = new ArrayBuffer(totalSize);
    const storage = {
      buffer: buffer,
      nulls: new Uint8Array(buffer, 0, nullBitmapSize),
      values: new ${constructorName}(buffer, alignedNullOffset, capacity)
    };
    self[${columnName}_sym] = storage;
    return storage;
  }`);
    }

    // Getter methods (native class getter syntax)
    getterMethods.push(`    get ${columnName}_nulls() { return allocate_${columnName}(this).nulls; }`);
    getterMethods.push(`    get ${columnName}_values() { return allocate_${columnName}(this).values; }`);
    getterMethods.push(`    get ${columnName}() { return allocate_${columnName}(this).values; }`);
  }

  // Generate the complete class code
  const classCode = `
(function() {
  'use strict';

  // Cache line size constant
  const CACHE_LINE_SIZE = 64;

  // Cache-aligned capacity calculation
  function getCacheAlignedCapacity(elementCount) {
    const totalBytes = elementCount * 1;
    const alignedBytes = Math.ceil(totalBytes / CACHE_LINE_SIZE) * CACHE_LINE_SIZE;
    return alignedBytes;
  }

  // Symbol registry for lazy column allocation checking
  const columnSymbols = {};

  // Symbol declarations for each lazy column
${symbolDeclarations.join('\n')}

  // Allocator functions for lazy columns
${allocatorFunctions.join('\n')}

  class ${className} {
    constructor(requestedCapacity) {
${constructorCode.join('\n')}
    }

    // Get column values if already allocated, without triggering allocation
    getColumnIfAllocated(columnName) {
      const sym = columnSymbols[columnName];
      return sym ? this[sym]?.values : undefined;
    }

    // Get column nulls bitmap if already allocated, without triggering allocation
    getNullsIfAllocated(columnName) {
      const sym = columnSymbols[columnName];
      return sym ? this[sym]?.nulls : undefined;
    }

    // Lazy column getters
${getterMethods.join('\n')}
  }

  return ${className};
})()
`;

  return classCode;
}

/**
 * Cache for generated ColumnBuffer classes.
 * Key: JSON-serialized schema (stable across identical schemas)
 * Value: Generated class constructor
 */
const classCache = new Map<string, new (capacity: number) => ColumnBuffer>();

/**
 * Create or retrieve a cached ColumnBuffer class for the given schema
 *
 * This is the cold-path function called at module initialization time.
 * The generated class is cached per schema to avoid regenerating for every buffer.
 */
export function getColumnBufferClass(schema: TagAttributeSchema): new (capacity: number) => ColumnBuffer {
  // Create cache key from schema (stable serialization)
  const cacheKey = JSON.stringify(schema);

  // Check cache first
  let BufferClass = classCache.get(cacheKey);

  if (!BufferClass) {
    // Generate class code
    const classCode = generateColumnBufferClass(schema).trim();

    // Compile with new Function()
    // This is safe because we control the code generation
    // eslint-disable-next-line @typescript-eslint/no-implied-eval, no-new-func
    BufferClass = new Function(`return ${classCode}`)() as new (capacity: number) => ColumnBuffer;

    // Cache for future use
    classCache.set(cacheKey, BufferClass);
  }

  return BufferClass;
}

/**
 * Create a ColumnBuffer instance using a generated class
 *
 * Uses direct properties for zero-indirection access.
 */
export function createGeneratedColumnBuffer(schema: TagAttributeSchema, requestedCapacity = 64): ColumnBuffer {
  const BufferClass = getColumnBufferClass(schema);
  return new BufferClass(requestedCapacity);
}
