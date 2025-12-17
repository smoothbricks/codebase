/**
 * Runtime code generation for ColumnBuffer classes with zero-indirection column access.
 *
 * Per specs/01b1_buffer_performance_optimizations.md:
 * - Generate concrete class at module initialization time (cold path)
 * - Direct properties for each column (no lazy getters, no wrapper objects)
 * - ${name}_nulls and ${name}_values share ONE ArrayBuffer per column
 * - System columns (timestamps, operations) as direct Float64Array/Uint8Array
 * - Zero indirection in the hot path
 *
 * WHY Runtime Generation:
 * - V8 hidden class stability: All properties defined in constructor
 * - Monomorphic access: V8 knows exact types at each call site
 * - Inline caching: Property access optimized by V8
 * - Cache alignment: ArrayBuffers aligned to 64-byte boundaries
 *
 * ## Lazy Column Allocation Strategy
 *
 * Lazy columns use a simple undefined-check pattern:
 * 1. Constructor defines `this._col_storage = undefined` for all lazy columns
 * 2. First access to `col_nulls` or `col_values` triggers allocation
 * 3. Allocation creates a shared ArrayBuffer for both nulls and values
 * 4. Subsequent accesses return the cached arrays
 *
 * WHY this approach (vs symbols + closures):
 * - Simpler code, easier to debug
 * - No closure memory overhead
 * - V8 optimizes undefined checks well
 * - Cache-friendly: nulls and values in same ArrayBuffer
 */

import { getSchemaFields, type TagAttributeSchema } from '../schema-types.js';
import { bufferHelpers } from './bufferHelpers.js';
import { type ColumnBuffer, DEFAULT_BUFFER_CAPACITY, type TypedColumnBuffer } from './types.js';

// Re-export for consumers
export { getAlignedCapacity } from './bufferHelpers.js';

/**
 * Extension options for injecting custom code into generated ColumnBuffer classes.
 *
 * WHY: Consumers (like lmao's SpanBuffer) need to add domain-specific properties
 * and methods to the generated class. Adding properties AFTER class instantiation
 * breaks V8 hidden class optimization. This extension mechanism allows injecting
 * code directly into the generated class for optimal performance.
 */
export interface ColumnBufferExtension {
  /**
   * Additional code to add to the constructor (after system columns are initialized).
   * Has access to `this` and any constructorParams.
   * @example
   * ```
   * this.traceId = traceId;
   * this.spanId = spanIdentity;
   * this.children = [];
   * ```
   */
  constructorCode?: string;

  /**
   * Additional methods to add to the class body.
   * @example
   * ```
   * isParentOf(other) {
   *   return this.traceId === other.traceId;
   * }
   * ```
   */
  methods?: string;

  /**
   * Additional code to add before the class definition.
   * Useful for helper functions, symbols, constants, etc.
   * These can reference dependencies by name (they're in scope).
   * @example
   * ```
   * const SPAN_STATE = { PENDING: 0, OK: 1, ERR: 2 };
   * ```
   */
  preamble?: string;

  /**
   * Constructor parameters beyond requestedCapacity.
   * These are added AFTER requestedCapacity in the constructor signature.
   * @example "traceId, spanIdentity, taskContext"
   */
  constructorParams?: string;

  /**
   * Runtime dependencies to inject into the generated class closure.
   * Keys become variable names available in preamble, constructorCode, and methods.
   * Values are the actual runtime objects/functions.
   *
   * NOTE: Dependencies are NOT included in the cache key - they should be
   * stable singleton values (like module-level functions).
   *
   * @example
   * ```
   * dependencies: {
   *   copyThreadIdTo: copyThreadIdTo,
   *   textEncoder: new TextEncoder()
   * }
   * // Then in constructorCode: "copyThreadIdTo(this._identity, 0);"
   * ```
   */
  dependencies?: Record<string, unknown>;
}

/**
 * Storage info for a schema field - determines how values are stored
 */
interface ColumnStorageInfo {
  constructorName: string;
  bytesPerElement: number;
  isBitPacked: boolean;
  schemaType: import('../schema-types.js').SchemaType | undefined;
  enumValues?: readonly string[];
  /**
   * When true, column is allocated eagerly in constructor (no null bitmap).
   * Used for columns written on every entry (like message).
   */
  isEager: boolean;
}

/**
 * Get TypedArray constructor name and byte size for a schema field
 */
function getTypedArrayInfo(schema: TagAttributeSchema, fieldName: string): ColumnStorageInfo {
  const fieldSchema = schema[fieldName];
  const schemaWithMetadata = fieldSchema as import('../schema-types.js').SchemaWithMetadata;
  const schemaType = schemaWithMetadata?.__schema_type;
  const isEager = schemaWithMetadata?.__eager === true;

  // Handle three string types
  if (schemaType === 'enum') {
    const enumValues = schemaWithMetadata.__enum_values;
    const enumCount = enumValues?.length ?? 0;

    // Uint8Array can hold 0-255 indices (256 values total)
    if (enumCount === 0 || enumCount <= 256) {
      return { constructorName: 'Uint8Array', bytesPerElement: 1, isBitPacked: false, schemaType, enumValues, isEager };
    }
    if (enumCount <= 65536) {
      return {
        constructorName: 'Uint16Array',
        bytesPerElement: 2,
        isBitPacked: false,
        schemaType,
        enumValues,
        isEager,
      };
    }
    return { constructorName: 'Uint32Array', bytesPerElement: 4, isBitPacked: false, schemaType, enumValues, isEager };
  }

  if (schemaType === 'category') {
    // Hot path: Store raw JavaScript strings (zero conversion cost)
    // Cold path: Arrow conversion builds sorted dictionary
    return { constructorName: 'Array', bytesPerElement: 0, isBitPacked: false, schemaType, isEager };
  }

  if (schemaType === 'text') {
    // Hot path: Store raw JavaScript strings (zero conversion cost)
    // Cold path: Arrow conversion calculates if dictionary saves space
    return { constructorName: 'Array', bytesPerElement: 0, isBitPacked: false, schemaType, isEager };
  }

  if (schemaType === 'number') {
    return { constructorName: 'Float64Array', bytesPerElement: 8, isBitPacked: false, schemaType, isEager };
  }

  if (schemaType === 'boolean') {
    // Boolean: bit-packed storage (8 booleans per byte) for Arrow compatibility
    return { constructorName: 'Uint8Array', bytesPerElement: 1, isBitPacked: true, schemaType, isEager };
  }

  // Default to Uint32Array
  return { constructorName: 'Uint32Array', bytesPerElement: 4, isBitPacked: false, schemaType, isEager };
}

/**
 * Generate the value assignment code for a setter method.
 *
 * Returns the code that assigns the value to the column storage.
 * The code has access to: `this._${col}_values` or `storage.values`, `pos`, `val`.
 *
 * WHY different strategies per type:
 * - enum: Compile-time lookup map for O(1) string→index conversion (no indexOf at runtime)
 * - boolean: Bit-packed storage using bitwise operations
 * - category/text: Direct string array assignment (no conversion)
 * - number: Direct Float64Array assignment
 */
function generateSetterValueAssignment(info: ColumnStorageInfo, _columnName: string, accessPrefix: string): string {
  const { schemaType, isBitPacked, enumValues } = info;

  if (schemaType === 'enum' && enumValues && enumValues.length > 0) {
    // Enum: Caller (TagWriter) has already converted string→index
    // We receive the numeric index directly - just store it
    return `${accessPrefix}_values[pos] = val;`;
  }

  if (isBitPacked) {
    // Boolean: Bit-packed storage (8 values per byte)
    // Arrow format: 1 = valid/true, 0 = null/false
    return [
      'const byteIdx = pos >>> 3;',
      '      const bitIdx = pos & 7;',
      '      if (val) {',
      `        ${accessPrefix}_values[byteIdx] |= (1 << bitIdx);`,
      '      } else {',
      `        ${accessPrefix}_values[byteIdx] &= ~(1 << bitIdx);`,
      '      }',
    ].join('\n');
  }

  // Direct assignment for category/text/number and default
  return `${accessPrefix}_values[pos] = val;`;
}

/**
 * Generate the null bit setting code.
 *
 * Arrow format: null bitmap uses 1 = valid (not null), 0 = null
 * This sets the bit to 1 (marking the value as valid/present).
 */
function generateNullBitSetting(accessPrefix: string): string {
  return `${accessPrefix}_nulls[pos >>> 3] |= (1 << (pos & 7));`;
}

/**
 * Generate the null bit clearing code (mark value as null).
 *
 * Arrow format: null bitmap uses 1 = valid (not null), 0 = null
 * This clears the bit to 0 (marking the value as null).
 *
 * For lazy columns, we need to trigger allocation first by accessing the getter.
 */
function generateNullBitClearing(accessPrefix: string, _isBitPacked: boolean): string {
  // Access the getter to trigger allocation, then clear the bit
  return `${accessPrefix}_nulls[pos >>> 3] &= ~(1 << (pos & 7));`;
}

/**
 * Get the default value literal for an eager column when null is passed.
 *
 * Since eager columns have no null bitmap, we write a default value.
 * This means consumers cannot distinguish between "explicitly set to 0" vs "null".
 */
function getDefaultValueLiteral(info: ColumnStorageInfo): string {
  const { schemaType, isBitPacked } = info;

  if (isBitPacked || schemaType === 'boolean') {
    return 'false'; // Boolean default
  }
  if (schemaType === 'enum' || schemaType === 'number') {
    return '0'; // Numeric default
  }
  if (schemaType === 'category' || schemaType === 'text') {
    return "''"; // Empty string default
  }
  return '0'; // Fallback
}

/**
 * Generate ColumnBuffer class code as a string
 *
 * Creates a concrete class with:
 * 1. EAGER system columns (timestamps, operations) - allocated in constructor
 * 2. LAZY user attribute columns (X_nulls, X_values) - getters that allocate on first access
 * 3. Shared ArrayBuffer per column (nulls and values use same buffer when allocated)
 * 4. Cache-aligned allocations
 *
 * WHY lazy for user columns:
 * - Many records only use a subset of schema attributes
 * - Lazy allocation saves memory for unused columns
 * - First access triggers allocation of shared ArrayBuffer for both _nulls and _values
 *
 * ## Lazy Column Pattern
 *
 * Instead of symbols + closures, we use a simpler undefined-check pattern:
 *
 * ```javascript
 * // Constructor: Initialize storage as undefined
 * this._userId_storage = undefined;
 *
 * // Getter: Allocate on first access, cache in storage
 * get userId_nulls() {
 *   let s = this._userId_storage;
 *   if (s === undefined) {
 *     s = this._allocate_userId();  // Sets _storage and _nulls/_values
 *   }
 *   return s.nulls;
 * }
 * ```
 *
 * This is simpler than symbols + closures and V8 optimizes undefined checks well.
 */
export function generateColumnBufferClass(
  schema: TagAttributeSchema,
  className = 'GeneratedColumnBuffer',
  extension?: ColumnBufferExtension,
): string {
  // Use getSchemaFields to filter out methods (validate, parse, etc.)
  const schemaFields = getSchemaFields(schema).map(([name]) => name);

  // Generate constructor code for eager system columns
  const constructorCode: string[] = [];

  // System columns (ALWAYS EAGER - written on every entry)
  constructorCode.push('    // System columns (eager - written on every entry)');
  constructorCode.push('    const alignedCapacity = helpers.getAlignedCapacity(requestedCapacity);');
  constructorCode.push('    this._alignedCapacity = alignedCapacity;');
  constructorCode.push('    this._timestamps = new BigInt64Array(alignedCapacity);');
  constructorCode.push('    this._operations = new Uint8Array(alignedCapacity);');
  constructorCode.push('');
  constructorCode.push('    // Buffer management (system properties use _ prefix)');
  constructorCode.push('    // NOTE: _writeIndex is tracked by ColumnWriter, not ColumnBuffer');
  constructorCode.push('    this._capacity = requestedCapacity;');
  constructorCode.push('    this._next = undefined;');

  // Lazy column storage initialization and allocator methods
  const lazyStorageInit: string[] = [];
  const allocatorMethods: string[] = [];
  const getterMethods: string[] = [];
  const setterMethods: string[] = [];
  // Eager column constructor code (allocated immediately, no null bitmap)
  const eagerColumnConstructorCode: string[] = [];
  // Track which columns are lazy for getColumnIfAllocated
  const lazyColumnNames: string[] = [];

  for (const fieldName of schemaFields) {
    const columnName = fieldName; // User columns have no prefix
    const storageInfo = getTypedArrayInfo(schema, fieldName);
    const { constructorName, bytesPerElement, isBitPacked, isEager } = storageInfo;

    if (isEager) {
      // EAGER column: Allocate in constructor, no null bitmap
      if (constructorName === 'Array') {
        eagerColumnConstructorCode.push(`    // Eager column: ${columnName} (no null bitmap)`);
        eagerColumnConstructorCode.push(`    this._${columnName}_values = new Array(alignedCapacity);`);
      } else if (isBitPacked) {
        eagerColumnConstructorCode.push(`    // Eager column: ${columnName} (bit-packed, no null bitmap)`);
        eagerColumnConstructorCode.push(
          `    this._${columnName}_values = new Uint8Array((alignedCapacity + 7) >>> 3);`,
        );
      } else {
        eagerColumnConstructorCode.push(`    // Eager column: ${columnName} (no null bitmap)`);
        eagerColumnConstructorCode.push(`    this._${columnName}_values = new ${constructorName}(alignedCapacity);`);
      }
      // Eager columns still need getters to expose _values as values (public API)
      getterMethods.push(`    get ${columnName}_values() { return this._${columnName}_values; }`);
      // Skip lazy column setup for eager columns
      continue;
    }

    // LAZY column: Initialize storage as undefined, allocate on first access
    lazyColumnNames.push(columnName);
    lazyStorageInit.push(`    this._${columnName}_nulls = undefined;`);
    lazyStorageInit.push(`    this._${columnName}_values = undefined;`);

    // Generate allocator method for this column
    if (isBitPacked) {
      // Bit-packed boolean: 8 values per byte for both nulls and values
      allocatorMethods.push(`
    _allocate_${columnName}() {
      const cap = this._alignedCapacity;
      const bitmapSize = (cap + 7) >>> 3;  // Bits to bytes
      const buf = new ArrayBuffer(bitmapSize + bitmapSize);  // nulls + values
      this._${columnName}_nulls = new Uint8Array(buf, 0, bitmapSize);
      this._${columnName}_values = new Uint8Array(buf, bitmapSize, bitmapSize);
    }`);
    } else if (constructorName === 'Array') {
      // String array: nulls is Uint8Array, values is JS Array
      allocatorMethods.push(`
    _allocate_${columnName}() {
      const cap = this._alignedCapacity;
      const nullSize = (cap + 7) >>> 3;
      this._${columnName}_nulls = new Uint8Array(nullSize);
      this._${columnName}_values = new Array(cap);
    }`);
    } else {
      // TypedArray: shared ArrayBuffer with aligned offset
      // Layout: [null bitmap | padding | values]
      allocatorMethods.push(`
    _allocate_${columnName}() {
      const cap = this._alignedCapacity;
      const nullSize = (cap + 7) >>> 3;  // Bits to bytes
      // Align values offset to element size (${bytesPerElement} bytes)
      const alignedOffset = ((nullSize + ${bytesPerElement - 1}) >>> ${Math.log2(bytesPerElement)}) << ${Math.log2(bytesPerElement)};
      const buf = new ArrayBuffer(alignedOffset + cap * ${bytesPerElement});
      this._${columnName}_nulls = new Uint8Array(buf, 0, nullSize);
      this._${columnName}_values = new ${constructorName}(buf, alignedOffset, cap);
    }`);
    }

    // Getters trigger allocation on first access
    getterMethods.push(`    get ${columnName}_nulls() {
      let v = this._${columnName}_nulls;
      if (v === undefined) {
        this._allocate_${columnName}();
        v = this._${columnName}_nulls;
      }
      return v;
    }`);
    getterMethods.push(`    get ${columnName}_values() {
      let v = this._${columnName}_values;
      if (v === undefined) {
        this._allocate_${columnName}();
        v = this._${columnName}_values;
      }
      return v;
    }`);
  }

  // Re-iterate for setter methods
  for (const fieldName of schemaFields) {
    const columnName = fieldName;
    const storageInfo = getTypedArrayInfo(schema, fieldName);
    const { isEager, isBitPacked } = storageInfo;

    if (isEager) {
      // EAGER column setter: Direct property access, no null bitmap
      // Null values write default (0, '', false) - no way to distinguish from actual 0/''
      const valueAssignment = generateSetterValueAssignment(storageInfo, columnName, `this._${columnName}`);
      const defaultValue = getDefaultValueLiteral(storageInfo);

      setterMethods.push(`    ${columnName}(pos, val) {
      if (val == null) val = ${defaultValue};
      ${valueAssignment}
      return this;
    }`);
    } else {
      // LAZY column setter: Handle null values properly
      // - null/undefined: allocate column, clear null bit (mark as null)
      // - valid value: allocate column, set null bit, write value
      const valueAssignment = generateSetterValueAssignment(storageInfo, columnName, `this.${columnName}`);
      const nullBitSetting = generateNullBitSetting(`this.${columnName}`);
      const nullBitClearing = generateNullBitClearing(`this.${columnName}`, isBitPacked);

      setterMethods.push(`    ${columnName}(pos, val) {
      if (val == null) {
        ${nullBitClearing}
      } else {
        ${valueAssignment}
        ${nullBitSetting}
      }
      return this;
    }`);
    }
  }

  // Build constructor signature with optional extension params
  const constructorSignature = extension?.constructorParams
    ? `requestedCapacity, ${extension.constructorParams}`
    : 'requestedCapacity';

  // Add lazy column storage initialization
  if (lazyStorageInit.length > 0) {
    constructorCode.push('');
    constructorCode.push('    // Lazy column storage (undefined until first access)');
    constructorCode.push(...lazyStorageInit);
  }

  // Add eager column initialization
  if (eagerColumnConstructorCode.length > 0) {
    constructorCode.push('');
    constructorCode.push(...eagerColumnConstructorCode);
  }

  // Add extension constructor code if provided
  if (extension?.constructorCode) {
    constructorCode.push('');
    constructorCode.push('    // Extension constructor code');
    const extensionLines = extension.constructorCode
      .trim()
      .split('\n')
      .map((line) => '    ' + line.trim());
    constructorCode.push(...extensionLines);
  }

  // Build extension methods if provided
  const extensionMethods = extension?.methods
    ? `
    // Extension methods
${extension.methods
  .trim()
  .split('\n')
  .map((line) => '    ' + line)
  .join('\n')}`
    : '';

  // Build preamble if provided
  const preambleCode = extension?.preamble ? `\n  // Extension preamble\n${extension.preamble}\n` : '';

  // Generate lazy column names array for runtime inspection
  const lazyColumnNamesLiteral = JSON.stringify(lazyColumnNames);

  // Generate the complete class code
  const classCode = `
  'use strict';

  // Lazy column names for runtime inspection
  const lazyColumnNames = ${lazyColumnNamesLiteral};
${preambleCode}
  class ${className} {
    constructor(${constructorSignature}) {
${constructorCode.join('\n')}
    }

    // Get column values if already allocated, without triggering allocation
    getColumnIfAllocated(columnName) {
      const key = '_' + columnName + '_values';
      return this[key];
    }

    // Get column nulls bitmap if already allocated, without triggering allocation
    getNullsIfAllocated(columnName) {
      const key = '_' + columnName + '_nulls';
      return this[key];
    }

    // Column allocator methods (lazy columns only)
${allocatorMethods.join('\n')}

    // Lazy column getters (trigger allocation on first access)
${getterMethods.join('\n')}

    // Column setter methods: buffer.column(position, value) => this
${setterMethods.join('\n')}
${extensionMethods}
  }

  return ${className};
`;

  return classCode;
}

/**
 * Cache for generated ColumnBuffer classes.
 * Key: JSON-serialized schema + extension (stable across identical configurations)
 * Value: Generated class constructor
 */
const classCache = new Map<string, new (capacity: number, ...args: unknown[]) => ColumnBuffer>();

/**
 * Create a stable cache key from schema and extension options.
 * Both are serialized to JSON for stable key generation.
 */
function createCacheKey(schema: TagAttributeSchema, extension?: ColumnBufferExtension): string {
  if (!extension) {
    return JSON.stringify(schema);
  }
  return JSON.stringify({ schema, extension });
}

/**
 * Create or retrieve a cached ColumnBuffer class for the given schema and extension
 *
 * This is the cold-path function called at module initialization time.
 * The generated class is cached per schema+extension to avoid regenerating for every buffer.
 *
 * @param schema - The tag attribute schema defining columns
 * @param extension - Optional extension for injecting constructor code, methods, etc.
 * @returns The generated class constructor
 */
export function getColumnBufferClass<S extends TagAttributeSchema>(
  schema: S,
  extension?: ColumnBufferExtension,
): new (
  capacity: number,
  ...args: unknown[]
) => TypedColumnBuffer<S> {
  // Create cache key from schema and extension (stable serialization)
  // NOTE: dependencies are NOT part of cache key - they should be stable singletons
  const cacheKey = createCacheKey(schema, extension);

  // Check cache first
  let BufferClass = classCache.get(cacheKey);

  if (!BufferClass) {
    // Generate class code
    const classCode = generateColumnBufferClass(schema, 'GeneratedColumnBuffer', extension).trim();

    // Compile with new Function()
    // Always inject bufferHelpers; extension dependencies are merged in
    // This is safe because we control the code generation
    const depNames = ['helpers'];
    const depValues: unknown[] = [bufferHelpers];

    if (extension?.dependencies && Object.keys(extension.dependencies).length > 0) {
      for (const [name, value] of Object.entries(extension.dependencies)) {
        depNames.push(name);
        depValues.push(value);
      }
    }

    // Create function that takes dependencies as parameters and returns the class
    // The classCode is the body of the function, ending with 'return ClassName;'
    // eslint-disable-next-line @typescript-eslint/no-implied-eval, no-new-func
    const factory = new Function(...depNames, classCode) as (
      ...args: unknown[]
    ) => new (
      capacity: number,
      ...args: unknown[]
    ) => ColumnBuffer;
    BufferClass = factory(...depValues);

    // Cache for future use
    classCache.set(cacheKey, BufferClass);
  }

  // Cast is safe because the generated class has all the typed setters and properties
  // that TypedColumnBuffer<S> requires - we generate them from the same schema
  return BufferClass as unknown as new (
    capacity: number,
    ...args: unknown[]
  ) => TypedColumnBuffer<S>;
}

/**
 * Create a ColumnBuffer instance using a generated class
 *
 * Uses direct properties for zero-indirection access.
 *
 * @param schema - The tag attribute schema defining columns
 * @param requestedCapacity - Initial buffer capacity (defaults to DEFAULT_BUFFER_CAPACITY)
 * @param extension - Optional extension for injecting constructor code, methods, etc.
 * @param constructorArgs - Additional constructor arguments (passed to extension constructor code)
 * @returns A new ColumnBuffer instance with typed setters based on schema
 */
export function createGeneratedColumnBuffer<S extends TagAttributeSchema>(
  schema: S,
  requestedCapacity = DEFAULT_BUFFER_CAPACITY,
  extension?: ColumnBufferExtension,
  ...constructorArgs: unknown[]
): TypedColumnBuffer<S> {
  const BufferClass = getColumnBufferClass(schema, extension);
  return new BufferClass(requestedCapacity, ...constructorArgs);
}
