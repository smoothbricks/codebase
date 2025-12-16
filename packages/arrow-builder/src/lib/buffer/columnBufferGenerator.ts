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
 */

import type { TagAttributeSchema } from '../schema-types.js';
import { bufferHelpers } from './bufferHelpers.js';
import type { ColumnBuffer } from './types.js';

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
 * 2. LAZY user attribute columns (X_nulls, X_values) - getters that allocate on first access
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
export function generateColumnBufferClass(
  schema: TagAttributeSchema,
  className = 'GeneratedColumnBuffer',
  extension?: ColumnBufferExtension,
): string {
  const schemaFields = Object.keys(schema);

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
  constructorCode.push('    this._writeIndex = 0;');
  constructorCode.push('    this._capacity = requestedCapacity;');
  constructorCode.push('    this._next = undefined;');

  // Generate symbol declarations and allocator functions for lazy columns
  const symbolDeclarations: string[] = [];
  const allocatorFunctions: string[] = [];
  const getterMethods: string[] = [];

  for (const fieldName of schemaFields) {
    const columnName = fieldName; // User columns have no prefix
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

  // Build constructor signature with optional extension params
  const constructorSignature = extension?.constructorParams
    ? `requestedCapacity, ${extension.constructorParams}`
    : 'requestedCapacity';

  // Add extension constructor code if provided
  if (extension?.constructorCode) {
    constructorCode.push('');
    constructorCode.push('    // Extension constructor code');
    // Indent extension code properly (each line gets 4 spaces)
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

  // Generate the complete class code
  // Note: 'helpers' is injected by getColumnBufferClass() via new Function('helpers', code)
  // The generated code is NOT an IIFE - it's the body of a function that receives helpers
  const classCode = `
  'use strict';

  // Symbol registry for lazy column allocation checking
  const columnSymbols = {};

  // Symbol declarations for each lazy column
${symbolDeclarations.join('\n')}

  // Allocator functions for lazy columns
${allocatorFunctions.join('\n')}
${preambleCode}
  class ${className} {
    constructor(${constructorSignature}) {
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
export function getColumnBufferClass(
  schema: TagAttributeSchema,
  extension?: ColumnBufferExtension,
): new (
  capacity: number,
  ...args: unknown[]
) => ColumnBuffer {
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

  return BufferClass;
}

/**
 * Create a ColumnBuffer instance using a generated class
 *
 * Uses direct properties for zero-indirection access.
 *
 * @param schema - The tag attribute schema defining columns
 * @param requestedCapacity - Initial buffer capacity (defaults to 64)
 * @param extension - Optional extension for injecting constructor code, methods, etc.
 * @param constructorArgs - Additional constructor arguments (passed to extension constructor code)
 * @returns A new ColumnBuffer instance
 */
export function createGeneratedColumnBuffer(
  schema: TagAttributeSchema,
  requestedCapacity = 64,
  extension?: ColumnBufferExtension,
  ...constructorArgs: unknown[]
): ColumnBuffer {
  const BufferClass = getColumnBufferClass(schema, extension);
  return new BufferClass(requestedCapacity, ...constructorArgs);
}
