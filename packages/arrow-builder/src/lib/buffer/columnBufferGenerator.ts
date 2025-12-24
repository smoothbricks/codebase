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
 * Lazy columns use inline allocation in the _nulls getter:
 * 1. Constructor defines `this._col_nulls = undefined` and `this._col_values = undefined`
 * 2. First access to `col_nulls` getter checks undefined, allocates BOTH arrays
 * 3. `col_values` getter just returns `this._col_values` (already allocated by nulls access)
 *
 * WHY inline allocation in _nulls getter:
 * - Accessing nulls always happens before values (to set valid bit)
 * - Single allocation path, no separate allocator methods
 * - V8 optimizes undefined checks well
 * - Cache-friendly: nulls and values in same ArrayBuffer
 */

import { type ColumnSchema, isColumnSchema, type SchemaType, type SchemaWithMetadata } from '../schema-types.js';
import { bufferHelpers } from './bufferHelpers.js';
import { type AnyColumnBuffer, type ColumnBuffer, DEFAULT_BUFFER_CAPACITY } from './types.js';

// Re-export types for consumers
export type { AnyColumnBuffer, ColumnBuffer };

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
   * this._trace_id = traceId;
   * this._span_id = spanIdentity;
   * this._children = [];
   * ```
   */
  constructorCode?: string;

  /**
   * Additional methods to add to the class body.
   * @example
   * ```
   * isParentOf(other) {
   *   return this._trace_id === other._trace_id;
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
  schemaType: SchemaType | undefined;
  enumValues?: readonly string[];
  /** When true, column is allocated eagerly in constructor (no null bitmap). */
  isEager: boolean;
}

/**
 * Get TypedArray constructor name and byte size for a schema field
 */
function getTypedArrayInfo(schema: ColumnSchema, fieldName: string): ColumnStorageInfo {
  const fieldSchema = schema.fields[fieldName];
  const schemaWithMetadata = fieldSchema as SchemaWithMetadata;
  const schemaType = schemaWithMetadata?.__schema_type;
  const isEager = schemaWithMetadata?.__eager === true;

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
    // >65536 values: Uint32Array (4 bytes)
    return { constructorName: 'Uint32Array', bytesPerElement: 4, isBitPacked: false, schemaType, enumValues, isEager };
  }

  if (schemaType === 'category') {
    return { constructorName: 'Array', bytesPerElement: 0, isBitPacked: false, schemaType, isEager };
  }

  if (schemaType === 'text') {
    return { constructorName: 'Array', bytesPerElement: 0, isBitPacked: false, schemaType, isEager };
  }

  if (schemaType === 'number') {
    return { constructorName: 'Float64Array', bytesPerElement: 8, isBitPacked: false, schemaType, isEager };
  }

  if (schemaType === 'boolean') {
    return { constructorName: 'Uint8Array', bytesPerElement: 1, isBitPacked: true, schemaType, isEager };
  }

  if (schemaType === 'bigUint64') {
    return { constructorName: 'BigUint64Array', bytesPerElement: 8, isBitPacked: false, schemaType, isEager };
  }

  // Default to Uint32Array for unknown types
  return { constructorName: 'Uint32Array', bytesPerElement: 4, isBitPacked: false, schemaType, isEager };
}

/**
 * Generate inline allocation code for a lazy column's _nulls getter.
 * This allocates BOTH nulls and values arrays in a shared ArrayBuffer.
 */
function generateInlineAllocation(columnName: string, info: ColumnStorageInfo): string {
  const { constructorName, bytesPerElement, isBitPacked } = info;

  if (isBitPacked) {
    // Bit-packed boolean: nulls and values both use 1 bit per element
    return `const cap = this._alignedCapacity;
        const bitmapSize = (cap + 7) >>> 3;
        const buf = new ArrayBuffer(bitmapSize + bitmapSize);
        v = this._${columnName}_nulls = new Uint8Array(buf, 0, bitmapSize);
        this._${columnName}_values = new Uint8Array(buf, bitmapSize, bitmapSize);`;
  }

  if (constructorName === 'Array') {
    // String array: nulls is bit-packed Uint8Array, values is JS Array (can't share ArrayBuffer)
    return `const cap = this._alignedCapacity;
        const nullSize = (cap + 7) >>> 3;
        v = this._${columnName}_nulls = new Uint8Array(nullSize);
        this._${columnName}_values = new Array(cap);`;
  }

  // TypedArray: shared ArrayBuffer with aligned offset
  const shift = Math.log2(bytesPerElement);
  return `const cap = this._alignedCapacity;
        const nullSize = (cap + 7) >>> 3;
        const alignedOffset = ((nullSize + ${bytesPerElement - 1}) >>> ${shift}) << ${shift};
        const buf = new ArrayBuffer(alignedOffset + cap * ${bytesPerElement});
        v = this._${columnName}_nulls = new Uint8Array(buf, 0, nullSize);
        this._${columnName}_values = new ${constructorName}(buf, alignedOffset, cap);`;
}

/** Generate value assignment code for a setter method. */
function generateSetterValueAssignment(info: ColumnStorageInfo, accessPrefix: string): string {
  const { schemaType, isBitPacked, enumValues } = info;

  if (schemaType === 'enum' && enumValues && enumValues.length > 0) {
    return `${accessPrefix}_values[pos] = val;`;
  }

  if (isBitPacked) {
    return `const byteIdx = pos >>> 3;
      const bitIdx = pos & 7;
      if (val) { ${accessPrefix}_values[byteIdx] |= (1 << bitIdx); }
      else { ${accessPrefix}_values[byteIdx] &= ~(1 << bitIdx); }`;
  }

  return `${accessPrefix}_values[pos] = val;`;
}

/** Generate null bit setting code (mark value as valid). */
function generateNullBitSetting(accessPrefix: string): string {
  return `${accessPrefix}_nulls[pos >>> 3] |= (1 << (pos & 7));`;
}

/** Generate null bit clearing code (mark value as null). */
function generateNullBitClearing(accessPrefix: string): string {
  return `${accessPrefix}_nulls[pos >>> 3] &= ~(1 << (pos & 7));`;
}

/** Get the default value literal for an eager column when null is passed. */
function getDefaultValueLiteral(info: ColumnStorageInfo): string {
  const { schemaType, isBitPacked } = info;
  if (isBitPacked || schemaType === 'boolean') return 'false';
  if (schemaType === 'enum' || schemaType === 'number') return '0';
  if (schemaType === 'bigUint64') return '0n';
  if (schemaType === 'category' || schemaType === 'text') return "''";
  return '0';
}

/**
 * Generate ColumnBuffer class code as a string.
 *
 * Creates a class with:
 * 1. System columns (timestamps, operations) - eager, allocated in constructor
 * 2. Lazy columns - _nulls getter allocates both nulls and values on first access
 * 3. Eager columns - allocated in constructor, no null bitmap
 */
export function generateColumnBufferClass(
  schema: ColumnSchema,
  className = 'GeneratedColumnBuffer',
  extension?: ColumnBufferExtension,
): string {
  // Schema should always be a ColumnSchema instance (LogSchema extends ColumnSchema)
  if (!isColumnSchema(schema)) {
    throw new Error(
      `Schema must be a ColumnSchema instance. Got: ${typeof schema}. ` +
        'This should not happen - schemas are wrapped at API boundaries.',
    );
  }

  const schemaFields = schema.fieldNames;

  // Buffer management
  const constructorCode: string[] = [
    '    const alignedCapacity = helpers.getAlignedCapacity(requestedCapacity);',
    '    this._alignedCapacity = alignedCapacity;',
    '    this._capacity = requestedCapacity;',
    '    this._overflow = undefined;',
  ];

  const getterMethods: string[] = [];
  const setterMethods: string[] = [];
  const lazyColumnNames: string[] = [];

  for (const fieldName of schemaFields) {
    const columnName = fieldName;
    const storageInfo = getTypedArrayInfo(schema, fieldName);
    const { constructorName, isBitPacked, isEager } = storageInfo;

    if (isEager) {
      // Eager column: allocate in constructor, no null bitmap
      if (constructorName === 'Array') {
        constructorCode.push(`    this._${columnName}_values = new Array(alignedCapacity);`);
      } else if (isBitPacked) {
        constructorCode.push(`    this._${columnName}_values = new Uint8Array((alignedCapacity + 7) >>> 3);`);
      } else {
        constructorCode.push(`    this._${columnName}_values = new ${constructorName}(alignedCapacity);`);
      }
      // Getter just returns the backing property
      getterMethods.push(`    get ${columnName}_values() { return this._${columnName}_values; }`);
      // Alias getter for direct access (used by expose())
      getterMethods.push(`    get ${columnName}() { return this.${columnName}_values; }`);

      // For enums, store the enum values array
      if (storageInfo.schemaType === 'enum' && storageInfo.enumValues) {
        constructorCode.push(`    this.${columnName}_enumValues = ${JSON.stringify(storageInfo.enumValues)};`);
      }

      // Eager setter: write default for null
      const valueAssignment = generateSetterValueAssignment(storageInfo, `this._${columnName}`);
      const defaultValue = getDefaultValueLiteral(storageInfo);
      setterMethods.push(`    ${columnName}(pos, val) {
      if (val == null) val = ${defaultValue};
      ${valueAssignment}
      return this;
    }`);
      continue;
    }

    // Lazy column: initialize as undefined
    lazyColumnNames.push(columnName);
    constructorCode.push(`    this._${columnName}_nulls = undefined;`);
    constructorCode.push(`    this._${columnName}_values = undefined;`);

    // For enums, store the enum values array
    if (storageInfo.schemaType === 'enum' && storageInfo.enumValues) {
      constructorCode.push(`    this.${columnName}_enumValues = ${JSON.stringify(storageInfo.enumValues)};`);
    }

    // _nulls getter: allocates BOTH nulls and values on first access
    const allocationCode = generateInlineAllocation(columnName, storageInfo);
    getterMethods.push(`    get ${columnName}_nulls() {
      let v = this._${columnName}_nulls;
      if (v === undefined) {
        ${allocationCode}
      }
      return v;
    }`);

    // _values getter: triggers allocation via _nulls if needed, then returns values
    getterMethods.push(`    get ${columnName}_values() {
      if (this._${columnName}_values === undefined) this.${columnName}_nulls;
      return this._${columnName}_values;
    }`);

    // Alias getter for direct access (used by expose())
    getterMethods.push(`    get ${columnName}() { return this.${columnName}_values; }`);

    // Lazy setter: handle null by clearing bit, valid value by setting bit
    const valueAssignment = generateSetterValueAssignment(storageInfo, `this.${columnName}`);
    const nullBitSetting = generateNullBitSetting(`this.${columnName}`);
    const nullBitClearing = generateNullBitClearing(`this.${columnName}`);
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

  // Extension constructor code
  if (extension?.constructorCode) {
    const extensionLines = extension.constructorCode
      .trim()
      .split('\n')
      .map((line) => `    ${line.trim()}`);
    constructorCode.push(...extensionLines);
  }

  const constructorSignature = extension?.constructorParams
    ? `requestedCapacity, ${extension.constructorParams}`
    : 'requestedCapacity';

  const extensionMethods = extension?.methods
    ? `\n${extension.methods
        .trim()
        .split('\n')
        .map((line) => `    ${line}`)
        .join('\n')}`
    : '';

  const preambleCode = extension?.preamble ? `\n${extension.preamble}\n` : '';

  // Custom inspect to avoid dumping huge TypedArrays in test output
  const inspectMethod = `
    [Symbol.for('nodejs.util.inspect.custom')](depth, opts) {
      return \`${className} { _writeIndex: \${this._writeIndex}, _capacity: \${this._capacity}, trace_id: \${this.trace_id ?? 'N/A'} }\`;
    }`;

  return `'use strict';
const lazyColumnNames = ${JSON.stringify(lazyColumnNames)};
class ${className} {
  constructor(${constructorSignature}) {
${preambleCode}
${constructorCode.join('\n')}
  }
  getColumnIfAllocated(columnName) { return this[\`_\${columnName}_values\`]; }
  getNullsIfAllocated(columnName) { return this[\`_\${columnName}_nulls\`]; }
${getterMethods.join('\n')}
${setterMethods.join('\n')}
${inspectMethod}
${extensionMethods}
}
return ${className};
`;
}

/**
 * Cache for generated ColumnBuffer classes.
 */
const classCache = new Map<string, new (capacity: number, ...args: unknown[]) => AnyColumnBuffer>();

function createCacheKey(schema: ColumnSchema, extension?: ColumnBufferExtension): string {
  if (!extension) return JSON.stringify(schema.fields);
  return JSON.stringify({ schema, extension });
}

/**
 * Create or retrieve a cached ColumnBuffer class for the given schema and extension.
 */
export function getColumnBufferClass<S extends ColumnSchema>(
  schema: S,
  extension?: ColumnBufferExtension,
): new (
  capacity: number,
  ...args: unknown[]
) => ColumnBuffer<S> {
  const cacheKey = createCacheKey(schema, extension);
  let BufferClass = classCache.get(cacheKey);

  if (!BufferClass) {
    const classCode = generateColumnBufferClass(schema, 'GeneratedColumnBuffer', extension).trim();

    const depNames = ['helpers'];
    const depValues: unknown[] = [bufferHelpers];

    if (extension?.dependencies && Object.keys(extension.dependencies).length > 0) {
      for (const [name, value] of Object.entries(extension.dependencies)) {
        depNames.push(name);
        depValues.push(value);
      }
    }

    // eslint-disable-next-line @typescript-eslint/no-implied-eval, no-new-func
    const factory = new Function(...depNames, classCode) as (
      ...args: unknown[]
    ) => new (
      capacity: number,
      ...args: unknown[]
    ) => AnyColumnBuffer;
    BufferClass = factory(...depValues);
    classCache.set(cacheKey, BufferClass);
  }

  return BufferClass as unknown as new (
    capacity: number,
    ...args: unknown[]
  ) => ColumnBuffer<S>;
}

/**
 * Create a ColumnBuffer instance using a generated class.
 */
export function createGeneratedColumnBuffer<S extends ColumnSchema>(
  schema: S,
  requestedCapacity = DEFAULT_BUFFER_CAPACITY,
  extension?: ColumnBufferExtension,
  ...constructorArgs: unknown[]
): ColumnBuffer<S> {
  const BufferClass = getColumnBufferClass(schema, extension);
  return new BufferClass(requestedCapacity, ...constructorArgs);
}
