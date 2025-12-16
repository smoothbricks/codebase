/**
 * Library Integration Pattern with Prefix Remapping
 *
 * WHY: Libraries need to write clean, domain-focused code (ctx.tag.status(200))
 * while avoiding naming conflicts when composed with other libraries.
 *
 * HOW: At composition time (cold path), we generate a remapped SpanLogger class
 * that exposes clean method names but writes to prefixed buffer columns.
 *
 * Per specs/01e_library_integration_pattern.md:
 * - Libraries define clean schemas without prefixes (status, method, url)
 * - Prefixing happens at composition time (http_status, http_method, http_url)
 * - Zero hot path overhead - all remapping done via code generation at module creation
 *
 * WHAT: This module provides:
 * - prefixSchema() - Rename schema fields with prefix
 * - createPrefixMapping() - Build clean→prefixed name mapping
 * - generateRemappedSpanLoggerClass() - Generate class with clean methods, prefixed writes
 * - createLibraryModule() - Library author API for defining modules
 * - moduleContextFactory() - Application API for composing libraries with prefixes
 */

import type { ModuleContextBuilder, RequestContext, SpanContext, TaskFunction } from './lmao.js';
import { createModuleContext } from './lmao.js';
import { S } from './schema/builder.js';
import type { FeatureFlagSchema } from './schema/defineFeatureFlags.js';
import { getEnumValues, getLmaoSchemaType } from './schema/typeGuards.js';
import type { TagAttributeSchema } from './schema/types.js';
import { getSchemaFields } from './schema/types.js';

/**
 * Library operation definition
 * Maps operation name to implementation function and span name
 */
export interface LibraryOperation<
  Args extends unknown[],
  Result,
  T extends TagAttributeSchema,
  FF extends FeatureFlagSchema,
  Env = Record<string, unknown>,
> {
  fn: TaskFunction<Args, Result, T, FF, Env>;
  spanName: string;
}

/**
 * Library module with operations
 * Created by library authors, consumed by applications
 */
export interface LibraryModule<
  T extends TagAttributeSchema,
  FF extends FeatureFlagSchema,
  Env = Record<string, unknown>,
  Ops extends Record<string, LibraryOperation<any[], any, T, FF, Env>> = Record<
    string,
    LibraryOperation<any[], any, T, FF, Env>
  >,
> {
  schema: T;
  operations: Ops;
  task<Args extends unknown[], Result>(
    name: string,
    fn: TaskFunction<Args, Result, T, FF, Env>,
  ): (ctx: RequestContext<FF, Env>, ...args: Args) => Promise<Result>;
}

/**
 * Prefix a tag attribute schema
 * Renames all fields with a prefix to avoid conflicts
 *
 * Example:
 * - Input: { status: S.number(), method: S.enum(['GET', 'POST']) }
 * - Prefix: 'http'
 * - Output: { http_status: S.number(), http_method: S.enum(['GET', 'POST']) }
 */
export function prefixSchema<T extends TagAttributeSchema>(schema: T, prefix: string): TagAttributeSchema {
  const prefixedSchema: TagAttributeSchema = {};

  // Get schema fields, excluding methods added by defineTagAttributes
  for (const [fieldName, fieldSchema] of getSchemaFields(schema)) {
    const prefixedName = `${prefix}_${fieldName}`;
    prefixedSchema[prefixedName] = fieldSchema;
  }

  return prefixedSchema;
}

/**
 * Prefix mapping: maps clean names to prefixed column names
 *
 * WHY: Library authors write ctx.tag.status() but buffer column is attr_http_status
 * HOW: At module creation time, we build this mapping once (cold path)
 *
 * @example
 * // Input: schema = { status: S.number(), method: S.enum([...]) }, prefix = 'http'
 * // Output: { status: 'http_status', method: 'http_method' }
 */
export interface PrefixMapping {
  [cleanName: string]: string; // cleanName -> prefixedColumnName (without attr_ prefix)
}

/**
 * Create a mapping from clean field names to prefixed column names
 *
 * WHY: This mapping is used by generateRemappedSpanLoggerClass to create methods
 * that write to prefixed columns but expose clean API names.
 *
 * @param schema - Clean schema (without prefix)
 * @param prefix - Prefix to apply (e.g., 'http')
 * @returns Mapping from clean name to prefixed name
 *
 * @example
 * const mapping = createPrefixMapping({ status: S.number() }, 'http');
 * // Returns: { status: 'http_status' }
 */
export function createPrefixMapping<T extends TagAttributeSchema>(schema: T, prefix: string): PrefixMapping {
  const mapping: PrefixMapping = {};

  for (const [fieldName] of getSchemaFields(schema)) {
    mapping[fieldName] = `${prefix}_${fieldName}`;
  }

  return mapping;
}

/**
 * Generate enum value mapping code for remapped class
 * Creates a switch-case statement for compile-time enum mapping
 *
 * WHY: Enums need compile-time mapping to Uint8 values for 1-byte storage
 */
function generateEnumMapping(fieldName: string, enumValues: readonly string[]): string {
  const cases = enumValues.map((value, index) => `    case ${JSON.stringify(value)}: return ${index};`).join('\n');

  return `
  function getEnumIndex_${fieldName}(value) {
    switch(value) {
${cases}
      default: return 0;
    }
  }`;
}

/**
 * Generate attribute writer method for remapped SpanLogger
 *
 * WHY: Each method must write to the PREFIXED column while being called by CLEAN name
 * HOW: Method name is cleanName, but buffer access uses prefixedColumnName
 *
 * @param cleanName - Clean method name (e.g., 'status')
 * @param prefixedColumnName - Prefixed column name (e.g., 'http_status')
 * @param schema - Field schema for type-specific handling
 * @param hasEnumMapping - Whether enum mapping function exists
 */
function generateRemappedAttributeWriter(
  cleanName: string,
  prefixedColumnName: string,
  schema: unknown,
  hasEnumMapping: boolean,
): string {
  const columnName = `attr_${prefixedColumnName}`;
  const lmaoType = getLmaoSchemaType(schema);

  // For enums, use pre-generated mapping function (uses cleanName for function)
  if (lmaoType === 'enum' && hasEnumMapping) {
    return `
    ${cleanName}(value) {
      // ALWAYS write to row 0 (span-start) - overwrite semantics
      // Clean method name: ${cleanName}, writes to prefixed column: ${columnName}
      const idx = 0;
      const enumIndex = getEnumIndex_${cleanName}(value);
      this._buffer.${columnName}_values[idx] = enumIndex;

      // Mark as non-null (bit 0 for idx 0)
      this._buffer.${columnName}_nulls[0] |= 1;

      return this;
    }`;
  }

  // For categories, use string interning
  if (lmaoType === 'category') {
    return `
    ${cleanName}(value) {
      // ALWAYS write to row 0 (span-start) - overwrite semantics
      // Clean method name: ${cleanName}, writes to prefixed column: ${columnName}
      const idx = 0;
      this._buffer.${columnName}_values[idx] = this._categoryInterner.intern(value);

      // Mark as non-null (bit 0 for idx 0)
      this._buffer.${columnName}_nulls[0] |= 1;

      return this;
    }`;
  }

  // For text, use raw storage with null/undefined handling
  if (lmaoType === 'text') {
    return `
    ${cleanName}(value) {
      // ALWAYS write to row 0 (span-start) - overwrite semantics
      // Clean method name: ${cleanName}, writes to prefixed column: ${columnName}
      const idx = 0;

      // Handle null/undefined: clear null bitmap bit and return early
      if (value === null || value === undefined) {
        this._buffer.${columnName}_nulls[0] &= ~1;  // Clear bit 0
        return this;
      }

      // Store the text value
      this._buffer.${columnName}_values[idx] = this._textStorage.store(value);

      // Mark as non-null (bit 0 for idx 0)
      this._buffer.${columnName}_nulls[0] |= 1;

      return this;
    }`;
  }

  // Boolean: bit-packed storage (8 values per byte)
  if (lmaoType === 'boolean') {
    return `
    ${cleanName}(value) {
      // ALWAYS write to row 0 (span-start) - overwrite semantics
      // Clean method name: ${cleanName}, writes to prefixed column: ${columnName}
      // Bit-packed: bit 0 of byte 0 is index 0
      if (value) {
        this._buffer.${columnName}_values[0] |= 1;
      } else {
        this._buffer.${columnName}_values[0] &= ~1;
      }

      // Mark as non-null (bit 0 for idx 0)
      this._buffer.${columnName}_nulls[0] |= 1;

      return this;
    }`;
  }

  // Generic writer for other types (number)
  return `
    ${cleanName}(value) {
      // ALWAYS write to row 0 (span-start) - overwrite semantics
      // Clean method name: ${cleanName}, writes to prefixed column: ${columnName}
      const idx = 0;
      this._buffer.${columnName}_values[idx] = value;

      // Mark as non-null (bit 0 for idx 0)
      this._buffer.${columnName}_nulls[0] |= 1;

      return this;
    }`;
}

/**
 * Generate a remapped SpanLogger class for library prefix support
 *
 * WHY: Library authors write clean code (ctx.tag.status(200)) but buffers use prefixed
 * columns (attr_http_status). This function generates a class at module creation time
 * (cold path) that exposes clean method names but writes to prefixed columns.
 *
 * HOW: Uses new Function() to generate optimized JavaScript code. The generated class
 * has methods named after clean schema fields but writes to prefixed buffer columns.
 *
 * WHAT: Returns executable JavaScript code string for a SpanLogger class with:
 * - Clean method names (status, method, url)
 * - Writes to prefixed columns (attr_http_status, attr_http_method, attr_http_url)
 * - Full support for enum/category/text type handling
 * - Method chaining support
 *
 * @param cleanSchema - Original clean schema (without prefix)
 * @param prefixMapping - Mapping from clean names to prefixed names
 * @param className - Name for the generated class
 * @returns Executable JavaScript code string
 *
 * @example
 * const code = generateRemappedSpanLoggerClass(
 *   { status: S.number(), method: S.enum(['GET', 'POST']) },
 *   { status: 'http_status', method: 'http_method' },
 *   'HttpSpanLogger'
 * );
 * // Generated class has .status() method that writes to attr_http_status
 */
export function generateRemappedSpanLoggerClass<T extends TagAttributeSchema>(
  cleanSchema: T,
  prefixMapping: PrefixMapping,
  className = 'RemappedSpanLogger',
): string {
  const schemaFields = getSchemaFields(cleanSchema);

  // Generate enum mapping functions (use cleanName for function names)
  const enumMappings: string[] = [];
  const enumFieldNames = new Set<string>();

  for (const [fieldName, fieldSchema] of schemaFields) {
    const lmaoType = getLmaoSchemaType(fieldSchema);
    const enumValues = getEnumValues(fieldSchema);

    if (lmaoType === 'enum' && enumValues) {
      enumMappings.push(generateEnumMapping(fieldName, enumValues));
      enumFieldNames.add(fieldName);
    }
  }

  // Generate attribute writer methods with remapping
  const attributeWriters = schemaFields.map(([cleanName, fieldSchema]) => {
    const prefixedName = prefixMapping[cleanName] || cleanName;
    return generateRemappedAttributeWriter(cleanName, prefixedName, fieldSchema, enumFieldNames.has(cleanName));
  });

  // Create schema type map for runtime type detection in scope()
  const schemaTypeMap = Object.fromEntries(
    schemaFields.map(([fieldName, fieldSchema]) => {
      return [fieldName, getLmaoSchemaType(fieldSchema) || 'unknown'];
    }),
  );

  // Create clean-to-prefixed mapping for with() and scope() methods
  const prefixMappingJson = JSON.stringify(prefixMapping);

  // Entry type constants
  const ENTRY_TYPE_INFO = 9;
  const ENTRY_TYPE_DEBUG = 10;
  const ENTRY_TYPE_WARN = 11;
  const ENTRY_TYPE_ERROR = 12;

  // Generate the complete class
  const classCode = `
(function() {
  'use strict';
  
  // Inline getTimestampNanos for performance (zero function call overhead)
  // performance.timeOrigin + performance.now() gives epoch milliseconds with sub-ms precision
  // Convert to microseconds first (safe as Number), then to nanoseconds as BigInt
  function getTimestampNanos() {
    const epochMicros = Math.round((performance.timeOrigin + performance.now()) * 1000);
    return BigInt(epochMicros) * 1000n;
  }
  
  ${enumMappings.join('\n')}
  
  // Schema type map for runtime type detection
  const SCHEMA_TYPES = ${JSON.stringify(schemaTypeMap)};
  
  // Prefix mapping: clean name -> prefixed name
  const PREFIX_MAPPING = ${prefixMappingJson};
  
  class ${className} {
    constructor(buffer, categoryInterner, textStorage, getBufferWithSpace, initialScopedAttributes = {}) {
      this._buffer = buffer;
      this._categoryInterner = categoryInterner;
      this._textStorage = textStorage;
      this._getBufferWithSpace = getBufferWithSpace;
      this._scopedAttributes = initialScopedAttributes;
    }
    
    // Tag getter - returns chainable API that writes to row 0 (span-start)
    get tag() {
      return this;
    }
    
    // with() method for bulk attribute setting - writes to row 0
    // Maps clean names to prefixed columns
    with(attributes) {
      const idx = 0;

      for (const [cleanKey, value] of Object.entries(attributes)) {
        // Map clean name to prefixed column name
        const prefixedKey = PREFIX_MAPPING[cleanKey] || cleanKey;
        const columnName = 'attr_' + prefixedKey;
        const column = this._buffer[columnName + '_values'];

        if (column && value !== null && value !== undefined) {
          // Process value based on schema type
          const fieldType = SCHEMA_TYPES[cleanKey];
          let processedValue = value;

          if (typeof value === 'string') {
            if (fieldType === 'category') {
              processedValue = this._categoryInterner.intern(value);
            } else if (fieldType === 'text') {
              processedValue = this._textStorage.store(value);
            }
          }

          column[idx] = processedValue;

          // Mark as non-null (bit 0 for idx 0)
          const nullBitmap = this._buffer[columnName + '_nulls'];
          if (nullBitmap) {
            nullBitmap[0] |= 1;
          }
        }
      }

      return this;
    }
    
    // Attribute writer methods (generated from schema with prefix remapping)
    ${attributeWriters.join('\n')}
    
    // Scoped attributes - maps clean names to prefixed columns
    scope(attributes) {
      for (const [cleanKey, value] of Object.entries(attributes)) {
        const prefixedKey = PREFIX_MAPPING[cleanKey] || cleanKey;
        const columnName = 'attr_' + prefixedKey;
        const column = this._buffer[columnName + '_values'];

        if (column && value !== null && value !== undefined) {
          // Process value based on schema type
          let processedValue = value;
          const fieldType = SCHEMA_TYPES[cleanKey];

          if (typeof value === 'string') {
            if (fieldType === 'category') {
              processedValue = this._categoryInterner.intern(value);
            } else if (fieldType === 'text') {
              processedValue = this._textStorage.store(value);
            }
          }

          this._scopedAttributes[cleanKey] = processedValue;
        }
      }

      // Pre-fill remaining buffer capacity with scoped attributes
      const startIdx = this._buffer.writeIndex;
      const endIdx = this._buffer.capacity;

      for (let idx = startIdx; idx < endIdx; idx++) {
        for (const [cleanKey, value] of Object.entries(this._scopedAttributes)) {
          const prefixedKey = PREFIX_MAPPING[cleanKey] || cleanKey;
          const columnName = 'attr_' + prefixedKey;
          const column = this._buffer[columnName + '_values'];
          if (column) {
            column[idx] = value;

            // Mark as non-null
            const nullBitmap = this._buffer[columnName + '_nulls'];
            if (nullBitmap) {
              const byteIndex = Math.floor(idx / 8);
              const bitOffset = idx % 8;
              nullBitmap[byteIndex] |= (1 << bitOffset);
            }
          }
        }
      }
    }
    
    // Message logging methods
    _writeMessage(entryType, message) {
      const result = this._getBufferWithSpace(this._buffer);
      this._buffer = result.buffer;

      const idx = this._buffer.writeIndex;

      // Write entry type
      this._buffer.operations[idx] = entryType;

      // Write timestamp (nanoseconds since epoch)
      this._buffer.timestamps[idx] = getTimestampNanos();

      // Write message to attr_logMessage column
      // Named logMessage (not message) to avoid conflict with SpanLogger.message() method
      // Converted to 'message' column in Arrow output per specs/01f_arrow_table_structure.md
      const messageColumn = this._buffer.attr_logMessage_values;
      if (messageColumn) {
        messageColumn[idx] = this._textStorage.store(message);

        const nullBitmap = this._buffer.attr_logMessage_nulls;
        if (nullBitmap) {
          const byteIndex = Math.floor(idx / 8);
          const bitOffset = idx % 8;
          nullBitmap[byteIndex] |= (1 << bitOffset);
        }
      }

      // Apply scoped attributes (already processed)
      for (const [cleanKey, value] of Object.entries(this._scopedAttributes)) {
        const prefixedKey = PREFIX_MAPPING[cleanKey] || cleanKey;
        const columnName = 'attr_' + prefixedKey;
        const column = this._buffer[columnName + '_values'];
        if (column) {
          column[idx] = value;

          const nullBitmap = this._buffer[columnName + '_nulls'];
          if (nullBitmap) {
            const byteIndex = Math.floor(idx / 8);
            const bitOffset = idx % 8;
            nullBitmap[byteIndex] |= (1 << bitOffset);
          }
        }
      }

      this._buffer.writeIndex++;
    }
    
    message(level, message) {
      const entryTypeMap = {
        'info': ${ENTRY_TYPE_INFO},
        'debug': ${ENTRY_TYPE_DEBUG},
        'warn': ${ENTRY_TYPE_WARN},
        'error': ${ENTRY_TYPE_ERROR}
      };
      this._writeMessage(entryTypeMap[level] || ${ENTRY_TYPE_INFO}, message);
    }
    
    info(message) {
      this._writeMessage(${ENTRY_TYPE_INFO}, message);
    }
    
    debug(message) {
      this._writeMessage(${ENTRY_TYPE_DEBUG}, message);
    }
    
    warn(message) {
      this._writeMessage(${ENTRY_TYPE_WARN}, message);
    }
    
    error(message) {
      this._writeMessage(${ENTRY_TYPE_ERROR}, message);
    }
  }

  return ${className};
})()
`;

  return classCode;
}

/**
 * Create a remapped SpanLogger class constructor from clean schema and prefix mapping
 *
 * WHY: This is the cold-path function called at module creation time. It compiles
 * the generated class code once and caches the constructor.
 *
 * @param cleanSchema - Clean schema (without prefix)
 * @param prefixMapping - Mapping from clean names to prefixed names
 * @returns Constructor for the remapped SpanLogger class
 */
export function createRemappedSpanLoggerClass<T extends TagAttributeSchema>(
  cleanSchema: T,
  prefixMapping: PrefixMapping,
): new (
  buffer: import('./types.js').SpanBuffer,
  categoryInterner: import('./codegen/spanLoggerGenerator.js').StringInterner,
  textStorage: import('./codegen/spanLoggerGenerator.js').TextStorage,
  getBufferWithSpace: import('./codegen/spanLoggerGenerator.js').GetBufferWithSpaceFn,
  initialScopedAttributes?: Record<string, unknown>,
) => import('./codegen/spanLoggerGenerator.js').BaseSpanLogger<T> {
  const classCode = generateRemappedSpanLoggerClass(cleanSchema, prefixMapping).trim();

  // Use Function constructor to create the class (cold path - happens once per schema/prefix combo)
  // eslint-disable-next-line @typescript-eslint/no-implied-eval, no-new-func
  const GeneratedClass = new Function(`return ${classCode}`)();

  return GeneratedClass;
}

/**
 * Create a library module with clean schema
 * Library authors use this to define their module
 *
 * Accepts both plain TagAttributeSchema and extended schemas from defineTagAttributes
 *
 * @param options - Library metadata, schema, and operations
 * @returns Library module with operations
 */
export function createLibraryModule<
  T extends TagAttributeSchema,
  FF extends FeatureFlagSchema = FeatureFlagSchema,
  Env = Record<string, unknown>,
  Ops extends Record<string, LibraryOperation<any[], any, T, FF, Env>> = Record<
    string,
    LibraryOperation<any[], any, T, FF, Env>
  >,
>(options: {
  gitSha: string;
  filePath: string;
  moduleName?: string;
  schema: T;
  operations?: Ops;
}): LibraryModule<T, FF, Env, Ops> {
  // Extract just the schema fields (without validation methods)
  const schemaFields = getSchemaFields(options.schema);
  const cleanSchema = {} as T;
  for (const [fieldName, fieldSchema] of schemaFields) {
    (cleanSchema as Record<string, unknown>)[fieldName] = fieldSchema;
  }

  // Create module context with clean schema (no prefix yet)
  const moduleContext = createModuleContext<T, FF, Env>({
    moduleMetadata: {
      gitSha: options.gitSha,
      filePath: options.filePath,
      moduleName: options.moduleName || options.filePath,
    },
    tagAttributes: cleanSchema,
  });

  return {
    schema: cleanSchema,
    operations: (options.operations || {}) as Ops,
    task: moduleContext.task,
  };
}

/**
 * Create a remapped tag proxy for library use
 *
 * WHY: Library task functions receive ctx.tag with clean method names,
 * but the underlying buffer uses prefixed column names.
 *
 * HOW: Uses a Proxy to intercept method calls and remap them to prefixed columns.
 * This approach has minimal overhead since Proxy is well-optimized in V8.
 *
 * @param originalTag - The tag API with prefixed method names (ctx.tag)
 * @param prefixMapping - Mapping from clean names to prefixed names
 * @returns Proxy that remaps clean method calls to prefixed methods
 */
function createRemappedTagProxy<T extends TagAttributeSchema>(
  originalTag: SpanContext<TagAttributeSchema, FeatureFlagSchema>['tag'],
  prefixMapping: PrefixMapping,
): SpanContext<T, FeatureFlagSchema>['tag'] {
  // Create proxy for the tag API
  const tagProxy: SpanContext<T, FeatureFlagSchema>['tag'] = new Proxy(originalTag as object, {
    get(target, prop: string) {
      // Check if this is a clean name that needs remapping
      const prefixedName = prefixMapping[prop];
      if (prefixedName && typeof (target as Record<string, unknown>)[prefixedName] === 'function') {
        // Return a function that calls the prefixed method but maintains chaining
        return (value: unknown) => {
          ((target as Record<string, unknown>)[prefixedName] as (v: unknown) => unknown)(value);
          return tagProxy; // Return proxy for chaining
        };
      }

      // Handle 'with' method specially - remap attribute keys
      if (prop === 'with') {
        return (attributes: Record<string, unknown>) => {
          const remappedAttributes: Record<string, unknown> = {};
          for (const [key, value] of Object.entries(attributes)) {
            const prefixedKey = prefixMapping[key] || key;
            remappedAttributes[prefixedKey] = value;
          }
          (target as { with: (attrs: Record<string, unknown>) => unknown }).with(remappedAttributes);
          return tagProxy;
        };
      }

      // For other properties/methods, pass through
      const value = (target as Record<string, unknown>)[prop];
      if (typeof value === 'function') {
        return value.bind(target);
      }
      return value;
    },
  }) as SpanContext<T, FeatureFlagSchema>['tag'];

  return tagProxy;
}

/**
 * Create a remapped scope function for library use
 *
 * @param originalScope - The scope function (ctx.scope)
 * @param prefixMapping - Mapping from clean names to prefixed names
 * @returns Function that remaps attribute keys before calling scope
 */
function createRemappedScopeFunction<T extends TagAttributeSchema>(
  originalScope: SpanContext<TagAttributeSchema, FeatureFlagSchema>['scope'],
  prefixMapping: PrefixMapping,
): SpanContext<T, FeatureFlagSchema>['scope'] {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- Type erasure at runtime boundary
  return (attributes: Record<string, unknown>) => {
    const remappedAttributes: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(attributes)) {
      const prefixedKey = prefixMapping[key] || key;
      remappedAttributes[prefixedKey] = value;
    }
    // Type assertion needed because we're remapping between different schema types at runtime
    (originalScope as (attrs: Record<string, unknown>) => void)(remappedAttributes);
  };
}

/**
 * Module context factory for library composition with prefix remapping
 *
 * WHY: Applications need to compose multiple libraries without naming conflicts.
 * Each library defines clean schemas, and this factory applies prefixes while
 * maintaining the clean API for library code.
 *
 * HOW: Creates a module context with prefixed schema, but wraps task functions
 * so that library code receives a remapped context where clean method names
 * (ctx.tag.status) map to prefixed buffer columns (attr_http_status).
 *
 * WHAT:
 * - Prefixes all schema field names (status → http_status)
 * - Creates task wrappers that remap ctx.log methods
 * - Library author writes: ctx.tag.status(200)
 * - Runtime writes to: buffer.attr_http_status[idx] = 200
 *
 * Per specs/01e_library_integration_pattern.md:
 * - Library writes: ctx.tag.status(200)
 * - Final column: http_status (with prefix)
 * - All mapping happens at task creation time (cold path)
 *
 * @param prefix - Prefix to apply to all schema fields (e.g., 'http', 'db')
 * @param moduleMetadata - Library metadata (gitSha, filePath, moduleName)
 * @param schema - Clean library schema (without prefix)
 * @param operations - Library operations to wrap
 * @returns Module context builder with prefixed schema and remapped task wrappers
 *
 * @example
 * const httpLib = moduleContextFactory(
 *   'http',
 *   { gitSha: 'abc', filePath: 'lib/http.ts', moduleName: 'http' },
 *   { status: S.number(), method: S.enum(['GET', 'POST']) }
 * );
 *
 * // Library task function uses clean names
 * const myTask = httpLib.task('request', async (ctx) => {
 *   ctx.tag.status(200);  // Writes to http_status column
 *   ctx.tag.method('GET'); // Writes to http_method column
 * });
 */
export function moduleContextFactory<
  T extends TagAttributeSchema,
  FF extends FeatureFlagSchema = FeatureFlagSchema,
  Env = Record<string, unknown>,
>(
  prefix: string,
  moduleMetadata: {
    gitSha: string;
    filePath: string;
    moduleName: string;
  },
  schema: T,
  operations?: Record<string, LibraryOperation<any[], any, T, FF, Env>>,
): ModuleContextBuilder<TagAttributeSchema, FF, Env> & {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- Type erasure at runtime boundary; type safety enforced via LibraryOperation generics
  operations: Record<string, (...args: any[]) => any>;
  /** Clean schema (without prefix) - useful for type inference */
  cleanSchema: T;
  /** Prefix mapping from clean names to prefixed names */
  prefixMapping: PrefixMapping;
} {
  // Create prefix mapping (cold path - done once)
  const prefixMapping = createPrefixMapping(schema, prefix);

  // Apply prefix to schema for buffer column creation
  const prefixedSchema = prefixSchema(schema, prefix);

  // Create module context with prefixed schema
  // This creates buffers with columns like attr_http_status
  const moduleContext = createModuleContext({
    moduleMetadata,
    tagAttributes: prefixedSchema,
  }) as ModuleContextBuilder<TagAttributeSchema, FF, Env>;

  // Create a wrapped task function that remaps the context
  const wrappedTask = <Args extends unknown[], Result>(
    name: string,
    fn: TaskFunction<Args, Result, T, FF, Env>,
  ): ((ctx: RequestContext<FF, Env>, ...args: Args) => Promise<Result>) => {
    // Get the underlying task wrapper from the module context
    const underlyingTask = moduleContext.task(name, async (ctx, ...args: Args) => {
      // Create remapped context for the library function
      // The ctx.tag here has prefixed methods (http_status), but we need clean methods (status)
      const remappedTag = createRemappedTagProxy<T>(ctx.tag, prefixMapping);

      // Remap the scope function to use clean attribute names
      const remappedScope = createRemappedScopeFunction<T>(ctx.scope, prefixMapping);

      // Create a new context with the remapped tag and scope
      // ctx.log remains unchanged (it's just the SpanLogger for logging)
      const remappedCtx: SpanContext<T, FF, Env> = {
        ...ctx,
        tag: remappedTag,
        scope: remappedScope,
      } as SpanContext<T, FF, Env>;

      // Call the library function with the remapped context
      return fn(remappedCtx, ...args);
    });

    return underlyingTask;
  };

  // Wrap operations with remapped task wrappers
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- Type erasure intentional at runtime boundary
  const wrappedOperations: Record<string, (...args: any[]) => any> = {};

  if (operations) {
    for (const [opName, opDef] of Object.entries(operations)) {
      wrappedOperations[opName] = wrappedTask(opDef.spanName, opDef.fn);
    }
  }

  return {
    // Type assertion needed because wrappedTask uses T (library schema) but return type uses TagAttributeSchema
    // This is intentional type erasure at the runtime boundary
    task: wrappedTask as ModuleContextBuilder<TagAttributeSchema, FF, Env>['task'],
    operations: wrappedOperations,
    cleanSchema: schema,
    prefixMapping,
  };
}

/**
 * Library factory result type
 */
export interface LibraryFactory<
  T extends TagAttributeSchema,
  FF extends FeatureFlagSchema = FeatureFlagSchema,
  Env = Record<string, unknown>,
> {
  task: <Args extends unknown[], Result>(
    name: string,
    fn: TaskFunction<Args, Result, T, FF, Env>,
  ) => (ctx: RequestContext<FF, Env>, ...args: Args) => Promise<Result>;
  operations: Record<string, (...args: unknown[]) => Promise<unknown>>;
}

/**
 * Example: HTTP library factory
 * Shows how a library would be structured
 */
export function createHttpLibrary(prefix = 'http'): LibraryFactory<TagAttributeSchema> {
  // Example HTTP schema (library defines clean names using S builder)
  // Note: In real usage, you would call defineTagAttributes() for validation
  const httpSchemaDefinition = {
    status: S.number(),
    method: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
    url: S.text(),
    duration: S.number(),
  };

  const moduleMetadata = {
    gitSha: 'dev',
    filePath: 'http-library',
    moduleName: 'http',
  };

  // Operations would be defined here
  const operations = {};

  // Pass the raw schema object (without methods)
  return moduleContextFactory(prefix, moduleMetadata, httpSchemaDefinition, operations);
}

/**
 * Example: Database library factory
 */
export function createDatabaseLibrary(prefix = 'db'): LibraryFactory<TagAttributeSchema> {
  // Example DB schema (library defines clean names using S builder)
  // Note: In real usage, you would call defineTagAttributes() for validation
  const dbSchemaDefinition = {
    query: S.text(),
    duration: S.number(),
    table: S.category(),
    operation: S.enum(['SELECT', 'INSERT', 'UPDATE', 'DELETE']),
  };

  const moduleMetadata = {
    gitSha: 'dev',
    filePath: 'db-library',
    moduleName: 'database',
  };

  const operations = {};

  // Pass the raw schema object (without methods)
  return moduleContextFactory(prefix, moduleMetadata, dbSchemaDefinition, operations);
}
