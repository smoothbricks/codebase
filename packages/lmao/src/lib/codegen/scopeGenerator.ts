/**
 * Runtime code generation for Scope classes
 *
 * Per user requirements:
 * - Scope is a SEPARATE generated class from column storage
 * - Only contains schema attributes (NOT system columns like timestamps, operations)
 * - Initialized with undefined for all values
 * - Used for inheriting data from parent spans
 * - Has _getScopeValues() method for pre-filling child spans
 *
 * Implementation:
 * - Uses new Function() for runtime class generation
 * - Each schema gets a cached Scope class
 * - Scope instances are created per TraceContext/SpanContext
 * - NO TypedArrays - just plain JavaScript properties
 * - NO system columns (timestamps, operations, labels)
 */

import type { TagAttributeSchema } from '../schema/types.js';
import { getSchemaFields } from '../schema/types.js';

/**
 * Generated Scope instance interface
 *
 * Each Scope has:
 * - Properties matching schema attributes (all initialized to undefined)
 * - Getters and setters for each property
 * - _getScopeValues() method to extract all values for inheritance
 */
export interface GeneratedScope {
  _getScopeValues(): Record<string, unknown>;
  [key: string]: unknown;
}

/**
 * Scope class constructor type
 */
export type ScopeClass = new () => GeneratedScope;

/**
 * Cache for generated Scope classes (per schema)
 * WeakMap allows garbage collection when schema is no longer referenced
 */
const scopeClassCache = new WeakMap<TagAttributeSchema, ScopeClass>();

/**
 * Generate Scope class code from schema
 *
 * Creates a class with:
 * - Private fields for each attribute (initialized to undefined)
 * - Getters and setters for type-safe access
 * - _getScopeValues() to extract all values as plain object
 *
 * @param schema - Tag attribute schema (user-defined attributes only)
 * @returns JavaScript code string for the Scope class
 */
function generateScopeClassCode(schema: TagAttributeSchema): string {
  // Get schema fields (excluding methods added by defineTagAttributes)
  const schemaFields = getSchemaFields(schema);
  const fieldNames = schemaFields.map(([name]) => name);

  // Generate private field declarations (all initialized to undefined)
  const privateFields = fieldNames.map((name) => `  _${name} = undefined;`).join('\n');

  // Generate getters and setters
  const accessors = fieldNames
    .map(
      (name) => `
  get ${name}() {
    return this._${name};
  }

  set ${name}(value) {
    this._${name} = value;
  }`,
    )
    .join('\n');

  // Generate _getScopeValues() method
  const getScopeValues = `
  _getScopeValues() {
    return {
      ${fieldNames.map((name) => `${name}: this._${name}`).join(',\n      ')}
    };
  }`;

  // Generate the complete class
  const classCode = `
(function() {
  'use strict';

  class GeneratedScope {
${privateFields}
${accessors}
${getScopeValues}
  }

  return GeneratedScope;
})()
`;

  return classCode;
}

/**
 * Create or retrieve cached Scope class for a schema
 *
 * This is the cold-path function called at module creation time.
 * Subsequent calls with the same schema return the cached class.
 *
 * @param schema - Tag attribute schema
 * @returns Scope class constructor
 *
 * @example
 * ```typescript
 * const schema = defineTagAttributes({
 *   userId: S.category(),
 *   requestId: S.category(),
 *   count: S.number()
 * });
 *
 * const ScopeClass = generateScopeClass(schema);
 * const scope = new ScopeClass();
 *
 * scope.userId = 'user123';
 * scope.count = 42;
 *
 * const values = scope._getScopeValues();
 * // { userId: 'user123', requestId: undefined, count: 42 }
 * ```
 */
export function generateScopeClass(schema: TagAttributeSchema): ScopeClass {
  // Check cache first
  let ScopeClass = scopeClassCache.get(schema);

  if (!ScopeClass) {
    // Generate and compile the class
    const classCode = generateScopeClassCode(schema).trim();

    // Use Function constructor to create the class
    // This is safe because we control the code generation
    // eslint-disable-next-line @typescript-eslint/no-implied-eval, no-new-func
    ScopeClass = new Function(`return ${classCode}`)() as ScopeClass;

    // Cache for future use
    scopeClassCache.set(schema, ScopeClass);
  }

  return ScopeClass;
}

/**
 * Create a new Scope instance from a schema
 *
 * Hot-path function called when creating span contexts.
 * All attributes are initialized to undefined.
 *
 * @param schema - Tag attribute schema
 * @returns New Scope instance with all attributes = undefined
 */
export function createScope(schema: TagAttributeSchema): GeneratedScope {
  const ScopeClass = generateScopeClass(schema);
  return new ScopeClass();
}

/**
 * Create a Scope instance pre-filled with parent values
 *
 * Used for child span inheritance.
 * Child gets a new Scope instance with parent's values copied over.
 *
 * @param schema - Tag attribute schema
 * @param parentScopeValues - Values from parent scope (via _getScopeValues())
 * @returns New Scope instance with parent values
 */
export function createScopeWithInheritance(
  schema: TagAttributeSchema,
  parentScopeValues: Record<string, unknown>,
): GeneratedScope {
  const scope = createScope(schema);

  // Copy parent values to child scope
  for (const [key, value] of Object.entries(parentScopeValues)) {
    if (value !== undefined) {
      scope[key] = value;
    }
  }

  return scope;
}
