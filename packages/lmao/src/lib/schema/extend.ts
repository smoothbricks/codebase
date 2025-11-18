import type { TagAttributeSchema } from './types.js';

// Merge two tag attribute schemas with conflict detection, and return a new merged schema
export function extendSchema<T extends TagAttributeSchema, U extends TagAttributeSchema>(
  base: T,
  extension: U
): T & U {
  // Check for conflicts
  for (const key of Object.keys(extension)) {
    if (key in base) {
      throw new Error(`Schema conflict: field '${key}' already exists in base schema`);
    }
  }
  return { ...base, ...extension };
}

// Extended schema interface keeps an .extend() method for chaining
export interface ExtendedSchema<T extends TagAttributeSchema> {
  extend<U extends TagAttributeSchema>(extension: U): ExtendedSchema<T & U>;
}

// Create a schema object that exposes fields and supports .extend()
export function createExtendedSchema<T extends TagAttributeSchema>(
  schema: T
): ExtendedSchema<T> {
  // Build a runtime object that mirrors the schema fields
  const proxy = { ...schema } as any;

  // Attach .extend() to enable chaining and return new extended schemas
  proxy.extend = <U extends TagAttributeSchema>(extension: U) => {
    const merged = extendSchema(schema, extension);
    // Recursively create the next extended schema from the merged one
    return createExtendedSchema(merged);
  };

  return proxy as ExtendedSchema<T>;
}
