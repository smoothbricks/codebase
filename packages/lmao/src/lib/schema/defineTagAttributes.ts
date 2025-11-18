import type { TagAttributeSchema } from './types.js';
import { createExtendedSchema } from './extend.js';
import type { ExtendedSchema } from './extend.js';

export function defineTagAttributes<T extends TagAttributeSchema>(
  schema: T
): ExtendedSchema<T> {
  // Validate each field in the provided schema
  for (const [key, field] of Object.entries(schema)) {
    if (!isValidField(field)) {
      throw new Error(`Invalid schema field for '${key}': ${JSON.stringify(field)}`);
    }
  }
  // Return an extendable schema object
  return createExtendedSchema(schema) as ExtendedSchema<T>;
}

function isValidField(field: unknown): boolean {
  if (typeof field !== 'object' || field === null) return false;

  // Optional field wrapper
  if ('optional' in (field as any) && (field as any).optional === true) {
    const inner = (field as any).field;
    return isValidField(inner);
  }

  // Base field with a type
  return (
    'type' in (field as any) &&
    typeof (field as any).type === 'string' &&
    ['string', 'number', 'boolean', 'union'].includes((field as any).type)
  );
}
