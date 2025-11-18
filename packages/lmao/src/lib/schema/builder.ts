import type { SchemaBuilder, SchemaField, StringSchemaField, OptionalSchemaField, UnionSchemaField } from './types.js';

export const S: SchemaBuilder = {
  string: () => {
    const base: StringSchemaField = { type: 'string' };
    // attach a .with() helper to satisfy fluent usage like S.string().with('hash')
    (base as any).with = (mask: 'hash' | 'url' | 'sql' | 'email') => ({ type: 'string', mask }) as StringSchemaField;
    // Cast to include the with method for TypeScript compatibility
    return base as StringSchemaField & { with: (mask: 'hash' | 'url' | 'sql' | 'email') => StringSchemaField };
  },
  number: () => ({ type: 'number' }),
  boolean: () => ({ type: 'boolean' }),
  union: <T extends readonly string[]>(values: T) => ({ type: 'union', values } as UnionSchemaField),
  optional: <T extends SchemaField>(field: T) => ({ optional: true, field } as OptionalSchemaField),
};
