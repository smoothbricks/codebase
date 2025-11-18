export type SchemaType = 'string' | 'number' | 'boolean' | 'union';

export interface BaseSchemaField<T extends SchemaType> {
  type: T;
}

export interface StringSchemaField extends BaseSchemaField<'string'> {
  mask?: 'hash' | 'url' | 'sql' | 'email';
}

export interface NumberSchemaField extends BaseSchemaField<'number'> {}

export interface BooleanSchemaField extends BaseSchemaField<'boolean'> {}

export interface UnionSchemaField extends BaseSchemaField<'union'> {
  values: readonly string[];
}

export type SchemaField =
  | StringSchemaField
  | NumberSchemaField
  | BooleanSchemaField
  | UnionSchemaField;

export interface OptionalSchemaField {
  optional: true;
  field: SchemaField;
}

export type TagAttributeSchema = Record<string, SchemaField | OptionalSchemaField>;

// To aid typing for S builder with a .with() method on strings
export type StringFieldWithWith = StringSchemaField & {
  with: (mask: 'hash' | 'url' | 'sql' | 'email') => StringSchemaField;
};

export interface SchemaBuilder {
  string: () => StringFieldWithWith;
  number: () => NumberSchemaField;
  boolean: () => BooleanSchemaField;
  union: <T extends readonly string[]>(values: T) => UnionSchemaField;
  optional: <T extends SchemaField>(field: T) => OptionalSchemaField;
}
