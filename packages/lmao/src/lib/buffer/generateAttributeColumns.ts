import type { TagAttributeSchema, SchemaField } from '../schema/types.js';

type TypedArray = Float64Array | Uint8Array | Uint16Array | Uint32Array;

export function generateAttributeColumns(
  schema: TagAttributeSchema,
  alignedCapacity: number
): Record<string, TypedArray> {
  const attributeColumns: Record<string, TypedArray> = {};

  for (const [fieldName, fieldConfig] of Object.entries(schema)) {
    const columnName = `attr_${fieldName}`;
    const actualField: SchemaField = 'optional' in fieldConfig ? fieldConfig.field : fieldConfig;

    let typedArray: TypedArray;
    switch (actualField.type) {
      case 'string':
      case 'union':
        typedArray = new Uint32Array(alignedCapacity);
        break;
      case 'number':
        typedArray = new Float64Array(alignedCapacity);
        break;
      case 'boolean':
        typedArray = new Uint8Array(alignedCapacity);
        break;
      default:
        typedArray = new Uint32Array(alignedCapacity);
    }

    attributeColumns[columnName] = typedArray;
  }

  return attributeColumns;
}
