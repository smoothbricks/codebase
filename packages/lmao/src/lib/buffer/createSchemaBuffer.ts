let nextGlobalSpanId = 1;

import { createEmptySpanBuffer } from './createSpanBuffer.js';
import { generateAttributeColumns } from './generateAttributeColumns.js';
import { getCacheAlignedCapacity } from './capacity.js';
import type { TagAttributeSchema } from '../schema/types.js';

export function createSchemaBuffer(schema: TagAttributeSchema, requestedCapacity = 64) {
  const attributeCount = Object.keys(schema).length;
  const alignedCapacity = getCacheAlignedCapacity(requestedCapacity, 1);

  const baseBuffer = createEmptySpanBuffer(nextGlobalSpanId++, requestedCapacity, attributeCount);
  const attributeColumns = generateAttributeColumns(schema, alignedCapacity);

  return {
    ...baseBuffer,
    ...attributeColumns,
  };
}
