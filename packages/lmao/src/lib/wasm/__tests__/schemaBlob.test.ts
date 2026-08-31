import { describe, expect, it } from 'bun:test';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import { THREAD_ATTRIBUTE_KINDS, THREAD_SYSTEM_COLUMNS } from '../../schema/systemSchema.js';
import { encodeSchemaBlob, schemaAttributeOrdinals, THREAD_SYSTEM_COLUMN_COUNT } from '../schemaBlob.js';

describe('thread-buffer schema blob', () => {
  it('encodes user fields after the system prefix and skips system columns', () => {
    const schema = defineLogSchema({
      n: S.number(),
      flag: S.boolean(),
      label: S.category(),
      kind: S.enum(['a', 'b']),
    });
    const blob = encodeSchemaBlob(schema);
    expect(THREAD_SYSTEM_COLUMN_COUNT).toBe(THREAD_SYSTEM_COLUMNS.length);

    const kinds = Object.fromEntries(THREAD_ATTRIBUTE_KINDS.map((kind) => [kind.name, kind.discriminant]));
    const expected: number[] = [];
    expected.push(kinds.number, 1, ...new TextEncoder().encode('n'));
    expected.push(kinds.boolean, 4, ...new TextEncoder().encode('flag'));
    expected.push(kinds.text, 5, ...new TextEncoder().encode('label'));
    expected.push(kinds.enum, 4, ...new TextEncoder().encode('kind'), 2, 0, 1, 97, 1, 98);
    expect(Array.from(blob)).toEqual(expected);

    const ordinals = schemaAttributeOrdinals(schema);
    expect(ordinals.get('n')).toBe(THREAD_SYSTEM_COLUMN_COUNT);
    expect(ordinals.get('kind')).toBe(THREAD_SYSTEM_COLUMN_COUNT + 3);
    expect(ordinals.has('message')).toBe(false);
  });
});
