import { describe, expect, it } from 'bun:test';
import { S } from '../../schema/builder.js';
import { ColumnSchema } from '../../schema/ColumnSchema.js';
import { getColumnBufferClass } from '../columnBufferGenerator.js';

describe('getColumnBufferClass cache key', () => {
  it('ignores extension dependencies when caching generated classes', () => {
    const schema = new ColumnSchema({ count: S.number() });

    const ClassWithDepA = getColumnBufferClass(schema, {
      methods: 'ping() { return 1; }',
      dependencies: { dep: { id: 'a' } },
    });

    const ClassWithDepB = getColumnBufferClass(schema, {
      methods: 'ping() { return 1; }',
      dependencies: { dep: { id: 'b' } },
    });

    expect(ClassWithDepA).toBe(ClassWithDepB);
  });

  it('supports dependency objects with circular references', () => {
    const schema = new ColumnSchema({ count: S.number() });
    const circular: { self?: unknown } = {};
    circular.self = circular;

    expect(() =>
      getColumnBufferClass(schema, {
        methods: 'ping() { return 1; }',
        dependencies: { circular },
      }),
    ).not.toThrow();
  });
});
