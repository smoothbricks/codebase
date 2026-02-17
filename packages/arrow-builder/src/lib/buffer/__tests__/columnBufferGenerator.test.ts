import { describe, expect, it } from 'bun:test';
import { S } from '../../schema/builder.js';
import { ColumnSchema } from '../../schema/ColumnSchema.js';
import { generateColumnBufferClass, getColumnBufferClass } from '../columnBufferGenerator.js';

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

describe('preamble contract split in ColumnBuffer', () => {
  it('classPreamble appears before class definition', () => {
    const schema = new ColumnSchema({ count: S.number() });
    const code = generateColumnBufferClass(schema, 'TestBuffer', {
      classPreamble: 'const MODULE_CONSTANT = 42;',
    });
    const preambleIdx = code.indexOf('const MODULE_CONSTANT = 42;');
    const classIdx = code.indexOf('class TestBuffer');
    expect(preambleIdx).toBeGreaterThan(-1);
    expect(classIdx).toBeGreaterThan(-1);
    expect(preambleIdx).toBeLessThan(classIdx);
  });

  it('constructorPreamble appears inside constructor body', () => {
    const schema = new ColumnSchema({ count: S.number() });
    const code = generateColumnBufferClass(schema, 'TestBuffer', {
      constructorPreamble: 'const localVar = 123;',
    });
    const preambleIdx = code.indexOf('const localVar = 123;');
    const constructorIdx = code.indexOf('constructor(');
    const firstBrace = code.indexOf('{', constructorIdx);
    expect(preambleIdx).toBeGreaterThan(firstBrace);
    // Also verify it appears before the normal constructor code
    const capacityIdx = code.indexOf('helpers.getAlignedCapacity');
    expect(preambleIdx).toBeLessThan(capacityIdx);
  });

  it('does not have old preamble field in interface', () => {
    const schema = new ColumnSchema({ count: S.number() });
    // Creating an extension with only the new fields should work
    const BufferClass = getColumnBufferClass(schema, {
      classPreamble: 'const X = 1;',
      constructorPreamble: 'const Y = 2;',
    });
    const buffer = new BufferClass(8);
    expect(buffer._capacity).toBe(8);
  });
});
