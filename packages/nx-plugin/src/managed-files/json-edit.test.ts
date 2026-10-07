import { describe, expect, it } from 'bun:test';
import { parseJson } from 'nx/src/devkit-exports.js';
import { editJsonText } from './json-edit.js';

/** The document `documented` parses to. A test states the document it wants, and the edit is the difference. */
const document = {
  extends: './tsconfig.json',
  compilerOptions: { noEmit: true, types: ['bun'] },
  include: ['scripts/**/*.test.ts'],
  references: [{ path: './a.json' }, { path: './b.json' }, { path: './c.json' }],
};

const documented = [
  '{',
  '  // Why it extends this: a Bun program.',
  '  "extends": "./tsconfig.json",',
  '  "compilerOptions": {',
  '    "noEmit": true, // required',
  '    /* the runtime types */',
  '    "types": ["bun"],',
  '  },',
  '  "include": ["scripts/**/*.test.ts"],',
  '  "references": [{ "path": "./a.json" }, { "path": "./b.json" }, { "path": "./c.json" }],',
  '}',
  '',
].join('\n');

describe('editing JSON text to parse as another document', () => {
  it('returns the text itself when nothing differs', () => {
    expect(editJsonText(documented, document)).toBe(documented);
  });

  it('adds a key and keeps every comment', () => {
    const edited = editJsonText(documented, { ...document, exclude: ['dist'] });

    expect(edited).toContain('// Why it extends this: a Bun program.');
    expect(edited).toContain('// required');
    expect(edited).toContain('/* the runtime types */');
    expect(parseJson(edited)).toEqual({ ...document, exclude: ['dist'] });
  });

  it('changes a scalar where it stands, nested', () => {
    const edited = editJsonText(documented, { ...document, compilerOptions: { noEmit: false, types: ['bun'] } });

    expect(edited).toContain('"noEmit": false, // required');
    expect(parseJson(edited)).toEqual({ ...document, compilerOptions: { noEmit: false, types: ['bun'] } });
  });

  it('adds a nested key and removes one, leaving the comment that is not about it', () => {
    const edited = editJsonText(documented, { ...document, compilerOptions: { noEmit: true, lib: ['es2024'] } });

    expect(parseJson(edited)).toEqual({ ...document, compilerOptions: { noEmit: true, lib: ['es2024'] } });
    expect(edited).toContain('// required');
  });

  it('removes a key', () => {
    const { include: _removed, ...without } = document;

    const edited = editJsonText(documented, without);

    expect(parseJson(edited)).toEqual(without);
    expect(edited).toContain('// Why it extends this: a Bun program.');
  });

  it('appends the elements an array gained, after the ones it had', () => {
    const include = ['scripts/**/*.test.ts', 'lib/**/*.test.ts', 'tests/**/*.ts'];

    const edited = editJsonText(documented, { ...document, include });

    expect(parseJson(edited)).toEqual({ ...document, include });
    expect(edited).toContain('/* the runtime types */');
  });

  it('creates a key in an empty object and an element in an empty array', () => {
    expect(editJsonText('{}\n', { a: 1 })).toBe('{"a": 1}\n');
    expect(editJsonText('{ "a": [] }\n', { a: [1] })).toBe('{ "a": [1] }\n');
    expect(editJsonText('{ /* nothing yet */ }\n', { a: 1 })).toBe('{"a": 1 /* nothing yet */ }\n');
  });

  it('removes the elements an array lost, wherever they were', () => {
    const edited = editJsonText(documented, { ...document, references: [{ path: './b.json' }] });

    expect(parseJson(edited)).toEqual({ ...document, references: [{ path: './b.json' }] });
    expect(edited).toContain('// required');
  });

  it('removes only the elements that are gone when equal elements remain', () => {
    expect(parseJson(editJsonText('{ "a": [1, 2, 1, 3] }\n', { a: [2, 3] }))).toEqual({ a: [2, 3] });
    expect(parseJson(editJsonText('{ "a": [1, 2, 1, 3] }\n', { a: [1, 1] }))).toEqual({ a: [1, 1] });
  });

  it('takes the last element out of a one-line array whole (a naive delete leaves `13`)', () => {
    // The space before the closer is the formatter's to tidy; what matters is the separator went with the element.
    expect(editJsonText('{ "a": [1, 2, 1, 3] }\n', { a: [1, 2, 1] })).toBe('{ "a": [1, 2, 1 ] }\n');
  });

  it('replaces an array it neither grew nor shrank', () => {
    const include = ['src/**/*.test.ts', 'scripts/**/*.test.ts'];

    const edited = editJsonText(documented, { ...document, include });

    expect(parseJson(edited)).toEqual({ ...document, include });
    expect(edited).toContain('// Why it extends this: a Bun program.');
  });

  it('replaces a value that changed type', () => {
    expect(parseJson(editJsonText('{ "extends": "./a.json" }\n', { extends: ['./a.json', './b.json'] }))).toEqual({
      extends: ['./a.json', './b.json'],
    });
    expect(parseJson(editJsonText('{ "a": { "b": 1 } }\n', { a: 2 }))).toEqual({ a: 2 });
  });

  it('keeps a trailing comma and a block comment where it found them', () => {
    const edited = editJsonText('{\n  /* lead */\n  "a": 1,\n}\n', { a: 1, b: 2 });

    expect(edited).toBe('{\n  /* lead */\n  "a": 1,\n"b": 2,\n}\n');
    expect(parseJson(edited)).toEqual({ a: 1, b: 2 });
  });

  it('is a fixed point: editing to the document the text already is changes nothing', () => {
    const include = ['scripts/**/*.test.ts', 'lib/**/*.test.ts'];
    const edited = editJsonText(documented, { ...document, include });

    expect(editJsonText(edited, { ...document, include })).toBe(edited);
  });

  it('is never fooled by a key that means nothing: undefined is no key', () => {
    expect(editJsonText('{ "a": 1 }\n', { a: 1, b: undefined })).toBe('{ "a": 1 }\n');
  });
});

describe('a comment that trails a member on its line belongs to that member', () => {
  it('stays with the key it follows when a key is added after it', () => {
    expect(editJsonText('{\n  "a": [1] // about a\n}\n', { a: [1], b: 2 })).toBe(
      '{\n  "a": [1], // about a\n"b": 2\n}\n',
    );
  });

  it('stays with the element it follows when an element is appended after it', () => {
    expect(editJsonText('{\n  "include": [\n    "a" // about a\n  ]\n}\n', { include: ['a', 'b'] })).toBe(
      '{\n  "include": [\n    "a", // about a\n"b"\n  ]\n}\n',
    );
  });

  it('is not given to the new member when the author already wrote a trailing comma', () => {
    expect(editJsonText('{\n  "a": 1, // about a\n}\n', { a: 1, b: 2 })).toBe('{\n  "a": 1, // about a\n"b": 2,\n}\n');
  });

  it('goes with the element that is removed', () => {
    expect(editJsonText('{\n  "include": [\n    "x", // about x\n    "y"\n  ]\n}\n', { include: ['y'] })).toBe(
      '{\n  "include": [\n    "y"\n  ]\n}\n',
    );
  });

  it('goes with the last element that is removed, leaving the previous one and its own comment', () => {
    const text = '{\n  "include": [\n    "x", // about x\n    "y" // about y\n  ]\n}\n';

    expect(editJsonText(text, { include: ['x'] })).toBe('{\n  "include": [\n    "x" // about x\n  ]\n}\n');
  });

  it('goes with the key that is removed, while a comment on a line of its own stays', () => {
    const text = '{\n  // about the lib\n  "lib": ["es2024"], // pinned\n  "noEmit": true\n}\n';

    expect(editJsonText(text, { noEmit: true })).toBe('{\n  // about the lib\n  "noEmit": true\n}\n');
  });

  it('is not confused by a comma inside a comment between members', () => {
    expect(editJsonText('{ "a": [1 /* a, b */, 2] }\n', { a: [2] })).toBe('{ "a": [ 2] }\n');
    expect(editJsonText('{ "a": [1, 2 // x, y\n] }\n', { a: [1] })).toBe('{ "a": [1 \n] }\n');
    expect(editJsonText('{ "a": [1 // x, y\n, 2] }\n', { a: [2] })).toBe('{ "a": [\n 2] }\n');
  });

  it('ends a line comment at a lone carriage return, as the parser does', () => {
    const edited = editJsonText('{"a": [1 // x\r, 2]}', { a: [2] });

    expect(parseJson(edited)).toEqual({ a: [2] });
  });

  it('is kept through CRLF and tab-indented text', () => {
    const crlf = '{\r\n\t"a": [\r\n\t\t"x", // about x\r\n\t\t"y"\r\n\t]\r\n}\r\n';

    expect(editJsonText(crlf, { a: ['y'] })).toBe('{\r\n\t"a": [\r\n\t\t"y"\r\n\t]\r\n}\r\n');
  });

  it('removes the only member and the only element', () => {
    expect(editJsonText('{ "a": 1 }\n', {})).toBe('{  }\n');
    expect(editJsonText('{ "a": [1] }\n', { a: [] })).toBe('{ "a": [] }\n');
    expect(editJsonText('{ "a": [1,] }\n', { a: [] })).toBe('{ "a": [] }\n');
  });

  it('keeps a comment that leads the next member when the member before it is removed', () => {
    expect(editJsonText('{"a": 1, /* why b */ "b": 2}', { b: 2 })).toBe('{ /* why b */ "b": 2}');
    expect(editJsonText('{ "a": [1, /* why 2 */ 2] }', { a: [2] })).toBe('{ "a": [ /* why 2 */ 2] }');
    expect(editJsonText('{ "a": [1, /* x */ /* y */ 2] }', { a: [2] })).toBe('{ "a": [ /* x */ /* y */ 2] }');
  });

  it('still takes a block comment that trails the member after its comma, with nothing behind it', () => {
    expect(editJsonText('{\n  "a": 1, /* about a */\n  "b": 2\n}\n', { b: 2 })).toBe('{\n  "b": 2\n}\n');
  });
});

describe('a document the edit cannot honor faithfully', () => {
  it('is edited at the key a parser would read, when a key is repeated', () => {
    const edited = editJsonText('{ "a": { "x": 1 }, "a": { "x": 2 } }\n', { a: { x: 3 } });

    expect(parseJson(edited)).toEqual({ a: { x: 3 } });
  });

  it('removes every property of a repeated key, since one removal leaves the other in the document', () => {
    expect(parseJson(editJsonText('{ "a": 1, "b": 0, "a": 2 }\n', { b: 0 }))).toEqual({ b: 0 });
  });

  it('is refused rather than written when the text is not JSON', () => {
    expect(() => editJsonText('{ "a": ', { a: 1 })).toThrow('not valid JSON');
  });
});
