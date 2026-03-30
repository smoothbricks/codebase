import { beforeEach, describe, expect, it } from 'bun:test';
import { BlockIndex } from '../src/block-index.js';

function md(...lines: string[]): string {
  return lines.join('\n');
}

describe('BlockIndex', () => {
  let idx: BlockIndex;

  beforeEach(() => {
    idx = new BlockIndex();
  });

  // --- addFile ---

  it('addFile indexes blocks from markdown, get() returns them', () => {
    idx.addFile('doc.md', md('```typescript {#greet file="src/greet.ts"}', 'export function greet() {}', '```'));

    const entry = idx.get('greet');
    expect(entry).toBeDefined();
    expect(entry?.id).toBe('greet');
    expect(entry?.file).toBe('doc.md');
    expect(entry?.target).toBe('src/greet.ts');
    expect(entry?.content).toBe('export function greet() {}');
    expect(entry?.language).toBe('typescript');
    expect(entry?.line).toBe(1);
  });

  it('addFile with same path twice replaces previous blocks', () => {
    idx.addFile('doc.md', md('```ts {#v1 file="a.ts"}', 'v1', '```'));
    expect(idx.get('v1')).toBeDefined();

    idx.addFile('doc.md', md('```ts {#v2 file="b.ts"}', 'v2', '```'));
    expect(idx.get('v1')).toBeUndefined();
    expect(idx.get('v2')).toBeDefined();
    expect(idx.get('v2')?.content).toBe('v2');
  });

  // --- removeFile ---

  it('removeFile clears blocks from that file', () => {
    idx.addFile('doc.md', md('```ts {#alpha file="a.ts"}', 'a', '```'));
    expect(idx.get('alpha')).toBeDefined();

    idx.removeFile('doc.md');
    expect(idx.get('alpha')).toBeUndefined();
    expect(idx.listBlocks('doc.md')).toEqual([]);
    expect(idx.getByTarget('a.ts')).toEqual([]);
  });

  // --- get ---

  it('get returns undefined for unknown id', () => {
    expect(idx.get('nonexistent')).toBeUndefined();
  });

  it('get returns the defining block by id', () => {
    idx.addFile('doc.md', md('```ts {#def file="d.ts"}', 'x', '```'));
    const def = idx.get('def');
    expect(def).toBeDefined();
    expect(def?.id).toBe('def');
  });

  // --- getByTarget ---

  it('getByTarget finds blocks by tangle target path', () => {
    idx.addFile(
      'doc.md',
      md('```ts {#a file="out.ts"}', 'const a = 1;', '```', '', '```ts {#b file="out.ts"}', 'const b = 2;', '```'),
    );

    const blocks = idx.getByTarget('out.ts');
    expect(blocks).toHaveLength(2);
    expect(blocks.map((b) => b.id)).toEqual(['a', 'b']);
  });

  it('getByTarget returns empty array for unknown target', () => {
    expect(idx.getByTarget('nope.ts')).toEqual([]);
  });

  // --- listTargets ---

  it('listTargets returns all unique file= targets', () => {
    idx.addFile(
      'doc.md',
      md(
        '```ts {#a file="x.ts"}',
        'a',
        '```',
        '',
        '```ts {#b file="y.ts"}',
        'b',
        '```',
        '',
        '```ts {#c file="x.ts"}',
        'c',
        '```',
      ),
    );

    const targets = idx.listTargets().sort();
    expect(targets).toEqual(['x.ts', 'y.ts']);
  });

  it('listTargets returns empty array on empty index', () => {
    expect(idx.listTargets()).toEqual([]);
  });

  // --- findReferences ---

  it('findReferences finds blocks containing <<blockId>>', () => {
    idx.addFile(
      'doc.md',
      md(
        '```ts {#helper}',
        'const h = 1;',
        '```',
        '',
        '```ts {#main file="main.ts"}',
        '<<helper>>',
        'const m = 2;',
        '```',
      ),
    );

    const refs = idx.findReferences('helper');
    expect(refs).toHaveLength(1);
    expect(refs[0]?.id).toBe('main');
  });

  it('findReferences returns empty for unreferenced block', () => {
    idx.addFile('doc.md', md('```ts {#lonely}', 'alone', '```'));
    expect(idx.findReferences('lonely')).toEqual([]);
  });

  // --- listBlocks ---

  it('listBlocks returns all blocks in a specific .md file', () => {
    idx.addFile('a.md', md('```ts {#x file="x.ts"}', 'x', '```', '', '```ts {#y file="y.ts"}', 'y', '```'));
    idx.addFile('b.md', md('```ts {#z file="z.ts"}', 'z', '```'));

    const aBlocks = idx.listBlocks('a.md');
    expect(aBlocks).toHaveLength(2);
    expect(aBlocks.map((b) => b.id)).toEqual(['x', 'y']);

    const bBlocks = idx.listBlocks('b.md');
    expect(bBlocks).toHaveLength(1);
  });

  it('listBlocks returns empty for unknown file', () => {
    expect(idx.listBlocks('nope.md')).toEqual([]);
  });

  // --- expand ---

  it('expand recursively resolves <<ref>> into full content', () => {
    idx.addFile(
      'doc.md',
      md(
        '```ts {#imports}',
        'import { foo } from "./foo";',
        '```',
        '',
        '```ts {#body}',
        'foo();',
        '```',
        '',
        '```ts {#main file="main.ts"}',
        '<<imports>>',
        '',
        '<<body>>',
        '```',
      ),
    );

    const expanded = idx.expand('main');
    expect(expanded).toBe('import { foo } from "./foo";\n\nfoo();');
  });

  it('expand preserves indentation of <<ref>> lines', () => {
    idx.addFile(
      'doc.md',
      md(
        '```ts {#inner}',
        'line1();',
        'line2();',
        '```',
        '',
        '```ts {#outer file="out.ts"}',
        'if (true) {',
        '  <<inner>>',
        '}',
        '```',
      ),
    );

    const expanded = idx.expand('outer');
    expect(expanded).toBe('if (true) {\n  line1();\n  line2();\n}');
  });

  it('expand throws on circular reference', () => {
    idx.addFile('doc.md', md('```ts {#a}', '<<b>>', '```', '', '```ts {#b}', '<<a>>', '```'));

    expect(() => idx.expand('a')).toThrow(/[Cc]ircular/);
  });

  it('expand throws on unknown block reference', () => {
    idx.addFile('doc.md', md('```ts {#x file="x.ts"}', '<<missing>>', '```'));
    expect(() => idx.expand('x')).toThrow(/not found/i);
  });

  // --- dependents ---

  it('dependents returns transitive reverse dependencies', () => {
    idx.addFile(
      'doc.md',
      md(
        '```ts {#leaf}',
        'const leaf = 1;',
        '```',
        '',
        '```ts {#mid}',
        '<<leaf>>',
        '```',
        '',
        '```ts {#root file="root.ts"}',
        '<<mid>>',
        '```',
      ),
    );

    const deps = idx.dependents('leaf');
    expect(deps).toHaveLength(2);
    const ids = deps.map((d) => d.id).sort();
    expect(ids).toEqual(['mid', 'root']);
  });

  it('dependents returns empty for block with no dependents', () => {
    idx.addFile('doc.md', md('```ts {#alone file="a.ts"}', 'x', '```'));
    expect(idx.dependents('alone')).toEqual([]);
  });

  // --- edge cases ---

  it('block with no id is indexed by file but not by id', () => {
    idx.addFile('doc.md', md('```ts {file="anon.ts"}', 'anon content', '```'));

    expect(idx.listBlocks('doc.md')).toHaveLength(1);
    expect(idx.listBlocks('doc.md')[0]?.target).toBe('anon.ts');
    expect(idx.getByTarget('anon.ts')).toHaveLength(1);
    // No id, so get() shouldn't find it
    expect(idx.get('anon.ts')).toBeUndefined();
  });

  it('multiple blocks targeting the same file from different source files', () => {
    idx.addFile('a.md', md('```ts {#a1 file="shared.ts"}', 'a1', '```'));
    idx.addFile('b.md', md('```ts {#b1 file="shared.ts"}', 'b1', '```'));

    const shared = idx.getByTarget('shared.ts');
    expect(shared).toHaveLength(2);
    expect(shared.map((b) => b.id).sort()).toEqual(['a1', 'b1']);
  });

  it('removeFile for a file with shared target does not affect other files', () => {
    idx.addFile('a.md', md('```ts {#a1 file="shared.ts"}', 'a1', '```'));
    idx.addFile('b.md', md('```ts {#b1 file="shared.ts"}', 'b1', '```'));

    idx.removeFile('a.md');
    expect(idx.getByTarget('shared.ts')).toHaveLength(1);
    expect(idx.getByTarget('shared.ts')[0]?.id).toBe('b1');
  });

  it('empty index returns sensible defaults everywhere', () => {
    expect(idx.get('x')).toBeUndefined();
    expect(idx.getByTarget('x')).toEqual([]);
    expect(idx.listTargets()).toEqual([]);
    expect(idx.findReferences('x')).toEqual([]);
    expect(idx.get('x')).toBeUndefined();
    expect(idx.listBlocks('x.md')).toEqual([]);
  });
});
