import { describe, expect, it } from 'bun:test';
import { parseFences } from '../src/fence-parser.js';

describe('parseFences', () => {
  it('extracts block with #name and file= attribute', () => {
    const md = [
      '```typescript {#signal-definitions file="src/signals/review.ts"}',
      'export const reviewSignals = defineSignals({});',
      '```',
    ].join('\n');

    const blocks = parseFences(md);
    expect(blocks).toHaveLength(1);
    expect(blocks[0]?.id).toBe('signal-definitions');
    expect(blocks[0]?.file).toBe('src/signals/review.ts');
    expect(blocks[0]?.language).toBe('typescript');
    expect(blocks[0]?.content).toBe('export const reviewSignals = defineSignals({});');
  });

  it('extracts block with only #name (no file)', () => {
    const md = ['```typescript {#helper-block}', 'const x = 1;', '```'].join('\n');

    const blocks = parseFences(md);
    expect(blocks).toHaveLength(1);
    expect(blocks[0]?.id).toBe('helper-block');
    expect(blocks[0]?.file).toBeUndefined();
  });

  it('extracts block with only file= (no #name)', () => {
    const md = ['```typescript {file="src/main.ts"}', 'console.log("hello");', '```'].join('\n');

    const blocks = parseFences(md);
    expect(blocks).toHaveLength(1);
    expect(blocks[0]?.id).toBeUndefined();
    expect(blocks[0]?.file).toBe('src/main.ts');
  });

  it('extracts <<ref>> references from body', () => {
    const md = [
      '```typescript {file="src/agent.ts"}',
      '<<signal-definitions>>',
      'export const agent = defineAgent({});',
      '```',
    ].join('\n');

    const blocks = parseFences(md);
    expect(blocks).toHaveLength(1);
    expect(blocks[0]?.refs).toEqual(['signal-definitions']);
  });

  it('extracts multiple <<ref>> references from one block', () => {
    const md = [
      '```typescript {file="src/agent.ts"}',
      '<<signal-definitions>>',
      '<<state-definitions>>',
      'export const agent = defineAgent({});',
      '```',
    ].join('\n');

    const blocks = parseFences(md);
    expect(blocks).toHaveLength(1);
    expect(blocks[0]?.refs).toEqual(['signal-definitions', 'state-definitions']);
  });

  it('ignores blocks without {…} attributes', () => {
    const md = ['```typescript', 'const x = 1;', '```', '', '```', 'plain block', '```'].join('\n');

    const blocks = parseFences(md);
    expect(blocks).toHaveLength(0);
  });

  it('parses multiple blocks in one file', () => {
    const md = [
      '# My Document',
      '',
      '```typescript {#block-a file="a.ts"}',
      'const a = 1;',
      '```',
      '',
      'Some prose here.',
      '',
      '```typescript {#block-b file="b.ts"}',
      'const b = 2;',
      '```',
    ].join('\n');

    const blocks = parseFences(md);
    expect(blocks).toHaveLength(2);
    expect(blocks[0]?.id).toBe('block-a');
    expect(blocks[0]?.file).toBe('a.ts');
    expect(blocks[1]?.id).toBe('block-b');
    expect(blocks[1]?.file).toBe('b.ts');
  });

  it('extracts language info string', () => {
    const md = ['```python {#my-script file="run.py"}', 'print("hello")', '```'].join('\n');

    const blocks = parseFences(md);
    expect(blocks).toHaveLength(1);
    expect(blocks[0]?.language).toBe('python');
  });

  it('reports correct 1-indexed line numbers', () => {
    const md = [
      '# Title',
      '',
      'Some text.',
      '',
      '```typescript {#first file="a.ts"}',
      'const a = 1;',
      '```',
      '',
      '```typescript {#second file="b.ts"}',
      'const b = 2;',
      '```',
    ].join('\n');

    const blocks = parseFences(md);
    expect(blocks).toHaveLength(2);
    expect(blocks[0]?.line).toBe(5);
    expect(blocks[1]?.line).toBe(9);
  });

  it('block content excludes fence open/close lines', () => {
    const md = ['```typescript {#test file="test.ts"}', 'line one', 'line two', 'line three', '```'].join('\n');

    const blocks = parseFences(md);
    expect(blocks).toHaveLength(1);
    expect(blocks[0]?.content).toBe('line one\nline two\nline three');
  });

  it('handles <<ref>> with surrounding whitespace', () => {
    const md = ['```typescript {file="src/main.ts"}', '  <<indented-ref>>  ', '```'].join('\n');

    const blocks = parseFences(md);
    expect(blocks[0]?.refs).toEqual(['indented-ref']);
  });

  it('ignores blocks with attributes but no #id and no file=', () => {
    // WHY: fenceparser highlight syntax like {1,3-5} is not Entangled
    const md = ['```typescript {data-x=42}', 'const x = 1;', '```'].join('\n');

    const blocks = parseFences(md);
    expect(blocks).toHaveLength(0);
  });

  it('handles empty block body', () => {
    const md = ['```typescript {#empty file="empty.ts"}', '```'].join('\n');

    const blocks = parseFences(md);
    expect(blocks).toHaveLength(1);
    expect(blocks[0]?.content).toBe('');
    expect(blocks[0]?.refs).toEqual([]);
  });

  it('does not treat inline <<ref>> as a noweb reference', () => {
    // WHY: only lines that are solely a <<ref>> (with optional whitespace) count
    const md = ['```typescript {file="src/main.ts"}', 'const x = get(<<not-a-ref>>);', '```'].join('\n');

    const blocks = parseFences(md);
    expect(blocks[0]?.refs).toEqual([]);
  });
});
