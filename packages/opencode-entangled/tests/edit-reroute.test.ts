import { beforeEach, describe, expect, it } from 'bun:test';
import { BlockIndex } from '../src/block-index.js';
import { createEditRerouteHook } from '../src/hooks/edit-reroute.js';

function md(...lines: string[]): string {
  return lines.join('\n');
}

describe('createEditRerouteHook', () => {
  let index: BlockIndex;
  let hook: ReturnType<typeof createEditRerouteHook>;

  beforeEach(() => {
    index = new BlockIndex();
    hook = createEditRerouteHook(index);
  });

  function makeInput(tool: string) {
    return { tool, sessionID: 'sess-1', callID: 'call-1' };
  }

  // --- Test 1: edit targeting .md at line inside a code block -> rerouted to .ts target ---

  it('reroutes edit targeting .md code block to .ts tangle target', async () => {
    index.addFile(
      'docs/guide.md',
      md(
        '# Guide',
        '',
        '```typescript {#greet file="src/greet.ts"}',
        'export function greet() {',
        '  return "hello";',
        '}',
        '```',
      ),
    );

    const output = {
      args: {
        filePath: 'docs/guide.md',
        oldString: 'return "hello"',
        newString: 'return "world"',
      },
    };

    await hook(makeInput('edit'), output);

    expect(output.args.filePath).toBe('src/greet.ts');
  });

  // --- Test 2: edit targeting .md at line outside code blocks -> unchanged ---

  it('passes through edit targeting .md prose outside code blocks', async () => {
    index.addFile(
      'docs/guide.md',
      md(
        '# Guide',
        '',
        'This is prose text.',
        '',
        '```typescript {#greet file="src/greet.ts"}',
        'export function greet() {}',
        '```',
      ),
    );

    const output = {
      args: {
        filePath: 'docs/guide.md',
        oldString: 'This is prose text.',
        newString: 'Updated prose.',
      },
    };

    await hook(makeInput('edit'), output);

    expect(output.args.filePath).toBe('docs/guide.md');
  });

  // --- Test 3: edit targeting .ts tangle target -> unchanged (pass through) ---

  it('passes through edit targeting .ts tangle target', async () => {
    const output = {
      args: {
        filePath: 'src/greet.ts',
        oldString: 'return "hello"',
        newString: 'return "world"',
      },
    };

    await hook(makeInput('edit'), output);

    expect(output.args.filePath).toBe('src/greet.ts');
  });

  // --- Test 4: edit targeting non-managed file -> unchanged ---

  it('passes through edit targeting non-managed file', async () => {
    const output = {
      args: {
        filePath: 'package.json',
        oldString: '"version": "1.0"',
        newString: '"version": "2.0"',
      },
    };

    await hook(makeInput('edit'), output);

    expect(output.args.filePath).toBe('package.json');
  });

  // --- Test 5: write tool targeting .md -> rerouted same as edit ---

  it('reroutes write targeting .md with tangled blocks to .ts target', async () => {
    index.addFile(
      'docs/guide.md',
      md('# Guide', '', '```typescript {#greet file="src/greet.ts"}', 'export function greet() {}', '```'),
    );

    const output = {
      args: {
        filePath: 'docs/guide.md',
        content: 'export function greet() { return "new"; }',
      },
    };

    await hook(makeInput('write'), output);

    expect(output.args.filePath).toBe('src/greet.ts');
  });

  // --- Test 6: read tool -> not intercepted ---

  it('does not intercept read tool', async () => {
    index.addFile(
      'docs/guide.md',
      md('```typescript {#greet file="src/greet.ts"}', 'export function greet() {}', '```'),
    );

    const output = {
      args: { filePath: 'docs/guide.md' },
    };

    await hook(makeInput('read'), output);

    expect(output.args.filePath).toBe('docs/guide.md');
  });

  // --- Test 7: bash tool -> not intercepted ---

  it('does not intercept bash tool', async () => {
    const output = {
      args: { command: 'cat docs/guide.md' },
    };

    await hook(makeInput('bash'), output);

    expect(output.args.command).toBe('cat docs/guide.md');
  });

  // --- Test 8: .md block with no file= target -> not rerouted ---

  it('does not reroute edit when .md block has no file= target', async () => {
    index.addFile('docs/guide.md', md('```typescript {#snippet}', 'const x = 1;', '```'));

    const output = {
      args: {
        filePath: 'docs/guide.md',
        oldString: 'const x = 1;',
        newString: 'const x = 2;',
      },
    };

    await hook(makeInput('edit'), output);

    // WHY: block has #id but no file= attribute, so there's no tangle target to reroute to
    expect(output.args.filePath).toBe('docs/guide.md');
  });

  // --- Additional edge cases ---

  it('reroutes to correct target when multiple blocks exist', async () => {
    index.addFile(
      'docs/guide.md',
      md(
        '```typescript {#a file="src/a.ts"}',
        'const a = 1;',
        '```',
        '',
        '```typescript {#b file="src/b.ts"}',
        'const b = 2;',
        '```',
      ),
    );

    const output = {
      args: {
        filePath: 'docs/guide.md',
        oldString: 'const b = 2;',
        newString: 'const b = 3;',
      },
    };

    await hook(makeInput('edit'), output);

    expect(output.args.filePath).toBe('src/b.ts');
  });

  it('passes through when .md file has no blocks indexed', async () => {
    const output = {
      args: {
        filePath: 'docs/plain.md',
        oldString: 'some text',
        newString: 'new text',
      },
    };

    await hook(makeInput('edit'), output);

    expect(output.args.filePath).toBe('docs/plain.md');
  });
});
