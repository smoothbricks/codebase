import { beforeEach, describe, expect, it } from 'bun:test';
import { BlockIndex } from '../../src/block-index.js';
import type { FileIO, RunCommand } from '../../src/tools/absorb.js';
import { createAbsorbTool } from '../../src/tools/absorb.js';

function createMockIO(files: Map<string, string>): FileIO {
  return {
    readFile: async (path: string) => {
      const content = files.get(path);
      if (content === undefined) throw new Error(`File not found: ${path}`);
      return content;
    },
    writeFile: async (path: string, content: string) => {
      files.set(path, content);
    },
  };
}

/** Get a file from the map, throwing if missing (avoids non-null assertions) */
function getFile(files: Map<string, string>, path: string): string {
  const content = files.get(path);
  if (content === undefined) throw new Error(`Expected file ${path} in mock fs`);
  return content;
}

const mockRunCommand: RunCommand = async () => ({ stdout: '', stderr: '', exitCode: 0 });

const SOURCE_TS = `import { createHooks } from '@example/core';

export const hooks = createHooks({
  onInit: () => {},
});

export const other = 42;
`;

const TARGET_MD = `# Agent

\`\`\`typescript {#state}
export const state = defineState({});
\`\`\`
`;

describe('entangled_absorb', () => {
  let files: Map<string, string>;
  let index: BlockIndex;
  let io: FileIO;

  beforeEach(() => {
    files = new Map<string, string>();
    files.set('src/hooks.ts', SOURCE_TS);
    files.set('agents/review.md', TARGET_MD);
    index = new BlockIndex();
    index.addFile('agents/review.md', TARGET_MD);
    io = createMockIO(files);
  });

  it('creates a new block at end of file when no insertAfter', async () => {
    const tool = createAbsorbTool(index, io, mockRunCommand);
    const result = await tool.execute({
      sourcePath: 'src/hooks.ts',
      startLine: 3,
      endLine: 5,
      targetMd: 'agents/review.md',
      blockName: 'hooks',
    });

    expect(result.metadata.success).toBe(true);
    expect(result.metadata.linesAbsorbed).toBe(3);

    // Verify the new block was appended to the markdown
    const md = getFile(files, 'agents/review.md');
    expect(md).toContain('```typescript {#hooks}');
    expect(md).toContain('export const hooks = createHooks({');
    expect(md).toContain('  onInit: () => {},');
    expect(md).toContain('});');

    // Verify source .ts has tangle marker replacing the absorbed lines
    const ts = getFile(files, 'src/hooks.ts');
    expect(ts).toContain('// ~/~ begin <<agents/review.md#hooks>>[0]');
    expect(ts).toContain('// ~/~ end');
    // WHY: the import and the `other` export should remain
    expect(ts).toContain("import { createHooks } from '@example/core';");
    expect(ts).toContain('export const other = 42;');
  });

  it('creates a new block after a specified block', async () => {
    const tool = createAbsorbTool(index, io, mockRunCommand);
    const result = await tool.execute({
      sourcePath: 'src/hooks.ts',
      startLine: 3,
      endLine: 5,
      targetMd: 'agents/review.md',
      blockName: 'hooks',
      insertAfter: 'state',
    });

    expect(result.metadata.success).toBe(true);

    const md = getFile(files, 'agents/review.md');
    const lines = md.split('\n');
    // WHY: the new block should appear after the state block's closing fence
    const stateCloseIdx = lines.findIndex(
      (l, i) => i > 0 && l.trim() === '```' && lines[i - 1]?.includes('defineState'),
    );
    const hooksOpenIdx = lines.findIndex((l) => l.includes('#hooks'));
    expect(hooksOpenIdx).toBeGreaterThan(stateCloseIdx);
  });

  it('appends to an existing block', async () => {
    const tool = createAbsorbTool(index, io, mockRunCommand);
    const result = await tool.execute({
      sourcePath: 'src/hooks.ts',
      startLine: 3,
      endLine: 5,
      targetMd: 'agents/review.md',
      blockName: 'state',
      appendTo: 'state',
    });

    expect(result.metadata.success).toBe(true);

    const md = getFile(files, 'agents/review.md');
    // WHY: the state block should now contain both the original content and the absorbed lines
    expect(md).toContain('export const state = defineState({});');
    expect(md).toContain('export const hooks = createHooks({');
  });

  it('adds prose before a new block', async () => {
    const tool = createAbsorbTool(index, io, mockRunCommand);
    await tool.execute({
      sourcePath: 'src/hooks.ts',
      startLine: 3,
      endLine: 5,
      targetMd: 'agents/review.md',
      blockName: 'hooks',
      prose: '## Hooks\n\nThe hook definitions:',
    });

    const md = getFile(files, 'agents/review.md');
    expect(md).toContain('## Hooks');
    expect(md).toContain('The hook definitions:');
    // WHY: prose should appear before the code block
    const proseIdx = md.indexOf('## Hooks');
    const fenceIdx = md.indexOf('```typescript {#hooks}');
    expect(fenceIdx).toBeGreaterThan(proseIdx);
  });

  it('returns error for source file not found', async () => {
    const tool = createAbsorbTool(index, io, mockRunCommand);
    const result = await tool.execute({
      sourcePath: 'nonexistent.ts',
      startLine: 1,
      endLine: 1,
      targetMd: 'agents/review.md',
      blockName: 'test',
    });

    expect(result.metadata.success).toBe(false);
    expect(result.output).toContain('Source file not found');
  });

  it('returns error for invalid line range', async () => {
    const tool = createAbsorbTool(index, io, mockRunCommand);
    const result = await tool.execute({
      sourcePath: 'src/hooks.ts',
      startLine: 100,
      endLine: 200,
      targetMd: 'agents/review.md',
      blockName: 'test',
    });

    expect(result.metadata.success).toBe(false);
    expect(result.output).toContain('Invalid line range');
  });

  it('returns error for appendTo with nonexistent block', async () => {
    const tool = createAbsorbTool(index, io, mockRunCommand);
    const result = await tool.execute({
      sourcePath: 'src/hooks.ts',
      startLine: 1,
      endLine: 1,
      targetMd: 'agents/review.md',
      blockName: 'test',
      appendTo: 'nonexistent',
    });

    expect(result.metadata.success).toBe(false);
    expect(result.output).toContain('Block not found for appendTo');
  });

  it('rebuilds the index after absorbing', async () => {
    const tool = createAbsorbTool(index, io, mockRunCommand);
    await tool.execute({
      sourcePath: 'src/hooks.ts',
      startLine: 3,
      endLine: 5,
      targetMd: 'agents/review.md',
      blockName: 'hooks',
    });

    // WHY: after absorb the index should contain the new "hooks" block
    const hooksBlock = index.get('hooks');
    expect(hooksBlock).toBeDefined();
    expect(hooksBlock?.file).toBe('agents/review.md');
  });
});
