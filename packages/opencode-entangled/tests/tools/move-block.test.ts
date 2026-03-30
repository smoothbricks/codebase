import { beforeEach, describe, expect, it } from 'bun:test';
import { BlockIndex } from '../../src/block-index.js';
import type { FileIO, RunCommand } from '../../src/tools/absorb.js';
import { createMoveBlockTool } from '../../src/tools/move-block.js';

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

const SOURCE_MD = `# Signals

\`\`\`typescript {#signals file="src/signals.ts"}
export const signals = defineSignals({
  foo: { value: {} },
});
\`\`\`

\`\`\`typescript {#helpers}
function helper() { return 42; }
\`\`\`
`;

const TARGET_MD = `# Agent

\`\`\`typescript {#state}
export const state = defineState({});
\`\`\`
`;

const REFS_MD = `# Entry

\`\`\`typescript {#entry file="src/entry.ts"}
<<signals>>
<<state>>
\`\`\`
`;

describe('entangled_move_block', () => {
  let files: Map<string, string>;
  let index: BlockIndex;
  let io: FileIO;

  beforeEach(() => {
    files = new Map<string, string>();
    files.set('agents/signals.md', SOURCE_MD);
    files.set('agents/agent.md', TARGET_MD);
    files.set('agents/refs.md', REFS_MD);
    index = new BlockIndex();
    index.addFile('agents/signals.md', SOURCE_MD);
    index.addFile('agents/agent.md', TARGET_MD);
    index.addFile('agents/refs.md', REFS_MD);
    io = createMockIO(files);
  });

  it('moves a block from one file to another', async () => {
    const tool = createMoveBlockTool(index, io, mockRunCommand);
    const result = await tool.execute({
      blockName: 'signals',
      targetMd: 'agents/agent.md',
    });

    expect(result.metadata.success).toBe(true);
    expect(result.metadata.from).toBe('agents/signals.md');
    expect(result.metadata.to).toBe('agents/agent.md');

    // WHY: block should be removed from source
    const source = getFile(files, 'agents/signals.md');
    expect(source).not.toContain('#signals');
    expect(source).not.toContain('defineSignals');
    // WHY: the helpers block should remain untouched
    expect(source).toContain('#helpers');

    // WHY: block should appear in target
    const target = getFile(files, 'agents/agent.md');
    expect(target).toContain('```typescript {#signals file="src/signals.ts"}');
    expect(target).toContain('defineSignals');
  });

  it('moves a block within the same file (reorder via insertAfter)', async () => {
    const tool = createMoveBlockTool(index, io, mockRunCommand);
    const result = await tool.execute({
      blockName: 'signals',
      targetMd: 'agents/signals.md',
      insertAfter: 'helpers',
    });

    expect(result.metadata.success).toBe(true);

    const md = getFile(files, 'agents/signals.md');
    const lines = md.split('\n');
    const helpersIdx = lines.findIndex((l) => l.includes('#helpers'));
    const signalsIdx = lines.findIndex((l) => l.includes('#signals'));
    // WHY: signals should now appear after helpers
    expect(signalsIdx).toBeGreaterThan(helpersIdx);
  });

  it('inserts after a specific block in the target file', async () => {
    const tool = createMoveBlockTool(index, io, mockRunCommand);
    const result = await tool.execute({
      blockName: 'signals',
      targetMd: 'agents/agent.md',
      insertAfter: 'state',
    });

    expect(result.metadata.success).toBe(true);

    const target = getFile(files, 'agents/agent.md');
    const lines = target.split('\n');
    const stateIdx = lines.findIndex((l) => l.includes('#state'));
    const signalsIdx = lines.findIndex((l) => l.includes('#signals'));
    expect(signalsIdx).toBeGreaterThan(stateIdx);
  });

  it('returns error when block is not found', async () => {
    const tool = createMoveBlockTool(index, io, mockRunCommand);
    const result = await tool.execute({
      blockName: 'nonexistent',
      targetMd: 'agents/agent.md',
    });

    expect(result.metadata.success).toBe(false);
    expect(result.output).toContain('Block not found: nonexistent');
  });

  it('returns error when insertAfter block is not found', async () => {
    const tool = createMoveBlockTool(index, io, mockRunCommand);
    const result = await tool.execute({
      blockName: 'signals',
      targetMd: 'agents/agent.md',
      insertAfter: 'nonexistent',
    });

    expect(result.metadata.success).toBe(false);
    expect(result.output).toContain('insertAfter block not found: nonexistent');
  });

  it('preserves <<ref>> references in other blocks', async () => {
    const tool = createMoveBlockTool(index, io, mockRunCommand);
    await tool.execute({
      blockName: 'signals',
      targetMd: 'agents/agent.md',
    });

    // WHY: the refs file should still contain <<signals>> — moving a block doesn't rename references
    const refs = getFile(files, 'agents/refs.md');
    expect(refs).toContain('<<signals>>');
    expect(refs).toContain('<<state>>');
  });

  it('rebuilds the index for both source and target files', async () => {
    const tool = createMoveBlockTool(index, io, mockRunCommand);
    await tool.execute({
      blockName: 'signals',
      targetMd: 'agents/agent.md',
    });

    // WHY: the index should reflect the block's new location
    const signalsBlock = index.get('signals');
    expect(signalsBlock).toBeDefined();
    expect(signalsBlock?.file).toBe('agents/agent.md');
  });

  it('creates target file when it does not exist', async () => {
    const tool = createMoveBlockTool(index, io, mockRunCommand);
    const result = await tool.execute({
      blockName: 'signals',
      targetMd: 'agents/new-file.md',
    });

    expect(result.metadata.success).toBe(true);

    const newFile = getFile(files, 'agents/new-file.md');
    expect(newFile).toContain('#signals');
    expect(newFile).toContain('defineSignals');
  });
});
