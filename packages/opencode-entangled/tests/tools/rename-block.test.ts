import { beforeEach, describe, expect, it } from 'bun:test';
import { BlockIndex } from '../../src/block-index.js';
import type { FileIO, RunCommand } from '../../src/tools/absorb.js';
import { createRenameBlockTool } from '../../src/tools/rename-block.js';

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

const DEFINITION_MD = `# Signals

\`\`\`typescript {#signals file="src/signals.ts"}
export const signals = defineSignals({});
\`\`\`
`;

const REFERENCING_MD = `# Agent

\`\`\`typescript {#agent file="src/agent.ts"}
<<signals>>
<<state>>
\`\`\`

\`\`\`typescript {#state}
export const state = defineState({});
\`\`\`
`;

describe('entangled_rename_block', () => {
  let files: Map<string, string>;
  let index: BlockIndex;
  let io: FileIO;

  beforeEach(() => {
    files = new Map<string, string>();
    files.set('agents/signals.md', DEFINITION_MD);
    files.set('agents/agent.md', REFERENCING_MD);
    index = new BlockIndex();
    index.addFile('agents/signals.md', DEFINITION_MD);
    index.addFile('agents/agent.md', REFERENCING_MD);
    io = createMockIO(files);
  });

  it('renames the block definition', async () => {
    const tool = createRenameBlockTool(index, io, mockRunCommand);
    const result = await tool.execute({ oldName: 'signals', newName: 'signal-defs' });

    expect(result.metadata.success).toBe(true);

    const defMd = getFile(files, 'agents/signals.md');
    expect(defMd).toContain('#signal-defs');
    expect(defMd).not.toContain('#signals');
  });

  it('updates all <<ref>> references across files', async () => {
    const tool = createRenameBlockTool(index, io, mockRunCommand);
    const result = await tool.execute({ oldName: 'signals', newName: 'signal-defs' });

    expect(result.metadata.success).toBe(true);
    expect(result.metadata.referencesUpdated).toBe(1);
    expect(result.metadata.filesUpdated).toBe(2);

    const agentMd = getFile(files, 'agents/agent.md');
    expect(agentMd).toContain('<<signal-defs>>');
    expect(agentMd).not.toContain('<<signals>>');
    // WHY: other references should be untouched
    expect(agentMd).toContain('<<state>>');
  });

  it('rebuilds the index with new name', async () => {
    const tool = createRenameBlockTool(index, io, mockRunCommand);
    await tool.execute({ oldName: 'signals', newName: 'signal-defs' });

    // WHY: old name should no longer resolve, new name should
    expect(index.get('signals')).toBeUndefined();
    expect(index.get('signal-defs')).toBeDefined();
    expect(index.get('signal-defs')?.file).toBe('agents/signals.md');
  });

  it('returns error for nonexistent block', async () => {
    const tool = createRenameBlockTool(index, io, mockRunCommand);
    const result = await tool.execute({ oldName: 'nonexistent', newName: 'foo' });

    expect(result.metadata.success).toBe(false);
    expect(result.output).toContain('Block not found');
  });

  it('handles block with no references', async () => {
    const tool = createRenameBlockTool(index, io, mockRunCommand);
    // WHY: "state" is defined in agent.md and referenced in agent.md — both in the same file
    const result = await tool.execute({ oldName: 'state', newName: 'agent-state' });

    expect(result.metadata.success).toBe(true);

    const agentMd = getFile(files, 'agents/agent.md');
    expect(agentMd).toContain('#agent-state');
    expect(agentMd).toContain('<<agent-state>>');
    expect(agentMd).not.toContain('#state');
    expect(agentMd).not.toContain('<<state>>');
  });
});
