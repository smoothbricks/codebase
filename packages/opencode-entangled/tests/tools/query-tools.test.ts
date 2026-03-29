import { beforeEach, describe, expect, it } from 'bun:test';
import { BlockIndex } from '../../src/block-index.js';
import { createBlockDependentsTool } from '../../src/tools/block-dependents.js';
import { createExpandTool } from '../../src/tools/expand.js';
import { createFindDefinitionTool } from '../../src/tools/find-definition.js';
import { createFindReferencesTool } from '../../src/tools/find-references.js';
import { createListBlocksTool } from '../../src/tools/list-blocks.js';
import { createListTargetsTool } from '../../src/tools/list-targets.js';

const TEST_MD = `\`\`\`typescript {#signals file="src/signals.ts"}
export const signals = defineSignals({});
\`\`\`

\`\`\`typescript {file="src/agent.ts"}
<<signals>>
<<state>>
export const agent = defineAgent({});
\`\`\`

\`\`\`typescript {#state}
export const state = defineState({});
\`\`\``;

describe('query tools', () => {
  let index: BlockIndex;

  beforeEach(() => {
    index = new BlockIndex();
    index.addFile('agents/review.md', TEST_MD);
  });

  // --- find-references ---

  describe('entangled_find_references', () => {
    it('finds blocks that reference a named block', async () => {
      const tool = createFindReferencesTool(index);
      const result = await tool.execute({ blockName: 'signals' });

      expect(result.title).toBe('References to <<signals>>');
      expect(result.metadata.count).toBe(1);

      const refs = JSON.parse(result.output);
      expect(refs).toHaveLength(1);
      expect(refs[0].file).toBe('agents/review.md');
    });

    it('returns empty for unreferenced block', async () => {
      const tool = createFindReferencesTool(index);
      const result = await tool.execute({ blockName: 'nonexistent' });

      expect(result.metadata.count).toBe(0);
      expect(JSON.parse(result.output)).toEqual([]);
    });
  });

  // --- find-definition ---

  describe('entangled_find_definition', () => {
    it('returns the block where a name is defined', async () => {
      const tool = createFindDefinitionTool(index);
      const result = await tool.execute({ blockName: 'signals' });

      expect(result.title).toBe('Definition of <<signals>>');
      expect(result.metadata.found).toBe(true);

      const def = JSON.parse(result.output);
      expect(def.file).toBe('agents/review.md');
      expect(def.line).toBe(1);
      expect(def.language).toBe('typescript');
      expect(def.content).toBe('export const signals = defineSignals({});');
    });

    it('returns not-found for unknown block', async () => {
      const tool = createFindDefinitionTool(index);
      const result = await tool.execute({ blockName: 'ghost' });

      expect(result.metadata.found).toBe(false);
      expect(result.output).toContain('not found');
    });
  });

  // --- list-blocks ---

  describe('entangled_list_blocks', () => {
    it('lists all blocks in a markdown file', async () => {
      const tool = createListBlocksTool(index);
      const result = await tool.execute({ filePath: 'agents/review.md' });

      expect(result.metadata.count).toBe(3);

      const blocks = JSON.parse(result.output);
      expect(blocks).toHaveLength(3);
      // WHY: first block has id "signals", second has no id (assembly block), third has id "state"
      expect(blocks[0].id).toBe('signals');
      expect(blocks[1].id).toBeUndefined();
      expect(blocks[2].id).toBe('state');
    });

    it('returns empty for unknown file', async () => {
      const tool = createListBlocksTool(index);
      const result = await tool.execute({ filePath: 'nope.md' });

      expect(result.metadata.count).toBe(0);
      expect(JSON.parse(result.output)).toEqual([]);
    });
  });

  // --- expand ---

  describe('entangled_expand', () => {
    it('recursively expands <<ref>> and returns full content', async () => {
      const tool = createExpandTool(index);
      const result = await tool.execute({ blockName: 'signals' });

      expect(result.metadata.expanded).toBe(true);
      // WHY: signals has no refs, so output is just the block content
      expect(result.output).toBe('export const signals = defineSignals({});');
    });

    it('returns error for unknown block', async () => {
      const tool = createExpandTool(index);
      const result = await tool.execute({ blockName: 'missing' });

      expect(result.metadata.expanded).toBe(false);
      expect(result.output).toContain('Error');
    });
  });

  // --- list-targets ---

  describe('entangled_list_targets', () => {
    it('lists all file= tangle targets', async () => {
      const tool = createListTargetsTool(index);
      const result = await tool.execute({});

      expect(result.metadata.count).toBe(2);

      const targets = JSON.parse(result.output) as string[];
      expect(targets.sort()).toEqual(['src/agent.ts', 'src/signals.ts']);
    });

    it('returns empty on empty index', async () => {
      const emptyIndex = new BlockIndex();
      const tool = createListTargetsTool(emptyIndex);
      const result = await tool.execute({});

      expect(result.metadata.count).toBe(0);
      expect(JSON.parse(result.output)).toEqual([]);
    });
  });

  // --- block-dependents ---

  describe('entangled_block_dependents', () => {
    it('finds blocks that depend on a named block', async () => {
      const tool = createBlockDependentsTool(index);
      const result = await tool.execute({ blockName: 'signals' });

      expect(result.metadata.count).toBe(1);

      const deps = JSON.parse(result.output);
      expect(deps).toHaveLength(1);
      expect(deps[0].file).toBe('agents/review.md');
    });

    it('returns empty for block with no dependents', async () => {
      const tool = createBlockDependentsTool(index);
      // WHY: the assembly block (no id) references signals and state, but nothing references it
      const result = await tool.execute({ blockName: 'nonexistent' });

      expect(result.metadata.count).toBe(0);
    });
  });
});
