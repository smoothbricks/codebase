import type { BlockIndex } from '../block-index.js';
import { defineTool, type InternalTool, type ZString, z } from '../tool-schema.js';

export function createListBlocksTool(index: BlockIndex): InternalTool<{ filePath: ZString }> {
  return defineTool({
    name: 'entangled_list_blocks',
    description: 'List all code blocks in a markdown file — shows id, language, target, and line number',
    parameters: z.object({
      filePath: z.string().describe('Path to the markdown file to list blocks from'),
    }),
    execute: async (args: { filePath: string }) => {
      const blocks = index.listBlocks(args.filePath);
      return {
        title: `Blocks in ${args.filePath}`,
        output: JSON.stringify(
          blocks.map((b) => ({
            id: b.id,
            line: b.line,
            language: b.language,
            target: b.target,
            refs: b.refs,
          })),
          null,
          2,
        ),
        metadata: { count: blocks.length },
      };
    },
  });
}
