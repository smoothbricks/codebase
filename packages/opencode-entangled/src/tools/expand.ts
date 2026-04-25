import type { BlockIndex } from '../block-index.js';
import { defineTool, type InternalTool, type ZString, z } from '../tool-schema.js';

export function createExpandTool(index: BlockIndex): InternalTool<{ blockName: ZString }> {
  return defineTool({
    name: 'entangled_expand',
    description: 'Recursively expand all <<ref>> references in a block and return the fully assembled content',
    parameters: z.object({
      blockName: z.string().describe('The block name to expand'),
    }),
    execute: async (args: { blockName: string }) => {
      try {
        const content = index.expand(args.blockName);
        return {
          title: `Expanded <<${args.blockName}>>`,
          // WHY: plain text for expand — consumers want the assembled source, not JSON-escaped strings
          output: content,
          metadata: { expanded: true },
        };
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        return {
          title: `Expanded <<${args.blockName}>>`,
          output: `Error: ${message}`,
          metadata: { expanded: false, error: message },
        };
      }
    },
  });
}
