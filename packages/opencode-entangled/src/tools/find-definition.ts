import { z } from 'zod';
import type { BlockIndex } from '../block-index.js';

export function createFindDefinitionTool(index: BlockIndex) {
  return {
    name: 'entangled_find_definition',
    description: 'Find where a named block is defined — returns file, line, and content',
    parameters: z.object({
      blockName: z.string().describe('The block name to find the definition of'),
    }),
    execute: async (args: { blockName: string }) => {
      const def = index.findDefinition(args.blockName);
      if (!def) {
        return {
          title: `Definition of <<${args.blockName}>>`,
          output: `Block "${args.blockName}" not found`,
          metadata: { found: false },
        };
      }
      return {
        title: `Definition of <<${args.blockName}>>`,
        output: JSON.stringify(
          { file: def.file, line: def.line, language: def.language, content: def.content },
          null,
          2,
        ),
        metadata: { found: true },
      };
    },
  };
}
