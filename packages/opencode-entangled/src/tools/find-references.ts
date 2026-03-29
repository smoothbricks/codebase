import { z } from 'zod';
import type { BlockIndex } from '../block-index.js';

export function createFindReferencesTool(index: BlockIndex) {
  return {
    name: 'entangled_find_references',
    description: 'Find all <<ref>> references to a named block across all markdown files',
    parameters: z.object({
      blockName: z.string().describe('The block name to search for references to'),
    }),
    execute: async (args: { blockName: string }) => {
      const refs = index.findReferences(args.blockName);
      return {
        title: `References to <<${args.blockName}>>`,
        output: JSON.stringify(
          refs.map((r) => ({ file: r.file, line: r.line, id: r.id })),
          null,
          2,
        ),
        metadata: { count: refs.length },
      };
    },
  };
}
