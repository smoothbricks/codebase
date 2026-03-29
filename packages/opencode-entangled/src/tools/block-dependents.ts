import { z } from 'zod';
import type { BlockIndex } from '../block-index.js';

export function createBlockDependentsTool(index: BlockIndex) {
  return {
    name: 'entangled_block_dependents',
    description: 'Find all blocks that depend on a named block (transitive reverse dependency graph)',
    parameters: z.object({
      blockName: z.string().describe('The block name to find dependents of'),
    }),
    execute: async (args: { blockName: string }) => {
      const deps = index.dependents(args.blockName);
      return {
        title: `Dependents of <<${args.blockName}>>`,
        output: JSON.stringify(
          deps.map((d) => ({ file: d.file, line: d.line, id: d.id })),
          null,
          2,
        ),
        metadata: { count: deps.length },
      };
    },
  };
}
