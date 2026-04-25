import type { BlockIndex } from '../block-index.js';
import { defineTool, type InternalTool, z } from '../tool-schema.js';

export function createListTargetsTool(index: BlockIndex): InternalTool<Record<never, never>> {
  return defineTool({
    name: 'entangled_list_targets',
    description: 'List all file= tangle targets across the project',
    parameters: z.object({}),
    execute: async (_args: Record<string, never>) => {
      const targets = index.listTargets();
      return {
        title: 'Tangle targets',
        output: JSON.stringify(targets, null, 2),
        metadata: { count: targets.length },
      };
    },
  });
}
