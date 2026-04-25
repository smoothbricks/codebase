import type { BlockIndex } from '../block-index.js';
import { defineTool, type InternalTool, type ZString, z } from '../tool-schema.js';
import type { FileIO, RunCommand } from './absorb.js';

export function createRenameBlockTool(
  index: BlockIndex,
  io: FileIO,
  runCommand: RunCommand,
): InternalTool<{ oldName: ZString; newName: ZString }> {
  return defineTool({
    name: 'entangled_rename_block',
    description: 'Rename a block id and update all <<ref>> references across all markdown files',
    parameters: z.object({
      oldName: z.string().describe('Current block name'),
      newName: z.string().describe('New block name'),
    }),
    execute: async (args: { oldName: string; newName: string }) => {
      const block = index.get(args.oldName);
      if (!block) {
        return {
          title: 'Rename block failed',
          output: `Block not found: ${args.oldName}`,
          metadata: { success: false },
        };
      }

      // Find all references to the old name
      const refs = index.findReferences(args.oldName);

      // Collect all unique files that need updating: the definition file + all referencing files
      const filesToUpdate = new Set<string>();
      filesToUpdate.add(block.file);
      for (const ref of refs) {
        filesToUpdate.add(ref.file);
      }

      for (const filePath of filesToUpdate) {
        let content: string;
        try {
          content = await io.readFile(filePath);
        } catch {
          continue;
        }

        // WHY: update #oldName in fence definitions and <<oldName>> in noweb references
        const updated = content
          .replace(new RegExp(`#${escapeRegex(args.oldName)}(?=[\\s}])`, 'g'), `#${args.newName}`)
          .replace(new RegExp(`<<${escapeRegex(args.oldName)}>>`, 'g'), `<<${args.newName}>>`)
          .replace(new RegExp(`<<${escapeRegex(args.oldName)}#`, 'g'), `<<${args.newName}#`);

        await io.writeFile(filePath, updated);
      }

      // Run entangled tangle
      const result = await runCommand('entangled', ['tangle']);

      // Rebuild index for all changed files
      for (const filePath of filesToUpdate) {
        try {
          const updated = await io.readFile(filePath);
          index.addFile(filePath, updated);
        } catch {
          // WHY: file may have been removed or become unreadable — skip
        }
      }

      return {
        title: `Renamed block "${args.oldName}" to "${args.newName}"`,
        output:
          result.exitCode === 0
            ? `Renamed definition and ${refs.length} reference(s) across ${filesToUpdate.size} file(s)`
            : `Renamed but tangle reported: ${result.stderr}`,
        metadata: {
          success: true,
          referencesUpdated: refs.length,
          filesUpdated: filesToUpdate.size,
          tangleExitCode: result.exitCode,
        },
      };
    },
  });
}

function escapeRegex(str: string): string {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
