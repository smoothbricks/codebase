import type { BlockIndex } from '../block-index.js';
import { defineTool, type InternalTool, type ZOptionalString, type ZString, z } from '../tool-schema.js';
import type { FileIO, RunCommand } from './absorb.js';
import { insertAfterBlock } from './block-utils.js';

export function createMoveBlockTool(
  index: BlockIndex,
  io: FileIO,
  runCommand: RunCommand,
): InternalTool<{ blockName: ZString; targetMd: ZString; insertAfter: ZOptionalString }> {
  return defineTool({
    name: 'entangled_move_block',
    description: 'Move a code block from one .md file to another',
    parameters: z.object({
      blockName: z.string().describe('Name of the block to move'),
      targetMd: z.string().describe('Destination .md file'),
      insertAfter: z.string().optional().describe('Insert after this block in the target'),
    }),
    execute: async (args: { blockName: string; targetMd: string; insertAfter?: string }) => {
      const block = index.get(args.blockName);
      if (!block) {
        return {
          title: 'Move block failed',
          output: `Block not found: ${args.blockName}`,
          metadata: { success: false },
        };
      }

      const sourceMd = block.file;

      // Read source markdown and extract the full fence block (open + body + close)
      let sourceContent: string;
      try {
        sourceContent = await io.readFile(sourceMd);
      } catch {
        return {
          title: 'Move block failed',
          output: `Source file not found: ${sourceMd}`,
          metadata: { success: false },
        };
      }

      const extraction = extractFenceBlock(sourceContent, args.blockName);
      if (!extraction) {
        return {
          title: 'Move block failed',
          output: `Could not locate fence block "${args.blockName}" in ${sourceMd}`,
          metadata: { success: false },
        };
      }

      // Remove block from source
      await io.writeFile(sourceMd, extraction.remaining);

      // Read or create target .md and insert the block
      let targetContent: string;
      try {
        targetContent = await io.readFile(args.targetMd);
      } catch {
        targetContent = '';
      }

      if (args.insertAfter) {
        const afterBlock = index.get(args.insertAfter);
        if (!afterBlock) {
          return {
            title: 'Move block failed',
            output: `insertAfter block not found: ${args.insertAfter}`,
            metadata: { success: false },
          };
        }
        targetContent = insertAfterBlock(targetContent, args.insertAfter, extraction.fenceText);
      } else {
        targetContent = targetContent.length > 0 ? `${targetContent}\n\n${extraction.fenceText}` : extraction.fenceText;
      }

      await io.writeFile(args.targetMd, targetContent);

      // Run entangled tangle
      const result = await runCommand('entangled', ['tangle']);

      // Rebuild index for both files
      const updatedSource = await io.readFile(sourceMd);
      index.addFile(sourceMd, updatedSource);
      const updatedTarget = await io.readFile(args.targetMd);
      index.addFile(args.targetMd, updatedTarget);

      return {
        title: `Moved block "${args.blockName}" from ${sourceMd} to ${args.targetMd}`,
        output:
          result.exitCode === 0
            ? 'Block moved and tangle verified'
            : `Block moved but tangle reported: ${result.stderr}`,
        metadata: {
          success: true,
          from: sourceMd,
          to: args.targetMd,
          tangleExitCode: result.exitCode,
        },
      };
    },
  });
}

interface ExtractionResult {
  /** The full fence block text (open fence + body + close fence) */
  fenceText: string;
  /** The source file content with the fence block removed */
  remaining: string;
}

/** Extract a complete fence block (open + body + close) from markdown content by block id.
 *  Returns the extracted text and the remaining content. */
function extractFenceBlock(content: string, blockId: string): ExtractionResult | undefined {
  const lines = content.split('\n');
  let blockStart = -1;
  let blockEnd = -1;
  let backtickCount = 0;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i] ?? '';

    if (blockStart === -1) {
      const openMatch = line.match(/^(`{3,})\S*\s*\{[^}]*#(\w[\w-]*)[^}]*\}/);
      if (openMatch && openMatch[2] === blockId) {
        blockStart = i;
        backtickCount = (openMatch[1] ?? '```').length;
      }
    } else if (blockEnd === -1) {
      const trimmed = line.trim();
      if (trimmed.length >= backtickCount && /^`+$/.test(trimmed)) {
        blockEnd = i;
        break;
      }
    }
  }

  if (blockStart === -1 || blockEnd === -1) return undefined;

  const fenceLines = lines.slice(blockStart, blockEnd + 1);

  // WHY: remove trailing blank lines after the block to avoid accumulating whitespace
  let removeEnd = blockEnd + 1;
  while (removeEnd < lines.length && (lines[removeEnd] ?? '').trim() === '') {
    removeEnd++;
  }
  // WHY: also remove leading blank line before the block if present
  let removeStart = blockStart;
  if (removeStart > 0 && (lines[removeStart - 1] ?? '').trim() === '') {
    removeStart--;
  }

  const remaining = [...lines.slice(0, removeStart), ...lines.slice(removeEnd)].join('\n');

  return {
    fenceText: fenceLines.join('\n'),
    remaining,
  };
}
