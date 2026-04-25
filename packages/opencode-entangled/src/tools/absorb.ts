import type { BlockIndex } from '../block-index.js';
import { defineTool, type InternalTool, type ZNumber, type ZOptionalString, type ZString, z } from '../tool-schema.js';
import { insertAfterBlock } from './block-utils.js';

export interface FileIO {
  readFile: (path: string) => Promise<string>;
  writeFile: (path: string, content: string) => Promise<void>;
}

export interface CommandResult {
  stdout: string;
  stderr: string;
  exitCode: number;
}

export type RunCommand = (cmd: string, args: string[]) => Promise<CommandResult>;

export function createAbsorbTool(
  index: BlockIndex,
  io: FileIO,
  runCommand: RunCommand,
): InternalTool<{
  sourcePath: ZString;
  startLine: ZNumber;
  endLine: ZNumber;
  targetMd: ZString;
  blockName: ZString;
  insertAfter: ZOptionalString;
  appendTo: ZOptionalString;
  prose: ZOptionalString;
}> {
  return defineTool({
    name: 'entangled_absorb',
    description:
      'Move lines from a .ts file into a new or existing .md code block, replacing the source lines with a tangle marker',
    parameters: z.object({
      sourcePath: z.string().describe('The .ts file to absorb lines from'),
      startLine: z.number().describe('First line to absorb (1-indexed)'),
      endLine: z.number().describe('Last line to absorb (1-indexed)'),
      targetMd: z.string().describe('Target .md file'),
      blockName: z.string().describe('Block name for the new/existing block'),
      insertAfter: z.string().optional().describe('Insert after this block name (for new blocks)'),
      appendTo: z.string().optional().describe('Append to existing block with this name'),
      prose: z.string().optional().describe('Prose text to add before the new block'),
    }),
    execute: async (args: {
      sourcePath: string;
      startLine: number;
      endLine: number;
      targetMd: string;
      blockName: string;
      insertAfter?: string;
      appendTo?: string;
      prose?: string;
    }) => {
      // Read source .ts file and extract the line range
      let sourceContent: string;
      try {
        sourceContent = await io.readFile(args.sourcePath);
      } catch {
        return {
          title: 'Absorb failed',
          output: `Source file not found: ${args.sourcePath}`,
          metadata: { success: false },
        };
      }

      const sourceLines = sourceContent.split('\n');

      if (args.startLine < 1 || args.endLine > sourceLines.length || args.startLine > args.endLine) {
        return {
          title: 'Absorb failed',
          output: `Invalid line range: ${args.startLine}-${args.endLine} (file has ${sourceLines.length} lines)`,
          metadata: { success: false },
        };
      }

      // WHY: 1-indexed to 0-indexed conversion; endLine is inclusive
      const extracted = sourceLines.slice(args.startLine - 1, args.endLine);
      const extractedContent = extracted.join('\n');

      // Read or create target .md file
      let mdContent: string;
      try {
        mdContent = await io.readFile(args.targetMd);
      } catch {
        mdContent = '';
      }

      if (args.appendTo) {
        // Append to existing block
        const block = index.get(args.appendTo);
        if (!block) {
          return {
            title: 'Absorb failed',
            output: `Block not found for appendTo: ${args.appendTo}`,
            metadata: { success: false },
          };
        }

        mdContent = appendToBlock(mdContent, args.appendTo, extractedContent);
      } else {
        // Create a new fenced code block
        const newBlock = buildNewBlock(args.blockName, extractedContent, args.prose);

        if (args.insertAfter) {
          const afterBlock = index.get(args.insertAfter);
          if (!afterBlock) {
            return {
              title: 'Absorb failed',
              output: `Block not found for insertAfter: ${args.insertAfter}`,
              metadata: { success: false },
            };
          }
          mdContent = insertAfterBlock(mdContent, args.insertAfter, newBlock);
        } else {
          // WHY: no insertAfter specified — append to end of file
          mdContent = mdContent.length > 0 ? `${mdContent}\n\n${newBlock}` : newBlock;
        }
      }

      await io.writeFile(args.targetMd, mdContent);

      // Replace absorbed lines in source .ts with tangle marker
      const tangleMarker = `// ~/~ begin <<${args.targetMd}#${args.blockName}>>[0]\n// ~/~ end`;
      const newSourceLines = [
        ...sourceLines.slice(0, args.startLine - 1),
        tangleMarker,
        ...sourceLines.slice(args.endLine),
      ];
      await io.writeFile(args.sourcePath, newSourceLines.join('\n'));

      // Run entangled tangle to verify
      const result = await runCommand('entangled', ['tangle']);

      // Rebuild index for the changed markdown file
      const updatedMd = await io.readFile(args.targetMd);
      index.addFile(args.targetMd, updatedMd);

      return {
        title: `Absorbed lines ${args.startLine}-${args.endLine} from ${args.sourcePath} into ${args.targetMd}#${args.blockName}`,
        output:
          result.exitCode === 0
            ? `Successfully absorbed ${extracted.length} lines`
            : `Absorbed ${extracted.length} lines but tangle reported: ${result.stderr}`,
        metadata: {
          success: true,
          linesAbsorbed: extracted.length,
          tangleExitCode: result.exitCode,
        },
      };
    },
  });
}

/** Build a new fenced code block with optional prose */
function buildNewBlock(blockName: string, content: string, prose?: string): string {
  const fence = `\`\`\`typescript {#${blockName}}\n${content}\n\`\`\``;
  if (prose) {
    return `${prose}\n\n${fence}`;
  }
  return fence;
}

/** Find the closing fence of a named block and append content before it */
function appendToBlock(mdContent: string, blockId: string, extraContent: string): string {
  const lines = mdContent.split('\n');
  const result: string[] = [];
  let insideTarget = false;
  let backtickCount = 0;
  let inserted = false;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i] ?? '';

    if (!insideTarget) {
      // WHY: look for the fence-open line that defines this block id
      const openMatch = line.match(/^(`{3,})\S*\s*\{[^}]*#(\w[\w-]*)[^}]*\}/);
      if (openMatch && openMatch[2] === blockId) {
        insideTarget = true;
        backtickCount = (openMatch[1] ?? '```').length;
      }
      result.push(line);
      continue;
    }

    // WHY: detect closing fence (same or more backticks, nothing else on the line)
    const trimmed = line.trim();
    if (trimmed.length >= backtickCount && /^`+$/.test(trimmed)) {
      // Insert the extra content before the closing fence
      result.push(extraContent);
      result.push(line);
      insideTarget = false;
      inserted = true;
      continue;
    }

    result.push(line);
  }

  if (!inserted) {
    // WHY: block was not found in the file content, which shouldn't happen
    // if the caller validated via BlockIndex — return content unchanged
    return mdContent;
  }

  return result.join('\n');
}
