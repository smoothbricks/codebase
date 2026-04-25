import type { BlockIndex } from '../block-index.js';
import { defineTool, type InternalTool, type ZOptionalString, type ZString, z } from '../tool-schema.js';
import { createAbsorbTool, type FileIO, type RunCommand } from './absorb.js';

// WHY: matches exported function/const/class declarations — the opening line is enough to find the start,
// then we scan for the balanced end. This avoids pulling in a full TS parser for a simple heuristic.
const EXPORT_RE = /^export\s+(?:async\s+)?(?:function|const|class)\s+(\w+)/;

export function createAbsorbFunctionTool(
  index: BlockIndex,
  io: FileIO,
  runCommand: RunCommand,
): InternalTool<{
  sourcePath: ZString;
  exportName: ZString;
  targetMd: ZString;
  blockName: ZOptionalString;
  prose: ZOptionalString;
}> {
  const absorbTool = createAbsorbTool(index, io, runCommand);

  return defineTool({
    name: 'entangled_absorb_function',
    description: 'Absorb an exported function/const/class from a .ts file into a markdown code block by export name',
    parameters: z.object({
      sourcePath: z.string().describe('The .ts file containing the function'),
      exportName: z.string().describe('Name of the exported function/const/class'),
      targetMd: z.string().describe('Target .md file'),
      blockName: z.string().optional().describe('Block name (defaults to kebab-case of exportName)'),
      prose: z.string().optional().describe('Prose text to add before the block'),
    }),
    execute: async (args: {
      sourcePath: string;
      exportName: string;
      targetMd: string;
      blockName?: string;
      prose?: string;
    }) => {
      let sourceContent: string;
      try {
        sourceContent = await io.readFile(args.sourcePath);
      } catch {
        return {
          title: 'Absorb function failed',
          output: `Source file not found: ${args.sourcePath}`,
          metadata: { success: false },
        };
      }

      const lines = sourceContent.split('\n');
      const range = findExportRange(lines, args.exportName);

      if (!range) {
        return {
          title: 'Absorb function failed',
          output: `Export "${args.exportName}" not found in ${args.sourcePath}`,
          metadata: { success: false },
        };
      }

      const blockName = args.blockName ?? toKebabCase(args.exportName);

      return absorbTool.execute({
        sourcePath: args.sourcePath,
        startLine: range.start,
        endLine: range.end,
        targetMd: args.targetMd,
        blockName,
        prose: args.prose,
      });
    },
  });
}

/** Find the 1-indexed start and end lines of an export declaration */
function findExportRange(lines: string[], exportName: string): { start: number; end: number } | undefined {
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i] ?? '';
    const match = line.match(EXPORT_RE);
    if (match && match[1] === exportName) {
      const startLine = i + 1; // WHY: convert 0-indexed to 1-indexed
      const endLine = findDeclarationEnd(lines, i);
      return { start: startLine, end: endLine };
    }
  }
  return undefined;
}

/** Find the end of a declaration by tracking brace balance.
 *  Returns 1-indexed line number. */
function findDeclarationEnd(lines: string[], startIdx: number): number {
  let braceDepth = 0;
  let parenDepth = 0;
  let started = false;

  for (let i = startIdx; i < lines.length; i++) {
    const line = lines[i] ?? '';

    for (const ch of line) {
      if (ch === '{') {
        braceDepth++;
        started = true;
      } else if (ch === '}') {
        braceDepth--;
      } else if (ch === '(') {
        parenDepth++;
        started = true;
      } else if (ch === ')') {
        parenDepth--;
      }
    }

    // WHY: for simple const declarations without braces (e.g. `export const x = 42;`),
    // we detect the end by finding a semicolon at the top level
    if (!started && line.includes(';')) {
      return i + 1;
    }

    if (started && braceDepth === 0 && parenDepth <= 0) {
      return i + 1;
    }
  }

  // WHY: if we can't determine the end, return the last line of the file
  return lines.length;
}

function toKebabCase(name: string): string {
  return name
    .replace(/([a-z0-9])([A-Z])/g, '$1-$2')
    .replace(/([A-Z]+)([A-Z][a-z])/g, '$1-$2')
    .toLowerCase();
}
