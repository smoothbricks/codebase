import type { BlockIndex } from '../block-index.js';
import type { FileDB } from '../filedb.js';

interface HookInput {
  tool: string;
  sessionID: string;
  callID: string;
}

interface HookOutput {
  args: Record<string, unknown>;
}

const INTERCEPTED_TOOLS = new Set(['edit', 'write']);

/**
 * Creates a `tool.execute.before` hook that reroutes edits targeting .md code blocks
 * to their tangled .ts file counterparts.
 *
 * WHY: the LLM sees .md files but its edits to code blocks should land in the tangled
 * output files so LSP feedback and file watchers work correctly.
 */
export function createEditRerouteHook(
  index: BlockIndex,
  _filedb: FileDB,
): (input: HookInput, output: HookOutput) => Promise<void> {
  return async (input, output) => {
    if (!INTERCEPTED_TOOLS.has(input.tool)) return;

    const filePath = output.args.filePath;
    if (typeof filePath !== 'string') return;

    // Only intercept .md files — .ts tangle targets pass through (tangle-stitch handles sync)
    if (!filePath.endsWith('.md')) return;

    const blocks = index.listBlocks(filePath);
    if (blocks.length === 0) return;

    // WHY: for 'edit' we match oldString against block content to find the right block.
    // For 'write' we check if any block in this file has a tangle target (first wins).
    if (input.tool === 'edit') {
      const oldString = output.args.oldString;
      if (typeof oldString !== 'string') return;

      // WHY: find the block whose content contains the oldString — that's the block being edited
      for (const block of blocks) {
        if (block.target && block.content.includes(oldString)) {
          output.args.filePath = block.target;
          return;
        }
      }
      // No matching block found — pass through (edit targets prose outside code blocks)
    } else if (input.tool === 'write') {
      // WHY: a write to a .md file that contains tangled blocks likely targets the first block's
      // tangle output. If no block has a target, pass through.
      const targetBlock = blocks.find((b) => b.target != null);
      if (targetBlock) {
        output.args.filePath = targetBlock.target;
      }
    }
  };
}
