/// <reference types="bun-types" />

// WHY: bun-types reference is needed because the lib tsconfig has "types": [],
// but this plugin entry point runs in Bun and needs node:fs APIs.

import { readFile as nodeReadFile, writeFile as nodeWriteFile, readdir } from 'node:fs/promises';
import { join, relative } from 'node:path';
import type { Plugin, PluginModule } from '@opencode-ai/plugin';
import { type ToolDefinition, tool } from '@opencode-ai/plugin';
import { BlockIndex } from './block-index.js';
import { FileDB } from './filedb.js';
import { createEditRerouteHook } from './hooks/edit-reroute.js';
import { createTangleStitchHook } from './hooks/tangle-stitch.js';
import type { InternalTool, ToolArgsShape, ToolInput } from './tool-schema.js';
import { createAbsorbTool, type FileIO, type RunCommand } from './tools/absorb.js';
import { createAbsorbFunctionTool } from './tools/absorb-function.js';
import { createBlockDependentsTool } from './tools/block-dependents.js';
import { createExpandTool } from './tools/expand.js';
import { createFindDefinitionTool } from './tools/find-definition.js';
import { createFindReferencesTool } from './tools/find-references.js';
import { createListBlocksTool } from './tools/list-blocks.js';
import { createListTargetsTool } from './tools/list-targets.js';
import { createMoveBlockTool } from './tools/move-block.js';
import { createRenameBlockTool } from './tools/rename-block.js';

// --- Re-exports for consumers ---
export type { BlockEntry } from './block-index.js';
export { BlockIndex } from './block-index.js';
export type { ParsedBlock } from './fence-parser.js';
export { FileDB } from './filedb.js';

// --- Helpers ---

async function listMdFilesRecursive(dir: string): Promise<string[]> {
  const results: string[] = [];
  const entries = await readdir(dir, { withFileTypes: true });

  for (const entry of entries) {
    const fullPath = join(dir, entry.name);

    // WHY: skip hidden dirs, node_modules, and dist to avoid indexing irrelevant files
    if (entry.isDirectory()) {
      if (entry.name.startsWith('.') || entry.name === 'node_modules' || entry.name === 'dist') continue;
      const nested = await listMdFilesRecursive(fullPath);
      results.push(...nested);
    } else if (entry.name.endsWith('.md')) {
      results.push(fullPath);
    }
  }

  return results;
}

function adaptTool<Shape extends ToolArgsShape>(t: InternalTool<Shape>): [string, ToolDefinition] {
  return [
    t.name,
    tool({
      description: t.description,
      args: t.parameters.shape,
      async execute(args: ToolInput<Shape>, context) {
        const r = await t.execute(args);
        context.metadata({ title: r.title, metadata: r.metadata });
        return r.output;
      },
    }),
  ];
}

// --- Plugin ---

const server: Plugin = async ({ directory, $ }) => {
  const index = new BlockIndex();
  const filedb = new FileDB(directory);

  const readFile = (path: string) => nodeReadFile(path, 'utf-8');
  const writeFile = (path: string, content: string) => nodeWriteFile(path, content, 'utf-8');

  const io: FileIO = { readFile, writeFile };

  const runCommand: RunCommand = async (cmd, args) => {
    // WHY: use nothrow() so non-zero exits don't throw — callers check exitCode
    const result = await $`${cmd} ${args}`.quiet().nothrow();
    return {
      stdout: result.stdout.toString(),
      stderr: result.stderr.toString(),
      exitCode: result.exitCode,
    };
  };

  // Build initial index by scanning for .md files
  await index.scanDirectory(
    () =>
      listMdFilesRecursive(directory).then((files) =>
        // WHY: BlockIndex stores relative paths so lookups match entangled's convention
        files.map((f) => relative(directory, f)),
      ),
    (relPath) => readFile(join(directory, relPath)),
  );

  // Load filedb (non-fatal if missing)
  await filedb.load(readFile);

  // Create hooks
  const editReroute = createEditRerouteHook(index);
  const tangleStitch = createTangleStitchHook(index, filedb, runCommand, readFile);

  return {
    'tool.execute.before': editReroute,
    'tool.execute.after': tangleStitch,
    tool: Object.fromEntries([
      adaptTool(createFindReferencesTool(index)),
      adaptTool(createFindDefinitionTool(index)),
      adaptTool(createListBlocksTool(index)),
      adaptTool(createExpandTool(index)),
      adaptTool(createListTargetsTool(index)),
      adaptTool(createBlockDependentsTool(index)),
      adaptTool(createAbsorbTool(index, io, runCommand)),
      adaptTool(createAbsorbFunctionTool(index, io, runCommand)),
      adaptTool(createMoveBlockTool(index, io, runCommand)),
      adaptTool(createRenameBlockTool(index, io, runCommand)),
    ]),
  };
};

export const EntangledPlugin: PluginModule = {
  id: 'entangled',
  server,
};

export default EntangledPlugin;
