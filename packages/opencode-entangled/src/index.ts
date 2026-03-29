/// <reference types="bun-types" />

// WHY: bun-types reference is needed because the lib tsconfig has "types": [],
// but this plugin entry point runs in Bun and needs Bun.file/Bun.write APIs.

import { readFile as nodeReadFile, writeFile as nodeWriteFile, readdir } from 'node:fs/promises';
import { join, relative } from 'node:path';
import type { Hooks, Plugin, PluginModule } from '@opencode-ai/plugin';
import { BlockIndex } from './block-index.js';
import { FileDB } from './filedb.js';
import { createEditRerouteHook } from './hooks/edit-reroute.js';
import { createTangleStitchHook } from './hooks/tangle-stitch.js';
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

export type { BlockEntry } from './block-index.js';
export { BlockIndex } from './block-index.js';
// --- Re-exports for consumers ---
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

// WHY: the internal tool factories return zod v3 schemas + typed execute functions,
// but @opencode-ai/plugin's ToolDefinition uses zod v4. At runtime the shapes are
// identical (description + args + execute), so we adapt structurally. The tool map in
// Hooks is typed as { [key: string]: ToolDefinition } which accepts structural matches.
type InternalTool = {
  name: string;
  description: string;
  parameters: { shape: Record<string, unknown> };
  execute: (args: Record<string, unknown>) => Promise<{ title: string; output: string; metadata: unknown }>;
};

function adaptTool(internal: InternalTool) {
  return {
    description: internal.description,
    args: internal.parameters.shape,
    async execute(args: Record<string, unknown>) {
      const result = await internal.execute(args);
      return result.output;
    },
  };
}

// WHY: we cast the tool map once at the plugin boundary because the internal tools use
// zod v3 while the plugin SDK expects zod v4. The runtime shape is identical
// (description + args shape + execute), but the zod type brands differ at compile time.
function adaptTools(tools: InternalTool[]): NonNullable<Hooks['tool']> {
  const result: Record<string, ReturnType<typeof adaptTool>> = {};
  for (const t of tools) {
    result[t.name] = adaptTool(t);
  }
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- WHY: zod v3/v4 brand mismatch; runtime shapes are identical
  return result as NonNullable<Hooks['tool']>;
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
  const editReroute = createEditRerouteHook(index, filedb);
  const tangleStitch = createTangleStitchHook(index, filedb, runCommand, readFile);

  return {
    'tool.execute.before': editReroute,
    'tool.execute.after': tangleStitch,
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- WHY: tool factories return typed execute args (contravariant); InternalTool uses Record<string, unknown> for uniform adaptation at the zod v3/v4 boundary
    tool: adaptTools([
      createFindReferencesTool(index),
      createFindDefinitionTool(index),
      createListBlocksTool(index),
      createExpandTool(index),
      createListTargetsTool(index),
      createBlockDependentsTool(index),
      createAbsorbTool(index, io, runCommand),
      createAbsorbFunctionTool(index, io, runCommand),
      createMoveBlockTool(index, io, runCommand),
      createRenameBlockTool(index, io, runCommand),
    ] as unknown as InternalTool[]),
  };
};

export const EntangledPlugin: PluginModule = {
  id: 'entangled',
  server,
};

export default EntangledPlugin;
