import type { BlockIndex } from '../block-index.js';
import type { FileDB } from '../filedb.js';

export interface HookInput {
  tool: string;
  sessionID: string;
  callID: string;
  args: Record<string, unknown>;
}

export interface RunCommandResult {
  stdout: string;
  stderr: string;
  exitCode: number;
}

type RunCommand = (cmd: string, args: string[]) => Promise<RunCommandResult>;

// WHY: injected readFile + runCommand keeps this testable without real filesystem or CLI
export function createTangleStitchHook(
  index: BlockIndex,
  filedb: FileDB,
  runCommand: RunCommand,
  readFile: (path: string) => Promise<string>,
): (input: HookInput, output: Record<string, unknown>) => Promise<void> {
  return async (input, output) => {
    if (input.tool !== 'edit' && input.tool !== 'write') return;

    const filePath = extractFilePath(input.args);
    if (!filePath) return;

    if (filePath.endsWith('.md')) {
      await handleMarkdownEdit(filePath, index, filedb, runCommand, readFile, output);
    } else if (filedb.isManaged(filePath)) {
      await handleTangleTargetEdit(filePath, index, filedb, runCommand, readFile, output);
    }
  };
}

function extractFilePath(args: Record<string, unknown>): string | undefined {
  // WHY: OpenCode edit/write tools pass the file path as `file_path` or `filePath`
  const raw = args.file_path ?? args.filePath ?? args.path;
  return typeof raw === 'string' ? raw : undefined;
}

async function handleMarkdownEdit(
  mdPath: string,
  index: BlockIndex,
  filedb: FileDB,
  runCommand: RunCommand,
  readFile: (path: string) => Promise<string>,
  output: Record<string, unknown>,
): Promise<void> {
  const blocks = index.listBlocks(mdPath);
  // WHY: only run tangle if this .md file actually has code blocks we track
  if (blocks.length === 0) return;

  const result = await runCommand('entangled', ['tangle']);
  if (result.exitCode !== 0) {
    appendError(output, 'entangled tangle', result);
    return;
  }

  // WHY: rebuild index for this file so subsequent lookups reflect the edit
  try {
    const content = await readFile(mdPath);
    index.addFile(mdPath, content);
  } catch {
    // WHY: file read failure after successful tangle is non-fatal
  }

  // WHY: reload filedb so isManaged() reflects any new tangle targets
  try {
    await filedb.load(readFile);
  } catch {
    // non-fatal
  }
}

async function handleTangleTargetEdit(
  targetPath: string,
  index: BlockIndex,
  filedb: FileDB,
  runCommand: RunCommand,
  readFile: (path: string) => Promise<string>,
  output: Record<string, unknown>,
): Promise<void> {
  const result = await runCommand('entangled', ['stitch']);
  if (result.exitCode !== 0) {
    appendError(output, 'entangled stitch', result);
    return;
  }

  // WHY: find which .md files contribute blocks to this target, then rebuild their index entries
  const affectedBlocks = index.getByTarget(targetPath);
  const affectedMdFiles = new Set(affectedBlocks.map((b) => b.file));

  for (const mdPath of affectedMdFiles) {
    try {
      const content = await readFile(mdPath);
      index.addFile(mdPath, content);
    } catch {
      // WHY: file read failure after successful stitch is non-fatal
    }
  }

  // WHY: reload filedb so stat info reflects the stitch
  try {
    await filedb.load(readFile);
  } catch {
    // non-fatal
  }
}

function appendError(output: Record<string, unknown>, command: string, result: RunCommandResult): void {
  const errorInfo = `[${command}] failed (exit ${result.exitCode}):\n${result.stderr || result.stdout}`;
  // WHY: append to existing output so the LLM sees the error alongside the tool result
  const existing = typeof output.error === 'string' ? `${output.error}\n` : '';
  output.error = existing + errorInfo;
}
