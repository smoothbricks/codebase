import { beforeEach, describe, expect, it } from 'bun:test';
import { BlockIndex } from '../src/block-index.js';
import { FileDB } from '../src/filedb.js';
import { createTangleStitchHook, type HookInput, type RunCommandResult } from '../src/hooks/tangle-stitch.js';

function md(...lines: string[]): string {
  return lines.join('\n');
}

const SAMPLE_MD = md('```typescript {#greet file="src/greet.ts"}', 'export function greet() { return "hi"; }', '```');

const FILEDB_JSON = JSON.stringify({
  version: '1.0',
  files: {
    'docs/main.md': { modified: '2025-01-01T00:00:00', hexdigest: 'aaa' },
    'src/greet.ts': { modified: '2025-01-01T00:00:00', hexdigest: 'bbb' },
  },
  targets: ['src/greet.ts'],
});

function successResult(): RunCommandResult {
  return { stdout: 'ok', stderr: '', exitCode: 0 };
}

function failureResult(): RunCommandResult {
  return { stdout: '', stderr: 'Error: something broke', exitCode: 1 };
}

describe('tangle-stitch hook', () => {
  let index: BlockIndex;
  let filedb: FileDB;
  let commands: { cmd: string; args: string[] }[];
  let runCommand: (cmd: string, args: string[]) => Promise<RunCommandResult>;
  let commandResult: RunCommandResult;
  let readFileStore: Record<string, string>;
  let readFile: (path: string) => Promise<string>;
  let hook: (input: HookInput, output: Record<string, unknown>) => Promise<void>;

  beforeEach(async () => {
    index = new BlockIndex();
    filedb = new FileDB('.');

    // WHY: seed the index with a known .md file so listBlocks returns entries
    index.addFile('docs/main.md', SAMPLE_MD);

    // WHY: FileDB.load() joins projectRoot + '.entangled/filedb.json' via posixJoin,
    // so with projectRoot='.' the resulting path is './.entangled/filedb.json'
    readFileStore = {
      './.entangled/filedb.json': FILEDB_JSON,
      'docs/main.md': SAMPLE_MD,
    };
    readFile = async (path: string) => {
      const content = readFileStore[path];
      if (content == null) throw new Error(`ENOENT: ${path}`);
      return content;
    };

    // WHY: load filedb so isManaged() works
    await filedb.load(readFile);

    commands = [];
    commandResult = successResult();
    runCommand = async (cmd, args) => {
      commands.push({ cmd, args });
      return commandResult;
    };

    hook = createTangleStitchHook(index, filedb, runCommand, readFile);
  });

  // --- Tool filtering ---

  it('edit on .ts tangle target runs entangled stitch', async () => {
    const output: Record<string, unknown> = {};
    await hook({ tool: 'edit', sessionID: 's1', callID: 'c1', args: { file_path: 'src/greet.ts' } }, output);

    expect(commands).toHaveLength(1);
    expect(commands[0]).toEqual({ cmd: 'entangled', args: ['stitch'] });
    expect(output.error).toBeUndefined();
  });

  it('edit on .md with blocks runs entangled tangle', async () => {
    const output: Record<string, unknown> = {};
    await hook({ tool: 'edit', sessionID: 's1', callID: 'c1', args: { file_path: 'docs/main.md' } }, output);

    expect(commands).toHaveLength(1);
    expect(commands[0]).toEqual({ cmd: 'entangled', args: ['tangle'] });
    expect(output.error).toBeUndefined();
  });

  it('write on .ts tangle target runs entangled stitch', async () => {
    const output: Record<string, unknown> = {};
    await hook({ tool: 'write', sessionID: 's1', callID: 'c1', args: { file_path: 'src/greet.ts' } }, output);

    expect(commands).toHaveLength(1);
    expect(commands[0]).toEqual({ cmd: 'entangled', args: ['stitch'] });
  });

  it('edit on non-managed file runs no command', async () => {
    const output: Record<string, unknown> = {};
    await hook({ tool: 'edit', sessionID: 's1', callID: 'c1', args: { file_path: 'random.ts' } }, output);

    expect(commands).toHaveLength(0);
    expect(output.error).toBeUndefined();
  });

  it('read tool triggers no action', async () => {
    const output: Record<string, unknown> = {};
    await hook({ tool: 'read', sessionID: 's1', callID: 'c1', args: { file_path: 'src/greet.ts' } }, output);

    expect(commands).toHaveLength(0);
  });

  it('bash tool triggers no action', async () => {
    const output: Record<string, unknown> = {};
    await hook({ tool: 'bash', sessionID: 's1', callID: 'c1', args: { command: 'ls' } }, output);

    expect(commands).toHaveLength(0);
  });

  // --- .md without blocks ---

  it('edit on .md without blocks runs no command', async () => {
    const output: Record<string, unknown> = {};
    // WHY: this .md has no code blocks in the index, so tangle would be pointless
    await hook({ tool: 'edit', sessionID: 's1', callID: 'c1', args: { file_path: 'empty.md' } }, output);

    expect(commands).toHaveLength(0);
  });

  // --- Error handling ---

  it('entangled command failure appends error to output', async () => {
    commandResult = failureResult();
    const output: Record<string, unknown> = {};
    await hook({ tool: 'edit', sessionID: 's1', callID: 'c1', args: { file_path: 'docs/main.md' } }, output);

    expect(commands).toHaveLength(1);
    expect(output.error).toBeDefined();
    expect(typeof output.error).toBe('string');
    expect(output.error as string).toContain('entangled tangle');
    expect(output.error as string).toContain('exit 1');
    expect(output.error as string).toContain('something broke');
  });

  it('entangled stitch failure appends error to output', async () => {
    commandResult = failureResult();
    const output: Record<string, unknown> = {};
    await hook({ tool: 'edit', sessionID: 's1', callID: 'c1', args: { file_path: 'src/greet.ts' } }, output);

    expect(output.error).toBeDefined();
    expect(output.error as string).toContain('entangled stitch');
  });

  it('successful command does not set error on output', async () => {
    const output: Record<string, unknown> = {};
    await hook({ tool: 'edit', sessionID: 's1', callID: 'c1', args: { file_path: 'docs/main.md' } }, output);

    expect(output.error).toBeUndefined();
  });

  // --- Index rebuild ---

  it('BlockIndex is rebuilt after successful tangle', async () => {
    const updatedMd = md(
      '```typescript {#greet file="src/greet.ts"}',
      'export function greet() { return "hello"; }',
      '```',
    );
    // WHY: after tangle, readFile returns updated content; the hook should re-index it
    readFileStore['docs/main.md'] = updatedMd;

    const output: Record<string, unknown> = {};
    await hook({ tool: 'edit', sessionID: 's1', callID: 'c1', args: { file_path: 'docs/main.md' } }, output);

    const entry = index.get('greet');
    expect(entry).toBeDefined();
    expect(entry?.content).toBe('export function greet() { return "hello"; }');
  });

  it('BlockIndex is rebuilt for affected .md files after successful stitch', async () => {
    const updatedMd = md(
      '```typescript {#greet file="src/greet.ts"}',
      'export function greet() { return "stitched"; }',
      '```',
    );
    readFileStore['docs/main.md'] = updatedMd;

    const output: Record<string, unknown> = {};
    await hook({ tool: 'edit', sessionID: 's1', callID: 'c1', args: { file_path: 'src/greet.ts' } }, output);

    const entry = index.get('greet');
    expect(entry).toBeDefined();
    expect(entry?.content).toBe('export function greet() { return "stitched"; }');
  });

  it('BlockIndex is NOT rebuilt after failed command', async () => {
    commandResult = failureResult();
    const updatedMd = md(
      '```typescript {#greet file="src/greet.ts"}',
      'export function greet() { return "should not appear"; }',
      '```',
    );
    readFileStore['docs/main.md'] = updatedMd;

    const output: Record<string, unknown> = {};
    await hook({ tool: 'edit', sessionID: 's1', callID: 'c1', args: { file_path: 'docs/main.md' } }, output);

    // WHY: on failure, the index should keep the old content
    const entry = index.get('greet');
    expect(entry?.content).toBe('export function greet() { return "hi"; }');
  });
});
