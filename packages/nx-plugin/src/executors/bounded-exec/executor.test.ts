import { afterEach, describe, expect, it } from 'bun:test';
import { access, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import {
  type BoundedExecContext,
  createProcessTreeKiller,
  type ProcessTreeKiller,
  runBoundedExec,
} from './executor.js';

const workspaces: string[] = [];
const originalStdoutWrite = process.stdout.write;
const originalStderrWrite = process.stderr.write;

describe('@smoothbricks/nx-plugin:bounded-exec', () => {
  afterEach(async () => {
    process.stdout.write = originalStdoutWrite;
    process.stderr.write = originalStderrWrite;
    await Promise.all(workspaces.splice(0).map((workspace) => rm(workspace, { recursive: true, force: true })));
  });

  it('runs a successful shell command with cwd and env', async () => {
    const workspace = await createWorkspace();
    await workspace.write(
      'package/print.js',
      'console.log(process.cwd()); console.log(process.env.BOUNDED_EXEC_VALUE);\n',
    );

    const result = await runBoundedExec(
      {
        command: 'node print.js',
        cwd: 'package',
        env: { BOUNDED_EXEC_VALUE: 'from-env' },
        timeoutMs: 5_000,
      },
      workspace.context,
      createProcessTreeKiller(),
    );

    expect(result.success).toBe(true);
    expect(result.terminalOutput).toContain(join(workspace.root, 'package'));
    expect(result.terminalOutput).toContain('from-env');
  });

  it('returns failure for a nonzero command', async () => {
    const workspace = await createWorkspace();

    const result = await runBoundedExec(
      { command: 'node -e "process.exit(7)"', timeoutMs: 5_000 },
      workspace.context,
      createProcessTreeKiller(),
    );

    expect(result.success).toBe(false);
    expect(result.terminalOutput).toContain('Command exited with status 7');
  });

  it('streams stdout and stderr while collecting terminal output', async () => {
    const workspace = await createWorkspace();
    const stdout: string[] = [];
    const stderr: string[] = [];
    process.stdout.write = captureWrite(stdout);
    process.stderr.write = captureWrite(stderr);

    const result = await runBoundedExec(
      { command: "node -e \"console.log('out-value'); console.error('err-value')\"", timeoutMs: 5_000 },
      workspace.context,
      createProcessTreeKiller(),
    );

    expect(result.success).toBe(true);
    expect(stdout.join('')).toContain('out-value');
    expect(stderr.join('')).toContain('err-value');
    expect(result.terminalOutput).toContain('out-value');
    expect(result.terminalOutput).toContain('err-value');
  });

  it('fails on timeout and reports bounded execution details', async () => {
    const workspace = await createWorkspace();

    const result = await runBoundedExec(
      { command: 'node -e "setTimeout(() => {}, 5000)"', timeoutMs: 50, killAfterMs: 0 },
      workspace.context,
      createProcessTreeKiller(),
    );

    expect(result.success).toBe(false);
    expect(result.terminalOutput).toContain('Command timed out after');
    expect(result.terminalOutput).toContain('timeoutMs=50');
    expect(result.terminalOutput).toContain(`cwd=${workspace.root}`);
  });

  it('uses graceful timeout termination before force-killing on POSIX', async () => {
    if (process.platform === 'win32') {
      return;
    }

    const workspace = await createWorkspace();
    const calls: string[] = [];
    const killer: ProcessTreeKiller = {
      async kill(pid, signal) {
        calls.push(signal);
        process.kill(-pid, signal);
      },
    };

    const result = await runBoundedExec(
      {
        command: 'node -e "process.on(\'SIGTERM\', () => process.exit(0)); setTimeout(() => {}, 5000)"',
        timeoutMs: 50,
        killAfterMs: 500,
      },
      workspace.context,
      killer,
    );

    expect(result.success).toBe(false);
    expect(calls).toEqual(['SIGTERM']);
  });

  it('force-kills after killAfterMs when graceful termination is ignored on POSIX', async () => {
    if (process.platform === 'win32') {
      return;
    }

    const workspace = await createWorkspace();
    const calls: string[] = [];
    const killer: ProcessTreeKiller = {
      async kill(pid, signal) {
        calls.push(signal);
        process.kill(-pid, signal);
      },
    };

    const result = await runBoundedExec(
      {
        command: 'node -e "process.on(\'SIGTERM\', () => {}); setTimeout(() => {}, 5000)"',
        timeoutMs: 50,
        killAfterMs: 10,
      },
      workspace.context,
      killer,
    );

    expect(result.success).toBe(false);
    expect(calls).toEqual(['SIGTERM', 'SIGKILL']);
  });

  it('forwards args by default and can suppress unparsed args', async () => {
    const workspace = await createWorkspace();

    const forwarded = await runBoundedExec(
      {
        command: 'node -e "console.log(process.argv.slice(1).join(\',\'))"',
        args: ['first'],
        __unparsed__: ['second'],
        timeoutMs: 5_000,
      },
      workspace.context,
      createProcessTreeKiller(),
    );
    const suppressed = await runBoundedExec(
      {
        command: 'node -e "console.log(process.argv.slice(1).join(\',\'))"',
        args: ['first'],
        __unparsed__: ['second'],
        forwardAllArgs: false,
        timeoutMs: 5_000,
      },
      workspace.context,
      createProcessTreeKiller(),
    );

    expect(forwarded.success).toBe(true);
    expect(forwarded.terminalOutput).toContain('first,second');
    expect(suppressed.success).toBe(true);
    expect(suppressed.terminalOutput).toContain('first');
    expect(suppressed.terminalOutput).not.toContain('second');
  });

  it('kills a POSIX child process group on timeout', async () => {
    if (process.platform === 'win32') {
      return;
    }

    const workspace = await createWorkspace();
    const marker = join(workspace.root, 'marker.txt');
    await workspace.write(
      'spawn-child.js',
      [
        "import { spawn } from 'node:child_process';",
        "spawn(process.execPath, ['-e', `setTimeout(() => require('node:fs').writeFileSync(process.argv[1], 'alive'), 700)` , process.argv[2]], { stdio: 'ignore' });",
        'setTimeout(() => {}, 5000);',
        '',
      ].join('\n'),
    );

    const result = await runBoundedExec(
      { command: `node spawn-child.js ${marker}`, timeoutMs: 50, killAfterMs: 50 },
      workspace.context,
      createProcessTreeKiller(),
    );

    await sleep(1_000);

    expect(result.success).toBe(false);
    expect(await exists(marker)).toBe(false);
  });
});

interface WorkspaceFixture {
  root: string;
  context: BoundedExecContext;
  write(filePath: string, contents: string): Promise<void>;
}

async function createWorkspace(): Promise<WorkspaceFixture> {
  const root = await mkdtemp(join(tmpdir(), 'smoothbricks-bounded-exec-'));
  workspaces.push(root);
  return {
    root,
    context: { root },
    async write(filePath: string, contents: string): Promise<void> {
      const absolutePath = join(root, filePath);
      await mkdir(dirname(absolutePath), { recursive: true });
      await writeFile(absolutePath, contents);
    },
  };
}

function captureWrite(chunks: string[]): typeof process.stdout.write {
  return ((
    chunk: string | Uint8Array,
    encodingOrCallback?: BufferEncoding | ((error?: Error | null) => void),
    callback?: (error?: Error | null) => void,
  ) => {
    chunks.push(chunk.toString());
    const cb = typeof encodingOrCallback === 'function' ? encodingOrCallback : callback;
    cb?.();
    return true;
  }) as typeof process.stdout.write;
}

async function exists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
