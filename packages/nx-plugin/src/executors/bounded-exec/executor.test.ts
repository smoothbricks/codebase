import { afterEach, describe, expect, it } from 'bun:test';
import { access, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import type { CpuBudget } from './cpu-tokens.js';
import {
  type BoundedExecContext,
  createProcessTreeKiller,
  type ProcessTreeKiller,
  runBoundedExec,
  type TempVolume,
} from './executor.js';
import type { RamTempAcquisition, RamTempError, RamTempLease, Result } from './ram-temp.js';

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
      null,
      null,
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
      null,
      null,
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
      null,
      null,
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
      null,
      null,
    );

    expect(result.success).toBe(false);
    expect(result.terminalOutput).toContain('Command timed out after');
    expect(result.terminalOutput).toContain('timeoutMs=50');
    expect(result.terminalOutput).toContain(`cwd=${workspace.root}`);
  });

  it('kills a silent command on the progress bound while the absolute ceiling is far away', async () => {
    const workspace = await createWorkspace();

    const result = await runBoundedExec(
      { command: 'node -e "setTimeout(() => {}, 30000)"', timeoutMs: 30_000, idleTimeoutMs: 100, killAfterMs: 0 },
      workspace.context,
      createProcessTreeKiller(),
      null,
      null,
    );

    expect(result.success).toBe(false);
    expect(result.terminalOutput).toContain('Command made no progress: no output for 100ms');
    expect(result.terminalOutput).toContain('idleTimeoutMs=100');
    // The bound that fired is named, so a wedge is never reported as a slow run.
    expect(result.terminalOutput).not.toContain('Command timed out after');
  });

  it('lets a slow but talking command outlive its progress bound many times over', async () => {
    const workspace = await createWorkspace();
    // Six 120ms gaps under a 400ms progress bound: total runtime is well past
    // idleTimeoutMs, so passing proves the bound measures silence, not duration.
    await workspace.write(
      'chatty.js',
      [
        'let ticks = 0;',
        'const timer = setInterval(() => {',
        '  console.log("tick " + ++ticks);',
        '  if (ticks === 6) {',
        '    clearInterval(timer);',
        '  }',
        '}, 120);',
        '',
      ].join('\n'),
    );

    const result = await runBoundedExec(
      { command: 'node chatty.js', timeoutMs: 30_000, idleTimeoutMs: 400 },
      workspace.context,
      createProcessTreeKiller(),
      null,
      null,
    );

    expect(result.success).toBe(true);
    expect(result.terminalOutput).toContain('tick 6');
    expect(result.terminalOutput).not.toContain('made no progress');
  });

  it('treats output on stderr alone as progress', async () => {
    const workspace = await createWorkspace();
    await workspace.write(
      'chatty-stderr.js',
      [
        'let ticks = 0;',
        'const timer = setInterval(() => {',
        '  console.error("tick " + ++ticks);',
        '  if (ticks === 5) {',
        '    clearInterval(timer);',
        '  }',
        '}, 120);',
        '',
      ].join('\n'),
    );

    const result = await runBoundedExec(
      { command: 'node chatty-stderr.js', timeoutMs: 30_000, idleTimeoutMs: 400 },
      workspace.context,
      createProcessTreeKiller(),
      null,
      null,
    );

    expect(result.success).toBe(true);
    expect(result.terminalOutput).toContain('tick 5');
  });

  it('applies the absolute ceiling to a command that keeps talking forever', async () => {
    const workspace = await createWorkspace();
    await workspace.write('runaway.js', ["setInterval(() => console.log('still here'), 10);", ''].join('\n'));

    const result = await runBoundedExec(
      { command: 'node runaway.js', timeoutMs: 300, idleTimeoutMs: 30_000, killAfterMs: 0 },
      workspace.context,
      createProcessTreeKiller(),
      null,
      null,
    );

    expect(result.success).toBe(false);
    expect(result.terminalOutput).toContain('Command timed out after');
    expect(result.terminalOutput).toContain('timeoutMs=300');
    expect(result.terminalOutput).not.toContain('made no progress');
  });

  it('imposes no progress bound when idleTimeoutMs is omitted', async () => {
    const workspace = await createWorkspace();

    const result = await runBoundedExec(
      { command: 'node -e "setTimeout(() => console.log(\'late\'), 400)"', timeoutMs: 30_000 },
      workspace.context,
      createProcessTreeKiller(),
      null,
      null,
    );

    expect(result.success).toBe(true);
    expect(result.terminalOutput).toContain('late');
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
      null,
      null,
    );

    expect(result.success).toBe(false);
    expect(calls[0]).toBe('SIGTERM');
    expect(calls.every((signal) => signal === 'SIGTERM' || signal === 'SIGKILL')).toBe(true);
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

    // `trap "" TERM` is a shell builtin, so the ignore disposition is in place
    // within a few ms of `sh` starting and is inherited by `sleep`. The previous
    // `node -e "process.on('SIGTERM', ...)"` needed p50 27.8ms just to reach
    // handler-installed, against a 50ms timeout — 1.8x margin on an idle
    // 18-core host, and none at all on a loaded or 3-core runner. When SIGTERM
    // arrives first the child dies on the default disposition, the run settles,
    // and the SIGKILL branch never executes, so `calls` is ['SIGTERM'] and the
    // assertion below fails for a reason that has nothing to do with escalation.
    const result = await runBoundedExec(
      {
        command: 'sh -c \'trap "" TERM; sleep 5\'',
        timeoutMs: 250,
        killAfterMs: 50,
      },
      workspace.context,
      killer,
      null,
      null,
    );

    expect(result.success).toBe(false);
    expect(calls).toEqual(['SIGTERM', 'SIGKILL']);
  });

  it('returns after force-kill even when the process tree ignores SIGKILL', async () => {
    // Real clock: this is the platform kill/reap path. Fake timers cannot
    // drive child_process exit or SIGKILL delivery.
    if (process.platform === 'win32') {
      return;
    }
    const workspace = await createWorkspace();
    let leaked: number | undefined;
    const killer: ProcessTreeKiller = {
      async kill(pid) {
        leaked = pid;
      },
    };
    const started = Date.now();
    const result = await runBoundedExec(
      { command: 'node -e "setTimeout(() => {}, 30000)"', timeoutMs: 50, killAfterMs: 10 },
      workspace.context,
      killer,
      null,
      null,
    );
    expect(Date.now() - started).toBeLessThan(5_000);
    expect(result.success).toBe(false);
    expect(result.terminalOutput).toContain('Force-killing timed out command');
    if (leaked !== undefined) {
      try {
        process.kill(-leaked, 'SIGKILL');
      } catch {
        // Already gone.
      }
      try {
        process.kill(leaked, 'SIGKILL');
      } catch {
        // Already gone.
      }
    }
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
      null,
      null,
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
      null,
      null,
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
      null,
      null,
    );

    await sleep(1_000);

    expect(result.success).toBe(false);
    expect(await exists(marker)).toBe(false);
  });

  it('runs the command with its RAM lease as TMPDIR, names held dead leases, and ends the lease after it exits', async () => {
    const workspace = await createWorkspace();
    const directory = join(workspace.root, 'lease');
    await mkdir(directory);
    const held = '/v/9-abcdef is kept: /v/9-abcdef/x.asif (/dev/disk9) still attached from below it';
    const volume = scriptedVolume({
      ok: true,
      value: { kind: 'leased', lease: leaseAt(directory), held: [held], reaped: [] },
    });

    const result = await runBoundedExec(
      { command: 'node -e "console.log(process.env.TMPDIR)"', timeoutMs: 5_000 },
      workspace.context,
      createProcessTreeKiller(),
      volume,
      null,
    );

    expect(result.success).toBe(true);
    expect(result.terminalOutput).toContain(directory);
    expect(result.terminalOutput).toContain(`RAM temp volume: dead lease ${held}\n`);
    expect(volume.released).toEqual([directory]);
  });

  it('stops what the command left working in its lease before the lease ends, and says so', async () => {
    const workspace = await createWorkspace();
    const directory = join(workspace.root, 'lease');
    await mkdir(directory);
    const volume = scriptedVolume(
      {
        ok: true,
        value: { kind: 'leased', lease: leaseAt(directory), held: [], reaped: ['4242 (nx daemon of a dead task)'] },
      },
      null,
      ['31337 (bun nx daemon --start)'],
    );

    const result = await runBoundedExec(
      { command: 'node -e "0"', timeoutMs: 5_000 },
      workspace.context,
      createProcessTreeKiller(),
      volume,
      null,
    );

    expect(result.success).toBe(true);
    expect(result.terminalOutput).toContain(
      'RAM temp volume: stopped 31337 (bun nx daemon --start), which the command left working in its lease\n',
    );
    expect(result.terminalOutput).toContain(
      'RAM temp volume: stopped 4242 (nx daemon of a dead task), which a dead task left working in its lease\n',
    );
    expect(volume.order).toEqual([`reap ${directory}`, `release ${directory}`]);
  });

  it('keeps the inherited TMPDIR inside a sandbox and says so once', async () => {
    const workspace = await createWorkspace();
    const volume = scriptedVolume({ ok: true, value: { kind: 'sandboxed', detail: 'lock: EPERM' } });

    const result = await runBoundedExec(
      { command: `node -e 'console.log("tmp=" + process.env.TMPDIR)'`, timeoutMs: 5_000, env: { TMPDIR: '/shed/tmp' } },
      workspace.context,
      createProcessTreeKiller(),
      volume,
      null,
    );

    expect(result.success).toBe(true);
    expect(result.terminalOutput).toContain('tmp=/shed/tmp');
    expect(result.terminalOutput.match(/RAM temp volume unavailable in this sandbox \(lock: EPERM\)/g)).toHaveLength(1);
    expect(volume.released).toEqual([]);
  });

  it('does not run the command when the volume cannot be provisioned', async () => {
    const workspace = await createWorkspace();
    const marker = join(workspace.root, 'ran');
    const volume = scriptedVolume({
      ok: false,
      error: { kind: 'provision-failed', step: 'hdiutil attach', detail: 'exit 1: Device not configured' },
    });

    const result = await runBoundedExec(
      { command: `touch ${marker}`, timeoutMs: 5_000 },
      workspace.context,
      createProcessTreeKiller(),
      volume,
      null,
    );

    expect(result.success).toBe(false);
    expect(result.terminalOutput).toBe('RAM temp volume: hdiutil attach failed: exit 1: Device not configured\n');
    expect(await exists(marker)).toBe(false);
  });

  it('names the full volume when a failed command left it without space', async () => {
    const workspace = await createWorkspace();
    const directory = join(workspace.root, 'lease');
    await mkdir(directory);
    const volume = scriptedVolume(
      { ok: true, value: { kind: 'leased', lease: leaseAt(directory), held: [], reaped: [] } },
      { kind: 'volume-full', mountpoint: workspace.root, capacityBytes: 1024 * 1024 * 1024, freeBytes: 1024 * 1024 },
    );

    const result = await runBoundedExec(
      { command: 'node -e "process.exit(1)"', timeoutMs: 5_000 },
      workspace.context,
      createProcessTreeKiller(),
      volume,
      null,
    );

    expect(result.success).toBe(false);
    expect(result.terminalOutput).toContain(`RAM temp volume ${workspace.root} is full: 1 MiB free of 1024 MiB`);
    expect(volume.released).toEqual([directory]);
  });

  it('starts the runner once granted, sized to its grant, and returns the tokens when it exits', async () => {
    const workspace = await createWorkspace();
    const asked: { want: number; checkout: string; command: string }[] = [];
    const released: number[] = [];
    const budget: CpuBudget = {
      async take(want, checkout, command) {
        asked.push({ want, checkout, command });
        return { granted: true, tokens: 3, release: () => released.push(3) };
      },
    };
    // `nextest run` in a trailing comment makes the shell command a nextest runner by invocation.
    const command =
      "node -e \"console.log('threads=' + process.env.NEXTEST_TEST_THREADS + ' tokens=' + process.env.BOUNDED_EXEC_CPU_TOKENS)\" # nextest run";

    const result = await runBoundedExec(
      { command, timeoutMs: 5_000, parallelism: 8, env: { NEXTEST_TEST_THREADS: '64' } },
      workspace.context,
      createProcessTreeKiller(),
      null,
      budget,
    );

    expect(result.success).toBe(true);
    expect(asked).toEqual([{ want: 8, checkout: workspace.root, command }]);
    expect(result.terminalOutput).toContain('threads=3 tokens=3');
    expect(result.terminalOutput).toMatch(
      /^cowshed: cpu-tokens wait done elapsed=\d+ms status=ok tokens=3\/8 runner=nextest$/m,
    );
    expect(released).toEqual([3]);
  });

  it('runs the command as asked, saying why, when the budget refuses', async () => {
    const workspace = await createWorkspace();
    const budget: CpuBudget = {
      async take() {
        return { granted: false, cause: 'predates', reason: 'the gateway predates the CPU budget' };
      },
    };

    // A nextest runner configured for 64 threads keeps them: nothing sizes it without a grant.
    const result = await runBoundedExec(
      {
        command: 'node -e "console.log(\'threads=\' + process.env.NEXTEST_TEST_THREADS)" # nextest run',
        timeoutMs: 5_000,
        env: { NEXTEST_TEST_THREADS: '64' },
      },
      workspace.context,
      createProcessTreeKiller(),
      null,
      budget,
    );

    expect(result.success).toBe(true);
    expect(result.terminalOutput).toContain('threads=64');
    expect(result.terminalOutput).toContain(
      'status=err runner=nextest runs unbudgeted: the gateway predates the CPU budget',
    );
  });

  it('says nothing and runs as asked where no gateway listens', async () => {
    const workspace = await createWorkspace();
    const budget: CpuBudget = {
      async take() {
        return { granted: false, cause: 'absent', reason: 'no socket' };
      },
    };

    const result = await runBoundedExec(
      { command: 'node -e "console.log(\'ran\')"', timeoutMs: 5_000 },
      workspace.context,
      createProcessTreeKiller(),
      null,
      budget,
    );

    expect(result.success).toBe(true);
    expect(result.terminalOutput).toBe('ran\n');
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

function leaseAt(directory: string): RamTempLease {
  return { directory, mountpoint: dirname(directory), capacityBytes: 1024 * 1024 * 1024 };
}

/** A temp volume whose answers are fixed, recording which leases were reaped and released, in order. */
function scriptedVolume(
  acquisition: Result<RamTempAcquisition, RamTempError>,
  full: RamTempError | null = null,
  leftovers: string[] = [],
): TempVolume & { released: string[]; order: string[] } {
  const released: string[] = [];
  const order: string[] = [];
  return {
    released,
    order,
    acquire: () => Promise.resolve(acquisition),
    reap(lease) {
      order.push(`reap ${lease.directory}`);
      return Promise.resolve({ ok: true, value: leftovers });
    },
    release(lease) {
      released.push(lease.directory);
      order.push(`release ${lease.directory}`);
      return Promise.resolve({ ok: true, value: undefined });
    },
    fullness: () => Promise.resolve(full),
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
