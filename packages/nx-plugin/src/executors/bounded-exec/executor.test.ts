import { afterEach, describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { access, mkdir, mkdtemp, open, readFile, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import { availableParallelism, tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { onProgressClock } from '../../__tests__/progress-clock.js';
import { ownedSocketDirectory } from '../../testing.js';
import { CPU_POOL_ENV, type CpuPool, createCpuPool, type InheritedCpuPool, readInheritedCpuPool } from './cpu-pool.js';
import { CPU_TOKENS_ENV, type CpuBudget, parseInheritedTokens, type TokenGrant } from './cpu-tokens.js';
import {
  type BoundedExecContext,
  createProcessTreeKiller,
  type ProcessTreeKiller,
  runBoundedExec,
  type TempVolume,
} from './executor.js';
import type { HangDiagnostics } from './hang-diagnostics.js';
import type { RamTempAcquisition, RamTempError, RamTempLease, Result } from './ram-temp.js';
import { VERDICT_FILE } from './verdict.js';

const workspaces: string[] = [];
const ownedProcessGroups = new Set<number>();
const originalStdoutWrite = process.stdout.write;
const originalStderrWrite = process.stderr.write;

const nextestWant = Math.min(8, parseInheritedTokens(process.env.NEXTEST_TEST_THREADS) ?? 8);

/**
 * Prints the budget variables it sees, then what a grandchild sees: a command's environment reaches its descendants,
 * so a budget that leaked past its grant shows on both lines.
 */
const BUDGET_PROBE = `const { execFileSync } = require('node:child_process');
const read = 'JSON.stringify([process.env.${CPU_POOL_ENV} ?? null, process.env.${CPU_TOKENS_ENV} ?? null])';
console.log(JSON.stringify([process.env.${CPU_POOL_ENV} ?? null, process.env.${CPU_TOKENS_ENV} ?? null]));
console.log(execFileSync('node', ['-p', read]).toString().trim());
`;

describe('@smoothbricks/nx-plugin:bounded-exec', () => {
  afterEach(async () => {
    process.stdout.write = originalStdoutWrite;
    process.stderr.write = originalStderrWrite;
    for (const pid of ownedProcessGroups) {
      try {
        process.kill(-pid, 'SIGKILL');
      } catch (cause) {
        if (!(cause instanceof Error && 'code' in cause && cause.code === 'ESRCH')) {
          throw new Error(`cleanup owned readiness process group pgid=${pid} failed`, { cause });
        }
      } finally {
        ownedProcessGroups.delete(pid);
      }
    }
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

  it('kills a silent command on the progress bound, with no ceiling at all', async () => {
    const workspace = await createWorkspace();

    const result = await runBoundedExec(
      { command: 'node -e "setTimeout(() => {}, 30000)"', idleTimeoutMs: 100, killAfterMs: 0 },
      workspace.context,
      createProcessTreeKiller(),
      null,
      null,
    );

    expect(result.success).toBe(false);
    expect(result.terminalOutput).toContain('Command made no progress: no test finished for 100ms');
    expect(result.terminalOutput).toContain('idleTimeoutMs=100');
    expect(result.terminalOutput).toContain('the runner named no test still running');
    // The bound that fired is named, so a hang is never reported as a slow run.
    expect(result.terminalOutput).not.toContain('Command timed out after');
  });

  it('kills a runner that keeps warning of a slow test but finishes none, and names the test', async () => {
    const workspace = await createWorkspace();
    // nextest's own lines: one test finishes, then the other only ever says SLOW, every 50ms.
    await workspace.write(
      'slow-runner.js',
      [
        "console.error('        PASS [   0.010s] (1/2) fake tests::quick');",
        'let periods = 0;',
        "setInterval(() => console.error('        SLOW [>  ' + ++periods * 0.05 + 's] (───) fake tests::hangs'), 50);",
        '',
      ].join('\n'),
    );

    // The window ends only once the runner has said SLOW, however long node takes to start.
    const { result } = await onProgressClock(
      (chunk) => chunk.includes('SLOW [>'),
      400,
      () =>
        runBoundedExec(
          { command: 'node slow-runner.js', idleTimeoutMs: 400, killAfterMs: 0 },
          workspace.context,
          createProcessTreeKiller(),
          null,
          null,
        ),
    );

    expect(result.success).toBe(false);
    expect(result.terminalOutput).toContain('SLOW [>');
    expect(result.terminalOutput).toContain('Command made no progress: no test finished for 400ms (idleTimeoutMs=400)');
    expect(result.terminalOutput).toContain('still running: fake tests::hangs (cwd=');
    expect(result.terminalOutput).not.toContain('fake tests::quick (cwd=');
  });

  it('emits a completed pre-expiry snapshot or an explicit incomplete capture without waiting for it', async () => {
    const workspace = await createWorkspace();
    // Child output drives the fake executor clock; the heartbeat period is not a timing oracle.
    await workspace.write('snapshot-clock.js', "setInterval(() => console.log('snapshot-clock'), 20);\n");
    for (const completes of [true, false]) {
      let pid: number | undefined;
      let signal: AbortSignal | undefined;
      const { result } = await onProgressClock(
        (chunk) => chunk.includes('snapshot-clock\n'),
        5_000,
        () =>
          runBoundedExec(
            { command: 'node snapshot-clock.js', idleTimeoutMs: 10_000, killAfterMs: 0 },
            workspace.context,
            createProcessTreeKiller(),
            null,
            null,
            null,
            async (request) => {
              pid = request.pid;
              signal = request.signal;
              if (!completes) return new Promise<HangDiagnostics>(() => {});
              const capturedAtMs = Date.now();
              return {
                startedAtMs: capturedAtMs,
                capturedAtMs,
                offsetFromExpiryMs: request.expiresAtMs - capturedAtMs,
                outcome: 'holders',
                text: 'owned pre-signal process observation\n',
              };
            },
          ),
      );
      expect(result.success).toBe(false);
      expect(pid).toBeGreaterThan(0);
      expect(signal?.aborted).toBe(true);
      expect(result.terminalOutput).toContain(`Hang diagnostics: runner pid=${pid}`);
      if (completes) {
        expect(result.terminalOutput).toContain('outcome=holders capturedAtMs=');
        expect(result.terminalOutput).toContain('actualExpiryOffsetMs=5000');
        expect(result.terminalOutput).toContain('owned pre-signal process observation');
      } else {
        expect(result.terminalOutput).toContain('capture incomplete at expiry');
        expect(result.terminalOutput).toContain('no empty-holder conclusion');
      }
    }
  });

  it('discards the previous snapshot when a test finishes and captures the next idle interval', async () => {
    const workspace = await createWorkspace();
    // Only observed child output advances the fake clock, never elapsed host time.
    await workspace.write(
      'snapshot-progress.js',
      [
        "const { existsSync } = require('node:fs');",
        'let progressed = false;',
        'setInterval(() => {',
        "  if (!progressed && existsSync('progress')) {",
        "    console.log('        PASS [   0.010s] (1/1) fake tests::progress');",
        '    progressed = true;',
        '  }',
        "  console.log('snapshot-clock');",
        '}, 20);',
        '',
      ].join('\n'),
    );
    const signals: Array<AbortSignal | undefined> = [];
    const { result } = await onProgressClock(
      (chunk) => chunk.includes('snapshot-clock\n'),
      5_000,
      () =>
        runBoundedExec(
          { command: 'node snapshot-progress.js', idleTimeoutMs: 10_000, killAfterMs: 0 },
          workspace.context,
          createProcessTreeKiller(),
          null,
          null,
          null,
          async (request) => {
            signals.push(request.signal);
            const ordinal = signals.length;
            if (ordinal === 1) await workspace.write('progress', 'go');
            const capturedAtMs = Date.now();
            return {
              startedAtMs: capturedAtMs,
              capturedAtMs,
              offsetFromExpiryMs: request.expiresAtMs - capturedAtMs,
              outcome: 'holders',
              text: `capture-interval-${ordinal}\n`,
            };
          },
        ),
    );
    expect(result.success).toBe(false);
    expect(signals).toHaveLength(2);
    expect(signals.every((signal) => signal?.aborted)).toBe(true);
    expect(result.terminalOutput).not.toContain('capture-interval-1');
    expect(result.terminalOutput).toContain('capture-interval-2');
  });

  it('kills a command whose only output is a heartbeat', async () => {
    const workspace = await createWorkspace();
    await workspace.write('heartbeat.js', ["setInterval(() => console.log('still waiting...'), 20);", ''].join('\n'));

    const { result } = await onProgressClock(
      (chunk) => chunk.includes('still waiting...'),
      300,
      () =>
        runBoundedExec(
          { command: 'node heartbeat.js', idleTimeoutMs: 300, killAfterMs: 0 },
          workspace.context,
          createProcessTreeKiller(),
          null,
          null,
        ),
    );

    expect(result.success).toBe(false);
    expect(result.terminalOutput).toContain('still waiting...');
    expect(result.terminalOutput).toContain('Command made no progress: no test finished for 300ms');
  });

  it('lets a run that keeps finishing tests outlive its progress bound', async () => {
    const workspace = await createWorkspace();
    // Ten tests under a 400ms progress bound and no ceiling, the clock moving 300ms at each: the run outlives the
    // bound, so passing proves the bound measures the gap between finished tests, not duration.
    await workspace.write(
      'progressing.js',
      [
        'let done = 0;',
        'const timer = setInterval(() => {',
        '  done += 1;',
        "  console.error('        PASS [   0.120s] (' + done + '/10) fake tests::t' + done);",
        '  if (done === 10) clearInterval(timer);',
        '}, 120);',
        '',
      ].join('\n'),
    );

    const { result, movedMs } = await onProgressClock(
      (chunk) => chunk.includes('PASS ['),
      300,
      () =>
        runBoundedExec(
          { command: 'node progressing.js', idleTimeoutMs: 400 },
          workspace.context,
          createProcessTreeKiller(),
          null,
          null,
        ),
    );

    expect(result.success).toBe(true);
    expect(result.terminalOutput).toContain('(10/10) fake tests::t10');
    expect(result.terminalOutput).not.toContain('made no progress');
    expect(movedMs).toBeGreaterThan(400);
  });

  it("counts bun's dots on stderr as finished tests", async () => {
    const workspace = await createWorkspace();
    // bun --dots writes one dot per finished test and no newline until another stream writes.
    await workspace.write(
      'dots.js',
      [
        'let done = 0;',
        'const timer = setInterval(() => {',
        "  process.stderr.write('.');",
        '  if (++done === 8) { clearInterval(timer); process.stderr.write("\\n"); }',
        '}, 120);',
        '',
      ].join('\n'),
    );

    const { result } = await onProgressClock(
      (chunk) => /^\.+$/.test(chunk),
      300,
      () =>
        runBoundedExec(
          { command: 'node dots.js', idleTimeoutMs: 400 },
          workspace.context,
          createProcessTreeKiller(),
          null,
          null,
        ),
    );

    expect(result.success).toBe(true);
    expect(result.terminalOutput).toContain('........');
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
    const ready = join(workspace.root, 'cooperative-ready.fifo');
    const handled = join(workspace.root, 'handled.txt');
    expect(spawnSync('mkfifo', [ready]).status).toBe(0);
    await workspace.write(
      'cooperative.js',
      [
        "const fs = require('node:fs');",
        "process.on('SIGTERM', () => {",
        "  fs.writeFileSync('handled.txt', 'handled pid=' + process.pid + '\\n');",
        '  process.exit(0);',
        '});',
        "fs.writeFileSync('cooperative-ready.fifo', 'ready pid=' + process.pid + '\\n');",
        'setTimeout(() => {}, 5000);',
        '',
      ].join('\n'),
    );
    let handlerPid: number | undefined;
    const termination = armedKiller('cooperative SIGTERM handler', async (pid) => {
      handlerPid = await armedPid(ready, pid, 'cooperative SIGTERM handler');
      expect(handlerPid, `handler readiness must name the command leader pid=${pid}`).toBe(pid);
    });

    const result = await runBoundedExec(
      { command: 'exec node cooperative.js', timeoutMs: 50, killAfterMs: 500 },
      workspace.context,
      termination.killer,
      null,
      null,
    );

    await termination.armed();
    expect(result.success).toBe(false);
    expect(await readFile(handled, 'utf8'), `cooperative handler pid=${handlerPid} must handle SIGTERM`).toBe(
      `handled pid=${handlerPid}\n`,
    );
    expect(termination.calls).toEqual(['SIGTERM']);
  });

  it('force-kills after killAfterMs when graceful termination is ignored on POSIX', async () => {
    if (process.platform === 'win32') {
      return;
    }

    const workspace = await createWorkspace();
    const ignoring = ignoresTerm(workspace.root);
    const calls: string[] = [];
    const killer: ProcessTreeKiller = {
      async kill(pid, signal) {
        if (signal === 'SIGTERM') await ignoring.armed();
        calls.push(signal);
        process.kill(-pid, signal);
      },
    };

    const result = await runBoundedExec(
      {
        command: ignoring.command,
        timeoutMs: 50,
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
    const ready = join(workspace.root, 'descendant-ready.fifo');
    const lifetime = join(workspace.root, 'descendant-lifetime.fifo');
    expect(spawnSync('mkfifo', [ready, lifetime]).status).toBe(0);
    await workspace.write(
      'group-child.js',
      [
        "const fs = require('node:fs');",
        "fs.openSync('descendant-lifetime.fifo', 'w');",
        "fs.writeFileSync('descendant-ready.fifo', 'ready pid=' + process.pid + '\\n');",
        "setTimeout(() => fs.writeFileSync(process.argv[2], 'alive'), 700);",
        '',
      ].join('\n'),
    );
    await workspace.write(
      'spawn-child.js',
      [
        "import { spawn } from 'node:child_process';",
        "spawn(process.execPath, ['group-child.js', process.argv[2]], { stdio: 'ignore' });",
        'setTimeout(() => {}, 5000);',
        '',
      ].join('\n'),
    );
    // Only the descendant owns the writer; EOF observes its actual terminal closure, not a time horizon.
    const closed = readToEnd(lifetime);
    let descendantPid: number | undefined;
    const termination = armedKiller('process-group descendant', async (pid) => {
      descendantPid = await armedPid(ready, pid, 'process-group descendant');
      expect(descendantPid, `ready descendant must differ from group leader pid=${pid}`).not.toBe(pid);
    });

    const result = await runBoundedExec(
      { command: `exec node spawn-child.js ${marker}`, timeoutMs: 50, killAfterMs: 50 },
      workspace.context,
      termination.killer,
      null,
      null,
    );

    await termination.armed();
    expect(await closed, `ready descendant pid=${descendantPid} must close its lifetime FIFO on exit`).toBe('');
    expect(result.success).toBe(false);
    expect(await exists(marker)).toBe(false);
  });

  it('force-kills what outlives a timed-out command in its process group', async () => {
    if (process.platform === 'win32') {
      return;
    }

    const workspace = await createWorkspace();
    const fifo = join(workspace.root, 'straggler.fifo');
    const ready = join(workspace.root, 'straggler-ready.fifo');
    expect(spawnSync('mkfifo', [fifo, ready]).status).toBe(0);
    // The leader exits on SIGTERM. The subshell beside it ignores SIGTERM, a disposition its
    // `sleep` inherits, and announces itself only once it holds the FIFO: from then on the
    // graceful signal cannot end it. A process forked while that signal goes out is the same
    // case, without the trap.
    await workspace.write(
      'straggle.sh',
      [
        '( trap "" TERM; exec 3>"$1"; echo ready; echo armed >"$2"; sleep 10; echo survived >&3 ) | {',
        '  read -r line; echo "$line"; exec sleep 30',
        '}',
        '',
      ].join('\n'),
    );
    // The straggler is the FIFO's only writer, so the read ends when it is gone: with nothing
    // written if it was killed, with "survived" if it lived to finish its sleep.
    const straggler = readToEnd(fifo);
    const termination = armedKiller('TERM-ignoring FIFO straggler', async (pid) => {
      expect(await readToEnd(ready), `straggler readiness pgid=${pid} must follow trap and FIFO open`).toBe('armed\n');
    });

    const result = await runBoundedExec(
      { command: `sh straggle.sh ${fifo} ${ready}`, timeoutMs: 5_000, killAfterMs: 30_000 },
      workspace.context,
      termination.killer,
      null,
      null,
    );

    await termination.armed();
    expect(result.terminalOutput).toContain('ready');
    expect(await straggler).toBe('');
    expect(result.success).toBe(false);
    expect(result.terminalOutput).toContain('Force-killing what the timed-out command left in its process group');
  }, 60_000);

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

  it('keeps inherited nextest and Cargo ceilings under oversized grants', async () => {
    const previousNextest = process.env.NEXTEST_TEST_THREADS;
    const previousCargo = process.env.CARGO_BUILD_JOBS;
    const previousRust = process.env.RUST_TEST_THREADS;
    process.env.NEXTEST_TEST_THREADS = '6';
    process.env.CARGO_BUILD_JOBS = '4';
    process.env.RUST_TEST_THREADS = '6';
    try {
      for (const tokens of [15, 18]) {
        const workspace = await createWorkspace();
        const asked: number[] = [];
        const released: number[] = [];
        const budget: CpuBudget = {
          async take(want) {
            asked.push(want);
            return heldGrant(tokens, released);
          },
        };
        for (const scenario of [
          { runner: 'nextest run', variable: 'NEXTEST_TEST_THREADS', cap: 6 },
          { runner: 'cargo test', variable: 'CARGO_BUILD_JOBS', cap: 4 },
        ]) {
          const command = `node -e "console.log('cap=' + process.env.${scenario.variable} + ' held=' + process.env.BOUNDED_EXEC_CPU_TOKENS)" # ${scenario.runner}`;
          const result = await runBoundedExec(
            {
              command,
              timeoutMs: 5_000,
              parallelism: 18,
              env: { NEXTEST_TEST_THREADS: '64', CARGO_BUILD_JOBS: '64', RUST_TEST_THREADS: '64' },
            },
            workspace.context,
            createProcessTreeKiller(),
            null,
            budget,
          );
          expect(result.success).toBe(true);
          expect(asked.at(-1)).toBe(scenario.cap);
          expect(result.terminalOutput).toContain(`cap=${scenario.cap} held=${tokens}`);
        }
        expect(released).toEqual([tokens, tokens]);
      }
    } finally {
      if (previousNextest === undefined) delete process.env.NEXTEST_TEST_THREADS;
      else process.env.NEXTEST_TEST_THREADS = previousNextest;
      if (previousCargo === undefined) delete process.env.CARGO_BUILD_JOBS;
      else process.env.CARGO_BUILD_JOBS = previousCargo;
      if (previousRust === undefined) delete process.env.RUST_TEST_THREADS;
      else process.env.RUST_TEST_THREADS = previousRust;
    }
  });

  it('starts the runner once granted, sized to its grant, and returns the tokens when it exits', async () => {
    const workspace = await createWorkspace();
    const asked: { want: number; checkout: string; command: string }[] = [];
    const released: number[] = [];
    const budget: CpuBudget = {
      async take(want, checkout, command) {
        asked.push({ want, checkout, command });
        return heldGrant(3, released);
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
    expect(asked).toEqual([{ want: nextestWant, checkout: workspace.root, command }]);
    expect(result.terminalOutput).toContain(`threads=${Math.min(3, nextestWant)} tokens=3`);
    expect(result.terminalOutput).toMatch(
      new RegExp(
        `^cowshed: cpu-tokens wait done elapsed=\\d+ms status=ok tokens=3/${nextestWant} runner=nextest$`,
        'm',
      ),
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

  it('sizes a runner to the grant its parent pool lent, takes nothing from the host, and returns nothing to it', async () => {
    const workspace = await createWorkspace();
    const asked: number[] = [];
    const released: number[] = [];
    const budget: CpuBudget = {
      async take(want) {
        asked.push(want);
        return heldGrant(1, released);
      },
    };
    const lent = lentCount(nextestWant, 3);

    const result = await withPool(3, (pool) =>
      runBoundedExec(
        {
          command: 'node -e "console.log(\'tokens=\' + process.env.BOUNDED_EXEC_CPU_TOKENS)" # nextest run',
          timeoutMs: 5_000,
          parallelism: 8,
        },
        workspace.context,
        createProcessTreeKiller(),
        null,
        budget,
        inheritedFrom(pool),
      ),
    );

    expect(result.success).toBe(true);
    expect(asked).toEqual([]);
    expect(released).toEqual([]);
    expect(result.terminalOutput).toContain(`tokens=${lent}\n`);
    expect(result.terminalOutput).toContain(`inherited=true tokens=${lent}/${nextestWant} runner=nextest`);
  });

  it('lends a runner no more than it asks for, even with no budget client', async () => {
    const workspace = await createWorkspace();
    const lent = lentCount(2, 16);

    const result = await withPool(16, (pool) =>
      runBoundedExec(
        {
          command: 'node -e "console.log(\'threads=\' + process.env.NEXTEST_TEST_THREADS)" # nextest run',
          timeoutMs: 5_000,
          parallelism: 2,
        },
        workspace.context,
        createProcessTreeKiller(),
        null,
        null,
        inheritedFrom(pool),
      ),
    );

    expect(result.success).toBe(true);
    expect(result.terminalOutput).toContain(`threads=${lent}\n`);
    expect(result.terminalOutput).toContain(`inherited=true tokens=${lent}/2 runner=nextest`);
  });

  it('takes and returns its own grant when no parent lent one', async () => {
    const workspace = await createWorkspace();
    const asked: number[] = [];
    const released: number[] = [];
    const budget: CpuBudget = {
      async take(want) {
        asked.push(want);
        return heldGrant(3, released);
      },
    };

    const result = await runBoundedExec(
      { command: 'node -e "console.log(1)" # nextest run', timeoutMs: 5_000, parallelism: 8 },
      workspace.context,
      createProcessTreeKiller(),
      null,
      budget,
      null,
    );

    expect(result.success).toBe(true);
    expect(asked).toEqual([nextestWant]);
    expect(released).toEqual([3]);
    expect(result.terminalOutput).toMatch(
      new RegExp(
        `^cowshed: cpu-tokens wait done elapsed=\\d+ms status=ok tokens=3/${nextestWant} runner=nextest$`,
        'm',
      ),
    );
    expect(result.terminalOutput).not.toContain('inherited');
  });

  it('records a borrowed grant as inherited, and an owned grant without that mark', async () => {
    const workspace = await createWorkspace();
    const command = 'node -e "0" # nextest run';
    const task = { id: 'app:test', hash: null };
    const borrowedDir = join(workspace.root, 'borrowed');
    const ownedDir = join(workspace.root, 'owned');
    const budget: CpuBudget = {
      async take() {
        return heldGrant(3, []);
      },
    };
    const lent = lentCount(nextestWant, 3);

    const borrowed = await withPool(3, (pool) =>
      runBoundedExec(
        { command, timeoutMs: 5_000, parallelism: 8 },
        { ...workspace.context, record: { task, directory: borrowedDir } },
        createProcessTreeKiller(),
        null,
        null,
        inheritedFrom(pool),
      ),
    );
    const owned = await runBoundedExec(
      { command, timeoutMs: 5_000, parallelism: 8 },
      { ...workspace.context, record: { task, directory: ownedDir } },
      createProcessTreeKiller(),
      null,
      budget,
      null,
    );

    expect(borrowed.success).toBe(true);
    expect(owned.success).toBe(true);
    const borrowedRecord: unknown = JSON.parse(await readFile(join(borrowedDir, VERDICT_FILE), 'utf8'));
    expect(borrowedRecord).toHaveProperty('cpu', {
      tokens: lent,
      want: nextestWant,
      waitedMs: expect.any(Number),
      inherited: true,
    });
    const ownedRecord: unknown = JSON.parse(await readFile(join(ownedDir, VERDICT_FILE), 'utf8'));
    expect(ownedRecord).toHaveProperty('cpu', { tokens: 3, want: nextestWant, waitedMs: expect.any(Number) });
    expect(ownedRecord).not.toHaveProperty('cpu.inherited');
  });

  it('records borrowed grant loss during timeout termination as an ordinary failure, not a bound', async () => {
    if (process.platform === 'win32') return;
    const workspace = await createWorkspace();
    const directory = join(workspace.root, 'lost-during-timeout');
    const ignoring = ignoresTerm(workspace.root);
    const calls: NodeJS.Signals[] = [];
    const result = await withPool(1, (pool) =>
      runBoundedExec(
        {
          command: ignoring.command,
          timeoutMs: 50,
          killAfterMs: 50,
        },
        { ...workspace.context, record: { task: { id: 'lost-during-timeout', hash: null }, directory } },
        {
          async kill(pid, signal) {
            if (signal === 'SIGTERM') {
              await ignoring.armed();
              const closed = await pool.close();
              if (!closed.ok) throw new Error(closed.error);
            }
            calls.push(signal);
            await createProcessTreeKiller().kill(pid, signal);
          },
        },
        null,
        null,
        inheritedFrom(pool),
      ),
    );
    expect(result.success).toBe(false);
    expect(calls).toEqual(['SIGTERM', 'SIGKILL']);
    expect(result.terminalOutput).toContain('cpu-tokens grant lost');
    const record: unknown = JSON.parse(await readFile(join(directory, VERDICT_FILE), 'utf8'));
    expect(record).toHaveProperty('verdict.outcome', 'failed');
  });

  it('asks the host budget when a parent pool refuses its descriptor, and says why', async () => {
    const workspace = await createWorkspace();
    const asked: number[] = [];
    const released: number[] = [];
    const budget: CpuBudget = {
      async take(want) {
        asked.push(want);
        return heldGrant(3, released);
      },
    };
    const gone = { socket: join(workspace.root, 'gone.sock'), tokens: 3 };

    const result = await runBoundedExec(
      { command: 'node -e "console.log(\'ran\')" # nextest run', timeoutMs: 5_000, parallelism: 8 },
      workspace.context,
      createProcessTreeKiller(),
      null,
      budget,
      gone,
    );

    expect(result.success).toBe(true);
    expect(asked).toEqual([nextestWant]);
    expect(released).toEqual([3]);
    expect(result.terminalOutput).toContain('cowshed: cpu-tokens parent pool refused: ');
    expect(result.terminalOutput).toContain(`status=ok tokens=3/${nextestWant} runner=nextest`);
    expect(result.terminalOutput).not.toContain('inherited=true');
  });

  it('ignores a target env that names the CPU budget, when no budget grants it', async () => {
    const workspace = await createWorkspace();

    const result = await runBoundedExec(
      {
        command: 'node -e "console.log(\'tokens=\' + process.env.BOUNDED_EXEC_CPU_TOKENS)"',
        timeoutMs: 5_000,
        env: { BOUNDED_EXEC_CPU_TOKENS: '977' },
      },
      workspace.context,
      createProcessTreeKiller(),
      null,
      null,
    );

    expect(result.success).toBe(true);
    expect(result.terminalOutput).toContain('tokens=undefined\n');
    expect(result.terminalOutput).toContain(
      `cowshed: BOUNDED_EXEC_CPU_TOKENS in the target's env is ignored: the CPU budget sets it`,
    );
  });

  it('sizes the command from its own grant, whatever the target env names', async () => {
    const workspace = await createWorkspace();
    const released: number[] = [];
    const budget: CpuBudget = {
      async take() {
        return heldGrant(2, released);
      },
    };

    const result = await runBoundedExec(
      {
        command: 'node -e "console.log(\'tokens=\' + process.env.BOUNDED_EXEC_CPU_TOKENS)" # nextest run',
        timeoutMs: 5_000,
        parallelism: 8,
        env: { BOUNDED_EXEC_CPU_TOKENS: '977' },
      },
      workspace.context,
      createProcessTreeKiller(),
      null,
      budget,
    );

    expect(result.success).toBe(true);
    expect(result.terminalOutput).toContain('tokens=2\n');
    expect(result.terminalOutput).not.toContain('977');
    expect(released).toEqual([2]);
  });

  it('refuses the run when its parent pool answers with a refusal, asking no host budget', async () => {
    const socketDirectory = await ownedSocketDirectory('cpu-refuse-', 'p.sock');
    workspaces.push(socketDirectory);
    const socket = join(socketDirectory, 'p.sock');
    // A live pool that refuses answers `other`, not `absent`: the run must fail, not ask the host.
    const refusing = createServer((connection) => {
      connection.once('data', () => {
        connection.end('{"ok":false,"code":"refused","error":"the pool is shutting down"}\n');
      });
    });
    await new Promise<void>((resolve) => refusing.listen(socket, resolve));
    const workspace = await createWorkspace();
    const asked: number[] = [];
    const budget: CpuBudget = {
      async take(want) {
        asked.push(want);
        return heldGrant(1, []);
      },
    };
    try {
      const result = await runBoundedExec(
        { command: 'touch ran.marker', timeoutMs: 5_000 },
        {
          ...workspace.context,
          record: { task: { id: 'refused', hash: null }, directory: join(workspace.root, 'refused') },
        },
        createProcessTreeKiller(),
        null,
        budget,
        { socket, tokens: 3 },
      );

      expect(result.success).toBe(false);
      expect(asked).toEqual([]);
      expect(await exists(join(workspace.root, 'ran.marker'))).toBe(false);
      expect(result.terminalOutput).toContain('cowshed: cpu-tokens parent pool refused: ');
      expect(result.terminalOutput).toContain('refusing to run the command without asking the host budget');
      expect(await readFile(join(workspace.root, 'refused', VERDICT_FILE), 'utf8')).toMatch(/"outcome":\s*"failed"/);
    } finally {
      await new Promise<void>((resolve) => refusing.close(() => resolve()));
    }
  });

  it('never gives a command a budget it was not granted, whatever pool or count its environment names', async () => {
    const workspace = await createWorkspace();
    await workspace.write('probe.js', BUDGET_PROBE);
    const previousPool = process.env[CPU_POOL_ENV];
    const previousTokens = process.env[CPU_TOKENS_ENV];
    // A stale descriptor in this process's own environment is no budget either.
    process.env[CPU_POOL_ENV] = '/stale.sock';
    process.env[CPU_TOKENS_ENV] = '5';
    try {
      const absent: CpuBudget = {
        async take() {
          return { granted: false, cause: 'absent', reason: 'no budget socket' };
        },
      };
      const unbudgeted = await withPool(3, (pool) =>
        runBoundedExec(
          { command: 'node probe.js', timeoutMs: 10_000, env: { ...pool.environment, [CPU_TOKENS_ENV]: '9' } },
          workspace.context,
          createProcessTreeKiller(),
          null,
          absent,
        ),
      );
      expect(unbudgeted.terminalOutput).toContain('[null,null]\n[null,null]\n');

      const released: number[] = [];
      let injected = '';
      const granted = await withPool(3, (pool) => {
        injected = inheritedFrom(pool).socket;
        return runBoundedExec(
          { command: 'node probe.js', timeoutMs: 10_000, env: { ...pool.environment, [CPU_TOKENS_ENV]: '9' } },
          workspace.context,
          createProcessTreeKiller(),
          null,
          {
            async take() {
              return heldGrant(2, released);
            },
          },
        );
      });
      const [own, grandchild] = granted.terminalOutput.split('\n').filter((line) => line.startsWith('['));
      expect(own).toBe(grandchild);
      expect(own).toMatch(/,"2"\]$/);
      expect(own).not.toContain(injected);
      expect(own).not.toContain('/stale.sock');
      expect(released).toEqual([2]);
    } finally {
      if (previousPool === undefined) {
        delete process.env[CPU_POOL_ENV];
      } else {
        process.env[CPU_POOL_ENV] = previousPool;
      }
      if (previousTokens === undefined) {
        delete process.env[CPU_TOKENS_ENV];
      } else {
        process.env[CPU_TOKENS_ENV] = previousTokens;
      }
    }
  });
});

interface WorkspaceFixture {
  root: string;
  context: BoundedExecContext;
  write(filePath: string, contents: string): Promise<void>;
}

/** Everything written to the FIFO at `path` until its last writer closes it. */
async function readToEnd(path: string): Promise<string> {
  const handle = await open(path, 'r');
  try {
    return await handle.readFile('utf8');
  } finally {
    await handle.close();
  }
}

/** Reuses the real killer, delaying delivery (not the bound) until the named fixture is armed. */
function armedKiller(subject: string, ready: (pid: number) => Promise<void>) {
  const real = createProcessTreeKiller();
  const calls: NodeJS.Signals[] = [];
  let armed: Promise<void> | undefined;
  return {
    calls,
    killer: {
      async kill(pid: number, signal: NodeJS.Signals) {
        ownedProcessGroups.add(pid);
        armed ??= ready(pid);
        await armed;
        calls.push(signal);
        await real.kill(pid, signal);
      },
    } satisfies ProcessTreeKiller,
    async armed() {
      expect(armed, `${subject}: readiness was never requested before timeout signal delivery`).toBeDefined();
      // The executor tolerates killer errors; re-await here so a failed readiness assertion cannot be swallowed.
      await armed;
    },
  };
}

/** A complete readiness line identifies a live member of the executor's own process group. */
async function armedPid(path: string, pgid: number, subject: string): Promise<number> {
  const text = await readToEnd(path);
  const match = /^ready pid=(\d+)\n$/.exec(text);
  const pid = Number(match?.[1]);
  const detail = `${subject}: ready=${JSON.stringify(text)} fifo=${path} pid=${pid} expected pgid=${pgid}`;
  expect(Number.isSafeInteger(pid) && pid > 0, `${detail}; state=unobserved (invalid readiness PID)`).toBe(true);
  const observed = spawnSync('/bin/ps', ['-o', 'pid=,ppid=,pgid=,stat=', '-p', String(pid)], { encoding: 'utf8' });
  const state = `${detail}; state=${observed.stdout.trim()} status=${observed.status} stderr=${observed.stderr} error=${observed.error}`;
  expect(observed.status, state).toBe(0);
  const [seenPid, , seenGroup, stat] = observed.stdout.trim().split(/\s+/);
  expect(Number(seenPid), state).toBe(pid);
  expect(Number(seenGroup), state).toBe(pgid);
  expect(stat !== undefined && !stat.startsWith('Z'), state).toBe(true);
  return pid;
}

/**
 * A command that ignores SIGTERM, a disposition its `sleep` inherits, and `armed`, which resolves once it does. A
 * killer that awaits `armed` before the graceful signal sends it to a command that cannot die of it: a bound that fires
 * before the command's shell has even run `trap` (a 250 ms timer lost that race in a loaded gate) ends it on the
 * default disposition, and the escalation under test never runs. The command's FIFO write waits for the reader, so
 * the announcement can neither precede the trap nor be missed.
 */
function ignoresTerm(root: string): { command: string; armed: () => Promise<void> } {
  const fifo = join(root, 'armed.fifo');
  expect(spawnSync('mkfifo', [fifo]).status).toBe(0);
  return {
    command: `sh -c 'trap "" TERM; echo armed > "$0"; exec sleep 5' '${fifo}'`,
    armed: async () => {
      expect(await readToEnd(fifo)).toBe('armed\n');
    },
  };
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

/** A granted host grant whose `closed` resolves on release, as the gateway's does when the grant's socket closes. */
function heldGrant(tokens: number, released: number[]): TokenGrant {
  let closeGrant!: () => void;
  const closed = new Promise<void>((resolve) => {
    closeGrant = resolve;
  });
  return {
    granted: true,
    tokens,
    release: () => {
      released.push(tokens);
      closeGrant();
    },
    closed,
  };
}

/** A parent's CPU pool of `tokens`, open while `body` runs. Its upstream never closes on its own. */
async function withPool<T>(tokens: number, body: (pool: CpuPool) => Promise<T>): Promise<T> {
  const created = await createCpuPool(tokens, new Promise<void>(() => {}));
  if (!created.ok) {
    throw new Error(`cpu pool: ${created.error}`);
  }
  let result: { ok: true; value: T } | { ok: false; error: unknown };
  try {
    result = { ok: true, value: await body(created.pool) };
  } catch (error) {
    result = { ok: false, error };
  }
  const outcome = await created.pool.close();
  if (!outcome.ok) {
    const failure = new Error(`cpu pool did not close: ${outcome.error}`);
    throw result.ok ? failure : new AggregateError([result.error, failure], 'CPU pool body and cleanup failed');
  }
  if (!result.ok) throw result.error;
  return result.value;
}

/** The descriptor a run under `pool` inherits: the pool's own environment, read back the way a child reads it. */
function inheritedFrom(pool: CpuPool): InheritedCpuPool {
  const parent = readInheritedCpuPool(pool.environment);
  if (parent === null) {
    throw new Error('the pool environment names no endpoint');
  }
  return parent;
}

/** The count a pool of `tokens` lends a run that wants `want`: min(want, tokens, ceil(cores / 2)). */
function lentCount(want: number, tokens: number): number {
  return Math.min(want, tokens, Math.ceil(availableParallelism() / 2));
}
