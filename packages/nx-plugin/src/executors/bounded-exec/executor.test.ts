import { afterAll, afterEach, beforeAll, describe, expect, it, jest } from 'bun:test';
import { spawn, spawnSync } from 'node:child_process';
import { once } from 'node:events';
import { closeSync } from 'node:fs';
import { access, mkdir, mkdtemp, open, readdir, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { createServer, type Socket } from 'node:net';
import { tmpdir, userInfo } from 'node:os';
import { dirname, join } from 'node:path';
import { enclosingTreeMarker, outsideTreesTmpdir } from '@smoothbricks/cargo-retain/testing/outside-trees';
import { churnScript, openForProbe, writerRemains } from '../../__tests__/churn-group.js';
import { FAKE_BUN_INVOKED, FAKE_BUN_RAN, fakeBun } from '../../__tests__/fake-bun.js';
import { onProgressClock } from '../../__tests__/progress-clock.js';
import { type StandInMode, standInCowshed } from '../../__tests__/stand-in-cowshed.js';
import { BOUNDED_TEST_IDLE_TIMEOUT_MS } from '../../bounded-test-policy.js';
import { processGroupHasMembers } from '../../process-group.js';
import { ownedSocketDirectory, processesOf } from '../../testing.js';
import { CPU_POOL_ENV, type CpuPool, createCpuPool, type InheritedCpuPool, readInheritedCpuPool } from './cpu-pool.js';
import {
  type BoundedExecContext,
  type BoundedExecResult,
  createProcessTreeKiller,
  type ProcessTreeKiller,
  resolveCowshedExecutable,
  runBoundedExec,
  type TempVolume,
} from './executor.js';
import { GATEWAY_SOCKET } from './gateway-lease.js';
import { captureHangDiagnostics, HANG_CAPTURE_MS, type HangDiagnostics } from './hang-diagnostics.js';
import { createNoSpaceWatch } from './no-space.js';
import { cowshedSubtreeOwner, type SubtreeOwner } from './owned-subtree.js';
import {
  createHostCommands,
  describeRamTempError,
  PRODUCTION,
  RAM_TEMP_CAPACITY_BYTES,
  type RamTempAcquisition,
  type RamTempError,
  type RamTempLease,
  RamTempVolume,
  type Result,
  ramTempPaths,
} from './ram-temp.js';
import { CPU_TOKENS_ENV } from './runner-sizing.js';
import { readRecord, type TempVolumeSpace, VERDICT_FILE } from './verdict.js';

const workspaces: string[] = [];
const ownedProcessGroups = new Set<number>();
const originalStdoutWrite = process.stdout.write;
const originalStderrWrite = process.stderr.write;

/** What the nextest cases declare as `parallelism`, and so what they ask: the suite runs under no inherited ceiling. */
const nextestWant = 8;

/**
 * Every sizing variable a holder exports to its command (specs/cowshed/05_gateway.md, "Runners are sized to the count",
 * "Cargo reads its holder's jobserver"). This suite's own process runs under them when a gate's `bounded-exec` started
 * it, and the executor reads them as inherited ceilings and pool beside the ones its cases construct, so the suite clears
 * them
 * (specs/cowshed/08_testing.md: fixtures clear every inherited sizing variable before they set their own).
 */
const INHERITED_SIZING_ENV = [
  CPU_POOL_ENV,
  CPU_TOKENS_ENV,
  'NEXTEST_TEST_THREADS',
  'RUST_TEST_THREADS',
  'CARGO_BUILD_JOBS',
  'CARGO_MAKEFLAGS',
  'MAKEFLAGS',
  'MFLAGS',
] as const;
const inheritedSizing = INHERITED_SIZING_ENV.map((name) => [name, process.env[name]] as const);

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
  beforeAll(() => {
    for (const name of INHERITED_SIZING_ENV) {
      delete process.env[name];
    }
  });

  afterAll(() => {
    for (const [name, value] of inheritedSizing) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  });

  afterEach(async () => {
    process.stdout.write = originalStdoutWrite;
    process.stderr.write = originalStderrWrite;
    for (const pid of ownedProcessGroups) {
      try {
        process.kill(-pid, 'SIGKILL');
      } catch (cause) {
        const code = cause instanceof Error && 'code' in cause ? cause.code : undefined;
        if (code !== 'ESRCH') {
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

    // bounded-exec requires a ceiling; at the leg's backstop it cannot end a run whose `node` is slow to start
    // under load.
    const result = await runBoundedExec(
      {
        command: 'node print.js',
        cwd: 'package',
        env: { BOUNDED_EXEC_VALUE: 'from-env' },
        timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
      },
      workspace.context,
      createProcessTreeKiller(),
      null,
    );

    expect(result.success).toBe(true);
    expect(result.terminalOutput).toContain(join(workspace.root, 'package'));
    expect(result.terminalOutput).toContain('from-env');
  });

  it('returns failure for a nonzero command', async () => {
    const workspace = await createWorkspace();

    const result = await runBoundedExec(
      { command: 'node -e "process.exit(7)"', timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS },
      workspace.context,
      createProcessTreeKiller(),
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
      {
        command: "node -e \"console.log('out-value'); console.error('err-value')\"",
        timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
      },
      workspace.context,
      createProcessTreeKiller(),
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

    // The command never exits, so only the ceiling under test can end it.
    const result = await runBoundedExec(
      { command: 'node -e "process.stdin.resume()"', timeoutMs: 50, killAfterMs: 0 },
      workspace.context,
      createProcessTreeKiller(),
      null,
    );

    expect(result.success).toBe(false);
    expect(result.terminalOutput).toContain('Command timed out after');
    expect(result.terminalOutput).toContain('timeoutMs=50');
    expect(result.terminalOutput).toContain(`cwd=${workspace.root}`);
  });

  it('kills a silent command on the progress bound, with no ceiling at all', async () => {
    const workspace = await createWorkspace();

    // The command never exits and never speaks, so only the progress bound under test can end it.
    const result = await runBoundedExec(
      { command: 'node -e "process.stdin.resume()"', idleTimeoutMs: 100, killAfterMs: 0 },
      workspace.context,
      createProcessTreeKiller(),
      null,
    );

    expect(result.success).toBe(false);
    expect(result.terminalOutput).toContain('Command made no progress: no test finished for 100ms');
    expect(result.terminalOutput).toContain('idleTimeoutMs=100');
    expect(result.terminalOutput).toContain('the runner named no test still running');
    // The bound that fired is named, so a hang is never reported as a slow run.
    expect(result.terminalOutput).not.toContain('Command timed out after');
  });

  it('runs the fixture first, then the command', async () => {
    const workspace = await createWorkspace();

    // Both bounds sit at the backstop, so each phase ends on its process exiting, never on a clock.
    const result = await runBoundedExec(
      {
        command: 'node -e "console.log(\'main-done\')"',
        fixtureCommand: 'node -e "console.log(\'fixture-done\')"',
        fixtureTimeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
        idleTimeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
        killAfterMs: 0,
      },
      workspace.context,
      createProcessTreeKiller(),
      null,
    );

    expect(result.success).toBe(true);
    expect(result.terminalOutput.indexOf('fixture-done')).toBeGreaterThanOrEqual(0);
    expect(result.terminalOutput.indexOf('main-done')).toBeGreaterThan(result.terminalOutput.indexOf('fixture-done'));
  });

  it('runs the fixture outside the progress bound: only its own ceiling ends a silent fixture', async () => {
    const workspace = await createWorkspace();
    const stdout = process.stdout.write;
    let output = '';
    let advanced = false;
    try {
      // The fixture says it runs, which the runner hook lets it do only once its build is acknowledged; then it never
      // exits or speaks again. Only the fake clock moves: past the progress bound (100) nothing may expire, so the
      // fixture's ceiling (200) ending the run proves the fixture ran outside the progress bound. Which bound is armed
      // when is bounds.test.ts's table; this is the one real-process check that the executor carries it out.
      process.stdout.write = (chunk) => {
        output += chunk.toString();
        if (!advanced && output.includes('fixture-running\n')) {
          advanced = true;
          queueMicrotask(() => {
            jest.advanceTimersByTime(150);
            jest.advanceTimersByTime(100);
          });
        }
        return true;
      };
      jest.useFakeTimers();
      const result = await runBoundedExec(
        {
          command: "node -e \"console.log('main' + '-ran')\"",
          fixtureCommand: 'node -e "console.log(\'fixture-running\'); process.stdin.resume()"',
          fixtureTimeoutMs: 200,
          idleTimeoutMs: 100,
          killAfterMs: 0,
        },
        workspace.context,
        createProcessTreeKiller(),
        null,
      );

      expect(advanced, result.terminalOutput).toBe(true);
      expect(result.success).toBe(false);
      expect(result.terminalOutput).toContain('Fixture command timed out after');
      expect(result.terminalOutput).toContain('fixtureTimeoutMs=200');
      expect(result.terminalOutput).not.toContain('Command made no progress');
      expect(result.terminalOutput).not.toContain('main-ran');
    } finally {
      jest.useRealTimers();
      process.stdout.write = stdout;
    }
  });

  it('fails the run on a failed fixture and never starts the command', async () => {
    const workspace = await createWorkspace();

    const result = await runBoundedExec(
      {
        command: "node -e \"console.log('main' + '-ran')\"",
        fixtureCommand: 'node -e "console.error(\'fixture-broke\'); process.exit(3)"',
        fixtureTimeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
        idleTimeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
        killAfterMs: 0,
      },
      workspace.context,
      createProcessTreeKiller(),
      null,
    );

    expect(result.success).toBe(false);
    expect(result.terminalOutput).toContain('fixture-broke');
    expect(result.terminalOutput).toContain('Fixture command exited with status 3');
    expect(result.terminalOutput).not.toContain('main-ran');
  });

  it('ends a fixture that outlives its own ceiling as a fixture bound, not a wedge', async () => {
    const workspace = await createWorkspace();

    // The fixture prints a finished-test line, then stalls: compilation output is not the command's progress,
    // so only the fixture ceiling can end it, and it must name itself rather than the hang it is not.
    const result = await runBoundedExec(
      {
        command: "node -e \"console.log('main' + '-ran')\"",
        fixtureCommand: 'node -e "console.log(\'test fixture::warm ... ok\'); process.stdin.resume()"',
        fixtureTimeoutMs: 150,
        idleTimeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
        killAfterMs: 0,
      },
      workspace.context,
      createProcessTreeKiller(),
      null,
    );

    expect(result.success).toBe(false);
    expect(result.terminalOutput).toContain('Fixture command timed out after');
    expect(result.terminalOutput).toContain('fixtureTimeoutMs=150');
    expect(result.terminalOutput).not.toContain('Command made no progress');
    expect(result.terminalOutput).not.toContain('main-ran');
  });

  it('refuses a fixture named without its ceiling, running neither command', async () => {
    const workspace = await createWorkspace();

    const result = await runBoundedExec(
      {
        // The type requires one bound; an Nx option set is untyped, so the refusal is the executor's to give.
        command: 'node -e "console.log(\'main-ran\')"',
        fixtureCommand: 'node -e "console.log(\'fixture-ran\')"',
        idleTimeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
        killAfterMs: 0,
      },
      workspace.context,
      createProcessTreeKiller(),
      null,
    );

    expect(result.success).toBe(false);
    expect(result.terminalOutput).toContain('fixtureCommand and a positive fixtureTimeoutMs must be named together');
    expect(result.terminalOutput).not.toContain('fixture-ran');
    expect(result.terminalOutput).not.toContain('main-ran');
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
        HANG_CAPTURE_MS,
        () =>
          runBoundedExec(
            { command: 'node snapshot-clock.js', idleTimeoutMs: 2 * HANG_CAPTURE_MS, killAfterMs: 0 },
            workspace.context,
            createProcessTreeKiller(),
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
                lockWaits: [],
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
        expect(result.terminalOutput).toContain(`actualExpiryOffsetMs=${HANG_CAPTURE_MS}`);
        expect(result.terminalOutput).toContain('owned pre-signal process observation');
      } else {
        expect(result.terminalOutput).toContain('capture incomplete at expiry');
        expect(result.terminalOutput).toContain('no empty-holder conclusion');
      }
    }
  });

  it('samples the stack a command hung on a FIFO waits in before the progress bound force-kills it', async () => {
    if (process.platform === 'win32') {
      return;
    }

    const workspace = await createWorkspace();
    // The test owns the FIFO and never opens it for writing, so the command's open(2) of it never returns.
    const fifo = spawnSync('mkfifo', [join(workspace.root, 'hang.fifo')], { encoding: 'utf8' });
    expect(fifo.status, fifo.stderr).toBe(0);
    const real = createProcessTreeKiller();
    const signals: string[] = [];
    let group: number | undefined;
    const killer: ProcessTreeKiller = {
      async kill(pid, signal) {
        signals.push(signal);
        group = pid;
        ownedProcessGroups.add(pid);
        await real.kill(pid, signal);
      },
    };
    // The command's complete `opening` line, read across output chunks: once it is seen the command is past its own
    // start and in, or about to enter, its open(2) of the FIFO.
    let seen = '';
    let ready = false;
    let readyWhenAsked: boolean | undefined;
    const watch = (chunk: string | Uint8Array): boolean => {
      seen += typeof chunk === 'string' ? chunk : Buffer.from(chunk).toString('utf8');
      if (/(?:^|\n)opening\n/.test(seen)) ready = true;
      return true;
    };
    // Real timers end to end: the capture starts its window before the 15 s bound, five seconds after the command
    // starts, and the bound expires on its own schedule, whatever the capture is doing then. The command ignores SIGTERM, so only the force-kill ends it.
    process.stdout.write = watch;
    process.stderr.write = watch;
    try {
      const result = await runBoundedExec(
        {
          command: `perl -e '$SIG{TERM} = "IGNORE"; $| = 1; print "opening\\n"; open(F, "<", "hang.fifo") or die "open: $!"'`,
          idleTimeoutMs: HANG_CAPTURE_MS + 5_000,
          killAfterMs: 0,
        },
        workspace.context,
        killer,
        null,
        null,
        (request) => {
          readyWhenAsked = ready;
          return captureHangDiagnostics(request);
        },
      );
      const output = result.terminalOutput;
      expect(result.success).toBe(false);
      expect(output).toContain(`Command made no progress: no test finished for ${HANG_CAPTURE_MS + 5_000}ms`);
      // The capture began after the command said it was opening the FIFO, and completed before the expiry.
      expect(readyWhenAsked, output).toBe(true);
      expect(output).toContain('Hang snapshot outcome=');
      if (process.platform === 'darwin') {
        // perl's sampled stack names the open(2) it is blocked in.
        expect(output).toMatch(
          /Analysis of sampling perl \(pid \d+\)[\s\S]*Perl_pp_open[\s\S]*\d+ __open\s+\(in libsystem_kernel\.dylib\)/,
        );
      } else {
        expect(output).toContain(`Stack samples not provided on ${process.platform}`);
      }
      expect(signals, output).toContain('SIGKILL');
      if (group === undefined) throw new Error('the command was never signalled');
      // The force-kill returns once the group is empty or has given up, so a group still listed is a survivor.
      expect(processGroupHasMembers(group), output).toBe(false);
      ownedProcessGroups.delete(group);
    } finally {
      process.stdout.write = originalStdoutWrite;
      process.stderr.write = originalStderrWrite;
    }
  });

  it('discards the previous snapshot when a test finishes and captures the next idle interval', async () => {
    const workspace = await createWorkspace();
    const socketRoot = await ownedSocketDirectory('snapshot-progress-', 'control.sock');
    workspaces.push(socketRoot);
    const endpoint = join(socketRoot, 'control.sock');
    const connected = Promise.withResolvers<Socket>();
    const publicationHeld = Promise.withResolvers<void>();
    const sockets = new Set<Socket>();
    const server = createServer((socket) => {
      sockets.add(socket);
      socket.once('close', () => sockets.delete(socket));
      connected.resolve(socket);
    });
    await workspace.write(
      'snapshot-progress.js',
      [
        "const { connect } = require('node:net');",
        "const { createInterface } = require('node:readline');",
        `const socket = connect(${JSON.stringify(endpoint)}, () => console.log('snapshot-clock\\nsnapshot-stage:initial'));`,
        "socket.on('error', error => { throw error; });",
        "createInterface({ input: socket }).on('line', command => {",
        "  if (command === 'hold') console.log('snapshot-clock\\nsnapshot-stage:held');",
        "  else if (command === 'progress') console.log('        PASS [   0.010s] (1/1) fake tests::progress\\nsnapshot-clock\\nsnapshot-stage:progress');",
        "  else if (command === 'expire') console.log('snapshot-clock\\nsnapshot-stage:captured');",
        "  else throw new Error('unexpected snapshot command: ' + command);",
        '});',
        '',
      ].join('\n'),
    );
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject);
      server.listen(endpoint, resolve);
    });
    const signals: Array<AbortSignal | undefined> = [];
    let partial = '';
    try {
      const { result, movedMs } = await onProgressClock(
        (chunk) => {
          const lines = (partial + chunk).split('\n');
          partial = lines.pop() ?? '';
          let move = false;
          for (const line of lines) {
            if (line === 'snapshot-stage:held') publicationHeld.resolve();
            if (
              line === 'snapshot-stage:initial' ||
              line === 'snapshot-stage:progress' ||
              line === 'snapshot-stage:captured'
            )
              move = true;
          }
          return move;
        },
        HANG_CAPTURE_MS,
        () =>
          runBoundedExec(
            { command: 'node snapshot-progress.js', idleTimeoutMs: 2 * HANG_CAPTURE_MS, killAfterMs: 0 },
            workspace.context,
            createProcessTreeKiller(),
            null,
            null,
            async (request) => {
              signals.push(request.signal);
              const ordinal = signals.length;
              const socket = await connected.promise;
              if (ordinal === 1) {
                // Deliberately deliver another heartbeat while publication is held. It cannot
                // spend the next interval: only the observed PASS releases that clock step.
                socket.write('hold\n');
                await publicationHeld.promise;
                socket.write('progress\n');
              } else {
                socket.write('expire\n');
              }
              const capturedAtMs = Date.now();
              return {
                startedAtMs: capturedAtMs,
                capturedAtMs,
                offsetFromExpiryMs: request.expiresAtMs - capturedAtMs,
                outcome: 'holders',
                lockWaits: [],
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
      expect(result.terminalOutput).toContain('snapshot-stage:held');
      expect(movedMs).toBe(3 * HANG_CAPTURE_MS);
    } finally {
      publicationHeld.resolve();
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
    }
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
        ),
    );

    expect(result.success).toBe(true);
    expect(result.terminalOutput).toContain('........');
  });

  it('applies the absolute ceiling to a command that keeps talking forever', async () => {
    const workspace = await createWorkspace();
    await workspace.write('runaway.js', ["setInterval(() => console.log('still here'), 10);", ''].join('\n'));

    const result = await runBoundedExec(
      { command: 'node runaway.js', timeoutMs: 300, idleTimeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS, killAfterMs: 0 },
      workspace.context,
      createProcessTreeKiller(),
      null,
    );

    expect(result.success).toBe(false);
    expect(result.terminalOutput).toContain('Command timed out after');
    expect(result.terminalOutput).toContain('timeoutMs=300');
    expect(result.terminalOutput).not.toContain('made no progress');
  });

  it('imposes no progress bound when idleTimeoutMs is omitted', async () => {
    const workspace = await createWorkspace();
    const go = join(workspace.root, 'go.fifo');
    expect(spawnSync('mkfifo', [go]).status).toBe(0);
    // Says `started`, then goes silent until the test writes `go.fifo`.
    await workspace.write(
      'silent.js',
      [
        "const fs = require('node:fs');",
        "console.log('started');",
        "fs.readFileSync('go.fifo');",
        "console.log('late');",
        '',
      ].join('\n'),
    );
    const { promise: started, resolve: sawStarted } = Promise.withResolvers<void>();
    process.stdout.write = captureWrite([], (text) => {
      if (text.includes('started')) sawStarted();
    });
    // Every bound is armed as the command spawns, before it can say `started`. The clock then moves to just short of
    // the ceiling while the command is silent: any progress bound would have fired there, and the ceiling has not.
    // Only then is the command released, so the run ends by its own exit.
    jest.useFakeTimers();
    let result: BoundedExecResult;
    try {
      const run = runBoundedExec(
        { command: 'node silent.js', timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS },
        workspace.context,
        createProcessTreeKiller(),
        null,
      );
      await started;
      jest.advanceTimersByTime(BOUNDED_TEST_IDLE_TIMEOUT_MS - 1);
      await writeFile(go, 'go\n');
      result = await run;
    } finally {
      jest.useRealTimers();
    }

    expect(result.success).toBe(true);
    expect(result.terminalOutput).toContain('late');
    expect(result.terminalOutput).not.toContain('made no progress');
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
        'process.stdin.resume();',
        '',
      ].join('\n'),
    );
    let handlerPid: number | undefined;
    const termination = armedKiller('cooperative SIGTERM handler', async (pid) => {
      handlerPid = await armedPid(ready, pid, 'cooperative SIGTERM handler');
      expect(handlerPid, `handler readiness must name the command leader pid=${pid}`).toBe(pid);
    });

    // The ceiling is the subject; the grace after the graceful signal is the leg's backstop, so the command ends by its
    // handler alone however slowly a loaded host runs it.
    const result = await runBoundedExec(
      { command: 'exec node cooperative.js', timeoutMs: 50, killAfterMs: BOUNDED_TEST_IDLE_TIMEOUT_MS },
      workspace.context,
      termination.killer,
      null,
    );

    await termination.armed();
    // This fixture has no descendants; runBoundedExec has observed its leader's exit.
    termination.release();
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
    const calls: Array<{ pid: number; signal: NodeJS.Signals }> = [];
    const killer: ProcessTreeKiller = {
      async kill(pid, signal) {
        if (signal === 'SIGTERM') await ignoring.armed();
        calls.push({ pid, signal });
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
    );

    expect(result.success).toBe(false);
    // The graceful signal comes first and once; every signal after it is a SIGKILL, at least one, and each names the
    // one group.
    expect(calls[0]?.signal).toBe('SIGTERM');
    const forced = calls.slice(1);
    expect(forced.length).toBeGreaterThanOrEqual(1);
    expect(forced.every(({ signal }) => signal === 'SIGKILL')).toBe(true);
    expect(new Set(calls.map(({ pid }) => pid)).size).toBe(1);
  });

  it('says a graceful signal that failed, and ends the command all the same', async () => {
    if (process.platform === 'win32') {
      return;
    }

    const workspace = await createWorkspace();
    const real = createProcessTreeKiller();
    let signalled: number | undefined;
    const killer: ProcessTreeKiller = {
      async kill(pid, signal) {
        signalled = pid;
        ownedProcessGroups.add(pid);
        if (signal === 'SIGTERM') throw new Error('the signal was refused');
        await real.kill(pid, signal);
      },
    };

    // Nothing waits on the graceful pass, so the force-kill is what ends the command, and it is made although the pass
    // before it failed.
    const result = await runBoundedExec(
      { command: 'exec sleep 600', timeoutMs: 1, killAfterMs: 0 },
      workspace.context,
      killer,
      null,
    );

    if (signalled === undefined) throw new Error('the killer was never asked to signal the command');
    const group = signalled;
    expect(result.success).toBe(false);
    expect(result.terminalOutput).toContain(`Terminating process group ${group} failed: the signal was refused`);
    // The force-kill returns once the group is empty or has given up, so a group still listed is a survivor.
    expect(processGroupHasMembers(group), result.terminalOutput).toBe(false);
    ownedProcessGroups.delete(group);
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
    const result = await runBoundedExec(
      { command: 'node -e "process.stdin.resume()"', timeoutMs: 50, killAfterMs: 10 },
      workspace.context,
      killer,
      null,
    );
    // The command never exits and the killer delivers nothing, so the run returned by giving up on the tree after its
    // force-kill, not by the command ending: the command is still alive here.
    if (leaked === undefined) throw new Error('the killer was never asked to kill the command');
    const pid = leaked;
    expect(() => process.kill(pid, 0)).not.toThrow();
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
        timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
      },
      workspace.context,
      createProcessTreeKiller(),
      null,
    );
    const suppressed = await runBoundedExec(
      {
        command: 'node -e "console.log(process.argv.slice(1).join(\',\'))"',
        args: ['first'],
        __unparsed__: ['second'],
        forwardAllArgs: false,
        timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
      },
      workspace.context,
      createProcessTreeKiller(),
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
    const ready = join(workspace.root, 'descendant-ready.fifo');
    const lifetime = join(workspace.root, 'descendant-lifetime.fifo');
    expect(spawnSync('mkfifo', [ready, lifetime]).status).toBe(0);
    await workspace.write(
      'group-child.js',
      [
        "const fs = require('node:fs');",
        "fs.openSync('descendant-lifetime.fifo', 'w');",
        "fs.writeFileSync('descendant-ready.fifo', 'ready pid=' + process.pid + '\\n');",
        // Its stdin is /dev/null, so it holds itself: a wait no event ever ends, until a signal does.
        'Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);',
        '',
      ].join('\n'),
    );
    await workspace.write(
      'spawn-child.js',
      [
        "import { spawn } from 'node:child_process';",
        "spawn(process.execPath, ['group-child.js'], { stdio: 'ignore' });",
        'process.stdin.resume();',
        '',
      ].join('\n'),
    );
    // Only the descendant owns the writer and it never exits on its own: EOF observes its kill, not a time horizon.
    const closed = readToEnd(lifetime);
    let descendantPid: number | undefined;
    const termination = armedKiller('process-group descendant', async (pid) => {
      descendantPid = await armedPid(ready, pid, 'process-group descendant');
      expect(descendantPid, `ready descendant must differ from group leader pid=${pid}`).not.toBe(pid);
    });

    const result = await runBoundedExec(
      { command: 'exec node spawn-child.js', timeoutMs: 50, killAfterMs: 50 },
      workspace.context,
      termination.killer,
      null,
    );

    await termination.armed();
    const descendantOutput = await closed;
    termination.release();
    expect(descendantOutput, `ready descendant pid=${descendantPid} must close its lifetime FIFO on exit`).toBe('');
    expect(result.success).toBe(false);
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
    // `tail` inherits. The reader echoes the subshell's line before it announces readiness, so the
    // signal, which waits for that announcement, never ends the run before its output is written.
    // Every script descendant inherits the lifetime FIFO, so EOF proves all of them ended, not
    // just the ignoring child. A fork during signal delivery inherits that witness too.
    await workspace.write(
      'straggle.sh',
      [
        'exec 3>"$1"',
        '( trap "" TERM; echo ready; exec tail -f /dev/null ) | {',
        '  read -r line; echo "$line"; echo armed >"$2"; exec tail -f /dev/null',
        '}',
        '',
      ].join('\n'),
    );
    // All script descendants own a writer; the read ends only when the last one exits.
    const straggler = readToEnd(fifo);
    const termination = armedKiller('TERM-ignoring FIFO straggler', async (pid) => {
      expect(await readToEnd(ready), `straggler readiness pgid=${pid} must follow trap and FIFO open`).toBe('armed\n');
    });

    // Nothing ends the command on its own: its subshell and `tail` never exit, so the bound is the event that ends
    // the run, and the killer holds its signal until the announcement above. A short bound races no output.
    const result = await runBoundedExec(
      { command: `sh straggle.sh ${fifo} ${ready}`, timeoutMs: 200, killAfterMs: BOUNDED_TEST_IDLE_TIMEOUT_MS },
      workspace.context,
      termination.killer,
      null,
      null,
    );

    await termination.armed();
    const stragglerOutput = await straggler;
    termination.release();
    expect(result.terminalOutput).toContain('ready');
    expect(stragglerOutput).toBe('');
    expect(result.success).toBe(false);
    expect(result.terminalOutput).toContain('Force-killing what the timed-out command left in its process group');
    expect(
      [...ownedProcessGroups],
      'leader exit and straggler FIFO EOF retire cleanup authority; afterEach must not signal their stale PGID',
    ).toEqual([]);
  });

  it('force-kills a group whose members fork as the force-kill goes out, leaving none behind', async () => {
    if (process.platform === 'win32') {
      return;
    }

    const workspace = await createWorkspace();
    const lifetime = join(workspace.root, 'churn-lifetime.fifo');
    const ready = join(workspace.root, 'churn-ready.fifo');
    expect(spawnSync('mkfifo', [lifetime, ready]).status).toBe(0);
    // macOS does not hold a fork back for a group signal in flight, so a member forked as SIGKILL goes out is not
    // reached by it. The command exits on SIGTERM; its forkers and sleepers ignore it and keep forking, so the
    // force-kill is made on a group that forks as fast as it can. Every member holds the write end of `lifetime`: its
    // end of file is the death of the last one, and a force-kill that is not repeated leaves a member that only the
    // backstop ends. The bound only starts the termination, and the killer holds its first signal until the group is up,
    // so the force-kill is made while the forkers still fork.
    await workspace.write('churn.sh', churnScript({ ignoreTerm: true }));
    const reader = openForProbe(lifetime);
    const gone = readToEnd(lifetime);
    const termination = armedKiller('churn group', async (pid) => {
      expect(await readToEnd(ready), `churn readiness must name the command's own pid=${pid}`).toBe(`${pid}\n`);
    });

    const result = await runBoundedExec(
      { command: `exec sh churn.sh ${lifetime} ${ready}`, timeoutMs: 1, killAfterMs: BOUNDED_TEST_IDLE_TIMEOUT_MS },
      workspace.context,
      termination.killer,
      null,
      null,
    );

    await termination.armed();
    try {
      // The force-kill returns once the group is empty or has given up, so no member is dying: a holder of the FIFO is
      // a survivor.
      expect(writerRemains(reader), result.terminalOutput).toBe(false);
    } finally {
      closeSync(reader);
    }
    expect(await gone, result.terminalOutput).toBe('');
    termination.release();
    expect(result.success).toBe(false);
    expect(result.terminalOutput).toContain('Force-killing what the timed-out command left in its process group');
    // The graceful signal goes first and once; then only SIGKILL, at least one, and every one names the one group.
    const [first, ...forced] = termination.delivered;
    expect(first?.signal).toBe('SIGTERM');
    expect(forced.length).toBeGreaterThanOrEqual(1);
    expect(forced.every(({ signal }) => signal === 'SIGKILL')).toBe(true);
    expect(new Set(termination.delivered.map(({ pid }) => pid)).size).toBe(1);
  });

  it('runs the command with its RAM lease as TMPDIR, names held dead leases, and ends the lease after it exits', async () => {
    const workspace = await createWorkspace();
    const directory = join(workspace.root, 'lease');
    await mkdir(directory);
    const held = '/v/9-abcdef is kept: /v/9-abcdef/x.dmg (/dev/disk9) still attached from below it';
    const retained =
      '/v/8-ghijkl archived at /v/.archive/0001792505574623-8-ghijkl: kept until 2 newer archives replace it';
    const volume = scriptedVolume({
      ok: true,
      value: { kind: 'leased', lease: leaseAt(directory), held: [held], reaped: [], gaps: [], retained: [retained] },
    });

    const result = await runBoundedExec(
      { command: 'node -e "console.log(process.env.TMPDIR)"', timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS },
      workspace.context,
      createProcessTreeKiller(),
      volume,
      null,
      undefined,
      await standInOwner(workspace.root),
    );

    expect(result.success).toBe(true);
    expect(result.terminalOutput).toContain(directory);
    expect(result.terminalOutput).toContain(`RAM temp volume: dead lease ${held}\n`);
    expect(result.terminalOutput).toContain(`RAM temp volume: dead lease ${retained}\n`);
    expect(volume.released).toEqual([directory]);
    // A passing run's scratch is not evidence anyone needs: the lease is released to be deleted. The handle's record
    // lands whenever the admission does, which no output line orders.
    expect(volume.order.filter((step) => !step.startsWith('record '))).toEqual([`release ${directory}`]);
  });

  it("releases a failed run's lease to be archived, and says where its scratch went", async () => {
    const workspace = await createWorkspace();
    const directory = join(workspace.root, 'lease');
    await mkdir(directory);
    const volume = scriptedVolume({
      ok: true,
      value: { kind: 'leased', lease: leaseAt(directory), held: [], reaped: [], gaps: [], retained: [] },
    });

    const result = await runBoundedExec(
      { command: 'node -e "process.exit(3)"', timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS },
      workspace.context,
      createProcessTreeKiller(),
      volume,
      null,
      undefined,
      await standInOwner(workspace.root),
    );

    expect(result.success).toBe(false);
    expect(volume.order.filter((step) => !step.startsWith('record '))).toEqual([`release ${directory} (kept)`]);
    expect(result.terminalOutput).toContain(`RAM temp volume: lease ${directory} archived by the scripted volume\n`);
  });

  it("retires what the command left running through its supervisor, names a dead task's retired subtree, then ends the lease", async () => {
    const workspace = await createWorkspace();
    const directory = join(workspace.root, 'lease');
    await mkdir(directory);
    const volume = scriptedVolume({
      ok: true,
      value: {
        kind: 'leased',
        lease: leaseAt(directory),
        held: [],
        reaped: ['4242 (nx)'],
        gaps: ['/v/9-abcdef: unobservedExit (pid 4243)'],
        retained: [],
      },
    });
    const leftover = join(workspace.root, 'leftover.pid');

    const result = await runBoundedExec(
      { command: `sleep 600 & echo $! > ${leftover}`, timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS },
      workspace.context,
      createProcessTreeKiller(),
      volume,
      null,
      undefined,
      await standInOwner(workspace.root),
    );

    const pid = Number(await readFile(leftover, 'utf8'));
    expect(result.success, result.terminalOutput).toBe(true);
    expect(result.terminalOutput).toContain(
      `cowshed: the supervisor stopped ${pid} (sleep), which the command left running\n`,
    );
    expect(await live([pid])).toEqual([]);
    expect(result.terminalOutput).toContain(
      "RAM temp volume: the supervisor stopped 4242 (nx), which a dead task's command left running\n",
    );
    expect(result.terminalOutput).toContain(
      "RAM temp volume: the supervisor's view of a dead task's command has a gap: /v/9-abcdef: unobservedExit (pid 4243)\n",
    );
    // The handle is recorded beside the lease once the supervisor admits the command, and the lease ends only after
    // the subtree it names was retired.
    expect(volume.order).toHaveLength(2);
    expect(volume.order[0]?.startsWith(`record ${directory} {"jobId":1,"index":`)).toBe(true);
    expect(volume.order[1]).toBe(`release ${directory}`);
  });

  it("retires only the command's own subtree: a bystander working in its directory survives", async () => {
    const workspace = await createWorkspace();
    const leftover = join(workspace.root, 'leftover.pid');
    // No task started it; it only works where the command works, as an editor, an indexer or another job's process can.
    const bystander = spawn('sleep', ['600'], { cwd: workspace.root, detached: true, stdio: 'ignore' });
    const bystanderExited = once(bystander, 'exit');
    try {
      const bystanderPid = bystander.pid ?? 0;

      const result = await runBoundedExec(
        { command: `sleep 600 & echo $! > ${leftover}`, timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS },
        workspace.context,
        createProcessTreeKiller(),
        null,
        null,
        undefined,
        await standInOwner(workspace.root),
      );

      // After: the supervisor ends exactly what its tree records under the command, and the bystander is not in it.
      const pid = Number(await readFile(leftover, 'utf8'));
      expect(result.success, result.terminalOutput).toBe(true);
      expect(result.terminalOutput).toContain(
        `cowshed: the supervisor stopped ${pid} (sleep), which the command left running\n`,
      );
      expect(await live([pid])).toEqual([]);
      expect(await live([bystanderPid])).toEqual([bystanderPid]);
    } finally {
      bystander.kill('SIGKILL');
      await bystanderExited;
    }
  });

  it('takes no RAM lease without a supervisor to own what the command leaves, and says which path ran', async () => {
    const workspace = await createWorkspace();
    const directory = join(workspace.root, 'lease');
    const volume = scriptedVolume({
      ok: true,
      value: { kind: 'leased', lease: leaseAt(directory), held: [], reaped: [], gaps: [], retained: [] },
    });

    const result = await runBoundedExec(
      { command: `node -e 'console.log("tmp=" + process.env.TMPDIR)'`, timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS },
      workspace.context,
      createProcessTreeKiller(),
      volume,
    );

    expect(result.success).toBe(true);
    expect(result.terminalOutput).toContain(
      "RAM temp volume not leased: no cowshed supervisor owns this command's processes (COWSHED_SUPERVISOR_SOCKET is unset)",
    );
    expect(result.terminalOutput).not.toContain(`tmp=${directory}`);
    expect(volume.order).toEqual([]);
  });

  it('runs a command the supervisor refused to admit, unowned, and says nothing will retire what it leaves', async () => {
    const workspace = await createWorkspace();
    const directory = join(workspace.root, 'lease');
    await mkdir(directory);
    const volume = scriptedVolume({
      ok: true,
      value: { kind: 'leased', lease: leaseAt(directory), held: [], reaped: [], gaps: [], retained: [] },
    });
    const marker = join(workspace.root, 'ran');

    const result = await runBoundedExec(
      { command: `touch ${marker}`, timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS },
      workspace.context,
      createProcessTreeKiller(),
      volume,
      null,
      undefined,
      await standInOwner(workspace.root, { admission: 'notMember', retire: 'retire' }),
    );

    expect(result.success).toBe(true);
    expect(await exists(marker)).toBe(true);
    expect(result.terminalOutput).toContain(
      'cowshed: the command ran without an owned subtree (supervisor: notMember); nothing retires what it left running\n',
    );
    expect(volume.order).toEqual([`release ${directory}`]);
  });

  it('fails a run whose leftovers the supervisor client could not retire, and names why', async () => {
    const workspace = await createWorkspace();

    const result = await runBoundedExec(
      { command: 'node -e 0', timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS },
      workspace.context,
      createProcessTreeKiller(),
      null,
      null,
      undefined,
      await standInOwner(workspace.root, { admission: 'admitted', retire: 'fail' }),
    );

    expect(result.success).toBe(false);
    expect(result.terminalOutput).toContain("cowshed: could not retire the command's processes: ");
    expect(result.terminalOutput).toContain('exited 1: stand-in: the supervisor socket is unreachable\n');
  });

  for (const unavailable of [
    { kind: 'unavailable', reason: 'unproven-owner', detail: 'the legacy marker does not prove gateway ownership' },
    { kind: 'unavailable', reason: 'gateway-unsupported', detail: 'the gateway does not serve ram-lease' },
    { kind: 'unavailable', reason: 'gateway-absent', detail: 'the owned gateway fixture socket is absent' },
  ] satisfies Extract<RamTempAcquisition, { kind: 'unavailable' }>[]) {
    for (const status of [0, 7]) {
      it(`uses owned ordinary scratch for ${unavailable.reason}, preserving command status ${status}`, async () => {
        await withOrdinaryTempWorkspace(async (workspace, ordinary) => {
          const volume = scriptedVolume({ ok: true, value: unavailable }, null, null, ordinary);
          const owner = await standInOwner(workspace.root);
          const evidenceAtRetirement: string[] = [];
          const supervised: SubtreeOwner = {
            rootArgv: (command) => owner.rootArgv(command),
            async retire(handle, graceMs) {
              const outcome = await owner.retire(handle, graceMs);
              const directory = await readFile(join(workspace.root, 'observed-tmpdir'), 'utf8');
              evidenceAtRetirement.push(await readFile(join(directory, 'command-evidence'), 'utf8'));
              return outcome;
            },
          };
          const record = { task: { id: 'optional:command', hash: null }, directory: join(workspace.root, 'verdict') };
          const result = await runBoundedExec(
            {
              command: 'node temp-probe.js',
              env: { PROBE_EXIT_CODE: String(status) },
              timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
            },
            { ...workspace.context, record },
            createProcessTreeKiller(),
            volume,
            null,
            undefined,
            supervised,
          );
          const directory = await readFile(join(workspace.root, 'observed-tmpdir'), 'utf8');
          expect(result.success, result.terminalOutput).toBe(status === 0);
          expect(dirname(directory)).toBe(ordinary);
          expect(enclosingTreeMarker(directory)).toBeNull();
          expect(directory).not.toStartWith(await realpath(join(workspace.root, 'ram3')));
          expect(result.terminalOutput).toContain(`actual-tmpdir=${directory}\n`);
          expect(result.terminalOutput).toContain(
            `RAM temp volume unavailable (${unavailable.reason}: ${unavailable.detail}); using owned ordinary TMPDIR ${directory}\n`,
          );
          expect(result.terminalOutput.match(/RAM temp volume unavailable/g)).toHaveLength(1);
          expect(result.terminalOutput).not.toContain('RAM temp volume: lease');
          expect(evidenceAtRetirement).toEqual(['command evidence\n']);
          expect(volume.order).toEqual([]);
          expect(volume.released).toEqual([]);
          expect((await readRecord(record.directory))?.verdict).toEqual(
            status === 0 ? { outcome: 'passed' } : { outcome: 'failed', exitCode: status, tests: [] },
          );
          if (status === 0) {
            expect(await exists(directory)).toBe(false);
            expect(result.terminalOutput).not.toContain('Ordinary temp directory retained');
          } else {
            expect(await readFile(join(directory, 'command-evidence'), 'utf8')).toBe('command evidence\n');
            expect(result.terminalOutput).toContain('Command exited with status 7:');
            expect(result.terminalOutput).toContain(
              `Ordinary temp directory retained at ${directory}: the run did not pass\n`,
            );
          }
        });
      });
    }
  }

  it("makes the user's ordinary scratch beside its RAM lock, outside every Git work tree and Cargo workspace", () => {
    const volume = new RamTempVolume(
      ramTempPaths(userInfo(), PRODUCTION),
      RAM_TEMP_CAPACITY_BYTES,
      createHostCommands(GATEWAY_SOCKET),
    );
    expect(volume.ordinaryParent).toBe('/private/tmp');
    expect(enclosingTreeMarker(volume.ordinaryParent)).toBeNull();
  });

  it('keeps an explicit target TMPDIR when optional RAM is unavailable without claiming its medium or cleaning it', async () => {
    await withOrdinaryTempWorkspace(async (workspace, ordinary) => {
      const target = join(workspace.root, 'target-temp');
      await mkdir(target);
      const volume = scriptedVolume(
        {
          ok: true,
          value: { kind: 'unavailable', reason: 'unproven-owner', detail: 'legacy marker' },
        },
        null,
        null,
        ordinary,
      );
      const result = await runBoundedExec(
        { command: 'node temp-probe.js', env: { TMPDIR: target }, timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS },
        workspace.context,
        createProcessTreeKiller(),
        volume,
        null,
        undefined,
        await standInOwner(workspace.root),
      );
      expect(result.success, result.terminalOutput).toBe(true);
      expect(await readFile(join(workspace.root, 'observed-tmpdir'), 'utf8')).toBe(target);
      expect(await readFile(join(target, 'command-evidence'), 'utf8')).toBe('command evidence\n');
      expect(await readdir(ordinary)).toEqual([]);
      expect(result.terminalOutput).toContain(`using target-provided TMPDIR ${target} (medium not inferred)\n`);
      expect(result.terminalOutput.match(/RAM temp volume unavailable/g)).toHaveLength(1);
      expect(result.terminalOutput).not.toContain('using owned ordinary TMPDIR');
      expect(volume.order).toEqual([]);
    });
  });

  for (const mode of ['admission-refused', 'retire-refused', 'retire-failed']) {
    it(`retains ordinary scratch when the command subtree is not retired: ${mode}`, async () => {
      await withOrdinaryTempWorkspace(async (workspace, ordinary) => {
        const volume = scriptedVolume(
          {
            ok: true,
            value: { kind: 'unavailable', reason: 'gateway-absent', detail: 'owned socket absent' },
          },
          null,
          null,
          ordinary,
        );
        const owner = await standInOwner(workspace.root, {
          admission: mode === 'admission-refused' ? 'notMember' : 'admitted',
          retire: mode === 'retire-failed' ? 'fail' : 'retire',
        });
        const supervised: SubtreeOwner =
          mode === 'retire-refused'
            ? {
                rootArgv: (command) => owner.rootArgv(command),
                retire: async () => ({ kind: 'refused', reason: 'jobEnded' }),
              }
            : owner;
        const result = await runBoundedExec(
          { command: 'node temp-probe.js', timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS },
          workspace.context,
          createProcessTreeKiller(),
          volume,
          null,
          undefined,
          supervised,
        );
        const directory = await readFile(join(workspace.root, 'observed-tmpdir'), 'utf8');
        expect(result.success, result.terminalOutput).toBe(mode !== 'retire-failed');
        expect(await readFile(join(directory, 'command-evidence'), 'utf8')).toBe('command evidence\n');
        expect(result.terminalOutput).toContain(
          `Ordinary temp directory retained at ${directory}: the command subtree was not retired\n`,
        );
        expect(volume.order).toEqual([]);
      });
    });
  }

  it('refuses a missing ordinary scratch parent rather than using inherited RAM or another directory', async () => {
    await withOrdinaryTempWorkspace(async (workspace, ordinary) => {
      const missing = join(ordinary, 'missing');
      const volume = scriptedVolume(
        { ok: true, value: { kind: 'unavailable', reason: 'gateway-unsupported', detail: 'no ram-lease' } },
        null,
        null,
        missing,
      );
      const result = await runBoundedExec(
        { command: 'node temp-probe.js', timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS },
        workspace.context,
        createProcessTreeKiller(),
        volume,
        null,
        undefined,
        await standInOwner(workspace.root),
      );
      expect(result.success).toBe(false);
      expect(result.terminalOutput).toContain('gateway-unsupported: no ram-lease');
      expect(result.terminalOutput).toContain(`could not create ordinary scratch under ${missing}:`);
      expect(await exists(join(workspace.root, 'observed-tmpdir'))).toBe(false);
      expect(await readdir(ordinary)).toEqual([]);
      expect(volume.order).toEqual([]);
    });
  });

  it('keeps the inherited TMPDIR inside a sandbox and says so once', async () => {
    const workspace = await createWorkspace();
    const volume = scriptedVolume({ ok: true, value: { kind: 'sandboxed', detail: 'lock: EPERM' } });

    const result = await runBoundedExec(
      {
        command: `node -e 'console.log("tmp=" + process.env.TMPDIR)'`,
        timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
        env: { TMPDIR: '/shed/tmp' },
      },
      workspace.context,
      createProcessTreeKiller(),
      volume,
      null,
      undefined,
      await standInOwner(workspace.root),
    );

    expect(result.success).toBe(true);
    expect(result.terminalOutput).toContain('tmp=/shed/tmp');
    expect(result.terminalOutput.match(/RAM temp volume unavailable in this sandbox \(lock: EPERM\)/g)).toHaveLength(1);
    expect(volume.released).toEqual([]);
  });

  for (const error of [
    { kind: 'provision-failed', step: 'hdiutil attach', detail: 'exit 1: Device not configured' },
    { kind: 'provision-failed', step: 'read marker', detail: 'EIO' },
    { kind: 'provision-failed', step: 'read marker', detail: 'malformed gateway provenance' },
    { kind: 'provision-failed', step: 'read ledger', detail: 'corrupt ledger' },
    { kind: 'provision-failed', step: 'pending detach', detail: 'unknown running custody' },
  ] satisfies RamTempError[]) {
    it(`does not run the command or fall back on a hard RAM error: ${error.detail}`, async () => {
      const workspace = await createWorkspace();
      const marker = join(workspace.root, 'ran');
      const volume = scriptedVolume({ ok: false, error });
      const result = await runBoundedExec(
        { command: `touch ${marker}`, timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS },
        workspace.context,
        createProcessTreeKiller(),
        volume,
        null,
        undefined,
        await standInOwner(workspace.root),
      );
      expect(result.success).toBe(false);
      expect(result.terminalOutput).toBe(`${describeRamTempError(error)}\n`);
      expect(await exists(marker)).toBe(false);
      expect(volume.order).toEqual([]);
    });
  }

  it('names the full volume when a failed command left it without space', async () => {
    const workspace = await createWorkspace();
    const directory = join(workspace.root, 'lease');
    await mkdir(directory);
    const volume = scriptedVolume(
      { ok: true, value: { kind: 'leased', lease: leaseAt(directory), held: [], reaped: [], gaps: [], retained: [] } },
      { kind: 'volume-full', mountpoint: workspace.root, capacityBytes: 1024 * 1024 * 1024, freeBytes: 1024 * 1024 },
    );

    const result = await runBoundedExec(
      { command: 'node -e "process.exit(1)"', timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS },
      workspace.context,
      createProcessTreeKiller(),
      volume,
      null,
      undefined,
      await standInOwner(workspace.root),
    );

    expect(result.success).toBe(false);
    expect(result.terminalOutput).toContain(`RAM temp volume ${workspace.root} is full: 1 MiB free of 1024 MiB`);
    expect(volume.released).toEqual([directory]);
  });

  it('names ENOSPC, its writer and the volume as it stood when the output said so, not after the command exited', async () => {
    // From a downstream land gate: a compiler test shard printed ENOSPC for a 1 KB write into its lease and failed two.
    // By its exit the volume had space again (`fullness` answers null here), so a probe after exit named nothing and
    // the run read as two failed tests.
    const workspace = await createWorkspace();
    const directory = join(workspace.root, 'lease');
    await mkdir(directory);
    const lease = leaseAt(directory);
    const space = {
      kind: 'measured',
      lease: directory,
      mountpoint: workspace.root,
      capacityBytes: 1024 * 1024 * 1024,
      freeBytes: 4096,
    } as const;
    const volume = scriptedVolume(
      { ok: true, value: { kind: 'leased', lease, held: [], reaped: [], gaps: [], retained: [] } },
      null,
      space,
    );
    const record = { task: { id: 'compiler:test-10', hash: 'h' }, directory: join(workspace.root, 'record') };
    const line = 'ENOSPC: no space left on device, write';
    // The command's ENOSPC line is this test's fixture. On the suite's own output the bounded leg that runs the suite
    // reads it as its own disk filling: a land gate recorded nx-plugin:test-shard1 as no-space from this line.
    const stdout: string[] = [];
    const stderr: string[] = [];
    process.stdout.write = captureWrite(stdout);
    process.stderr.write = captureWrite(stderr);

    const result = await runBoundedExec(
      { command: `node -e "console.error('${line}'); process.exit(1)"`, timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS },
      { ...workspace.context, record },
      createProcessTreeKiller(),
      volume,
      null,
      undefined,
      await standInOwner(workspace.root),
    );

    expect(result.success).toBe(false);
    expect(result.terminalOutput).toContain(
      `compiler:test-10: ENOSPC (no space left on device): the command printed "${line}"; its TMPDIR ` +
        `${directory} is on the RAM temp volume ${workspace.root}, which had 0.0 MiB free of 1024.0 MiB when that ` +
        'line arrived\n',
    );
    expect(result.terminalOutput).not.toContain('is full:');
    expect([...stdout, ...stderr].join('')).toContain(line);
    expect(await readRecord(record.directory)).toEqual({
      task: record.task.id,
      hash: 'h',
      verdict: { outcome: 'no-space', exitCode: 1, tests: [], line, volume: space },
    });
    // Read while the lease was still the task's, before its subtree was retired and the lease released to be archived.
    // The handle's record lands whenever the admission does, which no output line orders.
    expect(volume.order.filter((step) => !step.startsWith('record '))).toEqual([
      'space',
      `release ${directory} (kept)`,
    ]);
  });

  it("names the run's own jobserver fifo to its command, never one the target's env names", async () => {
    const workspace = await createWorkspace();
    const injected = '-j --jobserver-auth=fifo:/injected';
    const command = `node -e "console.log(JSON.stringify([process.env.CARGO_MAKEFLAGS, process.env.MAKEFLAGS, process.env.${CPU_POOL_ENV}]))"`;
    const result = await withPool(2, (pool) =>
      runBoundedExec(
        {
          command,
          timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
          env: { CARGO_MAKEFLAGS: injected, MAKEFLAGS: injected, MFLAGS: injected },
        },
        workspace.context,
        createProcessTreeKiller(),
        null,
        inheritedFrom(pool),
      ),
    );
    expect(result.success).toBe(true);
    for (const name of ['CARGO_MAKEFLAGS', 'MAKEFLAGS', 'MFLAGS']) {
      expect(result.terminalOutput).toContain(
        `cowshed: ${name} in the target's env is ignored: the CPU budget sets it`,
      );
    }
    // Cargo and make both read the fifo beside the sub-pool's socket that this run serves its descendants.
    expect(result.terminalOutput).toMatch(
      /^\["-j --jobserver-auth=fifo:(\/\S+)\/j","-j --jobserver-auth=fifo:\1\/j","\1\/p\.sock"\]$/m,
    );
    expect(result.terminalOutput).not.toContain('/injected');
  });

  it('says nothing and runs as asked when it inherits no pool', async () => {
    const workspace = await createWorkspace();

    const result = await runBoundedExec(
      { command: 'node -e "console.log(\'ran\')"', timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS },
      workspace.context,
      createProcessTreeKiller(),
      null,
    );

    expect(result.success).toBe(true);
    expect(result.terminalOutput).toBe('ran\n');
  });

  it('resolves the cowshed a run uses, in full precedence order', () => {
    const root = '/workspace';
    // The executor never chdirs, so an empty PATH component names its actual spawn cwd,
    // deliberately not the workspace root: the two directories differ on purpose here.
    const cwd = '/elsewhere';
    // Every level present at once, removed one at a time from the top: each step must fall
    // to exactly the next level, so the order itself is the assertion, not each level alone.
    const everywhere = new Set([
      '/opt/bin/cowshed',
      '/elsewhere/cowshed',
      '/workspace/packages/cowshed/bin/cowshed',
      '/workspace/node_modules/@smoothbricks/cowshed/bin/cowshed',
    ]);
    const present = (path: string) => everywhere.has(path);
    const select = (named?: string, inherited?: string, pathValue?: string) =>
      resolveCowshedExecutable(root, named, inherited, pathValue, cwd, present);

    // 1. A target-named executable wins, made absolute against the workspace root.
    expect(select('bin/cowshed', '/valid/bin/cowshed', '/opt/bin')).toBe('/workspace/bin/cowshed');
    // 2. An explicit inherited executable outranks PATH and the workspace launcher — the
    //    precedence createHostCommands always had — and keeps the coordinate the parent
    //    gave it: absolute verbatim, relative untouched for spawn's own resolution,
    //    never retargeted to the workspace root.
    expect(select(undefined, '/valid/bin/cowshed', '/opt/bin')).toBe('/valid/bin/cowshed');
    expect(select(undefined, 'bin/cowshed', '/opt/bin')).toBe('bin/cowshed');
    // 3. An installed cowshed on PATH wins (undefined: the bare name spawn already finds it).
    expect(select(undefined, undefined, '/opt/bin')).toBeUndefined();
    // 3b. An empty PATH component (the leading ':' below) names the current directory, as
    //     POSIX spawn resolution does — here the executor's cwd, not the workspace root:
    //     a cowshed in the cwd is installed too, and one merely in the root is not.
    expect(select(undefined, undefined, ':/usr/bin')).toBeUndefined();
    everywhere.delete('/elsewhere/cowshed');
    everywhere.add('/workspace/cowshed');
    expect(select(undefined, undefined, ':/usr/bin')).toBe('/workspace/packages/cowshed/bin/cowshed');
    everywhere.delete('/workspace/cowshed');
    // 4. Only when PATH provides none does the workspace's own launcher answer.
    expect(select(undefined, undefined, '/usr/bin')).toBe('/workspace/packages/cowshed/bin/cowshed');
    // 5. A linked workspace's launcher answers when the workspace has none of its own.
    everywhere.delete('/workspace/packages/cowshed/bin/cowshed');
    expect(select(undefined, undefined, '/usr/bin')).toBe('/workspace/node_modules/@smoothbricks/cowshed/bin/cowshed');
    // 6. No cowshed anywhere keeps the bare name, so the spawn failure names the missing tool.
    everywhere.clear();
    expect(select(undefined, undefined, '/usr/bin')).toBeUndefined();
    expect(select(undefined, undefined, undefined)).toBeUndefined();
  });

  it('sizes a runner to the grant its parent pool lent', async () => {
    const workspace = await createWorkspace();
    const lent = lentCount(nextestWant, 3);

    const result = await withPool(3, (pool) =>
      runBoundedExec(
        {
          command: 'node -e "console.log(\'tokens=\' + process.env.BOUNDED_EXEC_CPU_TOKENS)" # nextest run',
          timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
          parallelism: 8,
        },
        workspace.context,
        createProcessTreeKiller(),
        null,
        inheritedFrom(pool),
      ),
    );

    expect(result.success).toBe(true);
    expect(result.terminalOutput).toContain(`tokens=${lent}\n`);
    expect(result.terminalOutput).toMatch(
      new RegExp(
        `^cowshed: cpu-pool wait done elapsed=\\d+ms status=ok tokens=${lent}/${nextestWant} runner=nextest$`,
        'm',
      ),
    );
  });

  it('lends a runner no more than it asks for', async () => {
    const workspace = await createWorkspace();
    const lent = lentCount(2, 16);

    const result = await withPool(16, (pool) =>
      runBoundedExec(
        {
          command: 'node -e "console.log(\'threads=\' + process.env.NEXTEST_TEST_THREADS)" # nextest run',
          timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
          parallelism: 2,
        },
        workspace.context,
        createProcessTreeKiller(),
        null,
        inheritedFrom(pool),
      ),
    );

    expect(result.success).toBe(true);
    expect(result.terminalOutput).toContain(`threads=${lent}\n`);
    expect(result.terminalOutput).toContain(`status=ok tokens=${lent}/2 runner=nextest`);
  });

  it('records the grant its parent pool lent', async () => {
    const workspace = await createWorkspace();
    const directory = join(workspace.root, 'borrowed');
    const lent = lentCount(nextestWant, 3);

    const result = await withPool(3, (pool) =>
      runBoundedExec(
        { command: 'node -e "0" # nextest run', timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS, parallelism: 8 },
        { ...workspace.context, record: { task: { id: 'app:test', hash: null }, directory } },
        createProcessTreeKiller(),
        null,
        inheritedFrom(pool),
      ),
    );

    expect(result.success).toBe(true);
    const record: unknown = JSON.parse(await readFile(join(directory, VERDICT_FILE), 'utf8'));
    expect(record).toHaveProperty('cpu', { tokens: lent, want: nextestWant, waitedMs: expect.any(Number) });
  });

  it('keeps a command running when its borrowed grant closes, and judges it by its own exit', async () => {
    if (process.platform === 'win32') return;
    const workspace = await createWorkspace();
    const directory = join(workspace.root, 'outlives-its-grant');
    const started = join(workspace.root, 'started.fifo');
    const go = join(workspace.root, 'go.fifo');
    expect(spawnSync('mkfifo', [started, go]).status).toBe(0);
    // The run says the grant ended on its stderr; that line is the signal the close was heard while the command runs.
    const { promise: heard, resolve: hear } = Promise.withResolvers<void>();
    process.stderr.write = captureWrite([], (text) => {
      if (text.includes('cpu-pool grant ended')) hear();
    });
    const result = await withPool(1, async (pool) => {
      const running = runBoundedExec(
        {
          command: `sh -c 'echo started > "$0"; read -r line < "$1"' '${started}' '${go}'`,
          timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
        },
        { ...workspace.context, record: { task: { id: 'outlives-its-grant', hash: null }, directory } },
        createProcessTreeKiller(),
        null,
        inheritedFrom(pool),
      );
      // The command has started and waits on `go` while its borrowed grant closes under it.
      expect(await readToEnd(started)).toBe('started\n');
      const closed = await pool.close();
      if (!closed.ok) throw new Error(closed.error);
      await heard;
      await writeFile(go, 'go\n');
      return running;
    });
    expect(result.terminalOutput).toContain(
      'cowshed: cpu-pool grant ended: the parent pool closed its 1-token grant while the command ran; it runs on over budget until it ends',
    );
    expect(result.terminalOutput).not.toContain('Force-killing');
    expect(result.success).toBe(true);
    const record: unknown = JSON.parse(await readFile(join(directory, VERDICT_FILE), 'utf8'));
    expect(record).toHaveProperty('verdict.outcome', 'passed');
  });

  it('runs the command as asked, and says so, when its parent pool is gone', async () => {
    const workspace = await createWorkspace();
    const gone = { socket: join(workspace.root, 'gone.sock'), tokens: 3 };

    const result = await runBoundedExec(
      {
        command: 'node -e "console.log(\'ran\')" # nextest run',
        timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
        parallelism: 8,
      },
      workspace.context,
      createProcessTreeKiller(),
      null,
      gone,
    );

    expect(result.success).toBe(true);
    expect(result.terminalOutput).toContain('ran\n');
    expect(result.terminalOutput).toContain('cowshed: cpu-pool parent pool absent: ');
    expect(result.terminalOutput).toContain('; running the command as asked');
    expect(result.terminalOutput).not.toContain('status=ok');
  });

  it('ignores a target env that names the CPU budget, when no budget grants it', async () => {
    const workspace = await createWorkspace();

    const result = await runBoundedExec(
      {
        command: 'node -e "console.log(\'tokens=\' + process.env.BOUNDED_EXEC_CPU_TOKENS)"',
        timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
        env: { BOUNDED_EXEC_CPU_TOKENS: '977' },
      },
      workspace.context,
      createProcessTreeKiller(),
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

    const result = await withPool(2, (pool) =>
      runBoundedExec(
        {
          command: 'node -e "console.log(\'tokens=\' + process.env.BOUNDED_EXEC_CPU_TOKENS)" # nextest run',
          timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
          parallelism: 8,
          env: { BOUNDED_EXEC_CPU_TOKENS: '977' },
        },
        workspace.context,
        createProcessTreeKiller(),
        null,
        inheritedFrom(pool),
      ),
    );

    expect(result.success).toBe(true);
    expect(result.terminalOutput).toContain('tokens=2\n');
    expect(result.terminalOutput).not.toContain('977');
  });

  it('refuses the run when its parent pool answers with a refusal', async () => {
    const socketDirectory = await ownedSocketDirectory('cpu-refuse-', 'p.sock');
    workspaces.push(socketDirectory);
    const socket = join(socketDirectory, 'p.sock');
    // A live pool that refuses answers `other`, not `absent`: the run must fail, not run beside it.
    const refusing = createServer((connection) => {
      connection.once('data', () => {
        connection.end('{"ok":false,"code":"refused","error":"the pool is shutting down"}\n');
      });
    });
    await new Promise<void>((resolve) => refusing.listen(socket, resolve));
    const workspace = await createWorkspace();
    try {
      const result = await runBoundedExec(
        { command: 'touch ran.marker', timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS },
        {
          ...workspace.context,
          record: { task: { id: 'refused', hash: null }, directory: join(workspace.root, 'refused') },
        },
        createProcessTreeKiller(),
        null,
        { socket, tokens: 3 },
      );

      expect(result.success).toBe(false);
      expect(await exists(join(workspace.root, 'ran.marker'))).toBe(false);
      expect(result.terminalOutput).toContain('cowshed: cpu-pool parent pool refused: ');
      expect(await readFile(join(workspace.root, 'refused', VERDICT_FILE), 'utf8')).toMatch(/"outcome":\s*"failed"/);
    } finally {
      await new Promise<void>((resolve) => refusing.close(() => resolve()));
    }
  });

  describe('a leg that runs bun', () => {
    const declaresBun = JSON.stringify({ name: 'workspace', packageManager: 'bun@1.4.3' });
    const systemPath = process.env.PATH === undefined ? '' : `:${process.env.PATH}`;

    it('is refused before it asks for RAM or CPU when its first bun is another version, and records why', async () => {
      const workspace = await createWorkspace();
      await workspace.write('package.json', declaresBun);
      const bun = await fakeBun(join(workspace.root, 'bin'), '1.4.2');
      const volume = scriptedVolume({
        ok: true,
        value: {
          kind: 'leased',
          lease: leaseAt(join(workspace.root, 'lease')),
          held: [],
          reaped: [],
          gaps: [],
          retained: [],
        },
      });
      const record = { task: { id: 'compiler:test-3', hash: 'h' }, directory: join(workspace.root, 'record') };

      const result = await runBoundedExec(
        {
          command: 'bun run.js',
          env: { PATH: join(workspace.root, 'bin') + systemPath },
          timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
        },
        { ...workspace.context, record },
        createProcessTreeKiller(),
        volume,
      );

      expect(result.success).toBe(false);
      expect(result.terminalOutput).toContain('compiler:test-3: refusing to run the command');
      expect(result.terminalOutput).toContain('declares Bun 1.4.3');
      expect(result.terminalOutput).toContain('is Bun 1.4.2');
      expect(result.terminalOutput).toContain(bun);
      expect(result.terminalOutput).toContain("PATH from the target's env");
      // Nothing was asked of the host: no RAM lease, and the command never ran.
      expect(volume.order).toEqual([]);
      expect(await exists(join(workspace.root, 'bin', FAKE_BUN_RAN))).toBe(false);
      expect(await readRecord(record.directory)).toEqual({
        task: record.task.id,
        hash: 'h',
        verdict: { outcome: 'failed', exitCode: 1, tests: [] },
      });
    });

    it('runs on the declared version', async () => {
      const workspace = await createWorkspace();
      await workspace.write('package.json', declaresBun);
      await fakeBun(join(workspace.root, 'bin'), '1.4.3');

      const result = await runBoundedExec(
        {
          command: 'bun run.js',
          env: { PATH: join(workspace.root, 'bin') + systemPath },
          timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
        },
        workspace.context,
        createProcessTreeKiller(),
        null,
      );

      expect(result.success).toBe(true);
      expect(await exists(join(workspace.root, 'bin', FAKE_BUN_RAN))).toBe(true);
    });

    it('is not asked about a command that runs no bun, whatever bun is on PATH', async () => {
      const workspace = await createWorkspace();
      await workspace.write('package.json', declaresBun);
      await fakeBun(join(workspace.root, 'bin'), '1.4.2');

      const result = await runBoundedExec(
        {
          command: 'touch ran.marker',
          env: { PATH: join(workspace.root, 'bin') + systemPath },
          timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
        },
        workspace.context,
        createProcessTreeKiller(),
        null,
      );

      expect(result.success).toBe(true);
      expect(await exists(join(workspace.root, 'ran.marker'))).toBe(true);
      expect(await exists(join(workspace.root, 'bin', FAKE_BUN_INVOKED))).toBe(false);
    });

    it('names the inherited environment when the target sets no PATH of its own', async () => {
      const workspace = await createWorkspace();
      await workspace.write('package.json', declaresBun);
      await fakeBun(join(workspace.root, 'bin'), '1.4.2');
      const inherited = process.env.PATH;
      process.env.PATH = join(workspace.root, 'bin') + systemPath;

      try {
        const result = await runBoundedExec(
          { command: 'bun run.js', timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS },
          workspace.context,
          createProcessTreeKiller(),
          null,
        );

        expect(result.success).toBe(false);
        expect(result.terminalOutput).toContain('PATH from the inherited environment');
      } finally {
        if (inherited === undefined) delete process.env.PATH;
        else process.env.PATH = inherited;
      }
    });
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
      const unbudgeted = await withPool(3, (pool) =>
        runBoundedExec(
          {
            command: 'node probe.js',
            timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
            env: { ...pool.environment, [CPU_TOKENS_ENV]: '9' },
          },
          workspace.context,
          createProcessTreeKiller(),
          null,
        ),
      );
      expect(unbudgeted.terminalOutput).toContain('[null,null]\n[null,null]\n');

      let injected = '';
      const granted = await withPool(2, (lending) =>
        withPool(3, (pool) => {
          injected = inheritedFrom(pool).socket;
          return runBoundedExec(
            {
              command: 'node probe.js',
              timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
              parallelism: 2,
              env: { ...pool.environment, [CPU_TOKENS_ENV]: '9' },
            },
            workspace.context,
            createProcessTreeKiller(),
            null,
            inheritedFrom(lending),
          );
        }),
      );
      const [own, grandchild] = granted.terminalOutput.split('\n').filter((line) => line.startsWith('['));
      expect(own).toBe(grandchild);
      expect(own).toMatch(/,"2"\]$/);
      expect(own).not.toContain(injected);
      expect(own).not.toContain('/stale.sock');
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

describe('createNoSpaceWatch', () => {
  it("finds nextest's ENOSPC split across chunks, past colour codes, and reports it once", () => {
    // nextest's own report of a failed archive extraction into a full RAM lease, from a downstream land gate.
    const watch = createNoSpaceWatch();
    expect(watch.scan('stderr', 'Caused by:\n  No space left')).toBeNull();
    expect(watch.scan('stdout', ' on device (os error 28)\n')).toBeNull();
    expect(watch.scan('stderr', ' on device \u001b[1m(os error 28)\u001b[0m\nCommand exited')).toBe(
      'No space left on device (os error 28)',
    );
    expect(watch.scan('stderr', 'ENOSPC: no space left on device, write\n')).toBeNull();
  });

  it("finds Bun's errno name, and not a word that merely contains it", () => {
    const watch = createNoSpaceWatch();
    expect(watch.scan('stdout', 'XENOSPCY is not an errno\n')).toBeNull();
    expect(watch.scan('stdout', '    code: "ENOSPC"\n')).toBe('code: "ENOSPC"');
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
  const delivered: Array<{ pid: number; signal: NodeJS.Signals }> = [];
  let armed: Promise<void> | undefined;
  let group: number | undefined;
  return {
    calls,
    delivered,
    killer: {
      async kill(pid: number, signal: NodeJS.Signals) {
        group = pid;
        ownedProcessGroups.add(pid);
        armed ??= ready(pid);
        await armed;
        calls.push(signal);
        delivered.push({ pid, signal });
        await real.kill(pid, signal);
      },
    } satisfies ProcessTreeKiller,
    async armed() {
      expect(armed, `${subject}: readiness was never requested before timeout signal delivery`).toBeDefined();
      // The executor tolerates killer errors; re-await here so a failed readiness assertion cannot be swallowed.
      await armed;
    },
    /** Called only after leader exit and EOF from every descendant's inherited lifetime writer. */
    release() {
      if (group === undefined) throw new Error(`${subject}: no process group was armed`);
      ownedProcessGroups.delete(group);
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
  // Bare `ps` through PATH: NixOS has no /bin/ps, and a failed spawn leaves stdout null.
  const observed = spawnSync('ps', ['-o', 'pid=,ppid=,pgid=,stat=', '-p', String(pid)], { encoding: 'utf8' });
  const observedStdout = observed.stdout ?? '';
  const state = `${detail}; state=${observedStdout.trim()} status=${observed.status} stderr=${observed.stderr} error=${observed.error}`;
  expect(observed.status, state).toBe(0);
  const [seenPid, , seenGroup, stat] = observedStdout.trim().split(/\s+/);
  expect(Number(seenPid), state).toBe(pid);
  expect(Number(seenGroup), state).toBe(pgid);
  expect(stat !== undefined && !stat.startsWith('Z'), state).toBe(true);
  return pid;
}

/**
 * A command that ignores SIGTERM, a disposition its `tail` inherits, and never exits on its own, so only the escalation
 * can end it; and `armed`, which resolves once it ignores SIGTERM. A killer that awaits `armed` before the graceful
 * signal sends it to a command that cannot die of it: a bound that fires before the command's shell has even run
 * `trap` (a 250 ms timer lost that race in a loaded gate) ends it on the default disposition, and the escalation under
 * test never runs. The command's FIFO write waits for the reader, so the announcement can neither precede the trap nor
 * be missed.
 */
function ignoresTerm(root: string): { command: string; armed: () => Promise<void> } {
  const fifo = join(root, 'armed.fifo');
  expect(spawnSync('mkfifo', [fifo]).status).toBe(0);
  return {
    command: `sh -c 'trap "" TERM; echo armed > "$0"; exec tail -f /dev/null' '${fifo}'`,
    armed: async () => {
      expect(await readToEnd(fifo)).toBe('armed\n');
    },
  };
}

async function createWorkspace(parent: string = tmpdir()): Promise<WorkspaceFixture> {
  const root = await mkdtemp(join(parent, 'smoothbricks-bounded-exec-'));
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
  return { directory, mountpoint: dirname(directory), attach: 'scripted attach' };
}

/**
 * A temp volume whose answers are fixed, recording which handles were recorded, which leases released, each release
 * marked `(kept)` when its scratch was to be archived, and when space was read.
 */
function scriptedVolume(
  acquisition: Result<RamTempAcquisition, RamTempError>,
  full: RamTempError | null = null,
  space: TempVolumeSpace | null = null,
  ordinaryParent: string | null = null,
): TempVolume & { released: string[]; order: string[] } {
  const released: string[] = [];
  const order: string[] = [];
  return {
    released,
    order,
    acquire: () => Promise.resolve(acquisition),
    get ordinaryParent(): string {
      if (ordinaryParent === null) {
        throw new Error('the scripted volume was built without an ordinary parent to answer');
      }
      return ordinaryParent;
    },
    record(lease, handle) {
      order.push(`record ${lease.directory} ${JSON.stringify(handle)}`);
      return Promise.resolve({ ok: true, value: undefined });
    },
    release(lease, keep) {
      released.push(lease.directory);
      order.push(`release ${lease.directory}${keep ? ' (kept)' : ''}`);
      return Promise.resolve({ ok: true, value: keep ? [`${lease.directory} archived by the scripted volume`] : [] });
    },
    fullness: () => Promise.resolve(full),
    space(lease) {
      order.push('space');
      if (space === null) {
        throw new Error(`the scripted volume holding ${lease.directory} was built without a space reading to answer`);
      }
      return Promise.resolve(space);
    },
  };
}

/** The command's owner: the installed cowshed's subtree verbs, answered by a stand-in that speaks their wire shapes. */
async function standInOwner(
  root: string,
  mode: StandInMode = { admission: 'admitted', retire: 'retire' },
): Promise<SubtreeOwner> {
  return cowshedSubtreeOwner(await standInCowshed(join(root, `stand-in-${mode.admission}-${mode.retire}`), mode));
}

/** Which of `pids` still run, a zombie counting as exited: a reparented one waits on its reaper, not on anything here. */
async function live(pids: readonly number[]): Promise<number[]> {
  const listed = await processesOf(pids);
  return pids.filter((pid) => listed.some((entry) => entry.pid === pid && !entry.stat.startsWith('Z')));
}

function captureWrite(chunks: string[], onChunk?: (text: string) => void): typeof process.stdout.write {
  return ((
    chunk: string | Uint8Array,
    encodingOrCallback?: BufferEncoding | ((error?: Error | null) => void),
    callback?: (error?: Error | null) => void,
  ) => {
    const text = chunk.toString();
    chunks.push(text);
    onChunk?.(text);
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

/** The count a pool of `tokens` lends a run that wants `want`: min(want, tokens). */
function lentCount(want: number, tokens: number): number {
  return Math.min(want, tokens);
}

/**
 * Every path belongs to this fixture. Its ordinary parent, which the scripted volume names, lies outside every Git work
 * tree and Cargo workspace; its root is granted even when the suite inherits a RAM TMPDIR.
 */
async function withOrdinaryTempWorkspace(
  body: (workspace: WorkspaceFixture, ordinary: string) => Promise<void>,
): Promise<void> {
  const workspace = await createWorkspace(
    process.env.COWSHED_RUNTIME_LINK ?? (process.platform === 'darwin' ? '/private/tmp' : '/tmp'),
  );
  const inherited = process.env.TMPDIR;
  const ordinary = await mkdtemp(join(outsideTreesTmpdir(), 'bounded-exec-ordinary-'));
  workspaces.push(ordinary);
  const legacy = join(workspace.root, 'ram3');
  await workspace.write('ram3/.smoo-ram', 'legacy-volume-name\n');
  await workspace.write('ram3/existing', 'leave these bytes untouched\n');
  await workspace.write(
    'temp-probe.js',
    [
      "const fs = require('node:fs');",
      "const path = require('node:path');",
      'const directory = process.env.TMPDIR;',
      "fs.writeFileSync('observed-tmpdir', directory);",
      "fs.writeFileSync(path.join(directory, 'command-evidence'), 'command evidence\\n');",
      "console.log('actual-tmpdir=' + directory);",
      'process.exit(Number(process.env.PROBE_EXIT_CODE ?? 0));',
      '',
    ].join('\n'),
  );
  process.env.TMPDIR = legacy;
  try {
    await body(workspace, ordinary);
    expect((await readdir(legacy)).sort()).toEqual(['.smoo-ram', 'existing']);
    expect(await readFile(join(legacy, '.smoo-ram'), 'utf8')).toBe('legacy-volume-name\n');
    expect(await readFile(join(legacy, 'existing'), 'utf8')).toBe('leave these bytes untouched\n');
  } finally {
    if (inherited === undefined) delete process.env.TMPDIR;
    else process.env.TMPDIR = inherited;
  }
}
