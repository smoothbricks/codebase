import { describe, expect, it, jest } from 'bun:test';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { churnFailures, startChurn } from '../../__tests__/churn-group.js';
import { BOUNDED_TEST_IDLE_TIMEOUT_MS } from '../../bounded-test-policy.js';
import { processTable } from '../../testing.js';
import {
  captureHangDiagnostics,
  type HangCaptureRequest,
  type HangDiagnosticHost,
  type HangProbeResult,
  stopProbe,
} from './hang-diagnostics.js';

const ROOT = 101;
const STARTED = 'Thu Oct 15 12:34:56 2026';
const rootRow = `${ROOT} 1 ${ROOT} S 0.0 03:01 ${STARTED} runner --test selected`;
const childRow = `202 ${ROOT} ${ROOT} S 0.0 03:00 ${STARTED} child waiting`;
const grandchildRow = `303 202 ${ROOT} S 0.0 02:59 ${STARTED} grandchild holding pipe`;
const reparentedRow = `404 1 ${ROOT} S 0.0 02:58 ${STARTED} reparented group member`;
const unrelatedRow = `505 1 505 S 0.0 02:58 ${STARTED} unrelated process`;
const census = [rootRow, childRow, grandchildRow, reparentedRow, unrelatedRow].join('\n');
const fileObservation =
  'COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME\nchild 303 tester 7r REG 1,2 4 123 /fixture/held-file\n';
const exited = (stdout: string, code = 0, stderr = ''): HangProbeResult => ({ status: 'exited', code, stdout, stderr });
const request = (): HangCaptureRequest => ({
  pid: ROOT,
  pgid: ROOT,
  expiresAtMs: Date.now() + BOUNDED_TEST_IDLE_TIMEOUT_MS,
  deadlineMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
});

/** A `sample` report as the tool prints it: header, call graph, then the binary-image list a snapshot omits. */
const SAMPLE_REPORT = [
  'Analysis of sampling runner (pid 101) every 1 millisecond',
  'Call graph:',
  '    1000 Thread_1   DispatchQueue_1: com.apple.main-thread  (serial)',
  '      1000 read  (in libsystem_kernel.dylib) + 8  [0x18dd5a1c8]',
  '',
  'Binary Images:',
  '       0x100000000 -        0x100001000 +runner <00000000-0000-0000-0000-000000000000> /fixture/runner',
  '',
].join('\n');

/**
 * Answers the census probes in order from `results`, and every `sudo -n sample` stack probe, which runs beside them,
 * from `sample`.
 */
function scriptedHost(
  results: readonly HangProbeResult[],
  sample: (pid: number) => HangProbeResult | Promise<HangProbeResult> = () => exited(SAMPLE_REPORT),
) {
  const calls: { file: string; args: readonly string[]; timeoutMs: number }[] = [];
  const samples: { pid: number; args: readonly string[]; timeoutMs: number }[] = [];
  let index = 0;
  const host: HangDiagnosticHost = {
    platform: 'darwin',
    async probe(file, args, timeoutMs) {
      if (file === 'sudo' && args[1] === 'sample') {
        const pid = Number(args[2]);
        samples.push({ pid, args, timeoutMs });
        return sample(pid);
      }
      calls.push({ file, args, timeoutMs });
      const result = results[index++];
      if (result === undefined) throw new Error(`unexpected probe ${file}`);
      return result;
    },
  };
  return { host, calls, samples };
}

/** The first complete line a process prints. A stream that ends first fails, naming what it did print. */
async function firstLine(stdout: ReadableStream<Uint8Array>, who: string): Promise<string> {
  const reader = stdout.getReader();
  const decoder = new TextDecoder();
  let text = '';
  try {
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) throw new Error(`${who} ended before a complete line; observed ${JSON.stringify(text)}`);
      text += decoder.decode(chunk.value, { stream: true });
      const end = text.indexOf('\n');
      if (end !== -1) return text.slice(0, end);
    }
  } finally {
    reader.releaseLock();
  }
}

describe('pre-expiry hang diagnostics', () => {
  it('keeps grandchildren and reparented group members, but not unrelated host rows', async () => {
    const { host, calls } = scriptedHost([exited(census), exited(fileObservation), exited('', 1), exited(census)]);
    const capture = await captureHangDiagnostics(request(), host);
    expect(capture.outcome).toBe('holders');
    for (const row of [rootRow, childRow, grandchildRow, reparentedRow]) expect(capture.text).toContain(row);
    expect(capture.text).not.toContain(unrelatedRow);
    expect(capture.text).toContain('/fixture/held-file');
    expect(capture.text).toContain('not proof of a contested lock');
    expect(capture.text).toContain('pid=101 pgid=101');
    expect(calls.map((call) => call.file)).toEqual(['ps', 'sudo', 'sudo', 'ps']);
    expect(calls[1]?.args).toEqual(['-n', 'lsof', '-nP', '-p', '101,202,303,404']);
    expect(calls[2]?.args).toEqual(['-n', 'lsof', '-nP', '-F', 'pcn', '--', '/fixture/held-file']);
    expect(capture.lockWaits).toEqual([]);
    expect(capture.offsetFromExpiryMs).toBeGreaterThan(0);
    expect(capture.capturedAtMs).toBeGreaterThanOrEqual(capture.startedAtMs);
  });

  it('reports no matches only after successful resource inspection and stable identities', async () => {
    const { host } = scriptedHost([exited(rootRow), exited('', 1), exited(rootRow)]);
    const capture = await captureHangDiagnostics(request(), host);
    expect(capture.outcome).toBe('none');
    expect(capture.text).toContain('no open-file matches within its stated visibility');
  });

  it('names denied privileged access and limits fallback claims to ordinary-user visibility', async () => {
    const denied: HangProbeResult = {
      status: 'unavailable',
      detail: 'sudo: permission denied',
      stdout: '',
      stderr: '',
    };
    const { host, calls } = scriptedHost([exited(rootRow), denied, exited(fileObservation), exited(rootRow)]);
    const capture = await captureHangDiagnostics(request(), host);
    expect(capture.outcome).toBe('incomplete');
    expect(capture.text).toContain('sudo: permission denied');
    expect(capture.text).toContain('ordinary-user visibility only');
    expect(capture.text).toContain('/fixture/held-file');
    expect(calls[2]?.file).toBe('lsof');
  });

  it('retains partial failed output instead of replacing it with an empty fallback', async () => {
    const partial: HangProbeResult = {
      status: 'failed',
      detail: 'probe output exceeded bound; output is incomplete',
      stdout: fileObservation,
      stderr: 'read failed',
    };
    const { host } = scriptedHost([exited(rootRow), partial, exited('', 1), exited(rootRow)]);
    const capture = await captureHangDiagnostics(request(), host);
    expect(capture.outcome).toBe('incomplete');
    expect(capture.text).toContain('Partial privileged lsof output');
    expect(capture.text).toContain('/fixture/held-file');
    expect(capture.text).toContain('read failed');
  });

  it("keeps the verdict when lsof's only stderr is its fallback for a mount it could not stat", async () => {
    // Observed verbatim while a concurrent image detached: lsof exited 0 and listed every selected descriptor.
    const fallback =
      "lsof: WARNING: can't stat() apfs file system /private/tmp/hang-capture-repro/dmg.Gqndhm\n" +
      '      Output information may be incomplete.\n' +
      '      assuming "dev=100037e" from mount table\n';
    const warned = scriptedHost([
      exited(rootRow),
      exited(fileObservation, 0, fallback),
      exited('', 1),
      exited(rootRow),
    ]);
    const capture = await captureHangDiagnostics(request(), warned.host);
    expect(capture.outcome, capture.text).toBe('holders');
    expect(capture.text).toContain("can't stat() apfs file system /private/tmp/hang-capture-repro/dmg.Gqndhm");
    expect(capture.text).toContain('omits no descriptor of the selected processes');
    expect(warned.calls.map((call) => call.file)).toEqual(['ps', 'sudo', 'sudo', 'ps']);
    const empty = scriptedHost([exited(rootRow), exited('', 1, fallback), exited(rootRow)]);
    expect((await captureHangDiagnostics(request(), empty.host)).outcome).toBe('none');
    for (const stderr of [`${fallback}lsof: WARNING: other\n`, 'lsof: WARNING: other\n']) {
      const { host } = scriptedHost([exited(rootRow), exited(fileObservation, 0, stderr), exited(rootRow)]);
      const other = await captureHangDiagnostics(request(), host);
      expect(other.outcome, other.text).toBe('incomplete');
      expect(other.text).toContain('lsof: WARNING: other');
    }
  });

  it('names a lock wait for a file the run holds and a process outside the run also holds', async () => {
    // Pid 303 is the run's own grandchild, so its row in the holder census is not an outside holder.
    const outside = 'p303\ncchild\nf7\nn/fixture/held-file\np505\ncperl\nf3\nn/fixture/held-file\n';
    const { host, calls } = scriptedHost([exited(census), exited(fileObservation), exited(outside), exited(census)]);
    const capture = await captureHangDiagnostics(request(), host);
    expect(capture.outcome, capture.text).toBe('holders');
    expect(capture.lockWaits).toEqual([
      { path: '/fixture/held-file', waiters: [303], holders: [{ pid: 505, command: 'perl' }] },
    ]);
    expect(capture.text).toContain(
      'Lock wait (open-handle evidence, not a kernel lock owner): /fixture/held-file open in the run by pid 303 and outside it by pid 505 (perl)',
    );
    expect(calls[2]?.args).toEqual(['-n', 'lsof', '-nP', '-F', 'pcn', '--', '/fixture/held-file']);
  });

  it('names no lock wait for a file the run holds only through an unnumbered descriptor', async () => {
    const library =
      'COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME\nchild 303 tester txt REG 1,2 4 123 /fixture/library\n';
    const { host, calls } = scriptedHost([exited(census), exited(library), exited(census)]);
    const capture = await captureHangDiagnostics(request(), host);
    expect(capture.outcome).toBe('holders');
    expect(capture.lockWaits).toEqual([]);
    expect(calls.map((call) => call.file)).toEqual(['ps', 'sudo', 'ps']);
  });

  it('marks the capture incomplete, with no lock wait, when the holder census does not answer', async () => {
    const holders: HangProbeResult[] = [
      { status: 'timed-out', detail: 'probe exceeded 1500ms', stdout: '', stderr: '' },
      exited('p505\ncperl\nf3\nn/fixture/held-file\n', 0, 'lsof: WARNING: other\n'),
      exited('n/fixture/held-file\n'),
      exited('p505\ncperl\nq3\n'),
    ];
    for (const holder of holders) {
      const { host } = scriptedHost([exited(census), exited(fileObservation), holder, exited(census)]);
      const capture = await captureHangDiagnostics(request(), host);
      expect(capture.outcome, capture.text).toBe('incomplete');
      expect(capture.lockWaits).toEqual([]);
      expect(capture.text).not.toContain('Lock wait (open-handle');
    }
  });

  it('does not turn an exited root, reused pid, or changed descendant membership into no holders', async () => {
    const absent = scriptedHost([exited(unrelatedRow)]);
    expect((await captureHangDiagnostics(request(), absent.host)).outcome).toBe('incomplete');
    expect(absent.calls).toHaveLength(1);
    for (const after of [rootRow.replace(STARTED, 'Thu Oct 15 12:35:00 2026'), `${rootRow}\n${childRow}`]) {
      const { host } = scriptedHost([exited(rootRow), exited('', 1), exited(after)]);
      const capture = await captureHangDiagnostics(request(), host);
      expect(capture.outcome).toBe('incomplete');
      expect(capture.text).toContain('identities or membership changed');
    }
  });

  it('names process probe failures and malformed census output without throwing', async () => {
    for (const result of [
      exited('', 1, 'ps: denied'),
      exited('not a ps row'),
      { status: 'timed-out', detail: 'ps deadline', stdout: '', stderr: '' } satisfies HangProbeResult,
    ]) {
      const { host } = scriptedHost([result]);
      const capture = await captureHangDiagnostics(request(), host);
      expect(capture.outcome).toBe('incomplete');
      expect(capture.text).toContain('ps before');
    }
    const throwing: HangDiagnosticHost = {
      platform: 'darwin',
      async probe() {
        throw new Error('probe rejected');
      },
    };
    expect((await captureHangDiagnostics(request(), throwing)).text).toContain('probe rejected');
  });

  it('returns at its own deadline even when the probe never answers, and starts no later probe', async () => {
    let calls = 0;
    let signal: AbortSignal | undefined;
    const unresponsive: HangDiagnosticHost = {
      platform: 'darwin',
      probe(_file, _args, _timeoutMs, captureSignal) {
        calls += 1;
        signal = captureSignal;
        // An unanswered process boundary; the capture must not rely on its cooperation.
        return new Promise<HangProbeResult>(() => {});
      },
    };
    // The probe is held off by construction, so the 1 ms deadline timer is the only event that can end this capture.
    const capture = await captureHangDiagnostics({ ...request(), deadlineMs: 1 }, unresponsive);
    expect(capture.outcome).toBe('incomplete');
    expect(capture.text).toContain('1ms deadline reached during ps before');
    expect(signal?.aborted).toBe(true);
    expect(calls).toBe(1);
  });

  it('cancels a pending probe when its progress generation ends without starting another', async () => {
    const controller = new AbortController();
    let calls = 0;
    let seen: AbortSignal | undefined;
    const pending: HangDiagnosticHost = {
      platform: 'darwin',
      probe(_file, _args, _timeoutMs, signal) {
        calls += 1;
        seen = signal;
        return new Promise<HangProbeResult>(() => {});
      },
    };
    const collecting = captureHangDiagnostics({ ...request(), signal: controller.signal }, pending);
    expect(calls).toBe(1);
    controller.abort();
    const capture = await collecting;
    expect(capture.outcome).toBe('incomplete');
    expect(capture.text).toContain('Capture cancelled during ps before');
    expect(seen?.aborted).toBe(true);
    expect(calls).toBe(1);
  });

  it('starts no probe for a generation already cancelled', async () => {
    const controller = new AbortController();
    controller.abort();
    const { host, calls } = scriptedHost([]);
    const capture = await captureHangDiagnostics({ ...request(), signal: controller.signal }, host);
    expect(capture.outcome).toBe('incomplete');
    expect(capture.text).toContain('Capture cancelled');
    expect(calls).toHaveLength(0);
  });

  it('starts no process probe after the pre-expiry window or with no remaining budget', async () => {
    for (const bounds of [{ expiresAtMs: Date.now() - 1 }, { deadlineMs: 0 }]) {
      const { host, calls } = scriptedHost([]);
      const capture = await captureHangDiagnostics({ ...request(), ...bounds }, host);
      expect(capture.outcome).toBe('incomplete');
      expect(capture.text).toContain('no probe started');
      expect(calls).toEqual([]);
    }
  });

  it('keeps process-only visibility explicit on platforms without the Darwin open-file probe', async () => {
    const { host, calls, samples } = scriptedHost([exited(rootRow), exited(rootRow)]);
    const capture = await captureHangDiagnostics(request(), { ...host, platform: 'linux' });
    expect(capture.outcome).toBe('holders');
    expect(capture.text).toContain('observations below name processes only');
    expect(capture.text).toContain('Stack samples not provided on linux');
    expect(samples).toEqual([]);
    expect(calls.map((call) => call.file)).toEqual(['ps', 'ps']);
  });

  it('samples the runner and its leaf processes beside the open-file census, and closes the census after them', async () => {
    const answers: ((result: HangProbeResult) => void)[] = [];
    let allStarted!: () => void;
    const started = new Promise<void>((resolve) => {
      allStarted = resolve;
    });
    const { host, calls, samples } = scriptedHost(
      [exited(census), exited(fileObservation), exited('', 1), exited(census)],
      () =>
        new Promise<HangProbeResult>((resolve) => {
          answers.push(resolve);
          if (answers.length === 3) allStarted();
        }),
    );
    const capturing = captureHangDiagnostics(request(), host);
    // Every stack sample has started while none has answered, beside the open-file census; the closing census waits.
    await started;
    expect(samples.map((sample) => sample.pid)).toEqual([ROOT, 303, 404]);
    expect(calls[1]?.args).toEqual(['-n', 'lsof', '-nP', '-p', '101,202,303,404']);
    expect(calls.filter((call) => call.file === 'ps')).toHaveLength(1);
    for (const answer of answers) answer(exited(SAMPLE_REPORT));
    const capture = await capturing;
    expect(capture.outcome, capture.text).toBe('holders');
    expect(calls.map((call) => call.file)).toEqual(['ps', 'sudo', 'sudo', 'ps']);
    expect(samples[1]?.args).toEqual(['-n', 'sample', '303', '1', '-mayDie', '-file', '/dev/stdout']);
    for (const sample of samples) expect(sample.timeoutMs).toBeLessThanOrEqual(3_000);
    expect(capture.text).toContain('stack sample pid 303 (1s; binary image list omitted):');
    expect(capture.text).toContain('1000 read  (in libsystem_kernel.dylib) + 8');
    expect(capture.text).not.toContain('Binary Images:');
    expect(capture.text).not.toContain('/fixture/runner');
  });

  it('bounds stack samples to the runner and four live leaf processes, and names the leaves it left out', async () => {
    const workers = [601, 602, 603, 604, 605, 606].map(
      (pid) => `${pid} ${ROOT} ${ROOT} S 0.0 03:00 ${STARTED} worker ${pid}`,
    );
    const zombie = `607 ${ROOT} ${ROOT} Z 0.0 03:00 ${STARTED} (worker)`;
    const rows = [rootRow, ...workers, zombie].join('\n');
    const { host, samples } = scriptedHost([exited(rows), exited('', 1), exited(rows)]);
    const capture = await captureHangDiagnostics(request(), host);
    expect(capture.outcome, capture.text).toBe('none');
    expect(samples.map((sample) => sample.pid)).toEqual([ROOT, 601, 602, 603, 604]);
    expect(capture.text).toContain('not sampled: pid 605, 606\n');
  });

  it('records a stack sample sudo refuses as no stack, keeping the holder outcome and the closing census', async () => {
    const refused = exited('', 1, 'sudo: a password is required\n');
    const { host, calls } = scriptedHost([exited(rootRow), exited('', 1), exited(rootRow)], () => refused);
    const capture = await captureHangDiagnostics(request(), host);
    expect(capture.outcome, capture.text).toBe('none');
    expect(calls.map((call) => call.file)).toEqual(['ps', 'sudo', 'ps']);
    expect(capture.text).toContain('stack sample pid 101 stderr:\nsudo: a password is required');
    expect(capture.text).toContain('stack sample pid 101: no stack;');
  });

  it('ends a privileged probe through sudo, so its root lsof does not outlive sudo', async () => {
    // The privileged probe exists only on Darwin, where it runs `sudo -n lsof`.
    if (process.platform !== 'darwin') return;
    // `-r 30` lists once, then sleeps holding its pipe: alive and silent when stopped, as a slow scan is.
    const command = `lsof -r 30 -nP -p ${process.pid}`;
    const probe = spawn('sudo', ['-n', ...command.split(' ')], { detached: true, stdio: ['ignore', 'pipe', 'pipe'] });
    try {
      const exited = once(probe, 'exit');
      let stderr = '';
      probe.stderr.setEncoding('utf8').on('data', (chunk: string) => {
        stderr += chunk;
      });
      // The first listing is the readiness event: sudo has finished starting and lsof runs as root.
      const listed = await Promise.race([once(probe.stdout, 'data').then(() => true), exited.then(() => false)]);
      expect(listed, `sudo -n ${command} ended before listing: ${stderr}`).toBe(true);
      if (probe.pid === undefined) throw new Error('sudo has no pid');
      stopProbe('sudo', probe.pid);
      await exited;
      const left = (await processTable()).filter((entry) => entry.command.trim() === command);
      expect(left, 'root lsof rows left after sudo exited').toEqual([]);
    } finally {
      probe.stdout.destroy();
      probe.stderr.destroy();
    }
  });

  it('ends a probe whose members fork as it is stopped, leaving none behind', async () => {
    // macOS does not hold a fork back for a group signal in flight, so a member forked as SIGKILL goes out is not
    // reached by it. The probe is a group that forks as fast as it can; the end of file of the FIFO every member holds
    // is the death of the last one, and a stop that is not repeated leaves a case that only the backstop ends. The
    // probe is unreaped while the stop waits, so it says the group is still listed: that is not the proof.
    const directory = await mkdtemp(join(tmpdir(), 'hang-probe-churn-'));
    try {
      const churn = await startChurn(directory);
      stopProbe('sh', churn.group);
      expect({ gone: await churn.gone, stderr: churnFailures(churn.stderr()) }).toEqual({ gone: '', stderr: [] });
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });

  it('captures a real child, its grandchild, and an actually open file before either is stopped', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'hang-snapshot-'));
    const held = join(directory, 'held');
    await writeFile(held, 'owned file\n');
    const child = Bun.spawn(
      [
        process.execPath,
        '-e',
        `
      const fs = require('node:fs');
      fs.openSync(process.env.HANG_SNAPSHOT_FILE, 'r');
      const descendant = Bun.spawn([process.execPath, '-e', "console.log('ready'); process.stdin.resume()"], {
        stdin: 'pipe', stdout: 'pipe', stderr: 'ignore',
      });
      const ready = descendant.stdout.getReader();
      await ready.read();
      ready.releaseLock();
      console.log('PID=' + descendant.pid);
      process.stdin.resume();
    `,
      ],
      {
        env: { ...process.env, HANG_SNAPSHOT_FILE: held },
        stdin: 'pipe',
        stdout: 'pipe',
        stderr: 'pipe',
      },
    );
    let descendant: number | undefined;
    const failures: unknown[] = [];
    try {
      const reader = child.stdout.pipeThrough(new TextDecoderStream()).getReader();
      let text = '';
      try {
        // The first complete line is the readiness event; a child that exits first ends the stream instead. No timer
        // ends this loop.
        for (;;) {
          const chunk = await reader.read();
          if (chunk.done) throw new Error('child stdout ended before a complete PID line');
          text += chunk.value;
          if (text.includes('\n')) break;
        }
      } catch (cause) {
        throw new Error(`PID readiness failed; observed ${JSON.stringify(text)}`, { cause });
      } finally {
        reader.releaseLock();
      }
      const line = text.slice(0, text.indexOf('\n'));
      const match = /^PID=(\d+)$/.exec(line);
      descendant = match === null ? Number.NaN : Number(match[1]);
      expect(
        Number.isSafeInteger(descendant) && descendant > 0,
        `expected a tagged PID line, observed ${JSON.stringify(line)}`,
      ).toBe(true);
      // The capture budget (`HANG_CAPTURE_MS`) and probe limits are product constants this test cannot set, so the
      // clock is stopped while the real `ps` and `lsof` run: each ends when it exits; no timer cuts it short.
      jest.useFakeTimers();
      const capture = await captureHangDiagnostics({ ...request(), pid: child.pid, pgid: null }).finally(() =>
        jest.useRealTimers(),
      );
      expect(capture.outcome, capture.text).toBe('holders');
      expect(capture.text).toContain(String(child.pid));
      expect(capture.text).toContain(String(descendant));
      if (process.platform === 'darwin') expect(capture.text).toContain(held);
    } catch (error) {
      failures.push(error);
    }
    if (descendant !== undefined && Number.isSafeInteger(descendant) && descendant > 0) {
      try {
        process.kill(descendant, 'SIGKILL');
      } catch (error) {
        if (!(error instanceof Error && 'code' in error && error.code === 'ESRCH')) failures.push(error);
      }
    }
    try {
      child.kill('SIGKILL');
      await child.exited;
    } catch (error) {
      failures.push(error);
    }
    try {
      await rm(directory, { recursive: true, force: true });
    } catch (error) {
      failures.push(error);
    }
    if (failures.length > 0) {
      throw new AggregateError(failures, `hang snapshot fixture ${directory} failed`);
    }
  });

  it('names the process outside the run that holds the file a run is blocked on in flock', async () => {
    if (process.platform !== 'darwin') return;
    const directory = await mkdtemp(join(tmpdir(), 'hang-lock-wait-'));
    const file = join(directory, 'package-cache');
    await writeFile(file, '');
    // The holder takes the exclusive lock and lives until the test kills it. The run opens the same file, prints
    // `waiting`, and blocks in flock(2): the snapshot observes its open descriptor, not the blocked call itself.
    const holderScript = 'open(F, ">>", $ARGV[0]) or die; flock(F, 2) or die; $| = 1; print "held\\n"; sleep';
    const runScript = 'open(F, ">>", $ARGV[0]) or die; $| = 1; print "waiting\\n"; flock(F, 2) or die; sleep';
    const holder = Bun.spawn(['perl', '-e', holderScript, file], { stdin: 'ignore', stdout: 'pipe', stderr: 'pipe' });
    let run: typeof holder | undefined;
    const failures: unknown[] = [];
    try {
      expect(await firstLine(holder.stdout, 'lock holder')).toBe('held');
      run = Bun.spawn(['perl', '-e', runScript, file], { stdin: 'ignore', stdout: 'pipe', stderr: 'pipe' });
      expect(await firstLine(run.stdout, 'lock waiter')).toBe('waiting');
      jest.useFakeTimers();
      const capture = await captureHangDiagnostics({ ...request(), pid: run.pid, pgid: null }).finally(() =>
        jest.useRealTimers(),
      );
      const path = await realpath(file);
      expect(capture.outcome, capture.text).toBe('holders');
      expect(capture.lockWaits, capture.text).toEqual([
        { path, waiters: [run.pid], holders: [{ pid: holder.pid, command: 'perl' }] },
      ]);
      expect(capture.text).toContain(
        `Lock wait (open-handle evidence, not a kernel lock owner): ${path} open in the run by pid ${run.pid} and outside it by pid ${holder.pid} (perl)`,
      );
    } catch (error) {
      failures.push(error);
    }
    for (const child of [run, holder]) {
      if (child === undefined) continue;
      try {
        child.kill('SIGKILL');
        await child.exited;
      } catch (error) {
        failures.push(error);
      }
    }
    try {
      await rm(directory, { recursive: true, force: true });
    } catch (error) {
      failures.push(error);
    }
    if (failures.length > 0) {
      throw new AggregateError(failures, `lock wait fixture ${directory} failed`);
    }
  });
});
