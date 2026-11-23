import { describe, expect, it, jest } from 'bun:test';
import { mkdtemp, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { ProcessGroups, RunState } from '@smoothbricks/procinfo';
import * as procinfo from '@smoothbricks/procinfo';
import { churnFailures, startChurn } from '../../__tests__/churn-group.js';
import { BOUNDED_TEST_IDLE_TIMEOUT_MS } from '../../bounded-test-policy.js';
import {
  captureHangDiagnostics,
  HANG_CAPTURE_MS,
  type HangCaptureRequest,
  type HangDiagnosticHost,
  type HangProbeResult,
  type HangProcessReader,
  runProbe,
  stopProbe,
} from './hang-diagnostics.js';

const ROOT = 101;
const STARTED_US = 1_792_000_000_000_000;

/** One process as procinfo reads it: in the runner's group, asleep, idle and started at STARTED_US unless it says so. */
interface FakeProcess {
  readonly pid: number;
  readonly ppid: number;
  readonly pgid?: number;
  readonly state?: RunState;
  /** The CPU time it has used, in ms; null is a reading procinfo refuses. */
  readonly cpuMs?: number | null;
  readonly startedUs?: number;
  /** procinfo could not read its arguments in time and answered its command name alone. */
  readonly nameOnly?: boolean;
  readonly command: string;
}
const root: FakeProcess = { pid: ROOT, ppid: 1, command: 'runner --test selected' };
const child: FakeProcess = { pid: 202, ppid: ROOT, command: 'child waiting' };
const grandchild: FakeProcess = { pid: 303, ppid: 202, command: 'grandchild holding pipe' };
/** A member of the runner's group whose parent exited: no descendant of the runner, reached by its group alone. */
const reparented: FakeProcess = { pid: 404, ppid: 1, command: 'reparented group member' };
const unrelated: FakeProcess = { pid: 505, ppid: 1, pgid: 505, command: 'unrelated process' };
const HOST = [root, child, grandchild, reparented, unrelated];
const fileObservation =
  'COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME\nchild 303 tester 7r REG 1,2 4 123 /fixture/held-file\n';
const exited = (stdout: string, code = 0, stderr = ''): HangProbeResult => ({ status: 'exited', code, stdout, stderr });
const request = (): HangCaptureRequest => ({
  pid: ROOT,
  pgid: ROOT,
  awakeExpiresAtMs: performance.now() + BOUNDED_TEST_IDLE_TIMEOUT_MS,
  awakeNow: () => performance.now(),
});

/** The tree of a run that compiles: its runner, an idle child, and a compiler (pid 303) at `compilerMs` of CPU. */
const compileTree = (compilerMs: number | null): FakeProcess[] => [
  root,
  child,
  { pid: 303, ppid: 202, state: { state: 'running' }, cpuMs: compilerMs, command: 'rustc --crate-name hang_fixture' },
];
const COMPILE_CLASS = "a process in the test's tree was using CPU: likely a build inside the test";

/** A refusal as the procinfo addon throws it. */
function refused(message: string): Error {
  return Object.assign(new Error(message), { code: 'environment-missing', hint: 'read it again' });
}

/**
 * procinfo's answers about the processes `table()` holds now, each read as the kernel answers for one pid: a pid in no
 * row is reaped. `groups` is false where the kernel lists no group (Linux). Faked at procinfo's exported boundary, never
 * below it.
 */
function readerOf(table: () => readonly FakeProcess[], groups = true): { reader: HangProcessReader; reads(): number } {
  let reads = 0;
  const find = (pid: number): FakeProcess | undefined => {
    reads += 1;
    return table().find((entry) => entry.pid === pid);
  };
  const text = new TextEncoder();
  const processGroups: ProcessGroups = {
    pids(pgid) {
      reads += 1;
      return table()
        .filter((entry) => (entry.pgid ?? ROOT) === pgid)
        .map((entry) => entry.pid);
    },
    members() {
      throw new Error('the snapshot reads a group by its pids');
    },
    cpu() {
      throw new Error("the snapshot reads no group's CPU");
    },
  };
  const reader: HangProcessReader = {
    processLineages: (pids) =>
      pids.flatMap((pid) => {
        const entry = find(pid);
        return entry === undefined
          ? []
          : [{ pid, ppid: entry.ppid, pgid: entry.pgid ?? ROOT, zombie: entry.state?.state === 'zombie' }];
      }),
    childPids(pid) {
      reads += 1;
      return table()
        .filter((entry) => entry.ppid === pid)
        .map((entry) => entry.pid);
    },
    processGroups: groups ? processGroups : null,
    processRunState(pid) {
      const entry = find(pid);
      return entry === undefined ? { state: 'gone' } : (entry.state ?? { state: 'sleeping' });
    },
    processArguments(pid) {
      const entry = find(pid);
      if (entry === undefined) throw refused(`no process ${pid}`);
      const argv = entry.command.split(' ').map((argument) => text.encode(argument));
      return {
        executable: argv[0] ?? new Uint8Array(),
        argv: entry.nameOnly ? argv.slice(0, 1) : argv,
        nameOnly: entry.nameOnly ?? false,
      };
    },
    cpuTimeNs(pid) {
      const entry = find(pid);
      if (entry === undefined || entry.state?.state === 'zombie') return null;
      if (entry.cpuMs === null) throw refused(`cannot read the CPU time of process ${pid}`);
      return BigInt(entry.cpuMs ?? 0) * 1_000_000n;
    },
    startedUs(pid) {
      const entry = find(pid);
      return entry === undefined ? null : (entry.startedUs ?? STARTED_US);
    },
  };
  return { reader, reads: () => reads };
}

/**
 * The real process boundary with the CPU interval's wait taken out. The cases that use it hold the clock still while the
 * real lsof runs, so a timer would never fire; the tree is read through the real procinfo.
 */
const realProbes: HangDiagnosticHost = {
  platform: process.platform,
  probe: runProbe,
  sleep: async () => {},
  processes: procinfo,
};

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

/** An `fs_usage -w -f filesys` line: a process waiting in a read of an image on the store volume. */
const FS_USAGE_REPORT =
  '12:00:00.000000  RdData[A]  D=0x0001  B=0x1000  /dev/disk3s1  /fixture/store/image.dmg  0.512 W child.303\n';
/**
 * A Darwin host whose processes are `trees[0]` until the snapshot pauses between its two readings and `trees[1]` (else
 * the same) after: time passes only in the pause. Answers the open-file probes in order from `results`, and every
 * `sudo -n sample` stack probe, which runs beside them, from `sample`.
 */
function scriptedHost(
  trees: readonly (readonly FakeProcess[])[],
  results: readonly HangProbeResult[] = [],
  sample: (pid: number) => HangProbeResult | Promise<HangProbeResult> = () => exited(SAMPLE_REPORT),
) {
  const calls: { file: string; args: readonly string[]; timeoutMs: number }[] = [];
  const samples: { pid: number; args: readonly string[]; timeoutMs: number }[] = [];
  const traces: { pid: number; args: readonly string[]; timeoutMs: number }[] = [];
  const pauses: number[] = [];
  let tree = trees[0] ?? [];
  const { reader, reads } = readerOf(() => tree);
  let index = 0;
  const host: HangDiagnosticHost = {
    platform: 'darwin',
    processes: reader,
    async sleep(ms) {
      pauses.push(ms);
      tree = trees[1] ?? tree;
    },
    async probe(file, args, timeoutMs) {
      if (file === 'sudo' && args[1] === 'sample') {
        const pid = Number(args[2]);
        samples.push({ pid, args, timeoutMs });
        return sample(pid);
      }
      if (file === 'sudo' && args[1] === 'fs_usage') {
        traces.push({ pid: Number(args.at(-1)), args, timeoutMs });
        return exited(FS_USAGE_REPORT);
      }
      calls.push({ file, args, timeoutMs });
      const result = results[index++];
      if (result === undefined) throw new Error(`unexpected probe ${file}`);
      return result;
    },
  };
  return { host, calls, samples, traces, pauses, reads };
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
  //#region smoo/cowshed!n/hang-snapshot-tree
  it("reads the runner's descendants and its group's reparented members through procinfo, never a process table", async () => {
    const { host, calls } = scriptedHost([HOST], [exited(fileObservation), exited('', 1)]);
    const capture = await captureHangDiagnostics(request(), host);
    expect(capture.outcome).toBe('holders');
    for (const entry of [root, child, grandchild, reparented]) expect(capture.text).toContain(entry.command);
    expect(capture.text).not.toContain(unrelated.command);
    expect(capture.text).toContain('/fixture/held-file');
    expect(capture.text).toContain('not proof of a contested lock');
    expect(capture.text).toContain('pid=101 pgid=101');
    expect(calls.map((call) => call.file)).toEqual(['sudo', 'sudo']);
    expect(calls[0]?.args).toEqual(['-n', 'lsof', '-nP', '-p', '101,202,303,404']);
    expect(calls[1]?.args).toEqual(['-n', 'lsof', '-nP', '-F', 'pcn', '--', '/fixture/held-file']);
    expect(capture.lockWaits).toEqual([]);
    expect(capture.offsetFromExpiryMs).toBeGreaterThan(0);
    expect(capture.capturedAtMs).toBeGreaterThanOrEqual(capture.startedAtMs);
  });

  it('names each process with its lineage, state, run time, CPU time, start and command', async () => {
    const started = Date.now() * 1_000 - 61_000_000;
    const compiler: FakeProcess = { ...grandchild, state: { state: 'running' }, cpuMs: 4_250, startedUs: started };
    const { host } = scriptedHost([[root, child, compiler]], [exited('', 1)]);
    const capture = await captureHangDiagnostics(request(), host);
    expect(capture.text).toContain('process tree before: PID PPID PGID STATE ELAPSED CPU STARTED COMMAND');
    expect(capture.text).toMatch(
      new RegExp(
        `\\n303 202 101 running 01:0[12] 4\\.250s ${new Date(started / 1_000).toISOString()} grandchild holding pipe\\n`,
      ),
    );
  });

  it('reaches no reparented group member where the kernel lists no group, and says so', async () => {
    const { reader } = readerOf(() => HOST, false);
    const capture = await captureHangDiagnostics(request(), {
      ...scriptedHost([HOST]).host,
      platform: 'linux',
      processes: reader,
    });
    expect(capture.text).toContain(child.command);
    expect(capture.text).not.toContain(reparented.command);
    expect(capture.text).toContain('group 101 is not listed on linux');
  });

  it('names a process whose arguments procinfo could not read in time by its command name, and keeps its identity', async () => {
    const stuck: FakeProcess = { ...grandchild, state: { state: 'uninterruptible' }, nameOnly: true };
    const { host } = scriptedHost(
      [
        [root, child, stuck],
        [root, child, grandchild],
      ],
      [exited('', 1)],
    );
    const capture = await captureHangDiagnostics(request(), host);
    expect(capture.text).toContain('grandchild (command name only: its arguments were not read in time)');
    expect(capture.text).not.toContain('identities or membership changed');
  });
  //#endregion smoo/cowshed!n/hang-snapshot-tree

  it('reports no matches only after successful resource inspection and stable identities', async () => {
    const { host } = scriptedHost([[root]], [exited('', 1)]);
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
    const { host, calls } = scriptedHost([[root]], [denied, exited(fileObservation)]);
    const capture = await captureHangDiagnostics(request(), host);
    expect(capture.outcome).toBe('incomplete');
    expect(capture.text).toContain('sudo: permission denied');
    expect(capture.text).toContain('ordinary-user visibility only');
    expect(capture.text).toContain('/fixture/held-file');
    expect(calls[1]?.file).toBe('lsof');
  });

  it('retains partial failed output instead of replacing it with an empty fallback', async () => {
    const partial: HangProbeResult = {
      status: 'failed',
      detail: 'probe output exceeded bound; output is incomplete',
      stdout: fileObservation,
      stderr: 'read failed',
    };
    const { host } = scriptedHost([[root]], [partial, exited('', 1)]);
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
    const warned = scriptedHost([[root]], [exited(fileObservation, 0, fallback), exited('', 1)]);
    const capture = await captureHangDiagnostics(request(), warned.host);
    expect(capture.outcome, capture.text).toBe('holders');
    expect(capture.text).toContain("can't stat() apfs file system /private/tmp/hang-capture-repro/dmg.Gqndhm");
    expect(capture.text).toContain('omits no descriptor of the selected processes');
    expect(warned.calls.map((call) => call.file)).toEqual(['sudo', 'sudo']);
    const empty = scriptedHost([[root]], [exited('', 1, fallback)]);
    expect((await captureHangDiagnostics(request(), empty.host)).outcome).toBe('none');
    for (const stderr of [`${fallback}lsof: WARNING: other\n`, 'lsof: WARNING: other\n']) {
      const { host } = scriptedHost([[root]], [exited(fileObservation, 0, stderr)]);
      const other = await captureHangDiagnostics(request(), host);
      expect(other.outcome, other.text).toBe('incomplete');
      expect(other.text).toContain('lsof: WARNING: other');
    }
  });

  it('names a lock wait for a file the run holds and a process outside the run also holds', async () => {
    // Pid 303 is the run's own grandchild, so its row in the holder census is not an outside holder.
    const outside = 'p303\ncchild\nf7\nn/fixture/held-file\np505\ncperl\nf3\nn/fixture/held-file\n';
    const { host, calls } = scriptedHost([HOST], [exited(fileObservation), exited(outside)]);
    const capture = await captureHangDiagnostics(request(), host);
    expect(capture.outcome, capture.text).toBe('holders');
    expect(capture.lockWaits).toEqual([
      { path: '/fixture/held-file', waiters: [303], holders: [{ pid: 505, command: 'perl' }] },
    ]);
    expect(capture.text).toContain(
      'Lock wait (open-handle evidence, not a kernel lock owner): /fixture/held-file open in the run by pid 303 and outside it by pid 505 (perl)',
    );
    expect(calls[1]?.args).toEqual(['-n', 'lsof', '-nP', '-F', 'pcn', '--', '/fixture/held-file']);
  });

  it('stack-samples a named lock waiter that the leaf samples left out', async () => {
    // Pid 202 has a child, so no leaf sample takes its stack, yet it is the run's process the lock wait names: its
    // stack is what says whether it blocks in the lock call at all.
    const opened =
      'COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME\nchild 202 tester 7r REG 1,2 4 123 /fixture/held-file\n';
    const outside = 'p202\ncchild\nf7\nn/fixture/held-file\np505\ncperl\nf3\nn/fixture/held-file\n';
    const { host, samples } = scriptedHost([HOST], [exited(opened), exited(outside)]);
    const capture = await captureHangDiagnostics(request(), host);
    expect(capture.lockWaits.map((wait) => wait.waiters)).toEqual([[202]]);
    expect(samples.map((sample) => sample.pid).sort((left, right) => left - right)).toEqual([ROOT, 202, 303, 404]);
    expect(capture.text).toContain('stack sample pid 202 (1s; binary image list omitted)');
  });
  it('names no lock wait for a file the run holds only through an unnumbered descriptor', async () => {
    const library =
      'COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME\nchild 303 tester txt REG 1,2 4 123 /fixture/library\n';
    const { host, calls } = scriptedHost([HOST], [exited(library)]);
    const capture = await captureHangDiagnostics(request(), host);
    expect(capture.outcome).toBe('holders');
    expect(capture.lockWaits).toEqual([]);
    expect(calls.map((call) => call.file)).toEqual(['sudo']);
  });

  it('marks the capture incomplete, with no lock wait, when the holder census does not answer', async () => {
    const holders: HangProbeResult[] = [
      { status: 'timed-out', detail: 'probe exceeded 1500ms', stdout: '', stderr: '' },
      exited('p505\ncperl\nf3\nn/fixture/held-file\n', 0, 'lsof: WARNING: other\n'),
      exited('n/fixture/held-file\n'),
      exited('p505\ncperl\nq3\n'),
    ];
    for (const holder of holders) {
      const { host } = scriptedHost([HOST], [exited(fileObservation), holder]);
      const capture = await captureHangDiagnostics(request(), host);
      expect(capture.outcome, capture.text).toBe('incomplete');
      expect(capture.lockWaits).toEqual([]);
      expect(capture.text).not.toContain('Lock wait (open-handle');
    }
  });

  it('does not turn an exited root, reused pid, or changed descendant membership into no holders', async () => {
    const absent = scriptedHost([[unrelated]]);
    expect((await captureHangDiagnostics(request(), absent.host)).outcome).toBe('incomplete');
    expect(absent.calls).toHaveLength(0);
    for (const after of [[{ ...root, startedUs: STARTED_US + 4_000_000 }], [root, child]]) {
      const { host } = scriptedHost([[root], after], [exited('', 1)]);
      const capture = await captureHangDiagnostics(request(), host);
      expect(capture.outcome).toBe('incomplete');
      expect(capture.text).toContain('identities or membership changed');
    }
  });

  it('names a process tree procinfo cannot read, and a probe that throws, without throwing', async () => {
    const base = scriptedHost([HOST]).host;
    for (const processes of [
      {
        ...base.processes,
        childPids: () => {
          throw refused('cannot read the children of process 202: Operation not permitted');
        },
      },
      {
        ...base.processes,
        processLineages: () => {
          throw refused('cannot read process 101: Operation not permitted');
        },
      },
    ] satisfies HangProcessReader[]) {
      const capture = await captureHangDiagnostics(request(), { ...base, processes });
      expect(capture.outcome).toBe('incomplete');
      expect(capture.text).toContain('process tree before: could not read the process tree: cannot read');
    }
    const throwing: HangDiagnosticHost = {
      ...scriptedHost([[root]]).host,
      async probe() {
        throw new Error('probe rejected');
      },
    };
    expect((await captureHangDiagnostics(request(), throwing)).text).toContain('probe rejected');
  });

  //#region smoo/cowshed!n/awake-idle-bound
  it('lets an unanswered probe keep its remaining diagnostic budget on wake, then aborts it at expiry', async () => {
    let calls = 0;
    const signals: AbortSignal[] = [];
    const unresponsive: HangDiagnosticHost = {
      ...scriptedHost([[root]]).host,
      probe(_file, _args, _timeoutMs, captureSignal) {
        calls += 1;
        signals.push(captureSignal);
        // An unanswered process boundary; the capture must not rely on its cooperation.
        return new Promise<HangProbeResult>(() => {});
      },
    };
    jest.useFakeTimers();
    let awake = 0;
    try {
      const limit = 1_000;
      const capture = captureHangDiagnostics(
        { ...request(), awakeExpiresAtMs: limit, awakeNow: () => awake },
        unresponsive,
      );
      jest.advanceTimersByTime(limit); // Only the timer advances during sleep.
      expect(signals.some((signal) => signal.aborted)).toBe(false);
      awake = limit - 1;
      jest.advanceTimersByTime(limit - 1);
      expect(signals.some((signal) => signal.aborted)).toBe(false);
      awake = limit;
      jest.advanceTimersByTime(1);
      const result = await capture;
      expect(result.outcome).toBe('incomplete');
      expect(result.text).toContain('1000ms deadline reached during lsof privileged');
      expect(signals.every((signal) => signal.aborted)).toBe(true);
      // The open-file census and the runner's stack sample, started together; nothing after them.
      expect(calls).toBe(2);
    } finally {
      jest.useRealTimers();
    }
  });
  //#endregion smoo/cowshed!n/awake-idle-bound

  it('cancels a pending probe when its progress generation ends without starting another', async () => {
    const controller = new AbortController();
    let calls = 0;
    const seen: AbortSignal[] = [];
    const pending: HangDiagnosticHost = {
      ...scriptedHost([[root]]).host,
      probe(_file, _args, _timeoutMs, signal) {
        calls += 1;
        seen.push(signal);
        return new Promise<HangProbeResult>(() => {});
      },
    };
    const collecting = captureHangDiagnostics({ ...request(), signal: controller.signal }, pending);
    expect(calls).toBe(2);
    controller.abort();
    const capture = await collecting;
    expect(capture.outcome).toBe('incomplete');
    expect(capture.text).toContain('Capture cancelled during lsof privileged');
    expect(seen.every((signal) => signal.aborted)).toBe(true);
    expect(calls).toBe(2);
  });

  it('starts no probe and reads no process for a generation already cancelled', async () => {
    const controller = new AbortController();
    controller.abort();
    const { host, calls, reads } = scriptedHost([HOST]);
    const capture = await captureHangDiagnostics({ ...request(), signal: controller.signal }, host);
    expect(capture.outcome).toBe('incomplete');
    expect(capture.text).toContain('Capture cancelled');
    expect(calls).toHaveLength(0);
    expect(reads()).toBe(0);
  });

  it('starts no process probe and reads no process after the pre-expiry window or with no remaining budget', async () => {
    for (const bounds of [{ awakeExpiresAtMs: performance.now() - 1 }, { awakeExpiresAtMs: performance.now() }]) {
      const { host, calls, reads } = scriptedHost([HOST]);
      const capture = await captureHangDiagnostics({ ...request(), ...bounds }, host);
      expect(capture.outcome).toBe('incomplete');
      expect(capture.text).toContain('no probe started');
      expect(calls).toEqual([]);
      expect(reads()).toBe(0);
    }
  });

  it('keeps process-only visibility explicit on platforms without the Darwin open-file probe', async () => {
    const { host, calls, samples } = scriptedHost([[root]]);
    const capture = await captureHangDiagnostics(request(), { ...host, platform: 'linux' });
    expect(capture.outcome).toBe('holders');
    expect(capture.text).toContain('observations below name processes only');
    expect(capture.text).toContain('Stack samples not provided on linux');
    expect(samples).toEqual([]);
    expect(calls).toEqual([]);
  });

  it('names the process whose CPU time rose between the two readings, and classifies the expiry as a build in the test', async () => {
    const { host } = scriptedHost([compileTree(41_000), compileTree(48_000)], [exited('', 1)]);
    const capture = await captureHangDiagnostics(request(), host);
    expect(capture.cpuRising).toEqual([{ pid: 303, command: 'rustc --crate-name hang_fixture', gainedMs: 7_000 }]);
    expect(capture.text).toContain(`Classification: ${COMPILE_CLASS}`);
    expect(capture.text).toContain('a rise is named from 0.5 s of CPU');
    expect(capture.text).toContain('pid 303 (rustc --crate-name hang_fixture) gained 7.0 s of CPU');
    expect(capture.text).not.toContain('pid 202 (child waiting) gained');
    expect(capture.text).not.toContain('pid 101 (runner --test selected) gained');
  });

  it('names a process that gained 0.5 s, and classifies nothing when none did', async () => {
    const named = scriptedHost([compileTree(3_630), compileTree(4_130)], [exited('', 1)]);
    const rose = await captureHangDiagnostics(request(), named.host);
    expect(rose.cpuRising).toEqual([{ pid: 303, command: 'rustc --crate-name hang_fixture', gainedMs: 500 }]);
    const idle = scriptedHost([compileTree(3_630), compileTree(4_129)], [exited('', 1)]);
    const capture = await captureHangDiagnostics(request(), idle.host);
    expect(capture.cpuRising).toEqual([]);
    expect(capture.text).not.toContain(COMPILE_CLASS);
    expect(capture.text).toContain('no process of the tree gained 0.5 s of CPU');
  });

  it('says a CPU time it could not read rather than counting that process idle', async () => {
    const { host } = scriptedHost([compileTree(41_000), compileTree(null)], [exited('', 1)]);
    const capture = await captureHangDiagnostics(request(), host);
    expect(capture.cpuRising).toEqual([]);
    expect(capture.text).toContain('CPU time not read for pid 303');
  });

  it('classifies a build in the test on a platform without stack samples, after one pause between the readings', async () => {
    const { host, pauses } = scriptedHost([compileTree(41_000), compileTree(48_000)]);
    const capture = await captureHangDiagnostics(request(), { ...host, platform: 'linux' });
    // The rise is seen only because the closing reading came after the one pause, where the compiler ran.
    expect(capture.cpuRising.map((rise) => rise.pid)).toEqual([303]);
    expect(capture.text).toContain(COMPILE_CLASS);
    expect(pauses).toHaveLength(1);
    expect(pauses[0]).toBeGreaterThan(0);
    expect(pauses[0]).toBeLessThanOrEqual(HANG_CAPTURE_MS - 1_000);
  });

  it('ends a capture whose CPU interval outlasts its window at the window, naming the interval', async () => {
    jest.useFakeTimers();
    try {
      const requested: AbortSignal[] = [];
      let sleeping!: () => void;
      const asleep = new Promise<void>((resolve) => {
        sleeping = resolve;
      });
      const { host } = scriptedHost([compileTree(41_000)]);
      const waiting: HangDiagnosticHost = {
        ...host,
        platform: 'linux',
        sleep: (_ms, signal) => {
          requested.push(signal);
          sleeping();
          // Held off by construction: only the capture's own deadline can end it.
          return new Promise<void>(() => {});
        },
      };
      const capturing = captureHangDiagnostics({ ...request(), awakeExpiresAtMs: performance.now() + 1_100 }, waiting);
      await asleep;
      jest.advanceTimersByTime(1_100);
      const capture = await capturing;
      expect(capture.outcome).toBe('incomplete');
      expect(capture.cpuRising).toEqual([]);
      expect(capture.text).toContain('1100ms deadline reached during CPU interval');
      expect(requested).toHaveLength(1);
      expect(requested[0]?.aborted).toBe(true);
    } finally {
      jest.useRealTimers();
    }
  });

  it('samples the runner and its leaf processes beside the open-file census, and reads the tree again after them', async () => {
    const answers: ((result: HangProbeResult) => void)[] = [];
    let allStarted!: () => void;
    const started = new Promise<void>((resolve) => {
      allStarted = resolve;
    });
    const { host, calls, samples } = scriptedHost(
      [HOST],
      [exited(fileObservation), exited('', 1)],
      () =>
        new Promise<HangProbeResult>((resolve) => {
          answers.push(resolve);
          if (answers.length === 3) allStarted();
        }),
    );
    const capturing = captureHangDiagnostics(request(), host);
    // Every stack sample has started while none has answered, beside the open-file census; the closing reading waits.
    await started;
    expect(samples.map((sample) => sample.pid)).toEqual([ROOT, 303, 404]);
    expect(calls[0]?.args).toEqual(['-n', 'lsof', '-nP', '-p', '101,202,303,404']);
    for (const answer of answers) answer(exited(SAMPLE_REPORT));
    const capture = await capturing;
    expect(capture.outcome, capture.text).toBe('holders');
    expect(calls.map((call) => call.file)).toEqual(['sudo', 'sudo']);
    expect(samples[1]?.args).toEqual(['-n', 'sample', '303', '1', '-file', '/dev/stdout']);
    // Each sample may run until the closing reading has its one second left of the capture window.
    for (const sample of samples) expect(sample.timeoutMs).toBeLessThanOrEqual(HANG_CAPTURE_MS - 1_000);
    expect(capture.text.indexOf('stack sample pid 303 (1s; binary image list omitted):')).toBeLessThan(
      capture.text.indexOf('process tree after: PID'),
    );
    expect(capture.text).toContain('1000 read  (in libsystem_kernel.dylib) + 8');
    expect(capture.text).not.toContain('Binary Images:');
    expect(capture.text).not.toContain('/fixture/runner');
  });

  it('bounds stack samples to the runner and four live leaf processes, and names the leaves it left out', async () => {
    const workers = [601, 602, 603, 604, 605, 606].map(
      (pid): FakeProcess => ({ pid, ppid: ROOT, command: `worker ${pid}` }),
    );
    const zombie: FakeProcess = { pid: 607, ppid: ROOT, state: { state: 'zombie' }, command: '(worker)' };
    const { host, samples } = scriptedHost([[root, ...workers, zombie]], [exited('', 1)]);
    const capture = await captureHangDiagnostics(request(), host);
    expect(capture.outcome, capture.text).toBe('none');
    expect(samples.map((sample) => sample.pid)).toEqual([ROOT, 601, 602, 603, 604]);
    expect(capture.text).toContain('not sampled: pid 605, 606\n');
  });

  it('traces the file-system calls of a process in an uninterruptible kernel wait beside its stack', async () => {
    const stuck: FakeProcess = {
      pid: 303,
      ppid: 202,
      state: { state: 'uninterruptible' },
      command: 'grandchild stuck in the kernel',
    };
    const { host, traces } = scriptedHost([[root, child, stuck]], [exited('', 1)]);
    const capture = await captureHangDiagnostics(request(), host);
    expect(traces.map((trace) => trace.args)).toEqual([['-n', 'fs_usage', '-w', '-t', '5', '-f', 'filesys', '303']]);
    for (const trace of traces) expect(trace.timeoutMs).toBeLessThanOrEqual(HANG_CAPTURE_MS - 1_000);
    expect(capture.text).toContain('fs_usage pid 303 (5s of file-system calls):');
    expect(capture.text).toContain('/fixture/store/image.dmg');
  });

  it('records a stack sample sudo refuses as no stack, keeping the holder outcome and the closing reading', async () => {
    const refusedSample = exited('', 1, 'sudo: a password is required\n');
    const { host, calls } = scriptedHost([[root]], [exited('', 1)], () => refusedSample);
    const capture = await captureHangDiagnostics(request(), host);
    expect(capture.outcome, capture.text).toBe('none');
    expect(calls.map((call) => call.file)).toEqual(['sudo']);
    expect(capture.text).toContain('stack sample pid 101 stderr:\nsudo: a password is required');
    expect(capture.text).toContain('stack sample pid 101: no stack;');
    expect(capture.text).toContain('process tree after: PID');
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
      // clock is stopped while the real `lsof` runs: each ends when it exits; no timer cuts it short.
      jest.useFakeTimers();
      const capture = await captureHangDiagnostics({ ...request(), pid: child.pid, pgid: null }, realProbes).finally(
        () => jest.useRealTimers(),
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
      const capture = await captureHangDiagnostics({ ...request(), pid: run.pid, pgid: null }, realProbes).finally(() =>
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
