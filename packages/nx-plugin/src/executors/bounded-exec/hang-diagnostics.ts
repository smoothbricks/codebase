import { spawn } from 'node:child_process';
import type { ProcessArguments, RunState } from '@smoothbricks/procinfo';
import * as procinfo from '@smoothbricks/procinfo';
import { describeTeardown, killProcessGroupNow, TEARDOWN_PATIENCE } from '../../process-group.js';
import { atClockDeadline } from './awake-clock.js';
import { CPU_RISE_MS, CPU_RISING_CLASS, type CpuRise, type CpuSample, cpuChange, describeCpuRise } from './cpu-rise.js';
import { describeLockWait, type LockWait, lockCandidates, lockWaits, parseLsofTable } from './lock-wait.js';

/**
 * The pre-expiry capture window. Stack samples set it: under load averages of 27-52 on 18 cores one `sudo -n sample`
 * took 1.1-4.6 s, and in a five-second window two of eight real captures lost their sample at its limit.
 */
export const HANG_CAPTURE_MS = 10_000;
const MAX_PROBE_BYTES = 4 * 1024 * 1024;
/**
 * The least time between the two readings of the tree, which are the snapshot's two CPU samples. A process is named
 * after gaining 0.5 s (CPU_RISE_MS), so five seconds show a process at 10% of a core. On Darwin the stack samples have
 * usually spent most of it; elsewhere, or when sudo refuses them, the snapshot pauses, inside its window and early
 * enough that the closing reading keeps its reserve.
 */
const CPU_INTERVAL_MS = 5_000;
/**
 * Stack samples: `sample` suspends a process every millisecond for one second and reports its threads' call graph. The
 * runner and at most this many leaf processes are sampled, all at once, and then at most this many holders of a named lock wait
 * those left out. `-mayDie` is not passed: it reads symbols before sampling and measured about a second slower under
 * load, and a process that exits mid-sample is a raced capture all the same.
 */
const MAX_SAMPLED_LEAVES = 4;
const SAMPLE_SECONDS = 1;
/** How long a process in an uninterruptible kernel wait has its file-system calls traced. */
const FS_USAGE_SECONDS = 5;
/**
 * What the stack samples leave of the window for the closing reading of the tree and the verdict: they run until the
 * window's end is this near.
 */
const READING_RESERVE_MS = 1_000;
/**
 * lsof's mount-table fallback (darwin libproc dmnt.c, readmnt()): a mount it cannot stat(), such as one detaching under a
 * concurrent shed, is kept with its mount-table device. Descriptors come per process from libproc, so this warning omits
 * none of the selected processes' rows.
 */
const LSOF_MOUNT_FALLBACK =
  /^lsof: WARNING: can't stat\(\) \S+ file system .*\n {6}Output information may be incomplete\.\n {6}assuming "dev=[0-9a-f]+" from mount table\n/gm;

/** The lsof stderr that makes its census incomplete: everything except its mount-table fallback. */
function lsofFaults(stderr: string): string {
  return stderr.replace(LSOF_MOUNT_FALLBACK, '').trim();
}

/** A `sample` report up to its binary-image list: its header and call graph name the stacks; the load map is bulk. */
function callGraph(report: string): string {
  const end = report.indexOf('\nBinary Images:');
  return (end === -1 ? report : report.slice(0, end)).trim();
}

export interface HangDiagnostics {
  startedAtMs: number;
  capturedAtMs: number;
  /** Positive when the observation completed before the idle expiry. */
  offsetFromExpiryMs: number;
  outcome: 'holders' | 'none' | 'incomplete';
  /** Lock waits the snapshot names: empty unless the capture is complete, and always empty without the open-file census. */
  lockWaits: readonly LockWait[];
  /**
   * The processes of the tree whose CPU time rose between the two readings, largest gain first: empty when none did, and
   * when the closing reading did not complete. A build inside the test, never a restart of the idle bound.
   */
  cpuRising: readonly CpuRise[];
  text: string;
}

export interface HangCaptureRequest {
  pid: number;
  pgid: number | null;
  awakeExpiresAtMs: number;
  awakeNow(): number;
  signal?: AbortSignal;
}

export type HangProbeResult =
  | { status: 'exited'; code: number; stdout: string; stderr: string }
  | { status: 'unavailable' | 'timed-out' | 'failed'; detail: string; stdout: string; stderr: string };

/** The process boundary, injectable so unavailable and non-answering probes have exact witnesses. */
export interface HangDiagnosticHost {
  platform: NodeJS.Platform;
  probe(file: string, args: readonly string[], timeoutMs: number, signal: AbortSignal): Promise<HangProbeResult>;
  /** Waits `ms`, or until `signal` aborts: the capture's end ends its own CPU interval. */
  sleep(ms: number, signal: AbortSignal): Promise<void>;
  /** The runner's tree is read through it, one process at a time, never through a `ps` census of the process table. */
  processes: HangProcessReader;
}

/**
 * The process facts the snapshot reads, each from the kernel's own record of one process or one group: procinfo's own
 * exports (specs/procinfo/01_procinfo.md), which a test fakes at this boundary. A `ps` census of the whole process table
 * ran past its one-second limit under load averages of 40-90 on 18 cores and left wedged runs with no tree at all.
 */
export type HangProcessReader = Pick<
  typeof procinfo,
  'childPids' | 'cpuTimeNs' | 'processArguments' | 'processGroups' | 'processLineages' | 'processRunState' | 'startedUs'
>;

/** One process of the runner's tree, as procinfo read it. */
interface ObservedProcess extends CpuSample {
  readonly ppid: number;
  readonly pgid: number;
  readonly state: RunState;
  /** Why a fact of it was not read, each said in its row. */
  readonly unread: readonly string[];
}

function sleepUnlessAborted(ms: number, signal: AbortSignal, now: () => number): Promise<void> {
  return new Promise((resolve) => {
    const done = (): void => {
      cancelTimer();
      signal.removeEventListener('abort', done);
      resolve();
    };
    const cancelTimer = atClockDeadline(now() + ms, now, done);
    signal.addEventListener('abort', done, { once: true });
    if (signal.aborted) done();
  });
}

/** A pre-expiry observation, never an assertion that an open file is a contested lock. */
export function captureHangDiagnostics(
  request: HangCaptureRequest,
  source: HangDiagnosticHost = {
    platform: process.platform,
    probe: (file, args, timeoutMs, signal) => runProbe(file, args, timeoutMs, signal, undefined, request.awakeNow),
    sleep: (ms, signal) => sleepUnlessAborted(ms, signal, request.awakeNow),
    processes: procinfo,
  },
): Promise<HangDiagnostics> {
  const startedAtMs = Date.now();
  const { awakeNow, awakeExpiresAtMs } = request;
  const startedAwake = awakeNow();
  const endsAtMs = Math.min(startedAwake + HANG_CAPTURE_MS, awakeExpiresAtMs);
  const budgetMs = Math.max(0, Math.min(HANG_CAPTURE_MS, awakeExpiresAtMs - startedAwake));
  const controller = new AbortController();
  const lines = [
    `Hang snapshot: pid=${request.pid} pgid=${request.pgid ?? 'unknown'} startedAtMs=${startedAtMs}`,
    'Open files and process observations are evidence, not proof of a contested lock or its owner.',
  ];
  const active = new Set<string>();
  const during = (): string => (active.size === 0 ? 'the process tree reads' : [...active].join(', '));
  let finished = false;
  let waits: readonly LockWait[] = [];
  let rising: readonly CpuRise[] = [];
  // This package targets lib es2022, which does not declare Promise.withResolvers.
  return new Promise((resolve) => {
    const finish = (outcome: HangDiagnostics['outcome']): void => {
      if (finished) return;
      finished = true;
      cancelTimer();
      request.signal?.removeEventListener('abort', cancelled);
      controller.abort();
      const capturedAtMs = Date.now();
      const offsetFromExpiryMs = awakeExpiresAtMs - awakeNow();
      resolve({
        startedAtMs,
        capturedAtMs,
        offsetFromExpiryMs,
        outcome,
        lockWaits: outcome === 'incomplete' ? [] : waits,
        cpuRising: rising,
        text: `${lines.join('\n')}\nHang snapshot outcome=${outcome} capturedAtMs=${capturedAtMs} offsetFromExpiryMs=${offsetFromExpiryMs}\n`,
      });
    };
    const cancelTimer = atClockDeadline(endsAtMs, awakeNow, () => {
      lines.push(
        `Capture incomplete: ${Math.ceil(budgetMs)}ms deadline reached during ${during()}; no empty-holder conclusion.`,
      );
      finish('incomplete');
    });
    const cancelled = (): void => {
      if (finished) return;
      lines.push(`Capture cancelled during ${during()}; its progress generation is no longer current.`);
      finish('incomplete');
    };
    request.signal?.addEventListener('abort', cancelled, { once: true });
    if (request.signal?.aborted) {
      cancelled();
      return;
    }
    if (budgetMs === 0) {
      lines.push('Capture incomplete: the pre-expiry window already ended or has no budget; no probe started.');
      finish('incomplete');
      return;
    }
    const probe = async (name: string, file: string, args: readonly string[], limitMs: number) => {
      active.add(name);
      lines.push(`${name}: ${file} ${args.join(' ')}`);
      const result = await source.probe(
        file,
        args,
        Math.max(0, Math.min(limitMs, endsAtMs - awakeNow())),
        controller.signal,
      );
      active.delete(name);
      if (!finished) {
        lines.push(
          `${name}: ${result.status}${result.status === 'exited' ? ` code=${result.code}` : ` ${result.detail}`}`,
        );
        if (result.stderr.trim() !== '') lines.push(`${name} stderr:\n${result.stderr.trimEnd()}`);
      }
      return result;
    };
    /** The runner's tree, read now; null, said in the snapshot, when it cannot be read or holds no runner. */
    const census = (name: string): ObservedProcess[] | null => {
      let tree: ObservedProcess[];
      try {
        tree = readProcessTree(source.processes, request.pid, request.pgid);
      } catch (error) {
        lines.push(`${name}: could not read the process tree: ${errorText(error)}; not an empty process tree.`);
        return null;
      }
      const nowUs = Date.now() * 1_000;
      lines.push(`${name}: PID PPID PGID STATE ELAPSED CPU STARTED COMMAND`);
      lines.push(...tree.map((entry) => processRow(entry, nowUs)));
      if (!tree.some((entry) => entry.pid === request.pid)) {
        lines.push(`${name}: root pid ${request.pid} exited or is not visible; capture raced or is incomplete.`);
        return null;
      }
      return tree;
    };
    /** The runner and up to MAX_SAMPLED_LEAVES leaf processes of `tree` (members with no live child), and the leaves past that bound. */
    const leafSamples = (tree: readonly ObservedProcess[]): { sampled: number[]; omitted: number[] } => {
      const live = tree.filter((entry) => entry.state.state !== 'zombie');
      const parents = new Set(live.map((entry) => entry.ppid));
      const leaves = live.filter((entry) => entry.pid !== request.pid && !parents.has(entry.pid));
      return {
        sampled: [request.pid, ...leaves.slice(0, MAX_SAMPLED_LEAVES).map((entry) => entry.pid)],
        omitted: leaves.slice(MAX_SAMPLED_LEAVES).map((entry) => entry.pid),
      };
    };
    /**
     * One one-second call stack of `pid`, until the closing reading's reserve is all the window has left: a stack
     * names the call a process waits in, which the open-file census cannot. A sample sudo refuses, or one that does
     * not answer, is named as no stack for that process; stacks never change the holder outcome.
     */
    const sampleStack = async (pid: number): Promise<void> => {
      const name = `stack sample pid ${pid}`;
      // `-file /dev/stdout` writes the report to the probe's pipe alone, not also to a root-owned file in /tmp.
      const args = ['-n', 'sample', String(pid), String(SAMPLE_SECONDS), '-file', '/dev/stdout'];
      const result = await probe(name, 'sudo', args, endsAtMs - awakeNow() - READING_RESERVE_MS);
      if (finished) return;
      const report = callGraph(result.stdout);
      if (result.status === 'exited' && result.code === 0 && report !== '') {
        lines.push(`${name} (${SAMPLE_SECONDS}s; binary image list omitted):\n${report}`);
        return;
      }
      if (report !== '') lines.push(`Partial ${name} output:\n${report}`);
      lines.push(
        `${name}: no stack; sudo -n sample did not answer, so the call this process waits in is not observed.`,
      );
    };
    /** The runner's and its tree's leaf processes' stacks, taken at once. */
    const stackSamples = async (tree: readonly ObservedProcess[]): Promise<void> => {
      const { sampled, omitted } = leafSamples(tree);
      if (omitted.length > 0)
        lines.push(
          `Stack samples bounded to the runner and ${MAX_SAMPLED_LEAVES} leaf processes; not sampled: pid ${omitted.join(', ')}`,
        );
      await Promise.all(sampled.map(sampleStack));
    };
    /**
     * A process in an uninterruptible kernel wait (state U) is in a file-system or device call no signal ends, and its
     * stack stops at the system call. Its file-system calls over a few seconds (fs_usage) name the volume and files it
     * waits on, which tells a fixture doing its I/O on a contended shared volume from one serialised behind one APFS
     * operation. Bounded like the stack samples; one fs_usage sudo refuses is named as no trace.
     */
    const kernelWaits = async (tree: readonly ObservedProcess[]): Promise<void> => {
      const waiting = tree.filter((entry) => entry.state.state === 'uninterruptible').slice(0, MAX_SAMPLED_LEAVES);
      const limitMs = endsAtMs - awakeNow() - READING_RESERVE_MS;
      await Promise.all(
        waiting.map(async ({ pid }) => {
          const name = `fs_usage pid ${pid}`;
          const args = ['-n', 'fs_usage', '-w', '-t', String(FS_USAGE_SECONDS), '-f', 'filesys', String(pid)];
          const result = await probe(name, 'sudo', args, limitMs);
          if (finished) return;
          if (result.stdout.trim() !== '') {
            lines.push(`${name} (${FS_USAGE_SECONDS}s of file-system calls):\n${result.stdout.trimEnd()}`);
            return;
          }
          lines.push(
            `${name}: no trace; sudo -n fs_usage gave none, so the files this process waits on are not observed.`,
          );
        }),
      );
    };
    const collect = async (): Promise<void> => {
      if (request.pgid !== null && source.processes.processGroups === null) {
        lines.push(
          `Process group ${request.pgid} is not listed on ${source.platform}: procinfo lists no group there, so a member that left the runner's tree is not reached.`,
        );
      }
      const before = census('process tree before');
      if (before === null) return finish('incomplete');
      const beforeAtMs = awakeNow();
      let incomplete = false;
      let observed = before.length > 0;
      if (source.platform === 'darwin') {
        const openFiles = async (): Promise<void> => {
          const args = ['-nP', '-p', before.map((entry) => entry.pid).join(',')];
          // Bare `sudo`/`lsof` through PATH, like `ps` above: NixOS provides neither /usr/bin/sudo nor /usr/sbin/lsof.
          let resources = await probe('lsof privileged', 'sudo', ['-n', 'lsof', ...args], 1_500);
          if (finished) return;
          const answered = (result: HangProbeResult): boolean =>
            result.status === 'exited' &&
            (result.code === 0 || (result.code === 1 && lsofFaults(result.stderr) === ''));
          if (!answered(resources)) {
            incomplete = true;
            if (resources.stdout.trim() !== '')
              lines.push(`Partial privileged lsof output:\n${resources.stdout.trimEnd()}`);
            lines.push(
              'Privileged resource census unavailable; fallback is ordinary-user visibility only, not proof of no holders.',
            );
            resources = await probe('lsof user-scoped fallback', 'lsof', args, 1_000);
            if (finished) return;
          }
          if (resources.stdout.trim() !== '')
            lines.push(`lsof open files/sockets (not contested-lock proof):\n${resources.stdout.trimEnd()}`);
          if (!answered(resources) || lsofFaults(resources.stderr) !== '') incomplete = true;
          else if (resources.stderr.trim() !== '')
            lines.push(
              'lsof warned only that it could not stat() the named mounts and kept their mount-table devices; that omits no descriptor of the selected processes.',
            );
          observed = resources.stdout.trim() !== '';
          if (!observed && answered(resources))
            lines.push('lsof reported no open-file matches within its stated visibility.');
          // A lock wait is read from the tree's own open files: a lockable file two or more of its processes hold. Who
          // else on the host holds a file is never looked up: only a census of every process answers that, and a
          // process stalled in the kernel holds such a census with it. An incomplete census names no wait.
          const rows = incomplete ? [] : parseLsofTable(resources.stdout);
          waits = lockWaits(rows);
          const shared = new Set(waits.map((wait) => wait.path));
          const alone = lockCandidates(rows).filter((path) => !shared.has(path));
          if (alone.length > 0)
            lines.push(
              `Lockable files one process of the runner's tree holds; a holder outside the tree is not looked up: ${alone.join(', ')}`,
            );
          // A lock wait is open-handle evidence: only a holder's own stack says whether it blocks in the lock call.
          // Each holder the leaf samples left out is sampled the same way, inside the same window.
          const stacked = new Set(leafSamples(before).sampled);
          const holders = [...new Set(waits.flatMap((wait) => wait.holders))].filter((pid) => !stacked.has(pid));
          await Promise.all(holders.slice(0, MAX_SAMPLED_LEAVES).map(sampleStack));
        };
        await Promise.all([openFiles(), stackSamples(before), kernelWaits(before)]);
        if (finished) return;
      } else {
        lines.push(`Open-file census not provided on ${source.platform}; observations below name processes only.`);
        lines.push(`Stack samples not provided on ${source.platform}; no blocking call is observed.`);
      }
      const settleMs = Math.min(beforeAtMs + CPU_INTERVAL_MS, endsAtMs - READING_RESERVE_MS) - awakeNow();
      if (settleMs > 0) {
        active.add('CPU interval');
        await source.sleep(settleMs, controller.signal);
        active.delete('CPU interval');
        if (finished) return;
      }
      const after = census('process tree after');
      if (after === null) return finish('incomplete');
      const afterAtMs = awakeNow();
      const zombie = (entry: ObservedProcess): boolean => entry.state.state === 'zombie';
      if (
        before.length !== after.length ||
        before.some(
          (entry) =>
            !after.some(
              (other) =>
                other.pid === entry.pid &&
                other.ppid === entry.ppid &&
                other.pgid === entry.pgid &&
                other.startedUs === entry.startedUs &&
                zombie(other) === zombie(entry),
            ),
        )
      ) {
        incomplete = true;
        lines.push('Process identities or membership changed during capture; open-file census raced.');
      }
      // A zombie runs nothing: its CPU time is no longer read, and it is no idle process left unread.
      const live = (tree: readonly ObservedProcess[]) => tree.filter((entry) => !zombie(entry));
      const change = cpuChange(live(before), live(after));
      rising = change.rising;
      const intervalMs = afterAtMs - beforeAtMs;
      const bar = `${CPU_RISE_MS / 1_000} s`;
      if (change.unread.length > 0) {
        lines.push(
          `CPU time not read for pid ${change.unread.join(', ')}: a reading gave no CPU time or start for them, so they are not counted idle.`,
        );
      }
      if (rising.length > 0) {
        lines.push(
          `CPU rose in the tree over about ${intervalMs}ms between the readings (a rise is named from ${bar} of CPU):`,
          ...rising.map((rise) => `  ${describeCpuRise(rise)}`),
          `Classification: ${CPU_RISING_CLASS}`,
        );
      } else if (change.compared === 0) {
        lines.push('CPU: no process of the tree had a CPU time read in both readings, so none was compared.');
      } else {
        lines.push(`CPU: no process of the tree gained ${bar} of CPU over about ${intervalMs}ms between the readings.`);
      }
      if (!incomplete) {
        for (const wait of waits) {
          lines.push(
            `Lock wait (open-handle evidence among the runner's tree, not a kernel lock owner): ${describeLockWait(wait)}`,
          );
        }
      }
      finish(incomplete ? 'incomplete' : observed ? 'holders' : 'none');
    };
    void collect().catch((error: unknown) => {
      if (finished) return;
      lines.push(`Capture incomplete during ${during()}: ${error instanceof Error ? error.message : String(error)}`);
      finish('incomplete');
    });
  });
}

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * A process's argument vector as one command line, its control characters (a script's newlines) each a space so it
 * stays on its row, else the executable the kernel records for it. A vector procinfo could not read in time is the
 * command name alone, and says so.
 */
function commandLine(read: ProcessArguments): string {
  const text = new TextDecoder();
  const argv = read.argv.map((argument) => text.decode(argument));
  const line = (argv.length > 0 ? argv.join(' ') : text.decode(read.executable)).replace(/\p{Cc}/gu, ' ');
  return read.nameOnly ? `${line} (command name only: its arguments were not read in time)` : line;
}

/**
 * The runner's tree as the kernel links it now: the runner, the members of its retained process group where the kernel
 * lists a group (Darwin; Linux lists one to no one, specs/procinfo/01_procinfo.md), and every descendant of either, each
 * read for itself by pid, smallest pid first. A process reaped since it was listed is left out. A lineage, a group or a
 * child list that cannot be read throws, so the tree is unread, never smaller; a start, a CPU time or arguments that
 * cannot be read leave that fact unread and say why.
 */
function readProcessTree(reader: HangProcessReader, root: number, pgid: number | null): ObservedProcess[] {
  const groups = reader.processGroups;
  const pending = [root, ...(pgid === null || groups === null ? [] : groups.pids(pgid))];
  const seen = new Set<number>();
  const tree: ObservedProcess[] = [];
  for (let pid = pending.pop(); pid !== undefined; pid = pending.pop()) {
    if (seen.has(pid)) continue;
    seen.add(pid);
    const [lineage] = reader.processLineages([pid]);
    if (lineage === undefined) continue;
    const unread: string[] = [];
    const attempt = <T>(fact: string, read: () => T): T | null => {
      try {
        return read();
      } catch (error) {
        unread.push(`${fact}: ${errorText(error)}`);
        return null;
      }
    };
    const cpuNs = attempt('CPU time', () => reader.cpuTimeNs(pid));
    tree.push({
      pid,
      ppid: lineage.ppid,
      pgid: lineage.pgid,
      state: reader.processRunState(pid),
      startedUs: attempt('start', () => reader.startedUs(pid)),
      cpuMs: cpuNs === null ? null : Number(cpuNs / 1_000n) / 1_000,
      command: attempt('arguments', () => commandLine(reader.processArguments(pid))) ?? 'command unread',
      unread,
    });
    pending.push(...reader.childPids(pid));
  }
  return tree.sort((left, right) => left.pid - right.pid);
}

/** `ms` as ps prints an elapsed time: `[[D-]HH:]MM:SS`. */
function elapsedClock(ms: number): string {
  const seconds = Math.max(0, Math.floor(ms / 1_000));
  const two = (count: number): string => String(count).padStart(2, '0');
  const clock = `${two(Math.floor(seconds / 60) % 60)}:${two(seconds % 60)}`;
  const hours = Math.floor(seconds / 3_600) % 24;
  const days = Math.floor(seconds / 86_400);
  if (days > 0) return `${days}-${two(hours)}:${clock}`;
  return hours > 0 ? `${two(hours)}:${clock}` : clock;
}

/**
 * A process's row: pid, parent, group, state, how long it has run (now less its start), the CPU time it has used, its
 * start and its command, then each fact not read and why.
 */
function processRow(entry: ObservedProcess, nowUs: number): string {
  const elapsed = entry.startedUs === null ? '-' : elapsedClock((nowUs - entry.startedUs) / 1_000);
  const cpu = entry.cpuMs === null ? '-' : `${(entry.cpuMs / 1_000).toFixed(3)}s`;
  const started = entry.startedUs === null ? '-' : new Date(entry.startedUs / 1_000).toISOString();
  const state = entry.state.state === 'unread' ? `unread (${entry.state.reason})` : entry.state.state;
  const unread = entry.unread.length === 0 ? '' : ` [not read: ${entry.unread.join('; ')}]`;
  return `${entry.pid} ${entry.ppid} ${entry.pgid} ${state} ${elapsed} ${cpu} ${started} ${entry.command}${unread}`;
}

/**
 * Ends a probe at its limit without waiting for it, and says whether its group still lists members. An ordinary probe
 * runs as this user, so SIGKILL to its process group ends it and anything it started, and the kill is repeated while
 * the group lists members, since one forked as the signal goes out survives it. `sudo` keeps this user's real uid but
 * runs its command as root, which no signal from this user reaches: a group SIGKILL would end sudo alone and orphan
 * the command. Signalled itself, sudo relays SIGTERM to its command and exits only after it. While sudo starts it
 * holds root's real uid, and this throws EPERM. A probe is its caller's child and stays unreaped while the caller
 * waits here, and the kernel lists it as a member of its own group until then, so a group left listed is expected.
 */
export function stopProbe(file: string, pid: number): boolean {
  if (file === 'sudo') {
    process.kill(pid, 'SIGTERM');
    return false;
  }
  return killProcessGroupNow(pid, TEARDOWN_PATIENCE);
}

/** A probe owns its group and output pipes; its deadline never waits for an unresponsive child to reap. */
export function runProbe(
  file: string,
  args: readonly string[],
  timeoutMs: number,
  signal: AbortSignal,
  environment?: NodeJS.ProcessEnv,
  now: () => number = () => performance.now(),
): Promise<HangProbeResult> {
  // Keep the package's es2022-compatible event-completion convention.
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve({ status: 'timed-out', detail: 'capture deadline already reached', stdout: '', stderr: '' });
      return;
    }
    const child = spawn(file, [...args], { detached: true, stdio: ['ignore', 'pipe', 'pipe'], env: environment });
    let stdout = '';
    let stderr = '';
    let bytes = 0;
    let settled = false;
    const finish = (result: HangProbeResult): void => {
      if (settled) return;
      settled = true;
      cancelTimer();
      signal.removeEventListener('abort', aborted);
      child.stdout.destroy();
      child.stderr.destroy();
      child.unref();
      resolve(result);
    };
    const stop = (detail: string, status: 'timed-out' | 'failed' = 'timed-out'): void => {
      if (settled) return;
      let killFailure = '';
      if (child.pid !== undefined) {
        try {
          if (stopProbe(file, child.pid)) {
            killFailure = `; probe group cleanup: ${describeTeardown({ command: file, group: child.pid })}`;
          }
        } catch (error) {
          killFailure = `; probe ${file === 'sudo' ? 'stop through sudo' : 'group cleanup'}: ${error instanceof Error ? error.message : String(error)}`;
        }
      }
      finish({ status, detail: detail + killFailure, stdout, stderr });
    };
    const aborted = (): void => stop('capture deadline reached');
    const cancelTimer = atClockDeadline(now() + timeoutMs, now, () => stop(`probe exceeded ${timeoutMs}ms`));
    signal.addEventListener('abort', aborted, { once: true });
    const append = (stream: 'stdout' | 'stderr', chunk: string): void => {
      bytes += Buffer.byteLength(chunk);
      if (bytes > MAX_PROBE_BYTES) {
        stop(`probe output exceeded ${MAX_PROBE_BYTES} bytes; output is incomplete`, 'failed');
        return;
      }
      if (stream === 'stdout') stdout += chunk;
      else stderr += chunk;
    };
    child.stdout.setEncoding('utf8').on('data', (chunk: string) => append('stdout', chunk));
    child.stderr.setEncoding('utf8').on('data', (chunk: string) => append('stderr', chunk));
    child.once('error', (error) => {
      finish({ status: 'unavailable', detail: error.message, stdout, stderr });
    });
    child.once('close', (code, killedBy) => {
      finish(
        code === null
          ? { status: 'failed', detail: `probe ended by ${killedBy ?? 'unknown signal'}`, stdout, stderr }
          : { status: 'exited', code, stdout, stderr },
      );
    });
  });
}
