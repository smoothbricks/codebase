import { spawn } from 'node:child_process';
import { describeTeardown, killProcessGroupNow, TEARDOWN_PATIENCE } from '../../process-group.js';
import { type ProcessEntry, withDescendants } from '../../testing.js';
import {
  CPU_RISING_CLASS,
  type CpuRise,
  type CpuTime,
  cpuChange,
  cpuRiseThresholdsMs,
  describeCpuRise,
  describeCpuThresholds,
  parseCpuTime,
} from './cpu-rise.js';
import {
  describeHolders,
  type LockWait,
  lockCandidates,
  lockWaits,
  parseLsofFields,
  parseLsofTable,
} from './lock-wait.js';

/**
 * The pre-expiry capture window. Stack samples set it: under load averages of 27-52 on 18 cores one `sudo -n sample`
 * took 1.1-4.6 s, and in a five-second window two of eight real captures lost their sample at its limit.
 */
export const HANG_CAPTURE_MS = 10_000;
const MAX_PROBE_BYTES = 4 * 1024 * 1024;
const PS_FIELDS = 'pid=,ppid=,pgid=,stat=,%cpu=,etime=,cputime=,lstart=,command=';
/**
 * The least time between the two process censuses, which are the snapshot's two CPU samples. A process is named
 * after gaining 2 s where ps counts whole seconds and 0.5 s where it counts centiseconds (cpuRiseThresholdMs), so five
 * seconds show a process at 40% or at 10% of a core. On Darwin the stack samples have usually spent most of it;
 * elsewhere, or when sudo refuses them, the snapshot pauses, inside its window and early enough that the closing
 * census keeps its own limit.
 */
const CPU_INTERVAL_MS = 5_000;
/**
 * Stack samples: `sample` suspends a process every millisecond for one second and reports its threads' call graph. The
 * runner and at most this many leaf processes are sampled, all at once. `-mayDie` is not passed: it reads symbols
 * before sampling and measured about a second slower under load, and a process that exits mid-sample is a raced
 * capture all the same.
 */
const MAX_SAMPLED_LEAVES = 4;
const SAMPLE_SECONDS = 1;
/** How long a process in an uninterruptible kernel wait has its file-system calls traced. */
const FS_USAGE_SECONDS = 5;
/**
 * A process census's limit. The stack samples run until the closing census is this far from the window's end.
 */
const CENSUS_LIMIT_MS = 1_000;
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
   * The processes of the tree whose CPU time rose between the two censuses, largest gain first: empty when none did, and
   * when the closing census did not complete. A build inside the test, never a restart of the idle bound.
   */
  cpuRising: readonly CpuRise[];
  text: string;
}

export interface HangCaptureRequest {
  pid: number;
  pgid: number | null;
  expiresAtMs: number;
  deadlineMs: number;
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
}

interface ObservedProcess extends ProcessEntry {
  pgid: number;
  started: string;
  cpu: CpuTime | null;
  raw: string;
}

const host: HangDiagnosticHost = { platform: process.platform, probe: runProbe, sleep: sleepUnlessAborted };

function sleepUnlessAborted(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const done = (): void => {
      clearTimeout(timer);
      signal.removeEventListener('abort', done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    signal.addEventListener('abort', done, { once: true });
    if (signal.aborted) done();
  });
}

/** A pre-expiry observation, never an assertion that an open file is a contested lock. */
export function captureHangDiagnostics(
  request: HangCaptureRequest,
  source: HangDiagnosticHost = host,
): Promise<HangDiagnostics> {
  const startedAtMs = Date.now();
  const budgetMs = Math.max(0, Math.min(HANG_CAPTURE_MS, request.deadlineMs, request.expiresAtMs - startedAtMs));
  const endsAtMs = startedAtMs + budgetMs;
  const controller = new AbortController();
  const lines = [
    `Hang snapshot: pid=${request.pid} pgid=${request.pgid ?? 'unknown'} startedAtMs=${startedAtMs}`,
    'Open files and process observations are evidence, not proof of a contested lock or its owner.',
  ];
  const active = new Set<string>();
  const during = (): string => (active.size === 0 ? 'process census' : [...active].join(', '));
  let finished = false;
  let waits: readonly LockWait[] = [];
  let rising: readonly CpuRise[] = [];
  // This package targets lib es2022, which does not declare Promise.withResolvers.
  return new Promise((resolve) => {
    const finish = (outcome: HangDiagnostics['outcome']): void => {
      if (finished) return;
      finished = true;
      clearTimeout(timer);
      request.signal?.removeEventListener('abort', cancelled);
      controller.abort();
      const capturedAtMs = Date.now();
      const offsetFromExpiryMs = request.expiresAtMs - capturedAtMs;
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
    const timer = setTimeout(() => {
      lines.push(`Capture incomplete: ${budgetMs}ms deadline reached during ${during()}; no empty-holder conclusion.`);
      finish('incomplete');
    }, budgetMs);
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
        Math.max(0, Math.min(limitMs, endsAtMs - Date.now())),
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
    const census = async (name: string): Promise<ObservedProcess[] | null> => {
      // Bare `ps`, resolved through PATH like testing.ts's processesOf: NixOS has no /bin/ps.
      const result = await probe(name, 'ps', ['-A', '-o', PS_FIELDS], CENSUS_LIMIT_MS);
      if (finished) return null;
      if (result.status !== 'exited' || result.code !== 0) {
        lines.push(`${name}: could not inspect the process table; not an empty process tree.`);
        return null;
      }
      const parsed: ObservedProcess[] = [];
      for (const row of result.stdout.split('\n').filter((line) => line.trim() !== '')) {
        const match =
          /^\s*(\d+)\s+(\d+)\s+(\d+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+\s+\S+\s+\d+\s+\d\d:\d\d:\d\d\s+\d{4})\s+(.*)$/.exec(
            row,
          );
        if (match === null) {
          lines.push(`${name}: could not decode ps row: ${row}`);
          return null;
        }
        const [, pid, ppid, pgid, stat, , , cputime, started, command] = match;
        if (
          pid === undefined ||
          ppid === undefined ||
          pgid === undefined ||
          stat === undefined ||
          cputime === undefined ||
          started === undefined ||
          command === undefined
        ) {
          lines.push(`${name}: incomplete ps row: ${row}`);
          return null;
        }
        parsed.push({
          pid: Number(pid),
          ppid: Number(ppid),
          pgid: Number(pgid),
          stat,
          cpu: parseCpuTime(cputime),
          started,
          command,
          raw: row,
        });
      }
      const roots = [request.pid, ...parsed.filter((entry) => entry.pgid === request.pgid).map((entry) => entry.pid)];
      const selected = new Set(withDescendants(parsed, roots).map((entry) => entry.pid));
      const tree = parsed.filter((entry) => selected.has(entry.pid));
      lines.push(`${name}: PID PPID PGID STAT %CPU ELAPSED CPUTIME STARTED COMMAND`);
      lines.push(...tree.map((entry) => entry.raw));
      if (!tree.some((entry) => entry.pid === request.pid)) {
        lines.push(`${name}: root pid ${request.pid} exited or is not visible; capture raced or is incomplete.`);
        return null;
      }
      return tree;
    };
    /**
     * One-second call stacks of the runner and of its tree's leaf processes, taken at once: a stack names the call a
     * process waits in, which the open-file census cannot. A sample sudo refuses, or one that does not answer, is named
     * as no stack for that process; stacks never change the holder outcome.
     */
    const stackSamples = async (tree: readonly ObservedProcess[]): Promise<void> => {
      const live = tree.filter((entry) => !entry.stat.startsWith('Z'));
      const parents = new Set(live.map((entry) => entry.ppid));
      const leaves = live.filter((entry) => entry.pid !== request.pid && !parents.has(entry.pid));
      const omitted = leaves.slice(MAX_SAMPLED_LEAVES).map((entry) => entry.pid);
      if (omitted.length > 0)
        lines.push(
          `Stack samples bounded to the runner and ${MAX_SAMPLED_LEAVES} leaf processes; not sampled: pid ${omitted.join(', ')}`,
        );
      const limitMs = endsAtMs - Date.now() - CENSUS_LIMIT_MS;
      const sampled = [request.pid, ...leaves.slice(0, MAX_SAMPLED_LEAVES).map((entry) => entry.pid)];
      await Promise.all(
        sampled.map(async (pid) => {
          const name = `stack sample pid ${pid}`;
          // `-file /dev/stdout` writes the report to the probe's pipe alone, not also to a root-owned file in /tmp.
          const args = ['-n', 'sample', String(pid), String(SAMPLE_SECONDS), '-file', '/dev/stdout'];
          const result = await probe(name, 'sudo', args, limitMs);
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
        }),
      );
    };
    /**
     * A process in an uninterruptible kernel wait (state U) is in a file-system or device call no signal ends, and its
     * stack stops at the system call. Its file-system calls over a few seconds (fs_usage) name the volume and files it
     * waits on, which tells a fixture doing its I/O on a contended shared volume from one serialised behind one APFS
     * operation. Bounded like the stack samples; one fs_usage sudo refuses is named as no trace.
     */
    const kernelWaits = async (tree: readonly ObservedProcess[]): Promise<void> => {
      const waiting = tree.filter((entry) => entry.stat.startsWith('U')).slice(0, MAX_SAMPLED_LEAVES);
      const limitMs = endsAtMs - Date.now() - CENSUS_LIMIT_MS;
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
      const before = await census('ps before');
      if (finished) return;
      if (before === null) return finish('incomplete');
      const beforeAtMs = Date.now();
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
          // A lock wait needs a complete census of the run's files: a regular file the run holds on a numbered descriptor
          // that another process also holds open. One privileged lsof names those holders; a probe that did not answer
          // makes the capture incomplete, never an empty holder set.
          const rows = incomplete ? [] : parseLsofTable(resources.stdout);
          const candidates = lockCandidates(rows);
          if (candidates.length > 0) {
            const holders = await probe(
              'lsof lock holders',
              'sudo',
              ['-n', 'lsof', '-nP', '-F', 'pcn', '--', ...candidates],
              1_500,
            );
            if (finished) return;
            const openers =
              answered(holders) && lsofFaults(holders.stderr) === '' ? parseLsofFields(holders.stdout) : null;
            if (openers === null) {
              incomplete = true;
              lines.push('Lock holder census incomplete; no lock wait is named.');
            } else {
              waits = lockWaits(rows, new Set(before.map((entry) => entry.pid)), openers);
            }
          }
        };
        await Promise.all([openFiles(), stackSamples(before), kernelWaits(before)]);
        if (finished) return;
      } else {
        lines.push(`Open-file census not provided on ${source.platform}; observations below name processes only.`);
        lines.push(`Stack samples not provided on ${source.platform}; no blocking call is observed.`);
      }
      const settleMs = Math.min(beforeAtMs + CPU_INTERVAL_MS, endsAtMs - CENSUS_LIMIT_MS) - Date.now();
      if (settleMs > 0) {
        active.add('CPU interval');
        await source.sleep(settleMs, controller.signal);
        active.delete('CPU interval');
        if (finished) return;
      }
      const after = await census('ps after');
      if (finished) return;
      if (after === null) return finish('incomplete');
      const afterAtMs = Date.now();
      if (
        before.length !== after.length ||
        before.some(
          (entry) =>
            !after.some(
              (other) =>
                other.pid === entry.pid &&
                other.ppid === entry.ppid &&
                other.pgid === entry.pgid &&
                other.started === entry.started &&
                other.command === entry.command &&
                other.stat.startsWith('Z') === entry.stat.startsWith('Z'),
            ),
        )
      ) {
        incomplete = true;
        lines.push('Process identities or membership changed during capture; open-file census raced.');
      }
      const change = cpuChange(before, after);
      const thresholdsMs = cpuRiseThresholdsMs(before, after);
      rising = change.rising;
      const intervalMs = afterAtMs - beforeAtMs;
      if (change.unread.length > 0) {
        lines.push(
          `CPU time not read for pid ${change.unread.join(', ')}: ps printed none that parses, so they are not counted idle.`,
        );
      }
      if (rising.length > 0) {
        lines.push(
          `CPU rose in the tree over about ${intervalMs}ms between the censuses (a rise is named from ${describeCpuThresholds(thresholdsMs)} of CPU):`,
          ...rising.map((rise) => `  ${describeCpuRise(rise)}`),
          `Classification: ${CPU_RISING_CLASS}`,
        );
      } else if (thresholdsMs.length === 0) {
        lines.push('CPU: no process of the tree had a CPU time ps gave in both censuses, so none was compared.');
      } else {
        lines.push(
          `CPU: no process of the tree gained ${describeCpuThresholds(thresholdsMs)} of CPU over about ${intervalMs}ms between the censuses.`,
        );
      }
      if (!incomplete) {
        for (const wait of waits) {
          lines.push(
            `Lock wait (open-handle evidence, not a kernel lock owner): ${wait.path} open in the run by pid ${wait.waiters.join(', ')} and outside it by ${describeHolders(wait.holders)}`,
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
      clearTimeout(timer);
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
    const timer = setTimeout(() => stop(`probe exceeded ${timeoutMs}ms`), timeoutMs);
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
