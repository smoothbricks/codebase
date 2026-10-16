import { spawn } from 'node:child_process';
import { type ProcessEntry, withDescendants } from '../../testing.js';

export const HANG_CAPTURE_MS = 5_000;
const MAX_PROBE_BYTES = 4 * 1024 * 1024;
const PS_FIELDS = 'pid=,ppid=,pgid=,stat=,%cpu=,etime=,lstart=,command=';

export interface HangDiagnostics {
  startedAtMs: number;
  capturedAtMs: number;
  /** Positive when the observation completed before the idle expiry. */
  offsetFromExpiryMs: number;
  outcome: 'holders' | 'none' | 'incomplete';
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
}

interface ObservedProcess extends ProcessEntry {
  pgid: number;
  started: string;
  raw: string;
}

const host: HangDiagnosticHost = { platform: process.platform, probe: runProbe };

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
  let active = 'process census';
  let finished = false;
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
        text: `${lines.join('\n')}\nHang snapshot outcome=${outcome} capturedAtMs=${capturedAtMs} offsetFromExpiryMs=${offsetFromExpiryMs}\n`,
      });
    };
    const timer = setTimeout(() => {
      lines.push(`Capture incomplete: ${budgetMs}ms deadline reached during ${active}; no empty-holder conclusion.`);
      finish('incomplete');
    }, budgetMs);
    const cancelled = (): void => {
      if (finished) return;
      lines.push(`Capture cancelled during ${active}; its progress generation is no longer current.`);
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
      active = name;
      lines.push(`${name}: ${file} ${args.join(' ')}`);
      const result = await source.probe(
        file,
        args,
        Math.max(0, Math.min(limitMs, endsAtMs - Date.now())),
        controller.signal,
      );
      if (!finished) {
        lines.push(
          `${name}: ${result.status}${result.status === 'exited' ? ` code=${result.code}` : ` ${result.detail}`}`,
        );
        if (result.stderr.trim() !== '') lines.push(`${name} stderr:\n${result.stderr.trimEnd()}`);
      }
      return result;
    };
    const census = async (name: string): Promise<ObservedProcess[] | null> => {
      // Bare `ps`, resolved through PATH like testing.ts's processTable: NixOS has no /bin/ps.
      const result = await probe(name, 'ps', ['-A', '-o', PS_FIELDS], 1_000);
      if (finished) return null;
      if (result.status !== 'exited' || result.code !== 0) {
        lines.push(`${name}: could not inspect the process table; not an empty process tree.`);
        return null;
      }
      const parsed: ObservedProcess[] = [];
      for (const row of result.stdout.split('\n').filter((line) => line.trim() !== '')) {
        const match =
          /^\s*(\d+)\s+(\d+)\s+(\d+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+\s+\S+\s+\d+\s+\d\d:\d\d:\d\d\s+\d{4})\s+(.*)$/.exec(
            row,
          );
        if (match === null) {
          lines.push(`${name}: could not decode ps row: ${row}`);
          return null;
        }
        const [, pid, ppid, pgid, stat, , , started, command] = match;
        if (
          pid === undefined ||
          ppid === undefined ||
          pgid === undefined ||
          stat === undefined ||
          started === undefined ||
          command === undefined
        ) {
          lines.push(`${name}: incomplete ps row: ${row}`);
          return null;
        }
        parsed.push({ pid: Number(pid), ppid: Number(ppid), pgid: Number(pgid), stat, started, command, raw: row });
      }
      const roots = [request.pid, ...parsed.filter((entry) => entry.pgid === request.pgid).map((entry) => entry.pid)];
      const selected = new Set(withDescendants(parsed, roots).map((entry) => entry.pid));
      const tree = parsed.filter((entry) => selected.has(entry.pid));
      lines.push(`${name}: PID PPID PGID STAT %CPU ELAPSED STARTED COMMAND`);
      lines.push(...tree.map((entry) => entry.raw));
      if (!tree.some((entry) => entry.pid === request.pid)) {
        lines.push(`${name}: root pid ${request.pid} exited or is not visible; capture raced or is incomplete.`);
        return null;
      }
      return tree;
    };
    const collect = async (): Promise<void> => {
      const before = await census('ps before');
      if (finished) return;
      if (before === null) return finish('incomplete');
      let incomplete = false;
      let observed = before.length > 0;
      if (source.platform === 'darwin') {
        const args = ['-nP', '-p', before.map((entry) => entry.pid).join(',')];
        let resources = await probe('lsof privileged', '/usr/bin/sudo', ['-n', '/usr/sbin/lsof', ...args], 1_500);
        if (finished) return;
        const answered = (result: HangProbeResult): boolean =>
          result.status === 'exited' && (result.code === 0 || (result.code === 1 && result.stderr.trim() === ''));
        if (!answered(resources)) {
          incomplete = true;
          if (resources.stdout.trim() !== '')
            lines.push(`Partial privileged lsof output:\n${resources.stdout.trimEnd()}`);
          lines.push(
            'Privileged resource census unavailable; fallback is ordinary-user visibility only, not proof of no holders.',
          );
          resources = await probe('lsof user-scoped fallback', '/usr/sbin/lsof', args, 1_000);
          if (finished) return;
        }
        if (resources.stdout.trim() !== '')
          lines.push(`lsof open files/sockets (not contested-lock proof):\n${resources.stdout.trimEnd()}`);
        if (!answered(resources) || resources.stderr.trim() !== '') incomplete = true;
        observed = resources.stdout.trim() !== '';
        if (!observed && answered(resources))
          lines.push('lsof reported no open-file matches within its stated visibility.');
      } else {
        lines.push(`Open-file census not provided on ${source.platform}; observations below name processes only.`);
      }
      const after = await census('ps after');
      if (finished) return;
      if (after === null) return finish('incomplete');
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
      finish(incomplete ? 'incomplete' : observed ? 'holders' : 'none');
    };
    void collect().catch((error: unknown) => {
      if (finished) return;
      lines.push(`Capture incomplete during ${active}: ${error instanceof Error ? error.message : String(error)}`);
      finish('incomplete');
    });
  });
}

/** A probe owns its group and output pipes; its deadline never waits for an unresponsive child to reap. */
function runProbe(
  file: string,
  args: readonly string[],
  timeoutMs: number,
  signal: AbortSignal,
): Promise<HangProbeResult> {
  // Keep the package's es2022-compatible event-completion convention.
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve({ status: 'timed-out', detail: 'capture deadline already reached', stdout: '', stderr: '' });
      return;
    }
    const child = spawn(file, [...args], { detached: true, stdio: ['ignore', 'pipe', 'pipe'] });
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
          process.kill(-child.pid, 'SIGKILL');
        } catch (error) {
          killFailure = `; probe group cleanup: ${error instanceof Error ? error.message : String(error)}`;
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
