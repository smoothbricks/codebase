import { describe, expect, it } from 'bun:test';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  captureHangDiagnostics,
  type HangCaptureRequest,
  type HangDiagnosticHost,
  type HangProbeResult,
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
  expiresAtMs: Date.now() + 5_000,
  deadlineMs: 5_000,
});

function scriptedHost(results: readonly HangProbeResult[]) {
  const calls: { file: string; args: readonly string[]; timeoutMs: number }[] = [];
  let index = 0;
  const host: HangDiagnosticHost = {
    platform: 'darwin',
    async probe(file, args, timeoutMs) {
      calls.push({ file, args, timeoutMs });
      const result = results[index++];
      if (result === undefined) throw new Error(`unexpected probe ${file}`);
      return result;
    },
  };
  return { host, calls };
}

describe('pre-expiry hang diagnostics', () => {
  it('keeps grandchildren and reparented group members, but not unrelated host rows', async () => {
    const { host, calls } = scriptedHost([exited(census), exited(fileObservation), exited(census)]);
    const capture = await captureHangDiagnostics(request(), host);
    expect(capture.outcome).toBe('holders');
    for (const row of [rootRow, childRow, grandchildRow, reparentedRow]) expect(capture.text).toContain(row);
    expect(capture.text).not.toContain(unrelatedRow);
    expect(capture.text).toContain('/fixture/held-file');
    expect(capture.text).toContain('not proof of a contested lock');
    expect(capture.text).toContain('pid=101 pgid=101');
    expect(calls.map((call) => call.file)).toEqual(['/bin/ps', '/usr/bin/sudo', '/bin/ps']);
    expect(calls[1]?.args).toEqual(['-n', '/usr/sbin/lsof', '-nP', '-p', '101,202,303,404']);
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
    expect(calls[2]?.file).toBe('/usr/sbin/lsof');
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
    const { host, calls } = scriptedHost([exited(rootRow), exited(rootRow)]);
    const capture = await captureHangDiagnostics(request(), { ...host, platform: 'linux' });
    expect(capture.outcome).toBe('holders');
    expect(capture.text).toContain('observations below name processes only');
    expect(calls.map((call) => call.file)).toEqual(['/bin/ps', '/bin/ps']);
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
      const reader = child.stdout
        .pipeThrough(new TextDecoderStream(), { signal: AbortSignal.timeout(10_000) })
        .getReader();
      let text = '';
      try {
        // Real OS-pipe readiness cannot advance a fake test clock; abort only bounds a missing line.
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
      const capture = await captureHangDiagnostics({ ...request(), pid: child.pid, pgid: null });
      expect(capture.outcome).toBe('holders');
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
});
