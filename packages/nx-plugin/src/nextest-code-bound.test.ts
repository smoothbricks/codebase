import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { chmodSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { codeTime, describeCodeTime, parseWaitRecord, type WaitEdge } from './nextest-code-bound.js';

const WRAPPER = fileURLToPath(new URL('./nextest-code-bound.ts', import.meta.url));

const start = (pid: number, seq: number, atUs: number): WaitEdge => ({
  kind: 'start',
  pid,
  seq,
  atUs,
  diskClass: 'storage',
});
const end = (pid: number, seq: number, atUs: number): WaitEdge => ({ kind: 'end', pid, seq, atUs });

describe('lease-wait record', () => {
  it('reads both edge shapes as clients write them and refuses anything else', () => {
    expect(parseWaitRecord('start 7 0 100 storage\nend 7 0 250\n')).toEqual({
      ok: true,
      edges: [start(7, 0, 100), end(7, 0, 250)],
    });
    expect(parseWaitRecord('')).toEqual({ ok: true, edges: [] });
    for (const bad of [
      'start 7 0 100',
      'end 7 0',
      'end 7 0 1 x',
      'begin 7 0 1',
      'end 7 -1 5',
      'start 7 0 1.5 storage',
    ]) {
      expect(parseWaitRecord(`${bad}\n`).ok).toBe(false);
    }
  });
});

describe('code time', () => {
  it('is the wall time less every wait', () => {
    const time = codeTime([start(1, 0, 1_000), end(1, 0, 3_000), start(1, 1, 5_000), end(1, 1, 6_000)], 0, 10_000);
    expect(time).toEqual({ wallUs: 10_000, waitUs: 3_000, codeUs: 7_000, waits: 2, open: [], strayEnds: 0 });
  });

  it('counts concurrent waits of several processes once, as their union', () => {
    const edges = [
      start(1, 0, 1_000),
      start(2, 0, 2_000),
      end(1, 0, 4_000),
      end(2, 0, 5_000),
      start(1, 1, 4_500),
      end(1, 1, 4_800),
    ];
    expect(codeTime(edges, 0, 10_000)).toMatchObject({ waitUs: 4_000, codeUs: 6_000, waits: 3 });
  });

  it('clips waits to the test run', () => {
    expect(codeTime([start(1, 0, 0), end(1, 0, 3_000)], 1_000, 5_000)).toMatchObject({ waitUs: 2_000, codeUs: 2_000 });
  });

  it('counts a wait that never ended to the end of the test and names it', () => {
    const time = codeTime([start(1, 0, 1_000), end(1, 0, 2_000), start(9, 3, 8_000)], 0, 10_000);
    expect(time).toMatchObject({ waitUs: 3_000, codeUs: 7_000, open: [{ pid: 9, seq: 3, startedUs: 8_000 }] });
    expect(describeCodeTime(time, 30_000_000)).toContain("pid 9's wait 3 never ended");
  });

  it('counts an end without a start as code time', () => {
    expect(codeTime([end(1, 0, 2_000)], 0, 10_000)).toMatchObject({ waitUs: 0, codeUs: 10_000, strayEnds: 1 });
  });

  it('says whether the code time is within its class', () => {
    const time = codeTime([start(1, 0, 0), end(1, 0, 20_000_000)], 0, 45_000_000);
    expect(describeCodeTime(time, 30_000_000)).toBe(
      'nextest-code-bound: code 25.0s of 30.0s, within its class (wall 45.0s less 20.0s waiting for the host disk lease over 1 waits)',
    );
    expect(describeCodeTime(time, 20_000_000)).toStartWith(
      'nextest-code-bound: code 25.0s of 20.0s, OVER its class (a warning)',
    );
  });
});

describe('nextest-code-bound wrapper', () => {
  /**
   * A stand-in test binary: it records `wait` seconds of lease wait in the record its wrapper names,
   * from a child process as a disk-lease client in the test's tree would, then works `work` seconds
   * and exits `status`.
   */
  const runWrapped = (bound: string, wait: number, work: number, status: number) => {
    const root = mkdtempSync(join(tmpdir(), 'nextest-code-bound-'));
    try {
      const binary = join(root, 'fake-test');
      writeFileSync(
        binary,
        [
          '#!/bin/sh',
          'sh -c \'echo "start $$ 0 $(perl -MTime::HiRes=time -e "printf q(%d), time() * 1000000") namespace" >> "$COWSHED_LEASE_WAIT_RECORD"; ' +
            `sleep ${wait}; ` +
            'echo "end $$ 0 $(perl -MTime::HiRes=time -e "printf q(%d), time() * 1000000")" >> "$COWSHED_LEASE_WAIT_RECORD"\'',
          `sleep ${work}`,
          'echo "argument $1"',
          `exit ${status}`,
        ].join('\n'),
      );
      chmodSync(binary, 0o755);
      // The wrapper makes its record in its TMPDIR: this case's own root, so the count below is this run's alone.
      const run = spawnSync(WRAPPER, [bound, binary, '--exact'], {
        encoding: 'utf8',
        env: { ...process.env, TMPDIR: root },
      });
      const recordsLeft = readdirSync(root).filter((name) => name.startsWith('cowshed-lease-waits-')).length;
      return { status: run.status, stdout: run.stdout, stderr: run.stderr, recordsLeft };
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  };

  // Real processes and real time, so the cases sleep as little as proves each verdict: 0.75s in all.
  it('passes a test whose wall time is over its class only by its lease waits', () => {
    const run = runWrapped('0.3', 0.4, 0, 0);
    expect(run.stdout).toContain('argument --exact');
    expect(run.stderr).toMatch(
      /nextest-code-bound: code 0\.\d+s of 0\.3s, within its class \(wall \d+\.\d+s less \d+\.\ds waiting/,
    );
    expect(run.status).toBe(0);
    expect(run.recordsLeft).toBe(0);
  });

  it('reports code-time verdicts without changing the test exit status', () => {
    for (const { label, bound, work, status, verdict } of [
      {
        label: 'warns of a passing test whose code time is over its class, and still passes it',
        bound: '0.3',
        work: 0.35,
        status: 0,
        verdict: 'OVER its class (a warning)',
      },
      {
        label: "keeps a failing test's own exit status and still reports its split",
        bound: '30',
        work: 0,
        status: 3,
        verdict: 'within its class',
      },
    ]) {
      const run = runWrapped(bound, 0, work, status);
      expect(run.stderr, label).toContain(verdict);
      expect(run.status, label).toBe(status);
    }
  });

  it('refuses to run without a class', () => {
    expect(spawnSync(WRAPPER, ['soon', '/bin/true'], { encoding: 'utf8' }).status).toBe(2);
  });
});
