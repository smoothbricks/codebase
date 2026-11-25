import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const cli = join(import.meta.dir, '../../dist/bin/smoo-nx-suite-minimum.js');

function runCli(args: readonly string[], cwd: string, runtime: 'node' | 'bun' = 'node') {
  const argv = runtime === 'bun' ? ['--config=/dev/null', cli, ...args] : [cli, ...args];
  return spawnSync(runtime === 'bun' ? process.execPath : 'node', argv, { cwd, encoding: 'utf8' });
}

describe('smoo-nx-suite-minimum', () => {
  it('computes the whole-suite minimum from JUnit XML and --walls-json, with and without --aging-ms', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'smoo-nx-suite-minimum-'));
    try {
      const missing = join(dir, 'missing.xml');
      const junit = join(dir, 'junit.xml');
      const perCaseSec = (1079 / 955).toFixed(6);
      const cases = Array.from({ length: 955 }, (_, i) => `<testcase name="t${i}" time="${perCaseSec}"/>`).join('');
      await writeFile(junit, `<testsuites><testsuite name="s">${cases}</testsuite></testsuites>`, 'utf8');

      // Missing candidate (ENOENT) is skipped; without --aging-ms liveness alone needs 1 thread.
      const livenessOnly = runCli(['--want', '18', missing, junit], dir);
      expect(livenessOnly.status, livenessOnly.stderr).toBe(0);
      expect(livenessOnly.stdout.trim()).toBe('1');

      // With --aging-ms 30000, keeping the LPT wall within 30 s of the 18-thread wall requires 12 threads.
      const withAging = runCli(['--want', '18', '--aging-ms', '30000', missing, junit], dir);
      expect(withAging.status, withAging.stderr).toBe(0);
      expect(withAging.stdout.trim()).toBe('12');

      // Also runs cleanly on plain bun without preload.
      const onPlainBun = runCli(['--want=18', '--aging-ms=30000', missing, junit], dir, 'bun');
      expect(onPlainBun.status, onPlainBun.stderr).toBe(0);
      expect(onPlainBun.stdout.trim()).toBe('12');

      // --walls-json with explicit --idle-ms and --aging-ms.
      const viaWallsJson = runCli(
        [
          '--want',
          '18',
          '--idle-ms',
          '180000',
          '--aging-ms',
          '30000',
          '--walls-json',
          JSON.stringify(new Array<number>(955).fill(1_079_000 / 955)),
        ],
        dir,
      );
      expect(viaWallsJson.status, viaWallsJson.stderr).toBe(0);
      expect(viaWallsJson.stdout.trim()).toBe('12');

      // ENOENT-only candidates fall back to 1 (no minimum).
      const emptyFallback = runCli(['--want', '18', missing], dir);
      expect(emptyFallback.status, emptyFallback.stderr).toBe(0);
      expect(emptyFallback.stdout.trim()).toBe('1');

      // Leading-dash candidate path after `--` and CDATA/comments/negative times handled via verdict.ts junitCaseTimes.
      const oddPath = join(dir, '-odd.xml');
      await writeFile(
        oddPath,
        [
          '<testsuites><testsuite name="s">',
          '  <testcase name="long" time="200"><system-out><![CDATA[<testcase name="fake" time="999"/>]]></system-out></testcase>',
          '  <!-- <testcase name="commented" time="999"/> -->',
          '  <testcase name="short1" time="50"/>',
          '  <testcase name="short2" time="50"/>',
          '  <testcase name="neg" time="-5"/>',
          '  <testcase name="untimed"/>',
          '</testsuite></testsuites>',
        ].join('\n'),
        'utf8',
      );
      const afterDashDash = runCli(['--want', '18', '--idle-ms', '180000', '--', '-odd.xml'], dir);
      expect(afterDashDash.status, afterDashDash.stderr).toBe(0);
      expect(afterDashDash.stdout.trim()).toBe('2');
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  it('exits non-zero naming the path on non-ENOENT read errors', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'smoo-nx-suite-minimum-err-'));
    try {
      const directoryPath = join(dir, 'not-a-file');
      await mkdir(directoryPath);
      const result = runCli(['--want', '18', '--aging-ms', '30000', directoryPath], dir);
      expect(result.status).not.toBe(0);
      expect(result.stderr).toContain(directoryPath);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  it('rejects --bound-ms, invalid --aging-ms, invalid --idle-ms, invalid --want, and malformed flags', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'smoo-nx-suite-minimum-flags-'));
    try {
      const cases: readonly (readonly [readonly string[], string])[] = [
        [['--want', '18', '--bound-ms', '60000'], '--bound-ms'],
        [['--want', '18', '--bound-ms=60000'], '--bound-ms'],
        [['--want', '18', '--aging-ms', '-1'], '--aging-ms'],
        [['--want', '18', '--aging-ms', 'abc'], '--aging-ms'],
        [['--want', '18', '--aging-ms', 'NaN'], '--aging-ms'],
        [['--want', '18', '--aging-ms', 'Infinity'], '--aging-ms'],
        [['--want', '18', '--aging-ms='], '--aging-ms'],
        [['--want', '18', '--aging-ms'], '--aging-ms'],
        [['--want', '18', '--idle-ms', '-1'], '--idle-ms'],
        [['--want', '18', '--idle-ms', 'nope'], '--idle-ms'],
        [['--want', '18', '--idle-ms'], '--idle-ms'],
        [[], '--want'],
        [['--want', '0'], '--want'],
        [['--want', '1.5'], '--want'],
        [['--want', 'abc'], '--want'],
        [['--want', '18', '--walls-json', 'not-json'], '--walls-json'],
        [['--want', '18', '--walls-json', '[1,-2]'], '--walls-json'],
        [['--want', '18', '--walls-json', '[1]', 'a.xml'], '--walls-json'],
        [['--want', '18', '--unknown'], '--unknown'],
      ];
      for (const [args, expectedMention] of cases) {
        const result = runCli(args, dir);
        expect(result.status, `args=${JSON.stringify(args)} stdout=${result.stdout}`).toBe(2);
        expect(result.stderr, `args=${JSON.stringify(args)}`).toContain(expectedMention);
      }
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });
});
