import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { type ClockOracleKind, findClockOracles, formatClockOracle, TEST_CLOCK_ORACLE } from './test-clock-policy.js';
import { diffScope } from './test-walk-policy.js';

const lines = (...source: string[]): string => `${source.join('\n')}\n`;
// Assembled so this file's own cases carry no runner flag a scan of it would read as a bound.
const flag = ['--time', 'out'].join('');

describe('test-clock-policy', () => {
  // One walk: each planted site and each control is a step over one fixture directory, then the diff scope.
  it('refuses runner and test-owned deadlines, elapsed-time asserts and clocked bounds, passes the controls, and scopes to changed cases', () => {
    const root = mkdtempSync(join(tmpdir(), 'test-clock-policy-'));
    const write = (file: string, source: string): void => {
      mkdirSync(dirname(join(root, file)), { recursive: true });
      writeFileSync(join(root, file), source);
    };
    const kinds = (file: string) => findClockOracles(root, [file]).map((oracle) => oracle.kind);
    const git = (...args: string[]): string => {
      const result = spawnSync(
        'git',
        ['-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', ...args],
        {
          cwd: root,
          encoding: 'utf8',
          env: { ...process.env, GIT_CONFIG_GLOBAL: '/dev/null', GIT_CONFIG_NOSYSTEM: '1' },
        },
      );
      if (result.status !== 0) throw new Error(`git ${args.join(' ')}: ${result.stderr}`);
      return result.stdout;
    };
    try {
      const planted: readonly [ClockOracleKind, string, string][] = [
        ['runner-deadline', 'tests/a.test.ts', "test('slow', async () => { await child.exited; }, 60_000);"],
        [
          'runner-deadline',
          'tests/a.test.ts',
          "test('slow', async () => { await child.exited; }, { timeout: 60_000 });",
        ],
        ['runner-deadline', 'tests/a.test.ts', "test.each([1])('n %d', async (n) => { await run(n); }, 30_000);"],
        ['runner-deadline', 'tests/a.test.ts', 'setDefaultTimeout(60_000);'],
        [
          'clock-delta',
          'tests/a.test.ts',
          "test('fast', async () => { const started = Date.now(); await run(); expect(Date.now() - started).toBeLessThan(5_000); });",
        ],
        [
          'clock-delta',
          'tests/a.test.ts',
          "test('fast', () => { const t = performance.now(); work(); const elapsedMs = performance.now() - t; expect(elapsedMs < 1_000).toBe(true); });",
        ],
        [
          'clock-delta',
          'tests/a.test.ts',
          "test('deadline', () => { const deadline = Date.now() + 1_000; work(); expect(Date.now()).toBeLessThanOrEqual(deadline); });",
        ],
        [
          'clock-delta',
          'tests/a.test.ts',
          "test('close', () => { expect(Math.abs(stamp() - Date.now())).not.toBeGreaterThan(1); });",
        ],
        [
          'clock-delta',
          'tests/quick.rs',
          lines(
            '#[test]',
            'fn quick() {',
            '    let started = Instant::now();',
            '    work();',
            '    assert!(started.elapsed() < Duration::from_secs(10), "slow");',
            '}',
          ),
        ],
        [
          'clock-delta',
          'src/quick.rs',
          lines(
            'fn work() {}',
            '#[cfg(test)]',
            'mod tests {',
            '    #[test]',
            '    fn quick() {',
            '        let started = Instant::now();',
            '        let elapsed = started.elapsed();',
            '        assert!(Duration::from_millis(500) > elapsed);',
            '    }',
            '}',
          ),
        ],
        [
          'clocked-bound',
          'tests/a.test.ts',
          `test('times out', async () => { const result = await runBoundedExec({ command: 'node -e "setTimeout(() => {}, 5000)"', timeoutMs: 50 }); expect(result.success).toBe(false); });`,
        ],
        [
          'clocked-bound',
          'tests/a.test.ts',
          lines(
            "const SLOW = `test('slow', async () => { await new Promise((r) => setTimeout(r, 5000)); });`;",
            `test('per-test bound', async () => { expect(await verdictOf('bun test ${flag}=100 ./slow.test.ts', { 'slow.test.ts': SLOW })).toEqual({ outcome: 'bound' }); });`,
          ),
        ],
        [
          'clocked-bound',
          'tests/a.test.ts',
          "test('idle', async () => { await run({ argv: ['sleep', '5'], idleTimeoutSeconds: 1 }); });",
        ],
        [
          'clocked-bound',
          'tests/a.test.ts',
          "test('short stand-in', async () => { const path = await fakeChild('exec sleep 30'); await openService({ path, lspQueryTimeoutMs: 1_000 }); });",
        ],
        [
          'clocked-bound',
          'tests/bound.rs',
          lines(
            '#[test]',
            'fn bound() {',
            '    let outcome = run(Command::new("sleep").arg("5"), Options { timeout_ms: 50 });',
            '    assert!(outcome.timed_out);',
            '}',
          ),
        ],
        [
          'test-deadline',
          'tests/deadline.rs',
          lines(
            '#[test]',
            'fn event() {',
            '    let (tx, rx) = channel();',
            '    spawn(tx);',
            '    rx.recv_timeout(Duration::from_secs(10)).expect("the event");',
            '}',
          ),
        ],
        [
          'test-deadline',
          'tests/deadline.rs',
          lines(
            '#[tokio::test]',
            'async fn event() {',
            '    tokio::time::timeout(Duration::from_millis(500), served()).await.expect("served");',
            '}',
          ),
        ],
      ];
      for (const [kind, file, source] of planted) {
        write(file, source);
        expect([file, source, kinds(file)]).toEqual([file, source, [kind]]);
        rmSync(join(root, file));
      }
      write('tests/a.test.ts', planted[4]?.[2] ?? '');
      const diagnostic = findClockOracles(root, ['tests/a.test.ts']).map(formatClockOracle)[0] ?? '';
      for (const part of [
        `error[${TEST_CLOCK_ORACLE.id}]: ${TEST_CLOCK_ORACLE.message['clock-delta']}`,
        '  --> tests/a.test.ts:1:',
        '  = help: Elapsed time on a loaded host',
      ])
        expect(diagnostic).toContain(part);
      rmSync(join(root, 'tests/a.test.ts'));

      const controls: readonly [string, string][] = [
        [
          'tests/c.test.ts',
          "test('injected clock', () => { const timer = new MemoryTimer(); const started = timer.now(); timer.advance(30); expect(timer.now() - started).toBeLessThan(31); });",
        ],
        [
          'tests/c.test.ts',
          `test('exit decides', async () => { const run = await onProgressClock(() => false, 0, () => verdictOf('node -e ""', { timeoutMs: 5_000 })); expect(run.result).toEqual({ outcome: 'passed' }); });`,
        ],
        [
          'tests/c.test.ts',
          `test('subject held off by construction', async () => { const result = await runBoundedExec({ command: 'node -e "process.stdin.resume()"', timeoutMs: 50 }); expect(result.terminalOutput).toContain('timeoutMs=50'); });`,
        ],
        [
          'tests/c.test.ts',
          `test('bounds that cannot decide', async () => { await runBoundedExec({ command: 'node -e "setTimeout(() => {}, 400)"', idleTimeoutMs: 0, timeoutMs: 180_000 }); });`,
        ],
        [
          'tests/c.test.ts',
          "test('waited at least', async () => { const started = performance.now(); await lock.acquired; expect(performance.now() - started).toBeGreaterThanOrEqual(100); });",
        ],
        [
          'tests/c.test.ts',
          "test('stamped after', () => { const before = Date.now(); const record = make(); expect(record.at).toBeGreaterThanOrEqual(before); expect(before).toBeLessThanOrEqual(record.at); });",
        ],
        ['tests/c.test.ts', "test('cpu time', () => { expect(cpuTime.total).toBeLessThanOrEqual(100_000); });"],
        [
          'tests/c.test.ts',
          "test('stand-in outlasts the leg', async () => { const path = await fakeChild('exec sleep 3600'); await openService({ path, lspQueryTimeoutMs: 1_000 }); });",
        ],
        [
          'tests/c.test.ts',
          lines(
            'const elapsed = Date.now() - t0;',
            "test('shadowed', () => { const elapsed = timer.elapsed(); expect(elapsed).toBeLessThan(5); });",
          ),
        ],
        [
          'tests/c.rs',
          lines(
            '#[test]',
            'fn waited() {',
            '    let started = Instant::now();',
            '    acquire();',
            '    assert!(started.elapsed() >= Duration::from_millis(100));',
            '    let clock = ManualClock::new();',
            '    clock.advance(Duration::from_millis(30));',
            '    assert!(clock.elapsed() < Duration::from_millis(31));',
            '}',
          ),
        ],
        [
          'tests/c.rs',
          lines(
            '#[test]',
            'fn stamped() {',
            '    let wall_before = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros();',
            '    let since_us = start();',
            '    assert!(since_us >= wall_before);',
            '    let config = format!(r#"timeoutMs = 1; cmd = "sleep 5""#);',
            '}',
          ),
        ],
        [
          'tests/c.rs',
          lines(
            '#[tokio::test]',
            'async fn quiet() {',
            '    assert!(timeout(Duration::from_millis(50), rx.recv()).await.is_err());',
            '    timeout(NO_PROGRESS, served()).await.expect("served");',
            '}',
          ),
        ],
        [
          'tests/c.rs',
          lines(
            '#[tokio::test(start_paused = true)]',
            'async fn virtual_time() {',
            '    let deadline = Instant::now() + Duration::from_secs(5);',
            '    advance().await;',
            '    assert!(Instant::now() < deadline);',
            '    timeout(Duration::from_millis(500), served()).await.expect("served");',
            '}',
          ),
        ],
      ];
      for (const [file, source] of controls) {
        write(file, source);
        expect([source, kinds(file)]).toEqual([source, []]);
        rmSync(join(root, file));
      }

      // Diff scope: an existing elapsed bound in the upstream refuses nothing until its own case is edited, and then the
      // whole case is examined; the bin judges the same changes.
      const legacy = [
        "test('old elapsed bound', async () => {",
        '  const started = Date.now();',
        '  await run();',
        '  expect(Date.now() - started).toBeLessThan(5_000);',
        '});',
        "test('event case', async () => {",
        '  await child.exited;',
        '});',
      ];
      write('tests/legacy.test.ts', lines(...legacy));
      git('init', '--quiet', '-b', 'main');
      git('add', '.');
      git('commit', '--quiet', '-m', 'base');
      git('checkout', '--quiet', '-b', 'topic');
      const base = git('rev-parse', 'HEAD').trim();
      const judged = () => {
        const scope = diffScope(root, base);
        if (!scope.ok) throw new Error(scope.reason);
        return findClockOracles(root, scope.value.files, scope.value.changes).map((oracle) => [
          oracle.kind,
          oracle.line,
        ]);
      };
      expect(judged()).toEqual([]);
      write('tests/legacy.test.ts', lines(...legacy.slice(0, 6), '  await child.closed;', ...legacy.slice(7)));
      expect(judged()).toEqual([]);
      write(
        'tests/legacy.test.ts',
        lines(...legacy.slice(0, 2), '  await run({ verbose: true });', ...legacy.slice(3)),
      );
      expect(judged()).toEqual([['clock-delta', 4]]);
      const scope = diffScope(root, base);
      expect(
        (scope.ok ? findClockOracles(root, scope.value.files, scope.value.changes) : [])
          .map(formatClockOracle)
          .map((each) => each.split('\n').slice(0, 2)),
      ).toEqual([
        [
          `error[${TEST_CLOCK_ORACLE.id}]: ${TEST_CLOCK_ORACLE.message['clock-delta']}: expect(Date.now() - started).toBeLessThan(5_000)`,
          '  --> tests/legacy.test.ts:4:3',
        ],
      ]);
      write('tests/new.test.ts', planted[0]?.[2] ?? '');
      expect(judged()).toEqual([
        ['clock-delta', 4],
        ['runner-deadline', 1],
      ]);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
