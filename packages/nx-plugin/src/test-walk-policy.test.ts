import { describe, expect, it } from 'bun:test';
import { execFileSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { diffScope, findDuplicateScenarios, formatDuplicateScenario, TEST_ONE_WALK } from './test-walk-policy.js';

const lines = (...source: string[]): string => `${source.join('\n')}\n`;
const rust = (...body: string[]): string => lines('fn post() {}', '#[cfg(test)]', 'mod tests {', ...body, '}');

describe('test-walk-policy', () => {
  // One walk: each planted duplicate and each distinct-boundary control is a step over one fixture directory.
  it('refuses every duplicate shape in TypeScript and Rust, passes the controls, and scopes to changed cases', () => {
    const root = mkdtempSync(join(tmpdir(), 'test-walk-policy-'));
    const write = (file: string, source: string): void => {
      mkdirSync(dirname(join(root, file)), { recursive: true });
      writeFileSync(join(root, file), source);
    };
    const pairs = (file: string) =>
      findDuplicateScenarios(root, [file]).map((found) => [
        found.kind,
        found.first.name,
        found.second.name,
        found.shared,
      ]);
    const ts = 'tests/ledger.test.ts';
    const rs = 'src/ledger.rs';
    try {
      // The second case rebuilds the first one's ledger and posts the same entry, then only asserts more.
      write(
        ts,
        lines(
          "test('posts a sale', () => {",
          '  const ledger = openLedger(memoryLog());',
          "  ledger.post({ account: 'sales', amount: 50 });",
          "  expect(ledger.balance('sales')).toBe(50);",
          '});',
          "test('a sale lands in the journal', () => {",
          '  const book = openLedger(memoryLog());',
          "  book.post({ account: 'sales', amount: 50 });",
          '  expect(book.journal()).toHaveLength(1);',
          "  if (book.journal()[0] === undefined) throw new Error('no entry');",
          '});',
        ),
      );
      expect(pairs(ts)).toEqual([['same-path', 'posts a sale', 'a sale lands in the journal', 2]]);
      const diagnostic = findDuplicateScenarios(root, [ts]).map(formatDuplicateScenario)[0] ?? '';
      for (const part of [`error[${TEST_ONE_WALK.id}]`, `${ts}:6`, `${ts}:1`, '`posts a sale`', '2 shared', 'one walk'])
        expect(diagnostic).toContain(part);

      // Two cases that differ only in the posted and expected values are one table.
      write(
        ts,
        lines(
          "test('posts 50', () => {",
          '  const ledger = openLedger(memoryLog());',
          "  ledger.post({ account: 'sales', amount: 50 });",
          "  expect(ledger.balance('sales')).toBe(50);",
          '});',
          "test('posts 70', () => {",
          '  const ledger = openLedger(memoryLog());',
          "  ledger.post({ account: 'sales', amount: 70 });",
          "  expect(ledger.balance('sales')).toBe(70);",
          '});',
        ),
      );
      expect(pairs(ts)).toEqual([['literal-twin', 'posts 50', 'posts 70', 3]]);

      // Controls: the walk itself; one-statement table rows; a refusal whose path diverges after the shared setup;
      // a crash edge; a different input whose assertion shape differs.
      write(
        ts,
        lines(
          "test('one walk asserts between steps', () => {",
          '  const ledger = openLedger(memoryLog());',
          "  ledger.post({ account: 'sales', amount: 50 });",
          "  expect(ledger.balance('sales')).toBe(50);",
          "  ledger.post({ account: 'sales', amount: 20 });",
          "  expect(ledger.balance('sales')).toBe(70);",
          '});',
          "test('a closed period refuses a post', () => {",
          '  const ledger = openLedger(memoryLog());',
          "  ledger.close('2026-09');",
          "  expect(ledger.post({ account: 'sales', amount: 50 }).isErr()).toBe(true);",
          '});',
          "test('a reopened ledger replays its log', () => {",
          '  const log = memoryLog();',
          '  const ledger = openLedger(log);',
          "  ledger.post({ account: 'sales', amount: 50 });",
          '  ledger.crash();',
          "  expect(openLedger(log).balance('sales')).toBe(50);",
          '});',
          "test('parses a cent amount', () => expect(parseAmount('0.01')).toBe(1));",
          "test('parses a whole amount', () => expect(parseAmount('12')).toBe(1200));",
          "test('refuses an empty amount', () => {",
          "  const parsed = parseAmount('');",
          '  expect(parsed.isErr()).toBe(true);',
          '});',
          "test('keeps a negative amount', () => {",
          "  const parsed = parseAmount('-3');",
          '  expect(parsed).toBe(-300);',
          '});',
        ),
      );
      expect(pairs(ts)).toEqual([]);

      // Rust: a case walking a sibling's path again, and a literal twin.
      write(
        rs,
        rust(
          '    #[test]',
          '    fn reports_a_sale() {',
          '        let ledger = Ledger::from_entries(&[("sales", 50)]);',
          '        let report = ledger.report();',
          '        assert_eq!(report.balance("sales"), 50);',
          '    }',
          '    #[tokio::test]',
          '    async fn the_report_lists_the_sale() {',
          '        let book = Ledger::from_entries(&[("sales", 50)]);',
          '        let summary = book.report();',
          '        assert_eq!(summary.lines().len(), 1);',
          '        assert!(summary.lines().iter().all(|line| line.amount > 0));',
          '    }',
          '    #[test]',
          '    fn posts_70() {',
          '        let ledger = Ledger::from_entries(&[("sales", 50)]);',
          '        let report = ledger.report();',
          "        assert_eq!(report.balance('s'), 70); /* a char and a raw string are literals */",
          '    }',
        ),
      );
      expect(pairs(rs)).toEqual([
        ['same-path', 'reports_a_sale', 'the_report_lists_the_sale', 2],
        ['literal-twin', 'reports_a_sale', 'posts_70', 3],
      ]);

      // Rust: one expensive world, a different injected crash, then the same recovery and the same checks.
      write(
        rs,
        rust(
          '    #[test]',
          '    fn adopt_resumes_after_a_kill_between_clone_and_attach() {',
          '        let world = KillWindow::new("cloned").expect("kill window");',
          '        world.fence().expect("fence");',
          '        world.clone_template().expect("cloned");',
          '        let adopted = world.adopt().expect("adopt resumes");',
          '        assert_eq!(adopted.incarnation(), world.main.incarnation());',
          '        world.assert_adopted().expect("adopted");',
          '    }',
          '    #[test]',
          '    fn adopt_resumes_after_a_kill_between_attach_and_mount() {',
          '        let world = KillWindow::new("attached").expect("kill window");',
          '        drop(world.fenced_image().expect("attached"));',
          '        let adopted = world.adopt().expect("adopt resumes");',
          '        assert_eq!(adopted.incarnation(), world.main.incarnation());',
          '        world.assert_adopted().expect("adopted");',
          '        assert!(!world.staging.exists());',
          '    }',
        ),
      );
      expect(pairs(rs)).toEqual([
        [
          'same-scenario',
          'adopt_resumes_after_a_kill_between_clone_and_attach',
          'adopt_resumes_after_a_kill_between_attach_and_mount',
          4,
        ],
      ]);

      // Rust controls: an append through a mutable log is an action, not an assertion; a refusal ends in a different
      // action and outcome; a crash edge replays through a reopened handle.
      write(
        rs,
        rust(
          '    #[test]',
          '    fn an_unfenced_append_takes_tx_zero() {',
          '        let mut log = Log::open(MemoryLog::new());',
          '        let entry = frame(b"zero");',
          '        assert_eq!(log.append(&entry, Authority::Unchecked), Ok(0));',
          '    }',
          '    #[test]',
          '    fn a_stale_epoch_appends_nothing() {',
          '        let mut log = Log::open(MemoryLog::new());',
          '        let entry = frame(b"zero");',
          '        assert_eq!(log.append(&entry, Authority::Epoch(0)), Err(Fenced));',
          '        assert_eq!(log.tail(), None);',
          '    }',
          '    #[test]',
          '    fn a_closed_period_refuses() {',
          '        let mut ledger = Ledger::open(MemoryLog::new());',
          '        ledger.close("2026-09");',
          '        let refused = ledger.post("sales", 50);',
          '        assert!(refused.is_err());',
          '        assert_eq!(ledger.balance("sales"), 0);',
          '    }',
          '    #[test]',
          '    fn a_crash_replays() {',
          '        let mut ledger = Ledger::open(MemoryLog::new());',
          '        ledger.post("sales", 50);',
          '        let reopened = ledger.crash_and_reopen();',
          '        assert_eq!(reopened.balance("sales"), 50);',
          '        assert!(reopened.journal().len() == 1);',
          '    }',
        ),
      );
      expect(pairs(rs)).toEqual([]);

      // Diff scope: a duplicate pair already committed is judged only once either case changes.
      const git = (...args: string[]): string =>
        execFileSync('git', args, {
          cwd: root,
          encoding: 'utf8',
          env: { ...process.env, GIT_CONFIG_GLOBAL: '/dev/null', GIT_CONFIG_NOSYSTEM: '1' },
        });
      const legacy = [
        "test('posts 50', () => {",
        '  const ledger = openLedger(memoryLog());',
        "  ledger.post({ account: 'sales', amount: 50 });",
        "  expect(ledger.balance('sales')).toBe(50);",
        '});',
        "test('posts 70', () => {",
        '  const ledger = openLedger(memoryLog());',
        "  ledger.post({ account: 'sales', amount: 70 });",
        "  expect(ledger.balance('sales')).toBe(70);",
        '});',
        "test('reads an empty ledger', () => {",
        "  expect(openLedger(memoryLog()).balance('sales')).toBe(0);",
        '});',
      ];
      rmSync(join(root, rs));
      write(ts, lines(...legacy));
      git('init', '--quiet', '-b', 'main');
      git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'add', '.');
      git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '--quiet', '-m', 'base');
      const base = git('rev-parse', 'HEAD').trim();
      const judged = () => {
        const scope = diffScope(root, base);
        return findDuplicateScenarios(root, scope.files, scope.changes).map((found) => found.kind);
      };
      expect(judged()).toEqual([]);
      write(ts, lines(...legacy.slice(0, 11), "  expect(openLedger(memoryLog()).balance('sales')).toBe(0.0);", '});'));
      expect(judged()).toEqual([]);
      write(ts, lines(...legacy.slice(0, 8), "  expect(ledger.balance('sales')).toBe(71);", ...legacy.slice(9)));
      expect(judged()).toEqual(['literal-twin']);
      write('tests/new.test.ts', lines(...legacy.slice(0, 10)));
      expect(diffScope(root, base).changes.get('tests/new.test.ts')).toEqual([
        { start: 1, end: Number.POSITIVE_INFINITY },
      ]);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
