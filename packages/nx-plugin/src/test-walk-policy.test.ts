import { describe, expect, it } from 'bun:test';
import { execFileSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import {
  diffScope,
  findDuplicateScenarios,
  formatDuplicateScenario,
  TEST_ONE_WALK,
  upstreamBase,
} from './test-walk-policy.js';

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
    const inheritedBase = process.env.NX_BASE;
    delete process.env.NX_BASE;
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
      for (const part of [
        `error[${TEST_ONE_WALK.id}]`,
        `${ts}:6`,
        `${ts}:1`,
        '`posts a sale` only asserts after the 2 statements it shares with `a sale lands in the journal`',
        'its assertions belong in `a sale lands in the journal` after statement 2',
        'one walk',
      ])
        expect(diagnostic).toContain(part);

      // A pair whose bodies are identical is reported as a verbatim repeat.
      const ledgerCase = (title: string) => [
        `test('${title}', () => {`,
        '  const ledger = openLedger(memoryLog());',
        "  ledger.post({ account: 'sales', amount: 50 });",
        "  expect(ledger.balance('sales')).toBe(50);",
        '});',
      ];
      write(ts, lines(...ledgerCase('posts'), ...ledgerCase('posts again')));
      expect(findDuplicateScenarios(root, [ts]).map(formatDuplicateScenario)[0]).toContain(
        '`posts again` repeats `posts` verbatim (3 statements)',
      );

      // Diff scope picks the nearest partner among judged pairs only: A untouched, C changed, B untouched. B repeats
      // A over 3 statements and C over 2; judged on C's lines, the C~B pair is still reported.
      write(
        ts,
        lines(
          "test('A', () => {",
          '  const ledger = openLedger(memoryLog());',
          "  ledger.post({ account: 'sales', amount: 50 });",
          "  ledger.post({ account: 'fees', amount: 5 });",
          "  expect(ledger.balance('sales')).toBe(50);",
          '});',
          "test('C', () => {",
          '  const ledger = openLedger(memoryLog());',
          "  ledger.post({ account: 'sales', amount: 50 });",
          "  expect(ledger.balance('fees')).toBe(0);",
          '});',
          "test('B', () => {",
          '  const ledger = openLedger(memoryLog());',
          "  ledger.post({ account: 'sales', amount: 50 });",
          "  ledger.post({ account: 'fees', amount: 5 });",
          "  expect(ledger.balance('fees')).toBe(5);",
          '});',
        ),
      );
      const scoped = findDuplicateScenarios(root, [ts], new Map([[ts, [{ start: 8, end: 8 }]]]));
      expect(scoped.map((found) => [found.kind, found.first.name, found.second.name, found.shared])).toEqual([
        ['same-path', 'A', 'C', 2],
        ['same-path', 'C', 'B', 2],
      ]);

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

      // Outcome modifiers are part of a case: a passing row and an expected panic, or an ignored case, are not one
      // table. The same bodies without the modifiers are refused.
      const panicking = (attributes: string, name: string, amount: string) => [
        `    ${attributes}`,
        `    fn ${name}() {`,
        '        let ledger = Ledger::from_entries(&[("sales", 50)]);',
        `        ledger.withdraw("sales", ${amount});`,
        '    }',
      ];
      write(
        rs,
        rust(...panicking('#[test]', 'withdraws', '50'), ...panicking('#[test] #[should_panic]', 'overdraws', '51')),
      );
      expect(pairs(rs)).toEqual([]);
      write(
        rs,
        rust(...panicking('#[test]', 'withdraws', '50'), ...panicking('#[ignore = "slow"] #[test]', 'big', '51')),
      );
      expect(pairs(rs)).toEqual([]);
      write(
        rs,
        rust(...panicking('#[test]', 'withdraws', '50'), ...panicking('#[tokio::test]', 'withdraws_async', '51')),
      );
      expect(pairs(rs)).toEqual([['literal-twin', 'withdraws', 'withdraws_async', 2]]);

      // Struct-literal keys and tuple indexes are names, not locals or literals.
      write(
        rs,
        rust(
          '    #[test]',
          '    fn binds_the_host() {',
          '        let host = endpoint();',
          '        let config = Config { host: 1 };',
          '        assert_eq!(config.pair.0.1, host);',
          '    }',
          '    #[test]',
          '    fn binds_the_port() {',
          '        let port = endpoint();',
          '        let config = Config { port: 1 };',
          '        assert_eq!(config.pair.1.0, port);',
          '    }',
        ),
      );
      expect(pairs(rs)).toEqual([]);

      // A body that is one wrapper call walks the wrapper's statements: `block_on(async { … })` in Rust and
      // `await withHarness(async (h) => { … })` in TypeScript.
      const blockOn = (name: string, amount: string) => [
        '    #[test]',
        `    fn ${name}() {`,
        '        runtime().block_on(async {',
        '            let ledger = Ledger::open(MemoryLog::new()).await;',
        `            ledger.post("sales", ${amount}).await;`,
        `            assert_eq!(ledger.balance("sales").await, ${amount});`,
        '        });',
        '    }',
      ];
      write(rs, rust(...blockOn('posts_50', '50'), ...blockOn('posts_70', '70')));
      expect(pairs(rs)).toEqual([['literal-twin', 'posts_50', 'posts_70', 3]]);
      const harness = (title: string, amount: string) => [
        `test('${title}', async () => {`,
        '  await withHarness(async (h) => {',
        '    const ledger = h.ledger();',
        `    ledger.post({ account: 'sales', amount: ${amount} });`,
        `    expect(ledger.balance('sales')).toBe(${amount});`,
        '  });',
        '});',
      ];
      write(ts, lines(...harness('posts 50', '50'), ...harness('posts 70', '70')));
      expect(pairs(ts)).toEqual([['literal-twin', 'posts 50', 'posts 70', 3]]);

      // An expected failure, or a different hook chain, is part of a TypeScript case.
      write(
        ts,
        lines(
          ...ledgerCase('posts'),
          ...ledgerCase('posts again').map((row, i) => (i === 0 ? row.replace('test(', 'test.failing(') : row)),
        ),
      );
      expect(pairs(ts)).toEqual([]);
      write(
        ts,
        lines(
          "describe('fresh', () => {",
          '  beforeEach(() => { store = new Store(); });',
          ...ledgerCase('posts'),
          '});',
          "describe('seeded', () => {",
          '  beforeEach(() => { store = Store.seeded(); });',
          ...ledgerCase('posts'),
          '});',
        ),
      );
      expect(pairs(ts)).toEqual([]);

      // Known limit: a method call inside an assertion counts as a read. `drain()` acts on the queue, but no parser
      // can tell it from `size()`, so this pair is refused as same-path and review decides.
      write(
        ts,
        lines(
          "test('queues one job', async () => {",
          '  const queue = openQueue();',
          "  queue.push('job');",
          '  expect(queue.size()).toBe(1);',
          '});',
          "test('drains the job', async () => {",
          '  const queue = openQueue();',
          "  queue.push('job');",
          "  expect(await queue.drain()).toEqual(['job']);",
          '});',
        ),
      );
      expect(pairs(ts)).toEqual([['same-path', 'queues one job', 'drains the job', 2]]);

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
        if (!scope.ok) throw new Error(scope.reason);
        return findDuplicateScenarios(root, scope.value.files, scope.value.changes).map((found) => found.kind);
      };
      expect(judged()).toEqual([]);
      write(ts, lines(...legacy.slice(0, 11), "  expect(openLedger(memoryLog()).balance('sales')).toBe(0.0);", '});'));
      expect(judged()).toEqual([]);
      write(ts, lines(...legacy.slice(0, 8), "  expect(ledger.balance('sales')).toBe(71);", ...legacy.slice(9)));
      expect(judged()).toEqual(['literal-twin']);
      write('tests/new.test.ts', lines(...legacy.slice(0, 10)));
      const scope = diffScope(root, base);
      expect(scope.ok && scope.value.changes.get('tests/new.test.ts')).toEqual([
        { start: 1, end: Number.POSITIVE_INFINITY },
      ]);

      // Base selection: local main; a CI checkout with only origin/main; no main at all; NX_BASE set, and unresolvable.
      const commit = (...args: string[]) =>
        git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '--quiet', ...args);
      commit('-am', 'next');
      expect(upstreamBase(root)).toEqual({ ok: true, value: git('rev-parse', 'HEAD').trim() });
      git('checkout', '--quiet', '-b', 'topic');
      commit('--allow-empty', '-m', 'topic');
      const mainHead = git('rev-parse', 'main').trim();
      git('branch', '--quiet', '-D', 'main');
      git('update-ref', 'refs/remotes/origin/main', mainHead);
      expect(upstreamBase(root)).toEqual({ ok: true, value: mainHead });
      git('update-ref', '-d', 'refs/remotes/origin/main');
      const missing = upstreamBase(root);
      expect(missing.ok).toBe(false);
      expect(missing.ok ? '' : missing.reason).toContain('export NX_BASE');
      process.env.NX_BASE = base;
      expect(upstreamBase(root)).toEqual({ ok: true, value: base });
      process.env.NX_BASE = 'no-such-commit';
      expect(upstreamBase(root)).toEqual({ ok: false, reason: 'NX_BASE=no-such-commit names no commit' });
    } finally {
      if (inheritedBase === undefined) delete process.env.NX_BASE;
      else process.env.NX_BASE = inheritedBase;
      rmSync(root, { recursive: true, force: true });
    }
  });
});
