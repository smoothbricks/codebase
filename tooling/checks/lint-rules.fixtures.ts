/// <reference types="bun" />
// Planted fixtures for eslint.config.ts under the pinned lint Bun (`SMOO_LINT_BUN`): a config naming a rule `bun lint`
// silently ignores would pass every tree, so each kept rule is proven to fire on planted stdin source against the
// workspace's real eslint.config.ts, its clean twin is proven to pass, and a path outside every block is proven
// unscoped. Nothing is written to disk; `--stdin-filename` names the virtual path.
//
// (kept as fixtures, not *.test.ts: the name must stay invisible to bun test globs, since no package tsconfig owns
// this directory.)
import { spawnSync } from 'node:child_process';
import { dirname } from 'node:path';
import typia from 'typia';

interface LintMessage {
  readonly ruleId?: string | null;
  readonly line?: number;
  readonly message: string;
  readonly fatal?: boolean;
}

interface LintFileResult {
  readonly filePath: string;
  readonly messages: readonly LintMessage[];
}

interface FixtureCase {
  readonly label: string;
  readonly filename: string;
  readonly bad: string;
  readonly expected: readonly string[];
  readonly clean: string;
}

const parseLintReport = typia.json.createAssertParse<readonly LintFileResult[]>();
const CHECKED_RULES: ReadonlySet<string> = new Set([
  '@typescript-eslint/no-unsafe-type-assertion',
  '@typescript-eslint/consistent-type-assertions',
]);

function requireLintBun(): string {
  const value = process.env.SMOO_LINT_BUN;
  if (!value) {
    console.error('SMOO_LINT_BUN is unset; enter the devenv shell (devenv.smoo.nix fetches the lint Bun)');
    process.exit(1);
  }
  return value;
}

const lintBun = requireLintBun();

const workspace = dirname(dirname(import.meta.dir));
let failures = 0;

function lintStdin(filename: string, source: string): { status: number; messages: readonly LintMessage[] } | string {
  const result = spawnSync(
    lintBun,
    ['lint', '--no-script', '--type-aware', '-f', 'json', '--stdin', '--stdin-filename', filename],
    {
      cwd: workspace,
      encoding: 'utf8',
      input: source,
      timeout: 60_000,
    },
  );
  if (result.error !== undefined) {
    return `spawn error: ${result.error.message}${result.stderr ? `\n  stderr: ${result.stderr.trim()}` : ''}`;
  }
  if (result.signal !== null) {
    return `killed by signal ${result.signal}${result.stderr ? `\n  stderr: ${result.stderr.trim()}` : ''}`;
  }
  if (result.status !== 0 && result.status !== 1) {
    return `exited ${String(result.status)}\n  stdout: ${result.stdout.trim()}\n  stderr: ${result.stderr.trim()}`;
  }
  let report: readonly LintFileResult[];
  try {
    report = parseLintReport(result.stdout);
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    return `invalid JSON output: ${detail}\n  stdout: ${result.stdout.trim()}\n  stderr: ${result.stderr.trim()}`;
  }
  return {
    status: result.status,
    messages: report.flatMap((file) => file.messages),
  };
}

function formatRuleFindings(messages: readonly LintMessage[]): string[] {
  const findings: string[] = [];
  for (const message of messages) {
    if (message.fatal === true) {
      findings.push(`${message.line ?? 0} fatal: ${message.message}`);
    } else if (typeof message.ruleId === 'string') {
      findings.push(`${message.line ?? 0} ${message.ruleId}`);
    }
  }
  return findings;
}

function formatCheckedFindings(messages: readonly LintMessage[]): string[] {
  const findings: string[] = [];
  for (const message of messages) {
    if (message.fatal === true) {
      findings.push(`${message.line ?? 0} fatal: ${message.message}`);
    } else if (typeof message.ruleId === 'string' && CHECKED_RULES.has(message.ruleId)) {
      findings.push(`${message.line ?? 0} ${message.ruleId}`);
    }
  }
  return findings;
}

function assertBad(label: string, filename: string, source: string, expected: readonly string[]): void {
  const run = lintStdin(filename, source);
  if (typeof run === 'string') {
    failures += 1;
    console.error(`FAIL ${label} (bad, ${filename})\n  ${run}`);
    return;
  }
  const actual = formatRuleFindings(run.messages);
  if (run.status !== 1 || JSON.stringify(actual) !== JSON.stringify(expected)) {
    failures += 1;
    console.error(
      `FAIL ${label} (bad, ${filename})\n  exit:     ${run.status} (expected 1)\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`,
    );
  }
}

function assertNoCheckedRules(label: string, filename: string, source: string): void {
  const run = lintStdin(filename, source);
  if (typeof run === 'string') {
    failures += 1;
    console.error(`FAIL ${label} (${filename})\n  ${run}`);
    return;
  }
  const actual = formatCheckedFindings(run.messages);
  if (run.status !== 0 || actual.length > 0) {
    failures += 1;
    console.error(
      `FAIL ${label} (${filename})\n  exit:     ${run.status} (expected 0)\n  expected: []\n  actual:   ${JSON.stringify(actual)}`,
    );
  }
}

const ruleFixtures = [
  {
    name: 'no-unsafe-type-assertion',
    bad: 'const x: unknown = 1; export const y = x as string;\n',
    expected: ['1 @typescript-eslint/no-unsafe-type-assertion'],
    clean: 'export const y = 1 as number;\n',
  },
  {
    name: 'consistent-type-assertions (angle-bracket)',
    bad: 'export const y = <number>1;\n',
    expected: ['1 @typescript-eslint/consistent-type-assertions'],
    clean: 'export const y = 1 as number;\n',
  },
  {
    name: 'consistent-type-assertions (object-literal)',
    bad: 'export const o = { a: 1 } as { a: number };\n',
    expected: ['1 @typescript-eslint/consistent-type-assertions'],
    clean: 'export const o: { a: number } = { a: 1 };\n',
  },
] as const;

const scopedPaths = [
  { scope: 'packages', filename: 'packages/cli/src/lint-rules-fixture.ts' },
  { scope: 'tooling', filename: 'tooling/checks/lint-rules-fixture.ts' },
] as const;

const cases: FixtureCase[] = scopedPaths.flatMap(({ scope, filename }) =>
  ruleFixtures.map((fixture) => ({
    label: `${scope}: ${fixture.name}`,
    filename,
    bad: fixture.bad,
    expected: fixture.expected,
    clean: fixture.clean,
  })),
);

for (const fixture of cases) {
  assertBad(fixture.label, fixture.filename, fixture.bad, fixture.expected);
  assertNoCheckedRules(`${fixture.label} (clean)`, fixture.filename, fixture.clean);
}

const unscopedSource = [
  'const x: unknown = 1;',
  'export const a = x as string;',
  'export const b = <number>1;',
  'export const c = { a: 1 } as { a: number };',
  '',
].join('\n');
assertNoCheckedRules('outside every config block', 'packages/cli/lint-rules-fixture.ts', unscopedSource);

if (failures > 0) {
  console.error(`lint-rules fixtures: ${failures} failed`);
  process.exit(1);
}
console.log('lint-rules fixtures: every kept rule fired, every clean twin and unscoped path passed');
