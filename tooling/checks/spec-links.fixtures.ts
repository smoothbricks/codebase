/// <reference types="bun" />
// Planted fixtures for tooling/checks/spec-links.ts.
//
// A resolver that stopped firing looks exactly like drift-free specs, so each block plants one shape the verdict
// must catch plus the negative controls that keep extraction from claiming prose, examples and spec-internal
// links. Every planted backtick is joined from its code point, so this file holds no code reference of its own.
//
// (kept as fixtures, not *.test.ts: the name must stay invisible to bun test globs, since no package tsconfig owns
// this directory.)

import { mkdirSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import {
  extractRefs,
  findSpecLinkViolations,
  judgeRefs,
  pinnedMissingKeys,
  repoRelative,
  type SpecLinkFinding,
  type SpecLinkRow,
  type SpecLinkTree,
  type SpecLinkViolation,
  scanSpecLinks,
  trackedAfter,
} from './spec-links.ts';

let failures = 0;

function fail(what: string, expected: unknown, actual: unknown): void {
  failures++;
  console.error(`FAIL ${what}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`);
}

const T = String.fromCharCode(96);
const q = (s: string): string => T + s + T;
const SPEC = 'specs/widget/01.md';

const texts = new Map<string, string>([
  ['packages/widget/src/index.ts', 'export const a = 1;\nexport const b = 2;\nexport const c = 3;\n'],
  ['crates/core/src/lib.rs', 'fn run_core() {}\nstruct Widget;\nimpl Widget { fn open() {} }\n'],
  ['packages/widget/docs/guide.md', '# Guide\n'],
  ['a/lib/x.ts', 'export {};\n'],
  ['b/lib/x.ts', 'export {};\n'],
]);
const tree: SpecLinkTree = { files: [...texts.keys()], textOf: (p) => texts.get(p) };

function reasons(what: string, specText: string, expected: readonly string[]): SpecLinkFinding[] {
  const findings = judgeRefs(extractRefs(SPEC, specText), tree);
  const actual = findings.map((f) => f.reason);
  if (JSON.stringify(actual) !== JSON.stringify(expected)) fail(what, expected, findings);
  return findings;
}

function refs(what: string, specText: string, expected: number): void {
  const actual = extractRefs(SPEC, specText).length;
  if (actual !== expected) fail(what, expected, actual);
}

// ── path-line ──────────────────────────────────────────────────────────────────────────────────
reasons('a repo-relative path and line resolves', `See ${q('packages/widget/src/index.ts:2')}.`, []);
reasons('a unique suffix resolves', `See ${q('src/index.ts:1-3')}.`, []);
reasons('an elided path resolves through its segments', `See ${q('widget/.../index.ts:2')}.`, []);
reasons('a leading elision resolves', `See ${q('.../core/src/lib.rs:1')}.`, []);
reasons('a missing file fires', `See ${q('gone/src/nope.ts:3')}.`, ['missing-file']);
reasons('a line past the end fires', `See ${q('packages/widget/src/index.ts:2-99')}.`, ['line-out-of-range']);
reasons('an ambiguous suffix fires', `See ${q('lib/x.ts:1')}.`, ['ambiguous-file']);
refs('a bare file name with a line is not this form', `See ${q('index.ts:2')}.`, 0);
refs('an unticked path is prose', 'See packages/widget/src/index.ts:99 for details.', 0);
refs('a fenced example is not a reference', `Before.\n\n${T}${T}${T}\n${q('gone/src/nope.ts:3')}\n${T}${T}${T}\n`, 0);
reasons(
  'a reference after a fence still fires',
  `${T + T + T}\ncode\n${T}${T}${T}\n\nSee ${q('gone/src/nope.ts:3')}.`,
  ['missing-file'],
);

// ── link ───────────────────────────────────────────────────────────────────────────────────────
reasons('a link into code resolves', '[the code](../../packages/widget/src/index.ts)', []);
reasons('a link to a directory resolves', '[docs](../../packages/widget/docs)', []);
reasons('a link to a missing file fires', '[gone](../../packages/widget/src/gone.ts)', ['missing-file']);
reasons('a link fragment past the end fires', '[code](../../packages/widget/src/index.ts#L2-L99)', [
  'line-out-of-range',
]);
reasons('a link out of the repository fires', '[sibling](../../../elsewhere/src/index.ts)', ['escapes-repo']);
refs('a spec-internal link is out of scope even when absent', '[other](./02.md)', 0);
refs('an external link is out of scope', '[web](https://example.com/src/index.ts)', 0);
refs('an absolute href is not a relative link', '[regex](/|$)', 0);
refs('an image is not a link', '![img](../../packages/widget/src/gone.png)', 0);

// ── code entries ───────────────────────────────────────────────────────────────────────────────
reasons(
  'entries and their symbols resolve',
  `Code: [${q('crates/core/src/lib.rs')}] ${q('run_core')}, ${q('Widget::open')}.`,
  [],
);
reasons(
  'a suffix entry resolves and a missing symbol fires',
  `Code: [${q('core/src/lib.rs')}] ${q('run_core')}, ${q('gone')}.`,
  ['missing-symbol'],
);
reasons('an entry naming no file fires', `Code: [${q('gone/src/lib.rs')}] ${q('run_core')}.`, ['missing-file']);
reasons(
  'ticked prose after the list is not a symbol',
  `Code: [${q('crates/core/src/lib.rs')}] ${q('run_core')}. Later prose names ${q('whatever')} freely.`,
  [],
);
reasons(
  'entries may wrap across lines',
  `Code:\n[${q('crates/core/src/lib.rs')}] ${q('run_core')};\n[${q('core/src/lib.rs')}] ${q('gone')}.`,
  ['missing-symbol'],
);
refs('a bracketed path without Code: is not an entry', `See [${q('gone/src/lib.rs')}] for details.`, 0);

// ── shapes a review planted after the first landing ───────────────────────────────────────────
reasons('a directory link with a trailing slash resolves', '[docs](../../packages/widget/docs/)', []);
reasons('a link to the repository root resolves', '[root](../..)', []);
reasons('a range starting past the end fires', `See ${q('packages/widget/src/index.ts:99-1')}.`, ['line-out-of-range']);
reasons('a Code: entry may cite a column', `Code: [${q('crates/core/src/lib.rs:1:5')}] ${q('run_core')}.`, []);
refs('a tilde fence holds examples, not references', `~~~\n${q('gone/src/nope.ts:3')}\n~~~`, 0);

// ── the ratchet ────────────────────────────────────────────────────────────────────────────────
const brief = 'agents-todo/brief.md';
const owned = (path: string): boolean => path === brief;
const finding = judgeRefs(extractRefs(SPEC, `See ${q('gone/src/nope.ts:3')}.`), tree);
const row = (count: number): SpecLinkRow => ({
  spec: SPEC,
  kind: 'path-line',
  raw: 'gone/src/nope.ts:3',
  count,
  reason: 'Names a tree this repository does not hold.',
  defect: brief,
});
function verdict(what: string, violations: readonly SpecLinkViolation[], expected: readonly string[]): void {
  const actual = violations.map((v) => v.kind);
  if (JSON.stringify(actual) !== JSON.stringify(expected)) fail(what, expected, violations);
}
verdict('a ledgered finding passes', findSpecLinkViolations(finding, [row(1)], owned), []);
verdict('a new finding fails', findSpecLinkViolations(finding, [], owned), ['unledgered']);
verdict('a fixed reference whose row stayed fails', findSpecLinkViolations([], [row(1)], owned), ['count']);
verdict('a raised count fails', findSpecLinkViolations(finding, [row(2)], owned), ['count']);
verdict('two rows for one reference fail', findSpecLinkViolations(finding, [row(1), row(1)], owned), ['duplicate-row']);
verdict('a row without a reason fails', findSpecLinkViolations(finding, [{ ...row(1), reason: ' ' }], owned), [
  'unowned',
]);
verdict(
  'a row naming no tracked brief fails',
  findSpecLinkViolations(finding, [{ ...row(1), defect: 'agents-todo/gone.md' }], owned),
  ['unowned'],
);
const suffixTree: SpecLinkTree = {
  files: ['elsewhere/plugin/optimization.go'],
  textOf: (p) => (p === 'elsewhere/plugin/optimization.go' ? 'package plugin\n' : undefined),
};
const external = extractRefs(SPEC, `See ${q('plugin/optimization.go:1')}.`);
const unpinned = judgeRefs(external, suffixTree).length;
if (unpinned !== 0) fail('a unique suffix resolves an unledgered citation', 0, unpinned);
const pinned = judgeRefs(external, suffixTree, pinnedMissingKeys([{ ...row(1), raw: 'plugin/optimization.go:1' }])).map(
  (f) => f.reason,
);
if (JSON.stringify(pinned) !== JSON.stringify(['missing-file']))
  fail('a ledgered other-tree citation stays missing under a suffix match', ['missing-file'], pinned);
const exactTree: SpecLinkTree = { files: ['plugin/optimization.go'], textOf: () => 'package plugin\n' };
const exact = judgeRefs(
  external,
  exactTree,
  pinnedMissingKeys([{ ...row(1), raw: 'plugin/optimization.go:1' }]),
).length;
if (exact !== 0) fail('creating the cited file itself resolves a pinned citation', 0, exact);

// ── the tree a commit will hold ────────────────────────────────────────────────────────────────
// A doc lane runs this check before it stages its edit, so a file the edit adds is untracked and one it deletes is
// still tracked: the paths of the edit say which.
function held(what: string, pending: readonly string[], present: readonly string[], expected: readonly string[]): void {
  const actual = trackedAfter(['a.md', 'b.md'], pending, (p) => present.includes(p)).sort();
  if (JSON.stringify(actual) !== JSON.stringify(expected)) fail(what, expected, actual);
}
held('a file the edit adds joins the tracked ones', ['c.md'], ['a.md', 'b.md', 'c.md'], ['a.md', 'b.md', 'c.md']);
held('a file the edit deletes leaves them', ['b.md'], ['a.md'], ['a.md']);
held('a rename leaves its old path and holds its new one', ['b.md', 'c.md'], ['a.md', 'c.md'], ['a.md', 'c.md']);
held('a file the edit rewrites stays once', ['a.md'], ['a.md', 'b.md'], ['a.md', 'b.md']);
held('a deleted file that was never tracked changes nothing', ['z.md'], ['a.md', 'b.md'], ['a.md', 'b.md']);
function place(arg: string, expected: string | undefined): void {
  const actual = repoRelative(arg);
  if (actual !== expected) fail(`the path argument ${arg}`, expected, actual);
}
place('specs/a/01.md', 'specs/a/01.md');
place('./specs/a/../a/01.md', 'specs/a/01.md');
place('../elsewhere/01.md', undefined);
place('/abs/specs/a/01.md', undefined);
place('.', undefined);

// scanSpecLinks against a throwaway repository: its index lists the tracked tree and its working tree holds the edit.
// A spec the edit adds is judged, a file the same edit adds resolves a citation, a dangling symlink the edit adds is an
// entry of the tree (it makes a suffix ambiguous, and as a spec it fails the read), and a spec the edit deletes is not
// read (reading a tracked spec that is gone fails the scan).
const scratch = mkdtempSync(join(tmpdir(), 'spec-links-'));
try {
  const put = (path: string, text: string): void => {
    mkdirSync(dirname(join(scratch, path)), { recursive: true });
    writeFileSync(join(scratch, path), text);
  };
  const link = (path: string, target: string): void => {
    mkdirSync(dirname(join(scratch, path)), { recursive: true });
    symlinkSync(target, join(scratch, path));
  };
  const git = (...args: string[]): void => {
    const run = Bun.spawnSync(['git', ...args], { cwd: scratch, stdout: 'pipe', stderr: 'pipe' });
    if (run.exitCode !== 0) throw new Error(`git ${args.join(' ')} failed: ${run.stderr.toString().trim()}`);
  };
  const scan = async (what: string, pending: readonly string[], expected: readonly string[]): Promise<void> => {
    const { findings } = await scanSpecLinks(pending, scratch);
    const actual = findings.map((f) => `${f.ref.spec} ${f.reason}`);
    if (JSON.stringify(actual) !== JSON.stringify(expected)) fail(what, expected, findings);
  };
  git('init', '-q');
  put('src/lib.ts', 'export const a = 1;\n');
  put('specs/a/01.md', `See ${q('src/lib.ts:1')}.\n`);
  put('packages/a/src/util.ts', 'export const c = 3;\n');
  put('specs/a/04.md', `See ${q('src/util.ts:1')}.\n`);
  git('add', '-A');
  await scan('the tracked tree resolves', [], []);
  put('specs/a/02.md', `See ${q('src/gone.ts:1')}.\n`);
  await scan('an untracked spec is outside the tracked tree', [], []);
  await scan('a spec the edit adds is judged', ['specs/a/02.md'], ['specs/a/02.md missing-file']);
  put('src/gone.ts', 'export const b = 2;\n');
  await scan('a file the same edit adds resolves the citation', ['specs/a/02.md', 'src/gone.ts'], []);
  link('packages/b/src/util.ts', 'nowhere.ts');
  await scan(
    'a dangling symlink the edit adds is an entry of the tree',
    ['packages/b/src/util.ts'],
    ['specs/a/04.md ambiguous-file'],
  );
  rmSync(join(scratch, 'packages/b'), { recursive: true });
  link('specs/a/03.md', 'nowhere.md');
  const failed = await scanSpecLinks(['specs/a/03.md'], scratch).then(
    () => false,
    () => true,
  );
  if (!failed)
    fail('a dangling symlink spec the edit adds fails the scan, as it fails root lint once committed', true, failed);
  rmSync(join(scratch, 'specs/a/03.md'));
  rmSync(join(scratch, 'specs/a/01.md'));
  await scan('a spec the edit deletes is not read', ['specs/a/01.md', 'specs/a/02.md', 'src/gone.ts'], []);
} finally {
  rmSync(scratch, { recursive: true, force: true });
}

// The CLI refuses a path that is no file of the repository before it scans: exit 2, the usage.
for (const bad of ['../outside.md', 'specs']) {
  const run = Bun.spawnSync([process.execPath, join(import.meta.dir, 'spec-links.ts'), bad], {
    stdout: 'pipe',
    stderr: 'pipe',
  });
  if (run.exitCode !== 2) fail(`the path argument ${bad} is usage`, 2, run.exitCode);
}

if (failures > 0) {
  console.error(`Spec-link fixtures failed: ${failures} planted shape(s) did not get the verdict they plant.`);
  process.exit(1);
}
console.log('Spec-link fixtures passed: the check fires on every planted shape and on neither control.');
