/// <reference types="bun" />
// Planted fixtures for tooling/checks/spec-links.ts.
//
// A resolver that stopped firing looks exactly like drift-free specs, so each block plants one shape the verdict
// must catch plus the negative controls that keep extraction from claiming prose, examples and spec-internal
// links. Every planted backtick is joined from its code point, so this file holds no code reference of its own.
//
// (kept as fixtures, not *.test.ts: the name must stay invisible to bun test globs, since no package tsconfig owns
// this directory.)

import {
  extractRefs,
  findSpecLinkViolations,
  judgeRefs,
  type SpecLinkFinding,
  type SpecLinkRow,
  type SpecLinkTree,
  type SpecLinkViolation,
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

// ── the ratchet ────────────────────────────────────────────────────────────────────────────────
const brief = 'agent-todo/brief.md';
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
  findSpecLinkViolations(finding, [{ ...row(1), defect: 'agent-todo/gone.md' }], owned),
  ['unowned'],
);

if (failures > 0) {
  console.error(`Spec-link fixtures failed: ${failures} planted shape(s) did not get the verdict they plant.`);
  process.exit(1);
}
console.log('Spec-link fixtures passed: the check fires on every planted shape and on neither control.');
