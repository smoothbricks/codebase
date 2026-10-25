/// <reference types="bun" />
// SPEC-LINK: a code reference in a spec names code that exists.
//
// Specs cite code three ways:
//
//   - path-line: a backticked path/to/file.ext:LINE or :LINE-LINE reference;
//   - link: a Markdown link that resolves outside specs/, with an optional #L<line> fragment;
//   - code: a Code: entry list of bracketed paths, each followed by the symbols the passage
//     rests on.
//
// Each must resolve in this tree: the file exists (repo-relative, or the one tracked file a
// suffix-relative or ...-elided path names), a cited line is within the file, and a named symbol
// occurs in it. This check judges references that leave the spec tree for code; links between
// specs are out of its scope. Fenced code blocks hold examples, not references, and are blanked
// before scanning.
//
// Every reference already broken when this check lands is counted in spec-links.ledger.ts, one
// row per spec, reference and symbol with its reason and owning defect. The counts are exact: a
// new broken reference, a raised count, a fixed reference whose row stayed, and a row deleted
// without its fix all fail. Fix the reference and its row in the same commit; never ledger a new
// broken reference to make the check pass.
import { existsSync, readFileSync } from 'node:fs';
import { posix, resolve } from 'node:path';
import { SPEC_LINKS } from './spec-links.ledger.ts';

const REPO_ROOT = resolve(import.meta.dir, '..', '..');
// The backtick, joined from its code point so this file holds none of its own.
const TICK = String.fromCharCode(96);

export type SpecRefKind = 'path-line' | 'link' | 'code';

/** One code reference as written in a spec. */
export interface SpecRef {
  readonly spec: string;
  readonly line: number;
  readonly kind: SpecRefKind;
  /** The reference exactly as written: the ticked path with its lines, the href, or the entry path. */
  readonly raw: string;
  /** The path to resolve, without cited lines or a fragment. */
  readonly target: string;
  /** The greatest cited line, when the reference cites one. */
  readonly lastLine?: number;
  readonly symbols: readonly string[];
}

export type BrokenReason = 'missing-file' | 'ambiguous-file' | 'line-out-of-range' | 'missing-symbol' | 'escapes-repo';

/** One reference that does not resolve, with the target it was judged against. */
export interface SpecLinkFinding {
  readonly ref: SpecRef;
  readonly reason: BrokenReason;
  readonly detail: string;
  readonly symbol?: string;
}

/** A broken reference present when the check landed, owned by the defect that fixes it. */
export interface SpecLinkRow {
  readonly spec: string;
  readonly kind: SpecRefKind;
  readonly raw: string;
  readonly symbol?: string;
  readonly count: number;
  readonly reason: string;
  readonly defect: string;
}

export type SpecLinkViolation =
  | { readonly kind: 'unledgered'; readonly finding: SpecLinkFinding }
  | { readonly kind: 'count'; readonly row: SpecLinkRow; readonly found: number }
  | { readonly kind: 'duplicate-row'; readonly row: SpecLinkRow }
  | { readonly kind: 'unowned'; readonly row: SpecLinkRow };

/** The tracked tree a reference resolves against: every tracked path, and a file's text on demand. */
export interface SpecLinkTree {
  readonly files: readonly string[];
  readonly textOf: (path: string) => string | undefined;
}

const TICK_SPAN = new RegExp(`${TICK}([^${TICK}\\n]+)${TICK}`, 'g');
const PATH_LINE = /^([\w./@-]+\.[A-Za-z][A-Za-z0-9]{0,5}):(\d+)(?:-(\d+))?(?::\d+)?$/;
const LINK = /(?<!!)\[[^\]\n]*\]\((<[^>\n]+>|[^)\s]+)(?:\s+"[^"]*")?\)/g;
const SCHEME = /^[a-z][a-z0-9+.-]*:/i;
const FRAG_LINE = /^L(\d+)(?:-L(\d+))?$/;
const CODE_START = /\bCode:\s/g;
const ENTRY = new RegExp(`\\[\\s*${TICK}([^${TICK}]+)${TICK}\\s*\\]`, 'g');
const SYM_NEXT = new RegExp(`^\\s*${TICK}([^${TICK}]+)${TICK}\\s*([,;.]?)`);
const ENTRY_LINE = /^(.+?):(\d+)(?:-(\d+))?$/;
const FENCE = TICK + TICK + TICK;

/** Fenced blocks blanked to empty lines, so offsets and line numbers still name the original text. */
function blankFences(text: string): string {
  let fenced = false;
  return text
    .split('\n')
    .map((line) => {
      if (line.trimStart().startsWith(FENCE)) {
        fenced = !fenced;
        return '';
      }
      return fenced ? '' : line;
    })
    .join('\n');
}

function lineAt(text: string, offset: number): number {
  let line = 1;
  for (let i = 0; i < offset; i++) if (text.charCodeAt(i) === 10) line++;
  return line;
}

function decodeHref(href: string): string {
  try {
    return decodeURIComponent(href);
  } catch {
    return href;
  }
}

/** The comma-chained backticked symbols right after a Code: entry; the chain ends at the first separator that is not a comma. */
function symbolChain(after: string): string[] {
  const symbols: string[] = [];
  let rest = after;
  for (;;) {
    const m = SYM_NEXT.exec(rest);
    if (m === null) return symbols;
    symbols.push(m[1] ?? '');
    if (m[2] !== ',') return symbols;
    rest = rest.slice(m[0].length);
  }
}

/** Every code reference in one spec's text, in the order written. Pure: syntax only, no filesystem. */
export function extractRefs(spec: string, text: string): SpecRef[] {
  const blanked = blankFences(text);
  const specDir = posix.dirname(spec);
  const byKey = new Map<string, SpecRef>();
  const add = (ref: SpecRef): void => {
    const key = `${ref.line}|${ref.raw}`;
    const prior = byKey.get(key);
    // A path cited with a line inside a Code: entry is the code reference, which also carries its symbols.
    if (prior === undefined || (prior.kind === 'path-line' && ref.kind === 'code')) byKey.set(key, ref);
  };
  const lines = blanked.split('\n');
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i] ?? '';
    for (const m of line.matchAll(TICK_SPAN)) {
      const token = (m[1] ?? '').trim();
      const pm = PATH_LINE.exec(token);
      if (pm === null || !(pm[1] ?? '').includes('/')) continue;
      add({
        spec,
        line: i + 1,
        kind: 'path-line',
        raw: token,
        target: pm[1] ?? '',
        lastLine: Number(pm[3] ?? pm[2]),
        symbols: [],
      });
    }
    for (const m of line.matchAll(LINK)) {
      const href = (m[1] ?? '').replace(/^<|>$/g, '');
      if (SCHEME.test(href) || href.startsWith('#') || href.startsWith('/')) continue;
      const hash = href.indexOf('#');
      const target = decodeHref(hash === -1 ? href : href.slice(0, hash));
      if (target === '') continue;
      const resolved = posix.normalize(posix.join(specDir, target));
      if (resolved === 'specs' || resolved.startsWith('specs/')) continue;
      const frag = hash === -1 ? undefined : FRAG_LINE.exec(href.slice(hash + 1));
      add({
        spec,
        line: i + 1,
        kind: 'link',
        raw: href,
        target,
        ...(frag === null || frag === undefined ? {} : { lastLine: Number(frag[2] ?? frag[1]) }),
        symbols: [],
      });
    }
  }
  for (const cm of blanked.matchAll(CODE_START)) {
    const start = cm.index + cm[0].length;
    const end = blanked.indexOf('\n\n', start);
    const chunk = blanked.slice(start, end === -1 ? blanked.length : end);
    const entries: { readonly raw: string; readonly index: number; readonly end: number }[] = [];
    for (const em of chunk.matchAll(ENTRY))
      entries.push({ raw: (em[1] ?? '').trim(), index: em.index, end: em.index + em[0].length });
    for (let e = 0; e < entries.length; e++) {
      const entry = entries[e];
      if (entry === undefined) continue;
      const after = chunk.slice(entry.end, entries[e + 1]?.index ?? chunk.length);
      const lm = ENTRY_LINE.exec(entry.raw);
      add({
        spec,
        line: lineAt(blanked, start + entry.index),
        kind: 'code',
        raw: entry.raw,
        target: lm === null ? entry.raw : (lm[1] ?? ''),
        ...(lm === null ? {} : { lastLine: Number(lm[3] ?? lm[2]) }),
        symbols: symbolChain(after),
      });
    }
  }
  return [...byKey.values()].sort((a, b) => a.line - b.line || (a.raw < b.raw ? -1 : a.raw > b.raw ? 1 : 0));
}

function matchesElided(fileSegs: readonly string[], groups: readonly (readonly string[])[]): boolean {
  const runAt = (at: number, group: readonly string[]): boolean => group.every((seg, j) => fileSegs[at + j] === seg);
  let pos = 0;
  for (let g = 0; g < groups.length; g++) {
    const group = groups[g] ?? [];
    if (group.length === 0) continue;
    if (g === groups.length - 1) {
      const at = fileSegs.length - group.length;
      return at >= pos && runAt(at, group);
    }
    let found = -1;
    for (let at = pos; at + group.length <= fileSegs.length; at++) {
      if (runAt(at, group)) {
        found = at;
        break;
      }
    }
    if (found === -1) return false;
    pos = found + group.length;
  }
  return true;
}

type Resolution = { readonly path: string } | { readonly missing: true } | { readonly ambiguous: number };

/** The one tracked file a target names: itself, or a unique suffix or ...-elided match. */
function resolveRepoPath(target: string, files: readonly string[], set: ReadonlySet<string>): Resolution {
  if (set.has(target)) return { path: target };
  const segs = target.split('/');
  let hits: string[];
  if (segs.includes('...')) {
    const groups: string[][] = [[]];
    for (const seg of segs) {
      if (seg === '...') groups.push([]);
      else (groups[groups.length - 1] ?? []).push(seg);
    }
    hits = files.filter((f) => matchesElided(f.split('/'), groups));
  } else {
    hits = files.filter((f) => f.endsWith(`/${target}`));
  }
  if (hits.length === 1) return { path: hits[0] ?? '' };
  return hits.length === 0 ? { missing: true } : { ambiguous: hits.length };
}

/** Lines in a file's text; a trailing newline ends the last line, it does not add one. */
export function lineCount(text: string): number {
  if (text === '') return 0;
  const parts = text.split('\n');
  return parts[parts.length - 1] === '' ? parts.length - 1 : parts.length;
}

function escapeRegExp(s: string): string {
  const specials = `.*+?^$()|[]\\${String.fromCharCode(123, 125)}`;
  return s
    .split('')
    .map((ch) => (specials.includes(ch) ? `\\${ch}` : ch))
    .join('');
}

/** A cited symbol occurs in the file: every :: segment present as an identifier, a trailing (…) ignored. */
function symbolExists(body: string, symbol: string): boolean {
  const parts = symbol
    .replace(/\([^()]*\)$/, '')
    .trim()
    .split('::')
    .filter((p) => p !== '');
  return parts.length > 0 && parts.every((p) => new RegExp(`(?<![\\w])${escapeRegExp(p)}(?![\\w])`).test(body));
}

/** The pure verdict over extracted references: every reference that does not resolve in the tree. */
export function judgeRefs(refs: readonly SpecRef[], tree: SpecLinkTree): SpecLinkFinding[] {
  const set = new Set(tree.files);
  const findings: SpecLinkFinding[] = [];
  for (const ref of refs) {
    let path: string;
    if (ref.kind === 'link') {
      const resolved = posix.normalize(posix.join(posix.dirname(ref.spec), ref.target));
      if (resolved === '..' || resolved.startsWith('../')) {
        findings.push({ ref, reason: 'escapes-repo', detail: 'the link leaves this repository' });
        continue;
      }
      if (!set.has(resolved)) {
        if (tree.files.some((f) => f.startsWith(`${resolved}/`))) continue;
        findings.push({ ref, reason: 'missing-file', detail: `names no tracked file: ${resolved}` });
        continue;
      }
      path = resolved;
    } else {
      const resolution = resolveRepoPath(ref.target, tree.files, set);
      if ('missing' in resolution) {
        findings.push({ ref, reason: 'missing-file', detail: 'names no tracked file' });
        continue;
      }
      if ('ambiguous' in resolution) {
        findings.push({
          ref,
          reason: 'ambiguous-file',
          detail: `names ${resolution.ambiguous} tracked files; cite the repo-relative path`,
        });
        continue;
      }
      path = resolution.path;
    }
    const text = tree.textOf(path);
    if (text === undefined) {
      findings.push({ ref, reason: 'missing-file', detail: `names no readable tracked file: ${path}` });
      continue;
    }
    if (ref.lastLine !== undefined && ref.lastLine > lineCount(text)) {
      findings.push({
        ref,
        reason: 'line-out-of-range',
        detail: `cites line ${ref.lastLine} of ${path}, which has ${lineCount(text)} lines`,
      });
      continue;
    }
    for (const symbol of ref.symbols) {
      if (!symbolExists(text, symbol))
        findings.push({ ref, reason: 'missing-symbol', detail: `${path} has no ${symbol}`, symbol });
    }
  }
  return findings;
}

function refKey(spec: string, kind: SpecRefKind, raw: string, symbol: string | undefined): string {
  return `${spec}|${kind}|${raw}|${symbol ?? ''}`;
}

/**
 * The ratchet: every finding has exactly one ledger row, each row counts exactly the findings it
 * claims, and each row names its reason and an existing defect brief.
 */
export function findSpecLinkViolations(
  findings: readonly SpecLinkFinding[],
  ledger: readonly SpecLinkRow[],
  defectExists: (path: string) => boolean,
): SpecLinkViolation[] {
  const violations: SpecLinkViolation[] = [];
  const rows = new Map<string, SpecLinkRow>();
  for (const row of ledger) {
    const key = refKey(row.spec, row.kind, row.raw, row.symbol);
    if (rows.has(key)) violations.push({ kind: 'duplicate-row', row });
    else rows.set(key, row);
    if (row.reason.trim() === '' || !defectExists(row.defect)) violations.push({ kind: 'unowned', row });
  }
  const found = new Map<string, number>();
  for (const finding of findings) {
    const key = refKey(finding.ref.spec, finding.ref.kind, finding.ref.raw, finding.symbol);
    if (rows.has(key)) found.set(key, (found.get(key) ?? 0) + 1);
    else violations.push({ kind: 'unledgered', finding });
  }
  for (const [key, row] of rows) {
    const count = found.get(key) ?? 0;
    if (count !== row.count) violations.push({ kind: 'count', row, found: count });
  }
  return violations;
}

/** Every reference in tracked specs that does not resolve against the tracked tree. */
export async function scanSpecLinks(): Promise<{ checked: number; findings: SpecLinkFinding[] }> {
  const git = Bun.spawn(['git', 'ls-files', '--cached', '--others', '--exclude-standard', '-z'], {
    cwd: REPO_ROOT,
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const [out, diagnostic, exitCode] = await Promise.all([
    new Response(git.stdout).text(),
    new Response(git.stderr).text(),
    git.exited,
  ]);
  if (exitCode !== 0) throw new Error(`git ls-files failed with exit code ${exitCode}: ${diagnostic.trim()}`);
  const files = out.split('\0').filter((f) => f !== '');
  const cache = new Map<string, string | undefined>();
  const textOf = (path: string): string | undefined => {
    if (cache.has(path)) return cache.get(path);
    let text: string | undefined;
    try {
      text = readFileSync(resolve(REPO_ROOT, path), 'utf8');
    } catch {
      text = undefined;
    }
    cache.set(path, text);
    return text;
  };
  const refs = files
    .filter((f) => f.startsWith('specs/') && f.endsWith('.md'))
    .sort()
    .flatMap((spec) => {
      const text = textOf(spec);
      return text === undefined ? [] : extractRefs(spec, text);
    });
  return { checked: refs.length, findings: judgeRefs(refs, { files, textOf }) };
}

export function formatViolation(violation: SpecLinkViolation): string {
  switch (violation.kind) {
    case 'unledgered': {
      const f = violation.finding;
      return (
        f.ref.spec +
        ':' +
        f.ref.line +
        ': ' +
        f.reason +
        ': ' +
        f.ref.raw +
        (f.symbol === undefined ? '' : ` (${f.symbol})`) +
        ' — ' +
        f.detail
      );
    }
    case 'count':
      return (
        violation.row.spec +
        ': ledger row ' +
        violation.row.raw +
        ' counts ' +
        violation.row.count +
        ' occurrence(s) but the spec has ' +
        violation.found
      );
    case 'duplicate-row':
      return `${violation.row.spec}: two ledger rows claim ${violation.row.raw}`;
    case 'unowned':
      return (
        violation.row.spec +
        ': ledger row ' +
        violation.row.raw +
        ' needs a reason and a tracked defect brief (has ' +
        JSON.stringify(violation.row.defect) +
        ')'
      );
  }
}

if (import.meta.main) {
  const { checked, findings } = await scanSpecLinks();
  const violations = findSpecLinkViolations(findings, SPEC_LINKS, (path) => existsSync(resolve(REPO_ROOT, path)));
  if (violations.length === 0) {
    const sites = SPEC_LINKS.reduce((total, row) => total + row.count, 0);
    console.log(
      'Spec-link check passed: ' +
        checked +
        ' code reference(s) in specs checked; ' +
        SPEC_LINKS.length +
        ' ledger row(s) hold ' +
        sites +
        ' broken reference(s).',
    );
    process.exit(0);
  }
  console.error('Broken spec references the ledger does not account for:\n');
  console.error(violations.map(formatViolation).join('\n'));
  console.error(
    '\nFix the reference so it names the code that exists (or the code so it carries the symbol the spec rests ' +
      'on); a fixed reference deletes or lowers its row in tooling/checks/spec-links.ledger.ts in the same commit. ' +
      'Ledger a reference only when it names another tree, with its reason and owning defect.',
  );
  process.exit(1);
}
