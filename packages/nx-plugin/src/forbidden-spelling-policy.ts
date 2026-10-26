// FORBIDDEN-SPELLING: the AGENTS.md workarounds that have one spelling, judged on the lines a change adds. A rule here
// refuses a NEW site and removes none: existing sites need no allowance table, and the fixture source builds each
// forbidden spelling at runtime, so no repository file needs an exemption from the boundary it tests. The rules are
// pure (no I/O, no clock); `findForbiddenSpellings` is the shell that reads the files a `diffScope` names.
//
// What it cannot do. It reads one line at a time, so an argv list split across lines is judged by its first line only,
// and a quoted program whose arguments live in a variable (`probe('lsof', args)`) shows no option to judge: a quoted
// census is refused only where the line itself names a selecting option and no scoping one, while an unquoted shell
// command (`lsof | head`, `pgrep node`, `run: lsof`) is a census unless it names a pid scope. A spelling assembled at
// runtime or passed through a variable is invisible; a trailing comment is part of its code line; prose in a string
// literal that sits in command position reads as a command; a ledger kept inside a file with another name is not seen.
// Comment-only lines, Markdown and binary or generated text (`NOT_SCANNED`) are not read at all. A pure deletion
// changes no line it keeps, so its range is marked and no line rule judges it.
import { lstatSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import type { ChangeRange, DiffScope } from './test-walk-policy.js';

export type LineRuleId = 'NO-SKIP-NX-CACHE' | 'NO-HOST-CENSUS';
export type PathRuleId = 'NO-WAIVER-LEDGER';
export type SpellingRuleId = LineRuleId | PathRuleId;

export interface SpellingRule {
  readonly id: SpellingRuleId;
  /** The AGENTS.md rule this check enforces, by name. */
  readonly rule: 'Diagnosis' | 'Proof' | 'Workarounds';
  readonly message: string;
  readonly help: string;
}

export const SPELLING_RULES: Readonly<Record<SpellingRuleId, SpellingRule>> = {
  'NO-SKIP-NX-CACHE': {
    id: 'NO-SKIP-NX-CACHE',
    rule: 'Diagnosis',
    message: 'a cache bypass manufactures a pass',
    help:
      'A task that passes only with the Nx cache bypassed has a broken input, output or dependency declaration. ' +
      'Repair that at its owner; never bypass the cache, not even to diagnose.',
  },
  'NO-HOST-CENSUS': {
    id: 'NO-HOST-CENSUS',
    rule: 'Proof',
    message: 'a walk of the whole host reads processes nobody gave this code',
    help:
      'Start from pids the caller holds (a child it spawned, a daemon record) and read exactly those with `ps -p`, ' +
      '`lsof -p` or `pgrep -P`. A host-wide census asks every process, and one in an uninterruptible kernel wait ' +
      'holds the census with it; a signal chosen by name reaches a peer’s processes.',
  },
  'NO-WAIVER-LEDGER': {
    id: 'NO-WAIVER-LEDGER',
    rule: 'Workarounds',
    message: 'a new ledger of known-broken sites is a waiver list',
    help:
      'Fix the sites. A check that cannot pass on the current tree judges the lines a change adds instead, so it needs ' +
      'no list of current offenders; only an existing ledger may shrink.',
  },
};

/** Every line rule, in the order a line's findings are reported. */
export const LINE_RULES: readonly LineRuleId[] = ['NO-SKIP-NX-CACHE', 'NO-HOST-CENSUS'];

export interface SpellingFinding {
  readonly id: SpellingRuleId;
  readonly line: number;
  readonly column: number;
  readonly text: string;
}

/** A finding in one file of a change. */
export interface FileSpellingFinding extends SpellingFinding {
  readonly file: string;
}

// Prose and recorded data quote the forbidden forms as policy or history; every other text surface is read, including
// extensionless scripts and dot-configs. Patches carry upstream text, lock files and logs carry tool output.
const NOT_SCANNED =
  /\.(?:mdx?|txt|lock|log|svgz?|png|jpe?g|gif|webp|ico|pdf|woff2?|ttf|otf|mp[34]|mov|zip|gz|zst|wasm|a|so|dylib|dll|exe|db|sqlite|patch|diff|jsonl)$/u;

/** A line that is only a comment: `//`, `/*`, a block continuation `* text`, `# text` and `<!--`. */
const COMMENT_ONLY = /^\s*(?:\/\/|\/\*|\*(?:\s|\/|$)|#(?:\s|$)|<!--)/u;

// Each pattern spells its own subject through a one-letter class (`[c]ache`), so the pattern's source line does not
// match the pattern.
const SKIP_CACHE_FLAG =
  /(?<![\w-])--(?:skip-nx-[c]ache|disable-nx-[c]ache|skipNx[C]ache|disableNx[C]ache)(?![\w-]|=(?:false|0)\b)/u;
// Nx honors the switch only as the string `true`: a shell assignment, a property write, or an object, YAML or JSON key.
const SKIP_CACHE_ENV = /(?<!\w)NX_(?:SKIP|DISABLE)_NX_[C]ACHE["']?\s*[=:]\s*["']?true\b/u;
const PROC_LISTING =
  /(?:read_dir|readdir(?:Sync)?|listdir|scandir|iterdir|glob(?:Sync)?)\s*\(\s*(?:Path::new\(\s*)?["']\/proc\/?["']|(?<![\w/.-])(?:ls|find)(?:\s+[^\s|;&]+)*?\s+\/proc\/?(?=\s|$)/u;

/**
 * A program in command position (group 1 is what leads it): line start, a YAML `- ` or `key: ` prefix, a shell separator,
 * `sudo`/`exec`/`env`/`xargs` with their options and assignments, or a quote (an argv element or a quoted shell string).
 */
const CENSUS_PROGRAM =
  /(^\s*(?:-\s+)?(?:[\w.-]+:\s+)?|[;&|({`'"]\s*|\$\(\s*|\b(?:sudo|exec|env|xargs|command)\s+(?:-\S+\s+|\w+=\S*\s+)*)(?:\/(?:usr\/(?:local\/)?)?s?bin\/)?(ps|l[s]of|pg[r]ep|p[k]ill|kill[a]ll)(?=$|[\s;&|)}`'",\]])/gu;

/** The words after a program, up to the end of its pipeline segment: shell words and argv elements alike. */
function argumentWords(rest: string): string[] {
  const segment = rest.split(/[;&|]/u, 1)[0] ?? '';
  return segment.split(/[\s,[\]()'"`]+/u).filter((word) => word !== '');
}

/**
 * Whether `program` called with the words visible on its line reads every process of the host rather than pids the
 * caller holds: a selecting option and no scoping one (`-p`, `-P`, `-g`, `--pid`). `bare` is an unquoted shell command,
 * where `lsof` and `pgrep` select every process unless a pid scope says otherwise; a quoted program may take its
 * arguments from a variable, so it needs a visible selecting option. A signal chosen by name is refused wherever it is
 * spelled in command position.
 */
export function readsWholeHost(program: string, words: readonly string[], bare: boolean): boolean {
  const clusters = words.flatMap((word) => (/^-[A-Za-z]+$/u.test(word) ? [word.slice(1)] : []));
  const has = (letters: RegExp): boolean => clusters.some((cluster) => letters.test(cluster));
  const long = (names: RegExp): boolean => words.some((word) => names.test(word));
  if (/^(?:p[k]ill|kill[a]ll)$/u.test(program)) return true;
  switch (program) {
    case 'pgrep':
      return (bare || clusters.length > 0) && !has(/[PgGs]/u) && !long(/^--(?:parent|pgroup|session)/u);
    case 'lsof':
      // `-p` takes the pids; `-nP` is not `-p`, `-p123` and `-ap` are.
      return (
        (bare || clusters.length > 0 || words.some((word) => /^\+[Dd]$/u.test(word))) &&
        !words.some((word) => /^-(?:[A-Za-z]*p|p\S+)$/u.test(word))
      );
    case 'ps': {
      // BSD spelling `ps aux`: a first word of selection letters with no dash.
      const first = words[0] ?? '';
      const bsd = /^[auxwfeElLjvr]+$/u.test(first) && /[aex]/u.test(first);
      return !(has(/[pg]/u) || long(/^--(?:pid|ppid|sid|group)/u)) && (has(/[AaeExu]/u) || bsd);
    }
    default:
      return false;
  }
}

function censusColumn(line: string): number | undefined {
  for (const match of line.matchAll(CENSUS_PROGRAM)) {
    const program = match[2] ?? '';
    const start = match.index ?? 0;
    const words = argumentWords(line.slice(start + match[0].length));
    if (readsWholeHost(program, words, !/['"`]\s*$/u.test(match[1] ?? '')))
      return start + match[0].lastIndexOf(program);
  }
  return PROC_LISTING.exec(line)?.index;
}

function lineColumn(id: LineRuleId, line: string): number | undefined {
  switch (id) {
    case 'NO-SKIP-NX-CACHE':
      return (SKIP_CACHE_FLAG.exec(line) ?? SKIP_CACHE_ENV.exec(line))?.index;
    case 'NO-HOST-CENSUS':
      return censusColumn(line);
  }
}

/** The text of one file the rules read: not prose, not data, not a binary. */
export function isScanned(path: string): boolean {
  return !NOT_SCANNED.test(path);
}

/** The line rules' findings in `source`, on the lines `ranges` name only. */
export function findLineSpellings(
  source: string,
  ranges: readonly ChangeRange[],
  ids: readonly LineRuleId[],
): SpellingFinding[] {
  if (source.includes('\0')) return [];
  const findings: SpellingFinding[] = [];
  for (const [index, text] of source.split('\n').entries()) {
    const line = index + 1;
    if (!ranges.some((range) => range.deleted !== true && line >= range.start && line <= range.end)) continue;
    if (COMMENT_ONLY.test(text)) continue;
    for (const id of ids) {
      const column = lineColumn(id, text);
      if (column !== undefined) findings.push({ id, line, column: column + 1, text: text.trim() });
    }
  }
  return findings;
}

const LEDGER_PATH = /(?:^|\/)[^/]*(?:\.ledger\.[A-Za-z]+|known-(?:broken|failures)[^/]*)$/u;

/** A path rule's finding for a file this change adds. */
export function findAddedPathSpelling(path: string): SpellingFinding | undefined {
  return LEDGER_PATH.test(path) ? { id: 'NO-WAIVER-LEDGER', line: 1, column: 1, text: path } : undefined;
}

/** A complete scan, or the exact input whose inspection was unavailable. */
export type ForbiddenSpellingScan =
  | { readonly kind: 'scanned'; readonly findings: readonly FileSpellingFinding[] }
  | { readonly kind: 'unavailable'; readonly file: string; readonly detail: string };

/**
 * Every rule's findings in the change `scope` names, under `root`: the path rule on each file the change adds, and the
 * line rules on the changed lines of each scanned regular file. A missing path is inactive (a deleted file), a symlink
 * or directory is not read, and a binary is skipped; every other I/O failure makes the whole scan unavailable.
 */
export function findForbiddenSpellings(root: string, scope: DiffScope): ForbiddenSpellingScan {
  const findings: FileSpellingFinding[] = [];
  for (const file of scope.files) {
    const added = scope.added.has(file) ? findAddedPathSpelling(file) : undefined;
    if (added !== undefined) findings.push({ file, ...added });
    const ranges = scope.changes.get(file);
    if (ranges === undefined || !isScanned(file)) continue;
    let source: string;
    try {
      const path = join(root, file);
      if (!lstatSync(path).isFile()) continue;
      source = readFileSync(path, 'utf8');
    } catch (error) {
      if (error instanceof Error && 'code' in error && error.code === 'ENOENT') continue;
      return { kind: 'unavailable', file, detail: error instanceof Error ? error.message : String(error) };
    }
    for (const finding of findLineSpellings(source, ranges, LINE_RULES)) findings.push({ file, ...finding });
  }
  return { kind: 'scanned', findings };
}

/** One finding in the shape every check of this tree prints. */
export function formatSpellingFinding(file: string, finding: SpellingFinding): string {
  const rule = SPELLING_RULES[finding.id];
  return (
    `error[${rule.id}]: ${rule.message}: ${finding.text}\n` +
    `  --> ${file}:${finding.line}:${finding.column}\n` +
    `  = help: AGENTS.md ${rule.rule} rule. ${rule.help}`
  );
}
