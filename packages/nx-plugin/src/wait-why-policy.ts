import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { type RustToken, rustTokens } from './rust-tokens.js';

/**
 * WAIT-WHY (specs/cowshed/05_gateway.md, "Deadlines", Enforcement). Clippy refuses each cross-process wait a crate's
 * `clippy.toml` disallows; a site kept unbounded is suppressed for its one item or statement, and this check refuses
 * the suppression unless an adjacent `// WHY unbounded:` says what ends the wait. A reason that records `debt:` names
 * a stall the site leaves, not an end, and is refused: the site is bounded instead.
 */
export const WAIT_WHY = 'WAIT-WHY';

/** The waits every enrolled `clippy.toml` disallows, each named in its entry's `path`. */
export const WAIT_METHODS = [
  'std::process::Child::wait',
  'std::process::Child::wait_with_output',
  'tokio::process::Child::wait',
  'tokio::process::Child::wait_with_output',
  'tokio::process::Child::kill',
  'cowshed_os::fork_lock::Run::output_locked',
  'cowshed_os::fork_lock::Run::status_locked',
  'cowshed_os::fork_lock::RunAsync::output_locked',
  'cowshed_os::fork_lock::RunAsync::status_locked',
  'std::process::Command::output',
  'std::process::Command::status',
  'tokio::process::Command::output',
  'tokio::process::Command::status',
  'std::os::unix::net::UnixStream::connect',
  'tokio::net::UnixStream::connect',
  'libc::flock',
  'std::fs::File::lock',
  'std::fs::File::lock_shared',
  'libc::waitpid',
  'libc::waitid',
  'libc::wait4',
] as const;

/** A suppression covers a wait when a call it covers names one of these. */
const WAIT_NAMES: ReadonlySet<string> = new Set(WAIT_METHODS.map((path) => path.slice(path.lastIndexOf('::') + 2)));

/** The reason, with text after it: an empty `// WHY unbounded:` explains nothing. */
const WHY = /\/\/\s*WHY unbounded:\s*\S/u;
/** A reason that records debt: a waiver naming the stall it leaves, never what ends the wait. */
const DEBT = /\bdebt:/u;

const OPENERS = new Set(['(', '[', '{']);
const CLOSERS = new Set([')', ']', '}']);

export interface WaitWhyFinding {
  readonly path: string;
  readonly line: number;
  readonly message: string;
}

/** The index of the closer matching the opener at `open`, or the last token when the source ends first. */
function closing(tokens: readonly RustToken[], open: number): number {
  let depth = 0;
  for (let index = open; index < tokens.length; index += 1) {
    const text = tokens[index]?.text ?? '';
    if (OPENERS.has(text)) depth += 1;
    else if (CLOSERS.has(text)) {
      depth -= 1;
      if (depth === 0) return index;
    }
  }
  return tokens.length - 1;
}

/** `[start, end)` of the item or statement an outer attribute ending before `start` applies to. */
function following(tokens: readonly RustToken[], start: number): readonly [number, number] {
  let first = start;
  // Further attributes on the same item.
  while (tokens[first]?.text === '#' && tokens[first + 1]?.text === '[') first = closing(tokens, first + 1) + 1;
  let depth = 0;
  for (let index = first; index < tokens.length; index += 1) {
    const text = tokens[index]?.text ?? '';
    if (OPENERS.has(text)) depth += 1;
    else if (CLOSERS.has(text)) {
      // The enclosing block closes: the attribute was on its last expression.
      if (depth === 0) return [first, index];
      depth -= 1;
      if (depth === 0 && text === '}') {
        const next = tokens[index + 1]?.text;
        if (next === ';' || next === ',') return [first, index + 2];
        if (next !== '.' && next !== '?' && next !== 'else') return [first, index + 1];
      }
    } else if (depth === 0 && (text === ';' || text === ',')) return [first, index + 1];
  }
  return [first, tokens.length];
}

/** `[start, end)` of the scope an inner attribute ending before `start` applies to: the rest of its block. */
function enclosing(tokens: readonly RustToken[], start: number): readonly [number, number] {
  let depth = 0;
  for (let index = start; index < tokens.length; index += 1) {
    const text = tokens[index]?.text ?? '';
    if (OPENERS.has(text)) depth += 1;
    else if (CLOSERS.has(text)) {
      if (depth === 0) return [start, index];
      depth -= 1;
    }
  }
  return [start, tokens.length];
}

/** `allow(…)`/`expect(…)` naming `clippy::disallowed_methods`, also inside `cfg_attr`. */
function suppressesDisallowedMethods(attribute: readonly RustToken[]): boolean {
  const suppresses = attribute.some((token) => token.text === 'allow' || token.text === 'expect');
  return (
    suppresses &&
    attribute.some(
      (token, index) =>
        token.text === 'disallowed_methods' &&
        attribute[index - 1]?.text === '::' &&
        attribute[index - 2]?.text === 'clippy',
    )
  );
}

/**
 * The 1-based lines of each suppression of `clippy::disallowed_methods` in `source` that covers a call named like a
 * disallowed wait and has no `// WHY unbounded: <reason>` beside it, or whose reason records `debt:`. Beside it
 * means: in the comment and attribute lines directly above the attribute, on the attribute's own lines, or on the
 * first line of what it covers.
 */
export function unexplainedWaitAllows(source: string): number[] {
  const tokens = rustTokens(source);
  const lines = source.split('\n');
  const starts = [0];
  for (let index = source.indexOf('\n'); index !== -1; index = source.indexOf('\n', index + 1)) starts.push(index + 1);
  const lineOf = (offset: number): number => {
    let low = 0;
    let high = starts.length - 1;
    while (low < high) {
      const middle = (low + high + 1) >> 1;
      if ((starts[middle] ?? 0) <= offset) low = middle;
      else high = middle - 1;
    }
    return low + 1;
  };
  const found: number[] = [];
  for (let index = 0; index < tokens.length; index += 1) {
    if (tokens[index]?.text !== '#') continue;
    const inner = tokens[index + 1]?.text === '!';
    const open = index + (inner ? 2 : 1);
    if (tokens[open]?.text !== '[') continue;
    const close = closing(tokens, open);
    if (!suppressesDisallowedMethods(tokens.slice(open + 1, close))) continue;
    const [first, end] = inner ? enclosing(tokens, close + 1) : following(tokens, close + 1);
    const covered = tokens.slice(first, end);
    const calls = covered.some(
      (token, at) => token.kind === 'ident' && WAIT_NAMES.has(token.text) && covered[at + 1]?.text === '(',
    );
    if (!calls) continue;
    const attributeLine = lineOf(tokens[index]?.offset ?? 0);
    const coveredLine = covered[0] === undefined ? attributeLine : lineOf(covered[0].offset);
    let above = attributeLine - 1;
    while (above >= 1 && /^\s*(?:\/\/|#!?\[)/u.test(lines[above - 1] ?? '')) above -= 1;
    const beside = lines.slice(above, coveredLine);
    if (!beside.some((line) => WHY.test(line)) || beside.some((line) => DEBT.test(line))) found.push(attributeLine);
  }
  return found;
}

function rustFiles(directory: string): string[] {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) return entry.name === 'target' ? [] : rustFiles(path);
    return entry.name.endsWith('.rs') ? [path] : [];
  });
}

/**
 * WAIT-WHY over every crate directly under `crates` that has a `clippy.toml`: that file disallows every wait in
 * {@link WAIT_METHODS}, and every suppression in the crate covering one says why it is kept.
 */
export function checkWaitWhy(crates: string): WaitWhyFinding[] {
  const findings: WaitWhyFinding[] = [];
  for (const crate of readdirSync(crates, { withFileTypes: true })) {
    const root = join(crates, crate.name);
    const config = join(root, 'clippy.toml');
    if (!crate.isDirectory() || !existsSync(config)) continue;
    const toml = readFileSync(config, 'utf8');
    for (const method of WAIT_METHODS) {
      if (!toml.includes(`path = "${method}"`)) {
        findings.push({
          path: relative(crates, config),
          line: 1,
          message: `disallowed-methods lacks \`${method}\`; every crate with a clippy.toml disallows each wait`,
        });
      }
    }
    for (const file of rustFiles(root)) {
      for (const line of unexplainedWaitAllows(readFileSync(file, 'utf8'))) {
        findings.push({
          path: relative(crates, file),
          line,
          message:
            'allow(clippy::disallowed_methods) covers a cross-process wait without an adjacent ' +
            '`// WHY unbounded: <what ends the wait>`, or with one that records debt: instead of an end; bound it ' +
            'with cowshed_os::deadline or say what ends it',
        });
      }
    }
  }
  return findings;
}

export function formatWaitWhy(finding: WaitWhyFinding): string {
  return `${finding.path}:${finding.line}: error[${WAIT_WHY}] ${finding.message}`;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const crates = process.argv.slice(2);
  if (crates.length === 0) {
    process.stderr.write('usage: wait-why-policy.ts <crates-directory>...\n');
    process.exit(2);
  }
  for (const directory of crates) {
    const findings = checkWaitWhy(directory);
    for (const finding of findings) process.stderr.write(`${directory}/${formatWaitWhy(finding)}\n`);
    if (findings.length > 0) process.exitCode = 1;
    else process.stdout.write(`${WAIT_WHY} passed: every kept wait under ${directory} says what ends it\n`);
  }
}
