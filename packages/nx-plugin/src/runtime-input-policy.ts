import { accessSync, constants, readdirSync, readFileSync, realpathSync, statSync } from 'node:fs';
import { delimiter, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { RUNTIME_INPUT_SCRIPT } from './runtime-input.js';

/**
 * What a runtime input may run, and how its failures are read back after Nx refuses it.
 *
 * A runtime input runs wherever Nx hashes: a developer's Mac, a NixOS CI runner, a cowshed sandbox. Only the programs a
 * workspace declares are the same in all of them: devenv's Nix profile and the checkout's own `node_modules`. A
 * program the host's operating system ships is not. macOS ships `shasum` in /usr/bin; it is a Perl script, not
 * coreutils, and no devenv profile provides it (devenv's carries coreutils, `sha256sum` included). An input that named
 * it hashed on every Mac and exited 127 on the NixOS runner, which Nx reported only as
 * `invalid utf-8 sequence of 1 bytes from index 0` (runtime-input.sh says why).
 */

/** Words a POSIX `sh` runs itself, without a PATH lookup. */
const SHELL_BUILTINS: ReadonlySet<string> = new Set([
  '.',
  ':',
  '[',
  'alias',
  'break',
  'cd',
  'continue',
  'echo',
  'eval',
  'exit',
  'export',
  'false',
  'getopts',
  'hash',
  'local',
  'printf',
  'pwd',
  'read',
  'readonly',
  'return',
  'set',
  'shift',
  'test',
  'times',
  'trap',
  'true',
  'type',
  'ulimit',
  'umask',
  'unalias',
  'unset',
  'wait',
]);

/** Reserved words that may stand before a command's program. */
const SHELL_KEYWORDS: ReadonlySet<string> = new Set([
  '!',
  '{',
  '}',
  'if',
  'then',
  'else',
  'elif',
  'fi',
  'do',
  'done',
  'while',
  'until',
]);

/** Reserved words whose clause names no program (`for name in words`, `case word in`). */
const CLAUSE_KEYWORDS: ReadonlySet<string> = new Set(['for', 'case', 'esac', 'in']);

/** Shells whose `-c` operand is a script of further commands. */
const SHELLS: ReadonlySet<string> = new Set(['sh', 'bash']);

const ASSIGNMENT = /^[A-Za-z_][A-Za-z0-9_]*=/;

type Token =
  | { readonly kind: 'word'; readonly text: string }
  | { readonly kind: 'separator' }
  | { readonly kind: 'redirect' };

/** The index of the character that closes the quote opened before `from`, or the end of `text` when none does. */
function closingQuote(text: string, from: number, quote: string): number {
  for (let at = from; at < text.length; at += 1) {
    if (text[at] === '\\' && quote !== "'") at += 1;
    else if (text[at] === quote) return at;
  }
  return text.length;
}

/** The index of the `close` that balances the `open` before `from`, skipping quoted text, or the end of `text`. */
function closingBracket(text: string, from: number, open: string, close: string): number {
  let depth = 1;
  for (let at = from; at < text.length; at += 1) {
    const character = text[at];
    if (character === '\\') at += 1;
    else if (character === "'" || character === '"' || character === '`') at = closingQuote(text, at + 1, character);
    else if (character === open) depth += 1;
    else if (character === close) {
      depth -= 1;
      if (depth === 0) return at;
    }
  }
  return text.length;
}

/**
 * The shell tokens of `script`. A command substitution is part of its word, and the programs it runs are appended to
 * `nested`; quoting is removed from the words, which are only inspected, never run.
 */
export function shellTokens(script: string, nested: string[]): Token[] {
  const tokens: Token[] = [];
  let word: string | undefined;
  const flush = (): void => {
    if (word !== undefined) tokens.push({ kind: 'word', text: word });
    word = undefined;
  };
  const append = (text: string): void => {
    word = (word ?? '') + text;
  };
  /** Consumes the expansion starting with `$` or a backquote at `at`; returns the index after it. */
  const expansion = (at: number): number => {
    if (script[at] === '`') {
      const end = closingQuote(script, at + 1, '`');
      nested.push(...commandPrograms(script.slice(at + 1, end)));
      append('`…`');
      return end + 1;
    }
    if (script.startsWith('$((', at)) {
      const end = closingBracket(script, at + 2, '(', ')');
      append(script.slice(at, end + 1));
      return end + 1;
    }
    if (script.startsWith('$(', at)) {
      const end = closingBracket(script, at + 2, '(', ')');
      nested.push(...commandPrograms(script.slice(at + 2, end)));
      append('$(…)');
      return end + 1;
    }
    if (script.startsWith('${', at)) {
      const end = closingBracket(script, at + 2, '{', '}');
      append(script.slice(at, end + 1));
      return end + 1;
    }
    append('$');
    return at + 1;
  };
  let at = 0;
  while (at < script.length) {
    const character = script[at];
    if (character === "'") {
      const end = closingQuote(script, at + 1, "'");
      append(script.slice(at + 1, end));
      at = end + 1;
    } else if (character === '"') {
      append('');
      at += 1;
      while (at < script.length && script[at] !== '"') {
        if (script[at] === '\\' && at + 1 < script.length) {
          append('$`"\\\n'.includes(script[at + 1]) ? script[at + 1] : script.slice(at, at + 2));
          at += 2;
        } else if (script[at] === '$' || script[at] === '`') {
          at = expansion(at);
        } else {
          append(script[at]);
          at += 1;
        }
      }
      at += 1;
    } else if (character === '\\') {
      append(script.slice(at + 1, at + 2));
      at += 2;
    } else if (character === '$' || character === '`') {
      at = expansion(at);
    } else if (character === '#' && word === undefined) {
      at = script.indexOf('\n', at);
      if (at < 0) at = script.length;
    } else if (character === ' ' || character === '\t') {
      flush();
      at += 1;
    } else if (';&|()\n'.includes(character)) {
      flush();
      tokens.push({ kind: 'separator' });
      at += 1;
    } else if (character === '<' || character === '>') {
      // A file-descriptor number written against the operator (`2>&1`) is part of it, not a word.
      if (word !== undefined && /^\d+$/.test(word)) word = undefined;
      flush();
      at += 1;
      while (at < script.length && '<>&|'.includes(script[at])) at += 1;
      tokens.push({ kind: 'redirect' });
    } else {
      append(character);
      at += 1;
    }
  }
  flush();
  return tokens;
}

/** The programs one simple command runs, given its words in order. */
function simpleCommandPrograms(words: readonly string[]): string[] {
  let at = 0;
  while (at < words.length && (SHELL_KEYWORDS.has(words[at]) || ASSIGNMENT.test(words[at]))) at += 1;
  const program = words[at];
  if (program === undefined || CLAUSE_KEYWORDS.has(program)) return [];
  const operands = words.slice(at + 1);
  if (program === 'exec') return simpleCommandPrograms(operands);
  // `command -v name` looks a name up; `command name` runs it.
  if (program === 'command') return operands[0]?.startsWith('-') ? [] : simpleCommandPrograms(operands);
  if (program === 'env') {
    let operand = 0;
    while (operand < operands.length && operands[operand].startsWith('-') && operands[operand] !== '--') {
      operand += operands[operand] === '-u' || operands[operand] === '-C' ? 2 : 1;
    }
    if (operands[operand] === '--') operand += 1;
    return [program, ...simpleCommandPrograms(operands.slice(operand))];
  }
  if (SHELLS.has(program)) {
    // The wrapper runs its operands as one command.
    if (operands[0] === RUNTIME_INPUT_SCRIPT) return [program, ...simpleCommandPrograms(operands.slice(1))];
    const options = operands.findIndex((operand) => !operand.startsWith('-'));
    const flags = operands.slice(0, options < 0 ? operands.length : options);
    const script = options < 0 ? undefined : operands[options];
    if (script !== undefined && flags.some((flag) => flag !== '--' && !flag.startsWith('--') && flag.includes('c'))) {
      return [program, ...commandPrograms(script)];
    }
  }
  return [program];
}

/**
 * The programs `script` runs by name or path: the program of each simple command, through `env`, `exec`,
 * `command`, `sh -c` and command substitutions, and the command a `runtime-input.sh` input runs. Shell builtins are
 * not programs. A script file a command runs (`sh tooling/x.sh`) is its own source's concern, not read here.
 */
export function commandPrograms(script: string): string[] {
  const programs: string[] = [];
  const nested: string[] = [];
  let words: string[] = [];
  let redirectTarget = false;
  const end = (): void => {
    programs.push(...simpleCommandPrograms(words));
    words = [];
  };
  for (const token of shellTokens(script, nested)) {
    if (token.kind === 'separator') end();
    else if (token.kind === 'redirect') redirectTarget = true;
    else if (redirectTarget) redirectTarget = false;
    else words.push(token.text);
  }
  end();
  return [...programs, ...nested].filter((program) => !SHELL_BUILTINS.has(program));
}

/** Where a runtime input's programs are looked up, and the locations that count as declared rather than the host's. */
export interface ProgramSearch {
  /** The PATH the input runs with: Nx passes its own environment through. */
  readonly path: string;
  /** The workspace root: its `node_modules/.bin` and tooling are declared by the checkout. */
  readonly root: string;
  /**
   * The user's home. A developer's devenv shell keeps its PATH entries in the checkout direnv loaded (a main
   * checkout's `node_modules/.bin` while a shed is judged) and the user's own tools there; neither is the OS image.
   */
  readonly home: string;
}

/** A program a runtime input runs that the workspace does not declare, and why it is refused. */
export type HostProgram =
  /** PATH finds it outside /nix, the checkout, the home and /bin/sh or /usr/bin/env. */
  | { readonly program: string; readonly refusal: 'host'; readonly found: string }
  /** Nothing on PATH provides it. */
  | { readonly program: string; readonly refusal: 'absent' }
  /**
   * PATH finds it in the checkout's `node_modules/.bin` as a link to a build output of a package in this checkout. Bun
   * links a workspace package's bin only when its target exists at install time, and a fresh checkout installs before
   * it builds, so the name is absent there while every checkout that built first has it.
   */
  | { readonly program: string; readonly refusal: 'workspace-bin'; readonly found: string; readonly target: string };

function within(directory: string, file: string): boolean {
  const path = relative(directory, file);
  return path !== '' && !path.startsWith('..') && !isAbsolute(path);
}

function isExecutableFile(file: string): boolean {
  try {
    accessSync(file, constants.X_OK);
    return statSync(file).isFile();
  } catch (error) {
    if (error instanceof Error && 'code' in error && ['ENOENT', 'ENOTDIR', 'EACCES'].includes(String(error.code))) {
      return false;
    }
    throw error;
  }
}

function onPath(program: string, path: string): string | undefined {
  for (const directory of path.split(delimiter)) {
    // An empty entry means the working directory; Nx runs inputs from the root, which a name never resolves into.
    if (directory === '') continue;
    const candidate = join(directory, program);
    if (isExecutableFile(candidate)) return candidate;
  }
  return undefined;
}

/**
 * The only host paths every platform guarantees: NixOS keeps exactly these two outside the store, for `#!` lines and
 * for Nx, which runs every runtime input as `sh -c`.
 */
const GUARANTEED_HOST_PROGRAMS: ReadonlySet<string> = new Set(['/bin/sh', '/usr/bin/env']);
/**
 * The build output of a package in this checkout that `found`, a link in the checkout's `node_modules/.bin`, names;
 * undefined for any other file, an installed package's bin included.
 */
function workspaceBuildOutput(found: string, root: string): string | undefined {
  if (!within(join(root, 'node_modules', '.bin'), found)) return undefined;
  const physicalRoot = realpathSync(root);
  const target = realpathSync(found);
  return within(physicalRoot, target) && !relative(physicalRoot, target).split(sep).includes('node_modules')
    ? target
    : undefined;
}

/**
 * Every program `command` runs that the workspace does not declare: one PATH finds outside the Nix store, the
 * checkout, the home and the guaranteed host paths, one nothing on PATH provides, or one only a workspace package's
 * build output provides. A relative path names a file in the checkout.
 */
export function hostPrograms(command: string, search: ProgramSearch): HostProgram[] {
  const declared = (file: string): boolean =>
    file.startsWith('/nix/') ||
    GUARANTEED_HOST_PROGRAMS.has(file) ||
    within(search.root, file) ||
    within(search.home, file);
  return [...new Set(commandPrograms(command))].flatMap((program): HostProgram[] => {
    if (program.includes('/')) {
      const found = resolve(search.root, program);
      return declared(found) ? [] : [{ program, refusal: 'host', found }];
    }
    const found = onPath(program, search.path);
    if (found === undefined) return [{ program, refusal: 'absent' }];
    if (!declared(found)) return [{ program, refusal: 'host', found }];
    const target = workspaceBuildOutput(found, search.root);
    return target === undefined ? [] : [{ program, refusal: 'workspace-bin', found, target }];
  });
}

/** Why `host` is refused, naming where PATH found it and what to run instead. */
export function hostProgramReason(host: HostProgram): string {
  switch (host.refusal) {
    case 'host':
      return `PATH finds ${host.program} at ${host.found}, a host program no devenv profile declares`;
    case 'absent':
      return `nothing on PATH provides ${host.program}`;
    case 'workspace-bin':
      return (
        `PATH finds ${host.program} at ${host.found}, a link to this checkout's build output ${host.target}, ` +
        'which an install that runs before the build does not create; run that file with node or bun instead'
      );
  }
}

/** The directory under the workspace root where runtime-input.sh retains each failed input. */
export const RUNTIME_INPUT_FAILURES = '.runtime-input-failures';

function absentAsUndefined<T>(read: () => T): T | undefined {
  try {
    return read();
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return undefined;
    throw error;
  }
}

/**
 * The report of every failure runtime-input.sh retained under `root` at or after `sinceMs`, oldest first, each headed
 * by its path. Nx names a failed runtime input's tasks and their runtime inputs (`Could not hash …`), not which one
 * failed or why, and drops the input's stderr, so a headless run prints these after Nx fails: they name the command,
 * its exit status and its stderr. A record without a report (its evidence could not be written) is named as such.
 */
export function runtimeInputFailureReports(root: string, sinceMs: number): string[] {
  const directory = join(root, RUNTIME_INPUT_FAILURES);
  return (absentAsUndefined(() => readdirSync(directory)) ?? [])
    .map((name) => join(directory, name))
    .map((record) => ({ record, modifiedMs: statSync(record).mtimeMs }))
    .filter(({ modifiedMs }) => modifiedMs >= sinceMs)
    .sort((left, right) => left.modifiedMs - right.modifiedMs)
    .map(({ record }) => {
      const report = join(record, 'report');
      const text = absentAsUndefined(() => readFileSync(report, 'utf8'));
      return text === undefined ? `${record}: no report; its evidence is incomplete\n` : `${report}:\n${text}`;
    });
}
