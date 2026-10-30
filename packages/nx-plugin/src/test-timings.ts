import { readdir, readFile, stat } from 'node:fs/promises';
import { join, posix } from 'node:path';
import { parse as parseToml } from 'smol-toml';
import typia, { type IValidation } from 'typia';

import { runtimeInput, shellWord } from './runtime-input.js';
import { isRecord } from './type-guards.js';

/**
 * The check inferred beside a Bun suite that `--shard=N/M` targets split and a `--timings` manifest balances. Bun
 * charges a file the manifest does not name at the manifest's median, and it cuts the path-sorted files into contiguous
 * runs, so unnamed files cluster into one shard and push it past its deadline without a word. `lint` depends on this
 * target, which fails naming every such file instead.
 */
export const SHARD_TIMINGS_TARGET = 'shard-timings';
export const SHARD_TIMINGS_EXECUTOR = '@smoothbricks/nx-plugin:shard-timings';

/** The one command that rewrites a project's timing manifest: its suite, unsharded, under `--update-timings`. */
export function refreshTestTimingsCommand(projectName: string): string {
  return `smoo nx refresh-test-timings ${shellWord(projectName)}`;
}

/** One `bun test` suite whose shard targets differ only in `--shard=N/M`. */
export interface TimingsShardedSuite {
  /** The shard targets, by name, in declaration order. */
  readonly targets: readonly string[];
  /** Where every shard runs `bun test`, relative to the workspace root (`.` for the root itself). */
  readonly cwd: string;
  /** The `bun test` arguments every shard shares, which is all of them but `--shard`. */
  readonly args: readonly string[];
}

export type TimingsShardedSuiteResult =
  | { readonly ok: true; readonly suite: TimingsShardedSuite | null }
  | { readonly ok: false; readonly error: string };

const SHARD_FLAG = /^--shard=\d+\/\d+$/;

/**
 * The project's timings-sharded Bun suite, or `null` when no target runs `bun test` with both `--shard` and
 * `--timings`. A target that does but is not a plain `bun test` command (shell syntax, a positional filter, a value
 * flag spelled without `=`) is refused: the files it would run could only be guessed, and a guess is the silent
 * imbalance this check exists to end. Every shard must share one working directory and one argument list apart from
 * `--shard`, because "the files the shards run" is only defined for one suite.
 */
export function timingsShardedSuite(
  projectRoot: string,
  targets: Readonly<Record<string, unknown>>,
): TimingsShardedSuiteResult {
  let suite: TimingsShardedSuite | null = null;
  for (const [name, target] of Object.entries(targets)) {
    const options = isRecord(target) ? target.options : undefined;
    const command = isRecord(options) ? options.command : undefined;
    if (typeof command !== 'string' || !command.includes('--shard') || !command.includes('--timings')) continue;
    const parsed = shardArgs(command);
    if (!parsed.ok) return { ok: false, error: `target ${name}: ${parsed.error}` };
    const cwd = projectCwd(projectRoot, isRecord(options) ? options.cwd : undefined);
    if (suite === null) {
      suite = { targets: [name], cwd, args: parsed.args };
      continue;
    }
    if (cwd !== suite.cwd || parsed.args.join('\0') !== suite.args.join('\0')) {
      return {
        ok: false,
        error: `targets ${suite.targets.join(', ')} and ${name} shard one Bun timings suite but differ in more than --shard: \`bun test ${suite.args.join(' ')}\` in ${suite.cwd} against \`bun test ${parsed.args.join(' ')}\` in ${cwd}`,
      };
    }
    suite = { ...suite, targets: [...suite.targets, name] };
  }
  return { ok: true, suite };
}

function shardArgs(command: string): { ok: true; args: string[] } | { ok: false; error: string } {
  const words = shellWords(command);
  if (words === null) {
    return { ok: false, error: `\`${command}\` uses shell syntax beyond quoting; run \`bun test\` with plain flags` };
  }
  if (words[0] !== 'bun' || words[1] !== 'test') {
    return { ok: false, error: `\`${command}\` must start with \`bun test\`` };
  }
  const args = words.slice(2);
  const positional = args.find((word) => !word.startsWith('-'));
  if (positional !== undefined) {
    return {
      ok: false,
      error: `\`${command}\` passes \`${positional}\` positionally; give flag values as --flag=value and select files with --path-ignore-patterns`,
    };
  }
  const shards = args.filter((word) => word.startsWith('--shard'));
  if (shards.length !== 1 || !SHARD_FLAG.test(shards[0] ?? '')) {
    return { ok: false, error: `\`${command}\` must pass exactly one --shard=N/M` };
  }
  return { ok: true, args: args.filter((word) => !word.startsWith('--shard')) };
}

/** Nx runs a target without `cwd` from the workspace root and interpolates the two root tokens. */
function projectCwd(projectRoot: string, cwd: unknown): string {
  if (typeof cwd !== 'string' || cwd === '') return '.';
  return posix.normalize(cwd.replaceAll('{projectRoot}', projectRoot).replaceAll('{workspaceRoot}', '.'));
}

/** Unquoted, each of these makes the shell expand, redirect, chain or comment instead of passing a word on. */
const SHELL_SYNTAX = '|&;<>()$`*?[#~';

/**
 * The words a POSIX shell makes of a command that uses nothing but quoting, or `null` when it would expand, redirect
 * or chain anything: then the arguments `bun test` receives are not knowable from the text.
 */
function shellWords(command: string): string[] | null {
  const words: string[] = [];
  let word = '';
  let inWord = false;
  for (let index = 0; index < command.length; index++) {
    const char = command.charAt(index);
    if (char === "'") {
      const end = command.indexOf("'", index + 1);
      if (end < 0) return null;
      word += command.slice(index + 1, end);
      index = end;
      inWord = true;
    } else if (char === '"') {
      index++;
      while (index < command.length && command.charAt(index) !== '"') {
        let quoted = command.charAt(index);
        if (quoted === '$' || quoted === '`') return null;
        if (quoted === '\\' && '"\\$`'.includes(command.charAt(index + 1))) {
          index++;
          quoted = command.charAt(index);
        }
        word += quoted;
        index++;
      }
      if (index >= command.length) return null;
      inWord = true;
    } else if (char === '\\') {
      if (index + 1 >= command.length) return null;
      index++;
      word += command.charAt(index);
      inWord = true;
    } else if (/\s/.test(char)) {
      if (inWord) words.push(word);
      word = '';
      inWord = false;
    } else if (SHELL_SYNTAX.includes(char)) {
      return null;
    } else {
      word += char;
      inWord = true;
    }
  }
  if (inWord) words.push(word);
  return words;
}

/** The suite's test files measured against its `--timings` manifests. Paths are relative to the suite's cwd, sorted. */
export interface TimingsCoverage {
  /** Every `--timings` file, as the command names it. Bun merges them; the first is the one `--update-timings` writes. */
  readonly manifests: readonly string[];
  /** Every file the shards run between them. */
  readonly files: readonly string[];
  /** The files no manifest names, which Bun costs at the median. */
  readonly untimed: readonly string[];
}

export type TimingsCoverageResult =
  | { readonly ok: true; readonly coverage: TimingsCoverage }
  | { readonly ok: false; readonly error: string };

/** What `bun test --update-timings` writes: milliseconds per cwd-relative test file. */
export interface TimingManifest {
  version: number;
  files: Record<string, number>;
}

/** The `[test]` keys of `bunfig.toml` that change which files `bun test` finds. */
interface BunfigDiscovery {
  test?: {
    root?: string;
    pathIgnorePatterns?: string | string[];
  };
}

const parseTimingManifest = typia.json.createValidateParse<TimingManifest>();
const validateBunfigDiscovery = typia.createValidate<BunfigDiscovery>();

/**
 * Bun's test discovery for `args` in `cwd`, replayed without running a test: files named `*.test.*`, `*_test.*`,
 * `*.spec.*` or `*_spec.*` with a JavaScript extension, in any letter case, under bunfig's `test.root`, outside
 * `node_modules` and hidden directories. A symlink counts as its target, a directory entered and a file matched, and a
 * link to nothing as a file; the link's own name is the one the name rules see. `--path-ignore-patterns` globs match
 * the cwd-relative path as written and replace bunfig's `pathIgnorePatterns` when given; `--config=` names another
 * bunfig; `.gitignore` plays no part. These are the rules of Bun 1.4: `test-timings.test.ts` compares them with the
 * files `bun test --update-timings` writes for a fixture, so a Bun release that changes them fails there.
 */
export async function timingsCoverage(
  workspaceRoot: string,
  suite: Pick<TimingsShardedSuite, 'cwd' | 'args'>,
): Promise<TimingsCoverageResult> {
  const cwd = join(workspaceRoot, suite.cwd);
  const manifests = flagValues(suite.args, '--timings');
  if (manifests.length === 0) {
    return { ok: false, error: `\`bun test ${suite.args.join(' ')}\` names no --timings file` };
  }
  const bunfigPath = flagValues(suite.args, '--config').at(-1) ?? 'bunfig.toml';
  const bunfig = await readBunfigDiscovery(join(cwd, bunfigPath));
  if (!bunfig.ok) return bunfig;
  const cliIgnores = flagValues(suite.args, '--path-ignore-patterns');
  const bunfigIgnores = bunfig.test?.pathIgnorePatterns ?? [];
  const ignores =
    cliIgnores.length > 0 ? cliIgnores : typeof bunfigIgnores === 'string' ? [bunfigIgnores] : bunfigIgnores;
  const root = posix.normalize(bunfig.test?.root ?? '.');
  const files: string[] = [];
  await collectTestFiles(cwd, root === '.' ? '' : root.replace(/\/$/, ''), ignores, files);
  files.sort();
  const timed = new Set<string>();
  for (const manifest of manifests) {
    const read = await readTimingManifest(join(cwd, manifest));
    if (!read.ok) return { ok: false, error: `${join(suite.cwd, manifest)}: ${read.error}` };
    for (const file of Object.keys(read.files)) timed.add(file);
  }
  return { ok: true, coverage: { manifests, files, untimed: files.filter((file) => !timed.has(file)) } };
}

/**
 * What the shard-timings check reads, as Nx inputs: its timing manifests and its bunfig as files, and the names Bun's
 * discovery would find as {@link bunTestFileNamesInput}. Files alone cannot key the last: discovery finds gitignored
 * files too, and Nx hashes no ignored file.
 */
export function timingsSuiteInputs(suite: Pick<TimingsShardedSuite, 'cwd' | 'args'>): (string | { runtime: string })[] {
  const inCwd = (file: string) => `{workspaceRoot}/${posix.join(suite.cwd, file)}`;
  return [
    ...flagValues(suite.args, '--timings').map(inCwd),
    inCwd(flagValues(suite.args, '--config').at(-1) ?? 'bunfig.toml'),
    bunTestFileNamesInput(suite.cwd),
  ];
}

/**
 * The names of the files under `cwd` (workspace-relative) Bun's discovery could run, sorted, as a runtime input. It is a
 * superset of {@link timingsCoverage}'s rules (any extension, the whole tree whatever bunfig's `test.root`, no ignore
 * glob), so every file discovery would find changes the value, and a name discovery skips costs at most a cache miss.
 * Like Bun, it skips `node_modules` and hidden directories, follows symlinks (each directory once, by its real path),
 * takes a link to nothing for a file, and reads no `.gitignore`. It walks with Bun, which every workspace declares.
 */
export function bunTestFileNamesInput(cwd: string): { runtime: string } {
  return runtimeInput(`bun -e ${shellWord(LIST_TEST_FILE_NAMES)} ${shellWord(cwd)}`);
}

const LIST_TEST_FILE_NAMES = [
  'const { readdirSync, realpathSync, statSync } = require("node:fs");',
  'const names = []; const seen = new Set();',
  'const walk = (dir) => {',
  '  const real = realpathSync(dir); if (seen.has(real)) return; seen.add(real);',
  '  for (const entry of readdirSync(dir, { withFileTypes: true })) {',
  '    const path = dir === "." ? entry.name : dir + "/" + entry.name; const name = entry.name.toLowerCase();',
  '    let directory = entry.isDirectory();',
  '    if (entry.isSymbolicLink()) {',
  '      try { directory = statSync(path).isDirectory(); }',
  '      catch (error) { if (error?.code !== "ENOENT") throw error; }',
  '    }',
  '    if (directory) { if (!name.startsWith(".") && name !== "node_modules") walk(path); }',
  '    else if (/[._](test|spec)[.]/.test(name)) names.push(path);',
  '  }',
  '};',
  'process.chdir(process.argv[1]); walk("."); names.sort(); for (const name of names) console.log(name);',
].join(' ');

/**
 * The failure an untimed file earns: every such file, and the one command that refreshes the manifest. `cwd` is the
 * suite's workspace-relative directory, so the operator can find the files from the repository root.
 */
export function untimedTestFilesReport(projectName: string, cwd: string, coverage: TimingsCoverage): string {
  const manifest = posix.join(cwd, coverage.manifests[0] ?? '');
  return [
    `${manifest} names no duration for ${coverage.untimed.length} of the ${coverage.files.length} test files the shards of ${projectName} run.`,
    `Bun charges each the manifest's median and they cluster into one shard, which can push it past its deadline:`,
    ...coverage.untimed.map((file) => `  ${posix.join(cwd, file)}`),
    `Refresh the manifest with: ${refreshTestTimingsCommand(projectName)}`,
  ].join('\n');
}

const TEST_FILE = /[._](?:test|spec)\.(?:js|jsx|ts|tsx|mjs|cjs|mts|cts)$/;

async function collectTestFiles(cwd: string, dir: string, ignores: readonly string[], files: string[]): Promise<void> {
  for (const entry of await readdir(join(cwd, dir), { withFileTypes: true })) {
    const path = dir === '' ? entry.name : `${dir}/${entry.name}`;
    // Bun matches every name in lower case, and its ignore globs see the path as written.
    const name = entry.name.toLowerCase();
    // Bun follows a symlink to what it points at and takes one that points at nothing for a file.
    const resolved = entry.isSymbolicLink() ? await stat(join(cwd, path)).catch(() => null) : entry;
    if (resolved?.isDirectory()) {
      if (!name.startsWith('.') && name !== 'node_modules') {
        await collectTestFiles(cwd, path, ignores, files);
      }
    } else if (
      (resolved === null || resolved.isFile()) &&
      TEST_FILE.test(name) &&
      !ignores.some((glob) => posix.matchesGlob(path, glob))
    ) {
      files.push(path);
    }
  }
}

function flagValues(args: readonly string[], flag: string): string[] {
  const prefix = `${flag}=`;
  return args.filter((arg) => arg.startsWith(prefix)).map((arg) => arg.slice(prefix.length));
}

async function readBunfigDiscovery(
  path: string,
): Promise<({ ok: true } & BunfigDiscovery) | { ok: false; error: string }> {
  const text = await readOptionalFile(path);
  if (text === null) return { ok: true };
  let parsed: unknown;
  try {
    parsed = parseToml(text);
  } catch (error) {
    return { ok: false, error: `${path} is not valid TOML: ${error instanceof Error ? error.message : String(error)}` };
  }
  const validated = validateBunfigDiscovery(parsed);
  if (!validated.success) return { ok: false, error: `${path}: ${validationErrors(validated.errors)}` };
  return { ok: true, ...validated.data };
}

async function readTimingManifest(
  path: string,
): Promise<{ ok: true; files: Record<string, number> } | { ok: false; error: string }> {
  const text = await readOptionalFile(path);
  // Bun runs without the file and times nothing, so every test file is untimed.
  if (text === null) return { ok: true, files: {} };
  let validated: IValidation<TimingManifest>;
  try {
    validated = parseTimingManifest(text);
  } catch (error) {
    return { ok: false, error: `not valid JSON: ${error instanceof Error ? error.message : String(error)}` };
  }
  if (!validated.success) {
    return { ok: false, error: `not a Bun timing manifest: ${validationErrors(validated.errors)}` };
  }
  return { ok: true, files: validated.data.files };
}

async function readOptionalFile(path: string): Promise<string | null> {
  try {
    return await readFile(path, 'utf-8');
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return null;
    throw error;
  }
}

function validationErrors(errors: readonly IValidation.IError[]): string {
  return errors.map((error) => `${error.path.replace(/^\$input\.?/, '')}: expected ${error.expected}`).join(', ');
}
