import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { realpathSync } from 'node:fs';
import { chmod, lstat, readdir, realpath, rm, rmdir, stat } from 'node:fs/promises';
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import {
  BUILD_INPUTS_FILE,
  type BuildInputProducer,
  type BuildInputRecord,
  buildInputProducer,
  buildInputRecord,
  gitWorkspaceRoot,
  globPathspec,
  pathspecFiles,
  workspacePath,
  writeBuildInputs,
} from '@smoothbricks/build-inputs';
import { readJsonFile } from 'nx/src/devkit-exports.js';
import { EMIT_OVERRIDES } from './emit-overrides.js';
import type { TypeScriptEmitOptions } from './schema.js';

interface TypeScriptEmitContext {
  root: string;
}

interface TypeScriptEmitResult {
  success: boolean;
}

interface TsConfigJson {
  include?: unknown;
  exclude?: unknown;
  compilerOptions?: { outDir?: unknown };
}

export interface CompilerInvocation {
  command: 'ttsc';
  args: string[];
  cwd: string;
  /** Variables set over the caller's environment for this run. */
  env: Readonly<Record<string, string>>;
}

/** What a compiler run answers: whether it succeeded and, when it did, every file its emit produced (absolute). */
export type CompilerOutcome =
  | { readonly success: false }
  | { readonly success: true; readonly emitted: ReadonlySet<string> };

export type CompilerRunner = (invocation: CompilerInvocation) => Promise<CompilerOutcome>;

export default function typescriptEmitExecutor(
  options: TypeScriptEmitOptions,
  context: TypeScriptEmitContext,
): Promise<TypeScriptEmitResult> {
  const producer = buildInputProducer(process.env);
  return runTypeScriptEmit(options, context, runCompiler, producer);
}

/** Runs with the caller's compiler and task identity; only the Nx entry reads its environment. */
export async function runTypeScriptEmit(
  options: TypeScriptEmitOptions,
  context: TypeScriptEmitContext,
  runner: CompilerRunner,
  producer?: BuildInputProducer,
): Promise<TypeScriptEmitResult> {
  const cwd = isAbsolute(options.cwd) ? options.cwd : join(context.root, options.cwd);
  const tsConfigPath = isAbsolute(options.tsConfig) ? options.tsConfig : join(cwd, options.tsConfig);
  const tsConfig = readJsonFile<TsConfigJson>(tsConfigPath);
  const outputDirectory = emitOutputDirectory(tsConfigPath, tsConfig);

  // Taken before the emit, so a source edited while it runs is recorded with its earlier bytes and reads as changed.
  const inputs = outputDirectory === null ? null : typeScriptInputs(tsConfigPath, tsConfig, producer);
  const previous = outputDirectory === null ? null : await snapshotOutput(outputDirectory);
  const compiled = await runner({
    command: 'ttsc',
    args: ['-p', tsConfigPath, '--emit', '--verbose', ...EMIT_OVERRIDES],
    cwd,
    // ttsc then leaves an output that already holds its bytes alone (the managed ttsc patch, driver/rewrite.go). Only a
    // caller that prunes by the emitted-file list may ask: one that finds stale outputs by which files the emit did not
    // touch, as this executor did through 0.7.9, would remove every unchanged output.
    env: { TTSC_WRITE_IF_CHANGED: '1' },
  });
  if (!compiled.success) {
    return { success: false };
  }

  if (previous !== null) {
    // Spelled as the snapshot spells outDir's files: a temp directory reached as /var is /private/var to the compiler.
    const produced = new Set<string>();
    const missing: string[] = [];
    for (const file of compiled.emitted) {
      const real = await realpath(file).catch((error: unknown) => {
        if (errorCode(error) === 'ENOENT') return null;
        throw error;
      });
      if (real === null) missing.push(file);
      else produced.add(real);
    }
    // A list that names an output the emit did not leave cannot tell this emit's outputs from an earlier build's, so it
    // removes nothing, and the failed task leaves Nx nothing to cache.
    if (missing.length > 0) {
      console.error(
        `ttsc listed ${String(missing.length)} emitted file(s) that do not exist, so ${outputDirectory} was not pruned: ${missing.slice(0, 5).join(', ')}`,
      );
      return { success: false };
    }
    if (inputs !== null) produced.add(join(previous.root, BUILD_INPUTS_FILE));
    await removeUnproduced(previous, produced);
  }
  await makePackageBinsExecutable(cwd, options.executableOutputs ?? []);
  if (outputDirectory !== null && inputs !== null) writeBuildInputs(outputDirectory, inputs);

  return { success: true };
}

/**
 * The config's outDir, which inference declares as the target's only Nx output. The emit owns it whole: after a
 * successful emit it holds exactly what that emit wrote, so neither outDir nor the artifact Nx captures from it keeps a
 * file an earlier build wrote and this one no longer does (a bin moved to another package, a deleted module). The emit
 * writes outDir in place rather than into a fresh directory swapped over it: outDir's parent is usually the package
 * root, whose listing a concurrent ttsc transform of a consumer proves while it compiles (EMIT_OVERRIDES). A config
 * without a literal outDir emits beside its sources and owns no directory.
 */
function emitOutputDirectory(tsConfigPath: string, tsConfig: TsConfigJson): string | null {
  const outDir = tsConfig.compilerOptions?.outDir;
  if (typeof outDir !== 'string' || outDir.length === 0) return null;
  const configDirectory = dirname(tsConfigPath);
  const outputDirectory = resolve(configDirectory, outDir);
  if (!isInside(configDirectory, outputDirectory)) {
    throw new Error(`${tsConfigPath}: compilerOptions.outDir must stay inside the project: ${outDir}`);
  }
  return outputDirectory;
}

/**
 * What outDir held before the emit: its files and its directories, each directory before those inside it, all spelled
 * from outDir's real path.
 */
interface OutputSnapshot {
  readonly root: string;
  readonly files: ReadonlySet<string>;
  readonly directories: readonly string[];
}

/**
 * Lists outDir before the emit. A TypeScript source in it means outDir holds inputs, which no emit rewrites, so the
 * snapshot refuses before the compiler runs rather than let the removal take them.
 */
async function snapshotOutput(outputDirectory: string): Promise<OutputSnapshot> {
  const files = new Set<string>();
  const directories: string[] = [];
  const root = await lstat(outputDirectory).catch((error: unknown) => {
    if (errorCode(error) === 'ENOENT') return null;
    throw error;
  });
  if (root === null) return { root: outputDirectory, files, directories };
  // Removal follows outDir's own path only: through a link it would reach files outside the project.
  if (!root.isDirectory()) throw new Error(`${outputDirectory} is the emit's outDir but is not a directory`);
  const real = await realpath(outputDirectory);
  const visit = async (directory: string): Promise<void> => {
    for (const name of await readdir(directory)) {
      const path = join(directory, name);
      const stats = await lstat(path, { bigint: true });
      if (stats.isDirectory()) {
        directories.push(path);
        await visit(path);
        continue;
      }
      if (/\.[cm]?tsx?$/.test(name) && !/\.d\.[cm]?ts$/.test(name)) {
        throw new Error(`${outputDirectory} is the emit's outDir but holds the TypeScript source ${path}`);
      }
      files.add(path);
    }
  };
  await visit(real);
  return { root: real, files, directories };
}

/**
 * Removes what an earlier build left and this emit no longer produces: each file outDir held before the emit that is
 * not in `produced`, then each directory that removal emptied. The emit opts ttsc into writing an output only when
 * its bytes differ (`TTSC_WRITE_IF_CHANGED`, the managed ttsc patch's `driver/rewrite.go`), so an output this emit
 * produced unchanged keeps its file, inode and mtime included, and only the compiler's own list of what it emitted
 * tells it from a file an earlier build left. The executor tests hold that against the real compiler by emitting twice
 * and by deleting a module, and hold an identity-pruning emit without the opt-in to keeping every output.
 */
async function removeUnproduced({ files, directories }: OutputSnapshot, produced: ReadonlySet<string>): Promise<void> {
  for (const path of files) {
    if (!produced.has(path)) await rm(path, { force: true });
  }
  for (const directory of [...directories].reverse()) {
    await rmdir(directory).catch((error: unknown) => {
      const code = errorCode(error);
      if (code === 'ENOTEMPTY' || code === 'EEXIST' || code === 'ENOENT') return;
      throw error;
    });
  }
}

function errorCode(error: unknown): unknown {
  return error instanceof Error && 'code' in error ? error.code : undefined;
}

const TYPESCRIPT_SOURCES = ['ts', 'tsx', 'mts', 'cts'];

/**
 * The emit's input record (build-inputs.ts) for its outDir, which is the target's Nx output: the tracked and untracked
 * files the config's `include` matches less its `exclude`, and the config. `complete` carries the same pathspecs, so a
 * source added since this emit is an input the consumer finds missing from the record. A config without a literal
 * include gets none, nor does an emit that owns no outDir (emitOutputDirectory), and its consumers judge it by
 * modification time.
 */
function typeScriptInputs(
  tsConfigPath: string,
  tsConfig: TsConfigJson,
  producer?: BuildInputProducer,
): BuildInputRecord | null {
  const strings = (value: unknown) =>
    Array.isArray(value) ? value.filter((entry): entry is string => typeof entry === 'string') : [];
  const include = strings(tsConfig.include);
  if (include.length === 0) return null;
  const configDirectory = realpathSync(dirname(tsConfigPath));
  const workspaceRoot = gitWorkspaceRoot(configDirectory);
  if (workspaceRoot === null) return null;
  const prefix = workspacePath(workspaceRoot, configDirectory);
  // tsconfig names a directory to mean the TypeScript sources under it.
  const globs = (entry: string) =>
    /[*?[]/.test(entry) || /\.[a-z]+$/.test(entry)
      ? [entry]
      : TYPESCRIPT_SOURCES.map((extension) => `${entry.replace(/\/$/, '')}/**/*.${extension}`);
  const spec = (entry: string, exclude: boolean) =>
    globs(entry).map((glob) => globPathspec(prefix === '.' ? glob : `${prefix}/${glob}`, exclude));
  const complete = [
    ...include.flatMap((entry) => spec(entry, false)),
    ...strings(tsConfig.exclude).flatMap((entry) => spec(entry, true)),
  ];
  return buildInputRecord(
    workspaceRoot,
    [
      ...pathspecFiles(workspaceRoot, complete),
      workspacePath(workspaceRoot, join(configDirectory, basename(tsConfigPath))),
    ],
    complete,
    producer,
  );
}

/** Whether `path` lies strictly beneath `directory`. */
function isInside(directory: string, path: string): boolean {
  const beneath = relative(directory, path);
  return beneath.length > 0 && beneath !== '..' && !beneath.startsWith(`..${sep}`) && !isAbsolute(beneath);
}

async function makePackageBinsExecutable(cwd: string, outputs: readonly string[]): Promise<void> {
  for (const output of new Set(outputs)) {
    const outputPath = resolve(cwd, output);
    if (!isInside(cwd, outputPath)) {
      throw new Error(`Executable output must stay inside the project: ${output}`);
    }
    const outputStat = await stat(outputPath);
    // TypeScript creates emitted files with the process default mode. A package
    // bin that loses its execute bits is present but unusable through
    // node_modules/.bin, so restore the semantic guarantee declared by `bin`. A
    // chmod moves the file's ctime even when the mode stays, so a bin that kept
    // its bits is left alone.
    if ((outputStat.mode & 0o111) !== 0o111) await chmod(outputPath, outputStat.mode | 0o111);
  }
}

/**
 * The `--verbose` summary lines that name an emit's files, as the transform host that emitted prints them under its own
 * label (`// ttsc:`, `// ttsc-typia build:`): its header, its count and one line per file.
 */
const VERBOSE_SUMMARY = /^\/\/ [^:]+: tsconfig=.* cwd=(.*?) (?:sites=\d+ )?emit=true(?: .*)?$/;
const VERBOSE_COUNT = /^\/\/ [^:]+: emitted=(\d+) files$/;
const VERBOSE_FILE = /^ {2}\+ (.+)$/;

/** ttsc's `--verbose` output, read: the files its emit named, and the rest of the output to show as ttsc wrote it. */
export interface VerboseEmit {
  readonly emitted: ReadonlySet<string>;
  readonly output: string;
}

/**
 * Reads the emitted-file list of ttsc's `--verbose` summary. ttsc's own host and its direct-tsgo lane
 * (cmd/ttsc/build.go, BuildExecution `verboseBuildSummary`) and a plugin's transform host (typia's
 * `ttsc-typia`) all print it in one shape under their own label: a header `// <label>: tsconfig=<config> cwd=<cwd>
 * … emit=true …`, then `// <label>: emitted=<count> files`, then one `  + <path>` line per file, relative to that
 * cwd. Pruning trusts this list, so output without one, or a list longer or shorter than its count, is refused rather
 * than read as an emit that produced nothing.
 */
export function readVerboseEmit(stdout: string): VerboseEmit {
  const emitted = new Set<string>();
  const output: string[] = [];
  const lines = stdout.split('\n');
  let summaries = 0;
  let cwd: string | null = null;
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index] ?? '';
    output.push(line);
    const header = VERBOSE_SUMMARY.exec(line);
    if (header !== null) {
      cwd = header[1] ?? null;
      continue;
    }
    const count = VERBOSE_COUNT.exec(line);
    if (count === null || cwd === null) continue;
    const expected = Number(count[1]);
    for (let listed = 0; listed < expected; listed += 1) {
      index += 1;
      const file = VERBOSE_FILE.exec(lines[index] ?? '')?.[1];
      if (file === undefined) {
        throw new Error(`ttsc --verbose: the summary counts ${expected} emitted files but lists ${listed}`);
      }
      emitted.add(resolve(cwd, file));
    }
    if (VERBOSE_FILE.test(lines[index + 1] ?? '')) {
      throw new Error(`ttsc --verbose: the summary counts ${expected} emitted files but lists more`);
    }
    summaries += 1;
    cwd = null;
  }
  if (summaries === 0) throw new Error('ttsc --verbose: the emit printed no emitted-file summary');
  return { emitted, output: output.join('\n') };
}

async function runCompiler(invocation: CompilerInvocation): Promise<CompilerOutcome> {
  const child = spawn(invocation.command, invocation.args, {
    cwd: invocation.cwd,
    env: { ...process.env, ...invocation.env },
    stdio: ['inherit', 'pipe', 'inherit'],
    windowsHide: true,
  });
  let stdout = '';
  child.stdout.setEncoding('utf8');
  child.stdout.on('data', (chunk: string) => {
    stdout += chunk;
  });
  try {
    // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a TypeScript emit lasts as long as its work; the operator's interrupt or the enclosing runner's bound ends a hung one
    await once(child, 'close');
  } catch (error) {
    process.stdout.write(stdout);
    console.error(`${invocation.command}: ${error instanceof Error ? error.message : String(error)}`);
    return { success: false };
  }
  if (child.exitCode !== 0 || child.signalCode !== null) {
    process.stdout.write(stdout);
    return { success: false };
  }
  const read = readVerboseEmit(stdout);
  process.stdout.write(read.output);
  return { success: true, emitted: read.emitted };
}
