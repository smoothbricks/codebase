import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { realpathSync } from 'node:fs';
import { chmod, lstat, readdir, rm, rmdir, stat } from 'node:fs/promises';
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import {
  type BuildInputRecord,
  buildInputRecord,
  gitWorkspaceRoot,
  globPathspec,
  pathspecFiles,
  workspacePath,
  writeBuildInputs,
} from '@smoothbricks/build-inputs';
import { readJsonFile } from 'nx/src/devkit-exports.js';
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
}

export type CompilerRunner = (invocation: CompilerInvocation) => Promise<boolean>;

/**
 * What the emit changes about the project's own options: JavaScript and declarations from one non-incremental program.
 * They ride the command line onto the project's tsconfig. A synthesized config would have to sit beside that tsconfig,
 * because `${configDir}` and `paths` anchor to the directory of the config that consumes them, and a transient file there
 * changes the directory listing a concurrent ttsc transform of another project proves while it compiles; that
 * transform then refuses with `graph/accessible-entries-changed`.
 */
const EMIT_OVERRIDES = [
  '--composite',
  'false',
  '--declaration',
  'true',
  '--declarationMap',
  'true',
  '--emitDeclarationOnly',
  'false',
  '--incremental',
  'false',
];

export default function typescriptEmitExecutor(
  options: TypeScriptEmitOptions,
  context: TypeScriptEmitContext,
): Promise<TypeScriptEmitResult> {
  return runTypeScriptEmit(options, context, runCompiler);
}

export async function runTypeScriptEmit(
  options: TypeScriptEmitOptions,
  context: TypeScriptEmitContext,
  runner: CompilerRunner,
): Promise<TypeScriptEmitResult> {
  const cwd = isAbsolute(options.cwd) ? options.cwd : join(context.root, options.cwd);
  const tsConfigPath = isAbsolute(options.tsConfig) ? options.tsConfig : join(cwd, options.tsConfig);
  const tsConfig = readJsonFile<TsConfigJson>(tsConfigPath);
  const outputDirectory = emitOutputDirectory(tsConfigPath, tsConfig);

  // Taken before the emit, so a source edited while it runs is recorded with its earlier bytes and reads as changed.
  const inputs = outputDirectory === null ? null : typeScriptInputs(tsConfigPath, tsConfig);
  const previous = outputDirectory === null ? null : await snapshotOutput(outputDirectory);
  const javascriptSucceeded = await runner({
    command: 'ttsc',
    args: ['-p', tsConfigPath, '--emit', ...EMIT_OVERRIDES],
    cwd,
  });
  if (!javascriptSucceeded) {
    return { success: false };
  }

  // Before the execute bits: their chmod moves a bin's ctime, as a write does.
  if (previous !== null) await removeUnwritten(previous);
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
 * A file as writes change it: a write moves its mtime and, on POSIX, its ctime (Windows reports birth time there), and a
 * replacement gives it another inode.
 */
interface FileIdentity {
  readonly ino: bigint;
  readonly ctimeNs: bigint;
  readonly mtimeNs: bigint;
}

/** What outDir held before the emit: its files by identity, and its directories, each before those inside it. */
interface OutputSnapshot {
  readonly files: ReadonlyMap<string, FileIdentity>;
  readonly directories: readonly string[];
}

/**
 * Lists outDir before the emit. A TypeScript source in it means outDir holds inputs, which no emit rewrites, so the
 * snapshot refuses before the compiler runs rather than let the removal take them.
 */
async function snapshotOutput(outputDirectory: string): Promise<OutputSnapshot> {
  const files = new Map<string, FileIdentity>();
  const directories: string[] = [];
  const root = await lstat(outputDirectory).catch((error: unknown) => {
    if (errorCode(error) === 'ENOENT') return null;
    throw error;
  });
  if (root === null) return { files, directories };
  // Removal follows outDir's own path only: through a link it would reach files outside the project.
  if (!root.isDirectory()) throw new Error(`${outputDirectory} is the emit's outDir but is not a directory`);
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
      files.set(path, { ino: stats.ino, ctimeNs: stats.ctimeNs, mtimeNs: stats.mtimeNs });
    }
  };
  await visit(outputDirectory);
  return { files, directories };
}

/**
 * Removes what the emit left as it was: each file whose identity is still the one it had before the emit, then
 * each directory that removal emptied. The compiler truncates and rewrites every output of a non-incremental emit,
 * unchanged bytes included (TypeScript-Go's writer opens each output O_TRUNC, and the transform hosts ttsc loads write
 * through it), so an untouched file is one this emit no longer produces. The executor tests hold that contract against
 * the real compiler by emitting twice.
 */
async function removeUnwritten({ files, directories }: OutputSnapshot): Promise<void> {
  for (const [path, before] of files) {
    const now = await lstat(path, { bigint: true }).catch((error: unknown) => {
      if (errorCode(error) === 'ENOENT') return null;
      throw error;
    });
    const untouched =
      now !== null && now.ino === before.ino && now.ctimeNs === before.ctimeNs && now.mtimeNs === before.mtimeNs;
    if (untouched) await rm(path, { force: true });
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
function typeScriptInputs(tsConfigPath: string, tsConfig: TsConfigJson): BuildInputRecord | null {
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
    // node_modules/.bin, so restore the semantic guarantee declared by `bin`.
    await chmod(outputPath, outputStat.mode | 0o111);
  }
}

async function runCompiler(invocation: CompilerInvocation): Promise<boolean> {
  const child = spawn(invocation.command, invocation.args, {
    cwd: invocation.cwd,
    stdio: 'inherit',
    windowsHide: true,
  });
  try {
    // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a TypeScript emit lasts as long as its work; the operator's interrupt or the enclosing runner's bound ends a hung one
    await once(child, 'exit');
    return child.exitCode === 0 && child.signalCode === null;
  } catch (error) {
    console.error(`${invocation.command}: ${error instanceof Error ? error.message : String(error)}`);
    return false;
  }
}
