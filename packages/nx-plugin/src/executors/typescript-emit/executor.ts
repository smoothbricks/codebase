import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { realpathSync } from 'node:fs';
import { chmod, rm, stat, writeFile } from 'node:fs/promises';
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';

import { readJsonFile } from 'nx/src/devkit-exports.js';

import {
  type BuildInputRecord,
  buildInputRecord,
  gitWorkspaceRoot,
  globPathspec,
  pathspecFiles,
  workspacePath,
  writeBuildInputs,
} from '../../build-inputs.js';
import type { TypeScriptEmitOptions } from './schema.js';

interface TypeScriptEmitContext {
  root: string;
}

interface TypeScriptEmitResult {
  success: boolean;
}

interface TsConfigJson {
  references?: unknown;
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
  const overlayPath = join(dirname(tsConfigPath), `.ttsc-js-${process.pid}-${randomUUID()}.json`);
  const overlay = {
    extends: `./${basename(tsConfigPath)}`,
    compilerOptions: {
      composite: false,
      declaration: true,
      declarationMap: true,
      emitDeclarationOnly: false,
      incremental: false,
    },
    ...(Array.isArray(tsConfig.references) ? { references: tsConfig.references } : {}),
  };

  // Taken before the emit, so a source edited while it runs is recorded with its earlier bytes and reads as changed.
  const inputs = typeScriptInputs(tsConfigPath, tsConfig);
  await writeFile(overlayPath, `${JSON.stringify(overlay, null, 2)}\n`, { flag: 'wx' });
  try {
    const javascriptSucceeded = await runner({
      command: 'ttsc',
      args: ['-p', overlayPath, '--emit'],
      cwd,
    });
    if (!javascriptSucceeded) {
      return { success: false };
    }

    await makePackageBinsExecutable(cwd, options.executableOutputs ?? []);
    if (inputs !== null) writeBuildInputs(inputs.outputDirectory, inputs.record);

    return { success: true };
  } finally {
    await rm(overlayPath, { force: true });
  }
}

const TYPESCRIPT_SOURCES = ['ts', 'tsx', 'mts', 'cts'];

/**
 * The emit's input record (build-inputs.ts) for its outDir, which is the target's Nx output: the tracked and untracked
 * files the config's `include` matches less its `exclude`, and the config. `complete` carries the same pathspecs, so a
 * source added since this emit is an input the consumer finds missing from the record. A config without a literal
 * outDir or include gets none, and its consumers judge it by modification time.
 */
function typeScriptInputs(
  tsConfigPath: string,
  tsConfig: TsConfigJson,
): { readonly outputDirectory: string; readonly record: BuildInputRecord } | null {
  const outDir = tsConfig.compilerOptions?.outDir;
  const strings = (value: unknown) =>
    Array.isArray(value) ? value.filter((entry): entry is string => typeof entry === 'string') : [];
  const include = strings(tsConfig.include);
  if (typeof outDir !== 'string' || include.length === 0) return null;
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
  return {
    outputDirectory: resolve(configDirectory, outDir),
    record: buildInputRecord(
      workspaceRoot,
      [
        ...pathspecFiles(workspaceRoot, complete),
        workspacePath(workspaceRoot, join(configDirectory, basename(tsConfigPath))),
      ],
      complete,
    ),
  };
}

async function makePackageBinsExecutable(cwd: string, outputs: readonly string[]): Promise<void> {
  for (const output of new Set(outputs)) {
    const outputPath = resolve(cwd, output);
    const projectRelative = relative(cwd, outputPath);
    if (
      projectRelative.length === 0 ||
      projectRelative === '..' ||
      projectRelative.startsWith(`..${sep}`) ||
      isAbsolute(projectRelative)
    ) {
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
    await once(child, 'exit');
    return child.exitCode === 0 && child.signalCode === null;
  } catch (error) {
    console.error(`${invocation.command}: ${error instanceof Error ? error.message : String(error)}`);
    return false;
  }
}
