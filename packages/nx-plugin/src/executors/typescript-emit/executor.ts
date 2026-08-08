import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { rm, writeFile } from 'node:fs/promises';
import { basename, dirname, isAbsolute, join } from 'node:path';

import { readJsonFile } from 'nx/src/devkit-exports.js';

import type { TypeScriptEmitOptions } from './schema.js';

interface TypeScriptEmitContext {
  root: string;
}

interface TypeScriptEmitResult {
  success: boolean;
}

interface TsConfigJson {
  references?: unknown;
}

export interface CompilerInvocation {
  command: 'tsc' | 'ttsc';
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
      declaration: false,
      declarationMap: false,
      emitDeclarationOnly: false,
      incremental: false,
    },
    ...(Array.isArray(tsConfig.references) ? { references: tsConfig.references } : {}),
  };

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

    const declarationsSucceeded = await runner({
      command: 'tsc',
      args: ['-p', tsConfigPath, '--emitDeclarationOnly', '--declaration', '--declarationMap'],
      cwd,
    });
    return { success: declarationsSucceeded };
  } finally {
    await rm(overlayPath, { force: true });
  }
}

async function runCompiler(invocation: CompilerInvocation): Promise<boolean> {
  const child = spawn(invocation.command, invocation.args, {
    cwd: invocation.cwd,
    stdio: 'inherit',
    windowsHide: true,
  });
  try {
    const [code, signal] = (await once(child, 'exit')) as [number | null, NodeJS.Signals | null];
    return code === 0 && signal === null;
  } catch (error) {
    console.error(`${invocation.command}: ${error instanceof Error ? error.message : String(error)}`);
    return false;
  }
}
