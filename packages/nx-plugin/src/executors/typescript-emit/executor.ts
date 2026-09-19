import { randomUUID } from 'node:crypto';
import { chmod, mkdir, rm, stat, writeFile } from 'node:fs/promises';
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { readTsConfig } from '@nx/js';
import {
  TYPESCRIPT_LIBRARY_MANIFEST,
  TYPESCRIPT_TEST_MANIFEST,
  TYPESCRIPT_TEST_OUTPUT_DIRECTORY,
  typescriptEmissionManifest,
} from '@smoothbricks/validation/test-build';
import { TtscCompiler } from 'ttsc';
import type { TypeScriptEmitOptions } from './schema.js';

interface TypeScriptEmitContext {
  root: string;
}

interface TypeScriptEmitResult {
  success: boolean;
}

export default async function typescriptEmitExecutor(
  options: TypeScriptEmitOptions,
  context: TypeScriptEmitContext,
): Promise<TypeScriptEmitResult> {
  const cwd = isAbsolute(options.cwd) ? options.cwd : join(context.root, options.cwd);
  const tests = options.kind === 'tests';
  const declarations = !tests && options.kind !== 'javascript';
  const tsConfigPath = resolve(cwd, options.tsConfig);
  const project = readTsConfig(tsConfigPath);
  const configDirectory = join(cwd, '.cache', 'ttsc-config');
  await mkdir(configDirectory, { recursive: true });
  const overlayPath = join(configDirectory, `${randomUUID()}.json`);
  // The public ttsc API captures real emitted artifacts. Its supported config
  // wrapper changes emission only; original input globs and references remain.
  const overlay = {
    extends: tsConfigPath,
    compilerOptions: {
      noEmit: false,
      emitDeclarationOnly: false,
      composite: false,
      incremental: false,
      declaration: declarations,
      declarationMap: declarations,
      sourceMap: true,
      inlineSources: true,
      rewriteRelativeImportExtensions: true,
      ...(tests ? { rootDir: cwd, outDir: join(cwd, TYPESCRIPT_TEST_OUTPUT_DIRECTORY) } : {}),
    },
    references: project.projectReferences,
  };
  await writeFile(overlayPath, JSON.stringify(overlay), { flag: 'wx' });
  try {
    const result = new TtscCompiler({
      cwd,
      tsconfig: overlayPath,
      projectRoot: dirname(tsConfigPath),
      pluginConfigDir: dirname(tsConfigPath),
    }).compile();
    if (result.type === 'exception') {
      console.error(result.error);
      return { success: false };
    }
    for (const diagnostic of result.diagnostics ?? []) console.error(diagnostic);
    if (result.type === 'failure') return { success: false };

    const manifest = typescriptEmissionManifest(cwd, result.output);
    if (tests) await rm(join(cwd, TYPESCRIPT_TEST_OUTPUT_DIRECTORY), { recursive: true, force: true });
    for (const [name, content] of Object.entries(result.output)) {
      const outputPath = resolve(cwd, name);
      const owned = relative(cwd, outputPath);
      if (isAbsolute(owned) || owned === '..' || owned.startsWith(`..${sep}`)) {
        throw new Error(`Compiler output must stay inside the project: ${name}`);
      }
      await mkdir(dirname(outputPath), { recursive: true });
      await writeFile(outputPath, content);
    }
    const manifestPath = join(cwd, tests ? TYPESCRIPT_TEST_MANIFEST : TYPESCRIPT_LIBRARY_MANIFEST);
    await mkdir(dirname(manifestPath), { recursive: true });
    await writeFile(manifestPath, JSON.stringify(manifest));
    if (!tests) await makePackageBinsExecutable(cwd, options.executableOutputs ?? []);
    return { success: true };
  } finally {
    await rm(overlayPath, { force: true });
  }
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
    await chmod(outputPath, outputStat.mode | 0o111);
  }
}
