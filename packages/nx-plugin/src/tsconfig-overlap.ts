import { dirname, relative, sep } from 'node:path';
import { parseJsonConfigFileContent, readConfigFile, sys as typescriptSys } from 'typescript';

/** Normalize a file path to forward slashes so lib and test file sets compare consistently. */
function normalizePathKey(file: string): string {
  return file.replaceAll('\\', '/');
}

/**
 * Files included in both `tsconfig.lib.json` and `tsconfig.test.json`, deduplicated and sorted.
 *
 * Each source file of a project must belong to exactly one tsconfig so it is type-checked by
 * exactly one inferred target (`tsc-js` / `typecheck` for lib sources, `typecheck-tests` for tests).
 */
export function overlappingTsconfigFiles(libFiles: Iterable<string>, testFiles: Iterable<string>): string[] {
  const libSet = new Set<string>();
  for (const file of libFiles) {
    libSet.add(normalizePathKey(file));
  }
  const shared = new Set<string>();
  for (const file of testFiles) {
    const normalized = normalizePathKey(file);
    if (libSet.has(normalized)) {
      shared.add(normalized);
    }
  }
  return [...shared].sort();
}

/** A project whose library and test tsconfigs include the same files, as `smoo monorepo check` reports it. */
export interface TsconfigOverlap {
  readonly projectName: string;
  readonly libConfig: string;
  readonly testConfig: string;
  /** The shared files, project-relative with forward slashes, sorted. */
  readonly files: readonly string[];
  readonly message: string;
}

export interface TsconfigOverlapInput {
  readonly projectName: string;
  readonly projectRoot: string;
  readonly libConfig: string;
  readonly testConfig: string;
  readonly libFiles: Iterable<string>;
  readonly testFiles: Iterable<string>;
}

/**
 * Resolve a tsconfig's file set with TypeScript's own config loader (`readConfigFile` + `parseJsonConfigFileContent`).
 */
export function resolveTsconfigFileNames(configPath: string): readonly string[] {
  const readResult = readConfigFile(configPath, typescriptSys.readFile);
  if (readResult.error !== undefined) {
    const detail =
      typeof readResult.error.messageText === 'string'
        ? readResult.error.messageText
        : readResult.error.messageText.messageText;
    throw new Error(`${configPath}: TS${readResult.error.code}: ${detail}`);
  }
  const parsed = parseJsonConfigFileContent(readResult.config, typescriptSys, dirname(configPath));
  return parsed.fileNames;
}

/**
 * The overlap of a project whose library and test tsconfigs resolve any file in common, or undefined when they are
 * disjoint. A finding of `smoo monorepo check`, never a graph-time refusal: a consuming workspace whose configs still
 * overlap keeps a project graph (inference infers both checking targets, and the shared file is checked twice) while
 * its check names each file to exclude.
 */
export function tsconfigOverlap(input: TsconfigOverlapInput): TsconfigOverlap | undefined {
  const shared = overlappingTsconfigFiles(input.libFiles, input.testFiles);
  if (shared.length === 0) return undefined;
  const files = shared.map((file) => relative(input.projectRoot, file).split(sep).join('/')).sort();
  return {
    projectName: input.projectName,
    libConfig: input.libConfig,
    testConfig: input.testConfig,
    files,
    message:
      `${input.projectName}: ${input.libConfig} and ${input.testConfig} both include ${files.join(', ')}. ` +
      'Each source file must belong to exactly one tsconfig so it is type-checked once; ' +
      `exclude each shared file from ${input.libConfig} or ${input.testConfig}.`,
  };
}
