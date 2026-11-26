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

/** Typed graph-time refusal when a project's library and test tsconfigs include the same file. */
export class TsconfigOverlapError extends Error {
  readonly projectName: string;
  readonly libConfig: string;
  readonly testConfig: string;
  readonly files: readonly string[];

  constructor(projectName: string, libConfig: string, testConfig: string, files: readonly string[]) {
    super(
      `${projectName}: ${libConfig} and ${testConfig} both include ${files.join(', ')}. ` +
        'Each source file must belong to exactly one tsconfig so it is type-checked once; ' +
        `exclude each shared file from ${libConfig} or ${testConfig}.`,
    );
    this.name = 'TsconfigOverlapError';
    this.projectName = projectName;
    this.libConfig = libConfig;
    this.testConfig = testConfig;
    this.files = files;
  }
}

export interface RefuseOverlappingTsconfigFilesInput {
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

/** Refuse a project whose library and test tsconfigs resolve any file in common. */
export function refuseOverlappingTsconfigFiles(input: RefuseOverlappingTsconfigFilesInput): void {
  const shared = overlappingTsconfigFiles(input.libFiles, input.testFiles);
  if (shared.length === 0) return;
  const projectRelative = shared.map((file) => relative(input.projectRoot, file).split(sep).join('/')).sort();
  throw new TsconfigOverlapError(input.projectName, input.libConfig, input.testConfig, projectRelative);
}
