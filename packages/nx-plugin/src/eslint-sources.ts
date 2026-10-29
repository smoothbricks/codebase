import { existsSync } from 'node:fs';
import { dirname, isAbsolute, join, posix, relative, sep } from 'node:path';
import {
  type ParsedCommandLine,
  parseJsonConfigFileContent,
  readConfigFile,
  sys as typescriptSys,
  WatchDirectoryFlags,
} from 'typescript';
import { ESLINT_SOURCES_TOKEN } from './executors/all-verdicts/schema.js';
import { shellWord } from './runtime-input.js';
import { isNonSourceDirectory } from './source-directories.js';
import { freshIndexedFiles } from './workspace-index.js';

/** The module kinds ESLint is handed. */
const SOURCE_KINDS = '{js,jsx,cjs,mjs,ts,tsx,cts,mts}';

/** Every JavaScript and TypeScript module kind ESLint is handed, as one glob. */
export const ESLINT_SOURCES_GLOB = `**/*.${SOURCE_KINDS}`;

/** TypeScript's code for a declaration that selects no file: a test program of none, not a broken one. */
const NO_INPUTS_FOUND = 18003;

/** What ESLint lints for one project, and the lint inputs that key its verdict beyond the project's own files. */
export interface EslintSources {
  /** The files ESLint is handed, workspace-relative and sorted. */
  readonly files: readonly string[];
  /**
   * The test declaration, then a glob for each directory its include patterns walk and each other program file by
   * name: a file the program gains later is keyed without the graph naming it.
   */
  readonly inputs: readonly string[];
}

/**
 * Read a tsconfig with this plugin's own TypeScript. `@nx/js`'s readTsConfig does an
 * undeclared `require('typescript')` resolved from its own location: where that lands on the
 * TypeScript 7 native package (whose entry exports only its version), `sys` is undefined and
 * graph construction dies reading `sys.readFile`. The plugin declares TypeScript 6, whose
 * `sys` this uses, so the resolution is the one this package's manifest promises.
 */
function readTestTsConfig(testConfig: string): ParsedCommandLine {
  const readResult = readConfigFile(testConfig, typescriptSys.readFile);
  return parseJsonConfigFileContent(readResult.config, typescriptSys, dirname(testConfig));
}

/** The workspace-relative POSIX path of `absolute`, or null when it lies outside the workspace. */
function insideWorkspace(workspaceRoot: string, absolute: string): string | null {
  const path = relative(workspaceRoot, absolute).split(sep).join('/');
  return path === '..' || path.startsWith('../') || isAbsolute(path) ? null : path;
}

/** A directory a test declaration's include patterns walk, workspace-relative ('' is the workspace root). */
interface WalkedDirectory {
  readonly directory: string;
  readonly recursive: boolean;
}

function walks({ directory, recursive }: WalkedDirectory, path: string): boolean {
  const within = directory === '' ? path : path.startsWith(`${directory}/`) ? path.slice(directory.length + 1) : null;
  return within !== null && (recursive || !within.includes('/'));
}

function walkedInput({ directory, recursive }: WalkedDirectory): string {
  return `{workspaceRoot}/${directory === '' ? '' : `${directory}/`}${recursive ? '**/' : ''}*.${SOURCE_KINDS}`;
}

/**
 * What ESLint lints for `projectRoot`, out of `indexed`, the JavaScript and TypeScript modules of an Nx workspace file
 * index (.gitignore and .nxignore applied): its `src` files, and the files its `tsconfig.test.json` declares wherever
 * they live. Only an indexed module counts: an ignored one is build output a fresh checkout lacks, and a JSON file is
 * not ESLint's. Which index `indexed` comes from is the caller's: the graph's to describe a project, the filesystem's
 * now to run ESLint on it.
 */
export function eslintSources(workspaceRoot: string, projectRoot: string, indexed: ReadonlySet<string>): EslintSources {
  const files = new Set<string>();
  const sourcePrefix = projectRoot === '.' ? 'src/' : `${posix.normalize(projectRoot)}/src/`;
  for (const path of indexed) {
    if (
      path.startsWith(sourcePrefix) &&
      !path.slice(sourcePrefix.length).split('/').slice(0, -1).some(isNonSourceDirectory)
    ) {
      files.add(path);
    }
  }
  const inputs: string[] = [];
  const testConfig = join(workspaceRoot, projectRoot, 'tsconfig.test.json');
  if (existsSync(testConfig)) {
    // A test program can live anywhere the declaration selects, not just src.
    // Use the compiler's file set and the existing declaration-chain hasher,
    // so inherited includes and files outside this project keep their keys.
    const program = readTestTsConfig(testConfig);
    const errors = program.errors.filter(({ code }) => code !== NO_INPUTS_FOUND);
    if (errors.length > 0) {
      throw new Error(
        `${testConfig}: ${errors.map(({ code, messageText }) => `TS${code}: ${JSON.stringify(messageText)}`).join('; ')}`,
      );
    }
    inputs.push('{projectRoot}/tsconfig.test.json');
    // The directories the include patterns walk are keyed as globs, so a file added to one is keyed the moment it
    // exists, and the compiler's own set of walked directories says which they are.
    const walked: WalkedDirectory[] = [];
    for (const [absolute, flags] of Object.entries(program.wildcardDirectories ?? {})) {
      const directory = insideWorkspace(workspaceRoot, absolute);
      if (directory !== null && !directory.split('/').some(isNonSourceDirectory)) {
        walked.push({ directory, recursive: (flags & WatchDirectoryFlags.Recursive) !== 0 });
      }
    }
    inputs.push(...walked.map(walkedInput).sort());
    const named: string[] = [];
    for (const absolute of program.fileNames) {
      const path = insideWorkspace(workspaceRoot, absolute);
      if (path === null) {
        throw new Error(`${testConfig}: test source ${absolute} is outside the workspace and cannot be hashed`);
      }
      if (!indexed.has(path) || path.split('/').some(isNonSourceDirectory)) continue;
      files.add(path);
      if (!walked.some((entry) => walks(entry, path))) named.push(`{workspaceRoot}/${path}`);
    }
    inputs.push(...named.sort());
  }
  return { files: [...files].sort(), inputs: [...new Set(inputs)] };
}

/** What ESLint lints for `projectRoot` as the filesystem holds it now, through a fresh workspace file index. */
export function freshEslintSources(workspaceRoot: string, projectRoot: string): EslintSources {
  return eslintSources(workspaceRoot, projectRoot, new Set(freshIndexedFiles(workspaceRoot, [ESLINT_SOURCES_GLOB])));
}

/** `command` with each {@link ESLINT_SOURCES_TOKEN} replaced by the shell words of `files`. */
export function expandEslintSources(command: string, files: readonly string[]): string {
  const words = files.map((file) => shellWord(file)).join(' ');
  return command.replaceAll(ESLINT_SOURCES_TOKEN, () => words);
}
