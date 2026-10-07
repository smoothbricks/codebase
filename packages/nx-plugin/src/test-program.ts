import { posix } from 'node:path';
import type { Tree } from 'nx/src/devkit-exports.js';
import { isNonSourceDirectory } from './source-directories.js';

/**
 * Everything the test typecheck needs to know about one project's files.
 *
 * `tsconfig.test.json` is a program, and a program that selects none of the
 * project's tests passes `tsc --noEmit` by compiling nothing: the gate is green
 * while no test has ever been typechecked. So its `include` has to come from
 * where the tests are, and one that selects none of them has to be reported
 * rather than trusted.
 */
export interface TestProgram {
  /** Workspace-relative root of the project: `tooling`, `packages/app`. */
  readonly projectRoot: string;
  /** Project-relative TypeScript sources under the project, outside any build-output tree. */
  readonly files: readonly string[];
  /** A tsconfig by workspace-relative path; null when the file is absent. */
  readonly readConfig: (path: string) => Record<string, unknown> | null;
}

/** A directory listing the walker can use: an Nx `Tree`, or the disk behind a small adapter. */
export type ProjectTree = Pick<Tree, 'children' | 'isFile'>;

/** Why a test program typechecks none of what it exists to typecheck. */
export type TestProgramFault =
  | { readonly kind: 'no-tests' }
  | { readonly kind: 'tests-unselected'; readonly tests: readonly string[] };

const TYPESCRIPT_SOURCE = /\.(?:[cm]?ts|tsx)$/;
const TEST_FILE = /\.(?:test|spec)\.tsx?$/;
const TSX_TEST_FILE = /\.(?:test|spec)\.tsx$/;
const IN_TESTS_DIRECTORY = /(?:^|\/)__tests__\/.*\.tsx?$/;
/** An include that names an extension the TypeScript-source listing does not carry cannot be answered from it. */
const NON_TYPESCRIPT_PATTERN = /\.(?:[cm]?jsx?|json)$/;
/** TypeScript 5.5's template for the directory of the config being resolved, usable in an inherited pattern. */
const CONFIG_DIR = /^\$\{configDir\}/;
/** Matches no path: what an include ending in a bare `**` selects. */
const NEVER = /(?!)/;
/** What makes a directory a project of its own: a manifest or an Nx project file. */
const PROJECT_FILE = /^(?:package|project)\.json$/;
/**
 * Top-level directories whose `*.test.ts` files are data a harness compiles,
 * not suites. The stray-test policy skips the same one.
 */
const FIXTURE_DIRECTORY = 'fixtures';
const PACKAGE_CONVENTION_ROOT = 'src';
const TESTS_DIRECTORY = '__tests__';

/**
 * The TypeScript sources of a project, project-relative and sorted.
 *
 * A directory below the project that is a project of its own (its own manifest or Nx project file) is not part of
 * this one: its files are its own tests, found by its own policy pass, and folding them in would count them twice.
 * A directory that merely carries a tsconfig is not that: in a monorepo of Bun programs such a file routes the
 * directory's files to the parent's program (a `tests/tsconfig.json` extending `../tsconfig.test.json`), and
 * nothing typechecks the directory but that program.
 */
export function listTypeScriptSources(tree: ProjectTree, projectRoot: string): string[] {
  const files: string[] = [];
  const walk = (directory: string): void => {
    for (const name of childrenOf(tree, directory)) {
      const path = posix.join(directory, name);
      if (tree.isFile(path)) {
        if (TYPESCRIPT_SOURCE.test(name)) files.push(posix.relative(projectRoot, path));
      } else if (!isNonSourceDirectory(name) && !isNestedProject(tree, path)) {
        walk(path);
      }
    }
  };
  walk(projectRoot);
  return files.sort();
}

/** A directory with its own manifest or Nx project file is a project, and its files are that project's. */
function isNestedProject(tree: ProjectTree, directory: string): boolean {
  return childrenOf(tree, directory).some(
    (name) => PROJECT_FILE.test(name) && tree.isFile(posix.join(directory, name)),
  );
}

/** A dangling link (a collected build out-link) is neither a file nor a directory; anything else is a real fault. */
function childrenOf(tree: ProjectTree, directory: string): string[] {
  try {
    return tree.children(directory);
  } catch (error) {
    // ELOOP is a link cycle: there is nothing beneath it to list.
    if (
      error instanceof Error &&
      'code' in error &&
      (error.code === 'ENOENT' || error.code === 'ENOTDIR' || error.code === 'ELOOP')
    ) {
      return [];
    }
    throw error;
  }
}

function isTestSource(file: string): boolean {
  return TEST_FILE.test(file) || IN_TESTS_DIRECTORY.test(file);
}

/**
 * The top-level directory a test file belongs to (`.` for a file beside the
 * manifest), or null for a file that is not a test.
 */
function testRootOf(file: string): string | null {
  if (!isTestSource(file)) return null;
  const [first, second] = file.split('/');
  if (first === FIXTURE_DIRECTORY && second !== undefined) return null;
  return second === undefined ? '.' : (first ?? '.');
}

/**
 * The directories the test program is about. A package whose tests live in
 * `src/` is on the package convention, and stays on it: a test anywhere else is
 * a stray the stray-test policy reports, and widening the program here would
 * silently bless it. A project with no `src/` tests (the repository's `tooling`
 * keeps them beside its shell scripts) is on no convention, so the directories
 * its tests are in are the answer. A project with no tests yet gets the
 * convention, whose program then selects nothing and is reported.
 */
function programRoots(files: readonly string[]): string[] {
  const roots = [...new Set(files.map(testRootOf).filter((root): root is string => root !== null))].sort();
  if (roots.includes(PACKAGE_CONVENTION_ROOT)) return [PACKAGE_CONVENTION_ROOT];
  return roots.length > 0 ? roots : [PACKAGE_CONVENTION_ROOT];
}

/** The canonical globs for a test directory, plus the `.tsx` ones only where `.tsx` tests exist. */
function includeGlobs(roots: readonly string[], files: readonly string[]): string[] {
  return roots.flatMap((root) => {
    if (root === TESTS_DIRECTORY) return ['__tests__/**/*.ts', '__tests__/**/*.tsx'];
    const prefix = root === '.' ? '' : `${root}/**/`;
    const hasTsxTests = files.some((file) => testRootOf(file) === root && TSX_TEST_FILE.test(file));
    return [
      `${prefix}*.test.ts`,
      `${prefix}*.spec.ts`,
      ...(hasTsxTests ? [`${prefix}*.test.tsx`, `${prefix}*.spec.tsx`] : []),
      ...(root === '.' ? [] : [`${root}/**/__tests__/**/*.ts`, `${root}/**/__tests__/**/*.tsx`]),
      ...(root === PACKAGE_CONVENTION_ROOT ? ['src/test-suite-tracer.ts'] : []),
    ];
  });
}

/**
 * The globs a test program's `include` must carry, given the `include` it has.
 *
 * A new file, or a package on the `src/` convention, gets the canonical globs
 * (merging them is a no-op when they are there). A project off the convention
 * that wrote an `include` of its own gets a directory's globs only for the
 * directories whose tests that `include` does not already select, so a
 * hand-written include that already names `direnv`'s tests is never followed by four redundant entries.
 */
export function includeGlobsFor(program: TestProgram, config: Record<string, unknown>, isNew: boolean): string[] {
  const roots = programRoots(program.files);
  if (isNew || roots.includes(PACKAGE_CONVENTION_ROOT)) return includeGlobs(roots, program.files);
  const selects = fileSelector(program, config);
  if (selects === null) return includeGlobs(roots, program.files);
  const uncovered = roots.filter((root) =>
    program.files.some((file) => testRootOf(file) === root && !selects(posix.join(program.projectRoot, file))),
  );
  return includeGlobs(uncovered, program.files);
}

/**
 * How, if at all, this program typechecks none of what it exists to typecheck:
 * none of the project's tests, or, for a project with no tests at all, no file.
 * Unknowable is not a fault.
 */
export function testProgramFault(program: TestProgram, config: Record<string, unknown>): TestProgramFault | null {
  const selects = fileSelector(program, config);
  if (selects === null) return null;
  const selected = (file: string): boolean => selects(posix.join(program.projectRoot, file));
  const tests = program.files.filter((file) => testRootOf(file) !== null);
  if (tests.length === 0) return program.files.some(selected) ? null : { kind: 'no-tests' };
  return tests.some(selected) ? null : { kind: 'tests-unselected', tests };
}

interface Inherited {
  readonly values: readonly string[];
  /** The directory the values' relative patterns are written against: the config that declared them. */
  readonly directory: string;
}

/**
 * A tsconfig list option as TypeScript resolves it: the file's own, else the
 * last base in its `extends` that declares one. 'none' when no config in the
 * chain does; 'unknown' when the chain leaves what the plugin can read (a
 * package base such as `@tsconfig/node`).
 */
function inheritedList(
  program: TestProgram,
  configPath: string,
  config: Record<string, unknown>,
  field: 'files' | 'include' | 'exclude',
): Inherited | 'none' | 'unknown' {
  const own = config[field];
  if (Array.isArray(own)) {
    return {
      values: own.filter((entry): entry is string => typeof entry === 'string'),
      directory: posix.dirname(configPath),
    };
  }
  const extended = config.extends;
  const bases = typeof extended === 'string' ? [extended] : Array.isArray(extended) ? extended : [];
  for (let index = bases.length - 1; index >= 0; index--) {
    const base: unknown = bases[index];
    if (typeof base !== 'string' || !base.startsWith('.')) return 'unknown';
    const basePath = posix.join(posix.dirname(configPath), base.endsWith('.json') ? base : `${base}.json`);
    const baseConfig = program.readConfig(basePath);
    if (baseConfig === null) return 'unknown';
    const inherited = inheritedList(program, basePath, baseConfig, field);
    if (inherited !== 'none') return inherited;
  }
  return 'none';
}

/**
 * A pattern as a workspace-relative path: against the config that declared it,
 * or, for `${configDir}`, against the config being resolved. Null for one the
 * workspace-relative form cannot express (an absolute path).
 */
function resolvePattern(value: string, declaredIn: string, leafDirectory: string): string | null {
  if (value.startsWith('/')) return null;
  if (CONFIG_DIR.test(value)) return posix.join(leafDirectory, value.replace(CONFIG_DIR, ''));
  return posix.join(declaredIn, value);
}

/**
 * Whether TypeScript would put a workspace-relative file in this program, or
 * null when the answer depends on configuration outside the tree, a pattern
 * outside the project, or files the TypeScript listing does not carry. Follows
 * `files`, `include` and `exclude` through `extends`, each pattern relative to
 * the config that wrote it, and TypeScript's two defaults: no `include` and no
 * `files` means every file, and no `exclude` means `node_modules` and its kin.
 */
function fileSelector(program: TestProgram, config: Record<string, unknown>): ((path: string) => boolean) | null {
  const configPath = posix.join(program.projectRoot, 'tsconfig.test.json');
  const leaf = posix.dirname(configPath);
  const files = inheritedList(program, configPath, config, 'files');
  const include = inheritedList(program, configPath, config, 'include');
  const exclude = inheritedList(program, configPath, config, 'exclude');
  if (files === 'unknown' || include === 'unknown' || exclude === 'unknown') return null;

  const resolved = (list: Inherited): (string | null)[] =>
    list.values.map((value) => resolvePattern(value, list.directory, leaf));
  const fileList = files === 'none' ? [] : resolved(files);
  const includeList = include !== 'none' ? resolved(include) : files === 'none' ? [posix.join(leaf, '**/*')] : [];
  const excludeList =
    exclude === 'none'
      ? ['node_modules', 'bower_components', 'jspm_packages'].map((name) => posix.join(leaf, '**', name))
      : resolved(exclude);
  if ([...fileList, ...includeList, ...excludeList].some((pattern) => pattern === null)) return null;
  const known = (list: (string | null)[]): string[] => list.filter((pattern): pattern is string => pattern !== null);
  const includePatterns = known(includeList);
  if (
    includePatterns.some(
      (pattern) => !reachesInto(pattern, program.projectRoot) || NON_TYPESCRIPT_PATTERN.test(pattern),
    )
  ) {
    return null;
  }

  const included = includePatterns.map(includeRegExp);
  const excluded = known(excludeList).map(excludeRegExp);
  const named = known(fileList);
  return (path) =>
    named.includes(path) || (included.some((it) => it.test(path)) && !excluded.some((it) => it.test(path)));
}

/** Whether a pattern's fixed leading directories are inside, or contain, the project: else the project's files cannot answer for it. */
function reachesInto(pattern: string, projectRoot: string): boolean {
  const fixed: string[] = [];
  for (const segment of pattern.split('/')) {
    if (/[*?]/.test(segment)) break;
    fixed.push(segment);
  }
  const prefix = fixed.join('/');
  return (
    prefix === '' ||
    prefix === projectRoot ||
    prefix.startsWith(`${projectRoot}/`) ||
    projectRoot.startsWith(`${prefix}/`)
  );
}

/**
 * The regular-expression source for a run of pattern segments: `*` within a
 * segment, `?` one character, `**` any number of directories. In an `include`,
 * as in TypeScript, no wildcard matches a leading `.`, so hidden files and
 * directories are never selected by one; an `exclude` has no such rule and
 * removes them like anything else.
 */
function segmentsSource(segments: readonly string[], skipsHidden: boolean): string {
  const hidden = skipsHidden ? '(?!\\.)' : '';
  let source = '';
  segments.forEach((segment, index) => {
    if (segment === '**') {
      source += `(?:${hidden}[^/]+/)*`;
      return;
    }
    if (segment.startsWith('*') || segment.startsWith('?')) source += hidden;
    source += segment
      .replace(/[.+^${}()|[\]\\]/g, '\\$&')
      .replaceAll('*', '[^/]*')
      .replaceAll('?', '[^/]');
    if (index < segments.length - 1) source += '/';
  });
  return source;
}

function patternSegments(pattern: string): string[] {
  return pattern.split('/').filter((segment) => segment !== '' && segment !== '.');
}

/**
 * An `include` pattern as a regular expression over workspace-relative paths. A
 * last segment with no wildcard and no extension names a directory. A trailing
 * `**` is an error to TypeScript (TS5010) that selects nothing, so it selects
 * nothing here: reading it as "everything beneath" would bless a config that
 * `tsc` itself rejects.
 */
function includeRegExp(pattern: string): RegExp {
  const segments = patternSegments(pattern);
  const last = segments.at(-1);
  if (last === '**') return NEVER;
  if (last !== undefined && !/[*?.]/.test(last)) segments.push('**', '*');
  return new RegExp(`^${segmentsSource(segments, true)}$`);
}

/**
 * An `exclude` pattern: it removes what it matches and everything beneath a
 * match, so `src/foo*` drops `src/foobar/a.test.ts` (TypeScript ends the
 * pattern at `$` or `/`, not at the end of the path).
 */
function excludeRegExp(pattern: string): RegExp {
  const segments = patternSegments(pattern);
  if (segments.at(-1) === '**') segments.pop();
  return new RegExp(`^${segmentsSource(segments, false)}(?:$|/)`);
}
