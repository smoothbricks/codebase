import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { join, relative, sep } from 'node:path';

/**
 * A Go run by its top-level tests. `go test -json` reports every event and sums none, and a consumer that keeps only
 * part of a long output cannot count it, so the run that read every event states the sum where its output ends.
 */
export interface GoRunSummary {
  readonly passed: number;
  readonly failed: number;
  readonly skipped: number;
  readonly packages: number;
  readonly failedTests: readonly GoFailedTest[];
  /** Packages that failed with no failed test of their own: a build that did not compile, a `TestMain` that exited. */
  readonly failedPackages: readonly string[];
}

export interface GoFailedTest {
  readonly package: string;
  readonly test: string;
}

/** One package as `go list -f GO_LIST_FORMAT` prints it: its directory and its in-package and external test files. */
export interface GoPackageFiles {
  readonly package: string;
  readonly directory: string;
  readonly files: readonly string[];
}

export const GO_LIST_FORMAT = '{{.ImportPath}}\t{{.Dir}}\t{{join .TestGoFiles ","}}\t{{join .XTestGoFiles ","}}';

export function parseGoList(stdout: string): GoPackageFiles[] {
  const packages: GoPackageFiles[] = [];
  for (const line of stdout.split('\n')) {
    const [name, directory, own, external] = line.split('\t');
    if (name === undefined || name === '' || directory === undefined || directory === '') continue;
    packages.push({
      package: name,
      directory,
      files: [own ?? '', external ?? ''].flatMap((list) => (list === '' ? [] : list.split(','))),
    });
  }
  return packages;
}

/**
 * The file of a package that declares the top-level function `test`, and its line. Go admits one such function per
 * test package, and a directory holds two test packages (in-package and external): a name both declare has no one
 * file, and neither does a name none declares.
 */
export function declarationOf(
  test: string,
  files: readonly { readonly path: string; readonly text: string }[],
): { path: string; line: number } | null {
  const declares = new RegExp(`^func\\s+${test.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\s*\\(`);
  let found: { path: string; line: number } | null = null;
  for (const file of files) {
    const line = file.text.split('\n').findIndex((text) => declares.test(text));
    if (line === -1) continue;
    if (found !== null) return null;
    found = { path: file.path, line: line + 1 };
  }
  return found;
}

/** Each failed test whose declaration is found, as `<workspace path>:<line>`, keyed `<package>::<test>`. */
export function goDeclarations(
  failed: readonly GoFailedTest[],
  root: string,
  listed: readonly GoPackageFiles[],
  read: (path: string) => string | null,
): Map<string, string> {
  const declared = new Map<string, string>();
  for (const { package: name, test } of failed) {
    const found = listed.find((entry) => entry.package === name);
    if (found === undefined) continue;
    const files = found.files.flatMap((file) => {
      const path = join(found.directory, file);
      const text = read(path);
      return text === null ? [] : [{ path, text }];
    });
    const declaration = declarationOf(test, files);
    if (declaration === null) continue;
    declared.set(`${name}::${test}`, `${relative(root, declaration.path).split(sep).join('/')}:${declaration.line}`);
  }
  return declared;
}

/**
 * The lines a Go run's output ends with. A failed test is named at its declaration, the identity a reader matches a
 * source file by; one whose declaration was not found keeps its package, which no reader takes for a file.
 */
export function goRunSummaryText(summary: GoRunSummary, declared: ReadonlyMap<string, string>): string {
  const lines = [
    `go test summary: ${summary.passed} passed, ${summary.failed} failed, ${summary.skipped} skipped, ${summary.packages} packages`,
    ...summary.failedTests.map(
      ({ package: name, test }) => `go test failed: ${declared.get(`${name}::${test}`) ?? name} ${test}`,
    ),
    ...summary.failedPackages.map((name) => `go test package failed: ${name}`),
  ];
  return `${lines.join('\n')}\n`;
}

/** How long `go list` may take to name the failed packages' directories and test files. */
const GO_LIST_MS = 60_000;

/**
 * The declarations of a run's failed tests, asked of the toolchain that ran them: `go list` names each failed package's
 * directory and test files, and the file that declares the test is found among them. A list that does not answer
 * leaves every test at its package, which the summary then prints.
 */
export function declaredGoTests(
  failed: readonly GoFailedTest[],
  cwd: string,
  root: string,
  env: NodeJS.ProcessEnv,
): Map<string, string> {
  if (failed.length === 0) return new Map();
  const packages = [...new Set(failed.map((test) => test.package))];
  const listed = spawnSync('go', ['list', '-f', GO_LIST_FORMAT, ...packages], {
    cwd,
    env,
    encoding: 'utf8',
    timeout: GO_LIST_MS,
  });
  if (listed.status !== 0) return new Map();
  return goDeclarations(failed, root, parseGoList(listed.stdout), (path) => {
    try {
      return readFileSync(path, 'utf8');
    } catch (error) {
      // A listed file that cannot be read declares nothing here; any other failure is a bug to see.
      if (error instanceof Error && 'code' in error) return null;
      throw error;
    }
  });
}
