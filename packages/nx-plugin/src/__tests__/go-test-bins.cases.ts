import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { createRunProgress } from '../executors/bounded-exec/progress.js';
import type { NativeTestResult } from '../executors/bounded-exec/test-result-writer.js';
import { GO_TESTS_MANIFEST } from '../go-tests.js';

// The emitted bins, run by node as a consumer runs them, against the toolchain's own `go test -json`.
const packageRoot = resolve(import.meta.dir, '../..');
const build = join(packageRoot, 'dist/bin/smoo-nx-go-test-build.js');
const run = join(packageRoot, 'dist/bin/smoo-nx-go-test-run.js');

const PARITY_MODULE: Readonly<Record<string, string>> = {
  'go.mod': 'module example.com/parity\n\ngo 1.21\n',
  'fails/fails.go': 'package fails\n',
  'fails/fails_test.go': [
    'package fails',
    '',
    'import "testing"',
    '',
    'func TestPasses(t *testing.T) { t.Log("fine") }',
    'func TestFails(t *testing.T)  { t.Error("boom") }',
    'func TestSkips(t *testing.T)  { t.Skip("not here") }',
    'func TestParent(t *testing.T) {',
    '\tt.Run("passes", func(t *testing.T) {})',
    '\tt.Run("fails", func(t *testing.T) { t.Fatal("nested boom") })',
    '}',
    '',
  ].join('\n'),
  // Passes only in its package directory, where go test runs it.
  'reads/reads_test.go': [
    'package reads',
    '',
    'import (',
    '\t"os"',
    '\t"testing"',
    ')',
    '',
    'func TestReadsItsPackageDirectory(t *testing.T) {',
    '\tdata, err := os.ReadFile("testdata/fixture.txt")',
    '\tif err != nil || string(data) != "fixture" {',
    '\t\tt.Fatalf("read %q: %v", data, err)',
    '\t}',
    '}',
    '',
  ].join('\n'),
  'reads/testdata/fixture.txt': 'fixture',
  // Only an external test package.
  'external/external.go': 'package external\n\nfunc Two() int { return 2 }\n',
  'external/external_test.go': [
    'package external_test',
    '',
    'import (',
    '\t"testing"',
    '',
    '\t"example.com/parity/external"',
    ')',
    '',
    'func TestTwo(t *testing.T) {',
    '\tif external.Two() != 2 {',
    '\t\tt.Fatal("two")',
    '\t}',
    '}',
    '',
  ].join('\n'),
  'untested/untested.go': 'package untested\n',
};

async function writeModule(root: string, files: Readonly<Record<string, string>>): Promise<void> {
  for (const [path, text] of Object.entries(files)) {
    await mkdir(dirname(join(root, path)), { recursive: true });
    await writeFile(join(root, path), text);
  }
}

/** The toolchain the target hashes, with a build cache of the test's own and no network or workspace. */
function goEnvironment(root: string): NodeJS.ProcessEnv {
  return { ...process.env, GOCACHE: join(root, 'go-build'), GOWORK: 'off', GOTOOLCHAIN: 'local', GOPROXY: 'off' };
}

/** What bounded-exec records from `stdout` as `command`'s, with timings, which no two runs share, set aside. */
function recorded(command: string, stdout: string): NativeTestResult[] {
  const results: NativeTestResult[] = [];
  createRunProgress(command, (result) => results.push(result)).scan('stdout', stdout);
  return results
    .map((result) => ({
      ...result,
      durationMs: result.durationMs === null ? null : 0,
      failureOutput: result.failureOutput?.replace(/\(\d+\.\d+s\)/g, '(…s)') ?? null,
    }))
    .sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
}

describe('prebuilt Go test binaries', () => {
  it('run with the results and exit status go test -json gives the same packages', async () => {
    const root = await mkdtemp(join(tmpdir(), 'go-test-bins-'));
    try {
      const module = join(root, 'module');
      await writeModule(module, PARITY_MODULE);
      const env = goEnvironment(root);
      const direct = spawnSync('go', ['test', '-json', './...'], { cwd: module, env, encoding: 'utf8' });
      expect(direct.status).toBe(1);

      const built = spawnSync('node', [build, '../out', './...'], { cwd: module, env, encoding: 'utf8' });
      expect({ status: built.status, stderr: built.stderr }).toEqual({ status: 0, stderr: '' });
      expect(JSON.parse(readFileSync(join(root, 'out', GO_TESTS_MANIFEST), 'utf8'))).toEqual({
        version: 1,
        tests: [
          {
            binary: 'example.com%2Fparity%2Fexternal.test',
            dir: '../module/external',
            importPath: 'example.com/parity/external',
          },
          {
            binary: 'example.com%2Fparity%2Ffails.test',
            dir: '../module/fails',
            importPath: 'example.com/parity/fails',
          },
          {
            binary: 'example.com%2Fparity%2Freads.test',
            dir: '../module/reads',
            importPath: 'example.com/parity/reads',
          },
        ],
      });

      // Run from elsewhere: the manifest's paths are its own.
      const prebuilt = spawnSync('node', [run, '-p=2', join('out', GO_TESTS_MANIFEST)], {
        cwd: root,
        env,
        encoding: 'utf8',
      });
      expect(prebuilt.status).toBe(1);
      expect(prebuilt.stderr).toBe(
        'smoo-nx-go-test-run: 1 of 3 test binaries failed: example.com/parity/fails (exit 1)\n',
      );
      const results = recorded('smoo-nx-go-test-run -p=2 out/go-tests.json', prebuilt.stdout);
      expect(results).toEqual(recorded('go test -json ./...', direct.stdout));
      expect(results.map((result) => `${result.id} ${result.status}`)).toEqual([
        'example.com/parity/external::TestTwo pass',
        'example.com/parity/fails::TestFails fail',
        'example.com/parity/fails::TestParent fail',
        'example.com/parity/fails::TestParent/fails fail',
        'example.com/parity/fails::TestParent/passes pass',
        'example.com/parity/fails::TestPasses pass',
        'example.com/parity/fails::TestSkips skip',
        'example.com/parity/reads::TestReadsItsPackageDirectory pass',
      ]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('refuse an empty manifest unless told, and replace only a directory the build made', async () => {
    const root = await mkdtemp(join(tmpdir(), 'go-test-bins-'));
    try {
      const module = join(root, 'module');
      await writeModule(module, {
        'go.mod': 'module example.com/untested\n\ngo 1.21\n',
        'untested/untested.go': 'package untested\n',
      });
      const env = goEnvironment(root);
      await mkdir(join(root, 'out'));
      await writeFile(join(root, 'out', 'kept.txt'), 'not the build');
      const foreign = spawnSync('node', [build, '../out', './...'], { cwd: module, env, encoding: 'utf8' });
      expect(foreign.status).toBe(2);
      expect(foreign.stderr).toContain(`holds no ${GO_TESTS_MANIFEST}`);
      expect(existsSync(join(root, 'out', 'kept.txt'))).toBe(true);

      await rm(join(root, 'out', 'kept.txt'));
      await writeFile(join(root, 'out', GO_TESTS_MANIFEST), '{"version":1,"tests":[]}\n');
      await writeFile(join(root, 'out', 'stale.test'), 'a package that lost its tests');
      const built = spawnSync('node', [build, '../out', './...'], { cwd: module, env, encoding: 'utf8' });
      expect({ status: built.status, stderr: built.stderr }).toEqual({ status: 0, stderr: '' });
      expect(existsSync(join(root, 'out', 'stale.test'))).toBe(false);
      expect(JSON.parse(readFileSync(join(root, 'out', GO_TESTS_MANIFEST), 'utf8'))).toEqual({ version: 1, tests: [] });

      const manifest = join(root, 'out', GO_TESTS_MANIFEST);
      const refused = spawnSync('node', [run, manifest], { cwd: root, env, encoding: 'utf8' });
      expect({ status: refused.status, stdout: refused.stdout }).toEqual({ status: 2, stdout: '' });
      expect(refused.stderr).toContain('lists no test binaries');
      const allowed = spawnSync('node', [run, '--allow-empty', manifest], { cwd: root, env, encoding: 'utf8' });
      expect({ status: allowed.status, stdout: allowed.stdout, stderr: allowed.stderr }).toEqual({
        status: 0,
        stdout: '',
        stderr: '',
      });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
