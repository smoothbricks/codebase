#!/usr/bin/env node
import { type SpawnSyncReturns, spawnSync } from 'node:child_process';
import { lstatSync, mkdirSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { join, relative, resolve } from 'node:path';
import { GO_TESTS_MANIFEST, type GoTestBinary, type GoTestsManifest } from '../go-tests.js';

// Builds a Go module's test binaries ahead of their run, so the build can be a cached target of its own: run in the
// module, it builds one `go test -c` binary for each package with test files among <package pattern>..., as `go list`
// resolves them, and writes <out-dir>/go-tests.json naming each binary's package and directory for
// smoo-nx-go-test-run. <out-dir> is replaced whole, so no binary of a package that lost its tests lingers; a directory
// that holds no such manifest is never replaced. Exit 2 on a usage error or such a directory; go's own status when go
// fails, its diagnostics already on stderr.
const usage = 'usage: smoo-nx-go-test-build <out-dir> <package pattern>...\n';
const words = process.argv.slice(2);
const [outWord, ...patterns] = words;
if (outWord === undefined || patterns.length === 0 || words.some((word) => word.startsWith('-'))) {
  process.stderr.write(usage);
  process.exit(2);
}
const out = resolve(outWord);
// One record per package with test files: its import path and directory, each NUL-terminated; go list ends the
// output of every package that printed one with a newline. No path holds a NUL, so the records split exactly.
const LISTED = '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{"\\x00"}}{{.Dir}}{{"\\x00"}}{{end}}';

function refuse(reason: string): never {
  process.stderr.write(`smoo-nx-go-test-build: ${reason}\n`);
  process.exit(2);
}

function exists(path: string): boolean {
  try {
    lstatSync(path);
    return true;
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return false;
    throw error;
  }
}

/** `go args`; when it fails, this build ends with go's status after `cleanup`. */
function go(args: readonly string[], capture: boolean, cleanup: () => void): SpawnSyncReturns<string> {
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: go list resolves and go test -c compiles for as long as the module takes; the target that runs this build bounds it, or the operator's interrupt ends it
  const ran = spawnSync('go', args, { stdio: ['ignore', capture ? 'pipe' : 'inherit', 'inherit'], encoding: 'utf8' });
  if (ran.error !== undefined || ran.status !== 0) {
    cleanup();
    const ended = ran.error?.message ?? (ran.signal === null ? `exit ${ran.status}` : `killed by ${ran.signal}`);
    process.stderr.write(`smoo-nx-go-test-build: go ${args.join(' ')} failed: ${ended}\n`);
    process.exit(ran.status === null || ran.status === 0 ? 2 : ran.status);
  }
  return ran;
}

if (exists(out) && !exists(join(out, GO_TESTS_MANIFEST))) {
  refuse(`${out} exists and holds no ${GO_TESTS_MANIFEST}; only a directory this tool built is replaced`);
}
const packages = go(['list', '-f', LISTED, ...patterns], true, () => {})
  .stdout.split('\0\n')
  .filter((record) => record !== '')
  .map((record) => {
    const [importPath, dir, ...rest] = record.split('\0');
    if (importPath === undefined || dir === undefined || rest.length > 0) {
      refuse(`go list printed a record that is no import path and directory: ${JSON.stringify(record)}`);
    }
    return { importPath, dir };
  })
  .sort((a, b) => (a.importPath < b.importPath ? -1 : a.importPath > b.importPath ? 1 : 0));

const staging = `${out}.building-${process.pid}`;
const dropStaging = () => rmSync(staging, { recursive: true, force: true });
dropStaging();
mkdirSync(staging, { recursive: true });
const tests: GoTestBinary[] = [];
for (const { importPath, dir } of packages) {
  // Injective, so no two packages share a binary, and readable enough to name the package.
  const binary = `${encodeURIComponent(importPath)}.test`;
  go(['test', '-c', '-o', join(staging, binary), importPath], false, dropStaging);
  tests.push({ binary, dir: relative(out, dir) || '.', importPath });
}
const manifest: GoTestsManifest = { version: 1, tests };
writeFileSync(join(staging, GO_TESTS_MANIFEST), `${JSON.stringify(manifest, null, 2)}\n`);
rmSync(out, { recursive: true, force: true });
renameSync(staging, out);
process.stdout.write(`smoo-nx-go-test-build: ${tests.length} test binaries in ${out}\n`);
