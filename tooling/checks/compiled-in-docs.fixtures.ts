/// <reference types="bun" />
// Planted fixtures for tooling/checks/compiled-in-docs.ts (kept as fixtures, not *.test.ts, like spec-links.fixtures.ts).
//
// A crate that includes a Markdown file beside its sources and one from outside itself, two crates that include the
// same file, and the controls that must pass: a Markdown file beside the sources that nothing includes, a spec, and a
// virtual workspace manifest that owns no sources.
//
// They run as CI runs them: NX_WORKSPACE_DATA_DIRECTORY names the repository's shared workspace data, which is not the
// fixture's. A walk of the fixture under that directory takes the repository's files lock and, when another process
// holds it, loads the archive that process writes: the repository's files, in which no planted crate exists (CI's
// Lint, run 42738719423). The fixture's index must use the fixture's own data directory and leave the shared one alone.
import { mkdirSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { compiledIn, compiledPending, gitEnvironment } from './compiled-in-docs.ts';

let failures = 0;

function fail(what: string, expected: unknown, actual: unknown): void {
  failures++;
  console.error(`FAIL ${what}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`);
}

const scratch = mkdtempSync(join(tmpdir(), 'compiled-in-docs-'));
const shared = mkdtempSync(join(tmpdir(), 'compiled-in-docs-shared-data-'));
const sharedBefore = process.env.NX_WORKSPACE_DATA_DIRECTORY;
try {
  const put = (path: string, text: string): void => {
    mkdirSync(dirname(join(scratch, path)), { recursive: true });
    writeFileSync(join(scratch, path), text);
  };
  const git = (...args: string[]): void => {
    const run = Bun.spawnSync(['git', ...args], {
      cwd: scratch,
      stdout: 'pipe',
      stderr: 'pipe',
      env: gitEnvironment(),
    });
    if (run.exitCode !== 0) throw new Error(`git ${args.join(' ')} failed: ${run.stderr.toString().trim()}`);
  };
  put('Cargo.toml', '[workspace]\nmembers = ["crates/a", "crates/b"]\n');
  put('crates/a/Cargo.toml', '[package]\nname = "a"\nversion = "0.0.0"\n');
  put(
    'crates/a/src/lib.rs',
    'pub const NOTE: &str = include_str!("../NOTE.md");\npub const GUIDE: &str = include_str!("../../../shared/GUIDE.md");\n',
  );
  put('crates/a/NOTE.md', '# note\n');
  put('crates/a/README.md', '# readme\n');
  put('crates/b/Cargo.toml', '[package]\nname = "b"\nversion = "0.0.0"\n');
  put('crates/b/src/lib.rs', 'pub const GUIDE: &str = include_str!("../../../shared/GUIDE.md");\n');
  put('shared/GUIDE.md', '# guide\n');
  put('specs/a/01.md', '# spec\n');
  put('root-include.rs', 'pub const ROOT: &str = include_str!("ROOT.md");\n');
  put('ROOT.md', '# root\n');
  git('init', '-q');
  git('add', '-A');
  process.env.NX_WORKSPACE_DATA_DIRECTORY = shared;
  const compiled = compiledIn(scratch);
  const said = (what: string, pending: readonly string[], expected: readonly string[]): void => {
    const actual = compiledPending(pending, compiled).map((input) => `${input.path} <- ${input.crate} ${input.source}`);
    if (JSON.stringify(actual) !== JSON.stringify(expected)) fail(what, expected, actual);
  };
  said(
    'a file beside the sources, included by its crate, is named',
    ['crates/a/NOTE.md'],
    ['crates/a/NOTE.md <- crates/a crates/a/src/lib.rs'],
  );
  said(
    'a file from outside the crate, included by it, is named',
    ['shared/GUIDE.md'],
    ['shared/GUIDE.md <- crates/a crates/a/src/lib.rs', 'shared/GUIDE.md <- crates/b crates/b/src/lib.rs'],
  );
  said('a file nothing includes passes, though it sits in a crate', ['crates/a/README.md'], []);
  said('a spec passes', ['specs/a/01.md'], []);
  said('no pending path passes', [], []);

  said('a file a virtual workspace root includes is named by no crate', ['ROOT.md'], []);
  said(
    'one included file among others is the only one named',
    ['specs/a/01.md', 'crates/a/NOTE.md', 'crates/a/README.md'],
    ['crates/a/NOTE.md <- crates/a crates/a/src/lib.rs'],
  );
  const touched = readdirSync(shared);
  if (touched.length > 0) fail("the shared workspace data directory is not the fixture's", [], touched);
} finally {
  rmSync(scratch, { recursive: true, force: true });
  rmSync(shared, { recursive: true, force: true });
  if (sharedBefore === undefined) delete process.env.NX_WORKSPACE_DATA_DIRECTORY;
  else process.env.NX_WORKSPACE_DATA_DIRECTORY = sharedBefore;
}

if (failures > 0) {
  console.error(`Compiled-in fixtures failed: ${failures} planted shape(s) did not get the verdict they plant.`);
  process.exit(1);
}
console.log('Compiled-in fixtures passed: the check names every planted compiled-in file and passes every control.');
