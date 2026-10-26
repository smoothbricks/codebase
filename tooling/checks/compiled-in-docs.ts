/// <reference types="bun" />
// DOC-LANE: a doc commit carries no file a crate compiles in.
//
// The doc lane runs no task, so what it commits must not be an input of one whose result main's push or built
// products need. nx.json leaves *.md out of every project's default and production inputs, so a Markdown file reaches
// the Linux cross-lint (cargo-lint-cross, which the pre-push hook asks Nx for) and the cli's dist builds only when a
// crate's Rust sources include it with include_str! or include_bytes!, which the plugin adds to that crate's Cargo
// inputs (compiledInFiles; the cli targets name SKILL.md a second time, explicitly). A doc commit that changes such a
// file leaves main with no cross-lint result and no build at the bytes it now holds: the push is refused and the
// developer links run stale dist. That file is code that happens to be Markdown: it lands through a shed, whose land
// gates and builds it. Other Markdown that a gate task hashes (a package's README among its cargo-test inputs, a test
// fixture, the whole-tree policy targets, the docs site's .mdx) gates neither the push nor a linked dist: the next
// land re-gates it, visibly. A file the edit deletes is no longer there to be found; its include then fails the next
// land's gate.
//
//   bun tooling/checks/compiled-in-docs.ts <path>...
//
// names every path some crate compiles in and exits 1; any other path passes. The doc lane (doc-lane.ts) runs it with
// the edit's paths appended.
import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { posix, relative, resolve } from 'node:path';
import { type CompiledCargoInput, compiledInFiles } from '../../packages/nx-plugin/src/cargo-source-hash.ts';
import { repoRelative } from './spec-links.ts';

const REPO_ROOT = resolve(import.meta.dir, '..', '..');

/** A file some crate's Rust sources include: the crate (its directory) and the source that does. Repository-relative. */
export interface CompiledIn {
  readonly path: string;
  readonly crate: string;
  readonly source: string;
  readonly literal: string;
}

const GIT_REPOSITORY_VARIABLES = [
  'GIT_DIR',
  'GIT_WORK_TREE',
  'GIT_INDEX_FILE',
  'GIT_COMMON_DIR',
  'GIT_PREFIX',
  'GIT_OBJECT_DIRECTORY',
  'GIT_ALTERNATE_OBJECT_DIRECTORIES',
];

/** This process's environment without the variables that point git at another repository, so a hand run judges this one. */
export function gitEnvironment(): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = { ...process.env };
  for (const name of GIT_REPOSITORY_VARIABLES) delete env[name];
  return env;
}
/** The directory of every tracked Cargo.toml that declares a package: a virtual workspace manifest owns no sources. */
function crateDirectories(root: string): string[] {
  const listed = spawnSync('git', ['ls-files', '-z', '--', 'Cargo.toml', '**/Cargo.toml'], {
    cwd: root,
    encoding: 'utf8',
    env: gitEnvironment(),
  });
  if (listed.status !== 0) throw new Error(`git ls-files failed with status ${listed.status}: ${listed.stderr.trim()}`);
  return listed.stdout
    .split('\0')
    .filter((manifest) => manifest !== '' && /^\[package\]/m.test(readFileSync(resolve(root, manifest), 'utf8')))
    .map((manifest) => posix.dirname(manifest));
}

/** Every file the Rust sources of the repository's crates compile in, as the plugin finds them for its Cargo inputs. */
export function compiledIn(root: string): CompiledIn[] {
  const cache = new Map<string, CompiledCargoInput[]>();
  return crateDirectories(root).flatMap((crate) =>
    compiledInFiles(resolve(root, crate), cache).map((input) => ({
      path: relative(root, input.file),
      crate,
      source: relative(root, input.source),
      literal: input.literal,
    })),
  );
}

/** The compiled-in files among the pending paths, one entry per crate that includes each. */
export function compiledPending(pending: readonly string[], compiled: readonly CompiledIn[]): CompiledIn[] {
  const wanted = new Set(pending);
  return compiled.filter((input) => wanted.has(input.path));
}

if (import.meta.main) {
  const args = process.argv.slice(2);
  const pending = args.flatMap((arg) => repoRelative(arg) ?? []);
  if (pending.length !== args.length) {
    console.error(`Not repository-relative paths: ${args.filter((arg) => repoRelative(arg) === undefined).join(', ')}`);
    console.error('usage: bun tooling/checks/compiled-in-docs.ts [<path>...]');
    process.exit(2);
  }
  const found = compiledPending(pending, compiledIn(REPO_ROOT));
  if (found.length === 0) {
    console.log(`Compiled-in check passed: none of ${pending.length} path(s) is compiled into a crate.`);
    process.exit(0);
  }
  for (const input of found) {
    console.error(
      `${input.path} is compiled into ${input.crate}: ${input.source} includes ${JSON.stringify(input.literal)}.`,
    );
  }
  console.error(
    '\nA file a crate compiles in is an input of its Cargo targets (build, lint, Linux cross-lint, test). The doc lane runs ' +
      'none of them, so main would hold bytes no gate has built or cross-linted. Land it as code: commit it in a shed and ' +
      'run cowshed land <ws>.',
  );
  process.exit(1);
}
