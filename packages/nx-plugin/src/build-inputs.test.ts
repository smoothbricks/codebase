import { describe, expect, it } from 'bun:test';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdir, mkdtemp, readdir, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { outsideTreesTmpdir } from '@smoothbricks/cargo-retain/testing/outside-trees';
import {
  type BuildInputRecord,
  buildInputRecord,
  depInfoFiles,
  gitWorkspaceRoot,
  globPathspec,
  pathspecFiles,
  writeBuildInputs,
} from './build-inputs.js';

const GIT_IDENTITY = ['-c', 'user.name=fixture', '-c', 'user.email=fixture@example.test'];

function git(root: string, ...args: string[]): void {
  execFileSync('git', ['-C', root, ...GIT_IDENTITY, ...args], { stdio: 'pipe' });
}

function sha256(bytes: string | Uint8Array): string {
  return createHash('sha256').update(bytes).digest('hex');
}

/** Runs `body` in a new directory under the physical temp directory, removed however `body` ends. */
async function inTemporaryDirectory(body: (directory: string) => unknown): Promise<void> {
  const directory = await realpath(await mkdtemp(join(outsideTreesTmpdir(), 'smoo-build-inputs-')));
  try {
    await body(directory);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

/** The same, in a Git work tree whose `files` (workspace-relative path to text) are committed. */
function inGitRepository(files: Readonly<Record<string, string>>, body: (root: string) => unknown): Promise<void> {
  return inTemporaryDirectory(async (root) => {
    for (const [path, text] of Object.entries(files)) {
      await mkdir(dirname(join(root, path)), { recursive: true });
      await writeFile(join(root, path), text);
    }
    git(root, 'init', '-q');
    git(root, 'add', '.');
    git(root, 'commit', '-qm', 'fixture');
    await body(root);
  });
}

describe('depInfoFiles', () => {
  it('names the tracked files of this checkout for the paths of the clone that compiled', async () => {
    const files = {
      'package.json': '{}\n',
      'crates/a/Cargo.toml': '[package]\nname = "a"\n',
      'crates/a/src/lib.rs': 'pub fn a() {}\n',
      'crates/a/src/with space.rs': 'pub fn spaced() {}\n',
    };
    await inGitRepository(files, (root) => {
      const clone = '/elsewhere/clone';
      const registry = '/Users/x/.cargo/registry/src/foo-1.0';
      const lib = `${clone}/crates/a/src/lib.rs`;
      const spaced = `${clone}/crates/a/src/with\\ space.rs`;
      const manifest = `${clone}/crates/a/src/../Cargo.toml`;
      const climbing = `${clone}/crates/a/src/../../../package.json`;
      // As rustc writes one: a rule naming an output's dependencies, then an empty rule for each of them.
      const depInfo = (...dependencies: string[]) => {
        const rule = `${clone}/target/debug/deps/liba-0123.d: ${dependencies.join(' ')}`;
        return [rule, '', ...dependencies.map((path) => `${path}:`), ''].join('\n');
      };
      // The clone's absolute paths, one with an escaped space; a registry path that ends in no tracked path is dropped.
      expect(depInfoFiles(root, depInfo(spaced, `${registry}/src/lib.rs`, lib))).toEqual([
        'crates/a/src/lib.rs',
        'crates/a/src/with space.rs',
      ]);
      // Paths are normalized first, so one may climb out of its crate to a sibling file or to the clone root.
      expect(depInfoFiles(root, depInfo(lib, manifest, climbing))).toEqual([
        'crates/a/Cargo.toml',
        'crates/a/src/lib.rs',
        'package.json',
      ]);
      // Only the clone root most paths share counts: a registry file whose tail matches a tracked path is no input.
      expect(depInfoFiles(root, depInfo(`${registry}/crates/a/src/lib.rs`, spaced, manifest, climbing))).toEqual([
        'crates/a/Cargo.toml',
        'crates/a/src/with space.rs',
        'package.json',
      ]);
      // The checkout that compiled the artifact itself is the common case.
      expect(depInfoFiles(root, depInfo(`${root}/crates/a/src/lib.rs`))).toEqual(['crates/a/src/lib.rs']);
    });
  });
});

describe('pathspecFiles', () => {
  it('lists tracked and untracked-but-not-ignored matches, less the excluded and the ignored', async () => {
    const files = {
      '.gitignore': 'src/generated.ts\n',
      'src/index.ts': 'export const index = 1;\n',
      'src/index.test.ts': "import './index.js';\n",
      'src/nested/deep.ts': 'export const deep = 1;\n',
      'src/notes.md': '# notes\n',
      'other/outside.ts': 'export const outside = 1;\n',
    };
    await inGitRepository(files, async (root) => {
      await writeFile(join(root, 'src/added.ts'), 'export const added = 1;\n');
      await writeFile(join(root, 'src/generated.ts'), 'export const generated = 1;\n');
      const sources = globPathspec('src/**/*.ts');
      expect(pathspecFiles(root, [sources])).toEqual([
        'src/added.ts',
        'src/index.test.ts',
        'src/index.ts',
        'src/nested/deep.ts',
      ]);
      expect(pathspecFiles(root, [sources, globPathspec('src/**/*.test.ts', true)])).toEqual([
        'src/added.ts',
        'src/index.ts',
        'src/nested/deep.ts',
      ]);
    });
  });
});

describe('buildInputRecord', () => {
  it('records each file by the SHA-256 of its bytes, sorted by path, and `complete` only when given', async () => {
    await inTemporaryDirectory(async (root) => {
      // Not UTF-8, so a hash of decoded text would differ from the hash of these bytes.
      const binary = Buffer.from([0xff, 0xfe, 0x00, 0x80, 0x0a]);
      await mkdir(join(root, 'pkg'));
      await writeFile(join(root, 'pkg/z.bin'), binary);
      await writeFile(join(root, 'pkg/b.txt'), '');
      await writeFile(join(root, 'a.txt'), 'alpha\n');
      const complete = [':(glob)pkg/**/*.txt', ':(exclude,glob)pkg/**/*.test.txt'];
      const record = buildInputRecord(root, ['pkg/z.bin', 'a.txt', 'pkg/b.txt'], complete);
      expect(record.version).toBe(1);
      expect(Object.entries(record.files)).toEqual([
        ['a.txt', sha256('alpha\n')],
        ['pkg/b.txt', sha256('')],
        ['pkg/z.bin', sha256(binary)],
      ]);
      expect(record.complete).toEqual(complete);
      expect('complete' in buildInputRecord(root, ['a.txt'])).toBe(false);
    });
  });
});

describe('writeBuildInputs', () => {
  it('creates the output directory, writes the record as JSON and replaces an earlier one whole', async () => {
    await inTemporaryDirectory(async (root) => {
      const output = join(root, 'dist/nested');
      const recorded = join(output, '.build-inputs.json');
      const first: BuildInputRecord = {
        version: 1,
        files: { 'a.txt': sha256('alpha\n'), 'b.txt': sha256('beta\n') },
        complete: [':(glob)*.txt'],
      };
      writeBuildInputs(output, first);
      expect(JSON.parse(await readFile(recorded, 'utf8'))).toEqual(first);
      // The record alone: no `.build-inputs.json.<pid>` staging file is left beside it.
      expect(await readdir(output)).toEqual(['.build-inputs.json']);
      const second: BuildInputRecord = { version: 1, files: { 'c.txt': sha256('gamma\n') } };
      writeBuildInputs(output, second);
      expect(JSON.parse(await readFile(recorded, 'utf8'))).toEqual(second);
      expect(await readdir(output)).toEqual(['.build-inputs.json']);
    });
  });
});

describe('gitWorkspaceRoot', () => {
  it('is the physical root of the work tree from any directory inside it', async () => {
    await inGitRepository({ 'packages/lib/src/index.ts': 'export {};\n' }, (root) => {
      expect(gitWorkspaceRoot(root)).toBe(root);
      expect(gitWorkspaceRoot(join(root, 'packages/lib/src'))).toBe(root);
    });
  });

  it('is null outside a Git work tree, where a build records nothing', async () => {
    await inTemporaryDirectory((directory) => {
      expect(gitWorkspaceRoot(directory)).toBeNull();
    });
  });
});
