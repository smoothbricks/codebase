import { describe, expect, it } from 'bun:test';
import { execFileSync, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdir, mkdtemp, readdir, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { outsideTreesTmpdir } from '@smoothbricks/cargo-retain/testing/outside-trees';
import {
  type BuildInputProducer,
  type BuildInputRecord,
  buildInputProducer,
  buildInputRecord,
  cargoBuildInputs,
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
  it('names the tracked files of this checkout for the paths of the clone that compiled', () => {
    const tracked = new Set([
      'package.json',
      'crates/a/Cargo.toml',
      'crates/a/src/lib.rs',
      'crates/a/src/with space.rs',
    ]);
    const root = '/current/checkout';
    const clone = '/elsewhere/clone';
    const registry = '/registry/src/foo-1.0';
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
    expect(depInfoFiles(tracked, depInfo(spaced, `${registry}/src/lib.rs`, lib))).toEqual([
      'crates/a/src/lib.rs',
      'crates/a/src/with space.rs',
    ]);
    // Paths are normalized first, so one may climb out of its crate to a sibling file or to the clone root.
    expect(depInfoFiles(tracked, depInfo(lib, manifest, climbing))).toEqual([
      'crates/a/Cargo.toml',
      'crates/a/src/lib.rs',
      'package.json',
    ]);
    // Only the clone root most paths share counts: a registry file whose tail matches a tracked path is no input.
    expect(depInfoFiles(tracked, depInfo(`${registry}/crates/a/src/lib.rs`, spaced, manifest, climbing))).toEqual([
      'crates/a/Cargo.toml',
      'crates/a/src/with space.rs',
      'package.json',
    ]);
    // The checkout that compiled the artifact itself is the common case.
    expect(depInfoFiles(tracked, depInfo(`${root}/crates/a/src/lib.rs`))).toEqual(['crates/a/src/lib.rs']);
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

describe('buildInputProducer', () => {
  const task = { project: 'lib', target: 'tsc-js' };
  const cases: readonly {
    name: string;
    env: NodeJS.ProcessEnv;
    producer: BuildInputProducer | undefined;
  }[] = [
    { name: 'outside an Nx task', env: {}, producer: undefined },
    { name: 'missing project', env: { NX_TASK_TARGET_TARGET: 'tsc-js' }, producer: undefined },
    { name: 'missing target', env: { NX_TASK_TARGET_PROJECT: 'lib' }, producer: undefined },
    { name: 'configuration alone', env: { NX_TASK_TARGET_CONFIGURATION: 'production' }, producer: undefined },
    {
      name: 'empty project',
      env: { NX_TASK_TARGET_PROJECT: '', NX_TASK_TARGET_TARGET: 'tsc-js' },
      producer: undefined,
    },
    {
      name: 'empty target',
      env: { NX_TASK_TARGET_PROJECT: 'lib', NX_TASK_TARGET_TARGET: '' },
      producer: undefined,
    },
    {
      name: 'default configuration',
      env: { NX_TASK_TARGET_PROJECT: 'lib', NX_TASK_TARGET_TARGET: 'tsc-js' },
      producer: task,
    },
    {
      name: 'empty configuration is omitted',
      env: { NX_TASK_TARGET_PROJECT: 'lib', NX_TASK_TARGET_TARGET: 'tsc-js', NX_TASK_TARGET_CONFIGURATION: '' },
      producer: task,
    },
    {
      name: 'named configuration',
      env: {
        NX_TASK_TARGET_PROJECT: 'lib',
        NX_TASK_TARGET_TARGET: 'tsc-js',
        NX_TASK_TARGET_CONFIGURATION: 'production',
      },
      producer: { ...task, configuration: 'production' },
    },
  ];

  it.each(cases)('$name', ({ env, producer }) => {
    const actual = buildInputProducer(env);
    expect(actual).toEqual(producer);
    // No file inputs: constructing and serializing provenance needs no filesystem or process environment.
    const record = buildInputRecord('', [], undefined, actual);
    expect('producer' in record).toBe(producer !== undefined);
    expect(JSON.parse(JSON.stringify(record))).toEqual({
      version: 1,
      files: {},
      ...(producer === undefined ? {} : { producer }),
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

/**
 * The dep-info rustc writes for an artifact built from `sources` in the clone at `clone`: a rule naming its
 * dependencies, then an empty rule for each of them.
 */
function rustcDepInfo(clone: string, sources: readonly string[]): string {
  const paths = sources.map((source) => `${clone}/${source}`);
  return [`${clone}/target/release/cowshed: ${paths.join(' ')}`, '', ...paths.map((path) => `${path}:`), ''].join('\n');
}

describe('cargoBuildInputs', () => {
  // A Cargo workspace of three crates, of which a build reads two.
  const workspace = {
    'Cargo.toml': '[workspace]\nmembers = ["crates/*"]\n',
    'Cargo.lock': '# lock\n',
    'crates/a/Cargo.toml': '[package]\nname = "a"\n',
    'crates/a/src/lib.rs': 'pub fn a() {}\n',
    'crates/b/Cargo.toml': '[package]\nname = "b"\n',
    'crates/b/src/lib.rs': 'pub fn b() {}\n',
    'crates/c/Cargo.toml': '[package]\nname = "c"\n',
    'crates/c/src/lib.rs': 'pub fn c() {}\n',
  };

  it('records the sources rustc read, the manifest of each crate they belong to, and the workspace manifest and lock', async () => {
    await inGitRepository(workspace, (root) => {
      // Compiled in a sibling clone, as a shed's cloned `target/` records it; crate `c` was never read.
      const depInfo = rustcDepInfo('/elsewhere/clone', ['crates/a/src/lib.rs', 'crates/b/src/lib.rs']);
      const producer = { project: 'cli', target: 'cli-x64-linux', configuration: 'production' };
      expect(cargoBuildInputs(root, depInfo, root, producer)).toEqual(
        buildInputRecord(
          root,
          [
            'Cargo.lock',
            'Cargo.toml',
            'crates/a/Cargo.toml',
            'crates/a/src/lib.rs',
            'crates/b/Cargo.toml',
            'crates/b/src/lib.rs',
          ],
          undefined,
          producer,
        ),
      );
    });
  });

  it('leaves out a workspace lock the checkout does not hold', async () => {
    await inGitRepository({ ...workspace, '.gitignore': 'Cargo.lock\n' }, (root) => {
      const depInfo = rustcDepInfo(root, ['crates/a/src/lib.rs']);
      expect(cargoBuildInputs(root, depInfo, root)).toEqual(
        buildInputRecord(root, ['Cargo.toml', 'crates/a/Cargo.toml', 'crates/a/src/lib.rs']),
      );
    });
  });

  it('is null when the dep-info names no file of the checkout', async () => {
    await inGitRepository(workspace, (root) => {
      // A registry file whose tail is no tracked path is no input of this checkout.
      const depInfo = rustcDepInfo('/registry/src/foo-1.0', ['src/lib.rs']);
      expect(cargoBuildInputs(root, depInfo, root)).toBeNull();
    });
  });
});

describe('smoo-nx-build-inputs --input', () => {
  const cli = join(import.meta.dir, 'bin/smoo-nx-build-inputs.ts');
  const run = (cwd: string, args: readonly string[]) =>
    spawnSync(process.execPath, [cli, ...args], {
      cwd,
      encoding: 'utf8',
      env: { ...process.env, NX_TASK_TARGET_PROJECT: 'nx-plugin', NX_TASK_TARGET_TARGET: 'copy-assets' },
    });

  it('records the named files, relative to the working directory, and the Nx task that wrote them', () =>
    inGitRepository(
      { 'source/a.js': 'alpha\n', 'source/b.js': 'beta\n', 'plugin/package.json': '{}\n' },
      async (root) => {
        const plugin = join(root, 'plugin');
        const written = run(plugin, ['--input', '../source/b.js', '--input', '../source/a.js', '--output', 'runtime']);
        expect(written.status, written.stderr).toBe(0);
        const record: BuildInputRecord = JSON.parse(await readFile(join(plugin, 'runtime/.build-inputs.json'), 'utf8'));
        expect(record).toEqual({
          version: 1,
          files: { 'source/a.js': sha256('alpha\n'), 'source/b.js': sha256('beta\n') },
          producer: { project: 'nx-plugin', target: 'copy-assets' },
        });
      },
    ));

  it('refuses a named file that does not exist, writing no record', () =>
    inGitRepository({ 'plugin/package.json': '{}\n' }, async (root) => {
      const plugin = join(root, 'plugin');
      const refused = run(plugin, ['--input', '../source/gone.js', '--output', 'runtime']);
      expect(refused.status).toBe(2);
      expect(refused.stderr).toContain('../source/gone.js does not exist');
      expect(await readdir(plugin)).toEqual(['package.json']);
    }));
});
