import { afterAll, expect, it } from 'bun:test';
import { mkdir, mkdtemp, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { BUN_ROOT_LOCK_NODE, bunLockNodeName } from './bun-lock-scope.js';
import { type CargoClosureSource, cargoLockNode } from './cargo-closure-input.js';
import type { CargoResolve, LocalCargoPackage } from './cargo-source-hash.js';

const temporary: string[] = [];
afterAll(() => Promise.all(temporary.map((path) => rm(path, { recursive: true, force: true }))));

async function workspace(prefix: string): Promise<string> {
  const root = await realpath(await mkdtemp(join(tmpdir(), prefix)));
  temporary.push(root);
  return root;
}

const REGISTRY = 'registry+https://github.com/rust-lang/crates.io-index';
const locked = (name: string, version: string) => `${REGISTRY}#${name}@${version}`;

/** Two crates, `a` reaching serde and `b` reaching rand at `randVersion`, resolved in a checkout at `root`. */
async function cargoSource(root: string, randVersion: string): Promise<CargoClosureSource> {
  const local = (name: string): LocalCargoPackage => {
    const directory = join(root, 'crates', name);
    return {
      id: `path+file://${directory}#${name}@0.1.0`,
      name,
      version: '0.1.0',
      directory,
      manifest: join(directory, 'Cargo.toml'),
      sources: [join(directory, 'src/lib.rs')],
      governingManifest: join(root, 'Cargo.toml'),
    };
  };
  const [a, b] = [local('a'), local('b')];
  for (const pkg of [a, b]) await mkdir(pkg.directory, { recursive: true });
  const serde = locked('serde', '1.0.219');
  const rand = locked('rand', randVersion);
  const cargo: CargoResolve = {
    root,
    members: new Set([a.id, b.id]),
    local: [a, b],
    locked: new Map([
      [serde, REGISTRY],
      [rand, REGISTRY],
    ]),
    edges: new Map([
      [a.id, [serde]],
      [b.id, [rand]],
      [serde, []],
      [rand, []],
    ]),
  };
  return {
    manifest: 'Cargo.toml',
    workspaceRoot: root,
    root,
    cargo,
    indexed: new Set(),
    compiledFilesCache: new Map(),
  };
}

it('moves a Cargo lock-scope node only with the locked entries its crates reach', async () => {
  const root = await workspace('smoo-cargo-lock-');
  const before = await cargoSource(root, '0.9.1');
  const after = await cargoSource(root, '0.9.2');
  const hash = async (directory: string, source: CargoClosureSource) =>
    (await cargoLockNode(directory, [source])).data.hash;

  expect((await cargoLockNode('crates/a', [before])).name).toBe('cargo-lock:crates/a');
  // rand is reached from b alone: a's node keeps its hash, b's and the workspace's move.
  expect(await hash('crates/a', after)).toBe(await hash('crates/a', before));
  expect(await hash('crates/b', after)).not.toBe(await hash('crates/b', before));
  expect(await hash('.', after)).not.toBe(await hash('.', before));
  expect(await hash('crates/a', before)).not.toBe(await hash('crates/b', before));
});

it('digests path packages by name, so two checkouts of one lock share a Cargo lock-scope node', async () => {
  const [left, right] = [await workspace('smoo-cargo-lock-left-'), await workspace('smoo-cargo-lock-right-')];
  const node = async (root: string) => (await cargoLockNode('.', [await cargoSource(root, '0.9.1')])).data.hash;
  expect(await node(left)).toBe(await node(right));
});

it('refuses a Cargo lock-scope node no resolve covers, rather than keying it on nothing', async () => {
  await expect(cargoLockNode('crates/none', [])).rejects.toThrow('no Cargo workspace has a crate under it');
});

/**
 * TargetProjectLocator resolves from Nx's own workspace root, which Nx fixes from the process's working directory when
 * it loads, so the edges are computed in a Bun process started in the fixture.
 */
const EDGES = `
const { bunLockDependencies } = await import(Bun.argv[1]);
const node = (name, packageName, version) => ({ type: 'npm', name, data: { packageName, version } });
const lock = (root) => ({ type: 'bun', name: 'bun-lock:' + root, data: { packageName: 'bun-lock:' + root, version: '0.0.0' } });
const project = (root, packageName) => ({ root, metadata: { js: { packageName } } });
const edges = bunLockDependencies({
  workspaceRoot: process.cwd(),
  nxJsonConfiguration: {},
  projects: { root: project('.', 'root'), a: project('packages/a', 'a'), b: project('packages/b', 'b') },
  externalNodes: {
    'npm:left-pad': node('npm:left-pad', 'left-pad', '1.0.0'),
    'npm:x': node('npm:x', 'x', '2.0.0'),
    'bun-lock:.': lock('.'),
    'bun-lock:packages/a': lock('packages/a'),
    'bun-lock:packages/b': lock('packages/b'),
  },
  fileMap: { projectFileMap: {}, nonProjectFiles: [] },
  filesToProcess: { projectFileMap: {}, nonProjectFiles: [] },
});
process.stdout.write(JSON.stringify(edges.map(({ source, target }) => [source, target])));
`;

it("links a bun lock-scope node to its manifest's packages and members, and every member's to the root's", async () => {
  const root = await workspace('smoo-bun-lock-');
  const write = async (path: string, value: unknown) => {
    await mkdir(join(root, path, '..'), { recursive: true });
    await writeFile(join(root, path), `${JSON.stringify(value)}\n`);
  };
  await write('nx.json', {});
  await write('package.json', {
    name: 'root',
    private: true,
    workspaces: ['packages/*'],
    devDependencies: { 'left-pad': '1.0.0', a: 'workspace:*' },
  });
  await write('node_modules/left-pad/package.json', { name: 'left-pad', version: '1.0.0' });
  await write('packages/a/package.json', { name: 'a', dependencies: { x: '2.0.0', b: 'workspace:*' } });
  await write('packages/a/node_modules/x/package.json', { name: 'x', version: '2.0.0' });
  await write('packages/b/package.json', { name: 'b' });

  const child = Bun.spawn([process.execPath, '-e', EDGES, join(import.meta.dir, 'bun-lock-scope.ts')], {
    cwd: root,
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const [status, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  expect(status, stderr).toBe(0);
  expect(JSON.parse(stdout)).toEqual([
    // The root manifest's member `a` is no edge: every member reaches the root's node.
    [BUN_ROOT_LOCK_NODE, 'npm:left-pad'],
    [bunLockNodeName('packages/a'), BUN_ROOT_LOCK_NODE],
    [bunLockNodeName('packages/a'), bunLockNodeName('packages/b')],
    [bunLockNodeName('packages/a'), 'npm:x'],
    [bunLockNodeName('packages/b'), BUN_ROOT_LOCK_NODE],
  ]);
});
