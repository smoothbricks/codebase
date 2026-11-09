import { afterAll, expect, it } from 'bun:test';
import { mkdir, mkdtemp, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import type { ProjectGraph } from 'nx/src/devkit-exports.js';

import {
  BUN_ROOT_LOCK_NODE,
  bunLoadInput,
  bunLoadScope,
  bunLockNodeName,
  bunLockRuntimeNodeName,
  bunScopeInput,
  type ManifestDeclarations,
  manifestsAbove,
  specifierPackage,
  typescriptLoads,
  UndeclaredImportError,
  undeclaredImports,
} from './bun-lock-scope.js';
import { type CargoClosureSource, cargoLockNode } from './cargo-closure-input.js';
import { type CargoResolve, cargoSourceIndex, type LocalCargoPackage } from './cargo-source-hash.js';

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
      [a.id, [{ pkg: serde, devOnly: false }]],
      [b.id, [{ pkg: rand, devOnly: false }]],
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
    sources: cargoSourceIndex(root, []),
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

it("keys a crate's Cargo lock-scope node on what its dependencies link, never on their dev-dependencies", async () => {
  const root = await workspace('smoo-cargo-lock-dev-');
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
  const [top, mid] = [local('top'), local('mid')];
  for (const pkg of [top, mid]) await mkdir(pkg.directory, { recursive: true });
  // `top` links `mid`; `mid` links serde and tests with rand.
  const source = (serdeVersion: string, randVersion: string): CargoClosureSource => {
    const serde = locked('serde', serdeVersion);
    const rand = locked('rand', randVersion);
    const cargo: CargoResolve = {
      root,
      members: new Set([top.id, mid.id]),
      local: [top, mid],
      locked: new Map([
        [serde, REGISTRY],
        [rand, REGISTRY],
      ]),
      edges: new Map([
        [top.id, [{ pkg: mid.id, devOnly: false }]],
        [
          mid.id,
          [
            { pkg: serde, devOnly: false },
            { pkg: rand, devOnly: true },
          ],
        ],
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
      sources: cargoSourceIndex(root, []),
      compiledFilesCache: new Map(),
    };
  };
  const hash = async (directory: string, serdeVersion: string, randVersion: string) =>
    (await cargoLockNode(directory, [source(serdeVersion, randVersion)])).data.hash;
  // A rand bump moves only the node of the crate whose tests build it.
  expect(await hash('crates/top', '1.0.219', '0.9.2')).toBe(await hash('crates/top', '1.0.219', '0.9.1'));
  expect(await hash('crates/mid', '1.0.219', '0.9.2')).not.toBe(await hash('crates/mid', '1.0.219', '0.9.1'));
  // A serde bump moves both: `top` links what `mid` links.
  expect(await hash('crates/top', '1.0.220', '0.9.1')).not.toBe(await hash('crates/top', '1.0.219', '0.9.1'));
  expect(await hash('crates/mid', '1.0.220', '0.9.1')).not.toBe(await hash('crates/mid', '1.0.219', '0.9.1'));
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
const runtime = (root) => ({ type: 'bun', name: 'bun-lock-runtime:' + root, data: { packageName: 'bun-lock-runtime:' + root, version: '0.0.0' } });
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
    'bun-lock-runtime:.': runtime('.'),
    'bun-lock-runtime:packages/a': runtime('packages/a'),
    'bun-lock-runtime:packages/b': runtime('packages/b'),
  },
  fileMap: { projectFileMap: {}, nonProjectFiles: [] },
  filesToProcess: { projectFileMap: {}, nonProjectFiles: [] },
});
process.stdout.write(JSON.stringify(edges.map(({ source, target }) => [source, target])));
`;

it("links a manifest's lock-scope node to its packages and members' runtime nodes, the root's too, and no member's to the root's", async () => {
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
  await write('packages/a/package.json', {
    name: 'a',
    dependencies: { x: '2.0.0', b: 'workspace:*' },
    devDependencies: { 'left-pad': '1.0.0' },
  });
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
    // A member reaches only what its own manifest declares, a dependent only a member's runtime closure; the root's
    // node reaches the members the root declares.
    [BUN_ROOT_LOCK_NODE, bunLockRuntimeNodeName('packages/a')],
    [BUN_ROOT_LOCK_NODE, 'npm:left-pad'],
    [bunLockNodeName('packages/a'), bunLockRuntimeNodeName('packages/b')],
    [bunLockNodeName('packages/a'), 'npm:left-pad'],
    [bunLockNodeName('packages/a'), 'npm:x'],
    // The runtime node leaves out a's devDependencies.
    [bunLockRuntimeNodeName('packages/a'), bunLockRuntimeNodeName('packages/b')],
    [bunLockRuntimeNodeName('packages/a'), 'npm:x'],
  ]);
});

const declarations = (root: string, ...declared: string[]): ManifestDeclarations => ({
  root,
  declared: new Set(declared),
});
const member = declarations('packages/a', 'typescript', '@types/a');
const root = declarations('.', 'typescript', 'ttsc', '@types/bun', 'tslib', '@nx/expo');

it.each([
  ['a member declaration, by its node', [member, root], 'typescript', bunLockNodeName('packages/a')],
  ['a root-only declaration, by the package itself', [member, root], 'ttsc', 'ttsc'],
  ['a declaration of the root seen from the root', [root], 'typescript', 'typescript'],
  ['no declaration on the way', [member, root], 'left-pad', null],
] as const)('keys a package a process loads by name by %s', (_case, manifests, name, scope) => {
  expect(bunLoadScope(manifests, name)).toBe(scope);
});

it('keys the loads of a directory each once, sorted, and nothing no manifest declares', () => {
  expect(bunLoadInput([member, root], ['ttsc', 'typescript', 'left-pad', 'ttsc'])).toEqual({
    externalDependencies: [bunLockNodeName('packages/a'), 'ttsc'],
  });
});

it('names the @types and tslib a TypeScript compile finds by ancestry, never the root tooling', () => {
  expect(typescriptLoads([member, root])).toEqual(['@types/a', '@types/bun', 'tslib']);
});

it("puts a target's own lock-scope node first and every other scope once", () => {
  expect(bunScopeInput('packages/a', ['ttsc', bunLockNodeName('packages/a')], ['@types/bun', 'ttsc'])).toEqual({
    externalDependencies: [bunLockNodeName('packages/a'), '@types/bun', 'ttsc'],
  });
});

it.each([
  ['typia/lib/transform', 'typia'],
  ['@scope/tool/ttsc-plugin', '@scope/tool'],
  ['expo', 'expo'],
])('names the package of specifier %s', (specifier, name) => {
  expect(specifierPackage(specifier)).toBe(name);
});

it('reads the manifests at and above a directory, nearest first, skipping directories without one', async () => {
  const at = await workspace('smoo-manifests-above-');
  await mkdir(join(at, 'packages/a/src'), { recursive: true });
  await writeFile(join(at, 'package.json'), '{"devDependencies":{"ttsc":"1.0.0"}}\n');
  await writeFile(join(at, 'packages/a/package.json'), '{"dependencies":{"x":"1.0.0"},"peerDependencies":{"y":"*"}}\n');
  expect(manifestsAbove(at, 'packages/a/src')).toEqual([
    declarations('packages/a', 'y', 'x'),
    declarations('.', 'ttsc'),
  ]);
});

const npm = (name: string) => ({
  type: 'npm' as const,
  name: `npm:${name}`,
  data: { packageName: name, version: '1.0.0' },
});
const scopeNode = (projectRoot: string) => ({
  type: 'bun' as const,
  name: bunLockNodeName(projectRoot),
  data: { packageName: bunLockNodeName(projectRoot), version: '0.0.0' },
});
const graph: ProjectGraph = {
  nodes: {
    root: { name: 'root', type: 'lib', data: { root: '.' } },
    a: { name: 'a', type: 'lib', data: { root: 'packages/a' } },
    b: { name: 'b', type: 'lib', data: { root: 'packages/b' } },
  },
  externalNodes: {
    'npm:x': npm('x'),
    'npm:y': npm('y'),
    'npm:mitata': npm('mitata'),
    'npm:nx': npm('nx'),
    'npm:@types/bun': npm('@types/bun'),
    [BUN_ROOT_LOCK_NODE]: scopeNode('.'),
    [bunLockNodeName('packages/a')]: scopeNode('packages/a'),
    [bunLockNodeName('packages/b')]: scopeNode('packages/b'),
  },
  dependencies: {
    root: [{ source: 'root', target: 'npm:nx', type: 'static' }],
    a: [
      { source: 'a', target: 'npm:x', type: 'static' },
      // Reached through x: a package the member's own closure holds keys it.
      { source: 'a', target: 'npm:y', type: 'dynamic' },
      { source: 'a', target: 'npm:mitata', type: 'static' },
      { source: 'a', target: 'npm:nx', type: 'implicit' },
      // TypeScript loads @types by ancestry, which every TypeScript target keys (typescriptLoads).
      { source: 'a', target: 'npm:@types/bun', type: 'static' },
      { source: 'a', target: 'b', type: 'static' },
    ],
    b: [{ source: 'b', target: 'npm:nx', type: 'dynamic' }],
    'npm:x': [{ source: 'npm:x', target: 'npm:y', type: 'static' }],
    [BUN_ROOT_LOCK_NODE]: [
      { source: BUN_ROOT_LOCK_NODE, target: 'npm:mitata', type: 'static' },
      { source: BUN_ROOT_LOCK_NODE, target: 'npm:nx', type: 'static' },
    ],
    [bunLockNodeName('packages/a')]: [
      { source: bunLockNodeName('packages/a'), target: 'npm:x', type: 'static' },
      { source: bunLockNodeName('packages/a'), target: bunLockNodeName('packages/b'), type: 'static' },
    ],
  },
};

it("refuses a member's source import of a package only the root declares, naming project and package", () => {
  expect(undeclaredImports(graph)).toEqual([
    { project: 'a', root: 'packages/a', packageName: 'mitata' },
    // b's node reaches nothing of its own: a dependent's closure does not declare b's imports for it.
    { project: 'b', root: 'packages/b', packageName: 'nx' },
  ]);
  expect(() => {
    const findings = undeclaredImports(graph);
    if (findings.length > 0) throw new UndeclaredImportError(findings);
  }).toThrow('  a (packages/a): mitata\n  b (packages/b): nx');
});
