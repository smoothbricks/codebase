import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import {
  chmod,
  link,
  mkdir,
  mkdtemp,
  readFile,
  realpath,
  rename,
  rm,
  stat,
  symlink,
  utimes,
  writeFile,
} from 'node:fs/promises';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { expandOutputs, WorkspaceContext } from 'nx/src/native';
import { pastTick } from './__tests__/past-tick.js';
import { graphInstallationIdentity, workspaceSnapshot } from './ensure-built-witness.js';
import {
  type GraphInputContract,
  GraphInputReadCollector,
  graphInputContractIdentity,
} from './graph-input-contract.js';

const require = createRequire(import.meta.url);
const pure = 'export const createNodesV2 = ["**/project.json", (files) => files.map(file => [file, {}])];\n';
const sha = (text: string) => createHash('sha256').update(text).digest('hex');
let root = '';
let external = '';
// One Nx state per checkout (specs/cowshed/04_sandbox.md): the fixture is its own checkout, so the runner's
// NX_WORKSPACE_DATA_DIRECTORY (CI's shared host directory, a job's checkout `.nx`) must not name its witnesses.
const inheritedData = process.env.NX_WORKSPACE_DATA_DIRECTORY;
const data = () => join(root, '.nx', 'workspace-data');
const snapshot = () => workspaceSnapshot(root, new WorkspaceContext(root, data()), undefined);
const options = (kind: GraphInputContract['kind'] = 'native-only') => ({
  graphInputContract: {
    version: 1,
    kind,
    modules: [{ path: 'plugin.ts', sha256: sha(pure) }],
    packages: [],
  } satisfies GraphInputContract,
});
const configured = (kind: GraphInputContract['kind'] = 'native-only') => ({
  plugins: [{ plugin: './plugin.ts', options: options(kind) }],
});
const identity = (kind: GraphInputContract['kind'] = 'native-only') =>
  graphInputContractIdentity(root, configured(kind), snapshot());
const reader = (kind: GraphInputContract['kind'] = 'tsconfig-extends') =>
  new GraphInputReadCollector(root, './plugin.ts', options(kind));

beforeEach(async () => {
  root = await realpath(await mkdtemp(join(tmpdir(), 'graph-input-contract-')));
  external = await realpath(await mkdtemp(join(tmpdir(), 'graph-input-outside-')));
  await mkdir(data(), { recursive: true });
  process.env.NX_WORKSPACE_DATA_DIRECTORY = data();
  await mkdir(join(root, 'node_modules'), { recursive: true });
  for (const name of ['nx', 'typescript']) {
    await symlink(dirname(require.resolve(`${name}/package.json`)), join(root, 'node_modules', name), 'dir');
  }
  await writeFile(join(root, 'package.json'), '{"name":"fixture"}');
  await writeFile(join(root, 'nx.json'), '{}');
  await writeFile(join(root, '.gitignore'), '.nx/\nnode_modules/\n');
  await writeFile(join(root, 'plugin.ts'), pure);
  await writeFile(join(external, 'base.json'), '{"include":["src"]}');
  // A witness read in the tick its inputs changed in is unproven, and no reader trusts it.
  await pastTick();
});
afterEach(async () => {
  if (inheritedData === undefined) delete process.env.NX_WORKSPACE_DATA_DIRECTORY;
  else process.env.NX_WORKSPACE_DATA_DIRECTORY = inheritedData;
  await rm(root, { recursive: true, force: true });
  await rm(external, { recursive: true, force: true });
});

describe('owner-approved local graph contracts', () => {
  it('admits only the exact audited native-only module and refuses changed/new unapproved code', async () => {
    const original = identity();
    expect(original).not.toBeNull();
    expect(identity()).toEqual(original);
    await writeFile(join(root, 'plugin.ts'), pure.replace('**/project.json', '**/package.json'));
    expect(identity()).toBeNull();
    expect(graphInputContractIdentity(root, { plugins: ['./plugin.ts'] }, snapshot())).toBeNull();
    expect(
      graphInputContractIdentity(
        root,
        { plugins: [{ plugin: './plugin.ts', options: { graphInputContract: { kind: 'native-only' } } }] },
        snapshot(),
      ),
    ).toBeNull();
  });
  it('does not treat a kind alone, another module hash, a missing helper or an undeclared package as approval', async () => {
    const entry = "import { value } from './helper.js'; export { value };";
    await writeFile(join(root, 'plugin.ts'), entry);
    await writeFile(join(root, 'helper.ts'), 'export const value = 1;');
    const contract: GraphInputContract = {
      version: 1,
      kind: 'native-only',
      modules: [{ path: 'plugin.ts', sha256: sha(entry) }],
      packages: [],
    };
    expect(
      graphInputContractIdentity(
        root,
        { plugins: [{ plugin: './plugin.ts', options: { graphInputContract: contract } }] },
        snapshot(),
      ),
    ).toBeNull();
    const complete = {
      ...contract,
      modules: [...contract.modules, { path: 'helper.ts', sha256: sha('export const value = 1;') }],
    };
    expect(
      graphInputContractIdentity(
        root,
        { plugins: [{ plugin: './plugin.ts', options: { graphInputContract: complete } }] },
        snapshot(),
      ),
    ).not.toBeNull();
    await writeFile(join(root, 'helper.ts'), 'export const value = 2;');
    expect(
      graphInputContractIdentity(
        root,
        { plugins: [{ plugin: './plugin.ts', options: { graphInputContract: complete } }] },
        snapshot(),
      ),
    ).toBeNull();
    const externalImport = "import 'unapproved-package';";
    await writeFile(join(root, 'plugin.ts'), externalImport);
    expect(
      graphInputContractIdentity(
        root,
        {
          plugins: [
            {
              plugin: './plugin.ts',
              options: {
                graphInputContract: { ...contract, modules: [{ path: 'plugin.ts', sha256: sha(externalImport) }] },
              },
            },
          ],
        },
        snapshot(),
      ),
    ).toBeNull();
  });
  it('feeds the complete proof into installed package/alias identity and rejects a missing local proof', () => {
    const proof = identity();
    const resolver = () => ({ path: join(root, 'package.json'), packageJson: { name: 'fixture', version: '1' } });
    expect(graphInstallationIdentity(root, configured(), resolver, expandOutputs, null)).toBeNull();
    expect(graphInstallationIdentity(root, configured(), resolver, expandOutputs, proof)).not.toBeNull();
  });
});

describe('owning-reader external input byproducts', () => {
  it('retains the original result while binding external config bytes and missing candidates', async () => {
    const probe = reader();
    const path = join(external, 'base.json');
    const bytes = probe.read(path, () => '{"include":["src"]}');
    expect(bytes).toBe('{"include":["src"]}');
    expect(probe.exists(join(external, 'missing.json'))).toBe(false);
    expect(probe.finish().status).toBe('complete');
    expect(identity('tsconfig-extends')).not.toBeNull();
    const before = await (await import('node:fs/promises')).stat(path);
    await writeFile(path, '{"include":["lib"]}');
    await utimes(path, before.atime, before.mtime);
    expect(identity('tsconfig-extends')).toBeNull();
  });
  it('refuses changed membership, new/deleted candidates and native source generation', async () => {
    const probe = reader();
    probe.namespace(external);
    expect(probe.finish().status).toBe('complete');
    expect(identity('tsconfig-extends')).not.toBeNull();
    await writeFile(join(external, 'new.json'), '{}');
    expect(identity('tsconfig-extends')).toBeNull();
    const next = reader();
    next.namespace(external);
    next.finish();
    await rm(join(external, 'new.json'));
    expect(identity('tsconfig-extends')).toBeNull();
    const native = reader();
    native.finish();
    await writeFile(join(root, 'new-project.json'), '{}');
    expect(identity('tsconfig-extends')).toBeNull();
  });
  it('keeps unrelated sibling writes outside both scopes and upgrades a read directory to namespace evidence', async () => {
    const probe = reader();
    const path = join(external, 'base.json');
    probe.read(path, () => '{"include":["src"]}');
    // The prior file read saw this directory only as an ancestor; traversal upgrades it.
    probe.namespace(external);
    expect(probe.finish().status).toBe('complete');
    const before = identity('tsconfig-extends');
    expect(before).not.toBeNull();
    const sibling = `${root}-unrelated`;
    try {
      await writeFile(sibling, 'outside both the workspace and the external read scope');
      expect(identity('tsconfig-extends')).toEqual(before);
      await writeFile(join(external, 'new-member.json'), '{}');
      expect(identity('tsconfig-extends')).toBeNull();
    } finally {
      await rm(sibling, { force: true });
    }
  });
  it('revokes replacement of a real ancestor even when its hardlinked leaf stays identical', async () => {
    const first = join(external, 'first');
    const second = join(external, 'second');
    await mkdir(first);
    await mkdir(second);
    const path = join(first, 'base.json');
    await writeFile(path, '{}');
    await link(path, join(second, 'base.json'));
    await pastTick();
    const probe = reader();
    probe.read(path, () => '{}');
    expect(probe.finish().status).toBe('complete');
    expect(identity('tsconfig-extends')).not.toBeNull();
    const before = await stat(path);
    await rename(first, join(external, 'old-first'));
    await rename(second, first);
    expect((await stat(path)).ino).toBe(before.ino);
    expect(await readFile(path, 'utf8')).toBe('{}');
    expect(identity('tsconfig-extends')).toBeNull();
  });
  it('does not re-arm an old trace after a write/revert or an explicitly unproved reader', async () => {
    const probe = reader();
    probe.read(join(external, 'base.json'), () => 'before');
    await writeFile(join(external, 'base.json'), 'after');
    await writeFile(join(external, 'base.json'), '{"include":["src"]}');
    expect(probe.finish().status).toBe('unproved');
    expect(identity('tsconfig-extends')).toBeNull();
    const opaque = reader();
    opaque.refuse('opaque external runtime/file input');
    expect(opaque.finish()).toMatchObject({ status: 'unproved', detail: 'opaque external runtime/file input' });
    expect(identity('tsconfig-extends')).toBeNull();
  });
  it('falls back for missing, unreadable, malformed and wrong-version read witnesses', async () => {
    expect(identity('tsconfig-extends')).toBeNull();
    reader().finish();
    const { readdir } = await import('node:fs/promises');
    const name = (await readdir(data())).find((name) => name.startsWith('graph-input-') && name.endsWith('.json'));
    if (name === undefined) throw new Error('collector published no graph-input witness');
    const path = join(data(), name);
    const bytes = await readFile(path, 'utf8');
    await chmod(path, 0);
    try {
      expect(identity('tsconfig-extends')).toBeNull();
    } finally {
      await chmod(path, 0o600);
    }
    await writeFile(path, 'not json');
    expect(identity('tsconfig-extends')).toBeNull();
    await writeFile(path, bytes.replace('"version":1', '"version":2'));
    expect(identity('tsconfig-extends')).toBeNull();
  });
  it('uses actual Node resolution and revokes changed alias/package candidates immediately', async () => {
    const packageRoot = join(root, 'node_modules', 'base-config');
    await mkdir(packageRoot);
    await writeFile(join(packageRoot, 'package.json'), '{"name":"base-config","version":"1","exports":"./base.json"}');
    await writeFile(join(packageRoot, 'base.json'), '{}');
    await pastTick();
    const probe = reader();
    const local = createRequire(join(root, 'package.json'));
    expect(probe.resolve(root, 'base-config', () => local.resolve('base-config'))).toBe(join(packageRoot, 'base.json'));
    probe.finish();
    expect(identity('tsconfig-extends')).not.toBeNull();
    await writeFile(join(packageRoot, 'package.json'), '{"name":"base-config","version":"2","exports":"./other.json"}');
    await writeFile(join(packageRoot, 'other.json'), '{}');
    expect(identity('tsconfig-extends')).toBeNull();
  });
  it('revokes a missing outside candidate and names genuinely opaque loader inputs', async () => {
    const path = join(external, 'optional.json');
    const probe = reader();
    expect(probe.exists(path)).toBe(false);
    probe.finish();
    expect(identity('tsconfig-extends')).not.toBeNull();
    await writeFile(path, '{}');
    expect(identity('tsconfig-extends')).toBeNull();
    const previous = process.env.NODE_OPTIONS;
    process.env.NODE_OPTIONS = '--require /unbounded/preload.js';
    try {
      const opaque = reader();
      expect(opaque.read(join(external, 'base.json'), () => 'original-result')).toBe('original-result');
      expect(opaque.finish()).toMatchObject({
        status: 'unproved',
        detail: 'unbounded loader environment: NODE_OPTIONS',
      });
    } finally {
      if (previous === undefined) delete process.env.NODE_OPTIONS;
      else process.env.NODE_OPTIONS = previous;
    }
  });
  it('replays Bun resolution in a fresh process rather than inheriting a retargeted alias cache', async () => {
    const first = join(external, 'first');
    const second = join(external, 'second');
    for (const [directory, value] of [
      [first, '1'],
      [second, '2'],
    ]) {
      if (directory === undefined) throw new Error('fixture directory missing');
      await mkdir(directory);
      await writeFile(join(directory, 'package.json'), '{"name":"workspace-alias","exports":"./index.js"}');
      await writeFile(join(directory, 'index.js'), `export const value = ${value};`);
    }
    const alias = join(root, 'node_modules', 'workspace-alias');
    await symlink(first, alias, 'dir');
    const initial = Bun.resolveSync('workspace-alias', root);
    await rm(alias);
    await symlink(second, alias, 'dir');
    const probe = reader('reference-scan');
    const observed = probe.resolve(root, 'workspace-alias', () => initial, 'bun');
    expect(observed).toBe(initial);
    expect(probe.finish().status).toBe('complete');
    // The prior process result is deliberately stale; the bound fresh Bun replay must reject it.
    expect(identity('reference-scan')).toBeNull();
  });
  it('revokes changed Git-listed source even when Nx excludes its unchanged membership', async () => {
    execFileSync('git', ['init', '-q'], { cwd: root });
    await writeFile(join(root, '.nxignore'), 'outside-native.ts\n');
    await writeFile(join(root, 'outside-native.ts'), 'export const value = 1;\n');
    execFileSync('git', ['add', '--', 'outside-native.ts'], { cwd: root });
    expect(snapshot().files.map(({ file }) => file)).not.toContain('outside-native.ts');
    await pastTick();
    const probe = reader('reference-scan');
    probe.git(root, []);
    expect(probe.finish().status).toBe('complete');
    expect(identity('reference-scan')).not.toBeNull();
    const nativeFiles = snapshot().files;
    await writeFile(join(root, 'outside-native.ts'), 'export const value = 2;\n');
    expect(snapshot().files).toEqual(nativeFiles);
    expect(identity('reference-scan')).toBeNull();
  });
  it('revokes a symlink grandparent retarget even when its hardlinked leaf keeps identical bytes and inode', async () => {
    const first = join(external, 'first', 'deep');
    const second = join(external, 'second', 'deep');
    await mkdir(first, { recursive: true });
    await mkdir(second, { recursive: true });
    await writeFile(join(first, 'base.json'), '{"include":["src"]}');
    await link(join(first, 'base.json'), join(second, 'base.json'));
    const alias = join(external, 'alias');
    await symlink(dirname(first), alias, 'dir');
    const input = join(alias, 'deep', 'base.json');
    const before = await stat(input);
    const physical = await realpath(input);
    const bytes = await readFile(input, 'utf8');
    await pastTick();
    const probe = reader();
    expect(probe.read(input, () => bytes)).toBe(bytes);
    expect(probe.finish().status).toBe('complete');
    expect(identity('tsconfig-extends')).not.toBeNull();
    await rm(alias);
    await symlink(dirname(second), alias, 'dir');
    expect((await stat(input)).ino).toBe(before.ino);
    expect(await readFile(input, 'utf8')).toBe(bytes);
    expect(await realpath(input)).not.toBe(physical);
    expect(identity('tsconfig-extends')).toBeNull();
  });
  it('binds Git effective includes, index, membership and attributes through fixed read-only queries', async () => {
    execFileSync('git', ['init', '-q'], { cwd: root });
    execFileSync('git', ['config', 'include.path', join(external, 'gitconfig')], { cwd: root });
    await writeFile(join(external, 'gitconfig'), '[core]\n  ignorecase = true\n');
    await pastTick();
    const probe = reader('reference-scan');
    probe.git(root, ['plugin.ts']);
    expect(probe.finish().status).toBe('complete');
    expect(identity('reference-scan')).not.toBeNull();
    await writeFile(join(external, 'gitconfig'), '[core]\n  ignorecase = false\n');
    expect(identity('reference-scan')).toBeNull();
    const attrs = reader('reference-scan');
    attrs.git(root, ['plugin.ts']);
    attrs.finish();
    await writeFile(join(root, '.git', 'info', 'attributes'), '*.ts binary\n');
    expect(identity('reference-scan')).toBeNull();
  });
});
