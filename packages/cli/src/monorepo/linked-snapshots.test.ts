import { afterEach, describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import {
  chmodSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readlinkSync,
  realpathSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { managedAssetsRoot } from '@smoothbricks/nx-plugin/managed-assets';
import type { SnapshotBinding } from '../../../nx-plugin/managed/raw/tooling/direnv/linked-snapshots.ts';

/**
 * The snapshot module is a managed raw script that shell entry loads before anything is installed, so every
 * behavioral case loads it in a plain `bun -e` child, as secret-references.test.ts does. The provider is a real
 * git workspace; its Nx producer is the one boundary the fixture stands in for: the probe answers `hit` with a
 * receipt inventorying the provider's actual output bytes at that moment, or `miss`, or a frozen earlier receipt.
 */
const RAW = join(managedAssetsRoot, 'raw/tooling/direnv/linked-snapshots.ts');
const LINKS = join(managedAssetsRoot, 'raw/tooling/direnv/developer-links.ts');

const PROBE = `
import { createHash } from 'node:crypto';
import { lstatSync, readFileSync, readdirSync, readlinkSync } from 'node:fs';
import { join, relative } from 'node:path';
import { spawnSync } from 'node:child_process';
const git = (root, ...args) => spawnSync('git', ['-C', root, ...args], { encoding: 'utf8' }).stdout.trim();
function inventory(root, directory, outputs) {
  let entries;
  try { entries = readdirSync(directory, { withFileTypes: true }); } catch { return; }
  for (const entry of entries) {
    const at = join(directory, entry.name);
    if (entry.isDirectory()) inventory(root, at, outputs);
    else if (entry.isSymbolicLink()) outputs.push({ path: relative(root, at), type: 'symlink', mode: lstatSync(at).mode, linkText: readlinkSync(at) });
    else outputs.push({ path: relative(root, at), type: 'file', mode: lstatSync(at).mode, sha256: createHash('sha256').update(readFileSync(at)).digest('hex') });
  }
}
export function receiptOf(root, target) {
  const outputs = [];
  for (const directory of OUTPUTS[target] ?? []) inventory(root, join(root, directory), outputs);
  const head = git(root, 'rev-parse', 'HEAD');
  const tasks = [{ id: target, hash: target + ':' + head, outputs }];
  // A boundary control: the producer record a plugin publishes, shaped as its contract names it. The real-Nx
  // control in nx-plugin (linked-snapshots.nx.test.ts) proves the actual publication.
  const producer = {
    version: 1, target, argv: ['nx', 'run', target], trackedTreeClean: true, environmentIdentity: 'fixture-environment',
    inputs: [{ taskId: target, overrides: {}, declared: [], expanded: {}, hashDetails: { command: target, nodes: {}, runtime: {} }, files: [] }],
    tools: [], outputSha256: createHash('sha256').update(JSON.stringify(tasks)).digest('hex'), wallTimeMs: 1,
  };
  return { workspaceRoot: root, head, headTree: git(root, 'rev-parse', 'HEAD^{tree}'), fileInputProvenance: 'exact-file-inputs', runtimeEnvironmentProvenance: 'nx-task-hash-only', tasks, producer };
}
const OUTPUTS = { 'api:build': ['packages/api/dist', 'packages/leaf/dist'], 'api:native': ['.cache/native'] };
`;

const CAPTURE = `
${PROBE}
const snapshots = await import(Bun.argv[1]);
const [root, source, declaration, mode, frozen] = JSON.parse(Bun.argv[2]);
const probe = async (target, providerRoot) => {
  if (mode === 'miss') return { disposition: 'miss', reason: { kind: 'stale-outputs', taskId: target } };
  const receipt = structuredClone(frozen?.[target] ?? receiptOf(providerRoot, target));
  if (mode === 'no-producer') delete receipt.producer;
  if (mode === 'producer-v2') receipt.producer.version = 2;
  if (mode === 'other-argv') receipt.producer.argv = [...receipt.producer.argv, '--skip-nx-cache'];
  return { disposition: 'hit', receipt };
};
const binding = await snapshots.snapshotBinding(root, 'linked-api', source, declaration, probe);
const verified = binding.kind === 'snapshot' ? snapshots.verifySnapshot(join(binding.directory, '..', '..')).id : null;
process.stdout.write(JSON.stringify({ binding, verified }));
`;

const FREEZE = `
${PROBE}
process.stdout.write(JSON.stringify(Object.fromEntries(['api:build', 'api:native'].map((target) => [target, receiptOf(Bun.argv[1], target)]))));
`;

const DECLARATION = { targets: ['api:build', 'api:native'], artifactModules: ['linked-api/native'] };

const created: string[] = [];
afterEach(() => {
  for (const directory of created.splice(0)) rmSync(directory, { recursive: true, force: true });
});

function write(file: string, content: string): void {
  mkdirSync(join(file, '..'), { recursive: true });
  writeFileSync(file, content);
}

function git(root: string, ...args: string[]): string {
  const run = spawnSync('git', ['-C', root, ...args], { encoding: 'utf8' });
  if (run.status !== 0) throw new Error(`git ${args.join(' ')} failed: ${run.stderr}`);
  return run.stdout.trim();
}

interface Fixture {
  readonly provider: string;
  readonly source: string;
  readonly consumer: string;
}

/** A provider workspace whose `api` package links a sibling workspace package and a provider-local store entry. */
function fixture(): Fixture {
  const base = realpathSync(mkdtempSync(join(tmpdir(), 'linked-snapshots-')));
  created.push(base);
  const provider = join(base, 'provider');
  const source = join(provider, 'packages/api');
  write(
    join(provider, 'package.json'),
    JSON.stringify({ name: 'provider-root', private: true, workspaces: ['packages/*'] }),
  );
  write(join(provider, '.gitignore'), 'dist\n.cache\nnode_modules\n');
  write(join(provider, 'tsconfig.base.json'), JSON.stringify({ compilerOptions: { strict: true } }));
  write(
    join(source, 'package.json'),
    JSON.stringify({
      name: 'linked-api',
      version: '1.0.0',
      type: 'module',
      files: ['dist', 'src'],
      exports: {
        '.': { bun: './src/index.js', types: './dist/index.d.ts', default: './dist/index.js' },
        './native': './src/native.js',
      },
      dependencies: { 'linked-leaf': 'workspace:*', 'store-dep': '1.0.0' },
    }),
  );
  write(join(source, 'tsconfig.json'), JSON.stringify({ extends: '../../tsconfig.base.json' }));
  write(join(source, 'src/index.js'), 'export { leaf } from "linked-leaf";\nexport { store } from "store-dep";\n');
  write(
    join(source, 'src/native.js'),
    [
      "import { existsSync } from 'node:fs';",
      "import { fileURLToPath } from 'node:url';",
      'export function nativeArtifactPath(env = process.env) {',
      "  const path = fileURLToPath(new URL('../../../.cache/native/lib.bin', import.meta.url));",
      '  if (!existsSync(path)) throw new Error(`the fixture native library ${path} is not built: run \\`nx run api:native\\``);',
      '  return path;',
      '}',
      '',
    ].join('\n'),
  );
  write(join(source, 'dist/index.js'), 'export const built = 1;\n');
  write(join(source, 'dist/index.d.ts'), 'export declare const built: number;\n');
  const leaf = join(provider, 'packages/leaf');
  write(
    join(leaf, 'package.json'),
    JSON.stringify({
      name: 'linked-leaf',
      version: '1.0.0',
      type: 'module',
      files: ['dist'],
      exports: './dist/index.js',
    }),
  );
  write(join(leaf, 'dist/index.js'), 'export const leaf = "first";\n');
  const entry = join(provider, 'node_modules/.bun/store-dep@1.0.0/node_modules/store-dep');
  write(
    join(entry, 'package.json'),
    JSON.stringify({ name: 'store-dep', version: '1.0.0', type: 'module', exports: './index.js' }),
  );
  write(join(entry, 'index.js'), 'export const store = "store";\n');
  mkdirSync(join(source, 'node_modules'), { recursive: true });
  symlinkSync('../../leaf', join(source, 'node_modules/linked-leaf'));
  symlinkSync(
    '../../../node_modules/.bun/store-dep@1.0.0/node_modules/store-dep',
    join(source, 'node_modules/store-dep'),
  );
  write(join(provider, '.cache/native/lib.bin'), 'native-one');
  git(provider, 'init', '-q');
  git(provider, 'add', '-A');
  git(provider, '-c', 'user.email=fixture@example.invalid', '-c', 'user.name=fixture', 'commit', '-qm', 'provider');
  const consumer = join(base, 'consumer');
  write(join(consumer, 'package.json'), JSON.stringify({ name: 'consumer', private: true }));
  return { provider, source, consumer };
}

function run(script: string, args: string[]): unknown {
  const child = spawnSync(process.execPath, ['-e', script, ...args], { encoding: 'utf8' });
  if (child.status !== 0) throw new Error(`child failed: ${child.stderr}`);
  return JSON.parse(child.stdout);
}

function capture(
  { consumer, source }: Fixture,
  mode: 'hit' | 'miss' | 'no-producer' | 'producer-v2' | 'other-argv' = 'hit',
  frozen: unknown = null,
): { readonly binding: SnapshotBinding; readonly verified: string | null } {
  const result = run(CAPTURE, [RAW, JSON.stringify([consumer, source, DECLARATION, mode, frozen])]);
  if (typeof result !== 'object' || result === null) throw new Error('capture printed no result');
  const binding: unknown = Reflect.get(result, 'binding');
  const verified: unknown = Reflect.get(result, 'verified');
  if (typeof binding !== 'object' || binding === null) throw new Error('capture printed no binding');
  const kind: unknown = Reflect.get(binding, 'kind');
  if (kind === 'refused') return { binding: { kind, reason: String(Reflect.get(binding, 'reason')) }, verified: null };
  if (kind !== 'snapshot' || typeof verified !== 'string')
    throw new Error(`unexpected capture ${JSON.stringify(result)}`);
  return {
    binding: { kind, id: String(Reflect.get(binding, 'id')), directory: String(Reflect.get(binding, 'directory')) },
    verified,
  };
}

function snapshotOf(result: { readonly binding: SnapshotBinding }): {
  readonly id: string;
  readonly directory: string;
} {
  if (result.binding.kind !== 'snapshot') throw new Error(`expected a snapshot, got: ${result.binding.reason}`);
  return result.binding;
}

function refusalOf(result: { readonly binding: SnapshotBinding }): string {
  if (result.binding.kind !== 'refused') throw new Error(`expected a refusal, got snapshot ${result.binding.id}`);
  return result.binding.reason;
}

describe('linked package snapshots', () => {
  it('copies the provider closure under its census name and verifies it', () => {
    const checkout = fixture();
    const result = capture(checkout);
    const snapshot = snapshotOf(result);
    expect(result.verified).toBe(snapshot.id);
    expect(snapshot.directory).toBe(join(checkout.consumer, '.cache/linked-snapshots', snapshot.id, 'packages/api'));
    expect(readFileSync(join(snapshot.directory, 'dist/index.js'), 'utf8')).toBe('export const built = 1;\n');
    expect(readlinkSync(join(snapshot.directory, 'node_modules/linked-leaf'))).toBe('../../leaf');
    const root = join(snapshot.directory, '..', '..');
    expect(readFileSync(join(root, 'packages/leaf/dist/index.js'), 'utf8')).toBe('export const leaf = "first";\n');
    expect(readFileSync(join(root, '.cache/native/lib.bin'), 'utf8')).toBe('native-one');
    expect(readFileSync(join(root, 'tsconfig.base.json'), 'utf8')).toContain('strict');
    expect(
      lstatSync(join(root, 'node_modules/.bun/store-dep@1.0.0/node_modules/store-dep/index.js')).mode & 0o222,
    ).toBe(0);
    // An unchanged provider under the same receipt is the same snapshot, and an existing one is bound only once it verifies.
    expect(snapshotOf(capture(checkout)).id).toBe(snapshot.id);
  });

  it('reuses a same-tree receipt and names a distinct producer or task identity', () => {
    const checkout = fixture();
    const first = snapshotOf(capture(checkout));
    const originalBasis = git(checkout.provider, 'rev-parse', 'HEAD');
    const frozen = run(FREEZE, [checkout.provider]);
    const rehashed: unknown = JSON.parse(
      JSON.stringify(frozen).replaceAll('"hash":"api:build:', '"hash":"api:build-rehashed:'),
    );
    const otherHash = snapshotOf(capture(checkout, 'hit', rehashed));
    git(
      checkout.provider,
      '-c',
      'user.email=fixture@example.invalid',
      '-c',
      'user.name=fixture',
      'commit',
      '-qm',
      'same tree',
      '--allow-empty',
    );
    const sameTree = snapshotOf(capture(checkout, 'hit', frozen));
    expect(sameTree.id).toBe(first.id);
    expect(
      run(
        'const s = await import(Bun.argv[1]); process.stdout.write(JSON.stringify(s.verifySnapshot(Bun.argv[2]).provenance.producers.map(({ basis }) => basis)));',
        [RAW, join(sameTree.directory, '..', '..')],
      ),
    ).toEqual(DECLARATION.targets.map(() => originalBasis));
    // This unfrozen boundary simulates an actual new publication, not a consumer rewriting the old basis.
    const otherCommit = snapshotOf(capture(checkout));
    expect(new Set([first.id, otherHash.id, otherCommit.id]).size).toBe(3);
    // Each name verifies against its own census and receipt; the bytes are equal.
    for (const snapshot of [first, otherHash, otherCommit]) {
      expect(
        run(
          'const s = await import(Bun.argv[1]); process.stdout.write(JSON.stringify(s.verifySnapshot(Bun.argv[2]).id));',
          [RAW, join(snapshot.directory, '..', '..')],
        ),
      ).toBe(snapshot.id);
    }
    expect(readFileSync(join(otherCommit.directory, 'dist/index.js'), 'utf8')).toBe(
      readFileSync(join(first.directory, 'dist/index.js'), 'utf8'),
    );
  });

  it('refuses a sealed snapshot whose receipt was rewritten to another provenance', () => {
    const snapshot = snapshotOf(capture(fixture()));
    const root = join(snapshot.directory, '..', '..');
    const receiptPath = join(root, '.linked-snapshot.json');
    const receipt = readFileSync(receiptPath, 'utf8');
    chmodSync(receiptPath, 0o644);
    writeFileSync(receiptPath, receipt.replace(/"head":"[0-9a-f]+"/, `"head":"${'0'.repeat(40)}"`));
    expect(() => run('const s = await import(Bun.argv[1]); s.verifySnapshot(Bun.argv[2]);', [RAW, root])).toThrow(
      'are not its name',
    );
  });

  it('keeps a sealed snapshot through provider output churn and names the new build anew', () => {
    const checkout = fixture();
    const first = snapshotOf(capture(checkout));
    rmSync(join(checkout.provider, 'packages/leaf/dist'), { recursive: true });
    write(join(checkout.provider, 'packages/leaf/dist/index.js'), 'export const leaf = "rebuilt";\n');
    const second = capture(checkout);
    expect(snapshotOf(second).id).not.toBe(first.id);
    expect(readFileSync(join(first.directory, '..', 'leaf/dist/index.js'), 'utf8')).toBe(
      'export const leaf = "first";\n',
    );
    expect(
      run(
        'const s = await import(Bun.argv[1]); process.stdout.write(JSON.stringify(s.verifySnapshot(Bun.argv[2]).id));',
        [RAW, join(first.directory, '..', '..')],
      ),
    ).toBe(first.id);
  });

  it('refuses output the producer did not record for its current hash', () => {
    const checkout = fixture();
    const frozen = run(FREEZE, [checkout.provider]);
    write(join(checkout.source, 'dist/index.js'), 'export const built = 2;\n');
    expect(refusalOf(capture(checkout, 'hit', frozen))).toContain('is not the bytes Nx recorded');
  });

  it('refuses copied source that is not the attested committed tree', () => {
    const checkout = fixture();
    write(join(checkout.source, 'src/index.js'), 'export const edited = true;\n');
    expect(refusalOf(capture(checkout))).toContain('differs from tree');
    write(join(checkout.source, 'src/extra.js'), 'export {};\n');
    git(checkout.provider, 'checkout', '--', 'packages/api/src/index.js');
    expect(refusalOf(capture(checkout))).toMatch(
      /neither tracked at tree [0-9a-f]+ \(producer basis [0-9a-f]+\) nor an output/,
    );
  });

  it('retains each original producer basis when targets share a tree across empty commits', () => {
    const checkout = fixture();
    const originalBasis = git(checkout.provider, 'rev-parse', 'HEAD');
    const earlier = run(FREEZE, [checkout.provider]);
    git(
      checkout.provider,
      '-c',
      'user.email=fixture@example.invalid',
      '-c',
      'user.name=fixture',
      'commit',
      '-qm',
      'same tree',
      '--allow-empty',
    );
    const newerBasis = git(checkout.provider, 'rev-parse', 'HEAD');
    const later = run(FREEZE, [checkout.provider]);
    if (typeof earlier !== 'object' || earlier === null || typeof later !== 'object' || later === null)
      throw new Error('the producer fixture omitted its receipts');
    const mixed = { 'api:build': Reflect.get(earlier, 'api:build'), 'api:native': Reflect.get(later, 'api:native') };
    const snapshot = snapshotOf(capture(checkout, 'hit', mixed));
    const bases = run(
      'const s = await import(Bun.argv[1]); process.stdout.write(JSON.stringify(s.verifySnapshot(Bun.argv[2]).provenance.producers.map(({ basis }) => basis)));',
      [RAW, join(snapshot.directory, '..', '..')],
    );
    expect(bases).toEqual([originalBasis, newerBasis]);
  });
  it('refuses a cache hit with no producer completion receipt, or one of an unknown version', () => {
    const checkout = fixture();
    expect(refusalOf(capture(checkout, 'no-producer'))).toContain('has no producer completion receipt');
    expect(refusalOf(capture(checkout, 'producer-v2'))).toContain('producer receipt of unknown version 2');
  });

  it('names the same bytes anew under another production of them', () => {
    const checkout = fixture();
    const first = snapshotOf(capture(checkout));
    const other = snapshotOf(capture(checkout, 'other-argv'));
    expect(other.id).not.toBe(first.id);
    expect(readFileSync(join(other.directory, 'dist/index.js'), 'utf8')).toBe(
      readFileSync(join(first.directory, 'dist/index.js'), 'utf8'),
    );
  });

  it('refuses a target that is not current, naming the run that makes it so', () => {
    expect(refusalOf(capture(fixture(), 'miss'))).toContain('run `nx run api:build` there');
  });

  it("refuses an unbuilt native library in the provider's own words", () => {
    const checkout = fixture();
    rmSync(join(checkout.provider, '.cache/native'), { recursive: true });
    expect(refusalOf(capture(checkout))).toContain('is not built: run `nx run api:native`');
  });

  it('refuses an export the package does not publish', () => {
    const checkout = fixture();
    rmSync(join(checkout.source, 'dist/index.d.ts'));
    expect(refusalOf(capture(checkout))).toContain('which it does not publish');
  });

  it('refuses a link the snapshot cannot carry', () => {
    const checkout = fixture();
    const outside = join(checkout.provider, '..', 'outside');
    write(join(outside, 'package.json'), JSON.stringify({ name: 'outside', version: '1.0.0' }));
    symlinkSync(outside, join(checkout.source, 'node_modules/outside'));
    expect(refusalOf(capture(checkout))).toContain("outside the provider checkout and Bun's cache");
  });

  it('refuses a cowshed store its refresh has not linked onto the build volume', () => {
    const checkout = fixture();
    mkdirSync(join(checkout.consumer, '.cowshed'), { recursive: true });
    symlinkSync(join(checkout.consumer, '..'), join(checkout.consumer, '.cowshed/build'));
    expect(refusalOf(capture(checkout))).toContain('cowshed exec');
    expect(lstatSync(join(checkout.consumer, '.cache/linked-snapshots')).isDirectory()).toBe(true);
  });
});

/** A consumer plugin whose `ensure-built` module is `module`, so planDeclaredLinks takes its real default path. */
function installPlugin({ consumer }: Fixture, module: string): void {
  const plugin = join(consumer, 'node_modules/@smoothbricks/nx-plugin');
  write(
    join(plugin, 'package.json'),
    JSON.stringify({
      name: '@smoothbricks/nx-plugin',
      version: '0.0.0',
      type: 'module',
      exports: { './ensure-built': './ensure-built.js' },
    }),
  );
  write(join(plugin, 'ensure-built.js'), module);
}

/** The read-only probe a current plugin exports, answering from the provider's actual output bytes. */
const PROBE_BUILT = `${PROBE}\nexport async function probeBuilt({ target, cwd }) { return { disposition: 'hit', receipt: receiptOf(cwd, target) }; }\n`;

/** A plugin from before probeBuilt: its only entry builds, so calling it would write the provider. */
function buildingPlugin(marker: string): string {
  return `import { writeFileSync } from 'node:fs';\nexport async function ensureBuilt() { writeFileSync(${JSON.stringify(marker)}, 'built'); return { disposition: 'hit' }; }\n`;
}

const PLAN = `
const links = await import(Bun.argv[1]);
const root = Bun.argv[2];
const plan = await links.planDeclaredLinks(root);
links.applyDeclaredLinks(root, plan);
process.stdout.write(JSON.stringify(plan));
`;

describe('snapshot declarations', () => {
  function declare({ consumer, source }: Fixture): string {
    write(
      join(consumer, 'package.json'),
      JSON.stringify({
        name: 'consumer',
        private: true,
        smoo: {
          developerLinks: { 'linked-api': { directory: join('..', 'provider/packages/api'), snapshot: DECLARATION } },
        },
      }),
    );
    const installed = join(consumer, 'node_modules/.bun/linked-api@1.0.0/node_modules/linked-api');
    write(join(installed, 'package.json'), JSON.stringify({ name: 'linked-api', version: '1.0.0' }));
    symlinkSync('.bun/linked-api@1.0.0/node_modules/linked-api', join(consumer, 'node_modules/linked-api'));
    expect(source).toBe(join(consumer, '..', 'provider/packages/api'));
    return join(consumer, 'node_modules/linked-api');
  }

  it('binds an installed alias to the snapshot and records it', () => {
    const checkout = fixture();
    installPlugin(checkout, PROBE_BUILT);
    const alias = declare(checkout);
    const plan = run(PLAN, [LINKS, checkout.consumer]);
    expect(JSON.stringify(plan)).toContain('"kind":"snapshot"');
    expect(readlinkSync(alias)).toMatch(/^\.\.\/\.cache\/linked-snapshots\/[0-9a-f]{64}\/packages\/api$/);
    expect(readFileSync(join(checkout.consumer, 'node_modules/.smoo-declared-links'), 'utf8')).toContain(
      readlinkSync(alias),
    );
  });

  it('refuses before any call when the installed plugin has no read-only probeBuilt', () => {
    const checkout = fixture();
    const marker = join(checkout.provider, '..', 'provider-built');
    installPlugin(checkout, buildingPlugin(marker));
    const alias = declare(checkout);
    const before = readlinkSync(alias);
    const child = spawnSync(process.execPath, ['-e', PLAN, LINKS, checkout.consumer], { encoding: 'utf8' });
    expect(child.status, child.stderr).toBe(0);
    expect(child.stderr).toContain('has no read-only probeBuilt');
    expect(lstatSync(marker, { throwIfNoEntry: false })).toBeUndefined();
    expect(readlinkSync(alias)).toBe(before);
  });

  it('reports, never claims, an unrecorded alias that links the live checkout, however it is spelled', () => {
    const checkout = fixture();
    installPlugin(checkout, PROBE_BUILT);
    const alias = declare(checkout);
    rmSync(alias);
    symlinkSync(checkout.source, alias);
    const direct = spawnSync(process.execPath, ['-e', PLAN, LINKS, checkout.consumer], { encoding: 'utf8' });
    expect(direct.stderr).toContain('links the live checkout');
    expect(readlinkSync(alias)).toBe(checkout.source);
    // The same checkout through another spelling: a link to an ancestor of the provider.
    const spelled = join(checkout.consumer, '..', 'provider-alias');
    symlinkSync(checkout.provider, spelled);
    rmSync(alias);
    symlinkSync(join(spelled, 'packages/api'), alias);
    const respelled = spawnSync(process.execPath, ['-e', PLAN, LINKS, checkout.consumer], { encoding: 'utf8' });
    expect(respelled.stderr).toContain('links the live checkout');
    expect(readlinkSync(alias)).toBe(join(spelled, 'packages/api'));
  });

  it("keeps a developer's own checkout of another provider", () => {
    const checkout = fixture();
    installPlugin(checkout, PROBE_BUILT);
    const alias = declare(checkout);
    const own = join(checkout.consumer, '..', 'own-api');
    write(join(own, 'package.json'), JSON.stringify({ name: 'linked-api', version: '9.9.9' }));
    rmSync(alias);
    symlinkSync(own, alias);
    const child = spawnSync(process.execPath, ['-e', PLAN, LINKS, checkout.consumer], { encoding: 'utf8' });
    expect(child.stderr).not.toContain('links the live checkout');
    expect(readlinkSync(alias)).toBe(own);
  });

  it('converts unrecorded live links through one managed relink of the package', () => {
    const checkout = fixture();
    installPlugin(checkout, PROBE_BUILT);
    const alias = declare(checkout);
    rmSync(alias);
    symlinkSync(checkout.source, alias);
    // What a managed install does: Bun puts its store link back, then the root postinstall applies declarations.
    const relink = `
const links = await import(Bun.argv[1]);
const root = Bun.argv[2];
const { rmSync, symlinkSync } = await import('node:fs');
await links.keepDeveloperLinks(root, async () => {
  rmSync(root + '/node_modules/linked-api');
  symlinkSync('.bun/linked-api@1.0.0/node_modules/linked-api', root + '/node_modules/linked-api');
  links.applyDeclaredLinks(root, await links.planDeclaredLinks(root));
}, { relink: ['linked-api'] });
`;
    const child = spawnSync(process.execPath, ['-e', relink, LINKS, checkout.consumer], { encoding: 'utf8' });
    expect(child.status, child.stderr).toBe(0);
    expect(readlinkSync(alias)).toMatch(/^\.\.\/\.cache\/linked-snapshots\/[0-9a-f]{64}\/packages\/api$/);
    expect(readFileSync(join(checkout.consumer, 'node_modules/.smoo-declared-links'), 'utf8')).toContain(
      readlinkSync(alias),
    );
    const after = spawnSync(process.execPath, ['-e', PLAN, LINKS, checkout.consumer], { encoding: 'utf8' });
    expect(after.stderr).not.toContain('links the live checkout');
    expect(JSON.parse(after.stdout)).toEqual({ changes: [], unavailable: [] });
  });
});
