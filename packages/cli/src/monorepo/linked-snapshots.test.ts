import { afterAll, afterEach, beforeAll, describe, expect, it, spyOn } from 'bun:test';
import { spawnSync } from 'node:child_process';
import {
  chmodSync,
  cpSync,
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
import type { EnsureBuiltReceipt } from '@smoothbricks/nx-plugin/ensure-built';
import { managedAssetsRoot } from '@smoothbricks/nx-plugin/managed-assets';
import {
  applyDeclaredLinks,
  type DeclaredLinksPlan,
  keepDeveloperLinks,
  planDeclaredLinks,
} from '../../../nx-plugin/managed/raw/tooling/direnv/developer-links.ts';
import {
  type ProducerProbe,
  pluginProbe,
  type SnapshotBinding,
  snapshotBinding,
  verifySnapshot,
} from '../../../nx-plugin/managed/raw/tooling/direnv/linked-snapshots.ts';

/**
 * The snapshot module is a managed raw script that shell entry loads before anything is installed, and neither it nor
 * developer-links.ts has a top-level effect, so every behavioral case imports them and calls them here. What they say
 * on stderr is what they give `console.error`, and the refusal a postinstall dies of is the error
 * `planDeclaredLinks` throws. What stays a child process is a subject that is one: shell entry's own `--links` run,
 * and the `git` and `bun pm cache` runs the modules make themselves. The provider is a real git workspace; its Nx
 * producer is the one boundary the fixture stands in for: a plugin package whose probe answers `hit` with a receipt
 * inventorying the provider's actual output bytes at that moment, or `miss`, or a frozen earlier receipt. That both
 * modules load in a plain Bun with nothing installed is proven by the children that load them: the `--links` run
 * below and the shell-entry suite (setup-environment-links.test.ts).
 *
 * Bun keeps what it has imported or resolved for the life of its process. So a plugin never changes once it has
 * answered (a provider whose producer refused leaves a mark its probe reads), and a snapshot runs from the real path
 * of an alias, which a retargeted link would otherwise keep resolving to its first target.
 */

/** What a producer run that refused says, as SB's lmao producer did. */
const REFUSAL = 'Producer tool/module nodes changed across actual execution';

/** The mark a provider keeps once its latest producer run refused: what its plugin's probe then reports. */
const REFUSED_MARK = '.cache/producer-refused';

const PROBE = `
import { createHash } from 'node:crypto';
import { existsSync, lstatSync, readFileSync, readdirSync, readlinkSync } from 'node:fs';
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
  const [head, headTree] = git(root, 'rev-parse', 'HEAD', 'HEAD^{tree}').split('\\n');
  const tasks = [{ id: target, hash: target + ':' + head, outputs }];
  // A boundary control: the producer record a plugin publishes, shaped as its contract names it. The real-Nx
  // control in nx-plugin (linked-snapshots.nx.test.ts) proves the actual publication.
  const producer = {
    version: 1, target, argv: ['nx', 'run', target], trackedTreeClean: true, environmentIdentity: 'fixture-environment',
    inputs: [{ taskId: target, overrides: {}, declared: [], expanded: {}, hashDetails: { command: target, nodes: {}, runtime: {} }, files: [] }],
    tools: [], outputSha256: createHash('sha256').update(JSON.stringify(tasks)).digest('hex'), wallTimeMs: 1,
  };
  return { workspaceRoot: root, head, headTree, fileInputProvenance: 'exact-file-inputs', runtimeEnvironmentProvenance: 'nx-task-hash-only', tasks, producer };
}
const OUTPUTS = { 'api:build': ['packages/api/dist', 'packages/leaf/dist'], 'api:native': ['.cache/native'] };
`;

const PLUGIN_MANIFEST = JSON.stringify({
  name: '@smoothbricks/nx-plugin',
  version: '0.0.0',
  type: 'module',
  exports: { './ensure-built': './ensure-built.js' },
});

/** A plugin package at `directory` whose `ensure-built` module is `module`. */
function writePlugin(directory: string, module: string): void {
  write(join(directory, 'package.json'), PLUGIN_MANIFEST);
  write(join(directory, 'ensure-built.js'), module);
}

/**
 * The read-only probe a current plugin exports, answering from the provider's actual output bytes, or with the refusal
 * of its latest producer run once the provider holds the mark of one.
 */
const PROBE_BUILT = `${PROBE}
export async function probeBuilt({ target, cwd }) {
  if (existsSync(join(cwd, ${JSON.stringify(REFUSED_MARK)}))) return { disposition: 'failed', reason: { kind: 'provider-proof', taskId: target, workspaceRoot: cwd, detail: ${JSON.stringify(REFUSAL)} } };
  return { disposition: 'hit', receipt: receiptOf(cwd, target) };
}
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

// Ephemeral fixtures own no detached Git work: maintenance can remove its lock while the template is copied.
function git(root: string, ...args: string[]): string {
  const run = spawnSync('git', ['-c', 'gc.auto=0', '-c', 'maintenance.auto=false', '-C', root, ...args], {
    encoding: 'utf8',
  });
  if (run.status !== 0) throw new Error(`git ${args.join(' ')} failed: ${run.stderr}`);
  return run.stdout.trim();
}

interface Fixture {
  readonly provider: string;
  readonly source: string;
  readonly consumer: string;
  /** The plugin package that stands in for the provider's Nx producer. */
  readonly producer: string;
}

/** A provider workspace whose `api` package links a sibling workspace package and a provider-local store entry. */
function buildProvider(provider: string): void {
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
  // Solution-style, as lmao's: its sources take their compiler options only through the referenced project.
  write(join(source, 'tsconfig.json'), JSON.stringify({ files: [], references: [{ path: './tsconfig.lib.json' }] }));
  write(join(source, 'tsconfig.lib.json'), JSON.stringify({ extends: '../../tsconfig.base.json', include: ['src'] }));
  write(join(source, 'src/index.js'), 'export { leaf } from "linked-leaf";\nexport { store } from "store-dep";\n');
  write(
    join(source, 'src/native.js'),
    [
      "import { existsSync } from 'node:fs';",
      "import { fileURLToPath } from 'node:url';",
      'export function nativeArtifactPath(env = process.env) {',
      "  const path = fileURLToPath(new URL('../../../.cache/native/lib.bin', import.meta.url));",
      "  if (!existsSync(path)) throw new Error('the fixture native library ' + path + ' is not built: run `nx run api:native`');",
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
}

/** The provider every fixture starts from, built and committed once: each case writes to its own copy of it. */
let template: string | undefined;

beforeAll(() => {
  template = realpathSync(mkdtempSync(join(tmpdir(), 'linked-snapshots-template-')));
  buildProvider(join(template, 'provider'));
});

afterAll(() => {
  if (template !== undefined) rmSync(template, { recursive: true, force: true });
});

function fixture(): Fixture {
  if (template === undefined) throw new Error('the provider template is built before the first case runs');
  const base = realpathSync(mkdtempSync(join(tmpdir(), 'linked-snapshots-')));
  created.push(base);
  const provider = join(base, 'provider');
  cpSync(join(template, 'provider'), provider, { recursive: true, verbatimSymlinks: true });
  const consumer = join(base, 'consumer');
  write(join(consumer, 'package.json'), JSON.stringify({ name: 'consumer', private: true }));
  const producer = join(base, 'producer');
  writePlugin(producer, PROBE_BUILT);
  return { provider, source: join(provider, 'packages/api'), consumer, producer };
}

/** `run`'s result, with what it wrote to stderr: these modules say it with `console.error`. */
async function saying<T>(run: () => Promise<T>): Promise<{ readonly result: T; readonly said: string }> {
  const lines: string[] = [];
  const error = spyOn(console, 'error').mockImplementation((...args: unknown[]) => {
    lines.push(args.map(String).join(' '));
  });
  try {
    return { result: await run(), said: lines.join('\n') };
  } finally {
    error.mockRestore();
  }
}

/** `run` with each of `changes` set, or unset for undefined, in the environment the modules' children inherit. */
async function withEnv<T>(changes: Readonly<Record<string, string | undefined>>, run: () => Promise<T>): Promise<T> {
  const before = Object.fromEntries(Object.keys(changes).map((name) => [name, process.env[name]]));
  const set = (values: Readonly<Record<string, string | undefined>>): void => {
    for (const [name, value] of Object.entries(values)) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  };
  set(changes);
  try {
    return await run();
  } finally {
    set(before);
  }
}

/** How the producer answers a capture: its live receipt, or that bent to a miss or to a malformed producer record. */
type Mode = 'hit' | 'miss' | 'no-producer' | 'producer-v2' | 'other-argv';

/** Receipts the producer gave earlier, by target: answers that outlive the provider state they described. */
type Receipts = Readonly<Record<string, EnsureBuiltReceipt>>;

/** The receipt `probe` gives for `target` in the provider at `root`, which must be a hit. */
async function hitOf(probe: ProducerProbe, target: string, root: string): Promise<EnsureBuiltReceipt> {
  const answer = await probe(target, root);
  if (answer.disposition !== 'hit') {
    throw new Error(`the fixture producer is not current for ${target} in ${root}: ${JSON.stringify(answer)}`);
  }
  return answer.receipt;
}

/** What the producer says of `targets` now, kept to answer after the provider has moved on. */
async function freeze(checkout: Fixture, targets: readonly string[] = DECLARATION.targets): Promise<Receipts> {
  const live = pluginProbe(checkout.producer);
  const receipts: Record<string, EnsureBuiltReceipt> = {};
  for (const target of targets) receipts[target] = await hitOf(live, target, checkout.provider);
  return receipts;
}

/** The producer as `mode` bends its answer, or as `frozen` held it for a target. */
function probeOf(checkout: Fixture, mode: Mode, frozen: Receipts | null): ProducerProbe {
  const live = pluginProbe(checkout.producer);
  return async (target, providerRoot) => {
    if (mode === 'miss') return { disposition: 'miss', reason: { kind: 'stale-outputs', taskId: target } };
    const held = frozen?.[target];
    const receipt = held === undefined ? await hitOf(live, target, providerRoot) : structuredClone(held);
    if (mode === 'no-producer') Reflect.deleteProperty(receipt, 'producer');
    if (mode === 'producer-v2') Reflect.set(receipt.producer, 'version', 2);
    if (mode === 'other-argv') Reflect.set(receipt.producer, 'argv', [...receipt.producer.argv, '--skip-nx-cache']);
    return { disposition: 'hit', receipt };
  };
}

/** Captures `api` into the consumer's store as the producer answers by `mode` or `frozen`, and verifies it. */
async function capture(
  checkout: Fixture,
  mode: Mode = 'hit',
  frozen: Receipts | null = null,
): Promise<{ readonly binding: SnapshotBinding; readonly verified: string | null }> {
  const binding = await snapshotBinding(
    checkout.consumer,
    'linked-api',
    checkout.source,
    DECLARATION,
    probeOf(checkout, mode, frozen),
  );
  const verified = binding.kind === 'snapshot' ? verifySnapshot(join(binding.directory, '..', '..')).id : null;
  return { binding, verified };
}

/** The root `postinstall` (`bun tooling/direnv/developer-links.ts`) in `root`: plans the declared links, applies them. */
async function postinstall(root: string): Promise<{ readonly plan: DeclaredLinksPlan; readonly said: string }> {
  const { result: plan, said } = await saying(async () => {
    const planned = await planDeclaredLinks(root);
    applyDeclaredLinks(root, planned);
    return planned;
  });
  return { plan, said };
}

/**
 * The `leaf` an entry module exports once it has run. It runs from the real path of `entry`: Bun keeps the target a
 * link first resolved to, so an alias retargeted since would be read from its old directory.
 */
async function leafOf(entry: string): Promise<unknown> {
  const loaded: unknown = await import(realpathSync(entry));
  return typeof loaded === 'object' && loaded !== null ? Reflect.get(loaded, 'leaf') : undefined;
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
  it('copies the provider closure under its census name and verifies it', async () => {
    const checkout = fixture();
    const result = await capture(checkout);
    const snapshot = snapshotOf(result);
    expect(result.verified).toBe(snapshot.id);
    expect(snapshot.directory).toBe(join(checkout.consumer, '.cache/linked-snapshots', snapshot.id, 'packages/api'));
    expect(readFileSync(join(snapshot.directory, 'dist/index.js'), 'utf8')).toBe('export const built = 1;\n');
    expect(readlinkSync(join(snapshot.directory, 'node_modules/linked-leaf'))).toBe('../../leaf');
    const root = join(snapshot.directory, '..', '..');
    expect(readFileSync(join(root, 'packages/leaf/dist/index.js'), 'utf8')).toBe('export const leaf = "first";\n');
    expect(readFileSync(join(root, '.cache/native/lib.bin'), 'utf8')).toBe('native-one');
    expect(JSON.parse(readFileSync(join(snapshot.directory, 'tsconfig.lib.json'), 'utf8')).include).toEqual(['src']);
    expect(readFileSync(join(root, 'tsconfig.base.json'), 'utf8')).toContain('strict');
    expect(
      lstatSync(join(root, 'node_modules/.bun/store-dep@1.0.0/node_modules/store-dep/index.js')).mode & 0o222,
    ).toBe(0);
    // An unchanged provider under the same receipt is the same snapshot, and an existing one is bound only once it verifies.
    expect(snapshotOf(await capture(checkout)).id).toBe(snapshot.id);
  });

  it("lists the provider's tracked blobs without the operator's global git configuration", async () => {
    const checkout = fixture();
    // Frozen, so the fixture's own probe runs no git under the environment below.
    const frozen = await freeze(checkout);
    // A sandboxed shell entry keeps the operator's HOME but may not read its git configuration, and git dies
    // "unable to access" it; a self-referencing link fails git the same way (ELOOP).
    const home = join(checkout.consumer, '..', 'operator-home');
    mkdirSync(home);
    symlinkSync('.gitconfig', join(home, '.gitconfig'));
    const environment = {
      HOME: home,
      GIT_CONFIG_GLOBAL: undefined,
      GIT_CONFIG_NOSYSTEM: undefined,
      XDG_CONFIG_HOME: undefined,
    };
    const result = await withEnv(environment, () => capture(checkout, 'hit', frozen));
    expect(result.verified).toBe(snapshotOf(result).id);
  });

  it('reuses a same-tree receipt and names a distinct producer or task identity', async () => {
    const checkout = fixture();
    const first = snapshotOf(await capture(checkout));
    const originalBasis = git(checkout.provider, 'rev-parse', 'HEAD');
    const frozen = await freeze(checkout);
    const rehashed = Object.fromEntries(
      Object.entries(frozen).map(
        ([target, receipt]) =>
          [
            target,
            {
              ...receipt,
              tasks: receipt.tasks.map((task) => ({
                ...task,
                hash: task.hash.replace('api:build:', 'api:build-rehashed:'),
              })),
            },
          ] as const,
      ),
    );
    const otherHash = snapshotOf(await capture(checkout, 'hit', rehashed));
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
    const sameTree = snapshotOf(await capture(checkout, 'hit', frozen));
    expect(sameTree.id).toBe(first.id);
    expect(verifySnapshot(join(sameTree.directory, '..', '..')).provenance.producers.map(({ basis }) => basis)).toEqual(
      DECLARATION.targets.map(() => originalBasis),
    );
    // This unfrozen boundary simulates an actual new publication, not a consumer rewriting the old basis.
    const otherCommit = snapshotOf(await capture(checkout));
    expect(new Set([first.id, otherHash.id, otherCommit.id]).size).toBe(3);
    // Each name verifies against its own census and receipt; the bytes are equal.
    for (const snapshot of [first, otherHash, otherCommit]) {
      expect(verifySnapshot(join(snapshot.directory, '..', '..')).id).toBe(snapshot.id);
    }
    expect(readFileSync(join(otherCommit.directory, 'dist/index.js'), 'utf8')).toBe(
      readFileSync(join(first.directory, 'dist/index.js'), 'utf8'),
    );
  });

  it('refuses a sealed snapshot whose receipt was rewritten to another provenance', async () => {
    const snapshot = snapshotOf(await capture(fixture()));
    const root = join(snapshot.directory, '..', '..');
    const receiptPath = join(root, '.linked-snapshot.json');
    const receipt = readFileSync(receiptPath, 'utf8');
    chmodSync(receiptPath, 0o644);
    writeFileSync(receiptPath, receipt.replace(/"head":"[0-9a-f]+"/, `"head":"${'0'.repeat(40)}"`));
    expect(() => verifySnapshot(root)).toThrow('are not its name');
  });

  it('keeps a sealed snapshot through provider output churn and names the new build anew', async () => {
    const checkout = fixture();
    const first = snapshotOf(await capture(checkout));
    rmSync(join(checkout.provider, 'packages/leaf/dist'), { recursive: true });
    write(join(checkout.provider, 'packages/leaf/dist/index.js'), 'export const leaf = "rebuilt";\n');
    const second = await capture(checkout);
    expect(snapshotOf(second).id).not.toBe(first.id);
    expect(readFileSync(join(first.directory, '..', 'leaf/dist/index.js'), 'utf8')).toBe(
      'export const leaf = "first";\n',
    );
    expect(verifySnapshot(join(first.directory, '..', '..')).id).toBe(first.id);
  });

  it('refuses output the producer did not record for its current hash', async () => {
    const checkout = fixture();
    const frozen = await freeze(checkout);
    write(join(checkout.source, 'dist/index.js'), 'export const built = 2;\n');
    expect(refusalOf(await capture(checkout, 'hit', frozen))).toContain('is not the bytes Nx recorded');
  });

  it('refuses copied source that is not the attested committed tree', async () => {
    const checkout = fixture();
    write(join(checkout.source, 'src/index.js'), 'export const edited = true;\n');
    expect(refusalOf(await capture(checkout))).toContain('differs from tree');
    write(join(checkout.source, 'src/extra.js'), 'export {};\n');
    git(checkout.provider, 'checkout', '--', 'packages/api/src/index.js');
    expect(refusalOf(await capture(checkout))).toMatch(
      /neither tracked at tree [0-9a-f]+ \(producer basis [0-9a-f]+\) nor an output/,
    );
  });

  it('retains each original producer basis when targets share a tree across empty commits', async () => {
    const checkout = fixture();
    const originalBasis = git(checkout.provider, 'rev-parse', 'HEAD');
    const earlier = await freeze(checkout, ['api:build']);
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
    const later = await freeze(checkout, ['api:native']);
    const snapshot = snapshotOf(await capture(checkout, 'hit', { ...earlier, ...later }));
    const bases = verifySnapshot(join(snapshot.directory, '..', '..')).provenance.producers.map(({ basis }) => basis);
    expect(bases).toEqual([originalBasis, newerBasis]);
  });

  it('refuses a cache hit with no producer completion receipt, or one of an unknown version', async () => {
    const checkout = fixture();
    expect(refusalOf(await capture(checkout, 'no-producer'))).toContain('has no producer completion receipt');
    expect(refusalOf(await capture(checkout, 'producer-v2'))).toContain('producer receipt of unknown version 2');
  });

  it('names the same bytes anew under another production of them', async () => {
    const checkout = fixture();
    const first = snapshotOf(await capture(checkout));
    const other = snapshotOf(await capture(checkout, 'other-argv'));
    expect(other.id).not.toBe(first.id);
    expect(readFileSync(join(other.directory, 'dist/index.js'), 'utf8')).toBe(
      readFileSync(join(first.directory, 'dist/index.js'), 'utf8'),
    );
  });

  it('refuses a target that is not current, naming the run that makes it so', async () => {
    expect(refusalOf(await capture(fixture(), 'miss'))).toContain('run `nx run api:build` there');
  });

  it("refuses an unbuilt native library in the provider's own words", async () => {
    const checkout = fixture();
    rmSync(join(checkout.provider, '.cache/native'), { recursive: true });
    expect(refusalOf(await capture(checkout))).toContain('is not built: run `nx run api:native`');
  });

  it('refuses an export the package does not publish', async () => {
    const checkout = fixture();
    rmSync(join(checkout.source, 'dist/index.d.ts'));
    expect(refusalOf(await capture(checkout))).toContain('which it does not publish');
  });

  it('refuses a link the snapshot cannot carry', async () => {
    const checkout = fixture();
    const outside = join(checkout.provider, '..', 'outside');
    write(join(outside, 'package.json'), JSON.stringify({ name: 'outside', version: '1.0.0' }));
    symlinkSync(outside, join(checkout.source, 'node_modules/outside'));
    expect(refusalOf(await capture(checkout))).toContain("outside the provider checkout and Bun's cache");
  });

  it("names Bun's cache as the consumer's Bun does, without opening the read-only provider's manifest", async () => {
    const checkout = fixture();
    // The consumer's own Bun configuration names the cache, so only a lookup in the consumer root finds it. A
    // `BUN_INSTALL_CACHE_DIR` (a managed shell exports one) outranks every bunfig, so the children run without it.
    const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => key !== 'BUN_INSTALL_CACHE_DIR'));
    const cache = join(checkout.provider, '..', 'consumer-cache');
    write(join(checkout.consumer, 'bunfig.toml'), `[install.cache]\ndir = ${JSON.stringify(cache)}\n`);
    const printed = spawnSync(process.execPath, ['pm', 'cache'], { cwd: checkout.consumer, encoding: 'utf8', env });
    expect(printed.status, printed.stderr).toBe(0);
    expect(printed.stdout.trim()).toBe(cache);
    const cached = join(cache, 'links/cached@1.0.0/node_modules/cached');
    write(join(cached, 'package.json'), JSON.stringify({ name: 'cached', version: '1.0.0' }));
    symlinkSync(cached, join(checkout.source, 'node_modules/cached'));
    // A sandboxed consumer may only read its provider; `bun pm` refuses a manifest it cannot open for writing.
    chmodSync(join(checkout.provider, 'package.json'), 0o444);
    const snapshot = snapshotOf(await withEnv({ BUN_INSTALL_CACHE_DIR: undefined }, () => capture(checkout)));
    expect(readlinkSync(join(snapshot.directory, 'node_modules/cached'))).toBe(cached);
  });

  it('refuses a cowshed store its refresh has not linked onto the build volume', async () => {
    const checkout = fixture();
    mkdirSync(join(checkout.consumer, '.cowshed'), { recursive: true });
    symlinkSync(join(checkout.consumer, '..'), join(checkout.consumer, '.cowshed/build'));
    expect(refusalOf(await capture(checkout))).toContain('cowshed exec');
    expect(lstatSync(join(checkout.consumer, '.cache/linked-snapshots')).isDirectory()).toBe(true);
  });
});

/** Where an install leaves the consumer's published plugin: Bun's store entry its alias links. */
const PLUGIN_STORE = '.bun/@smoothbricks+nx-plugin@0.0.0/node_modules/@smoothbricks/nx-plugin';

/** A consumer's installed plugin, a link into Bun's store as an install leaves it: planDeclaredLinks's default path. */
function installPlugin({ consumer }: Fixture, module: string): void {
  writePlugin(join(consumer, 'node_modules', PLUGIN_STORE), module);
  mkdirSync(join(consumer, 'node_modules/@smoothbricks'), { recursive: true });
  symlinkSync(join('..', PLUGIN_STORE), join(consumer, 'node_modules/@smoothbricks/nx-plugin'));
}

/** A plugin from before probeBuilt: its only entry builds, so calling it would write the provider. */
function buildingPlugin(marker: string): string {
  return `import { writeFileSync } from 'node:fs';\nexport async function ensureBuilt() { writeFileSync(${JSON.stringify(marker)}, 'built'); return { disposition: 'hit' }; }\n`;
}

/** A plugin whose probe always reports the refusal of its provider's latest producer run, as SB's lmao producer did. */
const REFUSED_PROBE = `export async function probeBuilt({ target, cwd }) { return { disposition: 'failed', reason: { kind: 'provider-proof', taskId: target, workspaceRoot: cwd, detail: ${JSON.stringify(REFUSAL)} } }; }\n`;

/** What every `bun install` does before its postinstall: the alias links the lockfile's store entry again. */
function reinstall(alias: string): void {
  rmSync(alias);
  symlinkSync('.bun/linked-api@1.0.0/node_modules/linked-api', alias);
}

/** The provider's latest producer run refuses from now on, which every plugin's probe of it then reports. */
function refuseProducer({ provider }: Fixture): void {
  write(join(provider, REFUSED_MARK), '');
}

describe('snapshot declarations', () => {
  /** Declares the snapshot of `linked-api`, with any `extra` checkout declarations beside it. */
  function declare({ consumer, source }: Fixture, extra: Readonly<Record<string, string>> = {}): string {
    write(
      join(consumer, 'package.json'),
      JSON.stringify({
        name: 'consumer',
        private: true,
        smoo: {
          developerLinks: {
            'linked-api': { directory: join('..', 'provider/packages/api'), snapshot: DECLARATION },
            ...extra,
          },
        },
      }),
    );
    const installed = join(consumer, 'node_modules/.bun/linked-api@1.0.0/node_modules/linked-api');
    write(join(installed, 'package.json'), JSON.stringify({ name: 'linked-api', version: '1.0.0' }));
    symlinkSync('.bun/linked-api@1.0.0/node_modules/linked-api', join(consumer, 'node_modules/linked-api'));
    expect(source).toBe(join(consumer, '..', 'provider/packages/api'));
    return join(consumer, 'node_modules/linked-api');
  }

  function pin({ consumer }: Fixture, id: string): void {
    write(
      join(consumer, 'package.json'),
      JSON.stringify({
        name: 'consumer',
        private: true,
        smoo: { developerLinks: { 'linked-api': { directory: '../provider/packages/api', snapshot: id } } },
      }),
    );
  }

  it('keeps a sealed pin across provider commits and installs until the declaration moves', async () => {
    const checkout = fixture();
    const alias = declare(checkout);
    const first = snapshotOf(await capture(checkout));
    const basis = git(checkout.provider, 'rev-parse', 'HEAD');
    pin(checkout, first.id);
    await postinstall(checkout.consumer);
    const binding = readlinkSync(alias);
    write(join(checkout.source, 'src/index.js'), 'export const leaf = "second";\n');
    git(checkout.provider, 'add', '-A');
    git(
      checkout.provider,
      '-c',
      'user.email=fixture@example.invalid',
      '-c',
      'user.name=fixture',
      'commit',
      '-qm',
      'advance',
    );
    const head = git(checkout.provider, 'rev-parse', 'HEAD');
    const moved = await postinstall(checkout.consumer);
    expect(moved.said).toContain('stale pin');
    expect(moved.said).toContain(basis);
    expect(moved.said).toContain(head);
    expect(moved.said).toContain('continuing with the pin');
    expect(moved.plan.changes).toEqual([]);
    expect(readlinkSync(alias)).toBe(binding);
    expect(await leafOf(join(alias, 'src/index.js'))).toBe('first');
    reinstall(alias);
    await postinstall(checkout.consumer);
    expect(readlinkSync(alias)).toBe(binding);
    const next = snapshotOf(await capture(checkout));
    pin(checkout, next.id);
    await postinstall(checkout.consumer);
    expect(realpathSync(alias)).toBe(next.directory);
    expect(await leafOf(join(alias, 'src/index.js'))).toBe('second');
  });

  it('resolves a pin without its provider or an installed probe', async () => {
    const checkout = fixture();
    const alias = declare(checkout);
    const snapshot = snapshotOf(await capture(checkout));
    pin(checkout, snapshot.id);
    rmSync(checkout.provider, { recursive: true });
    const { said } = await postinstall(checkout.consumer);
    expect(said).toContain('cannot inspect');
    expect(said).toContain('continuing with the pin');
    expect(realpathSync(alias)).toBe(snapshot.directory);
    expect(await leafOf(join(alias, 'src/index.js'))).toBe('first');
  });

  for (const defect of ['missing', 'corrupt', 'wrong-package', 'malformed'] as const) {
    it(`refuses a ${defect} pin without falling back to a moving checkout`, async () => {
      const checkout = fixture();
      const alias = declare(checkout);
      const snapshot = snapshotOf(await capture(checkout));
      pin(checkout, snapshot.id);
      await postinstall(checkout.consumer);
      const before = readlinkSync(alias);
      pin(checkout, defect === 'missing' ? '0'.repeat(64) : defect === 'malformed' ? 'main' : snapshot.id);
      if (defect === 'corrupt') {
        const file = join(snapshot.directory, 'src/index.js');
        chmodSync(file, 0o644);
        writeFileSync(file, 'export const changed = true;\n');
      }
      if (defect === 'wrong-package') {
        write(
          join(checkout.consumer, 'package.json'),
          JSON.stringify({
            smoo: { developerLinks: { 'other-api': { directory: '../provider/packages/api', snapshot: snapshot.id } } },
          }),
        );
      }
      await expect(postinstall(checkout.consumer)).rejects.toThrow(
        defect === 'malformed' ? '64 lowercase hex' : 'cannot resolve pinned snapshot',
      );
      expect(readlinkSync(alias)).toBe(before);
    });
  }

  it('reports a stale pin from the shell status without requesting an install or changing it', async () => {
    const checkout = fixture();
    const alias = declare(checkout);
    const snapshot = snapshotOf(await capture(checkout));
    pin(checkout, snapshot.id);
    await postinstall(checkout.consumer);
    git(
      checkout.provider,
      '-c',
      'user.email=fixture@example.invalid',
      '-c',
      'user.name=fixture',
      'commit',
      '--allow-empty',
      '-qm',
      'advance',
    );
    for (const file of [
      'setup-environment.ts',
      'cargo-fetch-bound.ts',
      'cargo-fetch-run.ts',
      'developer-links.ts',
      'installed-bun-links.ts',
      'linked-snapshots.ts',
      'secret-references.ts',
      'uv-relocation.ts',
    ]) {
      write(
        join(checkout.consumer, 'tooling/direnv', file),
        readFileSync(join(managedAssetsRoot, 'raw/tooling/direnv', file), 'utf8'),
      );
    }
    git(checkout.consumer, 'init', '-q');
    const env = Object.fromEntries(
      Object.entries(process.env).filter(([key]) => !['DEVENV_ROOT', 'GITHUB_ACTIONS'].includes(key)),
    );
    for (const flags of [['--links', '--check'], ['--links']]) {
      const child = spawnSync(process.execPath, ['tooling/direnv/setup-environment.ts', ...flags], {
        cwd: checkout.consumer,
        encoding: 'utf8',
        env,
      });
      expect(child.status, child.stderr).toBe(0);
      expect(child.stderr).toContain('stale pin');
      expect(realpathSync(alias)).toBe(snapshot.directory);
    }
  });

  it('loads the capture probe from a pinned plugin rather than its moving provider', async () => {
    const checkout = fixture();
    const pluginName = '@smoothbricks/nx-plugin';
    const plugin = join(checkout.provider, 'packages/plugin');
    writePlugin(plugin, PROBE_BUILT);
    write(
      join(plugin, 'package.json'),
      JSON.stringify({
        name: pluginName,
        version: '0.0.0',
        type: 'module',
        files: ['ensure-built.js'],
        exports: { './ensure-built': './ensure-built.js' },
      }),
    );
    git(checkout.provider, 'add', '-A');
    git(
      checkout.provider,
      '-c',
      'user.email=fixture@example.invalid',
      '-c',
      'user.name=fixture',
      'commit',
      '-qm',
      'plugin',
    );
    const binding = await snapshotBinding(
      checkout.consumer,
      pluginName,
      plugin,
      { targets: ['plugin:build'], artifactModules: [] },
      probeOf(checkout, 'hit', null),
    );
    const { id } = snapshotOf({ binding });
    const alias = declare(checkout);
    installPlugin(checkout, REFUSED_PROBE);
    write(
      join(checkout.consumer, 'package.json'),
      JSON.stringify({
        smoo: {
          developerLinks: {
            'linked-api': { directory: '../provider/packages/api', snapshot: DECLARATION },
            [pluginName]: { directory: '../provider/packages/plugin', snapshot: id },
          },
        },
      }),
    );
    writePlugin(plugin, REFUSED_PROBE);
    const { said } = await postinstall(checkout.consumer);
    expect(said).not.toContain('no snapshot');
    expect(readlinkSync(alias)).toMatch(/linked-snapshots\/[0-9a-f]{64}\/packages\/api$/);
    expect(realpathSync(join(checkout.consumer, 'node_modules', pluginName))).toContain(`/linked-snapshots/${id}/`);
  });

  it('binds an installed alias to the snapshot and records it', async () => {
    const checkout = fixture();
    installPlugin(checkout, PROBE_BUILT);
    const alias = declare(checkout);
    const { plan } = await postinstall(checkout.consumer);
    expect(plan.changes.map(({ binding }) => binding.kind)).toEqual(['snapshot']);
    expect(readlinkSync(alias)).toMatch(/^\.\.\/\.cache\/linked-snapshots\/[0-9a-f]{64}\/packages\/api$/);
    expect(readFileSync(join(checkout.consumer, 'node_modules/.smoo-declared-links'), 'utf8')).toContain(
      readlinkSync(alias),
    );
  });

  it('probes through the plugin checkout a declaration links, never the published one an install put back', async () => {
    const checkout = fixture();
    const marker = join(checkout.provider, '..', 'provider-built');
    // A plain `bun install` re-points the declared plugin alias at the lockfile's version before its postinstall.
    installPlugin(checkout, buildingPlugin(marker));
    const declared = join(checkout.consumer, '..', 'plugin');
    writePlugin(declared, PROBE_BUILT);
    const alias = declare(checkout, { '@smoothbricks/nx-plugin': join('..', 'plugin') });
    const { said } = await postinstall(checkout.consumer);
    expect(said).not.toContain('no snapshot');
    expect(readlinkSync(alias)).toMatch(/^\.\.\/\.cache\/linked-snapshots\/[0-9a-f]{64}\/packages\/api$/);
    expect(realpathSync(join(checkout.consumer, 'node_modules/@smoothbricks/nx-plugin'))).toBe(declared);
    expect(lstatSync(marker, { throwIfNoEntry: false })).toBeUndefined();
  });

  it('refuses before any call when the installed plugin has no read-only probeBuilt', async () => {
    const checkout = fixture();
    const marker = join(checkout.provider, '..', 'provider-built');
    installPlugin(checkout, buildingPlugin(marker));
    const alias = declare(checkout);
    const before = readlinkSync(alias);
    const { said } = await postinstall(checkout.consumer);
    expect(said).toContain('has no read-only probeBuilt');
    expect(lstatSync(marker, { throwIfNoEntry: false })).toBeUndefined();
    expect(readlinkSync(alias)).toBe(before);
  });

  it('reports, never claims, an unrecorded alias that links the live checkout, however it is spelled', async () => {
    const checkout = fixture();
    installPlugin(checkout, PROBE_BUILT);
    const alias = declare(checkout);
    rmSync(alias);
    symlinkSync(checkout.source, alias);
    expect((await postinstall(checkout.consumer)).said).toContain('links the live checkout');
    expect(readlinkSync(alias)).toBe(checkout.source);
    // The same checkout through another spelling: a link to an ancestor of the provider.
    const spelled = join(checkout.consumer, '..', 'provider-alias');
    symlinkSync(checkout.provider, spelled);
    rmSync(alias);
    symlinkSync(join(spelled, 'packages/api'), alias);
    expect((await postinstall(checkout.consumer)).said).toContain('links the live checkout');
    expect(readlinkSync(alias)).toBe(join(spelled, 'packages/api'));
  });

  it("keeps a developer's own checkout of another provider", async () => {
    const checkout = fixture();
    installPlugin(checkout, PROBE_BUILT);
    const alias = declare(checkout);
    const own = join(checkout.consumer, '..', 'own-api');
    write(join(own, 'package.json'), JSON.stringify({ name: 'linked-api', version: '9.9.9' }));
    rmSync(alias);
    symlinkSync(own, alias);
    expect((await postinstall(checkout.consumer)).said).not.toContain('links the live checkout');
    expect(readlinkSync(alias)).toBe(own);
  });

  it('converts unrecorded live links through one managed relink of the package', async () => {
    const checkout = fixture();
    installPlugin(checkout, PROBE_BUILT);
    const alias = declare(checkout);
    rmSync(alias);
    symlinkSync(checkout.source, alias);
    // What a managed install does: Bun puts its store link back, then the root postinstall applies declarations.
    await saying(() =>
      keepDeveloperLinks(
        checkout.consumer,
        async () => {
          reinstall(alias);
          applyDeclaredLinks(checkout.consumer, await planDeclaredLinks(checkout.consumer));
        },
        { relink: ['linked-api'] },
      ),
    );
    expect(readlinkSync(alias)).toMatch(/^\.\.\/\.cache\/linked-snapshots\/[0-9a-f]{64}\/packages\/api$/);
    expect(readFileSync(join(checkout.consumer, 'node_modules/.smoo-declared-links'), 'utf8')).toContain(
      readlinkSync(alias),
    );
    const after = await postinstall(checkout.consumer);
    expect(after.said).not.toContain('links the live checkout');
    expect(after.plan).toEqual({ changes: [], unavailable: [], reports: [] });
  });

  it('keeps the recorded snapshot an install re-pointed when no new snapshot can be made', async () => {
    const checkout = fixture();
    installPlugin(checkout, PROBE_BUILT);
    const alias = declare(checkout);
    await postinstall(checkout.consumer);
    const bound = readlinkSync(alias);
    const id = bound.split('/')[3];
    const receipts = readFileSync(join(checkout.consumer, 'node_modules/.smoo-declared-links'), 'utf8');
    refuseProducer(checkout);
    reinstall(alias);
    const installed = await postinstall(checkout.consumer);
    expect(installed.said).toContain(REFUSAL);
    expect(installed.said).toContain(`its aliases stay bound to snapshot ${id}`);
    expect(readlinkSync(alias)).toBe(bound);
    expect(readFileSync(join(checkout.consumer, 'node_modules/.smoo-declared-links'), 'utf8')).toBe(receipts);
    // A shell entry with nothing installed leaves the binding as it is and says so.
    const entry = await postinstall(checkout.consumer);
    expect(entry.said).toContain('its current binding stays');
    expect(entry.plan.changes).toEqual([]);
  });

  it('says the installed version stays when the recorded snapshot is gone', async () => {
    const checkout = fixture();
    installPlugin(checkout, PROBE_BUILT);
    const alias = declare(checkout);
    await postinstall(checkout.consumer);
    rmSync(join(checkout.consumer, '.cache/linked-snapshots'), { recursive: true });
    refuseProducer(checkout);
    reinstall(alias);
    const { said } = await postinstall(checkout.consumer);
    expect(said).toContain('1 alias with no recorded snapshot keeps the installed version');
    expect(readlinkSync(alias)).toBe('.bun/linked-api@1.0.0/node_modules/linked-api');
  });
});
