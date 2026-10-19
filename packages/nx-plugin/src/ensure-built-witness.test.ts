import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
import { createHash } from 'node:crypto';
import { chmodSync, lstatSync, readFileSync, utimesSync, writeFileSync } from 'node:fs';
import {
  chmod,
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
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import fc from 'fast-check';
import type { ProjectGraph } from 'nx/src/config/project-graph';
import { expandOutputs, WorkspaceContext } from 'nx/src/native';
import { workspaceDataDirectoryForWorkspace } from 'nx/src/utils/cache-directory';
import type { readModulePackageJsonWithoutFallbacks } from 'nx/src/utils/package-json';
import typia from 'typia';
import { pastTick } from './__tests__/past-tick.js';
import {
  censusStart,
  type GraphInstallationCensus,
  type GraphWitness,
  graphInstallationIdentity,
  installationIdentityOf,
  movedNodes,
  type NodeWitness,
  nodeStamp,
  nodesUnchanged,
  provenInstallationCensus,
  readGraphWitness,
  recordedInstallationCensus,
  recordedStamp,
  reusableGraph,
  SAME_TICK_NS,
  snapshotsEqual,
  stampMove,
  workspaceSnapshot,
  writeWitness,
} from './ensure-built-witness.js';

let root = '';
const graph: ProjectGraph = {
  nodes: {
    app: { name: 'app', type: 'app', data: { root: 'packages/app', targets: { build: { executor: 'nx:noop' } } } },
  },
  dependencies: { app: [] },
};
const data = () => join(root, '.nx', 'workspace-data');
const snapshot = (previous?: GraphWitness['snapshot']) =>
  workspaceSnapshot(root, new WorkspaceContext(root, data()), previous);

beforeEach(async () => {
  root = await realpath(await mkdtemp(join(tmpdir(), 'ensure-built-witness-')));
  await mkdir(data(), { recursive: true });
  await mkdir(join(root, 'packages', 'app'), { recursive: true });
  await writeFile(join(root, '.gitignore'), '.nx/\n');
  await writeFile(join(root, 'nx.json'), '{}');
  await writeFile(join(root, 'packages', 'app', 'project.json'), '{"name":"app"}');
  await writeFile(join(root, 'packages', 'app', 'package.json'), '{"name":"app","version":"1"}');
  await writeFile(join(root, 'packages', 'app', 'source.txt'), 'before');
  await writeFile(join(root, 'bun.lock'), 'lock-before');
});
afterEach(async () => {
  await rm(root, { recursive: true, force: true });
});

function witness(): GraphWitness {
  return {
    version: 2,
    workspaceRoot: root,
    environment: 'env',
    installation: 'installed',
    installationUnproven: [],
    snapshot: snapshot(),
    projectGraph: graph,
  };
}

describe('authoritative graph witness', () => {
  it('returns the exact previously awaited graph only for unchanged complete inputs', () => {
    const previous = witness();
    expect(reusableGraph(previous, root, 'env', 'installed', snapshot(previous.snapshot))).toBe(graph);
    expect(reusableGraph(previous, root, 'env-changed', 'installed', previous.snapshot)).toBeNull();
    expect(reusableGraph(previous, root, 'env', 'provider-changed', previous.snapshot)).toBeNull();
    expect(reusableGraph(previous, root, 'env', null, previous.snapshot)).toBeNull();
    expect(reusableGraph(previous, root + '/other', 'env', 'installed', previous.snapshot)).toBeNull();
    expect(reusableGraph(null, root, 'env', 'installed', previous.snapshot)).toBeNull();
    expect(reusableGraph(previous, root, 'env', 'installed', { ...previous.snapshot, stable: false })).toBeNull();
  });

  for (const file of [
    'nx.json',
    'bun.lock',
    'packages/app/project.json',
    'packages/app/package.json',
    'packages/app/source.txt',
  ]) {
    it(`immediately rejects changed ${file}, including same-length writes with restored mtime`, async () => {
      const previous = witness();
      const path = join(root, file);
      const { stat } = await import('node:fs/promises');
      const before = await stat(path);
      const original = await readFile(path, 'utf8');
      await writeFile(path, 'x'.repeat(original.length));
      await utimes(path, before.atime, before.mtime);
      expect(reusableGraph(previous, root, 'env', 'installed', snapshot(previous.snapshot))).toBeNull();
    });
  }

  it('rejects newly created and deleted files, including manifests under broad output-like directories', async () => {
    const previous = witness();
    await mkdir(join(root, 'packages', 'app', 'dist'));
    await writeFile(join(root, 'packages', 'app', 'dist', 'project.json'), '{"name":"new-project"}');
    expect(snapshotsEqual(previous.snapshot, snapshot(previous.snapshot))).toBe(false);
    await rm(join(root, 'packages', 'app', 'dist'), { recursive: true });
    await rm(join(root, 'packages', 'app', 'source.txt'));
    expect(snapshotsEqual(previous.snapshot, snapshot(previous.snapshot))).toBe(false);
  });

  it('rehashes a replacement node even when its length and mtime match the previous one', async () => {
    const previous = witness();
    const path = join(root, 'packages', 'app', 'source.txt');
    const { stat } = await import('node:fs/promises');
    const before = await stat(path);
    await writeFile(path + '.replacement', 'AFTER!');
    await utimes(path + '.replacement', before.atime, before.mtime);
    await rename(path + '.replacement', path);
    expect(reusableGraph(previous, root, 'env', 'installed', snapshot(previous.snapshot))).toBeNull();
  });

  it('revokes node evidence for a write/revert even when pre/post content hashes are equal', async () => {
    const previous = witness();
    const path = join(root, 'packages/app/source.txt');
    await writeFile(path, 'AFTER!');
    await writeFile(path, 'before');
    const after = snapshot(previous.snapshot);
    expect(snapshotsEqual(previous.snapshot, after)).toBe(true);
    expect(nodesUnchanged(previous.snapshot.nodes)).toBe(false);
  });
  it('does not admit missing, unreadable, malformed or wrong-version evidence', async () => {
    const path = join(data(), 'ensure-built-graph.json');
    expect(readGraphWitness(path)).toBeNull();
    await writeFile(path, 'not JSON');
    expect(readGraphWitness(path)).toBeNull();
    writeWitness(path, witness());
    expect(readGraphWitness(path)?.projectGraph).toEqual(graph);
    await chmod(path, 0);
    try {
      expect(readGraphWitness(path)).toBeNull();
    } finally {
      await chmod(path, 0o600);
    }
    await writeFile(path, JSON.stringify({ ...witness(), version: 1 }));
    expect(readGraphWitness(path)).toBeNull();
  });
});

// A filesystem dates a change by the kernel's clock, and ZFS on Linux by its coarse tick (1 ms on CI's runner), so a
// same-size rewrite in the tick a stamp was read in leaves the whole stamp as it was: git's racily clean entries. No
// call sets a ctime, so the tick is constructed rather than raced for: each write is dated ahead with utimes, so every
// census reads it inside its change's tick however long the census takes, and the rewrite keeps that date and size.
// On a coarse clock the rewrite then leaves the stamp as recorded in most rounds, and a reader that trusts it reads
// the node as unchanged; on a fine one (APFS, ext4) its ctime still moves, and only there would trusting it pass.
describe('a stamp read in the tick its node changed in', () => {
  const rounds = 16;
  const source = 'packages/app/source.txt';

  it('is re-hashed by the next capture, which a same-size rewrite in that tick would otherwise escape', () => {
    const path = join(root, source);
    const context = new WorkspaceContext(root, data());
    const hashOf = ({ files }: GraphWitness['snapshot']) => files.find(({ file }) => file === source)?.hash;
    for (let round = 0; round < rounds; round++) {
      const ahead = new Date(Date.now() + 60_000);
      writeFileSync(path, `before ${round}`);
      utimesSync(path, ahead, ahead);
      const recorded = workspaceSnapshot(root, context, undefined);
      writeFileSync(path, `after! ${round}`);
      utimesSync(path, ahead, ahead);
      const current = workspaceSnapshot(root, context, recorded);
      // Its own stability check compares what it read; only a later capture distrusts the stamp.
      expect(recorded.stable).toBe(true);
      expect(hashOf(current)).not.toBe(hashOf(recorded));
    }
  });

  it('is recorded unproven, so a reader re-reads the node and finds the rewrite', () => {
    const path = join(root, 'stamped');
    const reread = (node: string) => readFileSync(node, 'utf8') === 'before';
    for (let round = 0; round < rounds; round++) {
      const ahead = new Date(Date.now() + 60_000);
      writeFileSync(path, 'before');
      utimesSync(path, ahead, ahead);
      const recorded: NodeWitness[] = [
        { path, stamp: recordedStamp(lstatSync(path, { bigint: true }), censusStart()) },
      ];
      expect(movedNodes(recorded, reread)).toBeNull();
      writeFileSync(path, 'after!');
      utimesSync(path, ahead, ahead);
      expect(nodesUnchanged(recorded)).toBe(false);
      expect(movedNodes(recorded, reread)).toBe(path);
    }
  });
});

describe('runtime graph witness storage', () => {
  it.each(['relative', 'absolute'])(
    'uses the %s workspace-data binding for missing and unreadable witnesses',
    async (kind) => {
      const inherited = process.env.NX_WORKSPACE_DATA_DIRECTORY;
      process.env.NX_WORKSPACE_DATA_DIRECTORY =
        kind === 'relative' ? '.nx/runtime-data' : join(root, '.nx', 'runtime-data');
      try {
        const directory = workspaceDataDirectoryForWorkspace(root);
        await mkdir(directory, { recursive: true });
        const original = witness();
        writeWitness(join(directory, 'ensure-built-graph.json'), original);
        expect(directory).toBe(join(root, '.nx', 'runtime-data'));
        const path = join(directory, 'ensure-built-graph.json');
        await chmod(path, 0);
        try {
          expect(readGraphWitness(path)).toBeNull();
        } finally {
          await chmod(path, 0o600);
        }
        expect(readGraphWitness(path)).toEqual(original);
        await rm(path);
        expect(readGraphWitness(path)).toBeNull();
        writeWitness(path, original);
        expect(readGraphWitness(path)).toEqual(original);
      } finally {
        if (inherited === undefined) delete process.env.NX_WORKSPACE_DATA_DIRECTORY;
        else process.env.NX_WORKSPACE_DATA_DIRECTORY = inherited;
      }
    },
  );
});

describe('installed graph input identity', () => {
  const parseInstallation = typia.json.createAssertParse<{
    resolutions: [string, string, string | null][];
    nodes: NodeWitness[];
    files: { path: string; sha256: string }[];
  }>();
  const parsed = (identity: string | null) => parseInstallation(typia.assert<string>(identity));
  async function packages() {
    const nx = join(root, 'installed-nx');
    const plugin = join(root, 'installed-plugin');
    const dependency = join(root, 'installed-dependency');
    for (const path of [nx, plugin, dependency]) {
      await mkdir(join(path, 'dist'), { recursive: true });
      await writeFile(join(path, 'package.json'), '{"name":"owned","version":"1","exports":"./dist/index.js"}');
      await writeFile(join(path, 'dist', 'index.js'), 'exports.value = 1;');
    }
    const resolver: typeof readModulePackageJsonWithoutFallbacks = (name) => {
      const path = name === 'nx' ? nx : name === '@smoothbricks/nx-plugin' ? plugin : dependency;
      const dependencies: Record<string, string> =
        name === '@smoothbricks/nx-plugin' ? { alias: 'npm:dependency@1' } : {};
      return {
        path: join(path, 'package.json'),
        packageJson: {
          name,
          version: '1',
          exports: './dist/index.js',
          files: ['dist'],
          dependencies,
        },
      };
    };
    return { nx, plugin, dependency, resolver };
  }

  it('binds public plugin bytes, package resolution metadata, dependency alias targets and tree membership', async () => {
    const { nx, plugin, dependency, resolver } = await packages();
    const output = join(plugin, 'dist', 'index.js');
    await utimes(output, 1_600_000_000, 1_600_000_000);
    const identity = () =>
      graphInstallationIdentity(root, { plugins: ['@smoothbricks/nx-plugin'] }, resolver, expandOutputs, null);
    const original = identity();
    expect(original).not.toBeNull();
    expect(identity()).toBe(original);
    const files = parsed(original).files;
    expect(files.map(({ path }) => path).sort()).toEqual(
      [nx, plugin, dependency].flatMap((path) => [join(path, 'dist', 'index.js'), join(path, 'package.json')]).sort(),
    );
    for (const file of files) {
      expect(file.sha256, file.path).toBe(
        createHash('sha256')
          .update(await readFile(file.path))
          .digest('hex'),
      );
    }
    const before = await stat(output);
    await writeFile(output, 'exports.value = 2;');
    await utimes(output, before.atime, before.mtime);
    expect((await stat(output)).size).toBe(before.size);
    expect((await stat(output)).mtimeMs).toBe(before.mtimeMs);
    const changed = identity();
    expect(changed).not.toBe(original);
    expect(parsed(changed).files.find(({ path }) => path === output)?.sha256).toBe(
      createHash('sha256').update('exports.value = 2;').digest('hex'),
    );
    expect(parsed(changed).files.find(({ path }) => path === output)?.sha256).not.toBe(
      files.find(({ path }) => path === output)?.sha256,
    );
    const emitted = identity();
    await writeFile(join(plugin, 'package.json'), '{"name":"renamed","version":"2","exports":"./other.js"}');
    expect(identity()).not.toBe(emitted);
    const manifest = identity();
    await writeFile(join(dependency, 'added.js'), 'exports.member = true;');
    expect(identity()).not.toBe(manifest);
    const member = identity();
    await rm(join(dependency, 'added.js'));
    expect(identity()).not.toBe(member);
  });

  it('streams every byte across multiple buffer fills, including the final partial fill', async () => {
    const { dependency, resolver } = await packages();
    const output = join(dependency, 'dist', 'payload.bin');
    const bytes = Buffer.alloc(2 * 64 * 1024 + 17, 0x61);
    await writeFile(output, bytes);
    const identity = () =>
      graphInstallationIdentity(root, { plugins: ['@smoothbricks/nx-plugin'] }, resolver, expandOutputs, null);
    const original = parsed(identity());
    expect(original.files.find(({ path }) => path === output)?.sha256).toBe(
      createHash('sha256').update(bytes).digest('hex'),
    );
    const before = await stat(output);
    bytes[64 * 1024] = 0x62;
    bytes[bytes.length - 1] = 0x63;
    await writeFile(output, bytes);
    await utimes(output, before.atime, before.mtime);
    expect((await stat(output)).size).toBe(before.size);
    const changed = parsed(identity());
    expect(changed.files.find(({ path }) => path === output)?.sha256).toBe(
      createHash('sha256').update(bytes).digest('hex'),
    );
    expect(changed.files.find(({ path }) => path === output)?.sha256).not.toBe(
      original.files.find(({ path }) => path === output)?.sha256,
    );
  });

  it('takes only a proven, unmoved file’s hash from a previous census and reads every other file again', async () => {
    const { plugin, resolver } = await packages();
    const output = join(plugin, 'dist', 'index.js');
    const census = (previous?: GraphInstallationCensus) =>
      typia.assert<GraphInstallationCensus>(
        provenInstallationCensus(
          root,
          { plugins: ['@smoothbricks/nx-plugin'] },
          resolver,
          expandOutputs,
          null,
          undefined,
          previous,
        ),
      );
    const hashOf = ({ files }: GraphInstallationCensus) => files.find(({ path }) => path === output)?.sha256;
    const bytes = (text: string) => createHash('sha256').update(text).digest('hex');
    await pastTick();
    const first = census();
    expect(first.unproven).toEqual([]);
    // A recorded hash that is not the file's: a census that takes it did not read the file.
    const recorded = {
      ...first,
      files: first.files.map((file) => (file.path === output ? { ...file, sha256: 'recorded' } : file)),
    };
    expect(hashOf(census(recorded))).toBe('recorded');
    // The witness keeps the census as its identity and its unproven files beside it.
    expect(recordedInstallationCensus(installationIdentityOf(recorded, null), [])).toEqual(recorded);
    expect(hashOf(census({ ...recorded, unproven: [output] }))).toBe(bytes('exports.value = 1;'));
    // Only its ctime moved: a same-mode chmod, as Bun's bin linking makes.
    chmodSync(output, lstatSync(output).mode & 0o7777);
    expect(hashOf(census(recorded))).toBe(bytes('exports.value = 1;'));
    // A same-size rewrite that keeps its mtime moves its ctime: read again.
    const before = await stat(output);
    await writeFile(output, 'exports.value = 2;');
    await utimes(output, before.atime, before.mtime);
    expect(hashOf(census(recorded))).toBe(bytes('exports.value = 2;'));
    // Dated ahead of the census, as a change in its tick is: recorded unproven, and read again by the next census.
    const ahead = new Date(Date.now() + 60_000);
    await utimes(output, ahead, ahead);
    const dated = census();
    expect(dated.unproven).toEqual([output]);
    expect(hashOf(census({ ...dated, files: recorded.files }))).toBe(bytes('exports.value = 2;'));
  });
  it('proves a declared dist link by its physical output tree and rejects changed public bytes', async () => {
    const { plugin, resolver } = await packages();
    const published = join(root, 'published-build-volume');
    await rename(join(plugin, 'dist'), published);
    await symlink(published, join(plugin, 'dist'), 'dir');
    const identity = () =>
      graphInstallationIdentity(root, { plugins: ['@smoothbricks/nx-plugin'] }, resolver, expandOutputs, null);
    const original = identity();
    expect(original).not.toBeNull();
    expect(identity()).toBe(original);
    const output = join(published, 'index.js');
    const before = await stat(output);
    await writeFile(output, 'exports.value = 2;');
    await utimes(output, before.atime, before.mtime);
    const changed = identity();
    expect(changed).not.toBe(original);
    expect(parsed(changed).files.find(({ path }) => path === output)?.sha256).toBe(
      createHash('sha256').update('exports.value = 2;').digest('hex'),
    );
  });

  it('binds an alias to its physical resolution target, not just its package name', async () => {
    const { resolver, dependency } = await packages();
    const alternative = join(root, 'alternative-dependency');
    await mkdir(alternative);
    await writeFile(join(alternative, 'package.json'), '{"name":"owned","version":"1"}');
    const original = graphInstallationIdentity(
      root,
      { plugins: ['@smoothbricks/nx-plugin'] },
      resolver,
      expandOutputs,
      null,
    );
    const changed: typeof resolver = (name, from) => {
      const result = resolver(name, from);
      return name === 'alias' ? { ...result, path: join(alternative, 'package.json') } : result;
    };
    expect(
      graphInstallationIdentity(root, { plugins: ['@smoothbricks/nx-plugin'] }, changed, expandOutputs, null),
    ).not.toBe(original);
    expect(dependency).not.toBe(alternative);
  });

  it('binds an installed dependency exposing only public subpaths, without inventing a main entry', async () => {
    const { plugin, dependency, resolver } = await packages();
    await mkdir(join(plugin, 'node_modules'), { recursive: true });
    await symlink(dependency, join(plugin, 'node_modules/alias'), 'dir');
    await writeFile(
      join(dependency, 'package.json'),
      JSON.stringify({ name: 'owned', version: '1', exports: { './data': './data.json' } }),
    );
    await writeFile(join(dependency, 'data.json'), '{"value":1}');
    const subpaths: typeof resolver = (name, paths) => {
      if (name === 'alias')
        throw Object.assign(new Error('No exports main defined'), { code: 'ERR_PACKAGE_PATH_NOT_EXPORTED' });
      return resolver(name, paths);
    };
    const identity = () =>
      graphInstallationIdentity(root, { plugins: ['@smoothbricks/nx-plugin'] }, subpaths, expandOutputs, null);
    const original = identity();
    expect(original).not.toBeNull();
    await writeFile(join(dependency, 'data.json'), '{"value":2}');
    expect(identity()).not.toBe(original);
  });

  it('uses package-root anchored publication patterns and Nx exclusions without dropping directory members', async () => {
    const { plugin, resolver } = await packages();
    const excluded = join(plugin, 'dist', 'state.tsbuildinfo');
    await writeFile(excluded, 'before');
    const anchored: typeof resolver = (name, paths) => {
      const resolved = resolver(name, paths);
      return { ...resolved, packageJson: { ...resolved.packageJson, files: ['/dist', '!**/*.tsbuildinfo'] } };
    };
    const identity = () =>
      graphInstallationIdentity(root, { plugins: ['@smoothbricks/nx-plugin'] }, anchored, expandOutputs, null);
    const original = identity();
    expect(original).not.toBeNull();
    await writeFile(excluded, 'after');
    expect(identity()).toBe(original);
    await writeFile(join(plugin, 'dist', 'index.js'), 'exports.value = 2;');
    expect(identity()).not.toBe(original);
  });

  it('does not bind an unpublished cache marker but names a public marker changed during census', async () => {
    const { plugin, resolver } = await packages();
    await mkdir(join(plugin, '.cache'));
    const privateMarker = join(plugin, '.cache', 'marker');
    const publicMarker = join(plugin, 'dist', 'index.js');
    await writeFile(privateMarker, 'before');
    const during = (marker: string) => {
      let changed = false;
      let detail = '';
      const mutate: typeof resolver = (name, paths) => {
        const resolved = resolver(name, paths);
        if (name === 'alias' && !changed) {
          writeFileSync(marker, 'after');
          changed = true;
        }
        return resolved;
      };
      const proof = graphInstallationIdentity(
        root,
        { plugins: ['@smoothbricks/nx-plugin'] },
        mutate,
        expandOutputs,
        null,
        (reason) => {
          detail = reason;
        },
      );
      return { proof, changed, detail };
    };
    const unrelated = during(privateMarker);
    expect(unrelated.changed).toBe(true);
    expect(unrelated.proof).not.toBeNull();
    expect(unrelated.detail).toBe('');
    const changed = during(publicMarker);
    expect(changed.changed).toBe(true);
    expect(changed.proof).toBeNull();
    expect(changed.detail).toContain(publicMarker);
  });

  it('retains the full tree without a publication declaration and names its changed marker', async () => {
    const { plugin, resolver } = await packages();
    await mkdir(join(plugin, '.cache'));
    const marker = join(plugin, '.cache', 'marker');
    await writeFile(marker, 'before');
    let changed = false;
    let detail = '';
    const undeclared: typeof resolver = (name, paths) => {
      const resolved = resolver(name, paths);
      if (name === 'alias' && !changed) {
        writeFileSync(marker, 'after');
        changed = true;
      }
      const { files, ...packageJson } = resolved.packageJson;
      expect(files).toEqual(['dist']);
      return { ...resolved, packageJson };
    };
    expect(
      graphInstallationIdentity(
        root,
        { plugins: ['@smoothbricks/nx-plugin'] },
        undeclared,
        expandOutputs,
        null,
        (reason) => {
          detail = reason;
        },
      ),
    ).toBeNull();
    expect(changed).toBe(true);
    expect(detail).toContain(marker);
  });

  it("re-hashes a public file whose ctime alone moved during census, as Bun's bin linking moves a shared store file", async () => {
    const { plugin, resolver } = await packages();
    const output = join(plugin, 'dist/index.js');
    const mode = (await stat(output)).mode & 0o7777;
    let rechmodded = false;
    let detail = '';
    // Any checkout's install that links a bin re-chmods its store target to the mode it already has.
    const sameMode: typeof resolver = (name, paths) => {
      const resolved = resolver(name, paths);
      if (name === 'alias' && !rechmodded) {
        chmodSync(output, mode);
        rechmodded = true;
      }
      return resolved;
    };
    const proof = graphInstallationIdentity(
      root,
      { plugins: ['@smoothbricks/nx-plugin'] },
      sameMode,
      expandOutputs,
      null,
      (reason) => {
        detail = reason;
      },
    );
    expect(rechmodded).toBe(true);
    expect(detail).toBe('');
    // Bound, with the bytes it read: the identity carries the stamps of that census, not a later one's.
    expect(parsed(proof).files.find(({ path }) => path === output)?.sha256).toBe(
      createHash('sha256').update('exports.value = 1;').digest('hex'),
    );
  });

  it('names every moved node, and re-reads only one whose ctime alone moved', async () => {
    const file = join(root, 'stamped');
    await writeFile(file, 'before');
    // A filesystem that stamps from the kernel's coarse tick (ZFS on Linux: 1 ms) keeps the stamp of a chmod or a
    // same-size rewrite that lands in the tick of the last change. No call sets a ctime, so the file is dated a second
    // back and recorded as it was then, when its ctime was its mtime: each change below moves it on every filesystem.
    const past = new Date(Date.now() - 1000);
    await utimes(file, past, past);
    const recorded = lstatSync(file, { bigint: true });
    const chmodded: NodeWitness[] = [
      { path: file, stamp: nodeStamp(Object.assign(recorded, { ctimeNs: recorded.mtimeNs })) },
    ];
    chmodSync(file, (await stat(file)).mode & 0o7777);
    expect(movedNodes(chmodded)).toBe(file);
    expect(movedNodes(chmodded, () => true)).toBeNull();
    const written: NodeWitness[] = [{ path: file, stamp: nodeStamp(lstatSync(file, { bigint: true })) }];
    await writeFile(file, 'after!');
    expect(movedNodes(written, () => true)).toBe(file);
    await rm(file);
    expect(movedNodes(written)).toBe(`${file} (ENOENT)`);
  });

  it('fails loudly for unreadable required module bytes rather than publishing an installation witness', async () => {
    const { plugin, resolver } = await packages();
    const output = join(plugin, 'dist/index.js');
    await chmod(output, 0);
    try {
      expect(() =>
        graphInstallationIdentity(root, { plugins: ['@smoothbricks/nx-plugin'] }, resolver, expandOutputs, null),
      ).toThrow();
    } finally {
      await chmod(output, 0o600);
    }
  });
  it('does not invent an input contract for unknown plugins or unresolved required dependencies', async () => {
    const { resolver } = await packages();
    expect(graphInstallationIdentity(root, { plugins: ['opaque-plugin'] }, resolver, expandOutputs, null)).toBeNull();
    expect(
      graphInstallationIdentity(
        root,
        {},
        () => {
          throw Object.assign(new Error('missing'), { code: 'MODULE_NOT_FOUND' });
        },
        expandOutputs,
        null,
      ),
    ).toBeNull();
  });

  it('refuses an unproved directory link instead of overlooking external plugin files', async () => {
    const { plugin, dependency, resolver } = await packages();
    await symlink(dependency, join(plugin, 'dist/external'), 'dir');
    expect(
      graphInstallationIdentity(root, { plugins: ['@smoothbricks/nx-plugin'] }, resolver, expandOutputs, null),
    ).toBeNull();
  });
});

// The classification every tool-node reader shares, over generated stat fields rather than a filesystem: no clock
// tick, timestamp granularity or concurrent install decides these verdicts, so they are the same on every platform.
describe('a stamp move is judged by the two stamps alone', () => {
  const u64 = fc.bigInt({ min: 0n, max: 2n ** 64n - 1n });
  const fields = fc.record({ dev: u64, ino: u64, mode: u64, size: u64, mtimeNs: u64, ctimeNs: u64 });
  const identity = fc.constantFrom('dev', 'ino', 'mode', 'size', 'mtimeNs');

  it('reads a node whose stamp is unchanged as the same node', () => {
    fc.assert(fc.property(fields, (stats) => stampMove(nodeStamp(stats), nodeStamp(stats)) === 'same'));
  });

  it('reads a move of the ctime alone as a ctime move, in either direction and at any distance', () => {
    fc.assert(
      fc.property(fields, u64, (stats, ctimeNs) => {
        fc.pre(ctimeNs !== stats.ctimeNs);
        return stampMove(nodeStamp(stats), nodeStamp({ ...stats, ctimeNs })) === 'ctime';
      }),
    );
  });

  it('reads a move of device, inode, mode, size or mtime as moved, whatever the ctime did', () => {
    fc.assert(
      fc.property(fields, identity, u64, u64, (stats, field, value, ctimeNs) => {
        fc.pre(value !== stats[field]);
        return stampMove(nodeStamp(stats), nodeStamp({ ...stats, [field]: value, ctimeNs })) === 'moved';
      }),
    );
  });

  // The racily clean rule: a stamp read within SAME_TICK_NS of its node's change (the later of ctime and mtime) proves
  // nothing, and only such a stamp costs a reader a re-read.
  it('records a stamp as read exactly when its census started a full tick after its node changed', () => {
    fc.assert(
      fc.property(fields, u64, (stats, start) => {
        const proven = start - (stats.ctimeNs > stats.mtimeNs ? stats.ctimeNs : stats.mtimeNs) >= SAME_TICK_NS;
        return proven === (recordedStamp(stats, start) === nodeStamp(stats));
      }),
    );
  });

  it('makes every reader re-read an unproven stamp, whatever the ctime of the node it reads', () => {
    fc.assert(
      fc.property(fields, u64, u64, (stats, start, ctimeNs) => {
        fc.pre(recordedStamp(stats, start) !== nodeStamp(stats));
        return stampMove(recordedStamp(stats, start), nodeStamp({ ...stats, ctimeNs })) === 'ctime';
      }),
    );
  });
});
