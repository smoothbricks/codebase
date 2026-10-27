import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { cpSync, existsSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { prunePublishedExports } from '../../cli/src/monorepo/published-exports.ts';

const root = fileURLToPath(new URL('../../..', import.meta.url));
/** Runs one step and logs its wall time, on failure too, so a slow run names the step that was slow. */
function run(step, file, args, options) {
  const started = performance.now();
  try {
    return execFileSync(file, args, options);
  } finally {
    console.error(`verify-packages: ${step} ${((performance.now() - started) / 1000).toFixed(2)}s`);
  }
}
const modeArgs = process.argv.slice(2);
assert.ok(
  modeArgs.length === 0 || (modeArgs.length === 1 && modeArgs[0] === '--fresh'),
  'usage: verify-packages.mjs [--fresh]',
);
const freshResolution = modeArgs[0] === '--fresh';
const exactVersionPattern = /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/;
function exactVersion(range) {
  const value = range.trim().replace(/^=/, '').trim();
  return exactVersionPattern.test(value) ? value : null;
}
function readManifest(path) {
  return JSON.parse(readFileSync(path, 'utf8'));
}
/** Locates one installed package manifest by Node resolution from another manifest. */
function resolveInstalledManifest(fromManifest, dep) {
  const require = createRequire(fromManifest);
  try {
    return require.resolve(`${dep}/package.json`);
  } catch {
    // Packages that do not export ./package.json resolve by their entry point instead.
  }
  let directory = dirname(require.resolve(dep));
  for (;;) {
    const candidate = join(directory, 'package.json');
    if (existsSync(candidate) && readManifest(candidate).name === dep) return candidate;
    const parent = dirname(directory);
    assert.notEqual(parent, directory, `cannot locate installed manifest for ${dep} from ${fromManifest}`);
    directory = parent;
  }
}
/**
 * Pins every consumer dependency edge to the version the repository's frozen install
 * resolved for that edge. One version per name becomes a plain override; a name the
 * frozen install resolves more than once keeps each version behind a parent-scoped
 * override. Anything the frozen install cannot answer refuses, so the pinned check
 * never silently fresh-resolves.
 */
function pinnedConsumerGraph(roots, directDependencies, packedNames) {
  const instances = new Map();
  const queue = [];
  const addInstance = (path, range) => {
    let instance = instances.get(path);
    if (!instance) {
      instance = { id: path, path, lockKey: null, manifest: readManifest(path), ranges: new Set() };
      instances.set(path, instance);
      queue.push(instance);
    }
    if (range) instance.ranges.add(range);
    return instance;
  };
  let lockPackages = null;
  let lockIndex = null;
  const workspaceLockPackages = () => {
    lockPackages ??= Bun.JSONC.parse(readFileSync(join(root, 'bun.lock'), 'utf8')).packages;
    return lockPackages;
  };
  const lockEntryMeta = (entry) => entry.find((value) => value !== null && typeof value === 'object') ?? {};
  const lockKeyFor = (dep, version) => {
    if (!lockIndex) {
      lockIndex = new Map();
      for (const [key, entry] of Object.entries(workspaceLockPackages())) {
        const identity = lockedPackageIdentity(entry[0]);
        const indexKey = `${identity.name}@${identity.version}`;
        if (!lockIndex.has(indexKey)) lockIndex.set(indexKey, []);
        lockIndex.get(indexKey).push(key);
      }
    }
    const keys = lockIndex.get(`${dep}@${version}`) ?? [];
    return keys.length > 0 ? [...keys].sort((a, b) => a.length - b.length)[0] : null;
  };
  const lockChild = (parentKey, dep) => {
    const packages = workspaceLockPackages();
    let prefix = parentKey;
    for (;;) {
      const candidate = `${prefix}/${dep}`;
      if (packages[candidate] && lockedPackageIdentity(packages[candidate][0]).name === dep)
        return { key: candidate, version: lockedPackageIdentity(packages[candidate][0]).version };
      const parentName = lockedPackageIdentity(packages[prefix][0]).name;
      if (prefix === parentName) break;
      prefix = prefix.slice(0, prefix.length - parentName.length - 1);
    }
    return packages[dep] && lockedPackageIdentity(packages[dep][0]).name === dep
      ? { key: dep, version: lockedPackageIdentity(packages[dep][0]).version }
      : null;
  };
  // A platform-filtered package (a wasm binding on a native host) is in the workspace
  // lock but not in the installed tree. Its subtree is walked from the lock's own
  // metadata and nested keys, which record exactly what the frozen install resolved.
  const addLockInstance = (key, range) => {
    const id = `lock:${key}`;
    let instance = instances.get(id);
    if (!instance) {
      const entry = workspaceLockPackages()[key];
      const identity = lockedPackageIdentity(entry[0]);
      const meta = lockEntryMeta(entry);
      instance = {
        id,
        path: null,
        lockKey: key,
        manifest: {
          name: identity.name,
          version: identity.version,
          dependencies: meta.dependencies,
          optionalDependencies: meta.optionalDependencies,
          peerDependencies: meta.peerDependencies,
          peerDependenciesMeta: Object.fromEntries((meta.optionalPeers ?? []).map((dep) => [dep, { optional: true }])),
        },
        ranges: new Set(),
      };
      instances.set(id, instance);
      queue.push(instance);
    }
    if (range) instance.ranges.add(range);
    return instance;
  };
  const deferredPeers = [];
  for (const root of roots) addInstance(root.manifestPath, root.version);
  const edges = [];
  const visited = new Set();
  while (queue.length > 0) {
    const instance = queue.shift();
    if (visited.has(instance.id)) continue;
    visited.add(instance.id);
    const manifest = instance.manifest;
    assert.ok(
      !manifest.bundledDependencies && !manifest.bundleDependencies,
      `pinned consumer cannot express bundled dependencies of ${manifest.name}@${manifest.version}`,
    );
    const groups = [
      ['dependencies', manifest.dependencies],
      ['optionalDependencies', manifest.optionalDependencies],
      ['peerDependencies', manifest.peerDependencies],
    ];
    for (const [kind, deps] of groups) {
      for (const [dep, range] of Object.entries(deps ?? {})) {
        if (dep.startsWith('@smoothbricks/')) {
          assert.ok(
            packedNames.has(dep) || directDependencies[dep],
            `pinned consumer reaches unpacked workspace package ${dep} from ${manifest.name}`,
          );
          continue;
        }
        if (kind === 'peerDependencies' && manifest.peerDependenciesMeta?.[dep]?.optional) continue;
        assert.ok(
          !range.includes(':'),
          `pinned consumer cannot express non-registry dependency ${dep}@${range} from ${manifest.name}`,
        );
        let childPath = null;
        let childLockKey = null;
        let version = null;
        if (!instance.path && kind === 'peerDependencies') {
          // A lock-walked package's peers are provided by an ancestor of the consumer
          // graph, as in the workspace install; they are checked against the pinned
          // graph below instead of being resolved here.
          deferredPeers.push({ parent: manifest.name, dep, range });
          continue;
        }
        if (instance.path) {
          try {
            childPath = resolveInstalledManifest(instance.path, dep);
            version = readManifest(childPath).version;
          } catch (error) {
            version = exactVersion(range);
            if (version) {
              childLockKey = lockKeyFor(dep, version);
              assert.ok(childLockKey, `cannot pin below ${dep}@${version}: it is absent from the workspace lock`);
            } else if (kind === 'peerDependencies') {
              version = directDependencies[dep] ?? null;
            }
            assert.ok(
              version,
              'cannot pin ' +
                manifest.name +
                '@' +
                manifest.version +
                ' dependency ' +
                dep +
                '@' +
                range +
                ' from the repository frozen install: ' +
                error.message,
            );
          }
        } else {
          const child = lockChild(instance.lockKey, dep);
          assert.ok(
            child,
            'cannot pin ' +
              manifest.name +
              '@' +
              manifest.version +
              ' dependency ' +
              dep +
              '@' +
              range +
              ': the workspace lock holds no resolution below ' +
              instance.lockKey,
          );
          childLockKey = child.key;
          version = child.version;
        }
        edges.push({ parent: manifest.name, parentRanges: instance.ranges, dep, version });
        if (childPath) addInstance(childPath, range);
        else if (childLockKey) addLockInstance(childLockKey, range);
      }
    }
  }
  const allowed = new Map();
  const allow = (dep, version) => {
    if (!allowed.has(dep)) allowed.set(dep, new Set());
    allowed.get(dep).add(version);
  };
  for (const [dep, version] of Object.entries(directDependencies)) allow(dep, version);
  for (const edge of edges) allow(edge.dep, edge.version);
  for (const peer of deferredPeers)
    assert.ok(
      allowed.has(peer.dep),
      `cannot pin peer ${peer.dep}@${peer.range} of ${peer.parent}: no pinned consumer package provides it`,
    );
  const overrides = {};
  for (const [dep, versions] of allowed) {
    if (versions.size === 1) overrides[dep] = [...versions][0];
    else if (directDependencies[dep]) overrides[dep] = directDependencies[dep];
  }
  const scoped = new Map();
  for (const edge of edges) {
    if (overrides[edge.dep] === edge.version) continue;
    const key = `${edge.parent}>${edge.dep}`;
    if (!scoped.has(key)) scoped.set(key, []);
    scoped.get(key).push(edge);
  }
  for (const [key, group] of scoped) {
    const versions = new Set(group.map((edge) => edge.version));
    if (versions.size === 1) {
      overrides[key] = [...versions][0];
      continue;
    }
    for (const edge of group) {
      assert.ok(
        edge.parentRanges.size > 0,
        `cannot scope a pin for ${key}: its parent was not reached by a declared range`,
      );
      for (const range of edge.parentRanges) {
        const scopedKey = `${edge.parent}@${range}>${edge.dep}`;
        assert.ok(
          !overrides[scopedKey] || overrides[scopedKey] === edge.version,
          `cannot express the frozen pins for ${scopedKey}`,
        );
        overrides[scopedKey] = edge.version;
      }
    }
  }
  return { overrides, allowed };
}
function lockedPackageIdentity(id) {
  const separator = id.startsWith('@') ? id.indexOf('@', id.indexOf('/') + 1) : id.indexOf('@');
  assert.ok(separator > 0, `cannot parse locked package identity ${id}`);
  return { name: id.slice(0, separator), version: id.slice(separator + 1) };
}
/** Refuses a consumer lockfile holding any version outside the frozen install's resolutions. */
function assertPinnedConsumerLock(lockfile, allowed) {
  const lock = Bun.JSONC.parse(lockfile.toString('utf8'));
  for (const entry of Object.values(lock.packages)) {
    const identity = lockedPackageIdentity(entry[0]);
    if (identity.name.startsWith('@smoothbricks/')) continue;
    assert.ok(
      allowed.get(identity.name)?.has(identity.version),
      `pinned consumer resolved ${identity.name}@${identity.version} outside the repository frozen install`,
    );
  }
}
const flechettePatch = {
  package: '@uwdata/flechette@2.7.0',
  path: 'patches/@uwdata%2Fflechette@2.7.0.patch',
  sha256: '5d5afe8a8df9f19b991c23809df690fa63992d093131f123a3584fd178304abc',
};
// Registry Flechette 2.7.0 has broken owner declarations. Model the real consumer-root
// patch recipe, never a source alias or skipLibCheck, and retain its exact bytes in evidence.
assert.equal(
  createHash('sha256')
    .update(readFileSync(join(root, flechettePatch.path)))
    .digest('hex'),
  flechettePatch.sha256,
  'packed consumers require the exact reviewed Flechette declaration patch',
);
const statebusNames = [
  'statebus-core',
  'statebus-react',
  'statebus-data-loader',
  'statebus-tanstack-query',
  'statebus-navigation-core',
  'statebus-navigation-browser',
];
const names = [...statebusNames, 'lmao', 'arrow-builder', 'validation'];
const manifests = new Map(
  names.map((name) => {
    const path = join(root, 'packages', name, 'package.json');
    return [name, JSON.parse(readFileSync(path, 'utf8'))];
  }),
);
const artifacts = join(root, '.cache', 'statebus-packages');
mkdirSync(artifacts, { recursive: true });
// A failed rerun must not leave a previous success report in the uploaded artifact.
rmSync(join(artifacts, 'validation.json'), { force: true });
const temporary = realpathSync(mkdtempSync(join(tmpdir(), 'statebus-packaged-consumer-')));
const external = new Map();
const dependencies = {};
const overrides = {};
const summaries = [];
const consumers = [];
try {
  // These six production packages are browser/isomorphic. The additional packed
  // operation/codec dependencies have their own platform contracts.
  for (const name of statebusNames) {
    const config = JSON.parse(readFileSync(join(root, 'packages', name, 'tsconfig.lib.json'), 'utf8'));
    assert.deepEqual(config.compilerOptions.types, [], `${name}: platform ambient types must be explicit`);
  }
  for (const name of names) {
    const manifest = manifests.get(name);
    const source = join(root, 'packages', name);
    const archive = join(artifacts, `${name}.tgz`);
    // Let Bun select published files and resolve workspace ranges without
    // mutating manifests that concurrent compiler tasks are reading.
    const sourceArchive = join(temporary, `${name}-source.tgz`);
    run(`${name}: pack source`, 'bun', ['pm', 'pack', '--filename', sourceArchive, '--ignore-scripts', '--quiet'], {
      cwd: source,
      stdio: 'inherit',
    });
    const staged = join(temporary, 'staged', name);
    mkdirSync(staged, { recursive: true });
    run(`${name}: unpack source`, 'tar', ['-xzf', sourceArchive, '-C', staged, '--strip-components=1']);
    const stagedManifestPath = join(staged, 'package.json');
    const stagedManifest = JSON.parse(readFileSync(stagedManifestPath, 'utf8'));
    writeFileSync(stagedManifestPath, `${JSON.stringify(prunePublishedExports(stagedManifest).manifest, null, 2)}\n`);
    run(`${name}: pack pruned`, 'bun', ['pm', 'pack', '--filename', archive, '--ignore-scripts', '--quiet'], {
      cwd: staged,
      stdio: 'inherit',
    });
    const target = join(temporary, 'inspected', name);
    mkdirSync(target, { recursive: true });
    run(`${name}: unpack pruned`, 'tar', ['-xzf', archive, '-C', target, '--strip-components=1']);
    const packed = JSON.parse(readFileSync(join(target, 'package.json'), 'utf8'));
    assert.equal(packed.name, manifest.name);
    assert.equal(packed.publishConfig.access, 'public');
    assert.ok(packed.nx.tags.includes('npm:public'));
    assert.ok(existsSync(join(target, packed.exports['.'].types)));
    assert.ok(existsSync(join(target, packed.exports['.'].import)));
    assert.ok(!existsSync(join(target, 'node_modules')));
    for (const [dep, version] of Object.entries({ ...packed.dependencies, ...packed.peerDependencies })) {
      if (dep.startsWith('@smoothbricks/') && manifests.has(dep.slice('@smoothbricks/'.length))) {
        assert.equal(
          version,
          manifests.get(dep.slice('@smoothbricks/'.length)).version,
          `${name}: packed workspace dependency mismatch for ${dep}`,
        );
      } else external.set(dep, source);
    }
    dependencies[packed.name] = `file:${archive}`;
    overrides[packed.name] = dependencies[packed.name];
    summaries.push({
      name: packed.name,
      version: packed.version,
      sha256: createHash('sha256').update(readFileSync(archive)).digest('hex'),
    });
  }
  // Pin direct third-party inputs to the repository's frozen install. Symlinking
  // workspace packages into /tmp made their declarations resolve peers from the
  // workspace/global store, bypassing the consumer's @types/react and hiding
  // dependency-closure problems.
  for (const dep of ['typescript', '@types/node']) external.set(dep, root);
  for (const dep of ['@types/react', '@types/react-dom', 'react-dom', 'happy-dom'])
    external.set(dep, join(root, 'packages', 'statebus-react'));
  const installedRoots = [];
  for (const [dep, source] of external) {
    const manifestPath = resolveInstalledManifest(join(source, 'package.json'), dep);
    const version = readManifest(manifestPath).version;
    dependencies[dep] = version;
    installedRoots.push({ manifestPath, version });
  }
  // The ordinary check pins the transitive graph to that same frozen install, so its
  // verdict is a function of hashed inputs and Nx may cache it. --fresh instead keeps
  // today's registry resolution for the separate non-gate check.
  const pinned = freshResolution
    ? null
    : pinnedConsumerGraph(installedRoots, dependencies, new Set(names.map((name) => `@smoothbricks/${name}`)));
  if (pinned) Object.assign(overrides, pinned.overrides);
  console.error(`verify-packages: resolution ${freshResolution ? 'fresh' : 'repository frozen install'}`);
  const runtimeEnv = { ...process.env, NODE_ENV: 'test' };
  for (const linker of ['isolated', 'hoisted']) {
    const consumer = join(temporary, linker);
    const nodeModules = join(consumer, 'node_modules');
    // Copy only authored source/configuration. Local editor output must not enter the test install.
    cpSync(new URL('./consumer/', import.meta.url), consumer, {
      recursive: true,
      filter: (path) => !['dist', 'node_modules', '.cache'].includes(path.split(sep).at(-1)),
    });
    mkdirSync(join(consumer, 'patches'), { recursive: true });
    cpSync(join(root, flechettePatch.path), join(consumer, flechettePatch.path));
    writeFileSync(
      join(consumer, 'package.json'),
      // These exact prereleases are not on npm yet. Map transitive requests to the
      // same unmodified tarballs; the packed semver edges were asserted above.
      JSON.stringify({
        name: 'statebus-packed-consumer',
        private: true,
        type: 'module',
        dependencies,
        overrides,
        patchedDependencies: { [flechettePatch.package]: flechettePatch.path },
      }),
    );
    // Keep every dependency inside this disposable install, including with Bun's
    // isolated linker. No workspace source, global store, or TS paths overrides.
    writeFileSync(join(consumer, 'bunfig.toml'), `[install]\nlinker = "${linker}"\nglobalStore = false\n`);
    run(`${linker}: bun install`, 'bun', ['install', '--backend=copyfile', '--ignore-scripts'], {
      cwd: consumer,
      env: runtimeEnv,
      stdio: 'inherit',
    });
    run(`${linker}: frozen bun install`, 'bun', ['install', '--frozen-lockfile', '--ignore-scripts'], {
      cwd: consumer,
      env: runtimeEnv,
      stdio: 'inherit',
    });
    if (pinned) assertPinnedConsumerLock(readFileSync(join(consumer, 'bun.lock')), pinned.allowed);
    const require = createRequire(join(consumer, 'package.json'));
    for (const name of names) {
      const resolved = realpathSync(require.resolve(`@smoothbricks/${name}`));
      assert.ok(resolved.startsWith(`${nodeModules}${sep}`), `Unexpected workspace/source resolution: ${resolved}`);
      // Check the actual declared entry, including packages with a nested dist layout.
      // Byte equality still rejects a registry/source fallback.
      const inspected = join(temporary, 'inspected', name);
      const packed = JSON.parse(readFileSync(join(inspected, 'package.json'), 'utf8'));
      assert.deepEqual(readFileSync(resolved), readFileSync(join(inspected, packed.exports['.'].import)));
      assert.equal(require(`@smoothbricks/${name}/package.json`).version, manifests.get(name).version);
    }
    run(
      `${linker}: tsc platform`,
      'node',
      [join(nodeModules, 'typescript', 'bin', 'tsc'), '-p', join(consumer, 'platform', 'tsconfig.json')],
      {
        cwd: consumer,
        stdio: 'inherit',
      },
    );
    run(`${linker}: node platform consumer`, 'node', [join(consumer, 'dist', 'platform', 'consumer.js')], {
      cwd: consumer,
      env: runtimeEnv,
      stdio: 'inherit',
    });
    run(`${linker}: bun platform consumer`, 'bun', [join(consumer, 'dist', 'platform', 'consumer.js')], {
      cwd: consumer,
      env: runtimeEnv,
      stdio: 'inherit',
    });
    // Strict-check the composed program, every package declaration included (skipLibCheck: false), and generate
    // its codecs from the imported public types with the repository's normal compiler. noEmitOnError: a type
    // error fails here and emits nothing to run.
    run(`${linker}: ttsc composed`, 'ttsc', ['-p', join(consumer, 'composed', 'tsconfig.json'), '--emit'], {
      cwd: consumer,
      env: runtimeEnv,
      stdio: 'inherit',
    });
    // Both runners select built StateBus exports and development React for act/StrictMode.
    for (const executable of ['scenario', 'edge-cases'])
      for (const runner of ['node', 'bun'])
        run(`${linker}: ${runner} ${executable}`, runner, [join(consumer, 'dist', 'composed', `${executable}.js`)], {
          cwd: consumer,
          env: runtimeEnv,
          stdio: 'inherit',
        });
    // The artifacts are Nx outputs the cache restores into other checkouts, so the
    // retained lockfiles name the tarballs by workspace-relative path, never this
    // checkout's absolute one.
    const lockfile = readFileSync(join(consumer, 'bun.lock'), 'utf8').replaceAll(artifacts, '.cache/statebus-packages');
    assert.ok(!lockfile.includes(root), 'consumer lockfile still names the checkout after normalization');
    writeFileSync(join(artifacts, `consumer-${linker}.bun.lock`), lockfile);
    consumers.push({ linker, lockfileSha256: createHash('sha256').update(lockfile).digest('hex') });
  }
  writeFileSync(
    join(artifacts, 'validation.json'),
    `${JSON.stringify(
      {
        formatVersion: 2,
        sourceCommit: process.env.GITHUB_SHA ?? process.env.STATEBUS_BENCH_COMMIT ?? 'working-tree',
        versions: process.versions,
        resolution: freshResolution ? 'fresh' : 'repository-frozen-install',
        packages: summaries,
        dependencyPatches: [flechettePatch],
        consumers,
        verified: [
          'strict declarations with the reviewed Flechette 2.7.0 consumer-root patch',
          'Node consumer',
          'Bun consumer',
          'packed dependency installations',
          'non-ambient composition consumer',
          'LMAO Op/Result binding',
          'generated typed codecs',
          'React lifecycle and no-I/O JSON replay',
        ],
      },
      null,
      2,
    )}\n`,
  );
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
