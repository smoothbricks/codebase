import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { cpSync, existsSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join, sep } from 'node:path';
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
  // Pin direct third-party inputs to the repository's frozen install, but let Bun
  // install a real consumer dependency graph. Symlinking workspace packages into
  // /tmp made their declarations resolve peers from the workspace/global store,
  // bypassing the consumer's @types/react and hiding dependency-closure problems.
  for (const dep of ['typescript', '@types/node']) external.set(dep, root);
  external.set('fast-check', join(root, 'packages', 'statebus-data-loader'));
  for (const dep of ['@types/react', '@types/react-dom', 'react-dom', 'happy-dom'])
    external.set(dep, join(root, 'packages', 'statebus-react'));
  for (const [dep, source] of external) {
    const require = createRequire(join(source, 'package.json'));
    dependencies[dep] = JSON.parse(readFileSync(require.resolve(`${dep}/package.json`), 'utf8')).version;
  }
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
    for (const executable of [
      'scenario',
      'edge-cases',
      'capture-scenarios',
      'execution-scenarios',
      'support-edges',
      'journal-edges',
    ])
      for (const runner of ['node', 'bun'])
        run(`${linker}: ${runner} ${executable}`, runner, [join(consumer, 'dist', 'composed', `${executable}.js`)], {
          cwd: consumer,
          env: runtimeEnv,
          stdio: 'inherit',
        });
    const lockfile = readFileSync(join(consumer, 'bun.lock'));
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
          'rolling checkpoint and effect retention',
          'owned codec migrations and causal positions',
          'typed execution policies, reactions and awaitable cleanup',
          'deny-default support metadata, IDs and portable failures',
          'fixed journal capacity, atomic checkpoint bounds and pre-materialization refusal',
        ],
      },
      null,
      2,
    )}\n`,
  );
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
