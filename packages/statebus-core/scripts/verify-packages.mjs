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
const temporary = mkdtempSync(join(tmpdir(), 'statebus-packaged-consumer-'));
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
    const manifestPath = join(source, 'package.json');
    const original = readFileSync(manifestPath);
    // Use the release packer's pure transform; never patch an already-created tarball.
    writeFileSync(manifestPath, `${JSON.stringify(prunePublishedExports(manifest).manifest, null, 2)}\n`);
    try {
      execFileSync('bun', ['pm', 'pack', '--filename', archive, '--quiet'], { cwd: source, stdio: 'inherit' });
    } finally {
      writeFileSync(manifestPath, original);
    }
    const target = join(temporary, 'inspected', name);
    mkdirSync(target, { recursive: true });
    execFileSync('tar', ['-xzf', archive, '-C', target, '--strip-components=1']);
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
  for (const dep of ['typescript', '@types/node', 'fast-check']) external.set(dep, root);
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
    writeFileSync(
      join(consumer, 'package.json'),
      // These exact prereleases are not on npm yet. Map transitive requests to the
      // same unmodified tarballs; the packed semver edges were asserted above.
      JSON.stringify({ name: 'statebus-packed-consumer', private: true, type: 'module', dependencies, overrides }),
    );
    // Keep every dependency inside this disposable install, including with Bun's
    // isolated linker. No workspace source, global store, or TS paths overrides.
    writeFileSync(join(consumer, 'bunfig.toml'), `[install]\nlinker = "${linker}"\nglobalStore = false\n`);
    execFileSync('bun', ['install', '--backend=copyfile', '--ignore-scripts'], {
      cwd: consumer,
      env: runtimeEnv,
      stdio: 'inherit',
    });
    execFileSync('bun', ['install', '--frozen-lockfile', '--ignore-scripts'], {
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
    execFileSync(
      'node',
      [join(nodeModules, 'typescript', 'bin', 'tsc'), '-p', join(consumer, 'platform', 'tsconfig.json')],
      {
        cwd: consumer,
        stdio: 'inherit',
      },
    );
    execFileSync('node', [join(consumer, 'dist', 'platform', 'consumer.js')], {
      cwd: consumer,
      env: runtimeEnv,
      stdio: 'inherit',
    });
    execFileSync('bun', [join(consumer, 'dist', 'platform', 'consumer.js')], {
      cwd: consumer,
      env: runtimeEnv,
      stdio: 'inherit',
    });
    execFileSync(
      'node',
      [join(nodeModules, 'typescript', 'bin', 'tsc'), '-p', join(consumer, 'composed', 'tsconfig.json')],
      { cwd: consumer, stdio: 'inherit' },
    );
    // Generate the example's codecs from its imported public types using the repository's
    // normal compiler. The preceding strict tsc pass still checks all package declarations.
    execFileSync('ttsc', ['-p', join(consumer, 'composed', 'tsconfig.json'), '--emit'], {
      cwd: consumer,
      env: runtimeEnv,
      stdio: 'inherit',
    });
    // Both runners select built StateBus exports and development React for act/StrictMode.
    for (const executable of ['scenario', 'edge-cases', 'journal'])
      for (const runner of ['node', 'bun'])
        execFileSync(runner, [join(consumer, 'dist', 'composed', `${executable}.js`)], {
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
        consumers,
        verified: [
          'strict declarations',
          'Node consumer',
          'Bun consumer',
          'packed dependency installations',
          'non-ambient composition consumer',
          'LMAO Op/Result binding',
          'generated typed codecs',
          'React lifecycle and no-I/O JSON replay',
          'rolling journal count/byte bounds and replay',
        ],
      },
      null,
      2,
    )}\n`,
  );
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
