import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { prunePublishedExports } from '../../cli/src/monorepo/published-exports.ts';

const root = fileURLToPath(new URL('../../..', import.meta.url));
const names = [
  'statebus-core',
  'statebus-react',
  'statebus-data-loader',
  'statebus-tanstack-query',
  'statebus-navigation-core',
  'statebus-navigation-browser',
  'lmao',
  'arrow-builder',
  'validation',
];
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
  for (const dep of ['typescript', '@types/node']) external.set(dep, root);
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
    mkdirSync(consumer);
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
    writeFileSync(
      join(consumer, 'consumer.ts'),
      readFileSync(new URL('./packed-consumer.fixture.txt', import.meta.url)),
    );
    writeFileSync(
      join(consumer, 'tsconfig.json'),
      JSON.stringify({
        compilerOptions: {
          strict: true,
          module: 'NodeNext',
          moduleResolution: 'NodeNext',
          target: 'ES2022',
          lib: ['ES2024', 'DOM'],
          outDir: 'out',
          types: [],
          skipLibCheck: false,
        },
        include: ['consumer.ts'],
      }),
    );
    const require = createRequire(join(consumer, 'package.json'));
    for (const name of names) {
      const resolved = realpathSync(require.resolve(`@smoothbricks/${name}`));
      assert.ok(resolved.startsWith(`${nodeModules}${sep}`), `Unexpected workspace/source resolution: ${resolved}`);
      // Check bytes as well as location: a registry/source fallback must not pass.
      assert.deepEqual(readFileSync(resolved), readFileSync(join(temporary, 'inspected', name, 'dist', 'index.js')));
      assert.equal(require(`@smoothbricks/${name}/package.json`).version, manifests.get(name).version);
    }
    execFileSync('node', [join(nodeModules, 'typescript', 'bin', 'tsc'), '-p', join(consumer, 'tsconfig.json')], {
      cwd: consumer,
      stdio: 'inherit',
    });
    execFileSync('node', [join(consumer, 'out', 'consumer.js')], { cwd: consumer, env: runtimeEnv, stdio: 'inherit' });
    execFileSync('bun', [join(consumer, 'out', 'consumer.js')], { cwd: consumer, env: runtimeEnv, stdio: 'inherit' });
    for (const name of ['codecs', 'library', 'scenario', 'edge-cases']) {
      writeFileSync(
        join(consumer, `${name}.ts`),
        readFileSync(new URL(`./consumer/${name}.fixture.txt`, import.meta.url)),
      );
    }
    writeFileSync(
      join(consumer, 'tsconfig.composed.json'),
      JSON.stringify({
        compilerOptions: {
          strict: true,
          module: 'NodeNext',
          moduleResolution: 'NodeNext',
          target: 'ES2022',
          lib: ['ES2024', 'DOM'],
          outDir: 'composed',
          types: ['node'],
          skipLibCheck: false,
        },
        // Deliberately exclude the legacy fixture and its ambient module augmentation.
        include: ['codecs.ts', 'library.ts', 'scenario.ts', 'edge-cases.ts'],
      }),
    );
    execFileSync(
      'node',
      [join(nodeModules, 'typescript', 'bin', 'tsc'), '-p', join(consumer, 'tsconfig.composed.json')],
      { cwd: consumer, stdio: 'inherit' },
    );
    // Generate the example's codecs from its imported public types using the repository's
    // normal compiler. The preceding strict tsc pass still checks all package declarations.
    execFileSync('ttsc', ['-p', join(consumer, 'tsconfig.composed.json'), '--emit'], {
      cwd: consumer,
      env: runtimeEnv,
      stdio: 'inherit',
    });
    // Both runners select built StateBus exports and development React for act/StrictMode.
    for (const executable of ['scenario', 'edge-cases'])
      for (const runner of ['node', 'bun'])
        execFileSync(runner, [join(consumer, 'composed', `${executable}.js`)], {
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
        ],
      },
      null,
      2,
    )}\n`,
  );
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
