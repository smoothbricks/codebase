import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
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
const consumer = mkdtempSync(join(tmpdir(), 'statebus-packaged-consumer-'));
const nodeModules = join(consumer, 'node_modules');
const external = new Map();
const summaries = [];
try {
  for (const name of names) {
    const manifest = manifests.get(name);
    const source = join(root, 'packages', name);
    const archive = join(artifacts, `${name}.tgz`);
    const manifestPath = join(source, 'package.json');
    const original = readFileSync(manifestPath);
    // Use the release packer's pure manifest transform, not a second export-map implementation.
    writeFileSync(manifestPath, `${JSON.stringify(prunePublishedExports(manifest).manifest, null, 2)}\n`);
    try {
      execFileSync('bun', ['pm', 'pack', '--filename', archive, '--quiet'], { cwd: source, stdio: 'inherit' });
    } finally {
      writeFileSync(manifestPath, original);
    }
    const target = join(nodeModules, '@smoothbricks', name);
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
    summaries.push({
      name: packed.name,
      version: packed.version,
      sha256: createHash('sha256').update(readFileSync(archive)).digest('hex'),
    });
  }
  // Only third-party dependencies may use the already-verified workspace install.
  // Every StateBus package and its transitive StateBus imports resolve to extracted tarballs.
  for (const dep of ['typescript', '@types/node']) external.set(dep, root);
  for (const dep of ['@types/react', '@types/react-dom', 'react-dom', 'happy-dom'])
    external.set(dep, join(root, 'packages', 'statebus-react'));
  for (const [dep, source] of external) {
    const require = createRequire(join(source, 'package.json'));
    const manifest = require.resolve(`${dep}/package.json`);
    const target = join(nodeModules, dep);
    mkdirSync(dirname(target), { recursive: true });
    symlinkSync(dirname(realpathSync(manifest)), target, 'dir');
  }
  writeFileSync(
    join(consumer, 'package.json'),
    JSON.stringify({ name: 'statebus-packed-consumer', private: true, type: 'module' }),
  );
  writeFileSync(join(consumer, 'consumer.ts'), readFileSync(new URL('./packed-consumer.fixture.txt', import.meta.url)));
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
        types: ['node'],
        skipLibCheck: false,
      },
      include: ['consumer.ts'],
    }),
  );
  const require = createRequire(join(consumer, 'package.json'));
  for (const name of names) {
    const resolved = realpathSync(require.resolve(`@smoothbricks/${name}`));
    assert.ok(
      resolved.startsWith(join(nodeModules, '@smoothbricks', name)),
      `Unexpected workspace/source resolution: ${resolved}`,
    );
  }
  execFileSync('node', [join(nodeModules, 'typescript', 'bin', 'tsc'), '-p', join(consumer, 'tsconfig.json')], {
    cwd: consumer,
    stdio: 'inherit',
  });
  execFileSync('node', [join(consumer, 'out', 'consumer.js')], { cwd: consumer, stdio: 'inherit' });
  // Published manifests select built exports even when Bun's development condition is active.
  const runtimeEnv = { ...process.env, NODE_ENV: 'test' };
  execFileSync('bun', [join(consumer, 'out', 'consumer.js')], { cwd: consumer, env: runtimeEnv, stdio: 'inherit' });
  for (const name of ['codecs', 'library', 'scenario']) {
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
      include: ['codecs.ts', 'library.ts', 'scenario.ts'],
    }),
  );
  execFileSync(
    'node',
    [join(nodeModules, 'typescript', 'bin', 'tsc'), '-p', join(consumer, 'tsconfig.composed.json')],
    { cwd: consumer, stdio: 'inherit' },
  );
  // React StrictMode checks need its development build, while StateBus must resolve built JS.
  // Node selects the import condition in either environment; its assertion rejects source paths.
  execFileSync('node', [join(consumer, 'composed', 'scenario.js')], { cwd: consumer, stdio: 'inherit' });
  // React's test build keeps act/StrictMode; every package resolution is independently checked.
  execFileSync('bun', [join(consumer, 'composed', 'scenario.js')], {
    cwd: consumer,
    env: runtimeEnv,
    stdio: 'inherit',
  });
  writeFileSync(
    join(artifacts, 'validation.json'),
    `${JSON.stringify(
      {
        formatVersion: 1,
        sourceCommit: process.env.GITHUB_SHA ?? process.env.STATEBUS_BENCH_COMMIT ?? 'working-tree',
        versions: process.versions,
        packages: summaries,
        verified: [
          'strict declarations',
          'Node consumer',
          'Bun consumer',
          'packed workspace resolutions',
          'non-ambient composition consumer',
          'LMAO Op/Result binding',
          'React lifecycle and no-I/O replay',
        ],
      },
      null,
      2,
    )}\n`,
  );
} finally {
  rmSync(consumer, { recursive: true, force: true });
}
