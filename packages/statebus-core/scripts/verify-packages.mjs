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

const root = fileURLToPath(new URL('../../..', import.meta.url));
const names = [
  'statebus-core',
  'statebus-react',
  'statebus-data-loader',
  'statebus-tanstack-query',
  'statebus-navigation-core',
  'statebus-navigation-browser',
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
    execFileSync('bun', ['pm', 'pack', '--filename', archive, '--quiet'], { cwd: source, stdio: 'inherit' });
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
      if (dep.startsWith('@smoothbricks/statebus-')) {
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
  for (const dep of ['@types/react', '@types/react-dom']) external.set(dep, join(root, 'packages', 'statebus-react'));
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
        types: [],
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
  execFileSync('bun', [join(consumer, 'out', 'consumer.js')], { cwd: consumer, stdio: 'inherit' });
  writeFileSync(
    join(artifacts, 'validation.json'),
    `${JSON.stringify(
      {
        formatVersion: 1,
        sourceCommit: process.env.GITHUB_SHA ?? process.env.STATEBUS_BENCH_COMMIT ?? 'working-tree',
        versions: process.versions,
        packages: summaries,
        verified: ['strict declarations', 'Node consumer', 'Bun consumer', 'packed workspace resolutions'],
      },
      null,
      2,
    )}\n`,
  );
} finally {
  rmSync(consumer, { recursive: true, force: true });
}
