import { describe, expect, test } from 'bun:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { isPackageJson, type PackageJson } from '../lib/json.js';
import { prunePublishedExports, publishPackDependencyVersion, withPublishManifest } from './publish-manifest.js';

function conditionKeys(exports: PackageJson['exports'], subpath: string): string[] {
  assert(exports && typeof exports === 'object');
  const conditions = exports[subpath];
  assert(conditions && typeof conditions === 'object');
  return Object.keys(conditions);
}

describe('prunePublishedExports', () => {
  test('drops conditions pointing at TypeScript source and keeps built entries', () => {
    const rootEntry = { types: './dist/index.d.ts', development: './src/index.ts', default: './loader.js' };
    const pkg: PackageJson = {
      name: 'demo',
      exports: {
        './package.json': './package.json',
        '.': rootEntry,
        './policy': { types: './dist/policy.d.mts', development: './src/policy.mts', default: './dist/policy.js' },
      },
    };

    const { manifest, adjustments } = prunePublishedExports(pkg);

    expect(adjustments).toEqual(['exports[.][development]', 'exports[./policy][development]']);
    expect(manifest.exports).toEqual({
      './package.json': './package.json',
      '.': { types: './dist/index.d.ts', default: './loader.js' },
      './policy': { types: './dist/policy.d.mts', default: './dist/policy.js' },
    });
    // The input manifest stays untouched — callers own the on-disk restore.
    expect(rootEntry).toHaveProperty('development');
  });

  test('prunes source targets under any condition and extension, keeping declaration files', () => {
    const pkg: PackageJson = {
      name: 'demo',
      exports: {
        '.': {
          types: './dist/index.d.ts',
          development: './src/index.tsx',
          bun: './src/index.cts',
          default: './dist/index.js',
        },
      },
    };

    const { manifest, adjustments } = prunePublishedExports(pkg);

    expect(adjustments).toEqual(['exports[.][development]', 'exports[.][bun]']);
    expect(manifest.exports).toEqual({ '.': { types: './dist/index.d.ts', default: './dist/index.js' } });
  });

  test('never prunes the types condition, even when it points at .ts source', () => {
    const pkg: PackageJson = {
      name: 'demo',
      exports: { '.': { types: './src/index.ts', default: './dist/index.js' } },
    };

    expect(prunePublishedExports(pkg)).toEqual({ manifest: pkg, adjustments: [] });
  });

  test('leaves a deliberately source-only package untouched', () => {
    // No built artifact exists anywhere in this shape, so pruning would
    // break resolution entirely instead of redirecting it.
    const pkg: PackageJson = {
      name: 'demo',
      exports: {
        './package.json': './package.json',
        '.': {
          types: './src/index.ts',
          development: './src/index.ts',
          import: './src/index.ts',
          default: './src/index.ts',
        },
      },
    };

    expect(prunePublishedExports(pkg)).toEqual({ manifest: pkg, adjustments: [] });
  });

  test('keeps a source-only subpath while pruning its built siblings', () => {
    const pkg: PackageJson = {
      name: 'demo',
      exports: {
        '.': { development: './src/index.ts', default: './dist/index.js' },
        './dev-only': { development: './src/dev.ts' },
      },
    };

    const { manifest, adjustments } = prunePublishedExports(pkg);

    expect(adjustments).toEqual(['exports[.][development]']);
    expect(manifest.exports).toEqual({
      '.': { default: './dist/index.js' },
      './dev-only': { development: './src/dev.ts' },
    });
  });

  test('handles the bare condition-object form of exports', () => {
    const pkg: PackageJson = {
      name: 'demo',
      exports: { development: './src/index.ts', default: './dist/index.js' },
    };

    const { manifest, adjustments } = prunePublishedExports(pkg);

    expect(adjustments).toEqual(['exports[development]']);
    expect(manifest.exports).toEqual({ default: './dist/index.js' });
  });

  test('is a no-op for manifests without TypeScript-source exports', () => {
    const pkg: PackageJson = { name: 'demo', exports: { '.': { default: './dist/index.js' } } };
    expect(prunePublishedExports(pkg)).toEqual({ manifest: pkg, adjustments: [] });

    const noExports: PackageJson = { name: 'demo' };
    expect(prunePublishedExports(noExports)).toEqual({ manifest: noExports, adjustments: [] });
  });
  test('moves the types condition to the front of published condition maps', () => {
    // publint EXPORT_TYPES_SHOULD_BE_FIRST: conditions are order-sensitive and
    // TypeScript must resolve `types` before a runtime condition in an earlier
    // key position wins. The workspace orders `bun`/`development` first so the
    // repo loads live source; the published manifest must not.
    const pkg: PackageJson = {
      name: 'demo',
      exports: {
        './package.json': './package.json',
        '.': { import: './dist/index.js', types: './dist/index.d.ts', default: './dist/index.js' },
        './sub': { node: './dist/sub.js', types: './dist/sub.d.ts' },
      },
    };

    const { manifest, adjustments } = prunePublishedExports(pkg);

    expect(adjustments).toEqual([
      'exports[.]: types condition moved first',
      'exports[./sub]: types condition moved first',
    ]);
    // Key order is the observable behavior, so assert it explicitly.
    expect(conditionKeys(manifest.exports, '.')).toEqual(['types', 'import', 'default']);
    expect(conditionKeys(manifest.exports, './sub')).toEqual(['types', 'node']);
  });

  test('reorder alone is an applied adjustment, not a no-op', () => {
    const pkg: PackageJson = {
      name: 'demo',
      exports: { '.': { bun: './dist/index.js', types: './dist/index.d.ts', default: './dist/index.js' } },
    };

    const { manifest, adjustments } = prunePublishedExports(pkg);

    expect(adjustments).toEqual(['exports[.]: types condition moved first']);
    expect(conditionKeys(manifest.exports, '.')).toEqual(['types', 'bun', 'default']);
  });
});

describe('withPublishManifest', () => {
  const manifestWithSourceExports = `${JSON.stringify(
    {
      name: 'demo',
      exports: { '.': { development: './src/index.ts', default: './loader.js' } },
    },
    null,
    2,
  )}\n`;

  function tempPackageDir(manifestText: string): string {
    const dir = mkdtempSync(join(tmpdir(), 'smoo-publish-manifest-'));
    writeFileSync(join(dir, 'package.json'), manifestText);
    return dir;
  }

  test('packs against the adjusted manifest and restores the original bytes', async () => {
    const dir = tempPackageDir(manifestWithSourceExports);
    try {
      const seen = await withPublishManifest(dir, async () => readFileSync(join(dir, 'package.json'), 'utf8'));
      expect(seen).toContain('"default": "./loader.js"');
      expect(seen).not.toContain('development');
      expect(readFileSync(join(dir, 'package.json'), 'utf8')).toBe(manifestWithSourceExports);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test('restores the original manifest when the pack throws', async () => {
    const dir = tempPackageDir(manifestWithSourceExports);
    try {
      await expect(
        withPublishManifest(dir, async () => {
          throw new Error('pack failed');
        }),
      ).rejects.toThrow('pack failed');
      expect(readFileSync(join(dir, 'package.json'), 'utf8')).toBe(manifestWithSourceExports);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test('leaves the manifest untouched when there is nothing to prune', async () => {
    const plain = `${JSON.stringify({ name: 'demo', exports: { '.': './dist/index.js' } }, null, 2)}\n`;
    const dir = tempPackageDir(plain);
    try {
      const seen = await withPublishManifest(dir, async () => readFileSync(join(dir, 'package.json'), 'utf8'));
      expect(seen).toBe(plain);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test('writes the resolved registry literal for pack and restores the workspace bytes', async () => {
    const plain = `${JSON.stringify({ name: '@priv.test/demo', version: '0.0.1', exports: { '.': './dist/index.js' } }, null, 2)}\n`;
    const dir = tempPackageDir(plain);
    try {
      const seen = await withPublishManifest(dir, async () => readFileSync(join(dir, 'package.json'), 'utf8'), {
        publishConfigRegistry: 'https://forgejo.example.test/api/packages/priv-owner/npm/',
      });
      expect(seen).toContain('"registry": "https://forgejo.example.test/api/packages/priv-owner/npm/"');
      expect(readFileSync(join(dir, 'package.json'), 'utf8')).toBe(plain);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test('restores the original manifest when packing with a registry literal throws', async () => {
    const plain = `${JSON.stringify({ name: '@priv.test/demo', version: '0.0.1', exports: { '.': './dist/index.js' } }, null, 2)}\n`;
    const dir = tempPackageDir(plain);
    try {
      await expect(
        withPublishManifest(
          dir,
          async () => {
            throw new Error('pack failed');
          },
          { publishConfigRegistry: 'https://forgejo.example.test/api/packages/priv-owner/npm/' },
        ),
      ).rejects.toThrow('pack failed');
      expect(readFileSync(join(dir, 'package.json'), 'utf8')).toBe(plain);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
  test('writes the reordered manifest for pack when only condition order changes', async () => {
    const original = `${JSON.stringify(
      {
        name: 'demo',
        exports: { '.': { import: './dist/index.js', types: './dist/index.d.ts', default: './dist/index.js' } },
      },
      null,
      2,
    )}\n`;
    const dir = tempPackageDir(original);
    try {
      const seen = await withPublishManifest(dir, async () => readFileSync(join(dir, 'package.json'), 'utf8'));
      const seenPkg: unknown = JSON.parse(seen);
      assert(isPackageJson(seenPkg));
      expect(conditionKeys(seenPkg.exports, '.')[0]).toBe('types');
      expect(readFileSync(join(dir, 'package.json'), 'utf8')).toBe(original);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test('pins workspace:* dependencies at their published version for a release pack and restores the bytes', async () => {
    const root = gitWorkspace(
      {
        app: {
          name: '@fixture/app',
          version: '1.2.0',
          nx: { name: 'app' },
          dependencies: { '@fixture/dep': 'workspace:*', 'left-pad': '^1.3.0' },
          devDependencies: { '@fixture/tool': 'workspace:*' },
          peerDependencies: { '@fixture/tool': 'workspace:^' },
        },
        dep: { name: '@fixture/dep', version: '0.2.1-next.0', nx: { name: 'dep' } },
        tool: { name: '@fixture/tool', version: '3.0.0', nx: { name: 'tool' } },
      },
      ['dep@0.2.0'],
    );
    const manifestPath = join(root, 'packages/app/package.json');
    try {
      const before = readFileSync(manifestPath, 'utf8');
      const seen: unknown = JSON.parse(
        await withPublishManifest(join(root, 'packages/app'), async () => readFileSync(manifestPath, 'utf8'), {
          workspaceRoot: root,
        }),
      );
      assert(isPackageJson(seen));
      expect(seen.dependencies).toEqual({ '@fixture/dep': '0.2.0', 'left-pad': '^1.3.0' });
      expect(seen.devDependencies).toEqual({ '@fixture/tool': '3.0.0' });
      // Any other workspace range is left for the packed-manifest check to refuse.
      expect(seen.peerDependencies).toEqual({ '@fixture/tool': 'workspace:^' });
      expect(readFileSync(manifestPath, 'utf8')).toBe(before);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});

/** A git workspace of `packages/<dir>` manifests whose one commit carries `tags`. */
function gitWorkspace(packages: Record<string, object>, tags: readonly string[]): string {
  const root = mkdtempSync(join(tmpdir(), 'smoo-publish-pins-'));
  writeFileSync(
    join(root, 'package.json'),
    JSON.stringify({ name: 'fixture-root', private: true, workspaces: ['packages/*'] }),
  );
  for (const [dir, manifest] of Object.entries(packages)) {
    mkdirSync(join(root, 'packages', dir), { recursive: true });
    writeFileSync(join(root, 'packages', dir, 'package.json'), `${JSON.stringify(manifest, null, 2)}\n`);
  }
  const steps = [
    ['init', '-q'],
    ['config', 'user.name', 'Test'],
    ['config', 'user.email', 'test@example.com'],
    ['commit', '-q', '--allow-empty', '-m', 'init'],
    ...tags.map((tag) => ['tag', tag]),
  ];
  for (const args of steps) {
    const result = spawnSync('git', args, { cwd: root, encoding: 'utf8' });
    if (result.status !== 0) {
      rmSync(root, { recursive: true, force: true });
      throw new Error(`git ${args.join(' ')} failed: ${result.error?.message ?? result.stderr}`);
    }
  }
  return root;
}

describe('publishPackDependencyVersion', () => {
  test('pins a prerelease at its last stable tag and any other version as written', () => {
    const root = gitWorkspace({}, ['foo@0.1.1', 'foo@0.1.2', 'foo@0.1.3-rc.1', 'bar@1.0.0-next.0']);
    try {
      const cases = [
        ['foo', '0.1.3-next.0', '0.1.2'], // the newest stable tag, past a newer prerelease tag
        ['foo', '0.4.0', '0.4.0'], // a stable version as written, whatever the tags say
        ['bar', '1.0.0-next.0', '1.0.0-next.0'], // no stable tag: the prerelease, for the packed-manifest check
        ['baz', '0.0.1-next.0', '0.0.1-next.0'], // no tag at all
      ] as const;
      expect(cases.map(([project, version]) => publishPackDependencyVersion(root, project, version))).toEqual(
        cases.map(([, , pinned]) => pinned),
      );
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test('refuses to pin a prerelease when the tags cannot be listed, naming the git command', () => {
    const dir = mkdtempSync(join(tmpdir(), 'smoo-publish-pins-no-repo-'));
    try {
      expect(() => publishPackDependencyVersion(dir, 'foo', '0.1.0-next.0')).toThrow(
        /git tag --list foo@\* .* in .* (exited|failed to start)/,
      );
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
