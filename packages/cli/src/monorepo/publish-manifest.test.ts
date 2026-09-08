import { describe, expect, test } from 'bun:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { isPackageJson, type PackageJson } from '../lib/json.js';
import { prunePublishedExports, withPublishManifest } from './publish-manifest.js';

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
});
