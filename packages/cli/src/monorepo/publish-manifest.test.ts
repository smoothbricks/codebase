import { describe, expect, test } from 'bun:test';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { PackageJson } from '../lib/json.js';
import { applyPublishConfigOverrides, withPublishManifest } from './publish-manifest.js';

describe('applyPublishConfigOverrides', () => {
  test('replaces top-level fields and removes the applied overrides from publishConfig', () => {
    const pkg: PackageJson = {
      name: 'demo',
      exports: { '.': { development: './src/index.ts', default: './loader.js' } },
      publishConfig: { access: 'public', exports: { '.': { default: './loader.js' } } },
    };

    const { manifest, applied } = applyPublishConfigOverrides(pkg);

    expect(applied).toEqual(['exports']);
    expect(manifest.exports).toEqual({ '.': { default: './loader.js' } });
    expect(manifest.publishConfig).toEqual({ access: 'public' });
    // The input manifest stays untouched — callers own the on-disk restore.
    expect(pkg.exports).toEqual({ '.': { development: './src/index.ts', default: './loader.js' } });
    expect(pkg.publishConfig?.exports).toBeDefined();
  });

  test('applies every documented override field', () => {
    const pkg: PackageJson = {
      name: 'demo',
      main: './src/main.ts',
      publishConfig: {
        main: './dist/main.js',
        module: './dist/main.mjs',
        types: './dist/main.d.ts',
        browser: './dist/browser.js',
        bin: { demo: './dist/cli.js' },
        imports: { '#internal': './dist/internal.js' },
      },
    };

    const { manifest, applied } = applyPublishConfigOverrides(pkg);

    expect(applied).toEqual(['main', 'module', 'types', 'browser', 'bin', 'imports']);
    expect(manifest.main).toBe('./dist/main.js');
    expect(manifest.module).toBe('./dist/main.mjs');
    expect(manifest.types).toBe('./dist/main.d.ts');
    expect(manifest.browser).toBe('./dist/browser.js');
    expect(manifest.bin).toEqual({ demo: './dist/cli.js' });
    expect(manifest.imports).toEqual({ '#internal': './dist/internal.js' });
    expect(manifest.publishConfig).toEqual({});
  });

  test('is a no-op without overrides', () => {
    const noPublishConfig: PackageJson = { name: 'demo', exports: './index.js' };
    expect(applyPublishConfigOverrides(noPublishConfig)).toEqual({ manifest: noPublishConfig, applied: [] });

    const accessOnly: PackageJson = { name: 'demo', publishConfig: { access: 'public' } };
    expect(applyPublishConfigOverrides(accessOnly).applied).toEqual([]);
  });
});

describe('withPublishManifest', () => {
  const manifestWithOverride = `${JSON.stringify(
    {
      name: 'demo',
      exports: { '.': { development: './src/index.ts', default: './loader.js' } },
      publishConfig: { access: 'public', exports: { '.': { default: './loader.js' } } },
    },
    null,
    2,
  )}\n`;

  function tempPackageDir(manifestText: string): string {
    const dir = mkdtempSync(join(tmpdir(), 'smoo-publish-manifest-'));
    writeFileSync(join(dir, 'package.json'), manifestText);
    return dir;
  }

  test('packs against the overridden manifest and restores the original bytes', async () => {
    const dir = tempPackageDir(manifestWithOverride);
    try {
      const seen = await withPublishManifest(dir, async () => readFileSync(join(dir, 'package.json'), 'utf8'));
      expect(seen).toContain('"default": "./loader.js"');
      expect(seen).not.toContain('development');
      expect(readFileSync(join(dir, 'package.json'), 'utf8')).toBe(manifestWithOverride);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test('restores the original manifest when the pack throws', async () => {
    const dir = tempPackageDir(manifestWithOverride);
    try {
      await expect(
        withPublishManifest(dir, async () => {
          throw new Error('pack failed');
        }),
      ).rejects.toThrow('pack failed');
      expect(readFileSync(join(dir, 'package.json'), 'utf8')).toBe(manifestWithOverride);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test('leaves the manifest untouched when there is nothing to apply', async () => {
    const plain = `${JSON.stringify({ name: 'demo', publishConfig: { access: 'public' } }, null, 2)}\n`;
    const dir = tempPackageDir(plain);
    try {
      const seen = await withPublishManifest(dir, async () => readFileSync(join(dir, 'package.json'), 'utf8'));
      expect(seen).toBe(plain);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
