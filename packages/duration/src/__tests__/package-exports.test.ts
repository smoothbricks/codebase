import { describe, expect, it } from 'bun:test';
import { isRecord } from '@smoothbricks/validation';

type ExportConditions = Record<string, string>;

type PackageManifest = {
  name: string;
  exports: Record<string, string | ExportConditions>;
};

const packageUrl = new URL('../../package.json', import.meta.url);

function assertPackageManifest(value: unknown): asserts value is PackageManifest {
  if (!isRecord(value) || typeof value.name !== 'string' || !isRecord(value.exports)) {
    throw new TypeError('package manifest must include name and exports');
  }
}

function getExportConditions(manifest: PackageManifest, subpath: string): ExportConditions {
  const entry = manifest.exports[subpath];

  if (!isRecord(entry)) {
    throw new TypeError(`export ${subpath} must use conditional exports`);
  }

  const conditions: ExportConditions = {};

  for (const [key, value] of Object.entries(entry)) {
    if (typeof value !== 'string') {
      throw new TypeError(`export ${subpath} condition ${key} must be a string`);
    }

    conditions[key] = value;
  }

  return conditions;
}

function pickTarget(conditions: ExportConditions, subpath: string, keys: string[]): string {
  for (const key of keys) {
    const value = conditions[key];
    if (value !== undefined) {
      return value;
    }
  }

  throw new TypeError(`export ${subpath} must declare one of: ${keys.join(', ')}`);
}

async function readManifest(): Promise<PackageManifest> {
  const manifest = await Bun.file(packageUrl).json();
  assertPackageManifest(manifest);
  return manifest;
}

async function importDeclaredModule(manifest: PackageManifest, subpath: string): Promise<Record<string, unknown>> {
  const conditions = getExportConditions(manifest, subpath);
  const target = pickTarget(conditions, subpath, ['import', 'default', 'development']);
  return import(new URL(target, packageUrl).href);
}

describe('package export contract', () => {
  it('declares the published root export', async () => {
    const manifest = await readManifest();
    const conditions = getExportConditions(manifest, '.');

    expect(manifest.name).toBe('@smoothbricks/duration');
    expect(manifest.exports['./package.json']).toBe('./package.json');
    expect(conditions.development).toBe('./src/index.ts');
    expect(conditions.types).toBe('./dist/index.d.ts');
    expect(conditions.import).toBe('./dist/index.js');
    expect(conditions.default).toBe('./dist/index.js');
    expect(await Bun.file(new URL(pickTarget(conditions, '.', ['types']), packageUrl)).exists()).toBe(true);
    expect(await Bun.file(new URL(pickTarget(conditions, '.', ['import', 'default']), packageUrl)).exists()).toBe(true);
  });

  it('loads the published root target', async () => {
    const manifest = await readManifest();
    const mod = await importDeclaredModule(manifest, '.');

    expect(typeof mod.addDuration).toBe('function');
    expect(typeof mod.parseDuration).toBe('function');
  });
});
