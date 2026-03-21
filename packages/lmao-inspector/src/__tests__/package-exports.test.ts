import { describe, expect, it } from 'bun:test';
import { isRecord } from '@smoothbricks/validation';

type ExportConditions = {
  development?: string;
  types?: string;
  import?: string;
  default?: string;
};

type PackageManifest = {
  name: string;
  exports: Record<string, string | ExportConditions>;
};

const packageUrl = new URL('../../package.json', import.meta.url);

function assertPackageManifest(value: unknown): asserts value is PackageManifest {
  if (!isRecord(value) || typeof value.name !== 'string' || !isRecord(value.exports)) {
    throw new TypeError('package manifest must include a name and exports map');
  }
}

function getExportConditions(manifest: PackageManifest, subpath: string): ExportConditions {
  const entry = manifest.exports[subpath];

  if (!isRecord(entry)) {
    throw new TypeError(`export ${subpath} must use conditional exports`);
  }

  const conditions: ExportConditions = {};

  for (const key of ['development', 'types', 'import', 'default'] as const) {
    const value = entry[key];
    if (value !== undefined && typeof value !== 'string') {
      throw new TypeError(`export ${subpath} condition ${key} must be a string`);
    }
    if (typeof value === 'string') {
      conditions[key] = value;
    }
  }

  return conditions;
}

function pickTarget(conditions: ExportConditions, subpath: string, keys: Array<keyof ExportConditions>): string {
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

describe('package export contract', () => {
  it('declares the source-backed root export', async () => {
    const manifest = await readManifest();
    const rootConditions = getExportConditions(manifest, '.');

    expect(manifest.name).toBe('@smoothbricks/lmao-inspector');
    expect(manifest.exports['./package.json']).toBe('./package.json');
    expect(rootConditions.development).toBe('./src/index.ts');
    expect(rootConditions.types).toBe('./src/index.ts');
    expect(rootConditions.import).toBe('./src/index.ts');
    expect(rootConditions.default).toBe('./src/index.ts');
    expect(await Bun.file(new URL('./src/index.ts', packageUrl)).exists()).toBe(true);
  });

  it('loads the declared root module', async () => {
    const manifest = await readManifest();
    const rootConditions = getExportConditions(manifest, '.');
    const mod = await import(new URL(pickTarget(rootConditions, '.', ['import', 'default']), packageUrl).href);

    expect(typeof mod.createQueryEngine).toBe('function');
    expect(typeof mod.createArchiveSource).toBe('function');
    expect(typeof mod.createStreamSource).toBe('function');
  });
});
