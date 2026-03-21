import { describe, expect, it } from 'bun:test';
import {
  getExportConditions,
  getImportTarget,
  getTypesTarget,
  importDeclaredModule,
  readPackageManifest,
} from '@smoothbricks/validation/testing';

const packageUrl = new URL('../../package.json', import.meta.url);

describe('package export contract', () => {
  it('declares the published root export', async () => {
    const manifest = await readPackageManifest(packageUrl);
    const conditions = getExportConditions(manifest, '.');

    expect(manifest.name).toBe('@smoothbricks/money');
    expect(manifest.exports['./package.json']).toBe('./package.json');
    expect(conditions.development).toBe('./src/index.ts');
    expect(conditions.types).toBe('./dist/index.d.ts');
    expect(conditions.import).toBe('./dist/index.js');
    expect(conditions.default).toBe('./dist/index.js');
    expect(await Bun.file(new URL(getTypesTarget(conditions, '.'), packageUrl)).exists()).toBe(true);
    expect(await Bun.file(new URL(getImportTarget(conditions, '.'), packageUrl)).exists()).toBe(true);
  });

  it('loads the published root target', async () => {
    const manifest = await readPackageManifest(packageUrl);
    const mod = await importDeclaredModule(manifest, packageUrl, '.');

    expect(typeof mod.Amount).toBe('function');
    expect(typeof mod.add).toBe('function');
    expect(typeof mod.allocateProportional).toBe('function');
    expect(typeof mod.getCurrency).toBe('function');
    expect(typeof mod.roundBasisToAmount).toBe('function');
  });
});
