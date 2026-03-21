import { describe, expect, it } from 'bun:test';
import {
  getExportConditions,
  getImportTarget,
  getTypesTarget,
  importDeclaredModule,
  readPackageManifest,
} from '../testing/index.js';

const packageUrl = new URL('../../package.json', import.meta.url);

describe('package export contract', () => {
  it('declares the published root export', async () => {
    const manifest = await readPackageManifest(packageUrl);
    const conditions = getExportConditions(manifest, '.');
    const testingConditions = getExportConditions(manifest, './testing');

    expect(manifest.name).toBe('@smoothbricks/validation');
    expect(manifest.exports['./package.json']).toBe('./package.json');
    expect(testingConditions.development).toBe('./src/testing/index.ts');
    expect(testingConditions.types).toBe('./dist/testing/index.d.ts');
    expect(testingConditions.import).toBe('./dist/testing/index.js');
    expect(testingConditions.default).toBe('./dist/testing/index.js');
    expect(conditions.development).toBe('./src/index.ts');
    expect(conditions.types).toBe('./dist/index.d.ts');
    expect(conditions.import).toBe('./dist/index.js');
    expect(conditions.default).toBe('./dist/index.js');
    expect(await Bun.file(new URL(getTypesTarget(conditions, '.'), packageUrl)).exists()).toBe(true);
    expect(await Bun.file(new URL(getImportTarget(conditions, '.'), packageUrl)).exists()).toBe(true);
    expect(await Bun.file(new URL(getTypesTarget(testingConditions, './testing'), packageUrl)).exists()).toBe(true);
    expect(await Bun.file(new URL(getImportTarget(testingConditions, './testing'), packageUrl)).exists()).toBe(true);
  });

  it('loads the published root target', async () => {
    const manifest = await readPackageManifest(packageUrl);
    const mod = await importDeclaredModule(manifest, packageUrl, '.');

    expect(typeof mod.assertDefined).toBe('function');
    expect(typeof mod.assertRecord).toBe('function');
    expect(typeof mod.isRecord).toBe('function');
    expect(typeof mod.parseJsonRecord).toBe('function');
    expect(typeof mod.safeJsonParse).toBe('function');
  });

  it('loads the published testing target', async () => {
    const manifest = await readPackageManifest(packageUrl);
    const mod = await importDeclaredModule(manifest, packageUrl, './testing');

    expect(typeof mod.assertPackageManifest).toBe('function');
    expect(typeof mod.importDeclaredModule).toBe('function');
    expect(typeof mod.pickTarget).toBe('function');
    expect(typeof mod.readPackageManifest).toBe('function');
  });
});
