import { describe, expect, it } from 'bun:test';
import { getExportConditions, importDeclaredModule, readPackageManifest } from '@smoothbricks/validation/testing';

const packageUrl = new URL('../../package.json', import.meta.url);

describe('package export contract', () => {
  it('declares the source-backed root export', async () => {
    const manifest = await readPackageManifest(packageUrl);
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
    const manifest = await readPackageManifest(packageUrl);
    const mod = await importDeclaredModule(manifest, packageUrl, '.');

    expect(typeof mod.createQueryEngine).toBe('function');
    expect(typeof mod.createArchiveSource).toBe('function');
    expect(typeof mod.createStreamSource).toBe('function');
  });
});
