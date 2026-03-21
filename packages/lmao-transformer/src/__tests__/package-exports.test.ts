import { describe, expect, it } from 'bun:test';
import {
  getExportConditions,
  importDeclaredModule,
  pickTarget,
  readPackageManifest,
} from '@smoothbricks/validation/testing';

const packageUrl = new URL('../../package.json', import.meta.url);

describe('package export contract', () => {
  it('declares emitted root targets', async () => {
    const manifest = await readPackageManifest(packageUrl);
    const rootConditions = getExportConditions(manifest, '.');

    expect(manifest.name).toBe('@smoothbricks/lmao-transformer');
    expect(manifest.exports['./package.json']).toBe('./package.json');
    expect(await Bun.file(new URL(pickTarget(rootConditions, '.', ['types']), packageUrl)).exists()).toBe(true);
    expect(await Bun.file(new URL(pickTarget(rootConditions, '.', ['import']), packageUrl)).exists()).toBe(true);
  });

  it('loads the declared root module', async () => {
    const manifest = await readPackageManifest(packageUrl);
    const mod = await importDeclaredModule(manifest, packageUrl, '.', ['import']);

    expect(typeof mod.createLmaoTransformer).toBe('function');
    expect(typeof mod.tryTransformTagChain).toBe('function');
    expect(typeof mod.findTagChainRoot).toBe('function');
  });
});
