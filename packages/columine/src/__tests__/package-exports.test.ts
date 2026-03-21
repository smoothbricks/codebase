import { describe, expect, it } from 'bun:test';
import {
  getExportConditions,
  importDeclaredModule,
  pickTarget,
  readPackageManifest,
} from '@smoothbricks/validation/testing';

const packageUrl = new URL('../../package.json', import.meta.url);

describe('package export contract', () => {
  it('declares the published root and wasm targets', async () => {
    const manifest = await readPackageManifest(packageUrl);
    const rootConditions = getExportConditions(manifest, '.');

    expect(manifest.name).toBe('@smoothbricks/columine');
    expect(manifest.exports['./package.json']).toBe('./package.json');
    expect(manifest.exports['./wasm']).toBe('./dist/columine.wasm');
    expect(await Bun.file(new URL(pickTarget(rootConditions, '.', ['types']), packageUrl)).exists()).toBe(true);
    expect(await Bun.file(new URL(pickTarget(rootConditions, '.', ['import', 'default']), packageUrl)).exists()).toBe(
      true,
    );
    expect(await Bun.file(new URL('./dist/columine.wasm', packageUrl)).exists()).toBe(true);
  });

  it('loads the declared root module', async () => {
    const manifest = await readPackageManifest(packageUrl);
    const mod = await importDeclaredModule(manifest, packageUrl, '.');

    expect(typeof mod.createPipeline).toBe('function');
    expect(typeof mod.getBackend).toBe('function');
    expect(typeof mod.parseReducerProgram).toBe('function');
  });
});
