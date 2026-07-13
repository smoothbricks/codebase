import { describe, expect, it } from 'bun:test';
import {
  getExportConditions,
  importDeclaredModule,
  pickTarget,
  readPackageManifest,
} from '@smoothbricks/validation/testing';

const packageUrl = new URL('../../package.json', import.meta.url);

describe('package export contract', () => {
  it('declares Bun adapter and runtime-register targets', async () => {
    const manifest = await readPackageManifest(packageUrl);
    const rootConditions = getExportConditions(manifest, '.');
    const bunConditions = getExportConditions(manifest, './bun');
    const registerConditions = getExportConditions(manifest, './bun-register');
    const pluginConditions = getExportConditions(manifest, './ttsc-plugin');

    expect(manifest.name).toBe('@smoothbricks/lmao-ttsc');
    expect(manifest.exports['./package.json']).toBe('./package.json');
    expect(await Bun.file(new URL(pickTarget(pluginConditions, './ttsc-plugin', ['types']), packageUrl)).exists()).toBe(
      true,
    );
    expect(
      await Bun.file(new URL(pickTarget(pluginConditions, './ttsc-plugin', ['require']), packageUrl)).exists(),
    ).toBe(true);
    expect(await Bun.file(new URL(pickTarget(rootConditions, '.', ['types']), packageUrl)).exists()).toBe(true);
    expect(await Bun.file(new URL(pickTarget(rootConditions, '.', ['import']), packageUrl)).exists()).toBe(true);
    expect(await Bun.file(new URL(pickTarget(bunConditions, '.', ['types']), packageUrl)).exists()).toBe(true);
    expect(await Bun.file(new URL(pickTarget(bunConditions, '.', ['import']), packageUrl)).exists()).toBe(true);
    expect(await Bun.file(new URL(pickTarget(registerConditions, '.', ['types']), packageUrl)).exists()).toBe(true);
    expect(await Bun.file(new URL(pickTarget(registerConditions, '.', ['import']), packageUrl)).exists()).toBe(true);
  });

  it('loads the declared Bun build adapter', async () => {
    const manifest = await readPackageManifest(packageUrl);
    const mod = await importDeclaredModule(manifest, packageUrl, '.', ['import']);

    expect(typeof mod.default).toBe('function');
    expect(mod.createBunTtscPlugin).toBe(mod.default);
  });

  it('loads the declared ttsc plugin descriptor', async () => {
    const manifest = await readPackageManifest(packageUrl);
    const mod = await importDeclaredModule(manifest, packageUrl, './ttsc-plugin', ['require']);

    expect(typeof mod.default).toBe('function');
  });
});
