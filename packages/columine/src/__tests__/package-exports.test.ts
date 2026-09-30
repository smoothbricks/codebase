import { describe, expect, it } from 'bun:test';
import { fileURLToPath } from 'node:url';
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
    expect(typeof mod.createParseCompactWasmBackend).toBe('function');
    expect('getBackend' in mod).toBe(false);
    expect(typeof mod.parseReducerProgram).toBe('function');
  });

  // The shipped modules are instantiated with an empty import object, so a wasm-bindgen import
  // (chrono's `wasmbind` unified in from a host-only crate did exactly this) is a load failure for
  // every consumer, not a warning.
  it('ships wasm modules that import nothing from wasm-bindgen', async () => {
    const dist = new URL('../../dist/', import.meta.url);
    const shipped = await Array.fromAsync(new Bun.Glob('**/*.wasm').scan(fileURLToPath(dist)));
    // Every file `dist/**/*.wasm` publishes is judged; the two cargo-wasm outputs must be among them.
    expect(shipped).toEqual(expect.arrayContaining(['columine.wasm', 'event_processor.wasm']));

    for (const file of shipped) {
      const module = await WebAssembly.compile(await Bun.file(new URL(file, dist)).arrayBuffer());
      const bindgenImports = WebAssembly.Module.imports(module)
        .filter((entry) => entry.module.startsWith('__wbindgen'))
        .map((entry) => `${file}: ${entry.module}.${entry.name}`);
      expect(bindgenImports).toEqual([]);
    }
  });
});
