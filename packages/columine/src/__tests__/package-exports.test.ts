import { describe, expect, it } from 'bun:test';
import {
  getExportConditions,
  importDeclaredModule,
  pickTarget,
  readPackageManifest,
} from '@smoothbricks/validation/testing';

type WasmLoaders = Pick<typeof import('../index.js'), 'loadColumineWasm' | 'loadParseBackend'>;

function hasWasmLoaders(module: Record<string, unknown>): module is Record<string, unknown> & WasmLoaders {
  return typeof module.loadColumineWasm === 'function' && typeof module.loadParseBackend === 'function';
}

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
  });

  it('loads both selected dev artifacts through the declared compiled root module', async () => {
    const manifest = await readPackageManifest(packageUrl);
    const mod = await importDeclaredModule(manifest, packageUrl, '.');

    expect(typeof mod.createPipeline).toBe('function');
    expect(typeof mod.createParseCompactWasmBackend).toBe('function');
    expect('getBackend' in mod).toBe(false);
    expect(typeof mod.parseReducerProgram).toBe('function');
    if (!hasWasmLoaders(mod)) throw new Error('Published columine module lacks its Wasm loaders');
    expect((await mod.loadColumineWasm()).backend).toBe('wasm');
    const parseBackend = await mod.loadParseBackend();
    try {
      expect(parseBackend.backend).toBe('event-processor-wasm');
    } finally {
      parseBackend.dispose();
    }
  });

  // Both modules are instantiated with an empty import object, so a wasm-bindgen import
  // (chrono's `wasmbind` unified in from a host-only crate did exactly this) is a load failure for
  // every consumer, not a warning.
  it('builds wasm modules that import nothing from wasm-bindgen', async () => {
    // Only this task's two declared development outputs participate in its verdict.
    const files = ['columine.wasm', 'event_processor.wasm'].map(
      (name) => new URL(`../../.cache/wasm-dev/${name}`, import.meta.url),
    );

    for (const file of files) {
      const module = await WebAssembly.compile(await Bun.file(file).arrayBuffer());
      const bindgenImports = WebAssembly.Module.imports(module)
        .filter((entry) => entry.module.startsWith('__wbindgen'))
        .map((entry) => `${file.pathname}: ${entry.module}.${entry.name}`);
      expect(bindgenImports).toEqual([]);
    }
  });
});
