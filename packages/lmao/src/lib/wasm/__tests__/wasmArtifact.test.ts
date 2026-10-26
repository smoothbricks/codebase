import { describe, expect, it } from 'bun:test';
import { selectWasmArtifact, sourceEntryStatus, wasmPackageRoot } from '../wasmAllocator.js';

const CHECKOUT = 'file:///work/packages/lmao';
const INSTALLED = 'file:///app/node_modules/@smoothbricks/lmao';

/** One package: the module the loader runs as, whether it holds its crate's source entry, and the build chosen. */
interface Scenario {
  readonly label: string;
  readonly module: string;
  readonly holdsSource: boolean;
  readonly root: string;
  readonly artifact: string;
  readonly producer: string | null;
}

const SCENARIOS: readonly Scenario[] = [
  {
    label: 'a checkout loads its dev build from source',
    module: `${CHECKOUT}/src/lib/wasm/wasmAllocator.ts`,
    holdsSource: true,
    root: `${CHECKOUT}/`,
    artifact: `${CHECKOUT}/.cache/wasm-dev/allocator.wasm`,
    producer: 'lmao:cargo-wasm',
  },
  {
    label: 'a checkout loads its dev build even through compiled dist/ts',
    module: `${CHECKOUT}/dist/ts/lib/wasm/wasmAllocator.js`,
    holdsSource: true,
    root: `${CHECKOUT}/`,
    artifact: `${CHECKOUT}/.cache/wasm-dev/allocator.wasm`,
    producer: 'lmao:cargo-wasm',
  },
  {
    label: 'an installed package loads the shipped build',
    module: `${INSTALLED}/dist/ts/lib/wasm/wasmAllocator.js`,
    holdsSource: false,
    root: `${INSTALLED}/`,
    artifact: `${INSTALLED}/dist/allocator.wasm`,
    producer: null,
  },
  {
    label: 'an installed package that publishes src/ still loads the shipped build',
    module: `${INSTALLED}/src/lib/wasm/wasmAllocator.ts`,
    holdsSource: false,
    root: `${INSTALLED}/`,
    artifact: `${INSTALLED}/dist/allocator.wasm`,
    producer: null,
  },
  {
    label: 'a sealed snapshot at a checkout-shaped path loads the shipping build',
    module: `${CHECKOUT}/dist/ts/lib/wasm/wasmAllocator.js`,
    holdsSource: false,
    root: `${CHECKOUT}/`,
    artifact: `${CHECKOUT}/dist/allocator.wasm`,
    producer: null,
  },
];

describe('sourceEntryStatus', () => {
  it('distinguishes source presence, absence, and failures without artifact I/O', () => {
    for (const status of [200, 204, 299]) expect(sourceEntryStatus(status)).toBe('present');
    expect(sourceEntryStatus(404)).toBe('absent');
    for (const status of [0, 301, 403, 405, 500]) expect(sourceEntryStatus(status)).toBe('error');
  });
});

describe('selectWasmArtifact', () => {
  it('selects the dev or shipping build solely from package root and source presence', () => {
    for (const scenario of SCENARIOS) {
      const root = wasmPackageRoot(scenario.module);
      const chosen = selectWasmArtifact(root, scenario.holdsSource);
      expect({ label: scenario.label, root: root.href, artifact: chosen.url.href, producer: chosen.producer }).toEqual({
        label: scenario.label,
        artifact: scenario.artifact,
        producer: scenario.producer,
        root: scenario.root,
      });
    }
  });
});
