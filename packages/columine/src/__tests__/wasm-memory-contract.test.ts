import { describe, expect, it } from 'bun:test';

import {
  selectWasmArtifact,
  sourceEntryStatus,
  WASM_MAX_PAGES,
  WASM_PAGE_BYTES,
  WasmHostRegion,
  WasmMemoryContractError,
  wasmPackageRoot,
} from '../wasm-memory-contract.js';

function contractError(run: () => unknown): WasmMemoryContractError {
  try {
    run();
  } catch (error) {
    if (error instanceof WasmMemoryContractError) return error;
    throw error;
  }
  throw new Error('expected a WasmMemoryContractError');
}

describe('wasm-memory-contract host region', () => {
  it('starts at the memory end and grows in place while it ends there', () => {
    const memory = new WebAssembly.Memory({ initial: 2, maximum: 256 });
    const region = new WasmHostRegion(memory);
    expect(region.offset).toBe(2 * WASM_PAGE_BYTES);

    const base = region.reserve(10 * 1024 * 1024 + 1);
    expect(base).toBe(2 * WASM_PAGE_BYTES);
    expect(memory.buffer.byteLength).toBe(base + Math.ceil((10 * 1024 * 1024 + 1) / WASM_PAGE_BYTES) * WASM_PAGE_BYTES);
    expect(region.reserve(1)).toBe(base);
  });

  it('refuses above its cap with a structured error and leaves memory unchanged', () => {
    const memory = new WebAssembly.Memory({ initial: 4, maximum: WASM_MAX_PAGES });
    const region = new WasmHostRegion(memory, 8);
    const error = contractError(() => region.reserve(4 * WASM_PAGE_BYTES + 1));
    expect(error.code).toBe('WASM_MEMORY_CAP_EXCEEDED');
    expect(memory.buffer.byteLength).toBe(4 * WASM_PAGE_BYTES);
    expect(region.reserve(4 * WASM_PAGE_BYTES)).toBe(4 * WASM_PAGE_BYTES);
  });

  it('refuses an invalid cap or byte count', () => {
    const memory = new WebAssembly.Memory({ initial: 1, maximum: 4 });
    expect(contractError(() => new WasmHostRegion(memory, 0)).code).toBe('INVALID_WASM_MEMORY_CONFIG');
    expect(contractError(() => new WasmHostRegion(memory, WASM_MAX_PAGES + 1)).code).toBe('INVALID_WASM_MEMORY_CONFIG');
    expect(contractError(() => new WasmHostRegion(memory).reserve(-1)).code).toBe('INVALID_WASM_WORKING_SET');
    expect(contractError(() => new WasmHostRegion(memory).reserve(1.5)).code).toBe('INVALID_WASM_WORKING_SET');
  });
});

const CHECKOUT = 'file:///work/packages/columine';
const INSTALLED = 'file:///app/node_modules/@smoothbricks/columine';

/** One package: the module the loader runs as, whether it holds its crate's source entry, and the build chosen. */
interface Scenario {
  readonly label: string;
  readonly module: string;
  readonly holdsSource: boolean;
  readonly root: string;
  readonly directory: string;
  readonly producer: string | null;
}

const SCENARIOS: readonly Scenario[] = [
  {
    label: 'a checkout loads its dev build from source',
    module: `${CHECKOUT}/src/wasm-memory-contract.ts`,
    holdsSource: true,
    root: `${CHECKOUT}/`,
    directory: `${CHECKOUT}/.cache/wasm-dev/`,
    producer: 'columine:cargo-wasm',
  },
  {
    label: 'a checkout loads its dev build even through compiled dist/ts',
    module: `${CHECKOUT}/dist/ts/wasm-memory-contract.js`,
    holdsSource: true,
    root: `${CHECKOUT}/`,
    directory: `${CHECKOUT}/.cache/wasm-dev/`,
    producer: 'columine:cargo-wasm',
  },
  {
    label: 'an installed package loads the shipped build',
    module: `${INSTALLED}/dist/ts/wasm-memory-contract.js`,
    holdsSource: false,
    root: `${INSTALLED}/`,
    directory: `${INSTALLED}/dist/`,
    producer: null,
  },
  {
    label: 'a sealed snapshot at a checkout-shaped path loads the shipping build',
    module: `${CHECKOUT}/dist/ts/wasm-memory-contract.js`,
    holdsSource: false,
    root: `${CHECKOUT}/`,
    directory: `${CHECKOUT}/dist/`,
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
  it('selects both artifacts solely from package root and source presence', () => {
    for (const scenario of SCENARIOS) {
      const root = wasmPackageRoot(scenario.module);
      expect(root.href).toBe(scenario.root);
      for (const fileName of ['columine.wasm', 'event_processor.wasm']) {
        const chosen = selectWasmArtifact(root, fileName, scenario.holdsSource);
        expect({ label: scenario.label, artifact: chosen.url.href, producer: chosen.producer }).toEqual({
          label: scenario.label,
          artifact: `${scenario.directory}${fileName}`,
          producer: scenario.producer,
        });
      }
    }
  });
});
