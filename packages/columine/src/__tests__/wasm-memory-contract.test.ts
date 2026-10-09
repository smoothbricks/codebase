import { describe, expect, it } from 'bun:test';

import { WASM_MAX_PAGES, WASM_PAGE_BYTES, WasmHostRegion, WasmMemoryContractError } from '../wasm-memory-contract.js';

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
