import { describe, expect, it } from 'bun:test';

import {
  calculateRequiredWasmBytes,
  calculateRequiredWasmPages,
  ensureWasmMemoryForWorkingSet,
  WASM_MAX_BYTES,
  WASM_PAGE_BYTES,
} from '../wasm-memory-contract.js';

describe('wasm-memory-contract', () => {
  it('sums full working set bytes (input + output + workspace + regions)', () => {
    const requiredBytes = calculateRequiredWasmBytes({
      inputBytes: 7,
      outputBytes: 11,
      workspaceBytes: 13,
      regionsBytes: 17,
    });

    expect(requiredBytes).toBe(48);
  });

  it('calculates required pages from required bytes', () => {
    expect(calculateRequiredWasmPages(0)).toBe(0);
    expect(calculateRequiredWasmPages(1)).toBe(1);
    expect(calculateRequiredWasmPages(WASM_PAGE_BYTES)).toBe(1);
    expect(calculateRequiredWasmPages(WASM_PAGE_BYTES + 1)).toBe(2);
  });

  it('grows wasm memory up to the required page count', () => {
    const memory = new WebAssembly.Memory({ initial: 1 });

    const result = ensureWasmMemoryForWorkingSet(memory, {
      inputBytes: WASM_PAGE_BYTES,
      outputBytes: WASM_PAGE_BYTES,
      workspaceBytes: WASM_PAGE_BYTES,
      regionsBytes: WASM_PAGE_BYTES,
    });

    expect(result.requiredPages).toBe(4);
    expect(result.currentPages).toBe(1);
    expect(result.grownPages).toBe(3);
    expect(result.totalPages).toBe(4);
    expect(memory.buffer.byteLength).toBe(WASM_PAGE_BYTES * 4);
  });

  it('throws when working set exceeds configured cap', () => {
    const memory = new WebAssembly.Memory({ initial: 1 });

    expect(() =>
      ensureWasmMemoryForWorkingSet(
        memory,
        {
          inputBytes: WASM_MAX_BYTES,
          outputBytes: 1,
          workspaceBytes: 0,
          regionsBytes: 0,
        },
        { maxPages: 1024 },
      ),
    ).toThrow('WASM working set requires');
  });

  it('throws for impossible config and invalid byte counts', () => {
    const memory = new WebAssembly.Memory({ initial: 1 });

    expect(() =>
      ensureWasmMemoryForWorkingSet(
        memory,
        {
          inputBytes: -1,
          outputBytes: 0,
          workspaceBytes: 0,
          regionsBytes: 0,
        },
        { maxPages: 0 },
      ),
    ).toThrow('maxPages must be a positive integer page count');

    expect(() =>
      ensureWasmMemoryForWorkingSet(memory, {
        inputBytes: -1,
        outputBytes: 0,
        workspaceBytes: 0,
        regionsBytes: 0,
      }),
    ).toThrow('inputBytes must be a non-negative integer byte count');
  });
});
