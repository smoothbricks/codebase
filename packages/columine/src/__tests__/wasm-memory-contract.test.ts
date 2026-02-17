import { describe, expect, it } from 'bun:test';

import {
  calculateRequiredWasmBytes,
  ensureWasmMemoryForWorkingSet,
  WASM_MAX_BYTES,
  WASM_PAGE_BYTES,
  WasmMemoryContractError,
} from '../wasm-memory-contract.js';

describe('wasm-memory-contract', () => {
  it('grows from small initial pages when working set requires it', () => {
    const memory = new WebAssembly.Memory({ initial: 2, maximum: 1024 });

    const result = ensureWasmMemoryForWorkingSet(memory, {
      inputBytes: 6 * 1024 * 1024,
      outputBytes: 2 * 1024 * 1024,
      workspaceBytes: 2 * 1024 * 1024,
      regionsBytes: 1 * 1024 * 1024,
    });

    expect(result.grownPages).toBeGreaterThan(0);
    expect(result.totalPages).toBe(result.requiredPages);
  });

  it('succeeds at or below 64MB cap', () => {
    const memory = new WebAssembly.Memory({ initial: 4, maximum: 1024 });
    const targetBytes = WASM_MAX_BYTES - WASM_PAGE_BYTES;

    const result = ensureWasmMemoryForWorkingSet(memory, {
      inputBytes: targetBytes - 2 * WASM_PAGE_BYTES,
      outputBytes: WASM_PAGE_BYTES,
      workspaceBytes: WASM_PAGE_BYTES,
      regionsBytes: 0,
    });

    expect(result.requiredBytes).toBe(targetBytes);
    expect(result.requiredPages).toBeLessThanOrEqual(1024);
  });

  it('fails above cap with structured error', () => {
    const memory = new WebAssembly.Memory({ initial: 4, maximum: 1024 });

    expect(() =>
      ensureWasmMemoryForWorkingSet(memory, {
        inputBytes: WASM_MAX_BYTES,
        outputBytes: WASM_PAGE_BYTES,
        workspaceBytes: 0,
        regionsBytes: 0,
      }),
    ).toThrow(WasmMemoryContractError);

    try {
      ensureWasmMemoryForWorkingSet(memory, {
        inputBytes: WASM_MAX_BYTES,
        outputBytes: WASM_PAGE_BYTES,
        workspaceBytes: 0,
        regionsBytes: 0,
      });
      throw new Error('expected cap error');
    } catch (error) {
      expect(error).toBeInstanceOf(WasmMemoryContractError);
      const contractError = error as WasmMemoryContractError;
      expect(contractError.code).toBe('WASM_MEMORY_CAP_EXCEEDED');
      expect(contractError.message).toContain('cap');
    }
  });

  it('required-bytes includes output and workspace, not only input', () => {
    const requiredBytes = calculateRequiredWasmBytes({
      inputBytes: 1024,
      outputBytes: 2048,
      workspaceBytes: 4096,
      regionsBytes: 512,
    });

    expect(requiredBytes).toBe(1024 + 2048 + 4096 + 512);
    expect(requiredBytes).toBeGreaterThan(1024);
  });
});
