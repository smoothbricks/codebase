export const WASM_PAGE_BYTES = 64 * 1024;
export const WASM_MAX_PAGES = 1024;
export const WASM_MAX_BYTES = WASM_PAGE_BYTES * WASM_MAX_PAGES;

export interface WasmWorkingSetBytes {
  inputBytes: number;
  outputBytes: number;
  workspaceBytes: number;
  regionsBytes: number;
}

export interface WasmMemoryGrowthOptions {
  maxPages?: number;
}

export interface WasmMemoryGrowthResult {
  requiredBytes: number;
  requiredPages: number;
  currentPages: number;
  grownPages: number;
  totalPages: number;
}

export type WasmMemoryContractErrorCode =
  | 'INVALID_WASM_MEMORY_CONFIG'
  | 'INVALID_WASM_WORKING_SET'
  | 'WASM_MEMORY_CAP_EXCEEDED';

export class WasmMemoryContractError extends Error {
  readonly code: WasmMemoryContractErrorCode;

  constructor(code: WasmMemoryContractErrorCode, message: string) {
    super(message);
    this.name = 'WasmMemoryContractError';
    this.code = code;
  }
}

export function createInvalidWasmMemoryConfigError(message: string): WasmMemoryContractError {
  return new WasmMemoryContractError('INVALID_WASM_MEMORY_CONFIG', message);
}

export function createInvalidWasmWorkingSetError(message: string): WasmMemoryContractError {
  return new WasmMemoryContractError('INVALID_WASM_WORKING_SET', message);
}

export function createWasmMemoryCapExceededError(requiredBytes: number, maxBytes: number): WasmMemoryContractError {
  return new WasmMemoryContractError(
    'WASM_MEMORY_CAP_EXCEEDED',
    `WASM working set requires ${requiredBytes} bytes but cap is ${maxBytes} bytes`,
  );
}

function ensureFiniteByteCount(value: number, field: keyof WasmWorkingSetBytes): void {
  if (!Number.isFinite(value) || value < 0 || !Number.isInteger(value)) {
    throw createInvalidWasmWorkingSetError(`${field} must be a non-negative integer byte count`);
  }
}

export function validateWasmWorkingSetBytes(workingSet: WasmWorkingSetBytes): void {
  ensureFiniteByteCount(workingSet.inputBytes, 'inputBytes');
  ensureFiniteByteCount(workingSet.outputBytes, 'outputBytes');
  ensureFiniteByteCount(workingSet.workspaceBytes, 'workspaceBytes');
  ensureFiniteByteCount(workingSet.regionsBytes, 'regionsBytes');
}

export function calculateRequiredWasmBytes(workingSet: WasmWorkingSetBytes): number {
  validateWasmWorkingSetBytes(workingSet);
  return workingSet.inputBytes + workingSet.outputBytes + workingSet.workspaceBytes + workingSet.regionsBytes;
}

export function calculateRequiredWasmPages(requiredBytes: number): number {
  if (!Number.isFinite(requiredBytes) || requiredBytes < 0 || !Number.isInteger(requiredBytes)) {
    throw createInvalidWasmWorkingSetError('requiredBytes must be a non-negative integer byte count');
  }
  return Math.ceil(requiredBytes / WASM_PAGE_BYTES);
}

function validateWasmMemoryGrowthOptions(options: WasmMemoryGrowthOptions): number {
  const maxPages = options.maxPages ?? WASM_MAX_PAGES;
  if (!Number.isFinite(maxPages) || maxPages < 1 || !Number.isInteger(maxPages)) {
    throw createInvalidWasmMemoryConfigError('maxPages must be a positive integer page count');
  }
  return maxPages;
}

export function ensureWasmMemoryForWorkingSet(
  memory: WebAssembly.Memory,
  workingSet: WasmWorkingSetBytes,
  options: WasmMemoryGrowthOptions = {},
): WasmMemoryGrowthResult {
  const maxPages = validateWasmMemoryGrowthOptions(options);
  const requiredBytes = calculateRequiredWasmBytes(workingSet);
  const requiredPages = calculateRequiredWasmPages(requiredBytes);

  if (requiredPages > maxPages) {
    throw createWasmMemoryCapExceededError(requiredBytes, maxPages * WASM_PAGE_BYTES);
  }

  const currentPages = memory.buffer.byteLength / WASM_PAGE_BYTES;
  if (!Number.isInteger(currentPages)) {
    throw createInvalidWasmMemoryConfigError('WebAssembly memory byte length must align to page size');
  }

  let grownPages = 0;
  if (requiredPages > currentPages) {
    grownPages = requiredPages - currentPages;
    memory.grow(grownPages);
  }

  return {
    requiredBytes,
    requiredPages,
    currentPages,
    grownPages,
    totalPages: currentPages + grownPages,
  };
}
