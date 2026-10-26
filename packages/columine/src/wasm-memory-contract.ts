export const WASM_PAGE_BYTES = 64 * 1024;
export const WASM_MAX_PAGES = 4096;
export const WASM_MAX_BYTES = WASM_PAGE_BYTES * WASM_MAX_PAGES;

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
    `WASM working set requires ${requiredBytes} bytes but configured cap is ${maxBytes} bytes`,
  );
}

export function calculateRequiredWasmPages(requiredBytes: number): number {
  if (!Number.isFinite(requiredBytes) || requiredBytes < 0 || !Number.isInteger(requiredBytes)) {
    throw createInvalidWasmWorkingSetError('requiredBytes must be a non-negative integer byte count');
  }
  return Math.ceil(requiredBytes / WASM_PAGE_BYTES);
}

/**
 * Host-owned pages above the module's own memory, never a fixed address inside
 * its shadow stack, static data or heap. The region starts at the memory's byte
 * length when it is constructed, which must precede the module's first native
 * call that can allocate: the linker sizes initial memory for stack and data,
 * and native allocation only ever appends pages past it.
 *
 * Native allocation may append pages between calls. A region that no longer
 * ends at the memory's end therefore moves to fresh pages there instead of
 * growing over them; a move doubles while the cap allows, so the pages it
 * strands stay below its final size. Contents are not carried across a move:
 * a caller reserves its complete working set first, then derives every pointer
 * from the returned offset and stages its bytes.
 */
export class WasmHostRegion {
  private start: number;
  private owned = 0;

  constructor(
    private readonly memory: WebAssembly.Memory,
    private readonly maxPages: number = WASM_MAX_PAGES,
  ) {
    if (!Number.isInteger(maxPages) || maxPages < 1 || maxPages > WASM_MAX_PAGES) {
      throw createInvalidWasmMemoryConfigError(`maxPages must be an integer page count in [1, ${WASM_MAX_PAGES}]`);
    }
    this.start = memory.buffer.byteLength;
  }

  /** First owned byte; stable until a `reserve` that must move. */
  get offset(): number {
    return this.start;
  }

  /**
   * Own at least `requiredBytes` from the returned offset. Owned pages are
   * reused while they suffice; otherwise the region enlarges in place only
   * while it still ends at the memory's end. A cap refusal throws
   * `WASM_MEMORY_CAP_EXCEEDED` and changes neither the region nor the memory.
   */
  reserve(requiredBytes: number): number {
    if (!Number.isSafeInteger(requiredBytes) || requiredBytes < 0) {
      throw createInvalidWasmWorkingSetError('requiredBytes must be a non-negative integer byte count');
    }
    if (requiredBytes <= this.owned) return this.start;
    const currentBytes = this.memory.buffer.byteLength;
    const moves = currentBytes > this.start + this.owned;
    const start = moves ? currentBytes : this.start;
    const requiredEnd = start + requiredBytes;
    const requiredPages = calculateRequiredWasmPages(requiredEnd);
    if (requiredPages > this.maxPages) {
      throw createWasmMemoryCapExceededError(requiredEnd, this.maxPages * WASM_PAGE_BYTES);
    }
    const doubledPages = moves ? calculateRequiredWasmPages(start + 2 * this.owned) : 0;
    const targetPages = doubledPages > requiredPages && doubledPages <= this.maxPages ? doubledPages : requiredPages;
    const currentPages = currentBytes / WASM_PAGE_BYTES;
    if (targetPages > currentPages) this.memory.grow(targetPages - currentPages);
    this.start = start;
    this.owned = this.memory.buffer.byteLength - start;
    return start;
  }
}

export function align8(value: number, label = 'aligned byte count'): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`${label} must be a non-negative safe integer`);
  }
  const remainder = value % 8;
  if (remainder === 0) {
    return value;
  }
  const result = value + (8 - remainder);
  if (!Number.isSafeInteger(result)) {
    throw new RangeError(`${label} exceeds JavaScript safe-integer arithmetic`);
  }
  return result;
}

/** Load a wasm artifact from an explicit path or the two default dist locations. */
export async function loadWasmBytes(
  customPath: string | URL | undefined,
  defaultFileName: string,
): Promise<ArrayBuffer | undefined> {
  if (customPath) {
    try {
      // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: the package's own wasm artifact at a caller-given path: a local file read under Bun and Node, or a static asset of the page's own origin in a browser, loaded the way its scripts are; a hung page load ends with the page
      const response = await fetch(customPath);
      if (response.ok) {
        return await response.arrayBuffer();
      }
    } catch {
      // File not found or other error
    }
    return undefined;
  }

  const defaultPaths = [
    new URL(`../${defaultFileName}`, import.meta.url),
    new URL(`../dist/${defaultFileName}`, import.meta.url),
  ];

  for (const path of defaultPaths) {
    try {
      // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: the package's own wasm artifact beside this module: a local file read under Bun and Node, or a static asset of the page's own origin in a browser, loaded the way its scripts are; a hung page load ends with the page
      const response = await fetch(path);
      if (response.ok) {
        return await response.arrayBuffer();
      }
    } catch {
      // Try next
    }
  }

  return undefined;
}
