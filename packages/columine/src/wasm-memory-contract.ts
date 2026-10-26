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

// The VM crate's tracked source entry. `files` publishes dist/ and never crates/, so a package that holds it is a
// checkout and one that lacks it is installed, or a snapshot of an installed one.
const SOURCE_ENTRY = 'crates/columine-wasm/src/lib.rs';

export function wasmPackageRoot(moduleUrl: string): URL {
  // Source modules sit in src/ and built ones in dist/ts/.
  return new URL(moduleUrl.includes('/dist/ts/') ? '../../' : '../', moduleUrl);
}

/** The build of one wasm artifact a package runs, and the Nx target that writes it where a checkout builds it. */
export interface WasmArtifact {
  readonly url: URL;
  /** `columine:cargo-wasm` in a checkout; `null` for an installed package, which ships its build. */
  readonly producer: string | null;
}

/**
 * Which build a package loads for `fileName`, by source authority
 * (packages/nx-plugin/README.md, "Cargo profiles"). A package holding its crate's tracked source entry is a checkout:
 * it loads its own dev build, .cache/wasm-dev/<fileName>, even through its compiled dist/ts, because those are the
 * bytes its sources and tests describe. Any other package loads the shipped build, dist/<fileName>. Neither stands in
 * for the other: whichever is selected and absent is an error. The I/O shell supplies source presence.
 */
export function selectWasmArtifact(root: URL, fileName: string, holdsSource: boolean): WasmArtifact {
  return holdsSource
    ? { url: new URL(`.cache/wasm-dev/${fileName}`, root), producer: 'columine:cargo-wasm' }
    : { url: new URL(`dist/${fileName}`, root), producer: null };
}

/** Only an absent source entry selects the shipping profile; other HTTP failures remain failures. */
export function sourceEntryStatus(status: number): 'present' | 'absent' | 'error' {
  if (status >= 200 && status < 300) return 'present';
  return status === 404 ? 'absent' : 'error';
}

async function fetchBytes(location: string | URL): Promise<BufferSource> {
  const address = typeof location === 'string' && URL.canParse(location) ? new URL(location) : location;
  if (
    typeof process !== 'undefined' &&
    process.versions?.node &&
    (typeof address === 'string' || address.protocol === 'file:')
  ) {
    const { readFile } = await import('node:fs/promises');
    return await readFile(address);
  }
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: the package's own wasm artifact at a caller-given or module-relative URL: a local file read under Bun, or a static asset of the page's own origin in a browser, loaded the way its scripts are; a hung page load ends with the page
  const response = await fetch(address);
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return await response.arrayBuffer();
}

/** The filesystem or served package tree, never the JavaScript output directory, answers source authority. */
async function holdsTrackedSource(entry: URL): Promise<boolean> {
  try {
    if (entry.protocol === 'file:') {
      const { access } = await import('node:fs/promises');
      await access(entry);
      return true;
    }
    // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a HEAD request for this module's own source entry on the page's origin; a hung page load ends with the page
    const response = await fetch(entry, { method: 'HEAD' });
    const status = sourceEntryStatus(response.status);
    if (status === 'error') throw new Error(`HTTP ${response.status}`);
    return status === 'present';
  } catch (cause) {
    if (entry.protocol === 'file:' && cause instanceof Error && Reflect.get(cause, 'code') === 'ENOENT') return false;
    throw new Error(`Could not check source authority at ${entry.href}.`, { cause });
  }
}

/** Load a wasm artifact from an explicit path, or from the build this package runs (see selectWasmArtifact). */
export async function loadWasmBytes(customPath: string | URL | undefined, fileName: string): Promise<BufferSource> {
  if (customPath) {
    return await fetchBytes(customPath).catch((cause: unknown) => {
      throw new Error(`Could not load ${fileName} from ${String(customPath)}.`, { cause });
    });
  }
  const root = wasmPackageRoot(import.meta.url);
  const holdsSource = await holdsTrackedSource(new URL(SOURCE_ENTRY, root));
  const artifact = selectWasmArtifact(root, fileName, holdsSource);
  const remedy =
    artifact.producer === null
      ? ' Reinstall @smoothbricks/columine, whose dist/ ships it.'
      : ` Run "nx run ${artifact.producer}".`;
  return await fetchBytes(artifact.url).catch((cause: unknown) => {
    throw new Error(`Could not load ${fileName} from ${artifact.url.href}.${remedy}`, { cause });
  });
}
