/**
 * TypeScript wrapper for the WASM allocator.
 *
 * Implements spec 01q: WASM Memory Architecture for SpanBuffer Storage
 *
 * The allocator provides O(1) alloc/free via freelists with memory reuse.
 * Each OpContext may have a different capacity - capacity is passed to each call.
 *
 * Buddy Allocation:
 * - Blocks are organized by tier (capacity 8, 16, 32, 64, 128, 256, 512)
 * - When freelist is empty, splits from larger tier
 * - When freeing, merges with adjacent blocks if both are free (address-based buddy merge)
 *
 * Spec link (88): realizes specs/lmao/01q_wasm_memory_architecture.md#smoo/lmao!n/wasm-mem (TS allocator wrapper).
 */

//#region smoo/lmao!n/wasm-mem.ts-allocator

// =============================================================================
// Types
// =============================================================================

export interface WasmAllocator {
  /** The underlying WASM memory */
  readonly memory: WebAssembly.Memory;
  /** Monotonic version of the current linear-memory ArrayBuffer. */
  readonly memoryVersion: number;
  /** Refresh canonical allocator views after growth and return memoryVersion. */
  refreshViews(): number;



  /** Cached views (recreated after grow, but pre-sized to avoid grow for benchmarks) */
  readonly u8: Uint8Array;
  readonly u32: Uint32Array;
  readonly i64: BigInt64Array;
  readonly f64: Float64Array;

  /** Default capacity for convenience methods */
  readonly capacity: number;

  /** Initialize allocator header (call once) */
  init(): void;

  /** Reset all freelists (for testing/benchmarking) */
  reset(): void;

  /** Allocate and release an exact logical byte extent with explicit alignment. */
  allocExact(byteLength: number, alignment: number): number;
  freeExact(offset: number, byteLength: number, alignment: number): void;


  // Span lifecycle (writes timestamp + entry_type)
  spanStart(systemPtr: number, identityPtr: number, traceRootPtr: number, capacity?: number): void;
  spanEndOk(systemPtr: number, traceRootPtr: number, capacity?: number): void;
  spanEndErr(systemPtr: number, traceRootPtr: number, capacity?: number): void;
  /** Write log entry, bump writeIndex, return idx written to */
  writeLogEntry(
    systemPtr: number,
    identityPtr: number,
    traceRootPtr: number,
    entryType: number,
    capacity?: number,
  ): number;


  // TraceRoot initialization
  initTraceRoot(traceRootPtr: number): void;

  // Debug/introspection
  getBumpPtr(): number;
  getAllocCount(): number;
  getFreeCount(): number;

  // Reading (for Arrow conversion)
  readTimestamp(systemPtr: number, rowIdx: number): bigint;
  readEntryType(systemPtr: number, rowIdx: number, capacity?: number): number;
  readWriteIndex(identityPtr: number): number;
  readColF64(colOffset: number, rowIdx: number, capacity?: number): number;
  readColIsValid(colOffset: number, rowIdx: number): number;

  // Thread ID management
  setThreadId(high: number, low: number): void;
  getThreadIdHigh(): number;
  getThreadIdLow(): number;
  isThreadIdSet(): number;
  getSpanIdCounter(): number;

  // Identity block operations (thread_id is global in header, use getThreadIdHigh/Low)
  allocIdentityRootForJsWrite(traceIdLen: number): bigint;
  allocIdentityChild(): number;
  freeIdentity(offset: number): void;
  readIdentitySpanId(identityPtr: number): number;
  readIdentityTraceIdLen(identityPtr: number): number;
  getIdentityTraceIdPtr(identityPtr: number): number;

  // Freelist statistics (O(1) - cascading stats stored in HEAD block)
  getFreelistLen(sizeClass: SizeClass, capacity?: number): number;
  getFreelistReuseCount(sizeClass: SizeClass, capacity?: number): number;
  getFreelistSplitCount(sizeClass: SizeClass, capacity?: number): number;
  getFreelistMergeCount(sizeClass: SizeClass, capacity?: number): number;
}

/**
 * Size class enum matching Zig's SizeClass
 */
export enum SizeClass {
  SpanSystem = 0,
  Col1B = 1,
  Col4B = 2,
  Col8B = 3,
  Identity = 4,
}

export interface WasmAllocatorOptions {
  /** Initial memory pages (64KB each). Values below the module minimum are clamped. */
  initialPages?: number;
  /** Maximum memory pages. Values below the effective initial page count are clamped. */
  maxPages?: number;
  /** Default capacity (rows per span buffer). Default: 64 */
  capacity?: number;
}

// =============================================================================
// WASM Exports Interface (matches Zig export names)
// =============================================================================

interface WasmExports {
  init(): void;
  reset(): void;
  alloc_exact(byteLength: number, alignment: number): number;
  free_exact(offset: number, byteLength: number, alignment: number): void;



  span_start(systemPtr: number, identityPtr: number, traceRootPtr: number, capacity: number): void;
  span_end_ok(systemPtr: number, traceRootPtr: number, capacity: number): void;
  span_end_err(systemPtr: number, traceRootPtr: number, capacity: number): void;
  write_log_entry(
    systemPtr: number,
    identityPtr: number,
    traceRootPtr: number,
    entryType: number,
    capacity: number,
  ): number;

  write_col_f64(colOffset: number, rowIdx: number, value: number, capacity: number): number;
  write_col_u32(colOffset: number, rowIdx: number, value: number, capacity: number): number;
  write_col_u8(colOffset: number, rowIdx: number, value: number, capacity: number): number;

  init_trace_root(traceRootPtr: number): void;

  get_bump_ptr(): number;
  get_alloc_count(): number;
  get_free_count(): number;

  read_timestamp(systemPtr: number, rowIdx: number): bigint;
  read_entry_type(systemPtr: number, rowIdx: number, capacity: number): number;
  read_write_index(identityPtr: number): number;
  read_col_f64(colOffset: number, rowIdx: number, capacity: number): number;
  read_col_is_valid(colOffset: number, rowIdx: number): number;

  // Thread ID management
  set_thread_id(high: number, low: number): void;
  get_thread_id_high(): number;
  get_thread_id_low(): number;
  is_thread_id_set(): number;
  get_span_id_counter(): number;

  // Identity block operations (thread_id is global, use get_thread_id_high/low)
  alloc_identity_root_for_js_write(traceIdLen: number): bigint;
  alloc_identity_child(): number;
  free_identity(offset: number): void;
  read_identity_span_id(identityPtr: number): number;
  read_identity_trace_id_len(identityPtr: number): number;
  get_identity_trace_id_ptr(identityPtr: number): number;

  // Freelist statistics (O(1) - cascading stats stored in HEAD block)
  get_freelist_len(sizeClass: number, capacity: number): number;
  get_freelist_reuse_count(sizeClass: number, capacity: number): number;
  get_freelist_split_count(sizeClass: number, capacity: number): number;
  get_freelist_merge_count(sizeClass: number, capacity: number): number;
}

function isWasmExports(value: unknown): value is WasmExports {
  if (typeof value !== 'object' || value === null) {
    return false;
  }

  return typeof Reflect.get(value, 'init') === 'function' && typeof Reflect.get(value, 'alloc_exact') === 'function';
}

// =============================================================================
// Implementation
// =============================================================================

const MIN_INITIAL_PAGES = 17; // ~1MB - matches WASM module minimum
const DEFAULT_MAX_PAGES = 16384; // 1GB max (can grow up to this)
const DEFAULT_CAPACITY = 64;

/**
 * Create typed views over WASM memory.
 * Must be called after memory creation and after any grow().
 */
function createViews(memory: WebAssembly.Memory) {
  return {
    u8: new Uint8Array(memory.buffer),
    u32: new Uint32Array(memory.buffer),
    i64: new BigInt64Array(memory.buffer),
    f64: new Float64Array(memory.buffer),
  };
}

/**
 * Create WasmAllocator from instantiated WASM module.
 */
function wrapWasmInstance(instance: WebAssembly.Instance, memory: WebAssembly.Memory, capacity: number): WasmAllocator {
  if (!isWasmExports(instance.exports)) {
    throw new Error('allocator.wasm exports did not match the expected ABI');
  }
  const exports = instance.exports;
  let views = createViews(memory);
  let currentBuffer = memory.buffer;
  let memoryVersion = 1;

  const refreshViews = (): number => {
    if (memory.buffer !== currentBuffer) {
      currentBuffer = memory.buffer;
      views = createViews(memory);
      memoryVersion++;
    }
    return memoryVersion;
  };


  // Initialize the allocator header
  exports.init();

  return {
    memory,
    capacity,
    get memoryVersion() {
      return refreshViews();
    },
    refreshViews,

    get u8() {
      refreshViews();
      return views.u8;
    },
    get u32() {
      refreshViews();
      return views.u32;
    },
    get i64() {
      refreshViews();
      return views.i64;
    },
    get f64() {
      refreshViews();
      return views.f64;
    },

    init: exports.init,
    reset: exports.reset,
    allocExact: exports.alloc_exact,
    freeExact: exports.free_exact,


    // Span lifecycle with optional capacity
    spanStart: (systemPtr, identityPtr, traceRootPtr, cap = capacity) =>
      exports.span_start(systemPtr, identityPtr, traceRootPtr, cap),
    spanEndOk: (systemPtr, traceRootPtr, cap = capacity) => exports.span_end_ok(systemPtr, traceRootPtr, cap),
    spanEndErr: (systemPtr, traceRootPtr, cap = capacity) => exports.span_end_err(systemPtr, traceRootPtr, cap),
    writeLogEntry: (systemPtr, identityPtr, traceRootPtr, entryType, cap = capacity) =>
      exports.write_log_entry(systemPtr, identityPtr, traceRootPtr, entryType, cap),


    initTraceRoot: exports.init_trace_root,

    getBumpPtr: exports.get_bump_ptr,
    getAllocCount: exports.get_alloc_count,
    getFreeCount: exports.get_free_count,

    readTimestamp: exports.read_timestamp,
    readEntryType: (systemPtr, rowIdx, cap = capacity) => exports.read_entry_type(systemPtr, rowIdx, cap),
    readWriteIndex: exports.read_write_index,
    readColF64: (colOffset, rowIdx, cap = capacity) => exports.read_col_f64(colOffset, rowIdx, cap),
    readColIsValid: exports.read_col_is_valid,

    // Thread ID management
    setThreadId: exports.set_thread_id,
    getThreadIdHigh: exports.get_thread_id_high,
    getThreadIdLow: exports.get_thread_id_low,
    isThreadIdSet: exports.is_thread_id_set,
    getSpanIdCounter: exports.get_span_id_counter,

    // Identity block operations (thread_id is global, use getThreadIdHigh/Low)
    allocIdentityRootForJsWrite: exports.alloc_identity_root_for_js_write,
    allocIdentityChild: exports.alloc_identity_child,
    freeIdentity: exports.free_identity,
    readIdentitySpanId: exports.read_identity_span_id,
    readIdentityTraceIdLen: exports.read_identity_trace_id_len,
    getIdentityTraceIdPtr: exports.get_identity_trace_id_ptr,

    // Freelist statistics with optional capacity
    getFreelistLen: (sizeClass, cap = capacity) => exports.get_freelist_len(sizeClass, cap),
    getFreelistReuseCount: (sizeClass, cap = capacity) => exports.get_freelist_reuse_count(sizeClass, cap),
    getFreelistSplitCount: (sizeClass, cap = capacity) => exports.get_freelist_split_count(sizeClass, cap),
    getFreelistMergeCount: (sizeClass, cap = capacity) => exports.get_freelist_merge_count(sizeClass, cap),
  };
}

/**
 * Create imports object for WASM instantiation.
 */
function createImports(memory: WebAssembly.Memory) {
  return {
    env: {
      memory,
      performanceNow: () => performance.now(),
      dateNow: () => Date.now(),
    },
  };
}

// Cache for compiled WASM module (shared across all allocators)
// This eliminates recompilation cost in benchmarks and production
let cachedWasmModule: WebAssembly.Module | null = null;

/**
 * Allocator artifact filename. Both implementations expose the identical ABI:
 * - `allocator.wasm` — Rust (`packages/lmao-rs/crates/lmao-wasm`), the default
 *   and the artifact shipped in the npm package. It also fixes a latent Zig
 *   freelist bug (FreeBlock bookkeeping overrunning sub-20-byte col_1b blocks).
 * - `allocator-zig.wasm` — Zig (`src/lib/wasm/allocator.zig`), reference build,
 *   opt-in via LMAO_WASM_ALLOCATOR=zig (Node/Bun only; browsers get the
 *   default). Built locally with `bun run build:zig-wasm`; not shipped.
 * `LMAO_WASM_ALLOCATOR=rs` is accepted as an alias of the default.
 */
function wasmArtifactName(): string {
  if (typeof process !== 'undefined' && process.env?.LMAO_WASM_ALLOCATOR === 'zig') {
    return 'allocator-zig.wasm';
  }
  return 'allocator.wasm';
}

/**
 * Load WASM bytes - handles both Node.js and browser environments.
 */
async function loadWasmBytes(): Promise<ArrayBuffer> {
  // Try Node.js first
  if (typeof process !== 'undefined' && process.versions?.node) {
    const { readFile } = await import('node:fs/promises');
    const { fileURLToPath } = await import('node:url');
    const { dirname, join } = await import('node:path');

    // __dirname equivalent for ESM
    const currentFile = fileURLToPath(import.meta.url);
    const currentDir = dirname(currentFile);
    const wasmPath = join(currentDir, '../../../dist', wasmArtifactName());

    const buffer = await readFile(wasmPath);
    // Create a proper ArrayBuffer from the Node.js Buffer
    const arrayBuffer = new ArrayBuffer(buffer.byteLength);
    new Uint8Array(arrayBuffer).set(buffer);
    return arrayBuffer;
  }

  // Browser: fetch from relative path
  const response = await fetch(new URL(`../../../dist/${wasmArtifactName()}`, import.meta.url));
  return response.arrayBuffer();
}

/**
 * Get compiled WASM module (cached after first load).
 */
async function getWasmModule(): Promise<WebAssembly.Module> {
  if (cachedWasmModule) {
    return cachedWasmModule;
  }

  const wasmBytes = await loadWasmBytes();
  cachedWasmModule = await WebAssembly.compile(wasmBytes);
  return cachedWasmModule;
}

/**
 * Create a WASM allocator instance.
 *
 * The allocator uses freelist for O(1) alloc/free with memory reuse.
 * Buddy allocation enables efficient memory management across different capacities.
 */
export async function createWasmAllocator(options?: WasmAllocatorOptions): Promise<WasmAllocator> {
  const initialPages = Math.max(options?.initialPages ?? MIN_INITIAL_PAGES, MIN_INITIAL_PAGES);
  const maxPages = Math.max(options?.maxPages ?? DEFAULT_MAX_PAGES, initialPages);
  const capacity = options?.capacity ?? DEFAULT_CAPACITY;

  // Create memory
  const memory = new WebAssembly.Memory({
    initial: initialPages,
    maximum: maxPages,
  });

  // Get cached compiled module (only compiles once)
  const module = await getWasmModule();

  // Instantiate with our memory
  const imports = createImports(memory);
  const instance = await WebAssembly.instantiate(module, imports);

  return wrapWasmInstance(instance, memory, capacity);
}

/**
 * Synchronous version for when WASM is pre-loaded.
 */
export function createWasmAllocatorSync(wasmModule: WebAssembly.Module, options?: WasmAllocatorOptions): WasmAllocator {
  const initialPages = Math.max(options?.initialPages ?? MIN_INITIAL_PAGES, MIN_INITIAL_PAGES);
  const maxPages = Math.max(options?.maxPages ?? DEFAULT_MAX_PAGES, initialPages);
  const capacity = options?.capacity ?? DEFAULT_CAPACITY;

  // Create memory
  const memory = new WebAssembly.Memory({
    initial: initialPages,
    maximum: maxPages,
  });

  // Instantiate with our memory
  const imports = createImports(memory);
  const instance = new WebAssembly.Instance(wasmModule, imports);

  return wrapWasmInstance(instance, memory, capacity);
}
//#endregion smoo/lmao!n/wasm-mem.ts-allocator
