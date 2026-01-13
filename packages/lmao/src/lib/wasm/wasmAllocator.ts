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
 */

// =============================================================================
// Types
// =============================================================================

export interface WasmAllocator {
  /** The underlying WASM memory */
  readonly memory: WebAssembly.Memory;

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

  // Block allocation - capacity determines tier and block size
  allocSpanSystem(capacity?: number): number;
  alloc1B(capacity?: number): number;
  alloc4B(capacity?: number): number;
  alloc8B(capacity?: number): number;

  // Block deallocation - returns block to freelist, may trigger buddy merge
  freeSpanSystem(offset: number, capacity?: number): void;
  free1B(offset: number, capacity?: number): void;
  free4B(offset: number, capacity?: number): void;
  free8B(offset: number, capacity?: number): void;

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

  // Column writes (auto-allocate if offset is 0)
  writeColF64(colOffset: number, rowIdx: number, value: number, capacity?: number): number;
  writeColU32(colOffset: number, rowIdx: number, value: number, capacity?: number): number;
  writeColU8(colOffset: number, rowIdx: number, value: number, capacity?: number): number;

  // TraceRoot initialization
  initTraceRoot(traceRootPtr: number): void;

  // Debug/introspection
  getBumpPtr(): number;
  getAllocCount(): number;
  getFreeCount(): number;
  getSpanSystemSize(capacity?: number): number;
  getCol1BSize(capacity?: number): number;
  getCol4BSize(capacity?: number): number;
  getCol8BSize(capacity?: number): number;

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
  allocIdentityRoot(traceIdPtr: number, traceIdLen: number): number;
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
  /** Initial memory pages (64KB each). Default: 17 (~1MB) */
  initialPages?: number;
  /** Maximum memory pages. Default: 16384 (1GB) */
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

  alloc_span_system(capacity: number): number;
  alloc_col_1b(capacity: number): number;
  alloc_col_4b(capacity: number): number;
  alloc_col_8b(capacity: number): number;

  free_span_system(offset: number, capacity: number): void;
  free_col_1b(offset: number, capacity: number): void;
  free_col_4b(offset: number, capacity: number): void;
  free_col_8b(offset: number, capacity: number): void;

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
  get_span_system_size(capacity: number): number;
  get_col_1b_size(capacity: number): number;
  get_col_4b_size(capacity: number): number;
  get_col_8b_size(capacity: number): number;

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
  alloc_identity_root(traceIdPtr: number, traceIdLen: number): number;
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

// =============================================================================
// Implementation
// =============================================================================

const DEFAULT_INITIAL_PAGES = 17; // ~1MB - matches WASM module minimum
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
  const exports = instance.exports as unknown as WasmExports;
  let views = createViews(memory);
  let currentBuffer = memory.buffer;

  // Initialize the allocator header
  exports.init();

  return {
    memory,
    capacity,

    get u8() {
      // Check if memory grew since last access
      if (memory.buffer !== currentBuffer) {
        currentBuffer = memory.buffer;
        views = createViews(memory);
      }
      return views.u8;
    },
    get u32() {
      if (memory.buffer !== currentBuffer) {
        currentBuffer = memory.buffer;
        views = createViews(memory);
      }
      return views.u32;
    },
    get i64() {
      if (memory.buffer !== currentBuffer) {
        currentBuffer = memory.buffer;
        views = createViews(memory);
      }
      return views.i64;
    },
    get f64() {
      if (memory.buffer !== currentBuffer) {
        currentBuffer = memory.buffer;
        views = createViews(memory);
      }
      return views.f64;
    },

    init: exports.init,
    reset: exports.reset,

    // Allocation with optional capacity (defaults to instance capacity)
    allocSpanSystem: (cap = capacity) => exports.alloc_span_system(cap),
    alloc1B: (cap = capacity) => exports.alloc_col_1b(cap),
    alloc4B: (cap = capacity) => exports.alloc_col_4b(cap),
    alloc8B: (cap = capacity) => exports.alloc_col_8b(cap),

    // Deallocation with optional capacity
    freeSpanSystem: (offset, cap = capacity) => exports.free_span_system(offset, cap),
    free1B: (offset, cap = capacity) => exports.free_col_1b(offset, cap),
    free4B: (offset, cap = capacity) => exports.free_col_4b(offset, cap),
    free8B: (offset, cap = capacity) => exports.free_col_8b(offset, cap),

    // Span lifecycle with optional capacity
    spanStart: (systemPtr, identityPtr, traceRootPtr, cap = capacity) =>
      exports.span_start(systemPtr, identityPtr, traceRootPtr, cap),
    spanEndOk: (systemPtr, traceRootPtr, cap = capacity) => exports.span_end_ok(systemPtr, traceRootPtr, cap),
    spanEndErr: (systemPtr, traceRootPtr, cap = capacity) => exports.span_end_err(systemPtr, traceRootPtr, cap),
    writeLogEntry: (systemPtr, traceRootPtr, idx, entryType, cap = capacity) =>
      exports.write_log_entry(systemPtr, traceRootPtr, idx, entryType, cap),

    // Column writes with optional capacity
    writeColF64: (colOffset, rowIdx, value, cap = capacity) => exports.write_col_f64(colOffset, rowIdx, value, cap),
    writeColU32: (colOffset, rowIdx, value, cap = capacity) => exports.write_col_u32(colOffset, rowIdx, value, cap),
    writeColU8: (colOffset, rowIdx, value, cap = capacity) => exports.write_col_u8(colOffset, rowIdx, value, cap),

    initTraceRoot: exports.init_trace_root,

    getBumpPtr: exports.get_bump_ptr,
    getAllocCount: exports.get_alloc_count,
    getFreeCount: exports.get_free_count,
    getSpanSystemSize: (cap = capacity) => exports.get_span_system_size(cap),
    getCol1BSize: (cap = capacity) => exports.get_col_1b_size(cap),
    getCol4BSize: (cap = capacity) => exports.get_col_4b_size(cap),
    getCol8BSize: (cap = capacity) => exports.get_col_8b_size(cap),

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
    allocIdentityRoot: exports.alloc_identity_root,
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
    const wasmPath = join(currentDir, '../../../dist/allocator.wasm');

    const buffer = await readFile(wasmPath);
    // Create a proper ArrayBuffer from the Node.js Buffer
    const arrayBuffer = new ArrayBuffer(buffer.byteLength);
    new Uint8Array(arrayBuffer).set(buffer);
    return arrayBuffer;
  }

  // Browser: fetch from relative path
  const response = await fetch(new URL('../../../dist/allocator.wasm', import.meta.url));
  return response.arrayBuffer();
}

/**
 * Create a WASM allocator instance.
 *
 * The allocator uses freelist for O(1) alloc/free with memory reuse.
 * Buddy allocation enables efficient memory management across different capacities.
 */
export async function createWasmAllocator(options?: WasmAllocatorOptions): Promise<WasmAllocator> {
  const initialPages = options?.initialPages ?? DEFAULT_INITIAL_PAGES;
  const maxPages = options?.maxPages ?? DEFAULT_MAX_PAGES;
  const capacity = options?.capacity ?? DEFAULT_CAPACITY;

  // Create memory
  const memory = new WebAssembly.Memory({
    initial: initialPages,
    maximum: maxPages,
  });

  // Load and compile WASM
  const wasmBytes = await loadWasmBytes();
  const module = await WebAssembly.compile(wasmBytes);

  // Instantiate with our memory
  const imports = createImports(memory);
  const instance = await WebAssembly.instantiate(module, imports);

  return wrapWasmInstance(instance, memory, capacity);
}

/**
 * Synchronous version for when WASM is pre-loaded.
 */
export function createWasmAllocatorSync(wasmModule: WebAssembly.Module, options?: WasmAllocatorOptions): WasmAllocator {
  const initialPages = options?.initialPages ?? DEFAULT_INITIAL_PAGES;
  const maxPages = options?.maxPages ?? DEFAULT_MAX_PAGES;
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
