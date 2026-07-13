/**
 * Benchmark: JS↔WASM boundary-crossing cost, isolated.
 *
 * Uses the checked-in allocator's current exact-allocation ABI so the raw-call
 * measurements stay aligned with the production WasmAllocator wrapper.
 *
 * Run: bun packages/lmao/benchmarks/wasm-boundary.bench.ts
 */

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { bench, boxplot, group, run, summary } from 'mitata';

const CAPACITY = 64;
const NULL_BYTES = Math.ceil(CAPACITY / 8);
const __dirname = dirname(fileURLToPath(import.meta.url));

const wasmBytes = readFileSync(join(__dirname, '../dist/allocator.wasm'));
const memory = new WebAssembly.Memory({ initial: 17, maximum: 256 });
const { instance } = await WebAssembly.instantiate(wasmBytes, {
  env: {
    memory,
    performanceNow: () => performance.now(),
    dateNow: () => Date.now(),
  },
});
const wasmExports = instance.exports;

type NumberWasmExport = (...args: number[]) => number;
type VoidWasmExport = (...args: number[]) => void;
type WasmExportFunction = NumberWasmExport | VoidWasmExport;

function isWasmExportFunction(value: unknown): value is WasmExportFunction {
  return typeof value === 'function';
}

function requireWasmFunction(
  name: 'init' | 'set_thread_id' | 'init_trace_root' | 'span_start',
): VoidWasmExport;
function requireWasmFunction(
  name:
    | 'alloc_exact'
    | 'get_bump_ptr'
    | 'write_col_f64'
    | 'read_col_f64'
    | 'alloc_identity_child'
    | 'read_entry_type'
    | 'read_write_index',
): NumberWasmExport;
function requireWasmFunction(name: string): WasmExportFunction {
  const value = Reflect.get(wasmExports, name);
  if (!isWasmExportFunction(value)) throw new TypeError(`allocator.wasm is missing export ${name}`);
  return value;
}

const init = requireWasmFunction('init');
const setThreadId = requireWasmFunction('set_thread_id');
const allocExact = requireWasmFunction('alloc_exact');
const getBumpPtr = requireWasmFunction('get_bump_ptr');
const writeColF64 = requireWasmFunction('write_col_f64');
const readColF64 = requireWasmFunction('read_col_f64');
const allocIdentityChild = requireWasmFunction('alloc_identity_child');
const initTraceRoot = requireWasmFunction('init_trace_root');
const spanStart = requireWasmFunction('span_start');
const readEntryType = requireWasmFunction('read_entry_type');
const readWriteIndex = requireWasmFunction('read_write_index');

init();
setThreadId(0x1234, 0x5678);

const colByteLength = NULL_BYTES + CAPACITY * Float64Array.BYTES_PER_ELEMENT;
const colOffset = allocExact(colByteLength, Float64Array.BYTES_PER_ELEMENT);
if (typeof colOffset !== 'number' || colOffset === 0) throw new Error('exact f64 column allocation failed');

let wasmF64 = new Float64Array(memory.buffer, colOffset + NULL_BYTES, CAPACITY);
const jsF64 = new Float64Array(CAPACITY);
let index = 0;

const semanticOffset = writeColF64(colOffset, 3, 42.5, CAPACITY);
const semanticValue = readColF64(colOffset, 3, CAPACITY);
if (semanticOffset !== colOffset || semanticValue !== 42.5) {
  throw new Error('allocator f64 boundary semantic check failed');
}

boxplot(() => {
  group('Single f64 write: boundary vs view vs plain JS', () => {
    bench('near-empty wasm call (get_bump_ptr)', () => getBumpPtr());

    bench('wasm export write_col_f64', () => {
      const row = index++ & (CAPACITY - 1);
      return writeColF64(colOffset, row, 42.5, CAPACITY);
    });

    bench('JS view into wasm memory', () => {
      if (wasmF64.buffer !== memory.buffer) {
        wasmF64 = new Float64Array(memory.buffer, colOffset + NULL_BYTES, CAPACITY);
      }
      const row = index++ & (CAPACITY - 1);
      wasmF64[row] = 42.5;
      return wasmF64[row];
    });

    bench('plain JS Float64Array', () => {
      const row = index++ & (CAPACITY - 1);
      jsF64[row] = 42.5;
      return jsF64[row];
    });
  });
});

summary(() => {
  group('64 f64 writes: JS loop vs 64 boundary crossings', () => {
    bench('64 wasm export calls', () => {
      let result = 0;
      for (let row = 0; row < CAPACITY; row++) result = writeColF64(colOffset, row, row, CAPACITY);
      return result;
    });

    bench('64 JS-view writes (0 crossings)', () => {
      if (wasmF64.buffer !== memory.buffer) {
        wasmF64 = new Float64Array(memory.buffer, colOffset + NULL_BYTES, CAPACITY);
      }
      for (let row = 0; row < CAPACITY; row++) wasmF64[row] = row;
      return wasmF64[CAPACITY - 1];
    });

    bench('64 plain JS writes', () => {
      for (let row = 0; row < CAPACITY; row++) jsF64[row] = row;
      return jsF64[CAPACITY - 1];
    });
  });
});

const strings = Array.from({ length: 256 }, (_, row) => `user-${row % 37}-request-${row % 11}`);
const encoder = new TextEncoder();
const stringAreaOffset = allocExact(65_536, 1);
if (typeof stringAreaOffset !== 'number' || stringAreaOffset === 0) {
  throw new Error('exact string staging allocation failed');
}
const stringArea = new Uint8Array(memory.buffer, stringAreaOffset, 65_536);

summary(() => {
  group('256-string flush: bulk encode into wasm vs JS Map dictionary', () => {
    bench('bulk encodeInto wasm memory + offsets', () => {
      let offset = 0;
      let row = 0;
      const offsets = new Uint32Array(strings.length + 1);
      for (const value of strings) {
        const { written } = encoder.encodeInto(value, stringArea.subarray(offset));
        offset += written;
        offsets[++row] = offset;
      }
      return offset ^ offsets[row];
    });

    bench('JS Map dictionary (count + dedupe)', () => {
      const dictionary = new Map<string, number>();
      for (const value of strings) {
        const count = dictionary.get(value);
        dictionary.set(value, count === undefined ? 1 : count + 1);
      }
      return dictionary.size;
    });

    bench('JS Map dedupe THEN bulk encode unique', () => {
      const dictionary = new Map<string, number>();
      for (const value of strings) {
        const count = dictionary.get(value);
        dictionary.set(value, count === undefined ? 1 : count + 1);
      }
      let offset = 0;
      for (const value of dictionary.keys()) {
        const { written } = encoder.encodeInto(value, stringArea.subarray(offset));
        offset += written;
      }
      return offset;
    });
  });
});

const spanSystemOffset = allocExact(CAPACITY * 9, 8);
const spanIdentityOffset = allocIdentityChild();
const traceRootOffset = allocExact(16, 8);
if (
  typeof spanSystemOffset !== 'number' ||
  typeof spanIdentityOffset !== 'number' ||
  typeof traceRootOffset !== 'number' ||
  spanSystemOffset === 0 ||
  spanIdentityOffset === 0 ||
  traceRootOffset === 0
) {
  throw new Error('WASM span boundary allocation failed');
}
initTraceRoot(traceRootOffset);
spanStart(spanSystemOffset, spanIdentityOffset, traceRootOffset, CAPACITY);
if (readWriteIndex(spanIdentityOffset) !== 2 || readEntryType(spanSystemOffset, 0, CAPACITY) !== 1) {
  throw new Error('WASM span boundary semantic check failed');
}

const jsTimestamps = new BigInt64Array(CAPACITY);
const jsEntryTypes = new Uint8Array(CAPACITY);
const anchor = BigInt(Date.now()) * 1_000_000n;
const perfAnchor = performance.now();

summary(() => {
  group('span_start: wasm export vs JS typed stores', () => {
    bench('wasm span_start export', () => {
      spanStart(spanSystemOffset, spanIdentityOffset, traceRootOffset, CAPACITY);
      return spanSystemOffset;
    });

    bench('JS: 2 typed stores + timestamp calc', () => {
      const timestamp = anchor + BigInt(Math.round((performance.now() - perfAnchor) * 1e6));
      jsTimestamps[0] = timestamp;
      jsEntryTypes[0] = 1;
      return timestamp;
    });
  });
});

console.log('WASM boundary-cost benchmark (current raw allocator.wasm ABI)\n');
await run({ colors: false });
