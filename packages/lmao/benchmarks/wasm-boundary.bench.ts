/**
 * Benchmark: JS↔WASM boundary-crossing cost, isolated.
 *
 * Motivation (lmao-rs investigation): lmao's hot path stayed in TS because
 * TypedArray writes beat NAPI, and WASM's warm-path win was only 1.06-1.44x.
 * Before porting anything to Rust-WASM, measure the FIXED cost of crossing the
 * boundary at all, using the checked-in Zig allocator.wasm's raw exports
 * (bypassing the TS wrapper, whose camelCase binding is currently stale):
 *
 * 1. near-empty export call        -> pure call overhead (get_bump_ptr)
 * 2. per-value write via export    -> write_col_f64(offset, idx, value)
 * 3. per-value write via JS view   -> Float64Array over the SAME wasm memory
 * 4. per-value write via plain JS  -> Float64Array over a JS ArrayBuffer
 * 5. bulk: 64 values via one JS loop into wasm memory vs 64 export calls
 * 6. strings: encodeInto JS->wasm memory bulk vs per-string Map dictionary op
 *
 * Run: bun run benchmarks/wasm-boundary.bench.ts
 */

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { bench, boxplot, group, run, summary } from 'mitata';

const __dirname = dirname(fileURLToPath(import.meta.url));

// =============================================================================
// Setup: instantiate allocator.wasm with raw exports
// =============================================================================

const wasmBytes = readFileSync(join(__dirname, '../dist/allocator.wasm'));
const memory = new WebAssembly.Memory({ initial: 17, maximum: 256 });
const { instance } = await WebAssembly.instantiate(wasmBytes, {
  env: {
    memory,
    performanceNow: () => performance.now(),
    dateNow: () => Date.now(),
  },
});
const ex = instance.exports as Record<string, CallableFunction> & {
  memory?: WebAssembly.Memory;
};

(ex.init as () => void)();
(ex.set_thread_id as (hi: number, lo: number) => void)(0x1234, 0x5678);

// Allocate one 8-byte column block (capacity 64) to write into.
const colOffset = (ex.alloc_col_8b as (cap: number) => number)(64);
if (colOffset === 0) throw new Error('alloc_col_8b failed');

// JS view over the SAME wasm linear memory (recreated if memory grows).
let wasmF64 = new Float64Array(memory.buffer);
const wasmValueBase = colOffset; // bitmap + padding handled inside wasm; for the
// JS-view variant we just write at a fixed valid region: reuse the block start.
// (We are measuring store cost, not layout fidelity.)

// Plain JS baseline
const jsF64 = new Float64Array(64);

// =============================================================================
// 1-4: single-value write cost
// =============================================================================

let i = 0;

boxplot(() => {
  group('Single f64 write: boundary vs view vs plain JS', () => {
    bench('near-empty wasm call (get_bump_ptr)', () => {
      return (ex.get_bump_ptr as () => number)();
    });

    bench('wasm export write_col_f64', () => {
      (ex.write_col_f64 as (off: number, idx: number, v: number) => void)(
        colOffset,
        i & 63,
        42.5,
      );
      i++;
    });

    bench('JS view into wasm memory', () => {
      if (wasmF64.buffer !== memory.buffer) wasmF64 = new Float64Array(memory.buffer);
      wasmF64[(wasmValueBase >> 3) + (i & 63)] = 42.5;
      i++;
    });

    bench('plain JS Float64Array', () => {
      jsF64[i & 63] = 42.5;
      i++;
    });
  });
});

// =============================================================================
// 5: bulk crossing — 64 values
// =============================================================================

summary(() => {
  group('64 f64 writes: one boundary crossing vs 64', () => {
    bench('64 wasm export calls', () => {
      for (let k = 0; k < 64; k++) {
        (ex.write_col_f64 as (off: number, idx: number, v: number) => void)(colOffset, k, k);
      }
    });

    bench('64 JS-view writes (0 crossings)', () => {
      if (wasmF64.buffer !== memory.buffer) wasmF64 = new Float64Array(memory.buffer);
      const base = wasmValueBase >> 3;
      for (let k = 0; k < 64; k++) wasmF64[base + k] = k;
    });

    bench('64 plain JS writes', () => {
      for (let k = 0; k < 64; k++) jsF64[k] = k;
    });
  });
});

// =============================================================================
// 6: string flush strategies (approach d)
// =============================================================================

const strings: string[] = [];
for (let k = 0; k < 256; k++) strings.push(`user-${k % 37}-request-${k % 11}`);
const encoder = new TextEncoder();
const stringArea = new Uint8Array(memory.buffer, 8 * 65536, 65536);

summary(() => {
  group('256-string flush: bulk encode into wasm vs JS Map dictionary', () => {
    bench('bulk encodeInto wasm memory + offsets', () => {
      let off = 0;
      const offsets = new Uint32Array(257);
      for (let k = 0; k < 256; k++) {
        const { written } = encoder.encodeInto(strings[k]!, stringArea.subarray(off));
        off += written;
        offsets[k + 1] = off;
      }
      return off;
    });

    bench('JS Map dictionary (count + dedupe)', () => {
      const dict = new Map<string, number>();
      for (let k = 0; k < 256; k++) {
        const s = strings[k]!;
        const c = dict.get(s);
        dict.set(s, c === undefined ? 1 : c + 1);
      }
      return dict.size;
    });

    bench('JS Map dedupe THEN bulk encode unique', () => {
      const dict = new Map<string, number>();
      for (let k = 0; k < 256; k++) {
        const s = strings[k]!;
        const c = dict.get(s);
        dict.set(s, c === undefined ? 1 : c + 1);
      }
      let off = 0;
      for (const s of dict.keys()) {
        const { written } = encoder.encodeInto(s, stringArea.subarray(off));
        off += written;
      }
      return off;
    });
  });
});

// =============================================================================
// span_start via wasm export vs JS-equivalent typed stores
// =============================================================================

const spanSysOffset = (ex.alloc_span_system as (cap: number) => number)(64);
const jsTimestamps = new BigInt64Array(64);
const jsEntryTypes = new Uint8Array(64);
let anchor = BigInt(Date.now()) * 1_000_000n;
let perfAnchor = performance.now();

summary(() => {
  group('span_start: wasm export vs JS typed stores', () => {
    bench('wasm span_start export', () => {
      return (ex.span_start as (off: number, root: number) => number)(spanSysOffset, 0);
    });

    bench('JS: 2 typed stores + timestamp calc', () => {
      const ts = anchor + BigInt(Math.round((performance.now() - perfAnchor) * 1e6));
      jsTimestamps[0] = ts;
      jsEntryTypes[0] = 1;
      return ts;
    });
  });
});

console.log('WASM boundary-cost benchmark (raw allocator.wasm exports)\n');
await run({ colors: false });
