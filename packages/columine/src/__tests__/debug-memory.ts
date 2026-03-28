// Quick debug script to check WASM memory layout
import { readFileSync } from 'node:fs';

type DebugVmExports = {
  vm_debug_shadow_addr: () => number;
  vm_debug_undo_entries_addr: () => number;
};

function isWasmFunction<T extends (...args: never[]) => unknown>(value: unknown): value is T {
  return typeof value === 'function';
}

function getDebugVmExports(instance: WebAssembly.Instance): DebugVmExports {
  const exports = instance.exports as {
    vm_debug_shadow_addr?: unknown;
    vm_debug_undo_entries_addr?: unknown;
  };
  if (
    !isWasmFunction<DebugVmExports['vm_debug_shadow_addr']>(exports.vm_debug_shadow_addr) ||
    !isWasmFunction<DebugVmExports['vm_debug_undo_entries_addr']>(exports.vm_debug_undo_entries_addr)
  ) {
    throw new Error('WASM module missing debug memory exports');
  }
  return {
    vm_debug_shadow_addr: exports.vm_debug_shadow_addr,
    vm_debug_undo_entries_addr: exports.vm_debug_undo_entries_addr,
  };
}

const wasmPath = new URL('../../dist/columine.wasm', import.meta.url);
const wasmBytes = readFileSync(wasmPath);
const wasmModule = await WebAssembly.compile(wasmBytes);
const instance = await WebAssembly.instantiate(wasmModule, {
  env: { memory: new WebAssembly.Memory({ initial: 96, maximum: 4096 }) },
});

const exports = getDebugVmExports(instance);

const stateRegionOffset = 64 * 1024; // 64KB, same as wasm-backend.ts

console.log('=== WASM Memory Layout Debug ===');
console.log(`stateRegionOffset: ${stateRegionOffset} (0x${stateRegionOffset.toString(16)})`);
console.log(`shadow_addr: ${exports.vm_debug_shadow_addr()} (0x${exports.vm_debug_shadow_addr().toString(16)})`);
console.log(
  `undo_entries_addr: ${exports.vm_debug_undo_entries_addr()} (0x${exports.vm_debug_undo_entries_addr().toString(16)})`,
);

const shadowEnd = exports.vm_debug_shadow_addr() + 4 * 1024 * 1024;
console.log(`shadow_end: ${shadowEnd} (0x${shadowEnd.toString(16)})`);

const undoEntriesEnd = exports.vm_debug_undo_entries_addr() + 16384 * 24; // UNDO_CAPACITY * sizeof(FlatUndoEntry)
console.log(`undo_entries_end: ${undoEntriesEnd} (0x${undoEntriesEnd.toString(16)})`);

// Check overlaps with state region (assuming state is ~524KB)
const stateEnd = stateRegionOffset + 524288;
console.log(`state_end (approx): ${stateEnd} (0x${stateEnd.toString(16)})`);

const shadowStart = exports.vm_debug_shadow_addr();
if (stateRegionOffset < shadowEnd && shadowStart < stateEnd) {
  console.log('*** OVERLAP: state region overlaps with shadow buffer! ***');
} else {
  console.log('OK: state region does NOT overlap with shadow buffer');
}

const undoStart = exports.vm_debug_undo_entries_addr();
if (stateRegionOffset < undoEntriesEnd && undoStart < stateEnd) {
  console.log('*** OVERLAP: state region overlaps with undo entries! ***');
} else {
  console.log('OK: state region does NOT overlap with undo entries');
}
