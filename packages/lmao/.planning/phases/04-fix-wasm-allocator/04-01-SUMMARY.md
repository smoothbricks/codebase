# Phase 4 Plan 1: Fix WASM Allocator Summary

**WASM allocator now handles memory growth correctly with efficient Zig-side allocation**

## Accomplishments

### 1. View Refresh Implementation (Task 1)

- Implemented automatic view refresh in WasmAllocator getters (u8, u32, i64, f64)
- Views are recreated on-demand when `memory.buffer` identity changes
- Identity check (`memory.buffer !== currentBuffer`) has minimal overhead (~1 CPU cycle)
- Eliminated "Underlying ArrayBuffer has been detached" errors

### 2. Efficient Zig Allocation (Tasks 2-3)

- Added `alloc_identity_root_for_js_write()` Zig function for direct writes
- Returns packed u64: `(identity_offset << 32) | trace_id_field_offset`
- JS writes trace_id bytes directly to identity block (1 copy instead of 3)
- Fixed type inference bug requiring explicit u32 casts for offset calculations
- Eliminated scratch buffer pattern entirely

### 3. WasmSpanBuffer Constructor Update (Task 4)

- Updated constructor to use `allocIdentityRootForJsWrite()`
- Direct write using fresh `Uint8Array(memory.buffer)` view (never detached)
- Added missing `_stats`, `_logSchema`, and `_columns` getters to match JS SpanBuffer API
- Fixed "Attempted to assign to readonly property" error by removing assignment to getter properties

### 4. Automatic Allocator Reset (Task 5)

- Updated `TestTracer.clear()` to automatically call `strategy.reset()` if available
- Eliminated need for manual reset calls in benchmarks (realistic usage pattern)
- Users don't need to know about allocator internals

### 5. WASM Module Caching (Bonus)

- Implemented cached compiled module (`cachedWasmModule`)
- Module compiles once and is reused across all allocator instances
- Reduces cold start overhead from disk I/O + compilation to just instantiation

### 6. Benchmark Results (Task 5-6)

**Warm benchmarks (realistic production usage):**

- Simple trace: WASM **1.12x faster** than JS
- Trace with tags: WASM **1.44x faster** than JS
- Nested spans: WASM **1.11x faster** than JS
- Multiple log entries: WASM **1.06x faster** than JS
- Memory reuse (100 traces): WASM **1.31x faster** than JS
- Trace with tags + nested spans: WASM **1.26x faster** than JS

**Cold start benchmarks (per-trace instantiation overhead):**

- JS 127-271x faster due to WASM instantiation cost
- Not a concern in production (strategy created once and reused)

**All 52 WASM allocator tests pass** - no regressions

## Files Created/Modified

- `packages/lmao/src/lib/wasm/wasmAllocator.ts`
  - View refresh logic in getters (lines 235-263)
  - Module caching (lines 349-391)

- `packages/lmao/src/lib/wasm/allocator.zig`
  - New `alloc_identity_root_for_js_write` function (lines 676-699)
  - Fixed type inference with explicit u32 casts

- `packages/lmao/src/lib/wasm/wasmSpanBuffer.ts`
  - Direct write pattern for trace_id (lines 245-256)
  - Added `_stats`, `_logSchema`, `_columns` getters (lines 615-625)
  - Fixed constructor to not assign to getter properties
  - Made stats object mutable via `Object.defineProperty` (lines 754-774)

- `packages/lmao/src/lib/tracers/TestTracer.ts`
  - Automatic strategy reset in `clear()` method (lines 133-136)

- `packages/lmao/benchmarks/js-vs-wasm.bench.ts`
  - Already configured for fair comparison (capacity=8 for both strategies)

## Decisions Made

1. **View refresh via identity check**: Minimal overhead, automatic recovery after memory growth
2. **Packed u64 return from Zig**: Clean API, upper 32 bits = identity offset, lower 32 bits = field offset
3. **Keep old allocIdentityRoot**: Backward compatibility maintained (though unused after this change)
4. **Direct write using fresh Uint8Array view**: Never detached, more reliable than cached views
5. **Automatic reset in TestTracer.clear()**: Realistic usage pattern, no manual allocator management
6. **Module caching**: Shared across all allocators, eliminates recompilation overhead

## Issues Encountered and Resolved

### Issue 1: View Detachment After Memory Growth

**Root Cause**: Views captured in closure never updated when `memory.buffer` changed **Solution**: Identity check in
getters to detect buffer changes and recreate views

### Issue 2: Scratch Buffer Anti-pattern

**Root Cause**: JS→scratch→identity (3 copies) due to JS-side allocation **Solution**: Zig function returns target
offset, JS writes directly (1 copy)

### Issue 3: Zig Type Inference Bug

**Root Cause**: `@offsetOf` return type caused overflow in offset calculation **Solution**: Explicit u32 casts for
intermediate variables:

```zig
const field_offset_in_struct: u32 = @offsetOf(Identity, "trace_id");
const trace_id_field_offset: u32 = offset + field_offset_in_struct;
```

### Issue 4: Readonly Property Assignment

**Root Cause**: Constructor tried to assign to getter-only properties **Solution**: Removed `this._logSchema`
assignment, use getter from `constructor.schema`

### Issue 5: Benchmark Allocator Exhaustion

**Root Cause**: Benchmark warmup runs exhausted allocator without reset **Solution**: Automatic reset in
`TestTracer.clear()`

## Architecture Improvements

1. **Separation of Concerns**: Memory allocation fully in Zig, JS handles only I/O
2. **No Manual Memory Management**: Users don't call `reset()` manually, handled by tracer lifecycle
3. **Module Caching**: Production apps benefit from cached compilation (one-time cost)
4. **Consistent API**: WasmSpanBuffer matches JS SpanBuffer getter patterns

## Performance Analysis

**Why WASM is faster in warm scenarios:**

- O(1) freelist allocation vs GC pressure
- Better cache locality from contiguous WASM memory
- No TypedArray allocation overhead per trace
- Memory reuse eliminates allocation churn

**Why cold start is slower:**

- WebAssembly.Memory creation
- Module instantiation with imports
- Allocator initialization (`init()` call)
- Not a production concern (strategy reused)

## Next Steps

Phase 4 complete! WASM benchmarks operational with fair comparison at capacity=8.

**Key Takeaway**: WASM is 1.06-1.44x faster than JS in realistic production usage (warm path). The allocator bug is
fixed, view refresh is automatic, and allocation is efficient.
