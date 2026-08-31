export {
  bindThreadSpanBuffer,
  isThreadSpanBufferWasmExports,
  THREAD_SPAN_BUFFER_OK,
  type ThreadSpanBufferBinding,
  type ThreadSpanBufferHandle,
  type ThreadSpanBufferWasmExports,
} from './lib/wasm/threadSpanBuffer.js';
export { WasmBufferStrategy, type WasmBufferStrategyOptions } from './lib/wasm/WasmBufferStrategy.js';
export {
  createWasmAllocator,
  createWasmAllocatorSync,
  WASM_NO_LAYOUT_OFFSET,
  WASM_SPAN_IDENTITY_CHILD,
  WASM_SPAN_IDENTITY_ROOT,
  type WasmAllocator,
  type WasmAllocatorOptions,
} from './lib/wasm/wasmAllocator.js';
export {
  isWasmSpanBufferInstance,
  type WasmSpanBufferInstance,
} from './lib/wasm/wasmSpanBuffer.js';
export {
  createWasmTraceRoot,
  createWasmTraceRootFactory,
  WasmTraceRoot,
} from './lib/wasm/wasmTraceRoot.js';
