export {
  bindThreadSpanBuffer,
  isThreadSpanBufferWasmExports,
  THREAD_ATTRIBUTE_KINDS,
  THREAD_SPAN_BUFFER_OK,
  type ThreadAttributeKind,
  type ThreadSpanBufferBinding,
  type ThreadSpanBufferHandle,
  type ThreadSpanBufferWasmExports,
} from './lib/wasm/threadSpanBuffer.js';
// Thread-lane buffer discriminator: platform runtimes assert which lane a
// captured buffer came from without reaching into lib internals.
export { isThreadSpanView } from './lib/wasm/threadSpanView.js';
export {
  createWasmAllocator,
  createWasmAllocatorSync,
  WASM_NO_LAYOUT_OFFSET,
  WASM_SPAN_IDENTITY_CHILD,
  WASM_SPAN_IDENTITY_ROOT,
  type WasmAllocator,
  type WasmAllocatorOptions,
} from './lib/wasm/wasmAllocator.js';
