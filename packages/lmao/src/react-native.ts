/**
 * React Native entry point for @smoothbricks/lmao/react-native
 *
 * React Native runs the pure-TypedArray JS-heap lane: the same `traceRoot.es.ts`
 * factory `./es` selects, over `performance.now()`. React Native installs
 * `global.performance.now` in `InitializeCore`, backed by the native monotonic
 * clock (`NativePerformance`, or `global.nativePerformanceNow` on the legacy
 * path), so the ES lane's microsecond stamps behave as designed.
 *
 * The lane is chosen by which entry point the app imports, not by probing the
 * host — this module names the ES TraceRoot at load and never re-decides.
 *
 * What this lane deliberately does NOT get:
 *
 * - **No WASM core.** Hermes exposes no `WebAssembly` global, so the wasm
 *   allocator and the wasm-backed thread span buffer cannot run there at all.
 *   Spans go to `JsBufferStrategy`. Those modules are still *reachable* from
 *   `./index.js` through `ThreadBufferStrategy`, and `wasmAllocator.ts` loads
 *   the artifact through `await import('node:fs/promises' | 'node:url' |
 *   'node:path')` behind a `process.versions.node` guard. The guard is never
 *   true on Hermes, but Metro resolves dynamic imports at bundle time and has
 *   no `node:` protocol handling, so a React Native app maps those three
 *   specifiers to an empty module via `resolver.resolveRequest`. Nothing on
 *   this lane evaluates them at runtime.
 * - **No `node:crypto`, no Bun preloads.** Neither module system exists on
 *   Hermes; `./node` and `./bun/preload` are not importable here.
 *
 * Host globals the app must provide before importing this module:
 *
 * - **`crypto.getRandomValues` — REQUIRED, no fallback.** Hermes declined to
 *   implement it (facebook/hermes#915: "crypto is not a part of the JS spec ...
 *   this should be added to React Native"), and React Native core does not ship
 *   it either. `traceId.ts` binds `crypto.getRandomValues` at module load, so an
 *   app without a provider fails loudly at import. That failure is the design:
 *   the discarded fallback produced `Math.random` trace ids, silently trading
 *   away 128 bits of entropy on the identity every stored trace and every
 *   external correlation keys on. Install a real provider —
 *   `react-native-get-random-values`, `expo-standard-web-crypto`, or
 *   `react-native-quick-crypto` — and import it before this entry point.
 * - **`TextEncoder` — native since React Native 0.74.** Hermes gained it in
 *   facebook/hermes@3863a36, first shipped in the Hermes build tagged
 *   `hermes-2024-02-20-RNv0.74.0`. The ES TraceRoot constructs one at module
 *   load to encode trace ids, so React Native below 0.74 needs a polyfill.
 * - **`TextDecoder` — never native on Hermes; polyfill required.** Message
 *   resolution, the vocabulary registry and the Arrow vocabulary dictionary each
 *   construct a UTF-8 `TextDecoder` at module load, so it must exist before this
 *   module is imported on any Hermes version. A UTF-8-only implementation
 *   (`@bacons/text-decoder`, `react-native-fast-encoder`) is sufficient; nothing
 *   here decodes a legacy encoding.
 */

// Re-export all main functionality
export * from './index.js';

// Export the ES TraceRoot factory for Tracer construction: React Native has
// performance.now() and no WASM, which is exactly the ES lane's contract.
//#region smoo/lmao!n/trace-root-timestamps.entry-points #react-native
export { createTraceRoot } from './lib/traceRoot.es.js';
//#endregion smoo/lmao!n/trace-root-timestamps.entry-points
