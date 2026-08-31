import { ThreadBufferStrategy } from '@smoothbricks/lmao';
import { createTraceRoot } from '@smoothbricks/lmao/es';
import { createWasmAllocator, createWasmAllocatorSync, type WasmAllocator } from '@smoothbricks/lmao/wasm';

import type { ScenarioRuntime, ScenarioSchema } from '../../lmao/benchmarks/plugin-scenario/scenario';
import { runSuperblockBenchmark, type SuperblockBenchmarkResult } from './superblock-benchmark';

const WASM_CAPACITY = 32;

/**
 * The web lane is the thread-buffer lane: `ThreadBufferStrategy` writes the
 * shared per-thread native row store through allocator.wasm, and the trace
 * root is the plain ES one — the JS clock owns thread-lane stamps and crosses
 * as an argument (specs/lmao/05_span_writer_lanes.md §5). The retired per-span
 * WASM lane's `createWasmTraceRootFactory`/`WasmBufferStrategy` pairing is
 * exactly what §4 of that spec removes from the public surface.
 */
async function loadBrowserModule(): Promise<WebAssembly.Module | undefined> {
  if (typeof document !== 'object') return undefined;
  const response = await fetch(new URL('allocator.wasm', document.baseURI));
  if (!response.ok) {
    // invariant throw: the Expo web artifact must ship its allocator beside index.html.
    throw new Error(`Unable to load allocator.wasm: HTTP ${response.status}`);
  }
  return WebAssembly.compile(await response.arrayBuffer());
}

/**
 * The superblock benchmark measures the WASM-core allocator itself, so it
 * keeps a real `WasmAllocator` — the thread runtime is a separate
 * instantiation of the same module and never exposes one. Held module-locally
 * rather than on `ScenarioRuntime`, which stays lane-neutral.
 */
let benchmarkAllocator: WasmAllocator | undefined;

export async function createPlatformRuntime(): Promise<ScenarioRuntime> {
  const module = await loadBrowserModule();
  benchmarkAllocator = module
    ? createWasmAllocatorSync(module, { capacity: WASM_CAPACITY })
    : await createWasmAllocator({ capacity: WASM_CAPACITY });
  const bufferStrategy = await ThreadBufferStrategy.create<ScenarioSchema>({ module });
  return {
    backend: 'wasm',
    bufferStrategy,
    createTraceRoot,
  };
}

export function runPlatformSuperblockBenchmark(_runtime: ScenarioRuntime): SuperblockBenchmarkResult {
  if (!benchmarkAllocator) {
    // invariant throw: the benchmark only runs against a created platform runtime.
    throw new Error('Web allocation benchmark requires createPlatformRuntime() first');
  }
  return runSuperblockBenchmark(benchmarkAllocator, () => performance.now());
}
