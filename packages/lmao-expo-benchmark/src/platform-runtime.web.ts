import {
  createWasmAllocator,
  createWasmAllocatorSync,
  createWasmTraceRootFactory,
  type WasmAllocator,
  WasmBufferStrategy,
} from '@smoothbricks/lmao/wasm';

import type { ScenarioRuntime, ScenarioSchema } from '../../lmao/benchmarks/plugin-scenario/scenario';
import { runSuperblockBenchmark, type SuperblockBenchmarkResult } from './superblock-benchmark';

const WASM_CAPACITY = 32;

async function createBrowserAllocator(): Promise<WasmAllocator> {
  if (typeof document !== 'object') {
    return createWasmAllocator({ capacity: WASM_CAPACITY });
  }

  const response = await fetch(new URL('allocator.wasm', document.baseURI));
  if (!response.ok) {
    // invariant throw: the Expo web artifact must ship its allocator beside index.html.
    throw new Error(`Unable to load allocator.wasm: HTTP ${response.status}`);
  }
  const wasmModule = await WebAssembly.compile(await response.arrayBuffer());
  return createWasmAllocatorSync(wasmModule, { capacity: WASM_CAPACITY });
}

export async function createPlatformRuntime(): Promise<ScenarioRuntime> {
  const allocator = await createBrowserAllocator();
  const bufferStrategy = await WasmBufferStrategy.create<ScenarioSchema>({ allocator });
  return {
    backend: 'wasm',
    bufferStrategy,
    createTraceRoot: createWasmTraceRootFactory(bufferStrategy.allocator),
  };
}

export function runPlatformSuperblockBenchmark(runtime: ScenarioRuntime): SuperblockBenchmarkResult {
  if (!(runtime.bufferStrategy instanceof WasmBufferStrategy)) {
    throw new Error('Web allocation benchmark requires WasmBufferStrategy');
  }
  return runSuperblockBenchmark(runtime.bufferStrategy.allocator, () => performance.now());
}
