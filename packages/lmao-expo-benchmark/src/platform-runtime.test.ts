import { describe, expect, it } from 'bun:test';
import { isWasmSpanBufferInstance } from '@smoothbricks/lmao/wasm';

import {
  createScenarioTracer,
  executeScenario,
  generateCanonicalSemanticSnapshot,
} from '../../lmao/benchmarks/plugin-scenario/scenario.js';
import { createPlatformRuntime as createNativeRuntime } from './platform-runtime.native.js';
import { createPlatformRuntime as createWebRuntime, runPlatformSuperblockBenchmark } from './platform-runtime.web.js';

describe('Expo platform runtime', () => {
  it('uses JavaScript buffers on native', async () => {
    const runtime = createNativeRuntime();
    expect(runtime.backend).toBe('js');

    const tracer = createScenarioTracer(runtime);
    const result = await executeScenario(tracer);
    expect(result.success).toBe(true);

    const rootBuffer = tracer.rootBuffers[0];
    if (rootBuffer === undefined) throw new Error('Native scenario did not capture a root buffer');
    expect(isWasmSpanBufferInstance(rootBuffer)).toBe(false);
    tracer.clear();
  });

  it('uses WASM buffers on web', async () => {
    const runtime = await createWebRuntime();
    expect(runtime.backend).toBe('wasm');
    const nativeCanonical = generateCanonicalSemanticSnapshot(createNativeRuntime());
    const wasmCanonical = generateCanonicalSemanticSnapshot(runtime);
    expect(wasmCanonical).toBe(nativeCanonical);

    const tracer = createScenarioTracer(runtime);
    const result = await executeScenario(tracer);
    expect(result.success).toBe(true);

    const rootBuffer = tracer.rootBuffers[0];
    if (rootBuffer === undefined) throw new Error('Web scenario did not capture a root buffer');
    expect(isWasmSpanBufferInstance(rootBuffer)).toBe(true);
    tracer.clear();

    const allocation = runPlatformSuperblockBenchmark(runtime);
    expect(allocation.capacity).toBe(64);
    expect(allocation.root.legacy.samplesNsPerOp).toHaveLength(allocation.sampleCount);
    expect(allocation.root.packed.samplesNsPerOp).toHaveLength(allocation.sampleCount);
    expect(allocation.overflow.legacy.samplesNsPerOp).toHaveLength(allocation.sampleCount);
    expect(allocation.overflow.packed.samplesNsPerOp).toHaveLength(allocation.sampleCount);
    expect(allocation.root.legacy.samplesNsPerOp.every((sample) => sample > 0)).toBe(true);
    expect(allocation.root.packed.samplesNsPerOp.every((sample) => sample > 0)).toBe(true);
    expect(allocation.overflow.legacy.samplesNsPerOp.every((sample) => sample > 0)).toBe(true);
    expect(allocation.overflow.packed.samplesNsPerOp.every((sample) => sample > 0)).toBe(true);
  });
});
