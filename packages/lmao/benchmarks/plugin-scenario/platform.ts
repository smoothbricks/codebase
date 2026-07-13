import { convertSpanTreeToArrowTable } from '@smoothbricks/lmao';
import {
  createScenarioTracer,
  executeScenario,
  generateCanonicalSemanticSnapshot,
  resetScenarioBufferStats,
  type ScenarioTraceRootFactory,
} from './scenario';

const PLATFORM_RESULT_SCHEMA_VERSION = 1;
const PLATFORM_WARMUP_ITERATIONS = 256;
const PLATFORM_SAMPLE_COUNT = 30;
const PLATFORM_ITERATIONS_PER_SAMPLE = 2_048;
const NANOSECONDS_PER_MILLISECOND = 1_000_000;

export interface PlatformScenarioOptions {
  now: () => number;
  createTraceRoot: ScenarioTraceRootFactory;
  platform: string;
  engine: string;
  variant: string;
  getDynamicFunctionCallCount?: () => number;
  onProgress?: (phase: string) => void;
}

export interface PlatformScenarioResult {
  schemaVersion: 1;
  platform: string;
  engine: string;
  variant: string;
  unit: 'ns/op';
  warmupIterations: number;
  sampleCount: number;
  iterationsPerSample: number;
  semanticJson: string;
  dynamicFunctionCalls: number | null;
  lifecycleNsPerOp: number[];
  lifecycleArrowNsPerOp: number[];
}

type ScenarioTracer = ReturnType<typeof createScenarioTracer>;
type ScenarioResult = Awaited<ReturnType<typeof executeScenario>>;

function warmLifecycle(tracer: ScenarioTracer): void {
  let lastResult: ScenarioResult | undefined;
  for (let iteration = 0; iteration < PLATFORM_WARMUP_ITERATIONS; iteration++) {
    tracer.clear();
    lastResult = executeScenario(tracer);
  }
  if (lastResult === undefined) {
    throw new Error('Lifecycle warmup executed no iterations');
  }
  tracer.clear();
}

function warmLifecycleArrow(tracer: ScenarioTracer): void {
  let lastResult: ScenarioResult | undefined;
  let lastRowCount = 0;
  for (let iteration = 0; iteration < PLATFORM_WARMUP_ITERATIONS; iteration++) {
    tracer.clear();
    lastResult = executeScenario(tracer);
    const rootBuffer = tracer.rootBuffers[0];
    if (!rootBuffer) {
      throw new Error('Arrow warmup did not capture a root buffer');
    }
    lastRowCount = convertSpanTreeToArrowTable(rootBuffer).numRows;
  }
  if (lastResult === undefined || lastRowCount === 0) {
    throw new Error('Arrow warmup executed no observable work');
  }
  tracer.clear();
}

function sampleLifecycle(tracer: ScenarioTracer, now: () => number, samples: number[]): void {
  let lastResult: ScenarioResult | undefined;
  for (let sample = 0; sample < PLATFORM_SAMPLE_COUNT; sample++) {
    const startedAt = now();
    for (let iteration = 0; iteration < PLATFORM_ITERATIONS_PER_SAMPLE; iteration++) {
      tracer.clear();
      lastResult = executeScenario(tracer);
    }
    const elapsedMilliseconds = now() - startedAt;
    if (!Number.isFinite(elapsedMilliseconds) || elapsedMilliseconds < 0) {
      throw new Error(`Lifecycle clock returned an invalid elapsed duration: ${String(elapsedMilliseconds)}`);
    }
    samples[sample] = (elapsedMilliseconds * NANOSECONDS_PER_MILLISECOND) / PLATFORM_ITERATIONS_PER_SAMPLE;
  }
  if (lastResult === undefined) {
    throw new Error('Lifecycle sampler executed no iterations');
  }
  tracer.clear();
}

function sampleLifecycleArrow(tracer: ScenarioTracer, now: () => number, samples: number[]): void {
  let lastResult: ScenarioResult | undefined;
  let lastRowCount = 0;
  for (let sample = 0; sample < PLATFORM_SAMPLE_COUNT; sample++) {
    const startedAt = now();
    for (let iteration = 0; iteration < PLATFORM_ITERATIONS_PER_SAMPLE; iteration++) {
      tracer.clear();
      lastResult = executeScenario(tracer);
      const rootBuffer = tracer.rootBuffers[0];
      if (!rootBuffer) {
        throw new Error('Arrow sampler did not capture a root buffer');
      }
      lastRowCount = convertSpanTreeToArrowTable(rootBuffer).numRows;
    }
    const elapsedMilliseconds = now() - startedAt;
    if (!Number.isFinite(elapsedMilliseconds) || elapsedMilliseconds < 0) {
      throw new Error(`Arrow clock returned an invalid elapsed duration: ${String(elapsedMilliseconds)}`);
    }
    samples[sample] = (elapsedMilliseconds * NANOSECONDS_PER_MILLISECOND) / PLATFORM_ITERATIONS_PER_SAMPLE;
  }
  if (lastResult === undefined || lastRowCount === 0) {
    throw new Error('Arrow sampler executed no observable work');
  }
  tracer.clear();
}

export function runPlatformScenario(options: PlatformScenarioOptions): PlatformScenarioResult {
  const dynamicFunctionCallsBefore = options.getDynamicFunctionCallCount?.() ?? null;
  options.onProgress?.('semantic-start');
  const semanticJson = generateCanonicalSemanticSnapshot(options.createTraceRoot);
  options.onProgress?.('semantic-end');

  const lifecycleTracer = createScenarioTracer(options.createTraceRoot);
  const arrowTracer = createScenarioTracer(options.createTraceRoot);
  resetScenarioBufferStats();

  options.onProgress?.('warmup-start');
  warmLifecycle(lifecycleTracer);
  warmLifecycleArrow(arrowTracer);
  options.onProgress?.('warmup-end');

  const lifecycleNsPerOp = new Array<number>(PLATFORM_SAMPLE_COUNT);
  const lifecycleArrowNsPerOp = new Array<number>(PLATFORM_SAMPLE_COUNT);
  options.onProgress?.('samples-start');
  sampleLifecycle(lifecycleTracer, options.now, lifecycleNsPerOp);
  sampleLifecycleArrow(arrowTracer, options.now, lifecycleArrowNsPerOp);
  options.onProgress?.('samples-end');

  const dynamicFunctionCallsAfter = options.getDynamicFunctionCallCount?.() ?? null;
  const dynamicFunctionCalls =
    dynamicFunctionCallsBefore === null || dynamicFunctionCallsAfter === null
      ? null
      : dynamicFunctionCallsAfter - dynamicFunctionCallsBefore;

  return {
    schemaVersion: PLATFORM_RESULT_SCHEMA_VERSION,
    platform: options.platform,
    engine: options.engine,
    variant: options.variant,
    unit: 'ns/op',
    warmupIterations: PLATFORM_WARMUP_ITERATIONS,
    sampleCount: PLATFORM_SAMPLE_COUNT,
    iterationsPerSample: PLATFORM_ITERATIONS_PER_SAMPLE,
    semanticJson,
    dynamicFunctionCalls,
    lifecycleNsPerOp,
    lifecycleArrowNsPerOp,
  };
}
