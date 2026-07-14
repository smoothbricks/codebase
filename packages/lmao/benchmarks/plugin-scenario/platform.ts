import { convertSpanTreeToArrowTable } from '@smoothbricks/lmao';
import {
  createScenarioTracer,
  executeScenario,
  generateCanonicalSemanticSnapshot,
  resetScenarioBufferStats,
  type ScenarioRuntime,
  type ScenarioTracer,
} from './scenario';

const PLATFORM_RESULT_SCHEMA_VERSION = 3;
const PLATFORM_WARMUP_ITERATIONS = 256;
const PLATFORM_SAMPLE_COUNT = 30;
const PLATFORM_ITERATIONS_PER_SAMPLE = 2_048;
const NANOSECONDS_PER_MILLISECOND = 1_000_000;

export type BenchmarkMode = 'cold' | 'steady' | 'diagnostic';

export interface PlatformScenarioOptions extends ScenarioRuntime {
  now: () => number;
  platform: string;
  engine: string;
  variant: string;
  mode: BenchmarkMode;
  moduleInitializationMs?: number;
  getDynamicFunctionCallCount?: () => number;
  onProgress?: (phase: string) => void;
}

interface PlatformScenarioResultBase {
  schemaVersion: 3;
  platform: string;
  engine: string;
  variant: string;
  backend: ScenarioRuntime['backend'];
  semanticJson: string;
}

export interface ColdPlatformScenarioResult extends PlatformScenarioResultBase {
  mode: 'cold';
  unit: 'ms';
  moduleInitializationMs: number;
  firstLifecycleMs: number;
  firstArrowMs: number;
}

export interface SteadyPlatformScenarioResult extends PlatformScenarioResultBase {
  mode: 'steady';
  unit: 'ns/op';
  warmupIterations: number;
  sampleCount: number;
  iterationsPerSample: number;
  lifecycleNsPerOp: number[];
  lifecycleArrowNsPerOp: number[];
}

export interface DiagnosticPlatformScenarioResult extends PlatformScenarioResultBase {
  mode: 'diagnostic';
  unit: 'count';
  dynamicFunctionCalls: number;
}

export type PlatformScenarioResult =
  | ColdPlatformScenarioResult
  | SteadyPlatformScenarioResult
  | DiagnosticPlatformScenarioResult;

function elapsedMilliseconds(now: () => number, startedAt: number, label: string): number {
  const elapsed = now() - startedAt;
  if (!Number.isFinite(elapsed) || elapsed < 0) {
    throw new Error(`${label} clock returned an invalid elapsed duration: ${String(elapsed)}`);
  }
  return elapsed;
}

function warmLifecycle(tracer: ScenarioTracer): void {
  let executed = false;
  for (let iteration = 0; iteration < PLATFORM_WARMUP_ITERATIONS; iteration++) {
    tracer.clear();
    executeScenario(tracer);
    executed = true;
  }
  if (!executed) throw new Error('Lifecycle warmup executed no iterations');
  tracer.clear();
}

function warmLifecycleArrow(tracer: ScenarioTracer): void {
  let executed = false;
  let lastRowCount = 0;
  for (let iteration = 0; iteration < PLATFORM_WARMUP_ITERATIONS; iteration++) {
    tracer.clear();
    executeScenario(tracer);
    executed = true;
    const rootBuffer = tracer.rootBuffers[0];
    if (!rootBuffer) throw new Error('Arrow warmup did not capture a root buffer');
    lastRowCount = convertSpanTreeToArrowTable(rootBuffer).numRows;
  }
  if (!executed || lastRowCount === 0) throw new Error('Arrow warmup executed no observable work');
  tracer.clear();
}

function sampleLifecycle(tracer: ScenarioTracer, now: () => number, samples: number[]): void {
  let executed = false;
  for (let sample = 0; sample < PLATFORM_SAMPLE_COUNT; sample++) {
    const startedAt = now();
    for (let iteration = 0; iteration < PLATFORM_ITERATIONS_PER_SAMPLE; iteration++) {
      tracer.clear();
      executeScenario(tracer);
      executed = true;
    }
    samples[sample] =
      (elapsedMilliseconds(now, startedAt, 'Lifecycle') * NANOSECONDS_PER_MILLISECOND) / PLATFORM_ITERATIONS_PER_SAMPLE;
  }
  if (!executed) throw new Error('Lifecycle sampler executed no iterations');
  tracer.clear();
}

function sampleLifecycleArrow(tracer: ScenarioTracer, now: () => number, samples: number[]): void {
  let executed = false;
  let lastRowCount = 0;
  for (let sample = 0; sample < PLATFORM_SAMPLE_COUNT; sample++) {
    const startedAt = now();
    for (let iteration = 0; iteration < PLATFORM_ITERATIONS_PER_SAMPLE; iteration++) {
      tracer.clear();
      executeScenario(tracer);
      executed = true;
      const rootBuffer = tracer.rootBuffers[0];
      if (!rootBuffer) throw new Error('Arrow sampler did not capture a root buffer');
      lastRowCount = convertSpanTreeToArrowTable(rootBuffer).numRows;
    }
    samples[sample] =
      (elapsedMilliseconds(now, startedAt, 'Arrow') * NANOSECONDS_PER_MILLISECOND) / PLATFORM_ITERATIONS_PER_SAMPLE;
  }
  if (!executed || lastRowCount === 0) throw new Error('Arrow sampler executed no observable work');
  tracer.clear();
}

function runColdScenario(options: PlatformScenarioOptions): ColdPlatformScenarioResult {
  const moduleInitializationMs = options.moduleInitializationMs;
  if (moduleInitializationMs === undefined || !Number.isFinite(moduleInitializationMs) || moduleInitializationMs < 0) {
    throw new Error('Cold mode requires a finite, non-negative moduleInitializationMs measurement');
  }
  if (options.getDynamicFunctionCallCount !== undefined) {
    throw new Error('Cold mode must not install the dynamic Function observer');
  }

  // Tracer construction is scenario setup. `firstLifecycleMs` intentionally measures
  // the first trace invocation itself; module graph initialization is reported separately.
  resetScenarioBufferStats();
  const tracer = createScenarioTracer(options);
  options.onProgress?.('first-lifecycle-start');
  const lifecycleStartedAt = options.now();
  const firstResult = executeScenario(tracer);
  const firstLifecycleMs = elapsedMilliseconds(options.now, lifecycleStartedAt, 'First lifecycle');
  options.onProgress?.('first-lifecycle-end');
  if (!firstResult.success) throw new Error(`Cold scenario failed: ${String(firstResult.error)}`);

  const rootBuffer = tracer.rootBuffers[0];
  if (!rootBuffer) throw new Error('Cold scenario did not capture a root buffer');
  options.onProgress?.('first-arrow-start');
  const arrowStartedAt = options.now();
  const firstArrowTable = convertSpanTreeToArrowTable(rootBuffer);
  const firstArrowMs = elapsedMilliseconds(options.now, arrowStartedAt, 'First Arrow conversion');
  options.onProgress?.('first-arrow-end');
  if (firstArrowTable.numRows === 0) throw new Error('Cold Arrow conversion produced no observable rows');
  tracer.clear();

  const semanticJson = generateCanonicalSemanticSnapshot(options);
  return {
    schemaVersion: PLATFORM_RESULT_SCHEMA_VERSION,
    mode: 'cold',
    platform: options.platform,
    engine: options.engine,
    variant: options.variant,
    backend: options.backend,
    unit: 'ms',
    semanticJson,
    moduleInitializationMs,
    firstLifecycleMs,
    firstArrowMs,
  };
}

function runSteadyScenario(options: PlatformScenarioOptions): SteadyPlatformScenarioResult {
  if (options.getDynamicFunctionCallCount !== undefined) {
    throw new Error('Steady mode must not install the dynamic Function observer');
  }
  options.onProgress?.('semantic-start');
  const semanticJson = generateCanonicalSemanticSnapshot(options);
  options.onProgress?.('semantic-end');
  const lifecycleTracer = createScenarioTracer(options);
  const arrowTracer = createScenarioTracer(options);
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
  return {
    schemaVersion: PLATFORM_RESULT_SCHEMA_VERSION,
    mode: 'steady',
    platform: options.platform,
    engine: options.engine,
    variant: options.variant,
    backend: options.backend,
    unit: 'ns/op',
    warmupIterations: PLATFORM_WARMUP_ITERATIONS,
    sampleCount: PLATFORM_SAMPLE_COUNT,
    iterationsPerSample: PLATFORM_ITERATIONS_PER_SAMPLE,
    semanticJson,
    lifecycleNsPerOp,
    lifecycleArrowNsPerOp,
  };
}

function runDiagnosticScenario(options: PlatformScenarioOptions): DiagnosticPlatformScenarioResult {
  const readDynamicFunctionCallCount = options.getDynamicFunctionCallCount;
  if (readDynamicFunctionCallCount === undefined) {
    throw new Error('Diagnostic mode requires the dynamic Function observer');
  }
  const dynamicFunctionCallsBefore = readDynamicFunctionCallCount();
  options.onProgress?.('semantic-start');
  // This semantic pass exercises tracing and Arrow conversion, forcing every lazy
  // compiler reachable from the scenario before the counter is read.
  const semanticJson = generateCanonicalSemanticSnapshot(options);
  options.onProgress?.('semantic-end');
  const dynamicFunctionCalls = readDynamicFunctionCallCount() - dynamicFunctionCallsBefore;
  if (!Number.isSafeInteger(dynamicFunctionCalls) || dynamicFunctionCalls < 0) {
    throw new Error(`Dynamic Function observer returned an invalid count: ${String(dynamicFunctionCalls)}`);
  }
  return {
    schemaVersion: PLATFORM_RESULT_SCHEMA_VERSION,
    mode: 'diagnostic',
    platform: options.platform,
    engine: options.engine,
    variant: options.variant,
    backend: options.backend,
    unit: 'count',
    semanticJson,
    dynamicFunctionCalls,
  };
}

export function runPlatformScenario(options: PlatformScenarioOptions): PlatformScenarioResult {
  switch (options.mode) {
    case 'cold':
      return runColdScenario(options);
    case 'steady':
      return runSteadyScenario(options);
    case 'diagnostic':
      return runDiagnosticScenario(options);
    default:
      throw new Error(`Unsupported benchmark mode: ${String(options.mode)}`);
  }
}
