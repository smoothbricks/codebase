import { describe, expect, it } from 'bun:test';
import { createTraceRoot } from '@smoothbricks/lmao/node';

import {
  type PlatformScenarioOptions,
  type PlatformScenarioResult,
  runPlatformScenario,
} from '../../lmao/benchmarks/plugin-scenario/platform.js';
import {
  createJsScenarioRuntime,
  type ScenarioTraceRootFactory,
} from '../../lmao/benchmarks/plugin-scenario/scenario.js';

const TEST_RUNTIME = createJsScenarioRuntime(createTraceRoot);

const COMMON_OPTIONS = {
  ...TEST_RUNTIME,
  platform: 'test-platform',
  engine: 'test-engine',
  variant: 'test-variant',
};

const COLD_KEYS = [
  'backend',
  'engine',
  'firstArrowMs',
  'firstLifecycleMs',
  'mode',
  'moduleInitializationMs',
  'platform',
  'schemaVersion',
  'semanticJson',
  'unit',
  'variant',
];

const STEADY_KEYS = [
  'backend',
  'engine',
  'iterationsPerSample',
  'lifecycleArrowNsPerOp',
  'lifecycleNsPerOp',
  'mode',
  'platform',
  'sampleCount',
  'schemaVersion',
  'semanticJson',
  'unit',
  'variant',
  'warmupIterations',
];

const DIAGNOSTIC_KEYS = [
  'backend',
  'dynamicFunctionCalls',
  'engine',
  'mode',
  'platform',
  'schemaVersion',
  'semanticJson',
  'unit',
  'variant',
];

function expectCommonResult(result: PlatformScenarioResult): void {
  expect(result.schemaVersion).toBe(3);
  expect(result.platform).toBe(COMMON_OPTIONS.platform);
  expect(result.engine).toBe(COMMON_OPTIONS.engine);
  expect(result.variant).toBe(COMMON_OPTIONS.variant);
  expect(result.backend).toBe(COMMON_OPTIONS.backend);

  const semanticResult = JSON.parse(result.semanticJson);
  expect(semanticResult.result).toBe(61);
  expect(semanticResult.rows).toHaveLength(12);
}

function createOptions(
  options: Pick<PlatformScenarioOptions, 'now' | 'mode'> &
    Partial<Pick<PlatformScenarioOptions, 'moduleInitializationMs' | 'getDynamicFunctionCallCount'>>,
): PlatformScenarioOptions {
  return {
    ...COMMON_OPTIONS,
    ...options,
  };
}

describe('runPlatformScenario', () => {
  it('reports only the first cold lifecycle and Arrow timings', () => {
    const events: string[] = [];
    const clockReadings = [10, 13, 20, 27];
    let clockReads = 0;
    const countingCreateTraceRoot: ScenarioTraceRootFactory = (traceId, tracer) => {
      events.push('trace');
      return createTraceRoot(traceId, tracer);
    };

    const result = runPlatformScenario({
      ...COMMON_OPTIONS,
      mode: 'cold',
      moduleInitializationMs: 7,
      createTraceRoot: countingCreateTraceRoot,
      now: () => {
        events.push('now');
        return clockReadings[clockReads++] ?? Number.NaN;
      },
    });

    expectCommonResult(result);
    expect(result.mode).toBe('cold');
    if (result.mode !== 'cold') {
      throw new Error('Expected a cold benchmark result');
    }

    expect(Object.keys(result).sort()).toEqual(COLD_KEYS);
    expect(result.unit).toBe('ms');
    expect(result.moduleInitializationMs).toBe(7);
    expect(result.firstLifecycleMs).toBe(3);
    expect(result.firstArrowMs).toBe(7);
    expect(clockReads).toBe(4);
    expect(events).toEqual(['now', 'trace', 'now', 'now', 'now', 'trace']);
  });

  it('preserves the steady sampling protocol without cold or diagnostic fields', () => {
    let clockReads = 0;
    const result = runPlatformScenario(
      createOptions({
        mode: 'steady',
        now: () => clockReads++,
      }),
    );

    expectCommonResult(result);
    expect(result.mode).toBe('steady');
    if (result.mode !== 'steady') {
      throw new Error('Expected a steady benchmark result');
    }

    const expectedNsPerOp = 1_000_000 / 2_048;
    expect(Object.keys(result).sort()).toEqual(STEADY_KEYS);
    expect(result.unit).toBe('ns/op');
    expect(result.warmupIterations).toBe(256);
    expect(result.sampleCount).toBe(30);
    expect(result.iterationsPerSample).toBe(2_048);
    expect(result.lifecycleNsPerOp).toEqual(Array.from({ length: 30 }, () => expectedNsPerOp));
    expect(result.lifecycleArrowNsPerOp).toEqual(Array.from({ length: 30 }, () => expectedNsPerOp));
    expect(clockReads).toBe(120);
  });

  it('reports the diagnostic counter delta without timing fields', () => {
    const counterReadings = [41, 48];
    let counterReads = 0;

    const result = runPlatformScenario(
      createOptions({
        mode: 'diagnostic',
        now: () => {
          throw new Error('Diagnostic mode must not read the benchmark clock');
        },
        getDynamicFunctionCallCount: () => counterReadings[counterReads++] ?? Number.NaN,
      }),
    );

    expectCommonResult(result);
    expect(result.mode).toBe('diagnostic');
    if (result.mode !== 'diagnostic') {
      throw new Error('Expected a diagnostic benchmark result');
    }

    expect(Object.keys(result).sort()).toEqual(DIAGNOSTIC_KEYS);
    expect(result.unit).toBe('count');
    expect(result.dynamicFunctionCalls).toBe(7);
    expect(counterReads).toBe(2);
  });

  it('rejects a cold run without a module initialization measurement before tracing', () => {
    let clockReads = 0;
    let traces = 0;
    const countingCreateTraceRoot: ScenarioTraceRootFactory = (traceId, tracer) => {
      traces++;
      return createTraceRoot(traceId, tracer);
    };

    expect(() =>
      Reflect.apply(runPlatformScenario, undefined, [
        {
          ...COMMON_OPTIONS,
          mode: 'cold',
          createTraceRoot: countingCreateTraceRoot,
          now: () => clockReads++,
        },
      ]),
    ).toThrow('Cold mode requires a finite, non-negative moduleInitializationMs measurement');
    expect(clockReads).toBe(0);
    expect(traces).toBe(0);
  });

  it('rejects a dynamic Function observer in cold mode before reading it', () => {
    let counterReads = 0;

    expect(() =>
      runPlatformScenario(
        createOptions({
          mode: 'cold',
          moduleInitializationMs: 4,
          now: () => 0,
          getDynamicFunctionCallCount: () => counterReads++,
        }),
      ),
    ).toThrow('Cold mode must not install the dynamic Function observer');
    expect(counterReads).toBe(0);
  });

  it('rejects a dynamic Function observer in steady mode before reading it', () => {
    let counterReads = 0;

    expect(() =>
      runPlatformScenario(
        createOptions({
          mode: 'steady',
          now: () => 0,
          getDynamicFunctionCallCount: () => counterReads++,
        }),
      ),
    ).toThrow('Steady mode must not install the dynamic Function observer');
    expect(counterReads).toBe(0);
  });

  it('requires the dynamic Function observer in diagnostic mode', () => {
    expect(() =>
      runPlatformScenario(
        createOptions({
          mode: 'diagnostic',
          now: () => 0,
        }),
      ),
    ).toThrow('Diagnostic mode requires the dynamic Function observer');
  });

  it('rejects an invalid runtime mode', () => {
    expect(() =>
      Reflect.apply(runPlatformScenario, undefined, [
        {
          ...COMMON_OPTIONS,
          mode: 'bogus',
          createTraceRoot,
          now: () => 0,
        },
      ]),
    ).toThrow('Unsupported benchmark mode: bogus');
  });
});
