import { createTraceRoot } from '@smoothbricks/lmao/es';
import type { BenchmarkMode } from '../../lmao/benchmarks/plugin-scenario/platform';
import { runPlatformScenario } from '../../lmao/benchmarks/plugin-scenario/platform';
import type { DynamicFunctionCounter } from './dynamic-function-counter';
import { getTransformVariant } from './transform-variant';

export interface HeadlessBenchmarkHost {
  readonly engine: string;
  readonly now: () => number;
  readonly writeLine: (line: string) => void;
  readonly mode: BenchmarkMode;
  readonly moduleInitializationMs: number;
}

export function runHeadlessBenchmark(
  host: HeadlessBenchmarkHost,
  functionCounter: DynamicFunctionCounter | undefined,
): void {
  try {
    const result = runPlatformScenario({
      now: host.now,
      createTraceRoot,
      platform: 'react-native-headless',
      engine: host.engine,
      onProgress: host.writeLine,
      variant: getTransformVariant(),
      mode: host.mode,
      ...(host.mode === 'cold' ? { moduleInitializationMs: host.moduleInitializationMs } : {}),
      ...(functionCounter === undefined ? {} : { getDynamicFunctionCallCount: functionCounter.readForScenario }),
    });
    host.writeLine(`LMAO_BENCH_RESULT ${JSON.stringify(result)}`);
  } finally {
    functionCounter?.restore();
  }
}
