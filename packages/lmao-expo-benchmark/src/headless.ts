import { createTraceRoot } from '@smoothbricks/lmao/es';

import { runPlatformScenario } from '../../lmao/benchmarks/plugin-scenario/platform';
import type { DynamicFunctionCounter } from './dynamic-function-counter';
import { getTransformVariant } from './transform-variant';

export interface HeadlessBenchmarkHost {
  readonly engine: string;
  readonly now: () => number;
  readonly writeLine: (line: string) => void;
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
      ...(functionCounter === undefined ? {} : { getDynamicFunctionCallCount: functionCounter.readForScenario }),
    });
    host.writeLine(`LMAO_BENCH_RESULT ${JSON.stringify(result)}`);
  } finally {
    functionCounter?.restore();
  }
}
