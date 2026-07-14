import { createTraceRoot } from '@smoothbricks/lmao/es';
import type { ScenarioRuntime } from '../../lmao/benchmarks/plugin-scenario/scenario';
import { createJsScenarioRuntime } from '../../lmao/benchmarks/plugin-scenario/scenario';
import type { SuperblockBenchmarkResult } from './superblock-benchmark';

export function createPlatformRuntime(): ScenarioRuntime {
  return createJsScenarioRuntime(createTraceRoot);
}

export function runPlatformSuperblockBenchmark(_runtime: ScenarioRuntime): SuperblockBenchmarkResult | undefined {
  return undefined;
}
