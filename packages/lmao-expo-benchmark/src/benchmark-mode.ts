declare const process: {
  readonly env: {
    readonly LMAO_BENCH_MODE?: string;
  };
};

import type { BenchmarkMode } from '../../lmao/benchmarks/plugin-scenario/platform';

export function getBenchmarkMode(): BenchmarkMode {
  const mode = process.env.LMAO_BENCH_MODE;
  if (mode !== 'cold' && mode !== 'steady' && mode !== 'diagnostic') {
    throw new Error('LMAO_BENCH_MODE must be exactly "cold", "steady", or "diagnostic".');
  }
  return mode;
}
