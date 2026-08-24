import { describe, expect, it } from 'bun:test';

import type { BenchmarkMode } from '../../lmao/benchmarks/plugin-scenario/platform.js';
import { getBenchmarkMode } from './benchmark-mode.js';

const MODE_ENVIRONMENT_VARIABLE = 'LMAO_BENCH_MODE';

function withBenchmarkMode(value: string | undefined, assertion: () => void): void {
  const previousValue = process.env[MODE_ENVIRONMENT_VARIABLE];

  try {
    if (value === undefined) {
      delete process.env[MODE_ENVIRONMENT_VARIABLE];
    } else {
      process.env[MODE_ENVIRONMENT_VARIABLE] = value;
    }
    assertion();
  } finally {
    if (previousValue === undefined) {
      delete process.env[MODE_ENVIRONMENT_VARIABLE];
    } else {
      process.env[MODE_ENVIRONMENT_VARIABLE] = previousValue;
    }
  }
}

describe('getBenchmarkMode', () => {
  it.each([
    ['cold', 'cold'],
    ['steady', 'steady'],
    ['diagnostic', 'diagnostic'],
  ] as const)('accepts the configured %s mode', (injectedMode: string, expectedMode: BenchmarkMode) => {
    withBenchmarkMode(injectedMode, () => {
      expect(getBenchmarkMode()).toBe(expectedMode);
    });
  });

  it.each(['warm', 'Cold', ''] as const)('rejects the configured %j mode', (invalidMode: string) => {
    withBenchmarkMode(invalidMode, () => {
      expect(() => getBenchmarkMode()).toThrow('LMAO_BENCH_MODE must be exactly "cold", "steady", or "diagnostic".');
    });
  });

  it('rejects a build with no injected mode', () => {
    withBenchmarkMode(undefined, () => {
      expect(() => getBenchmarkMode()).toThrow('LMAO_BENCH_MODE must be exactly "cold", "steady", or "diagnostic".');
    });
  });
});
