import { describe, expect, it } from 'bun:test';
import { createTraceRoot } from '@smoothbricks/lmao/node';

import { runPlatformScenario } from '../../lmao/benchmarks/plugin-scenario/platform.js';
import { observeDynamicFunctionCalls } from './dynamic-function-counter.js';

const COMMON_OPTIONS = {
  platform: 'test-platform',
  engine: 'test-engine',
  variant: 'test-variant',
  createTraceRoot,
  now: () => 0,
};

function requireFunctionCounter() {
  const counter = observeDynamicFunctionCalls();
  if (counter === undefined) {
    throw new Error('Bun must provide Proxy for this Function observer test');
  }
  return counter;
}

function expectTimingModeRejectsFunctionObserver(mode: 'cold' | 'steady'): void {
  const originalFunction = globalThis.Function;
  const counter = requireFunctionCounter();

  try {
    const sentinel = new Function('return 17');
    expect(sentinel()).toBe(17);

    expect(() =>
      runPlatformScenario({
        ...COMMON_OPTIONS,
        mode,
        ...(mode === 'cold' ? { moduleInitializationMs: 1 } : {}),
        getDynamicFunctionCallCount: counter.readForScenario,
      }),
    ).toThrow(`${mode === 'cold' ? 'Cold' : 'Steady'} mode must not install the dynamic Function observer`);

    expect(counter.readForScenario()).toBe(0);
    expect(counter.readForScenario()).toBe(1);
  } finally {
    counter.restore();
  }

  expect(globalThis.Function).toBe(originalFunction);
}

describe('dynamic Function diagnostics', () => {
  it('preserves Function call and construction while counting both operations', () => {
    const originalFunction = globalThis.Function;
    const counter = requireFunctionCounter();

    try {
      expect(globalThis.Function).not.toBe(originalFunction);

      const calledFunction = Function('return 41');
      const constructedFunction = new Function('return 42');
      expect(calledFunction()).toBe(41);
      expect(constructedFunction()).toBe(42);
      expect(counter.readForScenario()).toBe(0);
      expect(counter.readForScenario()).toBe(2);
    } finally {
      counter.restore();
    }

    expect(globalThis.Function).toBe(originalFunction);
  });

  it('refuses Proxy-instrumented cold measurements before reading the counter', () => {
    expectTimingModeRejectsFunctionObserver('cold');
  });

  it('refuses Proxy-instrumented steady measurements before reading the counter', () => {
    expectTimingModeRejectsFunctionObserver('steady');
  });
});
