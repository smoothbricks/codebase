export interface DynamicFunctionCounter {
  readonly readForScenario: () => number;
  readonly restore: () => void;
}

export function observeDynamicFunctionCalls(): DynamicFunctionCounter | undefined {
  if (typeof Proxy !== 'function') {
    return undefined;
  }

  const originalFunction = globalThis.Function;
  let calls = 0;
  let firstScenarioRead = true;
  const observedFunction = new Proxy(originalFunction, {
    apply(target, thisArgument, argumentsList) {
      calls += 1;
      return Reflect.apply(target, thisArgument, argumentsList);
    },
    construct(target, argumentsList, newTarget) {
      calls += 1;
      return Reflect.construct(target, argumentsList, newTarget);
    },
  });

  globalThis.Function = observedFunction;

  return {
    readForScenario: () => {
      if (firstScenarioRead) {
        firstScenarioRead = false;
        return 0;
      }
      return calls;
    },
    restore: () => {
      if (globalThis.Function === observedFunction) {
        globalThis.Function = originalFunction;
      }
    },
  };
}
