require('./src/install-text-encoding');

// React Native installs performance.now; the standalone Hermes VM used by CI does not.
// The Date.now fallback preserves millisecond units: steady batching amortizes its
// resolution, while cold single-operation values may be quantized and must be compared
// only within the same engine/host clock.
if (typeof globalThis.performance !== 'object' || typeof globalThis.performance.now !== 'function') {
  globalThis.performance = { now: () => Date.now() };
}
const now = () => globalThis.performance.now();

// The standalone VM has no JSI crypto host. This deterministic source isolates LMAO from bridge cost.
if (typeof globalThis.crypto !== 'object' || typeof globalThis.crypto.getRandomValues !== 'function') {
  let randomState = 0x9e3779b9;
  globalThis.crypto = {
    getRandomValues(array) {
      for (let index = 0; index < array.length; index++) {
        randomState ^= randomState << 13;
        randomState ^= randomState >>> 17;
        randomState ^= randomState << 5;
        array[index] = randomState & 0xff;
      }
      return array;
    },
  };
}
const engine = typeof HermesInternal === 'object' ? 'hermes' : 'javascript';
const writeLine = typeof print === 'function' ? (line) => print(line) : (line) => console.log(line);

const { getBenchmarkMode } = require('./src/benchmark-mode');
const { observeDynamicFunctionCalls } = require('./src/dynamic-function-counter');
const mode = getBenchmarkMode();
const functionCounter = mode === 'diagnostic' ? observeDynamicFunctionCalls() : undefined;
// `src/headless` statically imports the scenario graph. Timing the require from outside
// therefore includes evaluation of that graph instead of merely timing a cached import.
const moduleInitializationStartedAt = now();
const { runHeadlessBenchmark } = require('./src/headless');
const moduleInitializationMs = now() - moduleInitializationStartedAt;

runHeadlessBenchmark({ engine, now, writeLine, mode, moduleInitializationMs }, functionCounter);
