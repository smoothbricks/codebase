require('./src/install-text-encoding');

// React Native installs performance.now; the standalone Hermes VM used by CI does not.
// Batching thousands of operations per sample makes the Date.now fallback precise enough.
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

const { observeDynamicFunctionCalls } = require('./src/dynamic-function-counter');
const functionCounter = observeDynamicFunctionCalls();
const { runHeadlessBenchmark } = require('./src/headless');

runHeadlessBenchmark({ engine, now, writeLine }, functionCounter);
