/**
 * Timestamp API Benchmark
 *
 * Run with:
 *   node --experimental-strip-types packages/lmao/src/lib/__tests__/benchmark-timestamps.ts
 *   bun packages/lmao/src/lib/__tests__/benchmark-timestamps.ts
 */

declare const Bun: { version: string } | undefined;

const ITERATIONS = 10_000_000;
const WARMUP = 100_000;

interface BenchmarkResult {
  name: string;
  totalMs: number;
  perCallNs: number;
}

function benchmark(name: string, fn: () => void): BenchmarkResult {
  // Warmup
  for (let i = 0; i < WARMUP; i++) {
    fn();
  }

  // Force GC if available
  if (typeof globalThis.gc === 'function') {
    globalThis.gc();
  }

  const start = process.hrtime.bigint();
  for (let i = 0; i < ITERATIONS; i++) {
    fn();
  }
  const end = process.hrtime.bigint();

  const totalNs = Number(end - start);
  return {
    name,
    totalMs: totalNs / 1_000_000,
    perCallNs: totalNs / ITERATIONS,
  };
}

function printResults(results: BenchmarkResult[]): void {
  const separator = '='.repeat(70);
  const runtime = typeof Bun !== 'undefined' ? 'Bun ' + Bun.version : 'Node ' + process.version;
  console.log('\n' + separator);
  console.log('Runtime: ' + runtime);
  console.log('Iterations: ' + ITERATIONS.toLocaleString());
  console.log(separator);
  console.log('');
  console.log('API'.padEnd(45) + 'Total (ms)'.padStart(12) + 'Per call (ns)'.padStart(15));
  console.log('-'.repeat(70));

  const baseline = results[0].perCallNs;
  for (const r of results) {
    const ratio = r.perCallNs / baseline;
    const ratioStr = ratio === 1 ? '(baseline)' : '(' + ratio.toFixed(1) + 'x)';
    console.log(
      r.name.padEnd(45) + r.totalMs.toFixed(2).padStart(12) + (r.perCallNs.toFixed(2) + ' ' + ratioStr).padStart(15),
    );
  }
  console.log('');
}

function runBenchmarks(): void {
  const results: BenchmarkResult[] = [];

  // 1. Raw performance.now()
  results.push(
    benchmark('performance.now()', () => {
      performance.now();
    }),
  );

  // 2. Raw hrtime.bigint()
  results.push(
    benchmark('process.hrtime.bigint()', () => {
      process.hrtime.bigint();
    }),
  );

  // 3. timeOrigin + performance.now() (absolute time, Float64)
  const perfOrigin = performance.timeOrigin;
  results.push(
    benchmark('timeOrigin + performance.now()', () => {
      perfOrigin + performance.now();
    }),
  );

  // 4. Microseconds as Float64 (browser approach)
  results.push(
    benchmark('(timeOrigin + now()) * 1000 [us Float64]', () => {
      (perfOrigin + performance.now()) * 1000;
    }),
  );

  // 5. Microseconds as BigInt (what we need for Arrow)
  results.push(
    benchmark('BigInt(Math.trunc((origin + now()) * 1000))', () => {
      BigInt(Math.trunc((perfOrigin + performance.now()) * 1000));
    }),
  );

  // 6. hrtime to microseconds BigInt
  results.push(
    benchmark('process.hrtime.bigint() / 1000n [us]', () => {
      process.hrtime.bigint() / 1000n;
    }),
  );

  // 7. hrtime raw (nanoseconds, no conversion)
  results.push(
    benchmark('process.hrtime.bigint() raw [ns]', () => {
      process.hrtime.bigint();
    }),
  );

  // 8. Write Float64 to Float64Array
  const f64arr = new Float64Array(1);
  results.push(
    benchmark('Write to Float64Array', () => {
      f64arr[0] = (perfOrigin + performance.now()) * 1000;
    }),
  );

  // 9. Write BigInt to BigInt64Array
  const bi64arr = new BigInt64Array(1);
  results.push(
    benchmark('Write BigInt to BigInt64Array', () => {
      bi64arr[0] = BigInt(Math.trunc((perfOrigin + performance.now()) * 1000));
    }),
  );

  // 10. Write hrtime to BigInt64Array
  results.push(
    benchmark('Write hrtime.bigint() to BigInt64Array', () => {
      bi64arr[0] = process.hrtime.bigint();
    }),
  );

  // 11. Current anchor approach (simulated)
  const anchorEpoch = perfOrigin * 1000;
  const anchorPerf = performance.now() * 1000;
  results.push(
    benchmark('Anchor approach: epoch + (now - anchorPerf)', () => {
      anchorEpoch + (performance.now() * 1000 - anchorPerf);
    }),
  );

  printResults(results);

  // Summary
  console.log('SUMMARY:');
  console.log('-'.repeat(70));
  console.log('For zero-copy Arrow timestamps (BigInt64Array), best options are:');
  console.log('');

  const writeF64 = results.find((r) => r.name.includes('Write to Float64Array'));
  const writeBi64 = results.find((r) => r.name.includes('Write BigInt to BigInt64Array'));
  const writeHrtime = results.find((r) => r.name.includes('Write hrtime.bigint()'));

  if (writeF64 && writeBi64 && writeHrtime) {
    console.log('  1. Float64Array (convert to BigInt in cold path): ' + writeF64.perCallNs.toFixed(2) + ' ns/call');
    console.log('  2. BigInt64Array with performance API:            ' + writeBi64.perCallNs.toFixed(2) + ' ns/call');
    console.log('  3. BigInt64Array with hrtime.bigint():            ' + writeHrtime.perCallNs.toFixed(2) + ' ns/call');
    console.log('');
    console.log(
      'Hot path overhead for BigInt64Array: ' +
        ((writeBi64.perCallNs / writeF64.perCallNs - 1) * 100).toFixed(0) +
        '% slower than Float64Array',
    );
    console.log('');
  }
}

runBenchmarks();
