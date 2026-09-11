import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { cpus, platform, release } from 'node:os';
import { measure } from 'mitata';
import { SubscriberCountBatch } from '../dist/subscriber-counts.js';

// Reference loop at 8331a29cf68bb1bcedb2e28772e464279f347bbb, benchmark only.
function baseline(payloads) {
  let counts = payloads[0];
  for (let index = 1; index < payloads.length; index += 1) counts = { ...counts, ...payloads[index] };
  return counts;
}

const reversed = process.env.STATEBUS_BENCH_ORDER === 'candidate-first';
const order = reversed ? ['candidate', 'baseline'] : ['baseline', 'candidate'];
const checksums = [];
const measurements = [];
const memoryBefore = process.memoryUsage();
for (const size of [1, 32, 256]) {
  // Inputs and accumulator are outside timing. Owned publication output remains
  // inside timing. This phase does NOT claim zero allocations for owned output.
  const payloads = Array.from({ length: size }, (_, index) => Object.freeze({ [`state${index}`]: index % 3 }));
  const batch = new SubscriberCountBatch();
  function candidate() {
    // Match the dispatcher's single-event passthrough.
    if (payloads.length === 1) return payloads[0];
    for (let index = 0; index < payloads.length; index += 1) batch.append(payloads[index]);
    return batch.take();
  }
  const expected = baseline(payloads);
  assert.deepStrictEqual(candidate(), expected);
  const retained = Object.freeze(candidate());
  assert.deepStrictEqual(candidate(), retained);
  checksums.push({ size, sha256: createHash('sha256').update(JSON.stringify(expected)).digest('hex') });
  for (const name of order) {
    const operation = name === 'baseline' ? () => baseline(payloads) : candidate;
    let observed;
    function invoke() {
      observed = operation();
    }
    for (let warmup = 0; warmup < 2048; warmup += 1) invoke();
    const stats = await measure(invoke, {
      min_samples: 2048,
      max_samples: 8192,
      min_cpu_time: 100_000_000,
      batch_threshold: 0,
      warmup_samples: 2,
    });
    assert.deepStrictEqual(observed, expected);
    measurements.push({ name: `interest-owned-flush/${size}/${name}`, stats });
  }
}
const memoryAfter = process.memoryUsage();
function percentile(samples, fraction) {
  return samples[Math.min(samples.length - 1, Math.ceil(samples.length * fraction) - 1)];
}
const summary = measurements.map(({ name, stats }) => ({
  name,
  samples: stats.samples.length,
  p50: percentile(stats.samples, 0.5),
  p95: percentile(stats.samples, 0.95),
  p99: percentile(stats.samples, 0.99),
  p999: percentile(stats.samples, 0.999),
}));
const report = {
  formatVersion: 1,
  phase: 'interest-owned-flush',
  units: 'nanoseconds per complete wave; individual Mitata samples, no batching',
  baselineCommit: '8331a29cf68bb1bcedb2e28772e464279f347bbb',
  candidateCommit: process.env.STATEBUS_BENCH_COMMIT ?? 'working-tree',
  candidateSourceSha256: createHash('sha256')
    .update(readFileSync(new URL('../src/subscriber-counts.ts', import.meta.url)))
    .digest('hex'),
  order,
  checksums,
  environment: { versions: process.versions, platform: platform(), release: release(), cpu: cpus()[0]?.model },
  instrumentation: {
    memoryBefore,
    memoryAfter,
    memoryScope: 'process totals around the full benchmark; not per-operation allocations or peak/retained deltas',
    allocationsPerOperation: 'unavailable',
    gcPauses: 'unavailable',
    inlineCaches: 'unavailable',
    branchCounters: 'unavailable',
    wasmAndArrow: 'not exercised by this phase',
  },
  summary,
  measurements,
};
const engine = process.versions.bun ? 'bun' : 'node';
const output = new URL(`../.cache/benchmarks/interest-${engine}-${order[0]}.json`, import.meta.url);
mkdirSync(new URL('.', output), { recursive: true });
writeFileSync(output, `${JSON.stringify(report)}\n`);
console.log(`Raw samples: ${output.pathname}`);
console.log('| Scenario | Samples | p50 ns | p95 ns | p99 ns | p99.9 ns |');
console.log('| --- | ---: | ---: | ---: | ---: | ---: |');
for (const row of summary) {
  console.log(
    `| ${row.name} | ${row.samples} | ${row.p50.toFixed(2)} | ${row.p95.toFixed(2)} | ${row.p99.toFixed(2)} | ${row.p999.toFixed(2)} |`,
  );
}
