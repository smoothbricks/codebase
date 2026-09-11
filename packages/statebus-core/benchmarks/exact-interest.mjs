import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { cpus, platform, release } from 'node:os';
import { measure } from 'mitata';
import { StateInterestBatch, StateInterestRegistry } from '../dist/interest.js';
import { sameViewProps } from '../dist/view.js';

// The implementation at PR23 head 21215c95: JSON keys plus prefix reconstruction.
function originalMerge(previous, incoming) {
  const map = new Map();
  for (const change of previous)
    map.set(JSON.stringify([change.interest.key, typeof change.interest.id, change.interest.id ?? null]), change);
  for (const change of incoming)
    map.set(JSON.stringify([change.interest.key, typeof change.interest.id, change.interest.id ?? null]), change);
  return [...map.values()];
}
function originalWave(parts) {
  let result = parts[0];
  for (let index = 1; index < parts.length; index++) result = originalMerge(result, parts[index]);
  return result;
}
function originalProps(props) {
  return JSON.stringify([
    'object',
    Object.keys(props)
      .sort()
      .map((key) => [key, [typeof props[key], props[key]]]),
  ]);
}
const order =
  process.env.STATEBUS_BENCH_ORDER === 'candidate-first' ? ['candidate', 'baseline'] : ['baseline', 'candidate'];
const measurements = [];
const checksums = [];
const memoryBefore = process.memoryUsage();
async function run(name, operation, expected, verify = (actual) => assert.deepStrictEqual(actual, expected)) {
  let observed;
  const invoke = () => {
    observed = operation();
  };
  for (let index = 0; index < 128; index++) invoke();
  verify(observed);
  const stats = await measure(invoke, {
    min_samples: 512,
    max_samples: 4096,
    min_cpu_time: 100_000_000,
    batch_threshold: 0,
    warmup_samples: 2,
  });
  verify(observed);
  measurements.push({ name, stats });
}
for (const size of [1, 32, 256, 1000]) {
  const parts = Array.from({ length: size }, (_, id) =>
    Object.freeze([Object.freeze({ interest: Object.freeze({ key: 'records', id }), subscribers: id % 3 })]),
  );
  const expected = originalWave(parts);
  checksums.push({
    phase: 'owned-wave',
    size,
    sha256: createHash('sha256').update(JSON.stringify(expected)).digest('hex'),
  });
  const batch = new StateInterestBatch();
  const candidate = () => {
    for (let index = 0; index < parts.length; index++) batch.append(parts[index]);
    return batch.take();
  };
  const retained = Object.freeze(candidate());
  assert.deepStrictEqual(candidate(), expected);
  for (const variant of order)
    await run(
      `owned-wave/${size}/${variant}`,
      variant === 'candidate' ? candidate : () => originalWave(parts),
      expected,
    );
  assert.deepStrictEqual(retained, expected);
  // Scratch updates are a separate phase: owned publication is deliberately excluded.
  await run(
    `warmed-scratch/${size}/candidate`,
    () => {
      for (let index = 0; index < parts.length; index++) batch.append(parts[index]);
      batch.clear();
      return batch.retainedAddresses;
    },
    size,
  );
  assert.deepStrictEqual(candidate(), expected);
}
// Separate setup/growth costs; these measurements do not claim allocation-free setup.
for (const size of [1, 2048]) {
  const parts = Array.from({ length: size }, (_, id) => [{ interest: { key: 'records', id }, subscribers: 1 }]);
  const expected = originalWave(parts);
  await run(
    `cold-setup-and-owned-wave/${size}/candidate`,
    () => {
      const batch = new StateInterestBatch();
      for (const part of parts) batch.append(part);
      const output = batch.take();
      assert.ok(batch.retainedAddresses <= 1024);
      return output;
    },
    expected,
  );
}
const address = Object.freeze({ key: 'records', id: 7 });
const registry = new StateInterestRegistry(() => {});
const releaseLease = registry.acquire([address]);
const originalCounts = new Map([[JSON.stringify(['records', 'number', 7]), 1]]);
for (const variant of order) {
  await run(
    `warmed-count-read/${variant}`,
    variant === 'candidate'
      ? () => registry.countAt('records', 7)
      : () => originalCounts.get(JSON.stringify([address.key, typeof address.id, address.id ?? null])),
    1,
  );
}
releaseLease();
const props = Object.freeze({ id: 'article', locale: 'fr', page: 2 });
const equalProps = Object.freeze({ page: 2, locale: 'fr', id: 'article' });
const originalIdentity = originalProps(props);
for (const variant of order)
  await run(
    `unchanged-props/${variant}`,
    variant === 'candidate'
      ? () => sameViewProps(props, equalProps)
      : () => originalProps(equalProps) === originalIdentity,
    true,
  );
const memoryAfter = process.memoryUsage();
function percentile(samples, q) {
  return samples[Math.max(0, Math.ceil(samples.length * q) - 1)];
}
const summary = measurements.map(({ name, stats }) => {
  const sorted = [...stats.samples].sort((a, b) => a - b);
  return {
    name,
    samples: sorted.length,
    p50: percentile(sorted, 0.5),
    p95: percentile(sorted, 0.95),
    p99: percentile(sorted, 0.99),
    p999: percentile(sorted, 0.999),
  };
});
const report = {
  formatVersion: 1,
  units: 'nanoseconds per complete operation; Mitata individual unbatched samples',
  baselineCommit: '21215c95fd45c011dd2937df57a1c6d7808fd8bd',
  candidateCommit: process.env.STATEBUS_BENCH_COMMIT ?? 'working-tree',
  sourceSha256: Object.fromEntries(
    ['interest.ts', 'view.ts'].map((file) => [
      file,
      createHash('sha256')
        .update(readFileSync(new URL(`../src/${file}`, import.meta.url)))
        .digest('hex'),
    ]),
  ),
  order,
  checksums,
  summary,
  measurements,
  environment: { versions: process.versions, platform: platform(), release: release(), cpu: cpus()[0]?.model },
  instrumentation: {
    tracing:
      'Native repository-built candidate; these leaf functions declare no tracing Op. Separate plugin-on/off runs unavailable.',
    memoryBefore,
    memoryAfter,
    memoryScope:
      'Whole-process snapshots around the entire run; not per-op allocation, effective peak, or retained bounds.',
    allocationsPerOperation: 'unavailable',
    gcPauses: 'unavailable',
    inlineCaches: 'unavailable',
    branchCounters: 'unavailable',
  },
};
const engine = process.versions.bun ? 'bun' : 'node';
const output = new URL(`../.cache/benchmarks/exact-interest-${engine}-${order[0]}.json`, import.meta.url);
mkdirSync(new URL('.', output), { recursive: true });
writeFileSync(output, `${JSON.stringify(report)}\n`);
console.log(`Raw samples: ${output.pathname}`);
console.log('| Scenario | Samples | p50 ns | p95 ns | p99 ns | p99.9 ns |');
console.log('| --- | ---: | ---: | ---: | ---: | ---: |');
for (const row of summary)
  console.log(
    `| ${row.name} | ${row.samples} | ${row.p50.toFixed(2)} | ${row.p95.toFixed(2)} | ${row.p99.toFixed(2)} | ${row.p999.toFixed(2)} |`,
  );
