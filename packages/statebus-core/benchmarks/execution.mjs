/** Run separately for baseline and candidate packages; profiler and timing runs are intentionally separate. */
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { cpus } from 'node:os';
import { dirname, join } from 'node:path';
import { PerformanceObserver, performance } from 'node:perf_hooks';
import {
  bindEffect,
  composeLibraries,
  defineEffect,
  defineLibrary,
  ManualScheduler,
  mountLibrary,
} from '@smoothbricks/statebus-core';

const mode = process.env.STATEBUS_BENCH_MODE ?? 'timing';
if (mode !== 'timing' && mode !== 'allocation') throw Error('Use timing or allocation mode.');
if (mode === 'allocation' && process.versions.bun) throw Error('The allocation sampler requires V8/Node.');
const count = Number(process.env.STATEBUS_BENCH_ITERATIONS ?? 2000);
const sizes = (process.env.STATEBUS_BENCH_KEYS ?? '64,4096,16384').split(',').map(Number);
assert.ok(Number.isSafeInteger(count) && count >= 100);
for (const size of sizes) assert.ok(Number.isSafeInteger(size) && size > 0 && size < 100_000);
const require = createRequire(import.meta.url);
const entry = require.resolve('@smoothbricks/statebus-core');
const results = [];
const gc = [];
const observer = new PerformanceObserver((list) => {
  for (const item of list.getEntries())
    gc.push({ start: item.startTime, duration: item.duration, kind: item.detail.kind });
});
observer.observe({ entryTypes: ['gc'] });

function model(policy) {
  const definition = defineLibrary({
    name: 'execution-benchmark',
    requires: [],
    setup(scope) {
      const command = scope.command('command', () => true);
      const result = scope.event('result');
      const effect = defineEffect({
        command,
        result,
        policy,
        plan: (_state, payload) => payload,
        decode: () => undefined,
      });
      return { command, effect };
    },
  });
  const mount = mountLibrary(definition, 'benchmark');
  const errors = [];
  const runtime = composeLibraries(mount).createRuntime({
    scheduler: new ManualScheduler(),
    onError: (error) => errors.push(error),
  });
  return { runtime, errors, ...mount.exports };
}
function sumProfile(node) {
  let bytes = node.selfSize;
  for (const child of node.children) bytes += sumProfile(child);
  return bytes;
}
async function run(keys) {
  const { runtime, errors, command, effect } = model('latest-wins');
  const operation = Promise.withResolvers();
  let calls = 0;
  const binding = bindEffect(runtime, effect, {
    maxPending: keys + count + 1024,
    execute: () => {
      calls++;
      return operation.promise;
    },
    failure: (cause) => {
      throw cause;
    },
  });
  let sequence = 0;
  for (let key = 0; key < keys; key++) runtime.publish(command, { requestId: ++sequence, operationKey: key });
  runtime.flush();
  const publish = () => {
    runtime.publish(command, { requestId: ++sequence, operationKey: 'hot' });
    runtime.flush();
  };
  for (let warm = 0; warm < 256; warm++) publish();
  globalThis.gc?.();
  let session;
  if (mode === 'allocation') {
    const { Session } = await import('node:inspector/promises');
    session = new Session();
    session.connect();
    await session.post('HeapProfiler.startSampling', {
      samplingInterval: 4096,
      includeObjectsCollectedByMajorGC: true,
      includeObjectsCollectedByMinorGC: true,
    });
  }
  const samples = new Float64Array(count);
  const start = performance.now();
  for (let index = 0; index < count; index++) {
    const before = performance.now();
    publish();
    samples[index] = (performance.now() - before) * 1000;
  }
  const end = performance.now();
  let sampledBytes;
  if (session) {
    const { profile } = await session.post('HeapProfiler.stopSampling');
    sampledBytes = sumProfile(profile.head);
    session.disconnect();
  }
  samples.sort();
  // Let perf_hooks deliver GC entries, then exclude GC outside the measured interval.
  await new Promise((resolve) => setImmediate(resolve));
  const pauses = gc.filter((event) => event.start >= start && event.start <= end);
  assert.equal(calls, keys + 256 + count);
  results.push({
    workload: 'one-key supersession with unrelated pending keys',
    keys,
    operations: count,
    elapsedMs: end - start,
    flushMicroseconds:
      mode === 'timing'
        ? {
            p50: samples[Math.floor(count * 0.5)],
            p95: samples[Math.floor(count * 0.95)],
            p99: samples[Math.floor(count * 0.99)],
          }
        : undefined,
    sampledAllocatedBytes: sampledBytes,
    sampledAllocatedBytesPerOperation: sampledBytes === undefined ? undefined : sampledBytes / count,
    gc: {
      count: pauses.length,
      totalMs: pauses.reduce((sum, event) => sum + event.duration, 0),
      maxMs: Math.max(0, ...pauses.map((event) => event.duration)),
    },
  });
  operation.resolve(1);
  await runtime.drain();
  await binding.disposeAsync();
  runtime.dispose();
  assert.deepEqual(errors, []);
}
try {
  for (const size of sizes) await run(size);
  console.log(
    JSON.stringify(
      {
        formatVersion: 1,
        mode,
        versions: process.versions,
        cpu: cpus()[0]?.model,
        implementationSha256: Object.fromEntries(
          ['index.js', 'composition.js', 'effects.js', 'dispatch.js'].map((name) => [
            name,
            createHash('sha256')
              .update(readFileSync(join(dirname(entry), name)))
              .digest('hex'),
          ]),
        ),
        sourceCommit: process.env.STATEBUS_BENCH_COMMIT ?? 'unspecified',
        allocationMethod:
          mode === 'allocation'
            ? 'V8 sampling at 4096 bytes, including minor/major collected objects'
            : 'not measured in timing mode',
        notes:
          'Synthetic direct operations. Setup/teardown are outside measurement. Sampling includes harness/engine overhead; estimates are not exact allocated bytes. Timing and GC are local diagnostics, not latency guarantees.',
        results,
      },
      null,
      2,
    ),
  );
} finally {
  observer.disconnect();
}
