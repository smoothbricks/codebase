import assert from 'node:assert/strict';
import { realpath } from 'node:fs/promises';
import { join } from 'node:path';
import { filesetSource } from '../src/__tests__/cargo-closure-filesets.js';
import { dependencyClosure } from '../src/cargo-source-hash.js';
import { resolveOutputDirs } from '../src/executors/clean-outputs/executor.js';

// Read-only candidate census: no executor invocation, file removal, Cargo process, or Nx gate.
const root = await realpath(process.cwd());
for (const size of [16, 64, 256, 1024]) {
  const outputs = Array.from({ length: size }, (_, index) => `{projectRoot}/dist-${index}`);
  const options = { outputs, projectName: 'fixture', projectRoot: 'packages/fixture', workspaceRoot: root };
  const expected = new Set(outputs.map((_, index) => join(root, 'packages/fixture', `dist-${index}`)));
  assert.deepEqual(new Set(resolveOutputDirs(options)), expected);
  for (let warm = 0; warm < 5; warm += 1) resolveOutputDirs(options);
  const samples: number[] = [];
  let checksum = 0;
  const iterations = Math.max(1, Math.floor(256 / size));
  for (let sample = 0; sample < 9; sample += 1) {
    const start = performance.now();
    for (let iteration = 0; iteration < iterations; iteration += 1) checksum += resolveOutputDirs(options).length;
    samples.push(((performance.now() - start) * 1000) / iterations);
  }
  samples.sort((left, right) => left - right);
  console.log(JSON.stringify({ site: 'clean-outputs:resolveOutputDirs', size, medianUs: samples[4], checksum }));
}
for (const size of [16, 64, 256, 1024]) {
  const directories = Array.from({ length: size }, (_, index) => `crates/p${index}`);
  const source = filesetSource(root, directories, []);
  // A chain of overlapping diamonds has exponentially many paths but only two edges per vertex.
  const edges = new Map(
    source.cargo.local.map(({ id }, index) => [id, [index + 1, index + 2].filter((next) => next < size).map(String)]),
  );
  const cargo = { ...source.cargo, edges };
  // Root selects all local members; the traversal must still deduplicate the converging edge visits.
  assert.equal((await dependencyClosure(cargo, root)).size, size);
  for (let warm = 0; warm < 5; warm += 1) await dependencyClosure(cargo, root);
  const samples: number[] = [];
  let checksum = 0;
  for (let sample = 0; sample < 9; sample += 1) {
    const start = performance.now();
    for (let iteration = 0; iteration < 32; iteration += 1) checksum += (await dependencyClosure(cargo, root)).size;
    samples.push(((performance.now() - start) * 1000) / 32);
  }
  samples.sort((left, right) => left - right);
  console.log(
    JSON.stringify({
      site: 'cargo-source-hash:dependencyClosure',
      size,
      edges: 2 * size - 3,
      medianUs: samples[4],
      checksum,
    }),
  );
}
