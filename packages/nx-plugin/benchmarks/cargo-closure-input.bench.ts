import assert from 'node:assert/strict';
import { realpath } from 'node:fs/promises';
import { cpus } from 'node:os';
import { posix } from 'node:path';
import { cargoClosureInputs } from '../src/cargo-closure-input.js';
import { expandedFilesets, filesetSource } from '../src/__tests__/cargo-closure-filesets.js';

// Run from the workspace root: taskpolicy -b bun packages/nx-plugin/benchmarks/cargo-closure-input.bench.ts
// One caller, warmed resolution/source-scan data; the real async closure entry still performs its realpath.
const root = await realpath(process.cwd());
console.log(
  JSON.stringify({ runtime: process.version, platform: process.platform, arch: process.arch, cpu: cpus()[0]?.model }),
);
for (const size of [1, 4, 16, 25, 64, 256, 1024, 4096]) {
  const directories = Array.from({ length: size }, (_, index) => `crates/p${String(index).padStart(5, '0')}`);
  const source = filesetSource(root, directories, ['shared.rs']);
  const copies = { directoryEntries: 0, siblingNames: 0 };
  const expected = expandedFilesets(
    directories,
    ['Cargo.toml', 'shared.rs', ...directories.map((directory) => posix.join(directory, 'src/lib.rs'))],
    copies,
  );
  assert.deepEqual(await cargoClosureInputs('', [source]), expected);
  for (let warm = 0; warm < 3; warm += 1) await cargoClosureInputs('', [source]);
  const samples: number[] = [];
  let checksum = 0;
  const iterations = Math.max(1, Math.floor(4096 / size));
  for (let sample = 0; sample < 9; sample += 1) {
    const start = performance.now();
    for (let iteration = 0; iteration < iterations; iteration += 1) {
      const result = await cargoClosureInputs('', [source]);
      checksum += result.length;
    }
    samples.push(((performance.now() - start) * 1000) / iterations);
  }
  samples.sort((left, right) => left - right);
  console.log(
    JSON.stringify({
      size,
      inputs: expected.length,
      iterations,
      medianUs: samples[4],
      minUs: samples[0],
      maxUs: samples[8],
      checksum,
      expansionCopies: copies,
    }),
  );
}
