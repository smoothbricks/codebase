import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import crypto from 'node:crypto';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import { syncBuiltinESMExports } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { hashCargoPathInputs } from '../src/cargo-source-hash.js';

// Build both revisions without leaving a second implementation in the source tree.
// Node workers observe live builtin bindings; the production runtime remains unchanged.
if (process.versions.bun) {
  const workspace = resolve(import.meta.dirname, '../../..');
  const baselineRef = process.argv[2] ?? '7288dbc732b439ae07ec404b3351c9d910d511d6c';
  const oldSource = execFileSync('git', ['show', baselineRef + ':packages/nx-plugin/src/cargo-source-hash.ts'], {
    cwd: workspace,
    encoding: 'utf8',
  });
  await fsp.mkdir(join(workspace, '.nx'), { recursive: true });
  const buildDirectory = await fsp.mkdtemp(join(workspace, '.nx/cargo-source-read-'));
  try {
    const workers = new Map<string, string>();
    for (const label of ['before', 'after']) {
      const result = await Bun.build({
        entrypoints: [import.meta.filename],
        target: 'node',
        packages: 'external',
        outdir: buildDirectory,
        naming: label + '.mjs',
        plugins:
          label === 'before'
            ? [
                {
                  name: 'exact-baseline-source',
                  setup(build) {
                    build.onLoad({ filter: /[/\\]cargo-source-hash\.ts$/ }, ({ path }) => ({
                      contents: oldSource,
                      loader: 'ts',
                      resolveDir: dirname(path),
                    }));
                  },
                },
              ]
            : [],
      });
      if (!result.success) throw new AggregateError(result.logs, 'Cannot build ' + label + ' benchmark');
      const worker = result.outputs[0];
      assert.ok(worker);
      workers.set(label, worker.path);
    }
    for (const size of [1, 16, 64, 256]) {
      const digests = new Set<string>();
      for (const label of size === 16 || size === 256 ? ['after', 'before'] : ['before', 'after']) {
        const worker = workers.get(label);
        assert.ok(worker);
        const stdout = execFileSync('node', [worker, label, String(size)], {
          cwd: workspace,
          encoding: 'utf8',
        });
        process.stdout.write(stdout);
        for (const line of stdout.trim().split('\n')) {
          const row: unknown = JSON.parse(line);
          assert.ok(typeof row === 'object' && row !== null && 'digest' in row && typeof row.digest === 'string');
          digests.add(row.digest);
        }
      }
      assert.equal(digests.size, 1, 'v2 digests differ at ' + size + ' sources');
    }
  } finally {
    await fsp.rm(buildDirectory, { recursive: true, force: true });
  }
} else {
  // Run the Node bundle: Bun does not update named builtin imports after instrumentation.
  // Proxies forward every real operation. Track raw source buffers from read completion
  // through hash.update, separately from the existing UTF-8 decode/token allocations.
  const counts = new Map<string, { sync: number; async: number }>();
  const pending = new Map<Buffer, number>();
  let liveBytes = 0;
  let peakBytes = 0;
  let scope = '';
  function record(file: unknown, kind: 'sync' | 'async'): boolean {
    if (typeof file !== 'string' || !file.startsWith(scope) || !file.endsWith('.rs')) return false;
    const count = counts.get(file) ?? { sync: 0, async: 0 };
    count[kind] += 1;
    counts.set(file, count);
    return true;
  }
  const originalSync = fs.readFileSync;
  const originalAsync = fsp.readFile;
  const originalHash = crypto.createHash;
  fs.readFileSync = new Proxy(originalSync, {
    apply(target, receiver, args) {
      record(args[0], 'sync');
      return Reflect.apply(target, receiver, args);
    },
  });
  fsp.readFile = new Proxy(originalAsync, {
    apply(target, receiver, args) {
      const tracked = record(args[0], 'async');
      return Reflect.apply(target, receiver, args).then((value: unknown) => {
        if (tracked && Buffer.isBuffer(value)) {
          pending.set(value, value.length);
          liveBytes += value.length;
          peakBytes = Math.max(peakBytes, liveBytes);
        }
        return value;
      });
    },
  });
  crypto.createHash = new Proxy(originalHash, {
    apply(target, receiver, args) {
      const hash = Reflect.apply(target, receiver, args);
      hash.update = new Proxy(hash.update, {
        apply(update, self, inputs) {
          const result = Reflect.apply(update, self, inputs);
          const bytes: unknown = inputs[0];
          if (Buffer.isBuffer(bytes)) {
            liveBytes -= pending.get(bytes) ?? 0;
            pending.delete(bytes);
          }
          return result;
        },
      });
      return hash;
    },
  });
  syncBuiltinESMExports();
  const root = await fsp.realpath(await fsp.mkdtemp(join(tmpdir(), 'cargo-source-read-')));
  try {
    for (const size of [Number(process.argv[3])]) {
      const workspace = join(root, `size-${size}`);
      const source = join(workspace, 'src');
      await fsp.mkdir(source, { recursive: true });
      await fsp.writeFile(
        join(workspace, 'Cargo.toml'),
        '[package]\nname="read-census"\nversion="0.1.0"\nedition="2021"\n[workspace]\n',
      );
      const prefix = 'const DATA: &[u8] = include_bytes!("payload.bin");\n//';
      const bytes = prefix + 'x'.repeat(65536 - prefix.length - 1) + '\n';
      for (let index = 0; index < size; index += 1) {
        await fsp.writeFile(join(source, index === 0 ? 'lib.rs' : `file-${index}.rs`), bytes);
      }
      await fsp.writeFile(join(source, 'payload.bin'), 'included-data');
      const manifest = join(workspace, 'Cargo.toml');
      execFileSync('cargo', ['generate-lockfile', '--offline', '--manifest-path', manifest], { stdio: 'pipe' });
      const expected = await hashCargoPathInputs(manifest, workspace, { includeWorkspace: true });
      scope = source;
      for (let trial = 0; trial < 7; trial += 1) {
        {
          counts.clear();
          peakBytes = 0;
          const start = performance.now();
          const digest = await hashCargoPathInputs(manifest, workspace, { includeWorkspace: true });
          const milliseconds = performance.now() - start;
          assert.equal(digest, expected);
          assert.equal(counts.size, size);
          assert.equal(pending.size, 0);
          const reads = [...counts.values()].reduce((sum, count) => sum + count.sync + count.async, 0);
          assert.equal(reads, size * (process.argv[2] === 'before' ? 2 : 1));
          assert.equal(peakBytes, 65536);
          const discoveryAndHashSources = [...counts.values()].filter(
            (count) => count.sync !== 0 && count.async !== 0,
          ).length;
          console.log(
            JSON.stringify({
              implementation: process.argv[2],
              sources: size,
              bytesPerSource: 65536,
              trial,
              milliseconds,
              reads,
              discoveryAndHashSources,
              peakRawSourceBytes: peakBytes,
              rawSourceBytesAfterHash: liveBytes,
              digest,
            }),
          );
        }
      }
    }
  } finally {
    fs.readFileSync = originalSync;
    fsp.readFile = originalAsync;
    crypto.createHash = originalHash;
    syncBuiltinESMExports();
    await fsp.rm(root, { recursive: true, force: true });
  }
}
