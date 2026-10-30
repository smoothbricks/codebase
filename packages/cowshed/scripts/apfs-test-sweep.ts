/// <reference types="bun" />

import { randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { isAbsolute, join, relative, resolve, sep } from 'node:path';
import { ensureInPlace, NEXTEST_BINARIES_METADATA } from '../../nextest-extract/src/nextest-binaries.js';

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** The test build's actual non-test executable, in this checkout's target directory. */
export function sweepExecutable(targetDirectory: string, metadata: unknown): string {
  const build = record(metadata) ? metadata['rust-build-meta'] : undefined;
  const binaries = record(build) ? build['non-test-binaries'] : undefined;
  if (!record(binaries)) throw new Error('the test build has no non-test binary metadata');
  const paths: string[] = [];
  for (const entries of Object.values(binaries)) {
    if (!Array.isArray(entries)) throw new Error('nextest non-test binaries must be arrays');
    for (const entry of entries) {
      if (!record(entry) || entry.name !== 'cowshed-test-sweep' || entry.kind !== 'bin-exe') continue;
      if (typeof entry.path !== 'string' || isAbsolute(entry.path)) {
        throw new Error('cowshed-test-sweep needs a target-relative executable path');
      }
      const executable = resolve(targetDirectory, entry.path);
      const within = relative(targetDirectory, executable);
      if (within === '' || within === '..' || within.startsWith(`..${sep}`) || isAbsolute(within)) {
        throw new Error('cowshed-test-sweep escapes the target directory');
      }
      paths.push(executable);
    }
  }
  if (paths.length !== 1) throw new Error('the test build must contain exactly one cowshed-test-sweep executable');
  const executable = paths[0];
  if (executable === undefined) throw new Error('missing cowshed-test-sweep executable');
  return executable;
}

/**
 * The real-image lane setup: the test build's `cowshed-test-sweep` sweeps dead runs to completion before the lane's
 * fixtures start. It is nextest's `apfs-sweep` setup script for the lanes that run under nextest (.config/nextest.toml)
 * and the first step of `cowshed:integration-apfs`, so a lane replayed from cache starts no fixture and sweeps
 * nothing. Off macOS there is nothing to sweep. It runs the executable where `cargo-test-build` left it, building it
 * first when this checkout's build was a cache hit that restored only the metadata.
 */
async function main(): Promise<number> {
  if (process.platform !== 'darwin') return 0;
  const checkout = process.cwd();
  const target = await ensureInPlace(
    checkout,
    `smoo:${join(checkout, 'node_modules/@smoothbricks/nx-plugin/nextest.toml')}`,
    (line) => console.error(`apfs-test-sweep: ${line}`),
  );
  const metadata: unknown = JSON.parse(readFileSync(join(checkout, NEXTEST_BINARIES_METADATA), 'utf8'));
  const sweep = Bun.spawn([sweepExecutable(target, metadata), randomUUID()], {
    stdin: 'inherit',
    stdout: 'inherit',
    stderr: 'inherit',
  });
  return await sweep.exited;
}

if (import.meta.main) {
  try {
    process.exitCode = await main();
  } catch (error) {
    console.error(`APFS lane setup refused: ${error instanceof Error ? error.message : String(error)}`);
    process.exitCode = 1;
  }
}
