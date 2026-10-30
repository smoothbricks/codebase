/// <reference types="bun" />

import { randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { isAbsolute, join, relative, resolve, sep } from 'node:path';
import { nativeToolCommand } from '../../nx-plugin/src/tool-bins.js';

/** The exact platform executable named by the nextest runners' command owner. */
export function extractorCommand(platform: NodeJS.Platform, architecture: string): string {
  const supported =
    (platform === 'darwin' || platform === 'linux') && (architecture === 'arm64' || architecture === 'x64');
  return nativeToolCommand(
    supported ? { platform: `${platform}-${architecture}` } : null,
    '.',
    'nextest-extract',
    'smoo-nx-nextest-extract',
  );
}

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** The archive's actual non-test executable, remapped into its extracted target directory. */
export function sweepExecutable(extraction: string, metadata: unknown): string {
  const build = record(metadata) ? metadata['rust-build-meta'] : undefined;
  const binaries = record(build) ? build['non-test-binaries'] : undefined;
  if (!record(binaries)) throw new Error('nextest archive has no non-test binary metadata');
  const paths: string[] = [];
  for (const entries of Object.values(binaries)) {
    if (!Array.isArray(entries)) throw new Error('nextest non-test binaries must be arrays');
    for (const entry of entries) {
      if (!record(entry) || entry.name !== 'cowshed-test-sweep' || entry.kind !== 'bin-exe') continue;
      if (typeof entry.path !== 'string' || isAbsolute(entry.path)) {
        throw new Error('cowshed-test-sweep needs an archive-relative executable path');
      }
      const target = resolve(extraction, 'target');
      const executable = resolve(target, entry.path);
      const within = relative(target, executable);
      if (within === '' || within === '..' || within.startsWith(`..${sep}`) || isAbsolute(within)) {
        throw new Error('cowshed-test-sweep escapes the extracted target directory');
      }
      paths.push(executable);
    }
  }
  if (paths.length !== 1) throw new Error('nextest archive must contain exactly one cowshed-test-sweep executable');
  const executable = paths[0];
  if (executable === undefined) throw new Error('missing cowshed-test-sweep executable');
  return executable;
}

/**
 * The real-image lane setup: the archive's `cowshed-test-sweep` sweeps dead runs to completion before the lane's
 * fixtures start. It is nextest's `apfs-sweep` setup script for the lanes that run under nextest (.config/nextest.toml)
 * and the first step of `cowshed:integration-apfs`, so a lane replayed from cache starts no fixture and sweeps
 * nothing. Off macOS there is nothing to sweep.
 */
async function main(): Promise<number> {
  if (process.platform !== 'darwin') return 0;
  const extraction = Bun.spawn([extractorCommand(process.platform, process.arch), '.cache/nextest/archive.tar.zst'], {
    stdin: 'inherit',
    stdout: 'pipe',
    stderr: 'inherit',
  });
  const output = await new Response(extraction.stdout).text();
  const extracted = await extraction.exited;
  if (extracted !== 0) return extracted;
  const directory = output.trim();
  if (directory === '') throw new Error('nextest extraction returned no directory');
  const metadata: unknown = JSON.parse(readFileSync(join(directory, 'target/nextest/binaries-metadata.json'), 'utf8'));
  const sweep = Bun.spawn([sweepExecutable(directory, metadata), randomUUID()], {
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
