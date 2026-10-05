#!/usr/bin/env node
import { realpathSync } from 'node:fs';
import { resolve } from 'node:path';
import { type CargoPathInputsOptions, hashCargoPathInputs } from '../cargo-source-hash.js';

/** Flags first, in any order and at most once each, then at most one manifest; anything else is refused. */
function parseArguments(args: readonly string[]): { manifest: string; options: CargoPathInputsOptions } | undefined {
  const options: CargoPathInputsOptions = {};
  let index = 0;
  for (; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === '--include-workspace' && options.includeWorkspace === undefined) {
      options.includeWorkspace = true;
    } else if (arg === '--closure' && options.closure === undefined) {
      const directory = args[index + 1];
      if (directory === undefined || directory.startsWith('-')) return undefined;
      options.closure = directory;
      index += 1;
    } else if (arg?.startsWith('-')) {
      return undefined;
    } else {
      break;
    }
  }
  const rest = args.slice(index);
  if (rest.length > 1) return undefined;
  return { manifest: rest[0] ?? 'Cargo.toml', options };
}

const parsed = parseArguments(process.argv.slice(2));
if (parsed === undefined) {
  process.stdout.write('cargo-input-invalid-arguments\n');
  process.exit(2);
}
try {
  process.stdout.write(`${await hashCargoPathInputs(resolve(parsed.manifest), process.cwd(), parsed.options)}\n`);
} catch (error) {
  // Nx hashes stdout AND stderr even when a runtime input exits nonzero, so
  // both must be the same bytes in every checkout of one tree: stdout is a
  // fixed sentinel, and stderr is the cause with the checkout written as a
  // relative path. The cause still reaches whoever reads the failed input
  // (a dev watcher that reports failed inputs), instead of only the Cargo producer
  // learning it when it runs.
  process.stdout.write('cargo-input-unavailable\n');
  process.stderr.write(
    `smoo-nx-cargo-hash: ${checkoutRelative(error instanceof Error ? error.message : String(error))}\n`,
  );
  process.exitCode = 1;
}

/**
 * The cause without the checkout's absolute path or Cargo's lock-contention
 * chatter, which repeats a timing-dependent number of times. The longest
 * spelling of the checkout goes first: macOS's /private/tmp/x contains the
 * /tmp/x a shell reports as its cwd.
 */
function checkoutRelative(cause: string): string {
  const cwd = process.cwd();
  const roots = [...new Set([cwd, realpathSync(cwd)])].sort((left, right) => right.length - left.length);
  let relative = cause;
  for (const root of roots) relative = relative.split(`${root}/`).join('').split(root).join('.');
  return relative
    .split('\n')
    .filter((line) => !/^\s*Blocking waiting for file lock\b/.test(line))
    .join('\n')
    .trim();
}
