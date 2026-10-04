#!/usr/bin/env node
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
} catch {
  // Nx hashes stderr even when a runtime input exits nonzero. Only a fixed
  // sentinel belongs in this input; the Cargo producer reports the actual
  // missing dependency or invalid manifest when it runs.
  process.stdout.write('cargo-input-unavailable\n');
  process.exitCode = 1;
}
