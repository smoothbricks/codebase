#!/usr/bin/env node
import { resolve } from 'node:path';
import { hashCargoPathInputs } from '../cargo-source-hash.js';

const args = process.argv.slice(2);
if (args.length > 1) {
  process.stderr.write('usage: smoo-nx-cargo-hash [Cargo.toml]\n');
  process.exit(2);
}
try {
  process.stdout.write(`${await hashCargoPathInputs(resolve(args[0] ?? 'Cargo.toml'), process.cwd())}\n`);
} catch (error) {
  process.stderr.write(`smoo-nx-cargo-hash: ${error instanceof Error ? error.message : String(error)}\n`);
  process.stderr.write(
    'Restore the declared Cargo path dependencies and locked offline dependency cache, then rerun the Nx target.\n',
  );
  process.exitCode = 1;
}
