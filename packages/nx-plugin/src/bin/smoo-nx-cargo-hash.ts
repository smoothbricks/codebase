#!/usr/bin/env node
import { resolve } from 'node:path';
import { hashCargoPathInputs } from '../cargo-source-hash.js';

const args = process.argv.slice(2);
const includeWorkspace = args[0] === '--include-workspace';
const manifest = args[includeWorkspace ? 1 : 0];
if (args.length > (includeWorkspace ? 2 : 1) || manifest?.startsWith('-')) {
  process.stderr.write('usage: smoo-nx-cargo-hash [--include-workspace] [Cargo.toml]\n');
  process.exit(2);
}
try {
  process.stdout.write(
    `${await hashCargoPathInputs(resolve(manifest ?? 'Cargo.toml'), process.cwd(), { includeWorkspace })}\n`,
  );
} catch (error) {
  process.stderr.write(`smoo-nx-cargo-hash: ${error instanceof Error ? error.message : String(error)}\n`);
  process.stderr.write(
    'Restore the declared Cargo path dependencies and locked offline dependency cache, then rerun the Nx target.\n',
  );
  process.exitCode = 1;
}
