#!/usr/bin/env node
import {
  describeRetention,
  describeRetentionError,
  refusedAnywhere,
  retainCargoWorkspace,
} from '../cargo-retention.js';

// The report goes to stdout as JSON; the one-line summary and every failure go to stderr.
// Exit 0: planned, reclaimed or busy. 3: a pass was refused in any root (a profile it cannot read, a lock it cannot
// attempt). 1: no pass ran. 2: usage.
const USAGE = 'usage: smoo-nx-cargo-retain [--apply] [cargo-workspace-dir]';
const args = process.argv.slice(2);
const apply = args[0] === '--apply';
const operands = apply ? args.slice(1) : args;
if (operands.length > 1 || operands.some((operand) => operand.startsWith('-'))) {
  process.stderr.write(`${USAGE}\n`);
  process.exit(2);
}
const result = await retainCargoWorkspace(operands[0] ?? process.cwd(), { apply });
if (!result.ok) {
  process.stderr.write(`smoo-nx-cargo-retain: ${describeRetentionError(result.error)}\n`);
  process.exit(1);
}
process.stdout.write(`${JSON.stringify(result.value, null, 2)}\n`);
process.stderr.write(`smoo-nx-cargo-retain: ${describeRetention(result.value)}\n`);
process.exitCode = refusedAnywhere(result.value) ? 3 : 0;
