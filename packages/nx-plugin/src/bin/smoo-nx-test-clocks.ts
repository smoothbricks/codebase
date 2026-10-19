#!/usr/bin/env node
import { findClockOracles, formatClockOracle, TEST_CLOCK_ORACLE } from '../test-clock-policy.js';
import { diffScope, upstreamBase } from '../test-walk-policy.js';

// TEST-CLOCK-ORACLE over the current directory's repository, judged on the cases changed since the upstream merge base
// (`upstreamBase`, the base `smoo-nx-test-walks --base` prints for the lint target's runtime input). Every finding
// prints `error[…]` and exits 1. Exit 2 when git cannot answer.
if (process.argv.length > 2) {
  process.stderr.write('usage: smoo-nx-test-clocks\n');
  process.exit(2);
}
const root = process.cwd();
const base = upstreamBase(root);
if (!base.ok) {
  process.stderr.write(`${TEST_CLOCK_ORACLE.id} cannot find the upstream base: ${base.reason}\n`);
  process.exit(2);
}
const scope = diffScope(root, base.value);
if (!scope.ok) {
  process.stderr.write(`${TEST_CLOCK_ORACLE.id} cannot read the changes since ${base.value}: ${scope.reason}\n`);
  process.exit(2);
}
const oracles = findClockOracles(root, scope.value.files, scope.value.changes);
for (const oracle of oracles) process.stderr.write(`${formatClockOracle(oracle)}\n`);
if (oracles.length > 0) process.exit(1);
process.stdout.write(
  `${TEST_CLOCK_ORACLE.id} passed: no changed test case carries a runner deadline, bounds elapsed host time or races ` +
    `a configured bound against a timer (base ${base.value}).\n`,
);
