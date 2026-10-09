#!/usr/bin/env node
import {
  diffScope,
  findDuplicateScenarios,
  formatDuplicateScenario,
  REFUSES,
  TEST_ONE_WALK,
  upstreamBase,
} from '../test-walk-policy.js';

// TEST-ONE-WALK over the current directory's repository, judged on the cases changed since the upstream merge base
// (`upstreamBase`). `--base` prints only that base, for an Nx runtime input: the verdict depends on it and no file
// hash names it. Refusing kinds print `error[…]` and exit 1; advisory kinds print `warning[…]` for the writer and
// reviewer and leave the exit at 0. Exit 2 when git cannot answer.
const args = process.argv.slice(2);
if (args.length > 1 || (args.length === 1 && args[0] !== '--base')) {
  process.stderr.write('usage: smoo-nx-test-walks [--base]\n');
  process.exit(2);
}
const root = process.cwd();
const base = upstreamBase(root);
if (!base.ok) {
  process.stderr.write(`${TEST_ONE_WALK.id} cannot find the upstream base: ${base.reason}\n`);
  process.exit(2);
}
if (args[0] === '--base') {
  process.stdout.write(`${base.value}\n`);
  process.exit(0);
}
const scope = diffScope(root, base.value);
if (!scope.ok) {
  process.stderr.write(`${TEST_ONE_WALK.id} cannot read the changes since ${base.value}: ${scope.reason}\n`);
  process.exit(2);
}
const duplicates = findDuplicateScenarios(root, scope.value.files, scope.value.changes);
for (const duplicate of duplicates) process.stderr.write(`${formatDuplicateScenario(duplicate)}\n`);
const refused = duplicates.filter((duplicate) => REFUSES[duplicate.kind]).length;
if (refused > 0) process.exit(1);
process.stdout.write(
  `${TEST_ONE_WALK.id} passed: no changed test case repeats a sibling's path (base ${base.value}, ` +
    `${duplicates.length} advisory).\n`,
);
