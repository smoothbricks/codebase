#!/usr/bin/env node
import { findForbiddenSpellings, formatSpellingFinding, SPELLING_RULES } from '../forbidden-spelling-policy.js';
import { diffScope, upstreamBase } from '../test-walk-policy.js';

// FORBIDDEN-SPELLING over the current directory's repository, judged on the lines and files added since the upstream
// merge base (`upstreamBase`). Every finding prints `error[…]` and exits 1. Exit 2 when git cannot answer or an input
// cannot be inspected.
if (process.argv.length > 2) {
  process.stderr.write('usage: smoo-nx-forbidden-spellings\n');
  process.exit(2);
}
const root = process.cwd();
const base = upstreamBase(root);
if (!base.ok) {
  process.stderr.write(`FORBIDDEN-SPELLING cannot find the upstream base: ${base.reason}\n`);
  process.exit(2);
}
const scope = diffScope(root, base.value);
if (!scope.ok) {
  process.stderr.write(`FORBIDDEN-SPELLING cannot read the changes since ${base.value}: ${scope.reason}\n`);
  process.exit(2);
}
const scan = findForbiddenSpellings(root, scope.value);
if (scan.kind === 'unavailable') {
  process.stderr.write(`Cannot inspect FORBIDDEN-SPELLING input ${scan.file}: ${scan.detail}\n`);
  process.exit(2);
}
for (const finding of scan.findings) process.stderr.write(`${formatSpellingFinding(finding.file, finding)}\n`);
if (scan.findings.length > 0) process.exit(1);
process.stdout.write(
  `FORBIDDEN-SPELLING passed: no line or file added since the base spells ${Object.keys(SPELLING_RULES).join(', ')} ` +
    `(base ${base.value}).\n`,
);
