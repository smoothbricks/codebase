#!/usr/bin/env node
import { readFileSync, writeFileSync } from 'node:fs';

import { junitCaseTimes } from '../executors/bounded-exec/verdict.js';
import { measuredTimings, NEXTEST_TIMED_MIN_MS, NEXTEST_TIMINGS_PATH, readNextestTimings } from '../nextest-timings.js';

// Rewrites one crate's entry of the cargo workspace's nextest timing manifest (the current directory's
// .config/nextest-timings.json) from the JUnit reports of runs that together ran the crate's tests once,
// such as every shard of one gate: bounded-exec keeps each target's last report at
// .nx/workspace-data/bounded-exec/<project>%3A<target>/report.xml.
const usage = 'usage: smoo-nx-nextest-timings <crate> <report.xml>...\n';
const [crate, ...reports] = process.argv.slice(2);
if (crate === undefined || crate.startsWith('-') || reports.length === 0) {
  process.stderr.write(usage);
  process.exit(2);
}
const cases = reports.flatMap((report) => junitCaseTimes(readFileSync(report, 'utf-8')));
const timed = measuredTimings(cases, crate, NEXTEST_TIMED_MIN_MS);
const manifest = new Map(Object.entries(readNextestTimings(NEXTEST_TIMINGS_PATH)));
const previous = manifest.get(crate);
if (Object.keys(timed).length === 0) {
  manifest.delete(crate);
} else {
  manifest.set(crate, timed);
}
const sorted = [...manifest].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
writeFileSync(NEXTEST_TIMINGS_PATH, `${JSON.stringify(Object.fromEntries(sorted), null, 2)}\n`);
process.stderr.write(
  `smoo-nx-nextest-timings: ${crate}: ${Object.keys(timed).length} tests of at least ${NEXTEST_TIMED_MIN_MS} ms ` +
    `(was ${previous === undefined ? 0 : Object.keys(previous).length}) from ${cases.length} timed cases in ${reports.length} reports\n`,
);
