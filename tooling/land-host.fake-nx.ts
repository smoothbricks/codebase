#!/usr/bin/env bun
// A stand-in `nx` for land-host.test.ts: plays back the calls the test scripted, in order, and leaves behind what a
// real run leaves for tooling/land-host.sh to read in the working directory (the workspace root): what the run prints,
// and the bounded-exec verdict and report of each task it ran.
//
// FIX_STATE names a directory holding `plan.json`, an array of calls. A call whose arguments are not the next
// scripted one, or one past the end of the plan, exits 97 or 98 and says so: the script under test did something the
// scenario did not expect. An expected argument ending in `*` matches any argument that starts the same way.
import { appendFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import type { Call } from './land-host.plan.ts';

const state = process.env.FIX_STATE;
if (state === undefined) {
  throw new Error('FIX_STATE is not set');
}
const plan: readonly Call[] = JSON.parse(readFileSync(join(state, 'plan.json'), 'utf8'));
const counter = join(state, 'nx.count');
const index = existsSync(counter) ? Number(readFileSync(counter, 'utf8')) : 0;
const args = process.argv.slice(2).filter((arg) => !arg.startsWith('--outputStyle='));
appendFileSync(join(state, 'nx.log'), `${args.join(' ')}\n`);

const call = plan[index];
if (call === undefined) {
  process.stderr.write(`fake nx: unexpected call ${index + 1}: nx ${args.join(' ')}\n`);
  process.exit(98);
}
const matches =
  call.args.length === args.length &&
  call.args.every((expected, at) =>
    expected.endsWith('*') ? (args[at] ?? '').startsWith(expected.slice(0, -1)) : expected === args[at],
  );
if (!matches) {
  process.stderr.write(`fake nx: call ${index + 1} was nx ${args.join(' ')}, scripted nx ${call.args.join(' ')}\n`);
  process.exit(97);
}
writeFileSync(counter, String(index + 1));

const graphFile = args.find((arg) => arg.startsWith('--graph='))?.slice('--graph='.length);
if (call.graph !== undefined && graphFile !== undefined) {
  writeFileSync(graphFile, JSON.stringify(call.graph));
}
for (const { task, record, report } of call.records ?? []) {
  const directory = join('.nx/workspace-data/bounded-exec', encodeURIComponent(task));
  mkdirSync(directory, { recursive: true });
  writeFileSync(join(directory, 'verdict.json'), JSON.stringify(record));
  if (report !== undefined) {
    writeFileSync(join(directory, 'report.xml'), report);
  }
}
if (call.output !== undefined) {
  process.stdout.write(`${call.output}\n`);
}
process.exit(call.exit);
