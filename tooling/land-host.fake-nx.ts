#!/usr/bin/env bun
// A stand-in `nx` for land-host.test.ts: plays back the calls the test scripted, in order, and leaves behind what a
// real run leaves for tooling/land-host.sh to read: what the run prints, its exit status and, when the call names its
// rows, the run summary in Nx's cache directory. No report can override a failed gate.
//
// FIX_STATE names a directory holding `plan.json`, an array of calls. A call whose arguments are not the next
// scripted one, or one past the end of the plan, exits 97 or 98 and says so: the script under test did something the
// scenario did not expect. An expected argument ending in `*` matches any argument that starts the same way.
import { appendFileSync, existsSync, fstatSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import type { Call } from './land-host.plan.ts';

const state = process.env.FIX_STATE;
if (state === undefined) {
  throw new Error('FIX_STATE is not set');
}
const plan: readonly Call[] = JSON.parse(readFileSync(join(state, 'plan.json'), 'utf8'));
const counter = join(state, 'nx.count');
const index = existsSync(counter) ? Number(readFileSync(counter, 'utf8')) : 0;
const args = process.argv.slice(2);
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

if (call.requireFileOutput) {
  const stdout = fstatSync(1);
  const stderr = fstatSync(2);
  if (!stdout.isFile() || !stderr.isFile() || stdout.dev !== stderr.dev || stdout.ino !== stderr.ino) {
    process.stderr.write('fake nx: output must go directly to one regular capture file\n');
    process.exit(96);
  }
}

if (call.run !== undefined) {
  // The run summary Nx writes into its cache directory, the one the gate job names in NX_CACHE_DIRECTORY (the cowshed
  // stand-in names one, as the workspace supervisor does): what the packaged publisher and land-attribution.ts read.
  const cache = process.env.NX_CACHE_DIRECTORY;
  if (cache === undefined) {
    process.stderr.write('fake nx: a call that writes a run summary needs NX_CACHE_DIRECTORY\n');
    process.exit(95);
  }
  mkdirSync(cache, { recursive: true });
  writeFileSync(
    join(cache, 'run.json'),
    JSON.stringify({
      run: { command: `nx ${args.join(' ')}`, startTime: new Date().toISOString(), endTime: '', inner: false },
      tasks: call.run.map((row) => ({ ...row, cacheStatus: 'cache-miss' })),
    }),
  );
}

if (call.output !== undefined) {
  process.stdout.write(`${call.output}\n`);
}
process.exit(call.exit);
