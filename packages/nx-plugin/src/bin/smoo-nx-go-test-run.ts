#!/usr/bin/env node
import { spawn, spawnSync } from 'node:child_process';
import { statSync } from 'node:fs';
import { availableParallelism } from 'node:os';
import type { Readable, Writable } from 'node:stream';
import { type GoTestBinary, readGoTestsManifest } from '../go-tests.js';

// Runs the test binaries smoo-nx-go-test-build listed in <go-tests.json> as `go test -json` runs a package's binary:
// in its package directory, under `go tool test2json -t -p <import path>`, with the flags go test gives it, up to -p
// binaries at once (default GOMAXPROCS, else one per core, as go test's -p). Each event line reaches stdout whole, so
// bounded-exec reads results, verdict and progress from it as from go test (GO_TEST_RUNNER); `-json` is accepted and
// implied. A manifest listing no binary is refused unless --allow-empty says none is expected, so a package pattern
// that matched no tests cannot pass. Exit 1 when any binary failed, 2 on a usage error or a manifest it cannot run.
const usage = 'usage: smoo-nx-go-test-run [-p=N] [-json] [--allow-empty] <go-tests.json>\n';
// What `go test -json` passes every test binary it runs, its default -timeout included.
const GO_TEST_BINARY_FLAGS = ['-test.v=test2json', '-test.paniconexit0', '-test.timeout=10m0s'];
const COUNT = /^[1-9]\d*$/;

function refuse(reason: string): never {
  process.stderr.write(`smoo-nx-go-test-run: ${reason}\n`);
  process.exit(2);
}

interface RunArguments {
  readonly packages: number | null;
  readonly allowEmpty: boolean;
  readonly manifest: string;
}

function parseArguments(words: readonly string[]): RunArguments | null {
  let packages: number | null = null;
  let allowEmpty = false;
  let manifest: string | null = null;
  for (let at = 0; at < words.length; at += 1) {
    const word = words[at] ?? '';
    const flag = /^-p(?:=(.*))?$/.exec(word);
    if (flag !== null) {
      let value = flag[1];
      if (value === undefined) {
        at += 1;
        value = words[at];
      }
      if (value === undefined || !COUNT.test(value)) return null;
      packages = Number(value);
    } else if (word === '--allow-empty') {
      allowEmpty = true;
    } else if (word === '-json' || word === '-json=true') {
      // Implied: test2json writes nothing else.
    } else if (word.startsWith('-') || word === '' || manifest !== null) {
      return null;
    } else {
      manifest = word;
    }
  }
  return manifest === null ? null : { packages, allowEmpty, manifest };
}

/** Every complete line `from` writes reaches `to` whole, so concurrent binaries' events never split a line. */
function forwardLines(from: Readable, to: Writable): void {
  let pending: Buffer = Buffer.alloc(0);
  from.on('data', (chunk: Buffer) => {
    const bytes = pending.length === 0 ? chunk : Buffer.concat([pending, chunk]);
    const end = bytes.lastIndexOf(10);
    if (end === -1) {
      pending = bytes;
      return;
    }
    to.write(bytes.subarray(0, end + 1));
    pending = bytes.subarray(end + 1);
  });
  from.on('end', () => {
    if (pending.length > 0) to.write(Buffer.concat([pending, Buffer.from('\n')]));
  });
}

/** Null when the binary passed, else how it ended. */
function runBinary(test2json: string, test: GoTestBinary): Promise<string | null> {
  // The plugin's lib target (ES2022) has no Promise.withResolvers.
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a test binary runs as long as its tests do; the bounded-exec run around this bin owns their progress bound and ends a hung one with its process group
  return new Promise((resolve) => {
    const child = spawn(test2json, ['-t', '-p', test.importPath, test.binary, ...GO_TEST_BINARY_FLAGS], {
      cwd: test.dir,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    forwardLines(child.stdout, process.stdout);
    forwardLines(child.stderr, process.stderr);
    child.on('error', (error) => resolve(`could not start: ${error.message}`));
    child.on('close', (code, signal) =>
      resolve(code === 0 ? null : signal === null ? `exit ${code}` : `killed by ${signal}`),
    );
  });
}

const parsed = parseArguments(process.argv.slice(2));
if (parsed === null) {
  process.stderr.write(usage);
  process.exit(2);
}
const read = readGoTestsManifest(parsed.manifest);
if (!read.ok) refuse(read.reason);
const tests = read.tests;
if (tests.length === 0) {
  if (parsed.allowEmpty) process.exit(0);
  refuse(
    `${parsed.manifest} lists no test binaries, as when no package pattern matched a test; --allow-empty expects that`,
  );
}
for (const test of tests) {
  if (statSync(test.binary, { throwIfNoEntry: false })?.isFile() !== true) {
    refuse(`${parsed.manifest} lists ${test.binary} for ${test.importPath}, which is no file; build it again`);
  }
  if (statSync(test.dir, { throwIfNoEntry: false })?.isDirectory() !== true) {
    refuse(`${parsed.manifest} runs ${test.importPath} in ${test.dir}, which is no directory`);
  }
}
// eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: go builds test2json into its cache on its first use, for as long as that compile takes; the bounded-exec run around this bin ends a hung one
const tool = spawnSync('go', ['tool', '-n', 'test2json'], { stdio: ['ignore', 'pipe', 'inherit'], encoding: 'utf8' });
if (tool.error !== undefined || tool.status !== 0) {
  refuse(`go tool -n test2json named no test2json: ${tool.error?.message ?? `exit ${tool.status ?? tool.signal}`}`);
}
const test2json = tool.stdout.trim();
const inherited = process.env.GOMAXPROCS;
const packages =
  parsed.packages ?? (inherited !== undefined && COUNT.test(inherited) ? Number(inherited) : availableParallelism());
const failures: string[] = [];
const queue = tests.values();
const worker = async (): Promise<void> => {
  for (const test of queue) {
    const failure = await runBinary(test2json, test);
    if (failure !== null) failures.push(`${test.importPath} (${failure})`);
  }
};
await Promise.all(Array.from({ length: Math.min(packages, tests.length) }, worker));
if (failures.length > 0) {
  failures.sort();
  process.stderr.write(
    `smoo-nx-go-test-run: ${failures.length} of ${tests.length} test binaries failed: ${failures.join(', ')}\n`,
  );
  process.exitCode = 1;
}
