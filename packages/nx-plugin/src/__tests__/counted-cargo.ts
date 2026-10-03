import { expect } from 'bun:test';
import { chmod, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { delimiter, join } from 'node:path';

import { AggregateCreateNodesError } from 'nx/src/project-graph/error-types.js';
import * as cargoSourceHash from '../cargo-source-hash.js';

/** What the log of one fixture's `cargo metadata` children says happened, in the order it happened. */
export interface CargoMetadataRuns {
  /** `cargo metadata` children started. */
  readonly spawned: number;
  /** Those whose Cargo has read the workspace, whether or not the answer has been handed over yet. */
  readonly read: number;
  /** The most that were alive at once; a child killed before it answered never counts as ended. */
  readonly peak: number;
  /** Each child's pid, in the order they started. */
  readonly pids: readonly number[];
  /** Processes the children left running behind them (see `leaveGrandchildren`). */
  readonly grandchildren: readonly number[];
}

/** The log is appended to in the order things happen, so a running count over it is the true concurrency. */
export function cargoMetadataRuns(log: string): CargoMetadataRuns {
  const pids: number[] = [];
  const grandchildren: number[] = [];
  let read = 0;
  let alive = 0;
  let peak = 0;
  for (const line of log.split('\n')) {
    const [event, pid] = line.split(' ');
    switch (event) {
      case 'start':
        pids.push(Number(pid));
        alive += 1;
        peak = Math.max(peak, alive);
        break;
      case 'read':
        read += 1;
        break;
      case 'end':
        alive -= 1;
        break;
      case 'grandchild':
        grandchildren.push(Number(pid));
        break;
    }
  }
  return { spawned: pids.length, read, peak, pids, grandchildren };
}

/** A gate a test closes to hold every `cargo metadata` child at one point of its life, and opens to let them go on. */
export interface Latch {
  close(): Promise<void>;
  open(): Promise<void>;
}

export interface CountedCargo {
  /** The directory holding the `cargo` that goes ahead of the real one. */
  readonly bin: string;
  /**
   * What a process under test needs: this Cargo ahead of everything else on PATH, and the Nx
   * workspace-data directory (where Cargo resolutions are kept between processes) inside the
   * fixture, so the persistent cache is the fixture's own and is deleted with it, whatever
   * `NX_WORKSPACE_DATA_DIRECTORY` the test run itself inherited.
   */
  readonly environment: Readonly<Record<string, string>>;
  /** Sleep this long, after Cargo has read the workspace, before the answer is handed over. */
  holdFor(seconds: number): Promise<void>;
  /** Hold every child before it reads the workspace: an edit made now is one its answer includes. */
  readonly beforeRead: Latch;
  /** Hold every child after it read the workspace: an edit made now is one its answer predates. */
  readonly beforeAnswer: Latch;
  /** While set, a child that has read answers with this on stderr and exit code 101 instead. */
  failWith(stderr: string): Promise<void>;
  recover(): Promise<void>;
  /** Whether every child leaves a background process running behind it, as a real Cargo does with rustc. */
  leaveGrandchildren(leave: boolean): Promise<void>;
  runs(): Promise<CargoMetadataRuns>;
  /** Open every latch, so a child a failed test left held can finish and nothing outlives the fixture. */
  release(): Promise<void>;
}

export interface CannedCargo extends CountedCargo {
  /** What `cargo metadata` prints from now on: this document, as JSON. */
  answerWith(metadata: unknown): Promise<void>;
  /** What `cargo metadata` prints from now on: this text, whatever it is. */
  answerWithText(text: string): Promise<void>;
}

const quote = (value: string): string => `'${value.replaceAll("'", `'"'"'`)}'`;

/**
 * A `cargo` ahead of the real one on PATH. Every subcommand but `metadata` runs the real Cargo
 * (`forward` is the command line that does). A `metadata` child logs `start` as it begins, `read`
 * once it has its answer, and `end` after the answer has been handed over, so the log orders each
 * child's life against every other's. `answer` is the command line that produces the answer in
 * `$answer` and leaves Cargo's exit code in `$?`.
 */
async function shim(
  root: string,
  answer: string,
  forward: string,
): Promise<{ cargo: CountedCargo; directory: string }> {
  const real = Bun.which('cargo');
  if (real === null) throw new Error('cargo is not on PATH');
  const directory = join(root, 'counted-cargo');
  const bin = join(directory, 'bin');
  const log = join(directory, 'log');
  const hold = join(directory, 'hold');
  const failure = join(directory, 'failure');
  const grandchild = join(directory, 'grandchild');
  await mkdir(bin, { recursive: true });
  await Promise.all([writeFile(log, ''), writeFile(hold, '0')]);
  await writeFile(
    join(bin, 'cargo'),
    [
      '#!/bin/sh',
      `real=${quote(real)}`,
      `dir=${quote(directory)}`,
      `if [ "$1" != metadata ]; then ${forward}; fi`,
      'echo "start $$" >> "$dir/log"',
      'while [ -e "$dir/before-read" ]; do sleep 0.01; done',
      'answer="$dir/answer.$$"',
      answer,
      'status=$?',
      // Separate files keep descendants from holding Cargo's pipes open while preserving every error.
      // The descendant lives while the fixture does and until `release`, never a fixed span: a test
      // that fails before proving it dead leaves a loop that ends the moment the fixture is deleted.
      'if [ -e "$dir/grandchild" ]; then',
      '  ( while [ -d "$dir" ] && [ ! -e "$dir/released" ]; do sleep 0.05; done ) > "$dir/grandchild.stdout" 2> "$dir/grandchild.stderr" &',
      '  echo "grandchild $!" >> "$dir/log"',
      'fi',
      'echo "read $$" >> "$dir/log"',
      'while [ -e "$dir/before-answer" ]; do sleep 0.01; done',
      'sleep "$(cat "$dir/hold")"',
      'if [ -s "$dir/failure" ]; then',
      '  cat "$dir/failure" >&2',
      '  rm -f "$answer"',
      '  echo "end $$" >> "$dir/log"',
      '  exit 101',
      'fi',
      'cat "$answer"',
      'rm -f "$answer"',
      'echo "end $$" >> "$dir/log"',
      'exit "$status"',
      '',
    ].join('\n'),
  );
  await chmod(join(bin, 'cargo'), 0o755);
  const latch = (name: string): Latch => ({
    close: () => writeFile(join(directory, name), ''),
    open: () => rm(join(directory, name), { force: true }),
  });
  const beforeRead = latch('before-read');
  const beforeAnswer = latch('before-answer');
  const cargo: CountedCargo = {
    bin,
    environment: {
      PATH: `${bin}${delimiter}${process.env.PATH ?? ''}`,
      NX_WORKSPACE_DATA_DIRECTORY: join(root, '.nx/workspace-data'),
    },
    holdFor: (seconds) => writeFile(hold, String(seconds)),
    beforeRead,
    beforeAnswer,
    failWith: (stderr) => writeFile(failure, stderr),
    recover: () => rm(failure, { force: true }),
    leaveGrandchildren: (leave) => (leave ? writeFile(grandchild, '') : rm(grandchild, { force: true })),
    runs: async () => cargoMetadataRuns(await readFile(log, 'utf8')),
    release: async () => {
      // The marker ends every descendant loop; the latches end every held child.
      await writeFile(join(directory, 'released'), '');
      await Promise.all([beforeRead.open(), beforeAnswer.open()]);
    },
  };
  return { cargo, directory };
}

/** The real Cargo, counted: `metadata` prints what the real `cargo metadata` prints. */
export async function countedCargo(root: string): Promise<CountedCargo> {
  const { cargo } = await shim(root, '"$real" "$@" > "$answer"', 'exec "$real" "$@"');
  return cargo;
}

/**
 * A Cargo whose `metadata` prints whatever the test says, whatever is on disk: the cache's own behavior,
 * alone. Its other subcommands run the real Cargo under no rustup selection, which the canned answers do
 * not depend on.
 */
export async function cannedCargo(root: string): Promise<CannedCargo> {
  const { cargo, directory } = await shim(
    root,
    'cat "$dir/metadata.json" > "$answer"',
    'exec env -u RUSTUP_TOOLCHAIN -u RUSTUP_HOME "$real" "$@"',
  );
  const metadata = join(directory, 'metadata.json');
  await writeFile(metadata, '');
  return {
    ...cargo,
    answerWith: (document) => writeFile(metadata, JSON.stringify(document)),
    answerWithText: (text) => writeFile(metadata, text),
  };
}

/** `body` with `overrides` in this process's environment, which the module's Cargo children inherit. */
export async function withEnvironment<T>(
  overrides: Readonly<Record<string, string>>,
  body: () => Promise<T>,
): Promise<T> {
  const saved = Object.keys(overrides).map((key): [string, string | undefined] => [key, process.env[key]]);
  Object.assign(process.env, overrides);
  try {
    return await body();
  } finally {
    for (const [key, value] of saved) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
}

/** The error `promise` rejects with; a promise that resolves fails the test. */
export async function rejectionOf(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (error) {
    return error;
  }
  throw new Error('expected a rejection, but the promise resolved');
}

/**
 * `error` is the typed refusal of a Cargo resolution: named `CargoMetadataError`, with the manifest it
 * was asked about and the failure underneath. The class is looked up by name, not imported, so that a
 * tree without it fails the assertions that need it instead of the import of every test file that
 * needs it.
 */
export function expectCargoMetadataError(error: unknown, manifestPath: string): Error {
  const type: unknown = Reflect.get(cargoSourceHash, 'CargoMetadataError');
  if (typeof type !== 'function') throw new Error('cargo-source-hash.ts exports no CargoMetadataError');
  expect(error).toBeInstanceOf(type);
  if (!(error instanceof Error)) throw new Error(`expected a CargoMetadataError, got ${String(error)}`);
  expect(error.name).toBe('CargoMetadataError');
  expect(Reflect.get(error, 'manifestPath')).toBe(manifestPath);
  expect(error.cause).toBeInstanceOf(Error);
  return error;
}

/** `failure` is the whole refusal of a project-graph computation: Nx's aggregate of the per-file errors. */
export function aggregateOf(failure: unknown): AggregateCreateNodesError {
  if (!(failure instanceof AggregateCreateNodesError)) {
    throw new Error(`expected the graph computation to fail as an aggregate, got ${String(failure)}`);
  }
  return failure;
}

/**
 * `failure` is a project-graph computation refused for want of a Cargo resolution: an aggregate whose
 * every error is the typed `CargoMetadataError` for the workspace manifest `manifestPath`, and which
 * carries no partial result a runtime hash of the whole workspace could have filled.
 */
export function expectGraphRefusal(failure: unknown, manifestPath: string): AggregateCreateNodesError {
  const aggregate = aggregateOf(failure);
  expect(aggregate.errors.length).toBeGreaterThan(0);
  for (const [, error] of aggregate.errors) expectCargoMetadataError(error, manifestPath);
  expect(JSON.stringify(aggregate.partialResults)).not.toContain('--include-workspace');
  return aggregate;
}
