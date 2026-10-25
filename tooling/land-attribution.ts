/// <reference types="bun" />
// LAND-ATTRIBUTION: which commits of a red land change the inputs of each task its gate failed.
//
// The land's gate (tooling/land-host.sh) is one Nx invocation, and Nx writes its run summary, `run.json` in Nx's cache
// directory: one row for every task that reached a life cycle, with its exit code. A task Nx skipped because a task it
// waits for failed reaches none and has no row: it did not fail. `failed` reads that summary, refusing one an earlier
// invocation wrote (its `run.startTime` precedes the second the gate began in), and prints the tasks that failed.
//
// `attribute` names, for each failed task, the commits of the land (base..head) whose changed paths are among the files
// Nx hashes for that task or for a task it waits for (tooling/land-inputs.mts lists them, for the failed tasks alone):
// the commits to read first. A path a commit deleted is no file of the tree that failed, so a commit that only deletes
// inputs is not named. A failed task no commit reaches fails on what the land shares with main or on an input that is
// no workspace file (an environment variable, a runtime command, an external package).
//
//   bun tooling/land-attribution.ts failed --since EPOCH_SECONDS
//       The id of each task that failed in the run summary Nx wrote for the invocation that began at EPOCH_SECONDS.
//   bun tooling/land-attribution.ts attribute --base B --head H --inputs FILE
//       Failed task ids on stdin, one per line; one line per task naming the commits of B..H that change its inputs.
import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { cacheDir } from 'nx/src/utils/cache-directory.js';
import { isRecord, staleSummary } from './land-publish.ts';

/** A file read as what it claims to be, or the reason it is not. Nothing malformed is read as empty. */
export type Reading<T> =
  | { readonly kind: 'read'; readonly value: T }
  | { readonly kind: 'refused'; readonly why: string };

const refused = (why: string): { readonly kind: 'refused'; readonly why: string } => ({ kind: 'refused', why });

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function json(text: string, what: string): Reading<unknown> {
  try {
    return { kind: 'read', value: JSON.parse(text) };
  } catch (error) {
    return refused(`${what} is not JSON: ${message(error)}`);
  }
}

/**
 * The tasks that failed in a run summary Nx began writing at or after `since` (epoch milliseconds), sorted. Every row
 * must carry a task id and a numeric exit code, failed or not; a row without a hash is a task whose hashing failed.
 */
export function failedTasks(summaryJson: string, since: number): Reading<string[]> {
  const summary = json(summaryJson, 'the run summary');
  if (summary.kind === 'refused') return summary;
  if (!isRecord(summary.value) || !isRecord(summary.value.run) || !Array.isArray(summary.value.tasks)) {
    return refused('the run summary has no run object or no tasks array');
  }
  const stale = staleSummary(summary.value.run, since);
  if (stale !== undefined) return refused(stale);
  const failed = new Set<string>();
  for (const row of summary.value.tasks) {
    if (!isRecord(row) || typeof row.taskId !== 'string' || typeof row.status !== 'number') {
      return refused(`the run summary has a task without a task id or a numeric status: ${JSON.stringify(row)}`);
    }
    if (row.status !== 0) failed.add(row.taskId);
  }
  return { kind: 'read', value: [...failed].sort() };
}

/** The workspace files Nx hashes for each failed task and the tasks it waits for (tooling/land-inputs.mts). */
export type TaskInputs = Readonly<Record<string, readonly string[]>>;

export function parseInputs(text: string): Reading<TaskInputs> {
  const parsed = json(text, 'the task inputs');
  if (parsed.kind === 'refused') return parsed;
  if (!isRecord(parsed.value)) return refused('the task inputs are not an object of file lists');
  const inputs: Record<string, readonly string[]> = {};
  for (const [id, files] of Object.entries(parsed.value)) {
    if (!Array.isArray(files) || !files.every((file) => typeof file === 'string')) {
      return refused(`the task inputs of ${id} are not a list of paths: ${JSON.stringify(files)}`);
    }
    inputs[id] = files;
  }
  return { kind: 'read', value: inputs };
}

/** One commit of a land, with every path it changes (deleted ones too). */
export interface Commit {
  readonly sha: string;
  readonly subject: string;
  readonly paths: readonly string[];
}

/** A failed task and the commits that change its inputs, in the land's order. */
export interface Attribution {
  readonly task: string;
  readonly commits: readonly Commit[];
}

/** Each failed task once, sorted, with the commits whose changed paths are among its inputs. */
export function attribute(commits: readonly Commit[], inputs: TaskInputs, failed: readonly string[]): Attribution[] {
  return [...new Set(failed)].sort().map((task) => {
    const files = inputs[task];
    if (files === undefined) throw new Error(`land-attribution: the task inputs hold no files for ${task}`);
    const hashed = new Set(files);
    return { task, commits: commits.filter((commit) => commit.paths.some((path) => hashed.has(path))) };
  });
}

export function renderAttribution(attributions: readonly Attribution[], total: number): string {
  return attributions
    .map(({ task, commits }) => {
      if (commits.length === 0) {
        return `${task} failed; none of the land's ${total} commits changes a file Nx hashes for it or for a task it waits for (a commit that only deletes files is not seen): it fails on what the land shares with main, or on an input that is no workspace file\n`;
      }
      const named = commits.map((commit) => `${commit.sha.slice(0, 10)} ${commit.subject}`).join('; ');
      return `${task} failed; ${commits.length} of the land's ${total} commits change its inputs: ${named}\n`;
    })
    .join('');
}

function git(args: readonly string[]): string {
  const run = spawnSync('git', [...args], { encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
  if (run.status !== 0) {
    throw new Error(`land-attribution: git ${args.join(' ')} failed: ${run.error?.message ?? run.stderr.trim()}`);
  }
  return run.stdout;
}

/** The commits of base..head, oldest first, with the paths each changes. */
function landCommits(base: string, head: string): Commit[] {
  return git(['log', '--reverse', '--format=%H%x09%s', `${base}..${head}`])
    .split('\n')
    .filter((line) => line !== '')
    .map((line) => {
      const tab = line.indexOf('\t');
      const sha = line.slice(0, tab);
      const paths = git(['diff-tree', '-r', '--no-commit-id', '--name-only', '-z', '--no-renames', sha])
        .split('\0')
        .filter((path) => path !== '');
      return { sha, subject: line.slice(tab + 1), paths };
    });
}

function option(args: readonly string[], name: string): string {
  const at = args.indexOf(name);
  const value = at < 0 ? undefined : args[at + 1];
  if (value === undefined) throw new Error(`land-attribution: ${args[0] ?? 'the command'} needs ${name}`);
  return value;
}

function read<T>(path: string, reading: Reading<T>): T {
  if (reading.kind === 'refused') throw new Error(`land-attribution: ${path}: ${reading.why}`);
  return reading.value;
}

async function main(args: readonly string[]): Promise<void> {
  const [command] = args;
  if (command === 'failed') {
    const since = Number(option(args, '--since'));
    if (!Number.isInteger(since)) throw new Error('land-attribution: --since takes whole epoch seconds');
    const summary = join(cacheDir, 'run.json');
    if (!existsSync(summary)) throw new Error(`land-attribution: Nx wrote no run summary at ${summary}`);
    const failed = read(summary, failedTasks(readFileSync(summary, 'utf8'), since * 1000));
    process.stdout.write(failed.map((task) => `${task}\n`).join(''));
    return;
  }
  if (command === 'attribute') {
    const path = option(args, '--inputs');
    const inputs = read(path, parseInputs(readFileSync(path, 'utf8')));
    const commits = landCommits(option(args, '--base'), option(args, '--head'));
    const failed = (await Bun.stdin.text()).split('\n').filter((task) => task !== '');
    process.stdout.write(renderAttribution(attribute(commits, inputs, failed), commits.length));
    return;
  }
  throw new Error('usage: land-attribution.ts failed --since EPOCH | attribute --base B --head H --inputs FILE');
}

if (import.meta.main) {
  try {
    await main(process.argv.slice(2));
  } catch (error) {
    console.error(message(error));
    process.exit(1);
  }
}
