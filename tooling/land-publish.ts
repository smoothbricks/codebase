// The land's publish of the gate's Nx results. A land publishes only after its gate passed, so the localhost Nx cache
// holds only task results a verified tree produced. Sheds read that cache and never write it (05_gateway.md,
// "/nx/v1/cache/{hash}", accepted-project Nx artifacts; Danny's ruling, tg#1782: every shed reads the local remote
// cache, and only a land publishes). land-host.sh runs this twice: 'hashes' right after the gate passes, while the Nx
// run summary still belongs to the gate, and 'publish' once the Linux cross check passes.
//
//   land-publish.ts hashes RUN_JSON           print the distinct task hashes of the run's succeeded tasks
//   land-publish.ts publish CHECKOUT HASHES   store those tasks' results with the build volume's nx_publish.js

import { spawn } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { dirname, isAbsolute, join } from 'node:path';

const USAGE = 'usage: land-publish.ts hashes RUN_JSON | publish CHECKOUT HASHES_JSON\n';

/** A task hash as the gateway's cache route takes it (05_gateway.md). */
const TASK_HASH = /^[A-Za-z0-9_-]{1,128}$/;

/** The build volume that holds the one publisher. The land runs it; it never reimplements it. */
const BUILD_VOLUME = join(dirname(import.meta.dir), 'packages/cowshed/crates/cowshed-core/src/build_volume');

/** A refusal says why: a malformed summary or hash list is never guessed at. */
export type HashList =
  | { readonly kind: 'hashes'; readonly hashes: readonly string[] }
  | { readonly kind: 'refused'; readonly why: string };

const refusal = (why: string): HashList => ({ kind: 'refused', why });

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isTaskHash(value: unknown): value is string {
  return typeof value === 'string' && TASK_HASH.test(value);
}

/** The distinct hashes of the run's succeeded tasks (status 0), in run order. Every task must be well formed, failed or not. */
export function succeededHashes(summaryJson: string): HashList {
  let summary: unknown;
  try {
    summary = JSON.parse(summaryJson);
  } catch (error) {
    return refusal(`the run summary is not JSON: ${errorMessage(error)}`);
  }
  if (!isRecord(summary) || !isRecord(summary.run) || !Array.isArray(summary.tasks)) {
    return refusal('the run summary has no run object or no tasks array');
  }
  const tasks: readonly unknown[] = summary.tasks;
  const hashes = new Set<string>();
  for (const task of tasks) {
    if (!isRecord(task) || typeof task.status !== 'number') {
      return refusal(`the run summary has a task without a numeric status: ${JSON.stringify(task)}`);
    }
    if (!isTaskHash(task.hash)) {
      return refusal(`the run summary has an invalid task hash: ${JSON.stringify(task.hash)}`);
    }
    if (task.status === 0) hashes.add(task.hash);
  }
  return { kind: 'hashes', hashes: [...hashes] };
}

/** The hash list 'hashes' printed: a JSON array of distinct, well-formed task hashes. 'publish' checks it before spawning. */
export function hashList(text: string): HashList {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch (error) {
    return refusal(`the hash list is not JSON: ${errorMessage(error)}`);
  }
  if (!Array.isArray(value) || !value.every(isTaskHash)) {
    return refusal('the hash list is not a JSON array of valid task hashes');
  }
  if (new Set(value).size !== value.length) return refusal('the hash list repeats a task hash');
  return { kind: 'hashes', hashes: value };
}

/** cache_adapter.js first: it defines the cache nx_publish.js calls. Both go to 'node -' as one program. */
function bridgeProgram(): string {
  const adapter = readFileSync(join(BUILD_VOLUME, 'cache_adapter.js'), 'utf8');
  const publisher = readFileSync(join(BUILD_VOLUME, 'nx_publish.js'), 'utf8');
  return `${adapter}\n${publisher}`;
}

/** Runs the bridge as 'node - ARGS' with the program on stdin. Its stderr is ours; its stdout comes back. */
function runBridge(args: readonly string[]): Promise<{ readonly status: number; readonly stdout: string }> {
  // Read before spawning: a read that threw after the spawn would leave node waiting on a stdin that never closes.
  const program = bridgeProgram();
  return new Promise((resolve, reject) => {
    const child = spawn('node', ['-', ...args], { stdio: ['pipe', 'pipe', 'inherit'] });
    let stdout = '';
    child.stdout.setEncoding('utf8');
    child.stdout.on('data', (chunk: string) => {
      stdout += chunk;
    });
    child.on('error', reject);
    child.on('close', (status, signal) => {
      if (status === null) reject(new Error(`node ended by signal ${String(signal)}`));
      else resolve({ status, stdout });
    });
    child.stdin.on('error', reject);
    child.stdin.end(program);
  });
}

function fail(why: string): number {
  process.stderr.write(`land-publish: ${why}\n`);
  return 1;
}

async function main(argv: readonly string[]): Promise<number> {
  const [verb, first, second] = argv;
  if (verb === 'hashes' && argv.length === 2 && first !== undefined) {
    const result = succeededHashes(readFileSync(first, 'utf8'));
    if (result.kind === 'refused') return fail(result.why);
    process.stdout.write(`${JSON.stringify(result.hashes)}\n`);
    return 0;
  }
  if (verb === 'publish' && argv.length === 3 && first !== undefined && second !== undefined) {
    if (!isAbsolute(first)) return fail(`the checkout must be an absolute path, not ${first}`);
    const list = hashList(readFileSync(second, 'utf8'));
    if (list.kind === 'refused') return fail(list.why);
    const bridge = await runBridge([first, JSON.stringify(list.hashes)]);
    process.stdout.write(bridge.stdout);
    return bridge.status;
  }
  process.stderr.write(USAGE);
  return 64;
}

if (import.meta.main) {
  try {
    process.exitCode = await main(process.argv.slice(2));
  } catch (error) {
    process.exitCode = fail(errorMessage(error));
  }
}
