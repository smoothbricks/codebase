import { createHash } from 'node:crypto';
import { mkdir, rename, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { cacheDir, logger } from 'nx/src/devkit-exports.js';
import type { PostTasksExecutionContext, PreTasksExecutionContext } from 'nx/src/project-graph/plugins/public-api.js';
import type { TaskResult } from 'nx/src/tasks-runner/life-cycle.js';
import typia, { type tags } from 'typia';

/** A controller-selected hint namespace, not publication authority or a backend credential. */
export const NX_TASK_RECEIPT_ENV = 'COWSHED_NX_TASK_RECEIPT_ID';
type RequestId = string & tags.Pattern<'^[0-9a-f]{32}$'>;
const isRequestId = typia.createIs<RequestId>();
interface TaskHint {
  readonly task: Pick<TaskResult['task'], 'id' | 'hash'>;
  readonly status: TaskResult['status'];
  readonly code: TaskResult['code'];
}
interface ResultHint {
  readonly version: 1;
  readonly requestId: RequestId;
  readonly workspaceRoot: PostTasksExecutionContext['workspaceRoot'];
  readonly run: Pick<PostTasksExecutionContext, 'id' | 'argv' | 'startTime' | 'endTime'>;
  readonly tasks: readonly TaskHint[];
}
type WriteOutcome = { readonly ok: true } | { readonly ok: false; readonly path: string; readonly reason: string };
const WRITTEN: WriteOutcome = { ok: true };

function requestId(): RequestId | undefined {
  const value = process.env[NX_TASK_RECEIPT_ENV];
  return isRequestId(value) ? value : undefined;
}
export function preTasksExecution(_options: unknown, _context: PreTasksExecutionContext): void {
  const requested = process.env[NX_TASK_RECEIPT_ENV];
  if (requested !== undefined && !isRequestId(requested)) {
    logger.warn(
      `${NX_TASK_RECEIPT_ENV} is not a controller-selected 32-hex request id; no task hint will be recorded.`,
    );
  }
}
async function writeHint(hint: ResultHint): Promise<WriteOutcome> {
  const directory = join(cacheDir, '.cowshed-task-results', hint.requestId);
  const invocation = createHash('sha256').update(hint.run.id).digest('hex');
  const destination = join(directory, `${invocation}.json`);
  const temporary = join(directory, `.${invocation}-${process.pid}.tmp`);
  const json = JSON.stringify(hint);
  if (Buffer.byteLength(json) > 1024 * 1024) {
    return { ok: false, path: destination, reason: 'task-results hint exceeds the 1 MiB inspection envelope' };
  }
  try {
    await mkdir(directory, { recursive: true, mode: 0o700 });
    await writeFile(temporary, json, { mode: 0o600, flag: 'wx' });
    await rename(temporary, destination);
    return WRITTEN;
  } catch (cause) {
    // A leftover temporary makes the private inspector refuse the whole namespace; it can
    // never be mistaken for a successful hint. The workspace's own retirement owns the bytes.
    return { ok: false, path: destination, reason: String(cause) };
  }
}
export async function postTasksExecution(_options: unknown, context: PostTasksExecutionContext): Promise<void> {
  const id = requestId();
  if (id === undefined) return;
  const results = Object.values(context.taskResults);
  // Hash-only API inspections pair the hooks too but ran no task; never overwrite real hints.
  if (results.length === 0) return;
  const hint: ResultHint = {
    version: 1,
    requestId: id,
    workspaceRoot: context.workspaceRoot,
    run: { id: context.id, argv: context.argv, startTime: context.startTime, endTime: context.endTime },
    tasks: results.map(({ task, status, code }) => ({ task: { id: task.id, hash: task.hash }, status, code })),
  };
  const written = await writeHint(hint);
  if (!written.ok) logger.warn(`Cannot record Nx task-results hint ${written.path}: ${written.reason}`);
}
