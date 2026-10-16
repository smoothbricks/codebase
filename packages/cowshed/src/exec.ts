import typia from 'typia';
import type * as Api from './api.generated.js';
import * as N from './native.generated.js';
import type { NativeJobHandle } from './native.js';
import type { ExecRequest } from './types.js';

const assertExecRequest = typia.createAssertEquals<ExecRequest>();
const utf8 = new TextEncoder();

/**
 * The arguments and inline-stdin frame an exec sends. The request is checked exactly before anything is sent:
 * naming both `argv` and `script`, both `stdin` and `stdinWorkspacePath`, or any field the
 * request does not declare is refused. Every generated option field travels as the caller gave
 * it; only the command, stdin and the defaults `ExecOptions` documents are adapted — an omitted
 * option is absent (`null`), an omitted mode runs read-write, an omitted environment is empty —
 * and inline stdin travels as the upload frame.
 */
function execFrame(session: string | null, request: ExecRequest): [N.WorkerExecArguments, Uint8Array | undefined] {
  const { argv, script, stdin, stdinWorkspacePath, ...options } = assertExecRequest(request);
  const fields = {
    ...options,
    ...(argv !== undefined ? { argv: argv.map((data): Api.CommandArg => ({ encoding: 'utf8', data })) } : { script }),
    session,
    cwd: options.cwd ?? null,
    mode: options.mode ?? 'readWrite',
    env: options.env ?? {},
    trace: options.trace ?? null,
    stdoutCopy: options.stdoutCopy ?? null,
    stderrCopy: options.stderrCopy ?? null,
  };
  const [args, frame]: [N.WorkerExecArguments, Uint8Array | undefined] =
    typeof stdin === 'string'
      ? [{ ...fields, stdin: { kind: 'inline' } }, utf8.encode(stdin)]
      : stdin instanceof Uint8Array
        ? [{ ...fields, stdin: { kind: 'inline' } }, stdin]
        : stdin !== undefined
          ? [{ ...fields, stdin }, undefined]
          : stdinWorkspacePath !== undefined
            ? [{ ...fields, stdin: { kind: 'workspaceFile', workspacePath: stdinWorkspacePath } }, undefined]
            : [{ ...fields, stdin: { kind: 'empty' } }, undefined];
  return [args, frame];
}

/** Admits one job through worker.exec: a worker capability's own route, background work in its workspace sandbox. */
export async function exec(
  worker: N.NativeWorkerExec,
  session: string | null,
  request: ExecRequest,
): Promise<NativeJobHandle> {
  const [args, frame] = execFrame(session, request);
  return N.workerExec(worker, args, frame);
}

/**
 * Admits one job through coordinator.exec, the only route that may ask for the gate class or the host-gate scope, in
 * the workspace incarnation named by the fence. The answer is the job's id.
 */
export async function coordinatorExec(
  coordinator: N.NativeCoordinatorExec,
  fence: Pick<N.CoordinatorExecArguments, 'workspace' | 'workspaceIncarnation'>,
  request: ExecRequest,
): Promise<Api.JobId> {
  const [args, frame] = execFrame(null, request);
  return N.coordinatorExec(coordinator, { ...args, ...fence }, frame);
}
