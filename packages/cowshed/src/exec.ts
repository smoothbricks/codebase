import typia from 'typia';
import type * as Api from './api.generated.js';
import * as N from './native.generated.js';
import type { NativeJobHandle } from './native.js';
import type { ExecRequest } from './types.js';

const assertExecRequest = typia.createAssertEquals<ExecRequest>();
const utf8 = new TextEncoder();

/**
 * Admits one job through `worker.exec`. The request is checked exactly before anything is sent:
 * naming both `argv` and `script`, both `stdin` and `stdinWorkspacePath`, or any field the
 * request does not declare is refused. Every generated option field travels as the caller gave
 * it; only the command, stdin and the defaults `ExecOptions` documents are adapted — an omitted
 * option is absent (`null`), an omitted mode runs read-write, an omitted environment is empty —
 * and inline stdin travels as the upload frame.
 */
export async function exec(
  worker: N.NativeWorkerExec,
  session: string | null,
  request: ExecRequest,
): Promise<NativeJobHandle> {
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
  return N.workerExec(worker, args, frame);
}
