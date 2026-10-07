/// <reference types="bun" />
/// <reference types="node" />

import { describe, expect, it } from 'bun:test';
import { exec } from './exec.js';
import type { ExecOptions } from './types.js';

/**
 * A worker handle that records what `worker.exec` would send and refuses to admit anything: the
 * oracle is the exact arguments and frame that reach the addon, or that nothing reached it.
 */
function recordingWorker() {
  const sent: { readonly args: unknown; readonly bytes: Buffer | undefined }[] = [];
  const worker = {
    async exec(argumentsJson: string, bytes?: Buffer): Promise<never> {
      sent.push({ args: JSON.parse(argumentsJson), bytes });
      throw new Error('recorded');
    },
  };
  return { worker, sent };
}

describe('exec', () => {
  it('refuses a request naming both argv and script without sending it', async () => {
    const { worker, sent } = recordingWorker();
    await expect(
      // @ts-expect-error a request names exactly one of argv and script
      exec(worker, null, { argv: ['build'], script: { parts: ['make'], values: [] } }),
    ).rejects.toThrow();
    expect(sent).toEqual([]);
  });

  it('refuses a request naming both inline and workspace-file stdin without sending it', async () => {
    const { worker, sent } = recordingWorker();
    await expect(
      // @ts-expect-error a request names at most one stdin source
      exec(worker, null, { argv: ['cat'], stdin: 'text', stdinWorkspacePath: 'input.txt' }),
    ).rejects.toThrow();
    expect(sent).toEqual([]);
  });

  it('refuses a field the request does not declare without sending it', async () => {
    const { worker, sent } = recordingWorker();
    // An untyped caller, as plain JavaScript passes it.
    const request = JSON.parse('{"argv":["build"],"undeclaredField":"k"}');
    await expect(exec(worker, null, request)).rejects.toThrow();
    expect(sent).toEqual([]);
  });

  it('forwards every option as given and adapts only the command, stdin and defaults', async () => {
    const { worker, sent } = recordingWorker();
    const options = {
      cwd: 'src',
      mode: 'readOnly',
      env: { LANG: 'C' },
      trace: { traceId: '4bf92f3577b34da6a3ce929d0e0e4736', spanId: '00f067aa0ba902b7' },
      stdoutCopy: { path: 'out.log', policy: 'replace' },
      stderrCopy: { path: 'err.log', policy: 'createNew' },
    } satisfies ExecOptions;
    await expect(exec(worker, 'build', { argv: ['make', 'all'], stdin: 'input', ...options })).rejects.toThrow(
      'recorded',
    );
    await expect(exec(worker, null, { script: { parts: ['make'], values: [] } })).rejects.toThrow('recorded');
    await expect(exec(worker, null, { argv: ['cat'], stdinWorkspacePath: 'input.txt' })).rejects.toThrow('recorded');

    expect(sent).toEqual([
      {
        args: {
          ...options,
          session: 'build',
          argv: [
            { encoding: 'utf8', data: 'make' },
            { encoding: 'utf8', data: 'all' },
          ],
          stdin: { kind: 'inline' },
        },
        bytes: Buffer.from('input'),
      },
      {
        args: {
          session: null,
          script: { parts: ['make'], values: [] },
          cwd: null,
          mode: 'readWrite',
          env: {},
          trace: null,
          stdoutCopy: null,
          stderrCopy: null,
          stdin: { kind: 'empty' },
        },
        bytes: undefined,
      },
      {
        args: {
          session: null,
          argv: [{ encoding: 'utf8', data: 'cat' }],
          cwd: null,
          mode: 'readWrite',
          env: {},
          trace: null,
          stdoutCopy: null,
          stderrCopy: null,
          stdin: { kind: 'workspaceFile', workspacePath: 'input.txt' },
        },
        bytes: undefined,
      },
    ]);
  });
});
