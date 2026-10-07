/// <reference types="bun" />
/// <reference types="node" />

import { describe, expect, it } from 'bun:test';
import { openSync } from 'node:fs';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import {
  CowshedError,
  connectCoordinator,
  coordinatorEndpoint,
  type ErrorCode,
  openProject,
} from '../dist/ts/index.js';

/**
 * The oracle is the `ErrorCode` plus a non-empty hint, not the rendered English. `code` is core's
 * taxonomy and `hint` is a real property the addon sets, so both are contract; message copy is
 * not, and asserting substrings of it only pins wording.
 */
function requireCowshedError(error: unknown, code: ErrorCode): CowshedError {
  expect(error).toBeInstanceOf(CowshedError);
  if (!(error instanceof CowshedError)) {
    // invariant throw: the assertion above proves this branch unreachable.
    throw new Error('expected CowshedError');
  }
  expect(error.code).toBe(code);
  expect(error.hint.length).toBeGreaterThan(0);
  return error;
}

describe('Cowshed Node-API bindings', () => {
  it('rejects invalid inherited descriptors with the stable usage error', () => {
    try {
      coordinatorEndpoint(-1);
      throw new Error('expected coordinatorEndpoint to reject');
    } catch (error) {
      requireCowshedError(error, 'usage');
    }
  });

  it('consumes an inherited endpoint exactly once and preserves handshake errors', async () => {
    const root = await mkdtemp(join(tmpdir(), 'cowshed-napi-'));
    try {
      const path = join(root, 'not-a-socket');
      await writeFile(path, 'fixture');
      const endpoint = coordinatorEndpoint(openSync(path, 'r'));

      try {
        await openProject(endpoint, root);
        throw new Error('expected a regular-file endpoint to fail the controller handshake');
      } catch (error) {
        requireCowshedError(error, 'environment-missing');
      }

      try {
        await openProject(endpoint, root);
        throw new Error('expected a consumed endpoint to reject reuse');
      } catch (error) {
        requireCowshedError(error, 'conflict');
      }

      const coordinatorEndpointValue = coordinatorEndpoint(openSync(path, 'r'));
      try {
        await connectCoordinator(coordinatorEndpointValue, root);
        throw new Error('expected a regular-file endpoint to fail the coordinator handshake');
      } catch (error) {
        requireCowshedError(error, 'environment-missing');
      }
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('loads the built addon and preserves its error contract under Node', async () => {
    const script = `
      import { coordinatorEndpoint, CowshedError } from ${JSON.stringify(moduleUrl)};
      try {
        coordinatorEndpoint(-1);
        process.exitCode = 2;
      } catch (error) {
        if (!(error instanceof CowshedError)) throw error;
        if (error.code !== 'usage' || error.hint.length === 0) process.exitCode = 3;
      }
    `;
    const node = Bun.spawn(['node', '--input-type=module', '--eval', script], {
      cwd: join(import.meta.dir, '..'),
      stdin: 'ignore',
      stdout: 'pipe',
      stderr: 'pipe',
    });
    const [exitCode, stderr] = await Promise.all([node.exited, new Response(node.stderr).text()]);

    expect(stderr).toBe('');
    expect(exitCode).toBe(0);
  });

  /**
   * One `Cowshed` holds one controller connection for every handle, and `job.wait()` lasts as
   * long as the job. A second call on that connection must complete while the wait is pending.
   *
   * A Node controller serves the wire on one end of a socket pair and hands the other end to a
   * Node client as fd 3, the way a trusted spawner hands an endpoint over. The controller keeps
   * the wait open until it has answered non-follow logs and the job's status. Logs reach an empty
   * current-end chunk with eof=false; this is not the end of the running job. A client that holds
   * these other calls behind the wait deadlocks with it, and the spawn's deadline ends the pair.
   */
  it('reads current raw logs and status while job.wait() is pending on one connection', async () => {
    const client = `
      import { connectCoordinator, coordinatorEndpoint } from ${JSON.stringify(moduleUrl)};
      const coordinator = await connectCoordinator(coordinatorEndpoint(3), '/w/widget');
      const worker = await coordinator.worker('main');
      const job = await worker.exec({ argv: ['build'] });
      const ended = job.wait();
      const bytes = [];
      let offset = 0;
      let chunk;
      do {
        chunk = await job.logs({ stream: 'stdout', offset, follow: false });
        bytes.push(...chunk.bytes);
        offset = chunk.nextOffset;
      } while (chunk.bytes.length !== 0 && !chunk.eof);
      const status = await job.status();
      console.log(JSON.stringify({
        bytes, offset, eof: chunk.eof, status: status.state, ended: (await ended).state,
      }));
      process.exit(0);
    `;
    const calls = `
      const emptyStream = {
        storage: { kind: 'captured', artifact: { kind: 'inline', data: { encoding: 'utf8', data: '' } } },
        bytes: 0,
        sha256: 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
        summary: { version: 1, text: '', truncated: false },
      };
      const job = (ended) => ({
        repoId: 'acme/widget',
        workspaceIncarnation: incarnation,
        jobId: 1,
        state: ended ? 'exited' : 'running',
        grantRevision: 1,
        argv: [{ encoding: 'utf8', data: 'build' }],
        cwd: null,
        started: '2026-07-13T00:00:00Z',
        ...(ended ? { durationMs: 1, exit: { kind: 'exited', code: 0 } } : {}),
        stdout: emptyStream,
        stderr: emptyStream,
        trace: { traceId: '4bf92f3577b34da6a3ce929d0e0e4736', spanId: '00f067aa0ba902b7' },
        stdin: { kind: 'empty', bytes: 0, complete: true },
      });
      let statusAnswered = false;
      let logsAnswered = false;
      const waits = [];
      const call = (message) => {
        if (message.method === 'worker.exec') {
          answer(message.id, 1);
        } else if (message.method === 'job.logs') {
          const { stream, offset, follow } = message.params;
          if (stream !== 'stdout' || follow !== false || ![0, 4].includes(offset)) {
            throw new Error('unexpected logs request ' + JSON.stringify(message.params));
          }
          const bytes = offset === 0 ? Buffer.from([0, 255, 128, 10]) : Buffer.alloc(0);
          logsAnswered = offset === 4;
          send({
            id: message.id, ok: true, result: { eof: false, nextOffset: offset + bytes.length },
            error: null, binaryLength: bytes.length,
          });
          const head = Buffer.alloc(4);
          head.writeUInt32BE(bytes.length);
          socket.write(Buffer.concat([head, bytes]));
        } else if (message.method === 'job.status') {
          if (!logsAnswered) throw new Error('status arrived before the current-end log chunk');
          statusAnswered = true;
          answer(message.id, job(false));
          for (const id of waits.splice(0)) answer(id, job(true));
        } else if (message.method === 'job.wait' && !statusAnswered) {
          waits.push(message.id);
        } else if (message.method === 'job.wait') {
          answer(message.id, job(true));
        } else {
          return false;
        }
        return true;
      };
    `;
    expect(await scriptedPair(client, calls)).toEqual({
      exitCode: 0,
      stdout: JSON.stringify({ bytes: [0, 255, 128, 10], offset: 4, eof: false, status: 'running', ended: 'exited' }),
      stderr: '',
    });
  }, 30_000);

  /**
   * 07_api "Keyed admission": the addon carries an exec's admission key onto the controller wire
   * as given, and a keyed refusal reaches JavaScript as a `CowshedError` whose `admission` is the
   * controller's typed refusal, field for field: the conversion keeps the cause, not only the
   * code, message and hint.
   */
  it('reaches the keyed job and preserves conflict and stream-binding causes', async () => {
    const client = `
      import { connectCoordinator, coordinatorEndpoint, CowshedError } from ${JSON.stringify(moduleUrl)};
      const coordinator = await connectCoordinator(coordinatorEndpoint(3), '/w/widget');
      const worker = await coordinator.worker('main');
      const refusal = async (request) => {
        try {
          await worker.exec(request);
          return 'resolved';
        } catch (error) {
          return {
            ours: error instanceof CowshedError,
            code: error.code,
            admission: error.admission ?? null,
            nativeObject: error.cause instanceof Error &&
              typeof error.cause.admission === 'object' && error.cause.admission !== null &&
              JSON.stringify(error.cause.admission) === JSON.stringify(error.admission),
          };
        }
      };
      const job = await worker.exec({ argv: ['build'], admissionKey: 'op-1' });
      const reached = await worker.jobByKey('op-1');
      const bound = await refusal({ argv: ['stream'], admissionKey: 'op-stream' });
      const changed = await refusal({ argv: ['test'], admissionKey: 'op-1' });
      const unprovable = await refusal({ argv: ['build'], admissionKey: 'op-2' });
      console.log(JSON.stringify({ job: job.id, reached: reached.id, bound, changed, unprovable }));
      process.exit(0);
    `;
    const calls = `
      const seen = [];
      process.on('exit', () => console.log(JSON.stringify(seen)));
      const refuse = (id, admission, code = 'conflict') =>
        send({
          id,
          ok: false,
          result: null,
          error: { code, message: 'refused', hint: 'reach the keyed job', admission },
          binaryLength: null,
        });
      const call = (message) => {
        if (message.method === 'worker.jobByKey') {
          seen.push(['lookup', message.params.admissionKey]);
          answer(message.id, 7);
          return true;
        }
        if (message.method !== 'worker.exec') {
          return false;
        }
        const { admissionKey } = message.params;
        const argv = message.params.argv.map((arg) => arg.data);
        seen.push([admissionKey, argv]);
        if (admissionKey === 'op-stream') {
          refuse(message.id, { reason: 'stdinBound', jobId: 7 }, 'usage');
        } else if (admissionKey === 'op-2') {
          refuse(message.id, { reason: 'unprovable', setAside: '/w/widget/.cowshed/job/set-aside/layout-8' });
        } else if (argv[0] === 'test') {
          refuse(message.id, { reason: 'keyConflict', jobId: 7, fields: ['command'] });
        } else {
          answer(message.id, 7);
        }
        return true;
      };
    `;
    const { exitCode, stdout, stderr } = await scriptedPair(client, calls);
    expect({ exitCode, stdout: stdout.split('\n'), stderr }).toEqual({
      exitCode: 0,
      stdout: [
        JSON.stringify({
          job: 7,
          reached: 7,
          bound: {
            ours: true,
            code: 'usage',
            admission: { reason: 'stdinBound', jobId: 7 },
            nativeObject: true,
          },
          changed: {
            ours: true,
            code: 'conflict',
            admission: { reason: 'keyConflict', jobId: 7, fields: ['command'] },
            nativeObject: true,
          },
          unprovable: {
            ours: true,
            code: 'conflict',
            admission: { reason: 'unprovable', setAside: '/w/widget/.cowshed/job/set-aside/layout-8' },
            nativeObject: true,
          },
        }),
        JSON.stringify([
          ['op-1', ['build']],
          ['lookup', 'op-1'],
          ['op-stream', ['stream']],
          ['op-1', ['test']],
          ['op-2', ['build']],
        ]),
      ],
      stderr: '',
    });
  }, 30_000);
});

const moduleUrl = pathToFileURL(join(import.meta.dir, '..', 'dist', 'ts', 'index.js')).href;

/**
 * A Node controller serves the wire on one end of a socket pair and hands the other end to a Node
 * `client` as fd 3, the way a trusted spawner hands an endpoint over. It greets, opens the project
 * and the worker; `calls` defines `call(message)`, which answers the rest and returns false for a
 * call it does not script. The spawn's deadline ends a deadlocked pair.
 */
async function scriptedPair(
  client: string,
  calls: string,
): Promise<{ exitCode: number; stdout: string; stderr: string }> {
  const controller = `
    import { spawn } from 'node:child_process';
    const incarnation = '0198f2c0b7e34dc795f17b238b331c80';
    const opened = {
      'project.open': {
        repoId: 'acme/widget',
        binding: {
          version: 1,
          identities: [{ repoId: 'acme/widget', remoteName: null, remoteUrl: null, primary: true }],
        },
        gitRoot: '/w/widget',
        storeRoot: '/w/store',
      },
      'coordinator.worker': {
        info: {
          repoId: 'acme/widget',
          workspace: 'main',
          workspaceIncarnation: incarnation,
          role: 'main',
          mount: '/w/widget',
          state: 'attached',
          checkpoints: [],
          snapshotStale: false,
        },
        grants: { egress: [], read: [], revision: 0, sim: [], write: [] },
      },
    };
    const node = spawn(process.execPath, ['--input-type=module', '--eval', ${JSON.stringify(client)}], {
      stdio: ['ignore', 'inherit', 'inherit', 'pipe'],
    });
    const socket = node.stdio[3];
    const send = (value) => {
      const body = Buffer.from(JSON.stringify(value));
      const head = Buffer.alloc(4);
      head.writeUInt32BE(body.length);
      socket.write(Buffer.concat([head, body]));
    };
    const answer = (id, result) => send({ id, ok: true, result, error: null, binaryLength: null });
    ${calls}
    let greeted = false;
    const handle = (message) => {
      if (!greeted) {
        greeted = true;
        send({ version: message.version, nonce: message.nonce, repoId: 'acme/widget' });
      } else if (message.method in opened) {
        answer(message.id, opened[message.method]);
      } else if (!call(message)) {
        const error = { code: 'internal', message: 'unscripted ' + message.method, hint: 'script it' };
        send({ id: message.id, ok: false, result: null, error, binaryLength: null });
      }
    };
    let buffered = Buffer.alloc(0);
    socket.on('data', (chunk) => {
      buffered = Buffer.concat([buffered, chunk]);
      while (buffered.length >= 4 && buffered.length >= 4 + buffered.readUInt32BE(0)) {
        const length = buffered.readUInt32BE(0);
        handle(JSON.parse(buffered.subarray(4, 4 + length).toString()));
        buffered = buffered.subarray(4 + length);
      }
    });
    node.on('exit', (code) => {
      process.exitCode = code ?? 1;
      socket.destroy();
    });
  `;
  const node = Bun.spawn(['node', '--input-type=module', '--eval', controller], {
    cwd: join(import.meta.dir, '..'),
    stdin: 'ignore',
    stdout: 'pipe',
    stderr: 'pipe',
    // Bounds only a deadlocked pair; a working client ends in milliseconds. Killing the
    // controller closes the client's endpoint, which ends the client too.
    timeout: 20_000,
  });
  const [exitCode, stdout, stderr] = await Promise.all([
    node.exited,
    new Response(node.stdout).text(),
    new Response(node.stderr).text(),
  ]);
  return { exitCode, stdout: stdout.trim(), stderr };
}
