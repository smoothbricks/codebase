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

const moduleUrl = pathToFileURL(join(import.meta.dir, '..', 'dist', 'ts', 'index.js')).href;

/** What the scripted job wrote to stdout; `job.logs` and `job.tail` serve it. */
const scriptedStdout = 'prefix\nrest\n';

/** What a client run against `scriptedController` printed, and what its controller heard. */
interface ScriptedRun {
  exitCode: number;
  stdout: string;
  stderr: string;
  /**
   * The frames of every `job.progress` call, the offset of every `job.logs` read and the cursor of
   * every `job.tail` read, in arrival order, and any `job.kill`.
   */
  heard: string[];
}

/**
 * Runs `client` against a scripted controller: a Node controller serves the wire on one end of a
 * socket pair and hands the other end to a Node client as fd 3, the way a trusted spawner hands an
 * endpoint over. The job runs until it is released, as if blocked on a FIFO the controller holds:
 * `job.status` answers and then releases it, and so does the fourth running sample a
 * `job.progress` call answers. `job.wait` answers once the job is released; `job.status` once no
 * progress call is open. Each progress demand is answered with one frame: a running sample while
 * the job is held, then the terminal sample the sealed job reports, then the call's end; a close
 * ends the call at once. `job.logs` answers the scripted stdout from the requested offset as a
 * download frame; `job.tail` the bounded slice after the cursor, or the latest bounded tail when
 * the request names none. The controller prints what it heard as its last stdout line.
 */
async function scriptedController(client: string): Promise<ScriptedRun> {
  const controller = `
    import { spawn } from 'node:child_process';
    const incarnation = '0198f2c0b7e34dc795f17b238b331c80';
    const stdout = Buffer.from(${JSON.stringify(scriptedStdout)});
    const emptyStream = {
      storage: { kind: 'captured', artifact: { kind: 'inline', data: { encoding: 'utf8', data: '' } } },
      bytes: 0,
      sha256: 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
      summary: { version: 1, text: '', truncated: false },
    };
    const sample = (wallMs, running) => ({
      jobId: 1,
      sampledAt: '2026-07-13T00:00:00Z',
      wallMs,
      wallUs: wallMs * 1000,
      leaderPid: 4242,
      members: running ? [4242] : [],
      hostStart: { load1: 1.5, cores: 10 },
      host: { load1: 2.5, cores: 10 },
      rssBytes: running ? 4096 : 0,
      rssPeakBytes: 4096,
      volumes: { workspace: { kind: 'unavailable', reason: { kind: 'unconfigured' } } },
      stdout: { bytes: 0, lines: 0 },
      stderr: { bytes: 0, lines: 0 },
    });
    const terminal = sample(50, false);
    const job = (ended) => ({
      repoId: 'acme/widget',
      workspaceIncarnation: incarnation,
      jobId: 1,
      state: ended ? 'exited' : 'running',
      grantRevision: 1,
      argv: [{ encoding: 'utf8', data: 'build' }],
      cwd: null,
      started: '2026-07-13T00:00:00Z',
      ...(ended ? { durationMs: 1, exit: { kind: 'exited', code: 0 }, resources: terminal } : {}),
      stdout: emptyStream,
      stderr: emptyStream,
      trace: { traceId: '4bf92f3577b34da6a3ce929d0e0e4736', spanId: '00f067aa0ba902b7' },
      stdin: { kind: 'empty', bytes: 0, complete: true },
    });
    const results = {
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
      'worker.exec': 1,
    };
    const node = spawn(process.execPath, ['--input-type=module', '--eval', ${JSON.stringify(client)}], {
      stdio: ['ignore', 'inherit', 'inherit', 'pipe'],
    });
    const socket = node.stdio[3];
    const framed = (body) => {
      const head = Buffer.alloc(4);
      head.writeUInt32BE(body.length);
      return Buffer.concat([head, body]);
    };
    const send = (value) => socket.write(framed(Buffer.from(JSON.stringify(value))));
    const answer = (id, result) => send({ id, ok: true, result, error: null, binaryLength: null });
    const heard = [];
    let released = false;
    const waits = [];
    const statuses = [];
    const heldStatuses = [];
    const release = () => {
      released = true;
      for (const id of waits.splice(0)) answer(id, job(true));
    };
    const answerStatus = (id) => {
      answer(id, job(released));
      release();
    };
    // Each open progress call: how many running samples it answered, and whether it sent the terminal one.
    const streams = new Map();
    const end = (id) => {
      streams.delete(id);
      answer(id, {});
      if (streams.size === 0) for (const id of statuses.splice(0)) answerStatus(id);
    };
    const demand = (id) => {
      const stream = streams.get(id);
      // The review fixture withholds the second demand. A status answer is its causal barrier:
      // the client cannot attempt close until this controller actually heard the pending next.
      if (stream.held && stream.running === 1) {
        stream.blocked = true;
        for (const id of heldStatuses.splice(0)) answer(id, job(false));
        return;
      }
      if (stream.terminal) {
        end(id);
      } else if (released) {
        stream.terminal = true;
        send({ id, event: terminal });
      } else {
        send({ id, event: sample(10 * stream.running, true) });
        stream.running += 1;
        if (stream.running === 4) release();
      }
    };
    let greeted = false;
    const handle = (message) => {
      if (!greeted) {
        greeted = true;
        send({ version: message.version, nonce: message.nonce, repoId: 'acme/widget' });
      } else if ('demand' in message) {
        heard.push(message.demand);
        if (message.demand === 'close') end(message.id);
        else demand(message.id);
      } else if (message.method in results) {
        answer(message.id, results[message.method]);
      } else if (message.method === 'job.progress') {
        heard.push('open every ' + message.params.everyMs);
        streams.set(message.id, {
          running: 0, terminal: false, held: message.params.everyMs === 999, blocked: false,
        });
        demand(message.id);
      } else if (message.method === 'job.status' && [...streams.values()].some((stream) => stream.held)) {
        if ([...streams.values()].some((stream) => stream.blocked)) answer(message.id, job(false));
        else heldStatuses.push(message.id);
      } else if (message.method === 'job.status' && streams.size > 0) {
        statuses.push(message.id);
      } else if (message.method === 'job.status') {
        answerStatus(message.id);
      } else if (message.method === 'job.wait' && !released) {
        waits.push(message.id);
      } else if (message.method === 'job.wait') {
        answer(message.id, job(true));
      } else if (message.method === 'job.logs' && message.params.stream === 'stdout') {
        const { offset } = message.params;
        heard.push('logs from ' + offset);
        const bytes = stdout.subarray(offset);
        const result = { eof: true, nextOffset: offset + bytes.length };
        send({ id: message.id, ok: true, result, error: null, binaryLength: bytes.length });
        socket.write(framed(bytes));
      } else if (message.method === 'job.tail') {
        const { limits } = message.params;
        const latest = !('cursor' in message.params);
        heard.push(latest ? 'tail latest' : 'tail after ' + JSON.stringify(message.params.cursor));
        const start = latest ? Math.max(0, stdout.length - limits.bytesPerStream) : message.params.cursor.stdout;
        const end = latest ? stdout.length : Math.min(stdout.length, start + limits.bytesPerStream);
        answer(message.id, {
          stdout: { encoding: 'utf8', data: stdout.subarray(start, end).toString() },
          stderr: { encoding: 'utf8', data: '' },
          next: { stdout: end, stderr: 0 },
          stdoutTruncated: latest ? start > 0 : end < stdout.length,
          stderrTruncated: false,
        });
      } else {
        if (message.method === 'job.kill') heard.push('job.kill');
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
      console.log(JSON.stringify(heard));
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
  const lines = stdout.trim().split('\n');
  const heard = lines.pop() ?? '[]';
  return { exitCode, stdout: lines.join('\n'), stderr, heard: JSON.parse(heard) };
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
   * 07_api "Keyed admission": the actual addon carries an exec's admission key onto the
   * SCRIPTED controller wire as given. The admitted key resolves to a real native handle, while
   * exec refusals retain the controller's typed cause field for field. This proves native/public
   * projection, not the independent supervisor/store proof of admission or absence.
   */
  it('reaches the keyed job and preserves conflict and stream-binding causes', async () => {
    const client = `
      import assert from 'node:assert/strict';
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
      assert.ok(reached !== null, 'the scripted admitted key must resolve to its job');
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

  /**
   * `job.progress` is an `AsyncIterable` whose every step is one demand: the controller hears the
   * request and then one `next` per sample the loop takes, never one ahead of it. The job is held
   * until the latest sample and three periodic ones were read; then exactly one terminal sample,
   * the one the sealed job reports, precedes the end, and an ended stream sends no close.
   */
  it('iterates job progress one demand per sample through the terminal sample', async () => {
    const client = `
      import { isDeepStrictEqual } from 'node:util';
      import { connectCoordinator, coordinatorEndpoint } from ${JSON.stringify(moduleUrl)};
      const coordinator = await connectCoordinator(coordinatorEndpoint(3), '/w/widget');
      const worker = await coordinator.worker('main');
      const job = await worker.exec({ argv: ['build'] });
      const samples = [];
      for await (const sample of job.progress(10)) samples.push(sample);
      const sealed = await job.wait();
      const terminal = samples.at(-1);
      console.log(JSON.stringify({
        running: samples.slice(0, -1).map((sample) => sample.members.length),
        terminal: terminal.members.length,
        sealed: isDeepStrictEqual(terminal, sealed.resources),
        state: sealed.state,
      }));
      process.exit(0);
    `;
    expect(await scriptedController(client)).toEqual({
      exitCode: 0,
      stdout: JSON.stringify({ running: [1, 1, 1, 1], terminal: 0, sealed: true, state: 'exited' }),
      stderr: '',
      heard: ['open every 10', 'next', 'next', 'next', 'next', 'next'],
    });
  }, 30_000);

  /**
   * Breaking out of the loop closes the call and nothing else: the controller hears the close,
   * never a kill, and the job still runs. The controller answers the status only once no stream is
   * open, so a client that never sends its close deadlocks with it. An interval the declaration
   * refuses rejects the loop with the typed usage error before any request is sent.
   */
  it('closes job progress on break and leaves the job running', async () => {
    const client = `
      import { CowshedError, connectCoordinator, coordinatorEndpoint } from ${JSON.stringify(moduleUrl)};
      const coordinator = await connectCoordinator(coordinatorEndpoint(3), '/w/widget');
      const worker = await coordinator.worker('main');
      const job = await worker.exec({ argv: ['build'] });
      const seen = [];
      for await (const sample of job.progress(25)) {
        seen.push(sample.wallMs);
        if (seen.length === 2) break;
      }
      const status = await job.status();
      let refused = 'opened';
      try {
        for await (const sample of job.progress(0)) refused = 'sampled ' + sample.wallMs;
      } catch (error) {
        refused = error instanceof CowshedError ? error.code : String(error);
      }
      console.log(JSON.stringify({ seen, state: status.state, refused }));
      process.exit(0);
    `;
    expect(await scriptedController(client)).toEqual({
      exitCode: 0,
      stdout: JSON.stringify({ seen: [0, 10], state: 'running', refused: 'usage' }),
      stderr: '',
      heard: ['open every 25', 'next', 'close'],
    });
  }, 30_000);

  /**
   * The addon's `close` ends the call while a demand waits unanswered: the controller withholds the
   * second sample, and answers the status only once that demand has reached it, so the close is
   * sent after it. The waiting `next` resolves to the end. A close that waited behind the demand
   * would never reach the controller, which deadlocks the pair until the spawn's deadline.
   */
  it('closes a stream from the addon while a demand waits for its event', async () => {
    const nativeUrl = pathToFileURL(join(import.meta.dir, '..', 'dist', 'ts', 'native.js')).href;
    const execUrl = pathToFileURL(join(import.meta.dir, '..', 'dist', 'ts', 'exec.js')).href;
    const generatedUrl = pathToFileURL(join(import.meta.dir, '..', 'dist', 'ts', 'native.generated.js')).href;
    const client = `
      import { loadNativeModule } from ${JSON.stringify(nativeUrl)};
      import { exec } from ${JSON.stringify(execUrl)};
      import * as N from ${JSON.stringify(generatedUrl)};
      const native = loadNativeModule();
      const coordinator = await native.connectCoordinator(native.coordinatorEndpoint(3), '/w/widget');
      const worker = await coordinator.worker(JSON.stringify({ workspace: 'main' }));
      const job = await exec(worker, null, { argv: ['build'] });
      const events = await job.progress(JSON.stringify({ everyMs: 999 }));
      await events.next();
      const pending = events.next();
      const status = await N.jobStatus(job, {});
      await events.close();
      console.log(JSON.stringify({ next: await pending, state: status.state }));
      process.exit(0);
    `;
    expect(await scriptedController(client)).toEqual({
      exitCode: 0,
      stdout: JSON.stringify({ next: null, state: 'running' }),
      stderr: '',
      heard: ['open every 999', 'next', 'close'],
    });
  }, 30_000);

  /** The public iterator's `return` closes the call at once, never behind the `next` that waits. */
  it('returns from job progress while a demand waits for its event', async () => {
    const client = `
      import { connectCoordinator, coordinatorEndpoint } from ${JSON.stringify(moduleUrl)};
      const coordinator = await connectCoordinator(coordinatorEndpoint(3), '/w/widget');
      const worker = await coordinator.worker('main');
      const job = await worker.exec({ argv: ['build'] });
      const iterator = job.progress(999)[Symbol.asyncIterator]();
      await iterator.next();
      const pending = iterator.next();
      const status = await job.status();
      const returned = await iterator.return();
      console.log(JSON.stringify({ returned: returned.done, next: (await pending).done, state: status.state }));
      process.exit(0);
    `;
    expect(await scriptedController(client)).toEqual({
      exitCode: 0,
      stdout: JSON.stringify({ returned: true, next: true, state: 'running' }),
      stderr: '',
      heard: ['open every 999', 'next', 'close'],
    });
  }, 30_000);

  /**
   * A reader that already holds a stream's first bytes continues from there, never from zero; a
   * tail without a cursor sends none, never `null`, and reads the latest bounded tail, and one
   * with a cursor reads what follows it.
   */
  it('reads job logs from an offset and tails after a cursor', async () => {
    const offset = scriptedStdout.indexOf('rest');
    const client = `
      import { connectCoordinator, coordinatorEndpoint } from ${JSON.stringify(moduleUrl)};
      const coordinator = await connectCoordinator(coordinatorEndpoint(3), '/w/widget');
      const worker = await coordinator.worker('main');
      const job = await worker.exec({ argv: ['build'] });
      const chunk = await job.logs({ stream: 'stdout', offset: ${offset}, follow: false });
      const latest = await job.tail(undefined, { bytesPerStream: 5, linesPerStream: 10 });
      const after = await job.tail({ stdout: ${offset}, stderr: 0 }, { bytesPerStream: 64, linesPerStream: 10 });
      const slice = (tail) => ({ stdout: tail.stdout.data, next: tail.next.stdout, truncated: tail.stdoutTruncated });
      console.log(JSON.stringify({
        logs: { text: Buffer.from(chunk.bytes).toString(), nextOffset: chunk.nextOffset, eof: chunk.eof },
        latest: slice(latest),
        after: slice(after),
      }));
      process.exit(0);
    `;
    const end = scriptedStdout.length;
    expect(await scriptedController(client)).toEqual({
      exitCode: 0,
      stdout: JSON.stringify({
        logs: { text: 'rest\n', nextOffset: end, eof: true },
        latest: { stdout: 'rest\n', next: end, truncated: true },
        after: { stdout: 'rest\n', next: end, truncated: false },
      }),
      stderr: '',
      heard: [`logs from ${offset}`, 'tail latest', `tail after {"stdout":${offset},"stderr":0}`],
    });
  }, 30_000);
});

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
