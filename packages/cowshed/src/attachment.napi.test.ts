/// <reference types="bun" />
/// <reference types="node" />

import { describe, expect, it } from 'bun:test';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

const frameBytes = 64 * 1024;
const binaryBytes = 4 * 1024 * 1024;
const lineBytes = 6;

/** Shared byte recipe, not text/base64 transport; the view excludes both backing-store guards. */
const inputFixture = `
  const frameBytes = ${frameBytes};
  const line = new Uint8Array([102, 105, 114, 115, 116, 10]);
  const backing = new Uint8Array(${binaryBytes} + 37);
  for (let index = 0; index < backing.length; index++) {
    backing[index] = (index * 131 + (index >>> 8) * 17 + (index >>> 16)) & 255;
  }
  const binary = backing.subarray(13, 13 + ${binaryBytes});
`;

describe('Cowshed attachment Node-API boundary', () => {
  /**
   * Like native.test.ts, a Node controller passes its socketpair peer to the actual built-addon
   * client as fd 3. IPC carries only explicit reader demands and observations, never job calls
   * or stdin bytes. Each native write reply remains held until a status roundtrip has proved
   * that its bytes are not delivered and the public write promise has not resolved.
   *
   * This is an addon/core-client oracle over a SCRIPTED controller, not a real supervisor,
   * process, or pipe oracle. Output, EOF, running state and terminal release are scripted here;
   * backend acceptance owns the independent real-child proof. No timing-based synchronization
   * or additional deadline is used: the existing napi-test runner bounds a broken protocol.
   */
  it('preserves open stdin, bounded binary delivery, typed refusals and detach without cancellation', async () => {
    const moduleUrl = pathToFileURL(join(import.meta.dir, '..', 'dist', 'ts', 'index.js')).href;
    const client = `
      import assert from 'node:assert/strict';
      import { once } from 'node:events';
      import { CowshedError, connectCoordinator, coordinatorEndpoint } from ${JSON.stringify(moduleUrl)};
      ${inputFixture}
      const coordinator = await connectCoordinator(coordinatorEndpoint(3), '/w/widget');
      const worker = await coordinator.worker('main');
      const job = await worker.exec({ argv: ['attachment-oracle'], admissionKey: 'attachment-open', stdin: { kind: 'open' } });
      assert.equal(job.id, 1);
      const recovered = await worker.jobByKey('attachment-open');
      assert.ok(recovered !== null, 'the scripted admitted key must resolve to its job');
      assert.equal(recovered.id, job.id);
      let attachment = await recovered.attach();
      let cursor = 0;

      const deliver = async (bytes) => {
        let nextHeld = once(process, 'message');
        let resolved = false;
        const writing = attachment.write(bytes).then(() => { resolved = true; });
        const count = Math.ceil(bytes.byteLength / frameBytes);
        for (let index = 0; index < count; index++) {
          const [held] = await nextHeld;
          const length = Math.min(frameBytes, bytes.byteLength - index * frameBytes);
          assert.deepEqual(held, { kind: 'held', offset: cursor, bytes: length });
          assert.equal(resolved, false, 'write resolved before the reader demanded its frame');
          const status = await job.status();
          assert.equal(status.state, 'running');
          assert.deepEqual(status.stdin, { kind: 'stream', bytes: cursor, complete: false });
          assert.equal(resolved, false, 'held reply must backpressure the whole public write');
          if (index + 1 < count) nextHeld = once(process, 'message');
          process.send({ kind: 'demand', offset: cursor, bytes: length });
          cursor += length;
        }
        await writing;
        assert.equal(resolved, true);
        assert.equal((await job.status()).stdin.bytes, cursor);
      };

      await deliver(line);
      const output = await job.logs({ stream: 'stdout', offset: 0, follow: false });
      assert.ok(output.bytes instanceof Uint8Array);
      assert.deepEqual(Buffer.from(output.bytes), Buffer.from(line));
      assert.equal(output.nextOffset, line.byteLength);
      assert.equal(output.eof, false, 'the first line is readable before stdin EOF');
      assert.equal((await job.status()).stdin.complete, false);

      await attachment.detach();
      const detached = await job.status();
      assert.equal(detached.state, 'running');
      assert.deepEqual(detached.stdin, { kind: 'stream', bytes: cursor, complete: false });
      attachment = await job.attach();
      // This attachment must resume core's delivered cursor, not a second JS offset starting at 0.
      await deliver(binary);
      assert.equal(cursor, line.byteLength + binary.byteLength);

      await attachment.end();
      await attachment.end();
      const closed = await job.status();
      assert.equal(closed.state, 'running');
      assert.deepEqual(closed.stdin, { kind: 'stream', bytes: cursor, complete: true });
      await assert.rejects(attachment.write(new Uint8Array([108, 97, 116, 101, 10])), (error) => {
        assert.ok(error instanceof CowshedError);
        assert.equal(error.code, 'conflict');
        assert.ok(error.hint.length > 0);
        assert.deepEqual(error.stdin, { reason: 'ended', cursor });
        assert.ok(error.cause instanceof Error, 'normalization retains the native error');
        assert.deepEqual(error.cause.stdin, { reason: 'ended', cursor });
        return true;
      });
      assert.equal((await job.status()).stdin.bytes, cursor, 'a refused write delivers no bytes');

      await attachment.detach();
      const waitObserved = once(process, 'message');
      let ended = false;
      const waiting = job.wait().then((info) => { ended = true; return info; });
      const [observed] = await waitObserved;
      assert.deepEqual(observed, { kind: 'waiting' });
      const running = await job.status();
      assert.equal(running.state, 'running');
      assert.equal(ended, false, 'neither end nor detach completes the job');
      process.send({ kind: 'release' });
      const terminal = await waiting;
      assert.equal(terminal.state, 'exited');
      assert.deepEqual(terminal.exit, { kind: 'exited', code: 0 });
      assert.deepEqual(terminal.stdin, { kind: 'stream', bytes: cursor, complete: true });
      assert.equal((await job.status()).state, 'exited');

      // A separate scripted input has an existing delivered cursor and an unknowable next write.
      // Unlike ended, this refusal must not be dressed up as a safe retry or lose its cursor.
      const unknownJob = await worker.exec({ argv: ['delivery-unknown-oracle'], stdin: { kind: 'open' } });
      assert.equal(unknownJob.id, 2);
      const unknownAttachment = await unknownJob.attach();
      await assert.rejects(unknownAttachment.write(new Uint8Array([0, 255, 128, 10])), (error) => {
        assert.ok(error instanceof CowshedError);
        assert.equal(error.code, 'conflict');
        assert.ok(error.hint.length > 0);
        assert.deepEqual(error.stdin, { reason: 'deliveryUnknown', cursor: 3 });
        assert.ok(error.cause instanceof Error);
        assert.deepEqual(error.cause.stdin, { reason: 'deliveryUnknown', cursor: 3 });
        assert.equal(error.retry, undefined);
        assert.equal(error.cause.retry, undefined);
        return true;
      });
      await unknownAttachment.detach();
      const unknownStatus = await unknownJob.status();
      assert.equal(unknownStatus.state, 'running');
      assert.deepEqual(unknownStatus.stdin, { kind: 'stream', bytes: 3, complete: true });
      process.exit(0);
    `;
    const controller = `
      import assert from 'node:assert/strict';
      import { spawn } from 'node:child_process';
      import { createHash } from 'node:crypto';
      ${inputFixture}
      const repoId = 'acme/widget';
      const incarnation = '0198f2c0b7e34dc795f17b238b331c80';
      const fence = { repoId, workspace: 'main', workspaceIncarnation: incarnation };
      const jobFence = { ...fence, jobId: 1 };
      const emptyStream = {
        storage: { kind: 'captured', artifact: { kind: 'inline', data: { encoding: 'utf8', data: '' } } },
        bytes: 0,
        sha256: 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
        summary: { version: 1, text: '', truncated: false },
      };
      let cursor = 0;
      let closed = false;
      let released = false;
      let held = null;
      let waitId = null;
      let statusWhileWaiting = false;
      let openCalls = 0;
      let closeCalls = 0;
      let eofTransitions = 0;
      let detachCalls = 0;
      let refusedWrites = 0;
      let unknownWrites = 0;
      let unknownDetaches = 0;
      let demands = 0;
      let maxFrameBytes = 0;
      let outputBeforeEof = false;
      const delivered = [];
      const followLogs = [];
      const stdout = () => cursor >= line.byteLength ? Buffer.from(line) : Buffer.alloc(0);
      const info = (jobId = 1) => ({
        repoId,
        workspaceIncarnation: incarnation,
        jobId,
        state: jobId === 1 && released ? 'exited' : 'running',
        grantRevision: 1,
        argv: [{ encoding: 'utf8', data: jobId === 1 ? 'attachment-oracle' : 'delivery-unknown-oracle' }],
        cwd: null,
        started: '2026-07-13T00:00:00Z',
        ...(jobId === 1 && released ? { durationMs: 1, exit: { kind: 'exited', code: 0 } } : {}),
        stdout: jobId === 1 ? {
          ...emptyStream,
          storage: { kind: 'captured', artifact: { kind: 'inline', data: { encoding: 'utf8', data: stdout().toString() } } },
          bytes: stdout().length,
          sha256: createHash('sha256').update(stdout()).digest('hex'),
          summary: { version: 1, text: stdout().toString(), truncated: false },
        } : emptyStream,
        stderr: emptyStream,
        trace: { traceId: '4bf92f3577b34da6a3ce929d0e0e4736', spanId: '00f067aa0ba902b7' },
        stdin: jobId === 1
          ? { kind: 'stream', bytes: cursor, complete: closed }
          : { kind: 'stream', bytes: 3, complete: unknownWrites > 0 },
      });
      const node = spawn(process.execPath, ['--input-type=module', '--eval', ${JSON.stringify(client)}], {
        stdio: ['ignore', 'inherit', 'inherit', 'pipe', 'ipc'],
      });
      const socket = node.stdio[3];
      // Canonical server.rs codec: u32-BE JSON frame, then u32-BE raw frame when declared.
      const sendFrame = (bytes) => {
        const header = Buffer.alloc(4);
        header.writeUInt32BE(bytes.length);
        socket.write(Buffer.concat([header, bytes]));
      };
      const send = (value) => sendFrame(Buffer.from(JSON.stringify(value)));
      const answer = (id, result, bytes = null) => {
        send({ id, ok: true, result, error: null, binaryLength: bytes === null ? null : bytes.length });
        if (bytes !== null) sendFrame(bytes);
      };
      const answerLogs = (message) => {
        const { stream, offset } = message.params;
        const bytes = stream === 'stdout' ? stdout().subarray(offset) : Buffer.alloc(0);
        answer(message.id, { eof: released, nextOffset: offset + bytes.length }, bytes);
      };
      const flushLogs = () => {
        for (let index = followLogs.length - 1; index >= 0; index--) {
          const message = followLogs[index];
          if (message.params.jobId === 1 && (released || (message.params.stream === 'stdout' && message.params.offset < stdout().length))) {
            followLogs.splice(index, 1);
            answerLogs(message);
          }
        }
      };
      const handle = (message, bytes) => {
        assert.equal(message.steps, undefined, 'attachment calls do not request lifecycle steps');
        if (message.method !== 'job.attachWrite') {
          assert.equal(message.binaryLength, undefined, 'only stdin writes upload binary frames');
          assert.equal(bytes, null);
        }
        switch (message.method) {
          case 'project.open':
            assert.deepEqual(message.params, { path: '/w/widget' });
            answer(message.id, {
              repoId,
              binding: { version: 1, identities: [{ repoId, remoteName: null, remoteUrl: null, primary: true }] },
              gitRoot: '/w/widget',
              storeRoot: '/w/store',
            });
            break;
          case 'coordinator.worker':
            assert.deepEqual(message.params, { repoId, workspace: 'main' });
            answer(message.id, {
              info: {
                ...fence,
                role: 'main', mount: '/w/widget', state: 'attached',
                checkpoints: [], snapshotStale: false,
              },
              grants: { egress: [], read: [], revision: 0, sim: [], write: [] },
            });
            break;
          case 'worker.exec':
            openCalls++;
            assert.ok(openCalls <= 2);
            assert.deepEqual(message.params, {
              ...fence,
              argv: [{ encoding: 'utf8', data: openCalls === 1 ? 'attachment-oracle' : 'delivery-unknown-oracle' }],
              session: null, cwd: null, mode: 'readWrite', env: {}, trace: null,
              stdin: { kind: 'open' }, stdoutCopy: null, stderrCopy: null,
              ...(openCalls === 1 ? { admissionKey: 'attachment-open' } : {}),
            });
            answer(message.id, openCalls);
            break;
          case 'worker.jobByKey':
            assert.deepEqual(message.params, { ...fence, admissionKey: 'attachment-open' });
            answer(message.id, 1);
            break;
          case 'job.status': {
            const { jobId } = message.params;
            assert.ok(jobId === 1 || jobId === 2);
            assert.deepEqual(message.params, { ...jobFence, jobId });
            if (jobId === 1 && held !== null) held.statusChecks++;
            if (jobId === 1 && waitId !== null) statusWhileWaiting = true;
            answer(message.id, info(jobId));
            break;
          }
          case 'job.logs': {
            const { jobId, stream, offset, follow } = message.params;
            assert.ok(stream === 'stdout' || stream === 'stderr');
            assert.ok(Number.isSafeInteger(offset) && offset >= 0);
            assert.ok(jobId === 1 || jobId === 2);
            assert.ok(offset <= (jobId === 1 && stream === 'stdout' ? stdout().length : 0));
            assert.equal(typeof follow, 'boolean');
            assert.deepEqual(message.params, { ...jobFence, jobId, stream, offset, follow });
            if (jobId === 2) {
              assert.equal(follow, true);
              followLogs.push(message);
              break;
            }
            if (!follow) {
              assert.equal(closed, false);
              assert.equal(cursor, line.byteLength);
              outputBeforeEof = true;
              answerLogs(message);
            } else if (released || (stream === 'stdout' && offset < stdout().length)) {
              answerLogs(message);
            } else {
              // Core attachment output polls are allowed; an empty follow waits on real data.
              followLogs.push(message);
            }
            break;
          }
          case 'job.attachWrite': {
            assert.ok(Buffer.isBuffer(bytes));
            assert.equal(message.binaryLength, bytes.length);
            assert.ok(bytes.length > 0 && bytes.length <= frameBytes);
            if (message.params.jobId === 2) {
              assert.deepEqual(message.params, { ...jobFence, jobId: 2, offset: 3 });
              assert.deepEqual(bytes, Buffer.from([0, 255, 128, 10]));
              assert.equal(++unknownWrites, 1, 'unknown delivery must not trigger an automatic retry');
              send({
                id: message.id, ok: false, result: null, binaryLength: null,
                error: {
                  code: 'conflict', message: 'stdin delivery is unknown', hint: 'do not resend uncertain input',
                  stdin: { reason: 'deliveryUnknown', cursor: 3 },
                },
              });
              break;
            }
            assert.deepEqual(message.params, { ...jobFence, offset: cursor });
            assert.equal(held, null, 'the next frame cannot pass the held reply of its predecessor');
            if (closed) {
              assert.deepEqual(bytes, Buffer.from([108, 97, 116, 101, 10]));
              refusedWrites++;
              send({
                id: message.id, ok: false, result: null, binaryLength: null,
                error: {
                  code: 'conflict', message: 'job stdin ended', hint: 'do not write after EOF',
                  stdin: { reason: 'ended', cursor },
                },
              });
            } else {
              held = { message, bytes, statusChecks: 0 };
              node.send({ kind: 'held', offset: cursor, bytes: bytes.length });
            }
            break;
          }
          case 'job.attachClose':
            assert.deepEqual(message.params, jobFence);
            assert.equal(held, null, 'EOF follows delivery of every accepted frame');
            assert.equal(cursor, line.byteLength + binary.byteLength);
            closeCalls++;
            if (!closed) eofTransitions++;
            closed = true;
            answer(message.id, {});
            break;
          case 'job.detach':
            if (message.params.jobId === 2) {
              assert.deepEqual(message.params, { ...jobFence, jobId: 2 });
              assert.equal(++unknownDetaches, 1);
              assert.equal(unknownWrites, 1);
              answer(message.id, {});
              break;
            }
            assert.deepEqual(message.params, jobFence);
            assert.equal(released, false, 'detach does not release or kill the job');
            assert.equal(held, null);
            detachCalls++;
            assert.ok(detachCalls <= 2);
            assert.equal(cursor, detachCalls === 1 ? line.byteLength : line.byteLength + binary.byteLength);
            assert.equal(closed, detachCalls === 2, 'the first detach does not imply stdin EOF');
            answer(message.id, {});
            break;
          case 'job.wait':
            assert.deepEqual(message.params, jobFence);
            assert.equal(detachCalls, 2);
            assert.equal(waitId, null);
            waitId = message.id;
            node.send({ kind: 'waiting' });
            break;
          default:
            assert.fail('unscripted operation: ' + message.method);
        }
      };
      node.on('message', (message) => {
        switch (message.kind) {
          case 'demand': {
            assert.notEqual(held, null, 'reader demands exactly one pending frame');
            assert.ok(held.statusChecks > 0, 'a status roundtrip fences each held reply');
            assert.deepEqual(message, { kind: 'demand', offset: cursor, bytes: held.bytes.length });
            const expected = cursor === 0 ? Buffer.from(line) : Buffer.from(binary.subarray(cursor - line.length, cursor - line.length + held.bytes.length));
            assert.deepEqual(held.bytes, expected, 'raw bytes and every delivered offset are exact');
            delivered.push(held.bytes);
            maxFrameBytes = Math.max(maxFrameBytes, held.bytes.length);
            cursor += held.bytes.length;
            demands++;
            const id = held.message.id;
            held = null;
            answer(id, {});
            flushLogs();
            break;
          }
          case 'release':
            assert.deepEqual(message, { kind: 'release' });
            assert.equal(released, false);
            assert.notEqual(waitId, null);
            assert.equal(statusWhileWaiting, true, 'status remains available while wait is held');
            assert.equal(closeCalls, 2);
            assert.equal(eofTransitions, 1);
            assert.equal(refusedWrites, 1);
            released = true;
            answer(waitId, info());
            flushLogs();
            break;
          default:
            assert.fail('unscripted reader control: ' + message.kind);
        }
      });
      let greeted = false;
      let upload = null;
      let buffered = Buffer.alloc(0);
      socket.on('data', (chunk) => {
        buffered = Buffer.concat([buffered, chunk]);
        while (buffered.length >= 4) {
          const length = buffered.readUInt32BE(0);
          assert.ok(length <= (upload === null ? 16 * 1024 * 1024 : frameBytes));
          if (buffered.length < 4 + length) break;
          const body = buffered.subarray(4, 4 + length);
          buffered = buffered.subarray(4 + length);
          if (upload !== null) {
            const message = upload;
            upload = null;
            assert.equal(length, message.binaryLength, 'raw frame prefix agrees with binaryLength');
            handle(message, body);
          } else {
            const message = JSON.parse(body.toString());
            if (!greeted) {
              greeted = true;
              assert.deepEqual(Object.keys(message).sort(), ['nonce', 'version']);
              send({ version: message.version, nonce: message.nonce, repoId });
            } else if (message.binaryLength !== undefined) {
              assert.ok(Number.isSafeInteger(message.binaryLength));
              assert.ok(message.binaryLength >= 0 && message.binaryLength <= frameBytes);
              upload = message;
            } else {
              handle(message, null);
            }
          }
        }
      });
      node.on('close', (code) => {
        socket.destroy();
        if (code !== 0) {
          process.exitCode = code ?? 1;
          return;
        }
        assert.equal(upload, null);
        assert.equal(buffered.length, 0);
        assert.equal(held, null);
        assert.equal(released, true);
        assert.equal(outputBeforeEof, true);
        assert.equal(openCalls, 2);
        assert.equal(unknownWrites, 1);
        assert.equal(unknownDetaches, 1);
        assert.deepEqual(Buffer.concat(delivered), Buffer.concat([Buffer.from(line), Buffer.from(binary)]));
        console.log(JSON.stringify({
          boundary: 'built-addon/scripted-controller',
          deliveredBytes: cursor, deliveredFrames: delivered.length, demands,
          maxFrameBytes, closeCalls, eofTransitions, detachCalls, refusedWrites,
          deliveryUnknownRefusals: unknownWrites,
          outputBeforeEof, statusWhileWaiting, released,
        }));
      });
    `;
    const node = Bun.spawn(['node', '--input-type=module', '--eval', controller], {
      cwd: join(import.meta.dir, '..'),
      stdin: 'ignore',
      stdout: 'pipe',
      stderr: 'pipe',
    });
    const [exitCode, stdout, stderr] = await Promise.all([
      node.exited,
      new Response(node.stdout).text(),
      new Response(node.stderr).text(),
    ]);

    expect({ exitCode, stdout: stdout.trim(), stderr }).toEqual({
      exitCode: 0,
      stdout: JSON.stringify({
        boundary: 'built-addon/scripted-controller',
        deliveredBytes: lineBytes + binaryBytes,
        deliveredFrames: 1 + binaryBytes / frameBytes,
        demands: 1 + binaryBytes / frameBytes,
        maxFrameBytes: frameBytes,
        closeCalls: 2,
        eofTransitions: 1,
        detachCalls: 2,
        refusedWrites: 1,
        deliveryUnknownRefusals: 1,
        outputBeforeEof: true,
        statusWhileWaiting: true,
        released: true,
      }),
      stderr: '',
    });
  });
});
