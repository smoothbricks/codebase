import { describe, expect, it } from 'bun:test';
import { once } from 'node:events';
import { rmSync } from 'node:fs';
import { createServer, type Socket } from 'node:net';
import { join } from 'node:path';
import { BOUNDED_TEST_IDLE_TIMEOUT_MS } from '../../test-backstop.js';
import { ownedSocketDirectory } from '../../testing.js';
import { detachGatewayRamDisk, readDetachAnswer } from './ram-detach.js';

describe('gateway-owned RAM detach outcome', () => {
  it('distinguishes native terminal status, proven unstarted refusal, and unknown custody', () => {
    expect(
      readDetachAnswer(
        JSON.stringify({
          ok: true,
          ramDetach: {
            outcome: 'terminal',
            status: { exit: 16 },
            stdout: '',
            stderr: 'busy',
            stdoutTruncated: false,
            stderrTruncated: false,
          },
        }),
      ),
    ).toEqual({ status: 16, stdout: '', stderr: 'busy' });
    expect(
      readDetachAnswer(
        JSON.stringify({
          ok: true,
          ramDetach: {
            outcome: 'terminal',
            status: { signal: 9 },
            stdout: '',
            stderr: '',
            stdoutTruncated: false,
            stderrTruncated: false,
          },
        }),
      )?.status,
    ).toBe(137);
    expect(
      readDetachAnswer(
        JSON.stringify({ ok: true, ramDetach: { outcome: 'not-started', reason: 'entry changed; nothing detached' } }),
      )?.status,
    ).toBe(103);
    expect(
      readDetachAnswer(JSON.stringify({ ok: true, ramDetach: { outcome: 'unknown', reason: 'wait failed' } })),
    ).toBeNull();
    expect(readDetachAnswer('{}')).toBeNull();
  });
  it('reads past the gateway heartbeat lines that precede a slow detach answer', async () => {
    // 05_gateway.md "Deadlines": a control answer not ready yet is preceded by {"ok":true,"progress":n} lines.
    const root = await ownedSocketDirectory('rd-', 'gw.sock');
    const socket = join(root, 'gw.sock');
    const server = createServer({ allowHalfOpen: true }, (client) => {
      client.on('end', () => {
        client.write(`${JSON.stringify({ ok: true, progress: 1 })}\n`);
        client.write(`${JSON.stringify({ ok: true, progress: 2 })}\n`);
        client.end(
          `${JSON.stringify({ ok: true, ramDetach: { outcome: 'terminal', status: { exit: 0 }, stdout: 'detached', stderr: '', stdoutTruncated: false, stderrTruncated: false } })}\n`,
        );
      });
      client.resume();
    });
    server.listen(socket);
    await once(server, 'listening');
    try {
      const output = await detachGatewayRamDisk(socket, '/dev/disk7', '0x17', BOUNDED_TEST_IDLE_TIMEOUT_MS);
      expect(output).toEqual({ status: 0, stdout: 'detached', stderr: '' });
    } finally {
      server.close();
      await once(server, 'close');
      rmSync(root, { recursive: true, force: true });
    }
  });
  it('retains the exact request socket for a late gateway terminal receipt after bounded caller wait', async () => {
    const root = await ownedSocketDirectory('rd-', 'gw.sock');
    const socket = join(root, 'gw.sock');
    const request = Promise.withResolvers<{ client: Socket; line: string }>();
    const server = createServer({ allowHalfOpen: true }, (client) => {
      let line = '';
      client.on('data', (chunk: Buffer) => {
        line += chunk.toString('utf8');
      });
      client.on('end', () => request.resolve({ client, line }));
    });
    server.listen(socket);
    await once(server, 'listening');
    try {
      const output = detachGatewayRamDisk(socket, '/dev/disk7', '0x17', 1);
      const received = await request.promise;
      expect(JSON.parse(received.line)).toEqual({ op: 'ram-detach', device: '/dev/disk7', registryEntry: '0x17' });
      const bounded = await output;
      expect(bounded.pending).toBeDefined();
      expect(received.client.destroyed).toBe(false);
      received.client.end(
        `${JSON.stringify({ ok: true, ramDetach: { outcome: 'terminal', status: { exit: 0 }, stdout: 'detached', stderr: '', stdoutTruncated: false, stderrTruncated: false } })}\n`,
      );
      expect(await bounded.pending?.completion).toEqual({ status: 0, stdout: 'detached', stderr: '' });
    } finally {
      server.close();
      await once(server, 'close');
      rmSync(root, { recursive: true, force: true });
    }
  });
});
