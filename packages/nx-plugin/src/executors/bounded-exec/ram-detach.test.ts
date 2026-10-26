import { describe, expect, it } from 'bun:test';
import { once } from 'node:events';
import { rmSync } from 'node:fs';
import { createServer, type Socket } from 'node:net';
import { join } from 'node:path';
import { ownedSocketDirectory } from '../../testing.js';
import {
  detachGatewayRamDisk,
  HEARTBEAT_SILENCE_MS,
  readDetachAnswer,
  type SilenceTimers,
  UNPROVEN_SILENCE_MS,
} from './ram-detach.js';

const TERMINAL = {
  ok: true,
  ramDetach: {
    outcome: 'terminal',
    status: { exit: 0 },
    stdout: 'detached',
    stderr: '',
    stdoutTruncated: false,
    stderrTruncated: false,
  },
};

/**
 * A clock that moves only when the test fires the armed silence timer. `nextArm()` resolves with the bound of the next
 * timer the client arms: the client's own sign that it has read what the stand-in wrote.
 */
function manualTimers() {
  let now = 0;
  let armed: { ms: number; at: number; fire: () => void } | null = null;
  let onArm: ((ms: number) => void) | null = null;
  const timers: SilenceTimers = {
    now: () => now,
    after(ms, fire) {
      const timer = { ms, at: now + ms, fire };
      armed = timer;
      onArm?.(ms);
      onArm = null;
      return () => {
        if (armed === timer) armed = null;
      };
    },
  };
  return {
    timers,
    armedMs: () => armed?.ms ?? null,
    nextArm(): Promise<number> {
      const { promise, resolve } = Promise.withResolvers<number>();
      onArm = resolve;
      return promise;
    },
    /** Move the clock to the armed timer's deadline and fire it. */
    expire() {
      const timer = armed;
      if (timer === null) throw new Error('no silence timer is armed');
      armed = null;
      now = timer.at;
      timer.fire();
    },
  };
}

/** A stand-in gateway that hands each accepted request (its line, once the client half-closed) to the test. */
async function standIn() {
  const root = await ownedSocketDirectory('rd-', 'gw.sock');
  const socket = join(root, 'gw.sock');
  const request = Promise.withResolvers<{ client: Socket; line: string }>();
  const accepted: Socket[] = [];
  const server = createServer({ allowHalfOpen: true }, (client) => {
    accepted.push(client);
    let line = '';
    client.on('data', (chunk: Buffer) => {
      line += chunk.toString('utf8');
    });
    client.on('end', () => request.resolve({ client, line }));
  });
  server.listen(socket);
  await once(server, 'listening');
  return {
    socket,
    request: request.promise,
    async close() {
      for (const client of accepted) client.destroy();
      server.close();
      await once(server, 'close');
      rmSync(root, { recursive: true, force: true });
    },
  };
}

/** Write `bytes` and wait until the kernel took them, so the next write reaches the client as its own chunk. */
function written(client: Socket, bytes: string | Buffer): Promise<void> {
  const { promise, resolve } = Promise.withResolvers<void>();
  client.write(bytes, () => resolve());
  return promise;
}

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
    const gateway = await standIn();
    try {
      const clock = manualTimers();
      const output = detachGatewayRamDisk(gateway.socket, '/dev/disk7', '0x17', clock.timers);
      const { client, line } = await gateway.request;
      expect(JSON.parse(line)).toEqual({ op: 'ram-detach', device: '/dev/disk7', registryEntry: '0x17' });
      client.write(`${JSON.stringify({ ok: true, progress: 1 })}\n`);
      client.write(`${JSON.stringify({ ok: true, progress: 2 })}\n`);
      client.end(`${JSON.stringify(TERMINAL)}\n`);
      expect(await output).toEqual({ status: 0, stdout: 'detached', stderr: '' });
      expect(clock.armedMs()).toBeNull();
    } finally {
      await gateway.close();
    }
  });

  it('fails SILENT when the gateway writes nothing for the unproven silence, naming socket, bound and last progress', async () => {
    const gateway = await standIn();
    try {
      const clock = manualTimers();
      const output = detachGatewayRamDisk(gateway.socket, '/dev/disk7', '0x17', clock.timers);
      await gateway.request;
      expect(clock.armedMs()).toBe(UNPROVEN_SILENCE_MS);
      clock.expire();
      const silent = await output;
      expect(silent.status).toBe(1);
      expect(silent.stderr).toBe(
        `SILENT: ram-detach: the gateway at ${gateway.socket} wrote no heartbeat or answer for ${UNPROVEN_SILENCE_MS} ms (waited ${UNPROVEN_SILENCE_MS} ms; last progress: request sent); the gateway's detach keeps running, not killed`,
      );
      expect(silent.pending).toBeDefined();
    } finally {
      await gateway.close();
    }
  });

  it('holds a gateway that has heartbeated to the heartbeat silence, reset by each heartbeat', async () => {
    const gateway = await standIn();
    try {
      const clock = manualTimers();
      const output = detachGatewayRamDisk(gateway.socket, '/dev/disk7', '0x17', clock.timers);
      const { client } = await gateway.request;
      const first = clock.nextArm();
      await written(client, `${JSON.stringify({ ok: true, progress: 1 })}\n`);
      expect(await first).toBe(HEARTBEAT_SILENCE_MS);
      const second = clock.nextArm();
      await written(client, `${JSON.stringify({ ok: false, progress: 2 })}\n`);
      expect(await second).toBe(HEARTBEAT_SILENCE_MS);
      clock.expire();
      const silent = await output;
      expect(silent.status).toBe(1);
      expect(silent.stderr).toBe(
        `SILENT: ram-detach: the gateway at ${gateway.socket} wrote no heartbeat or answer for ${HEARTBEAT_SILENCE_MS} ms (waited ${HEARTBEAT_SILENCE_MS} ms; last progress: heartbeat 2); the gateway's detach keeps running, not killed`,
      );
      expect(silent.pending).toBeDefined();
    } finally {
      await gateway.close();
    }
  });

  it('takes a progress line with any other field as the answer, which proves no terminal outcome', async () => {
    const gateway = await standIn();
    try {
      const clock = manualTimers();
      const output = detachGatewayRamDisk(gateway.socket, '/dev/disk7', '0x17', clock.timers);
      const { client } = await gateway.request;
      const extra = JSON.stringify({ ok: true, progress: 1, ramDetach: null });
      client.end(`${extra}\n`);
      const answer = await output;
      expect(answer.status).toBe(1);
      expect(answer.stderr).toBe(`gateway detach reply proves no terminal outcome: ${extra}`);
      expect(await answer.pending?.completion).toBeNull();
    } finally {
      await gateway.close();
    }
  });

  it("reports a gateway's error answer as unknown custody, never a heartbeat or a terminal status", async () => {
    const gateway = await standIn();
    try {
      const clock = manualTimers();
      const output = detachGatewayRamDisk(gateway.socket, '/dev/disk7', '0x17', clock.timers);
      const { client } = await gateway.request;
      const refusal = JSON.stringify({ ok: false, code: 'internal', error: 'the disk runner failed' });
      client.write(`${JSON.stringify({ ok: true, progress: 1 })}\n`);
      client.end(`${refusal}\n`);
      const answer = await output;
      expect(answer.status).toBe(1);
      expect(answer.stderr).toBe(`gateway detach reply proves no terminal outcome: ${refusal}`);
    } finally {
      await gateway.close();
    }
  });

  it('reads an answer split across writes, inside a UTF-8 codepoint, whole', async () => {
    const gateway = await standIn();
    try {
      const clock = manualTimers();
      const output = detachGatewayRamDisk(gateway.socket, '/dev/disk7', '0x17', clock.timers);
      const { client } = await gateway.request;
      const answer = Buffer.from(
        `${JSON.stringify({ ...TERMINAL, ramDetach: { ...TERMINAL.ramDetach, stdout: '"disk7" éjecté' } })}\n`,
        'utf8',
      );
      const split = answer.indexOf(Buffer.from('é', 'utf8')) + 1;
      await written(client, answer.subarray(0, split));
      client.end(answer.subarray(split));
      expect(await output).toEqual({ status: 0, stdout: '"disk7" éjecté', stderr: '' });
    } finally {
      await gateway.close();
    }
  });

  it('retains the exact request socket for a late gateway terminal receipt after the silence ended the wait', async () => {
    const gateway = await standIn();
    try {
      const clock = manualTimers();
      const output = detachGatewayRamDisk(gateway.socket, '/dev/disk7', '0x17', clock.timers);
      const received = await gateway.request;
      clock.expire();
      const bounded = await output;
      expect(bounded.pending).toBeDefined();
      expect(received.client.destroyed).toBe(false);
      received.client.end(`${JSON.stringify(TERMINAL)}\n`);
      expect(await bounded.pending?.completion).toEqual({ status: 0, stdout: 'detached', stderr: '' });
    } finally {
      await gateway.close();
    }
  });
});
