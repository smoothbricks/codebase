import { afterEach, describe, expect, it } from 'bun:test';
import { spawn } from 'node:child_process';
import {
  describeRemoteHttpTimeout,
  REMOTE_HTTP_BOUND_MS,
  type RemoteHttpTimers,
  remoteChild,
  remoteRequest,
} from './remote-http.js';

/** A clock that moves only when the test fires the one armed timer, as the bound passing. */
function manualTimers() {
  let now = 0;
  let armed: { at: number; fire: () => void } | null = null;
  const timers: RemoteHttpTimers = {
    now: () => now,
    after(ms, fire) {
      const timer = { at: now + ms, fire };
      armed = timer;
      return () => {
        if (armed === timer) armed = null;
      };
    },
  };
  return {
    timers,
    armed: () => armed !== null,
    /** Move the clock to the armed timer's deadline and fire it. */
    expire() {
      const timer = armed;
      if (timer === null) throw new Error('no timer is armed');
      armed = null;
      now = timer.at;
      timer.fire();
    },
  };
}

let stop: (() => void) | null = null;
afterEach(() => {
  stop?.();
  stop = null;
});

/** A stand-in remote service; `received` resolves once it holds the request. */
function serve(answer: (received: () => void) => Response | Promise<Response>) {
  const { promise: received, resolve } = Promise.withResolvers<void>();
  const server = Bun.serve({ port: 0, fetch: () => answer(resolve) });
  stop = () => server.stop(true);
  return { url: `http://127.0.0.1:${server.port}/repos/o/r/releases`, received };
}

describe('remoteRequest', () => {
  it('answers TIMEOUT at the remote-http bound when the service never answers', async () => {
    const clock = manualTimers();
    const service = serve((received) => {
      received();
      return new Promise<Response>(() => {});
    });
    const answer = remoteRequest('read release', service.url, { method: 'GET' }, clock.timers);
    await service.received;
    clock.expire();
    const result = await answer;
    expect(result).toEqual({
      ok: false,
      error: {
        code: 'TIMEOUT',
        op: 'read release',
        peer: new URL(service.url).origin,
        boundMs: REMOTE_HTTP_BOUND_MS,
        elapsedMs: REMOTE_HTTP_BOUND_MS,
        last: 'nothing',
      },
    });
    if (result.ok) throw new Error('unreachable');
    expect(describeRemoteHttpTimeout(result.error)).toBe(
      `TIMEOUT: read release: ${new URL(service.url).origin} gave no whole answer within the remote-http bound of ${REMOTE_HTTP_BOUND_MS} ms (waited ${REMOTE_HTTP_BOUND_MS} ms; last progress: nothing)`,
    );
  });

  it('answers the whole body and disarms its timer', async () => {
    const clock = manualTimers();
    const service = serve(() => new Response('{"id":7}', { status: 201 }));
    const result = await remoteRequest('create release', service.url, { method: 'POST', body: '{}' }, clock.timers);
    expect(result).toEqual({ ok: true, value: { status: 201, ok: true, body: '{"id":7}' } });
    expect(clock.armed()).toBe(false);
  });

  it('lets a transport failure through as fetch raised it', async () => {
    const clock = manualTimers();
    const service = serve(() => new Response(''));
    const url = service.url;
    stop?.();
    stop = null;
    await expect(remoteRequest('read release', url, { method: 'GET' }, clock.timers)).rejects.toThrow();
    expect(clock.armed()).toBe(false);
  });
});

describe('remoteChild', () => {
  it('answers TIMEOUT at the remote-http bound and leaves the client it started running', async () => {
    const clock = manualTimers();
    const child = spawn('sleep', ['600'], { stdio: 'ignore' });
    const answer = remoteChild('gh secret list', child, clock.timers);
    const { promise: closed, resolve } = Promise.withResolvers<NodeJS.Signals | null>();
    child.once('close', (_code, signal) => resolve(signal));
    await new Promise((settle) => child.once('spawn', settle));
    clock.expire();
    expect(await answer).toEqual({
      ok: false,
      error: {
        code: 'TIMEOUT',
        op: 'gh secret list',
        peer: 'sleep',
        boundMs: REMOTE_HTTP_BOUND_MS,
        elapsedMs: REMOTE_HTTP_BOUND_MS,
        last: 'spawned',
      },
    });
    expect([child.exitCode, child.signalCode]).toEqual([null, null]);
    child.kill('SIGKILL');
    expect(await closed).toBe('SIGKILL');
  });

  it('answers the exit of a client that finished and disarms its timer', async () => {
    const clock = manualTimers();
    const child = spawn('sh', ['-c', 'exit 3'], { stdio: 'ignore' });
    expect(await remoteChild('gh api', child, clock.timers)).toEqual({ ok: true, value: { code: 3, signal: null } });
    expect(clock.armed()).toBe(false);
  });

  it('rejects with the spawn error of a client that cannot start', async () => {
    const clock = manualTimers();
    const child = spawn('/nonexistent/gh', [], { stdio: 'ignore' });
    await expect(remoteChild('gh api', child, clock.timers)).rejects.toThrow('ENOENT');
    expect(clock.armed()).toBe(false);
  });
});
