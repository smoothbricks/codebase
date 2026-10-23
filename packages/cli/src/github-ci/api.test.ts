import { afterEach, describe, expect, it } from 'bun:test';
import { createServer, Server as HttpServer, type IncomingMessage } from 'node:http';
import { createServer as createNetServer, type Server as NetServer } from 'node:net';
import { recordingRetryShell } from '../lib/__tests__/recording-retry-shell.js';
import { ciApiRequest, postCiStatus } from './api.js';

const SHA = 'a'.repeat(40);
const STATUS = { state: 'success', context: 'Deploy Production', description: 'Deploy Production passed' } as const;

/** One scripted answer of the loopback forge. */
interface Answer {
  status: number;
  headers?: Record<string, string>;
}

interface Forge {
  environment: NodeJS.ProcessEnv;
  /** `METHOD path` of every request the forge received, in order. */
  requests: string[];
  bodies: unknown[];
}

const servers: Array<HttpServer | NetServer> = [];

afterEach(async () => {
  await Promise.all(
    servers.splice(0).map(
      (server) =>
        new Promise<void>((resolve) => {
          server.close(() => resolve());
          if (server instanceof HttpServer) server.closeAllConnections();
        }),
    ),
  );
});

/** Starts `server` on a loopback port, closes it after the test, and returns the environment that points CI at it. */
async function listen(server: HttpServer | NetServer): Promise<NodeJS.ProcessEnv> {
  servers.push(server);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (address === null || typeof address === 'string') throw new Error('loopback server did not report a port');
  const origin = `http://127.0.0.1:${address.port}`;
  return {
    GITHUB_SERVER_URL: origin,
    GITHUB_API_URL: origin,
    GITHUB_REPOSITORY: 'owner/repo',
    GITHUB_TOKEN: 'token-1',
  };
}

/** A real HTTP server on loopback answering each request with the next scripted answer (the last repeats). */
async function forge(answers: Answer[]): Promise<Forge> {
  const requests: string[] = [];
  const bodies: unknown[] = [];
  const server = createServer((request: IncomingMessage, response) => {
    const chunks: Buffer[] = [];
    request.on('data', (chunk: Buffer) => chunks.push(chunk));
    request.on('end', () => {
      requests.push(`${request.method} ${request.url}`);
      const text = Buffer.concat(chunks).toString('utf8');
      bodies.push(text === '' ? undefined : JSON.parse(text));
      const answer = answers[Math.min(requests.length - 1, answers.length - 1)];
      response.writeHead(answer.status, { 'Content-Type': 'application/json', ...answer.headers });
      response.end(JSON.stringify({ id: 1 }));
    });
  });
  return { requests, bodies, environment: await listen(server) };
}

/** A loopback server whose first connection is reset before any HTTP answer and whose later ones answer 201. */
async function resetOnce(): Promise<{ environment: NodeJS.ProcessEnv; connections: () => number }> {
  let connections = 0;
  const server = createNetServer((socket) => {
    connections += 1;
    const first = connections === 1;
    socket.on('error', () => undefined);
    socket.once('data', () => {
      if (first) socket.resetAndDestroy();
      else socket.end('HTTP/1.1 201 Created\r\nContent-Length: 0\r\nConnection: close\r\n\r\n');
    });
  });
  return { environment: await listen(server), connections: () => connections };
}

describe('postCiStatus', () => {
  it('retries a 500 and succeeds on the next answer, logging exactly one retry', async () => {
    const { environment, requests, bodies } = await forge([{ status: 500 }, { status: 201 }]);
    const { shell, sleeps, retries } = recordingRetryShell();

    await postCiStatus(SHA, STATUS, environment, shell);

    expect(requests).toEqual([`POST /repos/owner/repo/statuses/${SHA}`, `POST /repos/owner/repo/statuses/${SHA}`]);
    expect(bodies).toEqual([STATUS, STATUS]);
    expect(sleeps).toHaveLength(1);
    expect(retries).toHaveLength(1);
    expect(retries[0]?.reason).toContain('HTTP 500');
  });

  it('gives up after a bounded number of attempts and names the status POST and the HTTP code', async () => {
    const { environment, requests } = await forge([{ status: 502 }]);
    const { shell, sleeps, retries } = recordingRetryShell();

    const failure = await postCiStatus(SHA, STATUS, environment, shell).then(
      () => null,
      (error: unknown) => error,
    );

    if (!(failure instanceof Error)) throw new Error('expected the status POST to fail');
    const { message } = failure;
    expect(message).toContain(`POST /statuses/${SHA}`);
    expect(message).toContain('HTTP 502');
    expect(message).toContain('Deploy Production');
    expect(requests).toHaveLength(5);
    // Exponential backoff from one second, between the five attempts and never after the last.
    expect(sleeps).toEqual([1000, 2000, 4000, 8000]);
    expect(retries).toHaveLength(4);
  });

  it('does not retry a 4xx: the forge already gave its verdict', async () => {
    const { environment, requests } = await forge([{ status: 422 }]);
    const { shell, sleeps, retries } = recordingRetryShell();

    await expect(postCiStatus(SHA, STATUS, environment, shell)).rejects.toThrow('HTTP 422');

    expect(requests).toHaveLength(1);
    expect(sleeps).toEqual([]);
    expect(retries).toEqual([]);
  });

  it('does not retry a refused redirect: that is not a transport failure', async () => {
    const { environment, requests } = await forge([
      { status: 302, headers: { Location: 'http://127.0.0.1:1/elsewhere' } },
    ]);
    const { shell, sleeps, retries } = recordingRetryShell();

    await expect(postCiStatus(SHA, STATUS, environment, shell)).rejects.toThrow('Could not record commit status');

    expect(requests).toHaveLength(1);
    expect(sleeps).toEqual([]);
    expect(retries).toEqual([]);
  });

  it('retries a connection reset, which no HTTP answer announced', async () => {
    const { environment, connections } = await resetOnce();
    const { shell, sleeps } = recordingRetryShell();

    await postCiStatus(SHA, STATUS, environment, shell);

    expect(connections()).toBe(2);
    expect(sleeps).toHaveLength(1);
  });

  it('waits at least as long as Retry-After asks', async () => {
    const { environment } = await forge([{ status: 503, headers: { 'Retry-After': '7' } }, { status: 201 }]);
    const { shell, sleeps } = recordingRetryShell();

    await postCiStatus(SHA, STATUS, environment, shell);

    expect(sleeps).toEqual([7000]);
  });

  it('stops retrying when Retry-After asks for more than the retry bound allows', async () => {
    const { environment, requests } = await forge([
      { status: 503, headers: { 'Retry-After': '600' } },
      { status: 201 },
    ]);
    const { shell, sleeps } = recordingRetryShell();

    await expect(postCiStatus(SHA, STATUS, environment, shell)).rejects.toThrow('HTTP 503');

    expect(requests).toHaveLength(1);
    expect(sleeps).toEqual([]);
  });
});

describe('ciApiRequest', () => {
  it('sends a request nobody opted in to retry exactly once: a workflow dispatch is not idempotent', async () => {
    const { environment, requests } = await forge([{ status: 500 }, { status: 204 }]);

    await expect(
      ciApiRequest('/actions/workflows/ci.yml/dispatches', 'POST', { ref: 'main' }, environment),
    ).rejects.toThrow('HTTP 500');

    expect(requests).toEqual(['POST /repos/owner/repo/actions/workflows/ci.yml/dispatches']);
  });
});
