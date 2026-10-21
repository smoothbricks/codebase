import { createServer, type Server, type Socket } from 'node:net';
import type { RetryClock } from '../executors/bounded-exec/cpu-tokens.js';

/**
 * Helpers for tests that play the gateway (or a CPU pool) on a real unix socket. Each gateway answer is a set of lines the
 * client reads as one reply: the queued line, then the decision. A grant's connection stays open, as the gateway's does,
 * so the grant lives until its client releases it or the stand-in closes.
 */

/** Every connection a stand-in has accepted, so that closing the stand-in can end the grants a test left open. */
const clientsOf = new WeakMap<Server, Set<Socket>>();

/** Listen on `socket` as a gateway stand-in that runs `serve` per client, until the caller closes it. */
export async function listen(socket: string, serve: (client: Socket) => void): Promise<Server> {
  const clients = new Set<Socket>();
  const server = createServer({ allowHalfOpen: true }, (client) => {
    clients.add(client);
    client.once('close', () => clients.delete(client));
    serve(client);
  });
  clientsOf.set(server, clients);
  const { promise, resolve, reject } = Promise.withResolvers<void>();
  server.once('error', reject);
  server.listen(socket, resolve);
  await promise;
  server.off('error', reject);
  return server;
}

/**
 * Stop a gateway stand-in, ending the connections it holds, and wait until its socket is gone, so the path can be bound
 * again. A stand-in already stopped is left as it is.
 */
export async function closeServer(server: Server): Promise<void> {
  if (!server.listening) return;
  for (const client of clientsOf.get(server) ?? []) client.destroy();
  const { promise, resolve } = Promise.withResolvers<void>();
  server.close(() => resolve());
  await promise;
}

/**
 * A gateway that answers each connection with the next of `replies`. The connection stays open after the answer, as a
 * grant's does; a refusal's close is not waited for, since the client settles on the refusal's line.
 */
export function scripted(replies: readonly string[]): (client: Socket) => void {
  const queue = [...replies];
  return (client) => {
    client.once('data', () => client.write(queue.shift() ?? ''));
  };
}

/** A clock the test moves by hand: each `sleep` advances `now` by its milliseconds, then runs `between`. */
export function fakeClock(between: () => Promise<void> = () => Promise.resolve()): RetryClock {
  let time = 0;
  return {
    now: () => time,
    async sleep(ms) {
      time += ms;
      await between();
    },
  };
}

/** A gateway's answer to a cut-off ask: the queued line, then the install that restarted it, then the close. */
export function restartedBy(pid: number, servesWithinMs = 180_000): string {
  return (
    '{"ok":true,"lease":"queued"}\n' +
    `{"ok":false,"code":"install-restart","error":"the gateway restarted under install ${pid}","install":{"pid":${pid},"binary":"/abs/cowshed","servesWithinMs":${servesWithinMs}}}\n`
  );
}

/** A gateway's answer to a grant of two tokens: queued, then granted. */
export const GRANT_TWO = '{"ok":true,"lease":"queued"}\n{"ok":true,"lease":"granted","tokens":2}\n';

/** A gateway's answer to a grant of three tokens: queued, then granted. */
export const GRANT_THREE = '{"ok":true,"lease":"queued"}\n{"ok":true,"lease":"granted","tokens":3}\n';
