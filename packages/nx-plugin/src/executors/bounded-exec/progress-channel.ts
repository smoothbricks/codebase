import { rm } from 'node:fs/promises';
import { createServer, type Server, type Socket } from 'node:net';
import { join } from 'node:path';
import { ownedSocketDirectory } from '../../testing.js';

/**
 * A run's progress channel: a running test reports its own forward motion on it, and each record restarts the idle
 * bound as a finished test does (specs/cowshed/08_testing.md, "Integration tests (real substrate)"). An agent harness
 * that enforces its own no-progress bound over what its run does (a span written, a byte of output) reports each
 * movement that check sees, so a long test that keeps moving is not ended because it is the last test still running.
 * CPU is never a record: a compile inside a test reports nothing and is still ended at the bound, where the pre-signal
 * snapshot names it (cpu-rise.ts).
 *
 * The channel is a Unix stream socket the executor serves for one run, in a fresh 0700 directory, and names in the
 * command's environment as {@link PROGRESS_SOCKET_ENV}. Each line a client writes is one record and must be
 * {@link PROGRESS_RECORD}; anything else is a broken reporter, said and failed, never read as progress. A client may
 * write one record per connection or many. The server ends its side when the client ends its own, after every record
 * the client wrote was read, so a client that waits for that end knows its records counted.
 */
export const PROGRESS_SOCKET_ENV = 'BOUNDED_EXEC_PROGRESS_SOCKET';
/** The one record: something the test watches moved. */
export const PROGRESS_RECORD = 'moved';

const CHANNEL_DIRECTORY = 'bounded-progress-';
const CHANNEL_SOCKET = 'p.sock';
/** How much of a refused line its refusal quotes. */
const QUOTED_CHARS = 80;

export interface ProgressChannel {
  /** The socket's path, as the command's environment names it. */
  readonly socket: string;
  /** Ends every connection and removes the socket's directory; the error that left something behind, or null. */
  close(): Promise<string | null>;
}

export interface ProgressHandlers {
  /** A record was read. */
  readonly moved: () => void;
  /** A client wrote something that is not a record; its connection is ended. */
  readonly invalid: (detail: string) => void;
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function quoted(line: string): string {
  return JSON.stringify(line.length > QUOTED_CHARS ? `${line.slice(0, QUOTED_CHARS)}…` : line);
}

/** Serve one run's progress channel; a channel that cannot be served is the run's refusal, never a silent bound. */
export async function serveProgressChannel(
  handlers: ProgressHandlers,
): Promise<{ ok: true; channel: ProgressChannel } | { ok: false; error: string }> {
  let directory: string;
  try {
    directory = await ownedSocketDirectory(CHANNEL_DIRECTORY, CHANNEL_SOCKET);
  } catch (error) {
    return { ok: false, error: `its directory could not be made: ${describe(error)}` };
  }
  const socketPath = join(directory, CHANNEL_SOCKET);
  const clients = new Set<Socket>();
  const serve = (client: Socket): void => {
    clients.add(client);
    client.on('close', () => clients.delete(client));
    // A client that dies mid-record (its test was killed) loses that record with its connection; nothing else is read.
    client.on('error', () => {});
    client.setEncoding('utf8');
    let partial = '';
    const refuse = (detail: string): void => {
      partial = '';
      client.destroy();
      handlers.invalid(detail);
    };
    client.on('data', (text: string) => {
      const lines = (partial + text).split('\n');
      partial = lines.pop() ?? '';
      for (const line of lines) {
        if (line !== PROGRESS_RECORD) {
          refuse(`a progress record is the line ${PROGRESS_RECORD}, not ${quoted(line)}`);
          return;
        }
        handlers.moved();
      }
      // No prefix longer than the record can still become one.
      if (partial.length > PROGRESS_RECORD.length) {
        refuse(`a progress record is the line ${PROGRESS_RECORD}, not ${quoted(partial)}`);
      }
    });
    client.on('end', () => {
      if (partial !== '') refuse(`a progress connection ended inside a record: ${quoted(partial)}`);
    });
  };
  const server: Server = createServer(serve);
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: listen is this process's own bind of a path in its fresh directory; the kernel answers it with the listening callback or an error, and no peer takes part
  const listened = await new Promise<Error | null>((settle) => {
    server.once('error', settle);
    server.listen(socketPath, () => {
      server.off('error', settle);
      settle(null);
    });
  });
  const close = async (): Promise<string | null> => {
    const failures: string[] = [];
    for (const client of clients) client.destroy();
    if (listened === null) {
      // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: every accepted connection was destroyed just above, so the server closes without waiting on a peer
      const closed = await new Promise<Error | undefined>((settle) => server.close(settle));
      if (closed !== undefined) failures.push(`its socket did not close: ${closed.message}`);
    }
    try {
      await rm(directory, { recursive: true, force: true });
    } catch (error) {
      failures.push(`its directory was not removed: ${describe(error)}`);
    }
    return failures.length === 0 ? null : `the progress channel at ${socketPath}: ${failures.join('; ')}`;
  };
  if (listened !== null) {
    const left = await close();
    return {
      ok: false,
      error: `it could not listen at ${socketPath}: ${listened.message}${left === null ? '' : `; ${left}`}`,
    };
  }
  server.on('error', (error) => {
    handlers.invalid(`the progress channel at ${socketPath} failed: ${error.message}`);
  });
  return { ok: true, channel: { socket: socketPath, close } };
}
