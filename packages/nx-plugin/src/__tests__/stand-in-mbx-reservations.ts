import { once } from 'node:events';
import { rm } from 'node:fs/promises';
import { createServer, type Socket } from 'node:net';
import { join } from 'node:path';
import typia from 'typia';
import type { MbxPriority } from '../executors/bounded-exec/mbx-pool.js';
import { ownedSocketDirectory } from '../testing.js';

/**
 * A line a client writes to `mbx reservations` (mbx's docs/scheduling.md, "Serve reservations to many commands"):
 * exactly these fields, as the host's installed mbx accepts them.
 */
type HeardLine = { type: 'reserve'; id: number; cpus: number; priority: MbxPriority } | { type: 'release'; id: number };

const validLine = typia.createValidateEquals<HeardLine>();

/** `raw` as a line mbx accepts, or why mbx refuses it: an unknown field is refused as mbx's serde refuses it. */
function heardLine(raw: string): HeardLine | { readonly refused: string } {
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch (error) {
    return { refused: `not a reservation request: ${String(error)}` };
  }
  const valid = validLine(value);
  return valid.success
    ? valid.data
    : {
        refused: `not a reservation request: ${valid.errors
          .map((error) => `${error.path} must be ${error.expected}`)
          .join('; ')}`,
      };
}

/** One `reserve` line the stand-in heard, and the connection it came on, counted from one. */
export interface HeardReserve {
  readonly id: number;
  readonly cpus: number;
  readonly priority: MbxPriority;
  readonly connection: number;
}

/** The stand-in's answer to a reserve: the cores it grants, a refusal's error, or none, as a full pool keeps it waiting. */
export type StandInAnswer = number | { readonly refused: string } | null;

export interface StandInReservations {
  /** The socket a run publishes in `MBX_RESERVATIONS_SOCKET`. */
  readonly socket: string;
  /** Every reserve line heard, in order. */
  readonly reserves: readonly HeardReserve[];
  /** The ids of every release line heard, in order. */
  readonly releases: readonly number[];
  /** Connections accepted so far. */
  connections(): number;
  /** Settles once `count` release lines have been heard. */
  released(count: number): Promise<void>;
  close(): Promise<void>;
}

/**
 * mbx's reservation endpoint at its published interface: a Unix socket that answers each `reserve` line with
 * `answer`'s grant or refusal and records every connection, reserve and release it hears. It stands in for
 * `mbx reservations`, mbx's own and tested in mbx's suite, so a leaf suite pins only what bounded-exec asks and does
 * with the answer.
 */
export async function standInReservations(
  answer: (heard: HeardReserve) => StandInAnswer,
): Promise<StandInReservations> {
  const directory = await ownedSocketDirectory('mbx-r-stand-in-', 'r.sock');
  const socket = join(directory, 'r.sock');
  const reserves: HeardReserve[] = [];
  const releases: number[] = [];
  const waiting: { count: number; heard: () => void }[] = [];
  const open = new Set<Socket>();
  let connections = 0;
  const server = createServer((client) => {
    const connection = ++connections;
    open.add(client);
    client.on('close', () => open.delete(client));
    client.on('error', () => client.destroy());
    client.setEncoding('utf8');
    let unread = '';
    client.on('data', (text: string) => {
      unread += text;
      for (let end = unread.indexOf('\n'); end !== -1; end = unread.indexOf('\n')) {
        const line = heardLine(unread.slice(0, end));
        unread = unread.slice(end + 1);
        if ('refused' in line) {
          // mbx answers a line it does not accept with an error and ends the connection.
          client.end(`${JSON.stringify({ type: 'error', error: line.refused })}\n`);
          return;
        }
        if (line.type === 'release') {
          releases.push(line.id);
          for (const wait of waiting.filter((wait) => releases.length >= wait.count)) wait.heard();
          continue;
        }
        const heard = { id: line.id, cpus: line.cpus, priority: line.priority, connection };
        reserves.push(heard);
        const said = answer(heard);
        if (said === null) continue;
        client.write(
          `${JSON.stringify(
            typeof said === 'number'
              ? { type: 'granted', id: line.id, cpus: said }
              : { type: 'refused', id: line.id, error: said.refused },
          )}\n`,
        );
      }
    });
  });
  server.listen(socket);
  await once(server, 'listening');
  return {
    socket,
    reserves,
    releases,
    connections: () => connections,
    released: (count) =>
      releases.length >= count ? Promise.resolve() : new Promise<void>((heard) => waiting.push({ count, heard })),
    async close() {
      for (const client of open) client.destroy();
      server.close();
      await rm(directory, { recursive: true, force: true });
    },
  };
}
