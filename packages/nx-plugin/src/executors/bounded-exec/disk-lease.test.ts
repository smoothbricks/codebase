import { afterEach, describe, expect, it } from 'bun:test';
import { rmSync } from 'node:fs';
import { createServer, type Server, type Socket } from 'node:net';
import { join } from 'node:path';
import { ownedSocketDirectory } from '../../testing.js';
import { diskClassOf, takeDiskLease } from './disk-lease.js';

/**
 * Real, short bounds: the client's half-close and grant expiry are socket behavior against the
 * platform clock, which fake timers would not drive through a real Unix socket.
 */
const QUICK = { ackMs: 200, afterCloseMs: 5_000, grantMs: 500 };

const servers: Server[] = [];
const directories: string[] = [];

afterEach(() => {
  for (const server of servers.splice(0)) {
    server.close();
  }
  for (const directory of directories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

/**
 * A socket path in a directory of its own, made in the socket namespace the sandbox admits for Unix
 * sockets: a literal `/tmp` path binds, but no sandboxed process may connect to it, so a client of it
 * would see a refusal in place of the absent socket or the gateway the test means.
 */
async function socketPath(name: string): Promise<string> {
  const directory = await ownedSocketDirectory('dl-', name);
  directories.push(directory);
  return join(directory, name);
}

/** A gateway stand-in on a socket in a directory of its own that runs `serve` per client. */
async function gateway(serve: (client: Socket) => void): Promise<string> {
  const socket = await socketPath('gw.sock');
  const server = createServer({ allowHalfOpen: true }, serve);
  const { promise, resolve, reject } = Promise.withResolvers<void>();
  server.once('error', reject);
  server.listen(socket, resolve);
  await promise;
  server.off('error', reject);
  servers.push(server);
  return socket;
}

describe('cowshed disk-lifecycle lease client', () => {
  it('classes programs by what they change', () => {
    expect(diskClassOf('/usr/sbin/diskutil')).toBe('storage');
    expect(diskClassOf('/usr/bin/hdiutil')).toBe('storage');
    expect(diskClassOf('/sbin/umount')).toBe('namespace');
    expect(diskClassOf('/sbin/mount')).toBeNull();
    expect(diskClassOf('/usr/sbin/ioreg')).toBeNull();
  });

  it('asks with one line, waits for queued then granted, and holds until released', async () => {
    const { promise: request, resolve: requested } = Promise.withResolvers<string>();
    const { promise: closed, resolve: close } = Promise.withResolvers<void>();
    const socket = await gateway((client) => {
      client.once('data', (chunk) => {
        requested(chunk.toString('utf8'));
        client.write('{"ok":true,"lease":"queued"}\n{"ok":true,"lease":"granted"}\n');
      });
      // The gateway takes a lease back when it reads EOF. With allowHalfOpen the socket stays open
      // after the peer closes until the server ends it, so 'close' is not that signal on Linux.
      client.on('end', () => close());
    });
    const lease = await takeDiskLease(socket, 'storage', '/usr/bin/hdiutil detach /dev/disk9', QUICK);
    expect(await request).toBe(
      '{"op":"disk-lease","class":"storage","command":"/usr/bin/hdiutil detach /dev/disk9"}\n',
    );
    expect(lease.granted).toBe(true);
    if (lease.granted) {
      lease.release();
    }
    await closed;
  });

  it('recognizes a gateway that predates leases by its answer to the half-close', async () => {
    const socket = await gateway((client) => {
      client.resume();
      client.on('end', () =>
        client.end('{"ok":false,"code":"invalid-request","error":"unknown gateway control operation"}\n'),
      );
    });
    const lease = await takeDiskLease(socket, 'namespace', '/sbin/umount /x', QUICK);
    expect(lease).toEqual({
      granted: false,
      cause: 'predates',
      reason:
        'the gateway predates disk leases (invalid-request: unknown gateway control operation); restart it with `cowshed setup`',
    });
  });

  it('runs unleased, said as absent, when no gateway listens', async () => {
    const lease = await takeDiskLease(await socketPath('none.sock'), 'storage', 'x', QUICK);
    expect(lease.granted === false && lease.cause).toBe('absent');
  });

  it('runs unleased when the grant never comes', async () => {
    const socket = await gateway((client) => {
      client.once('data', () => client.write('{"ok":true,"lease":"queued"}\n'));
    });
    const lease = await takeDiskLease(socket, 'storage', 'x', QUICK);
    expect(lease).toEqual({ granted: false, cause: 'other', reason: 'the gateway granted nothing within 500 ms' });
  });
});
