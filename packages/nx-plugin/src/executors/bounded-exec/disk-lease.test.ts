import { afterEach, describe, expect, it } from 'bun:test';
import { rmSync } from 'node:fs';
import { createServer, type Server, type Socket } from 'node:net';
import { diskClassOf, takeDiskLease } from './disk-lease.js';

/**
 * Real, short bounds: the client's half-close and grant expiry are socket behavior against the
 * platform clock, which fake timers would not drive through a real Unix socket.
 */
const QUICK = { ackMs: 200, afterCloseMs: 5_000, grantMs: 500 };

const servers: Server[] = [];
const sockets: string[] = [];

afterEach(() => {
  for (const server of servers.splice(0)) {
    server.close();
  }
  for (const socket of sockets.splice(0)) {
    rmSync(socket, { force: true });
  }
});

/** A gateway stand-in on a socket under /private/tmp (sun_path is 104 bytes) that runs `serve` per client. */
async function gateway(serve: (client: Socket) => void): Promise<string> {
  const socket = `/private/tmp/smoo-dl-${process.pid}-${sockets.length}.sock`;
  rmSync(socket, { force: true });
  const server = createServer({ allowHalfOpen: true }, serve);
  const { promise, resolve } = Promise.withResolvers<void>();
  server.listen(socket, resolve);
  await promise;
  servers.push(server);
  sockets.push(socket);
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
      client.on('close', () => close());
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
    const lease = await takeDiskLease(`/private/tmp/smoo-dl-${process.pid}-none.sock`, 'storage', 'x', QUICK);
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
