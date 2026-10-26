import { afterEach, describe, expect, it, jest } from 'bun:test';
import { rmSync } from 'node:fs';
import { mkdtemp, readFile } from 'node:fs/promises';
import { createServer, type Server, type Socket } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { BOUNDED_TEST_IDLE_TIMEOUT_MS } from '../../bounded-test-policy.js';
import { ownedSocketDirectory } from '../../testing.js';
import { diskClassOf, takeDiskLease, unixMicros, waitRecord } from './disk-lease.js';
import { holdLease, type LeaseBounds } from './gateway-lease.js';

/**
 * The leg's backstop for each lease bound these cases are not about. The stand-ins answer at once, refuse at once, or
 * are absent, so no such bound decides a verdict; a case that did wait on one would hang, and the leg's idle timeout
 * ends the run (packages/nx-plugin/README.md, "Bounded Test Targets"). `grantMs: null` waits for as long as the gateway
 * keeps the request queued.
 */
const BACKSTOP: LeaseBounds = {
  ackMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
  afterCloseMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
  grantMs: null,
};

const servers: Server[] = [];
const directories: string[] = [];

afterEach(() => {
  jest.useRealTimers();
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

/** A fresh directory under the OS temp dir, removed after the test. */
async function scratchDirectory(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), 'disk-lease-record-'));
  directories.push(directory);
  return directory;
}

describe('cowshed disk-lifecycle lease client', () => {
  it('classes programs by what they change', () => {
    expect(diskClassOf('/usr/sbin/diskutil')).toBe('storage');
    expect(diskClassOf('/usr/bin/hdiutil')).toBe('storage');
    expect(diskClassOf('/sbin/mount_apfs')).toBe('namespace');
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
    const lease = await takeDiskLease(socket, 'storage', '/usr/bin/hdiutil detach /dev/disk9', BACKSTOP, 'shared');
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
    // The ack bound is the subject: this gateway answers nothing until it reads EOF, so only the expiry of the ack bound
    // half-closes the request. The bound is armed on connect, right after the request is written, and the read below
    // happens before the advance, so the advance expires an armed ack bound and no other bound is due.
    jest.useFakeTimers();
    const { promise: requested, resolve: sawRequest } = Promise.withResolvers<void>();
    const socket = await gateway((client) => {
      client.once('data', () => sawRequest());
      client.on('end', () =>
        client.end('{"ok":false,"code":"invalid-request","error":"unknown gateway control operation"}\n'),
      );
    });
    const ackMs = 200;
    const lease = takeDiskLease(socket, 'namespace', '/sbin/umount /x', { ...BACKSTOP, ackMs }, 'shared');
    await requested;
    jest.advanceTimersByTime(ackMs);
    expect(await lease).toEqual({
      granted: false,
      cause: 'predates',
      reason: `the lease endpoint ${socket} predates disk leases (invalid-request: unknown gateway control operation); restart it with \`cowshed setup\``,
    });
  });

  it('asks for the whole phase by naming its share, and only then', async () => {
    const { promise: request, resolve: requested } = Promise.withResolvers<string>();
    const socket = await gateway((client) => {
      client.once('data', (chunk) => {
        requested(chunk.toString('utf8'));
        client.write('{"ok":true,"lease":"queued"}\n{"ok":true,"lease":"granted"}\n');
      });
    });
    const lease = await takeDiskLease(socket, 'storage', '/usr/bin/hdiutil detach /dev/disk9', BACKSTOP, 'alone');
    expect(await request).toBe(
      '{"op":"disk-lease","class":"storage","share":"alone","command":"/usr/bin/hdiutil detach /dev/disk9"}\n',
    );
    expect(lease.granted).toBe(true);
    if (lease.granted) {
      lease.release();
    }
  });

  it('recognizes a gateway that predates the share field by its refusal at once', async () => {
    const socket = await gateway((client) => {
      client.once('data', () =>
        client.end(
          '{"ok":false,"code":"invalid-request","error":"gateway control encoding failed: gateway control request contains an unknown field"}\n',
        ),
      );
    });
    const lease = await takeDiskLease(socket, 'storage', 'x', BACKSTOP, 'alone');
    expect(lease.granted === false && lease.cause).toBe('predates-field');
  });

  it('runs unleased, said as absent, when no gateway listens', async () => {
    const lease = await takeDiskLease(await socketPath('none.sock'), 'storage', 'x', BACKSTOP, 'shared');
    expect(lease.granted === false && lease.cause).toBe('absent');
  });

  it('runs unleased when the grant never comes', async () => {
    // The grant bound is the subject, so this case advances a clock it owns. `holdLease` arms the grant bound when the
    // gateway's `queued` is read, and `sawQueued` runs in that same read, so the advance below expires an armed grant
    // bound and nothing else is due: the grant never comes, so only that expiry ends the wait.
    jest.useFakeTimers();
    const { promise: queued, resolve: sawQueued } = Promise.withResolvers<void>();
    const socket = await gateway((client) => {
      client.once('data', () => client.write('{"ok":true,"lease":"queued"}\n'));
    });
    const lease = holdLease(
      socket,
      { op: 'disk-lease', class: 'storage', command: 'x' },
      'disk leases',
      { ...BACKSTOP, grantMs: 500 },
      () => sawQueued(),
    );
    await queued;
    jest.advanceTimersByTime(500);
    expect(await lease).toEqual({
      granted: false,
      cause: 'other',
      reason: `the lease endpoint ${socket} granted nothing within 500 ms`,
    });
  });

  it('appends a start and an end line per wait, numbered per record and stamped in microseconds', async () => {
    const file = join(await scratchDirectory(), 'waits');
    const record = waitRecord(file);

    const before = unixMicros();
    record.begin('storage')();
    record.begin('namespace')();
    const after = unixMicros();

    const text = await readFile(file, 'utf8');
    expect(text.endsWith('\n')).toBe(true);
    const lines = text.slice(0, -1).split('\n');
    // The stamp follows the seq on every line; blanking it leaves each line's shape exact.
    const shapes = lines.map((line) => line.replace(/^(\S+ \S+ \S+) \d+/, '$1 <stamp>'));
    expect(shapes).toEqual([
      `start ${process.pid} 0 <stamp> storage`,
      `end ${process.pid} 0 <stamp>`,
      `start ${process.pid} 1 <stamp> namespace`,
      `end ${process.pid} 1 <stamp>`,
    ]);
    for (const line of lines) {
      const stamp = line.split(' ')[3];
      expect(stamp).toMatch(/^\d+$/);
      expect(Number(stamp)).toBeGreaterThanOrEqual(before);
      expect(Number(stamp)).toBeLessThanOrEqual(after);
    }
  });

  it('never fails a wait for a record it cannot write', async () => {
    // A directory cannot be appended to, so every write to this record fails.
    const record = waitRecord(await scratchDirectory());
    expect(() => record.begin('storage')()).not.toThrow();
    expect(() => record.begin('namespace')()).not.toThrow();
  });
});
