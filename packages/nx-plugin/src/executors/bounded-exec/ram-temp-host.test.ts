import { describe, expect, it } from 'bun:test';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmdirSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { userInfo } from 'node:os';
import { join } from 'node:path';
import {
  createHostCommands,
  type HostCommands,
  operationDisk,
  type RamDisk,
  type RamTempError,
  type RamTempPaths,
  RamTempVolume,
  ramDisksIn,
} from './ram-temp.js';
import { registryDevice } from './ram-temp-fixtures.js';

const MIB = 1024 * 1024;

/**
 * A volume of its own for one test: a lock beside it, and a name and a size no other test
 * process uses, since the size and the user are what identify a RAM disk as this volume's.
 */
function scratchVolume(index: number): { parent: string; paths: RamTempPaths; capacity: number } {
  const parent = mkdtempSync(join(process.env.TMPDIR ?? '/tmp', 'rt-'));
  const name = `smoo-ram-test-${process.pid}-${index}`;
  return {
    parent,
    paths: {
      mountpoint: join('/Volumes', name),
      lockFile: join(parent, 'v.lock'),
      volumeName: name,
      stagingName: `${name}-new`,
      user: userInfo().username,
    },
    capacity: 64 * MIB + (process.pid * 4 + index + 1) * 512,
  };
}

function attachedDisks(paths: RamTempPaths, capacity: number): RamDisk[] {
  let info: string;
  try {
    info = execFileSync('/usr/bin/hdiutil', ['info'], { encoding: 'utf8' });
  } catch (error) {
    // A host without hdiutil has no RAM disk attached.
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
      return [];
    }
    throw error;
  }
  return ramDisksIn(info).filter((disk) => disk.user === paths.user && disk.sectors === capacity / 512);
}

function detachAll(paths: RamTempPaths, capacity: number): void {
  for (const disk of attachedDisks(paths, capacity)) {
    execFileSync('/usr/bin/hdiutil', ['detach', '-force', disk.physical]);
  }
}

/** A cowshed sandbox cannot attach images: creation fails at the host instead of leasing. */
function expectSandboxed(error: RamTempError): void {
  expect(error.kind).toBe('provision-failed');
  if (error.kind === 'provision-failed') {
    expect(['hdiutil info', 'hdiutil attach']).toContain(error.step);
  }
}

describe('RAM temp volume on this host', () => {
  it('reclaims what a dead creator left, shares the volume between leases, and detaches it at the last release, never mounting beside it as "<name> 1"', async () => {
    const { parent, paths, capacity } = scratchVolume(1);
    // The host's own commands, with one injectable image in the I/O Registry listing.
    const host = createHostCommands();
    const attachedBelow: { image: string | null } = { image: null };
    const commands: HostCommands = {
      async run(file, args) {
        const output = await host.run(file, args);
        return file === '/usr/sbin/ioreg' && attachedBelow.image !== null
          ? {
              ...output,
              stdout: [output.stdout, ...registryDevice(`file://${attachedBelow.image}`, 'disk999')].join('\n'),
            }
          : output;
      },
    };
    const alive = new Set([101, 202]);
    const first = new RamTempVolume(paths, capacity, commands, 101, (pid) => alive.has(pid));
    const second = new RamTempVolume(paths, capacity, commands, 202, (pid) => alive.has(pid));
    const run = async (file: string, args: string[]): Promise<string> => {
      const output = await host.run(file, args);
      if (output.status !== 0) {
        throw new Error(`${file} ${args.join(' ')}: exit ${output.status}: ${output.stderr}`);
      }
      return output.stdout;
    };
    const volume = new RamTempVolume(paths, capacity, host);
    try {
      // The debris: a volume mounted at the mountpoint by a creator that died before its marker
      // (the order this module used to create in), and a RAM disk whose deadlined attach finished
      // with nobody left to format it.
      const sectors = `ram://${capacity / 512}`;
      const orphan = await host.run('/usr/bin/hdiutil', ['attach', '-nomount', sectors]);
      if (orphan.status !== 0) {
        const acquired = await volume.acquire();
        expect(acquired.ok).toBe(false);
        if (!acquired.ok) {
          expectSandboxed(acquired.error);
        }
        return;
      }
      const physical = /^(\/dev\/disk\d+)\b/.exec(orphan.stdout)?.[1] ?? '';
      const container = operationDisk(await run('/usr/sbin/diskutil', ['apfs', 'createContainer', physical])) ?? '';
      const orphanVolume =
        operationDisk(
          await run('/usr/sbin/diskutil', ['apfs', 'addVolume', container, 'APFS', paths.volumeName, '-nomount']),
        ) ?? '';
      await run('/usr/sbin/diskutil', ['mount', '-mountOptions', 'nobrowse', orphanVolume]);
      await run('/usr/bin/hdiutil', ['attach', '-nomount', sectors]);
      expect(attachedDisks(paths, capacity).map((disk) => disk.mountpoints)).toEqual([[paths.mountpoint], []]);

      // An unmarked volume that carries a lease is someone's TMPDIR: it is named, not detached.
      const lease = join(paths.mountpoint, `${process.pid}-abcdef`);
      mkdirSync(lease);
      const refused = await volume.acquire();
      expect(refused).toEqual({
        ok: false,
        error: {
          kind: 'provision-failed',
          step: 'reclaim',
          detail: `${physical} at ${paths.mountpoint} has no marker but carries leases ${process.pid}-abcdef; not detached`,
        },
      });
      expect(existsSync(lease)).toBe(true);
      rmdirSync(lease);

      const a = await first.acquire();
      if (!a.ok || a.value.kind !== 'leased') {
        throw new Error(`lease over the debris failed: ${JSON.stringify(a)}`);
      }
      expect(a.value.kind).toBe('leased');
      expect(a.value.lease.mountpoint).toBe(paths.mountpoint);
      expect(readFileSync(join(paths.mountpoint, '.smoo-ram'), 'utf8')).toBe(`${paths.volumeName}\n`);
      expect(existsSync(`${paths.mountpoint} 1`)).toBe(false);
      expect(attachedDisks(paths, capacity).map((disk) => disk.mountpoints)).toEqual([[paths.mountpoint]]);

      const leaseA = a.value.lease;
      const b = await second.acquire();
      if (!b.ok || b.value.kind !== 'leased') {
        throw new Error(`second lease failed: ${JSON.stringify(b)}`);
      }
      const leaseB = b.value.lease;
      expect(leaseA.mountpoint).toBe(paths.mountpoint);
      expect(leaseB.mountpoint).toBe(paths.mountpoint);
      expect(leaseA.directory).not.toBe(leaseB.directory);
      expect(statSync(leaseA.directory).dev).not.toBe(statSync(parent).dev);
      writeFileSync(join(leaseA.directory, 'scratch'), 'on RAM');

      expect(await first.release(leaseA)).toEqual({ ok: true, value: undefined });
      expect(existsSync(leaseA.directory)).toBe(false);
      expect(statSync(leaseB.directory).dev).not.toBe(statSync(parent).dev);

      // The second task dies without releasing, with an image still attached from its lease: the
      // registry entry is injected, so the test spends no attach of its own. The next task keeps
      // the lease and names it instead of detaching another owner's image.
      alive.delete(202);
      const image = join(leaseB.directory, 'store', 'main.asif');
      attachedBelow.image = image;
      const third = new RamTempVolume(paths, capacity, commands, 101, (pid) => alive.has(pid));
      const c = await third.acquire();
      if (!c.ok || c.value.kind !== 'leased') {
        throw new Error(`third lease failed: ${JSON.stringify(c)}`);
      }
      expect(c.value.held).toEqual([
        `${leaseB.directory} is kept: ${image} (/dev/disk999) still attached from below it; detach through its owner`,
      ]);
      expect(existsSync(leaseB.directory)).toBe(true);
      expect(await third.release(c.value.lease)).toEqual({ ok: true, value: undefined });
      expect(existsSync(c.value.lease.directory)).toBe(false);
      expect(statSync(leaseB.directory).dev).not.toBe(statSync(parent).dev);

      // Once its owner has detached the image, the next task reclaims the lease, and its release,
      // the last, detaches the volume.
      attachedBelow.image = null;
      const fourth = new RamTempVolume(paths, capacity, commands, 101, (pid) => alive.has(pid));
      const d = await fourth.acquire();
      if (!d.ok || d.value.kind !== 'leased') {
        throw new Error(`fourth lease failed: ${JSON.stringify(d)}`);
      }
      expect(d.value.held).toEqual([]);
      expect(existsSync(leaseB.directory)).toBe(false);
      expect(await fourth.release(d.value.lease)).toEqual({ ok: true, value: undefined });
      expect(existsSync(paths.mountpoint)).toBe(false);
      expect(attachedDisks(paths, capacity)).toEqual([]);
    } finally {
      detachAll(paths, capacity);
      rmSync(parent, { recursive: true, force: true });
    }
  }, 60_000);
});
