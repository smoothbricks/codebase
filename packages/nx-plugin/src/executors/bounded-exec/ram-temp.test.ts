import { describe, expect, it } from 'bun:test';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

import {
  createHostCommands,
  type HostCommands,
  imagesBackedUnder,
  operationDisk,
  type RamTempPaths,
  RamTempVolume,
  ramTempPaths,
  volumeNameOf,
} from './ram-temp.js';

const MIB = 1024 * 1024;

describe('RAM temp volume output parsing', () => {
  it('names the disk diskutil apfs reports it created', () => {
    const container =
      'Creating APFS Container\nCreated new APFS Container disk271\nDisk from APFS operation: disk271\nFinished APFS operation on disk270\n';
    expect(operationDisk(container)).toBe('disk271');
    expect(operationDisk('Exporting new APFS Volume\nDisk from APFS operation: disk271s1\n')).toBe('disk271s1');
    expect(operationDisk('Error: -69808\n')).toBeNull();
  });

  it('reads the volume name diskutil info prints', () => {
    expect(volumeNameOf('   Device Identifier:         disk271s1\n   Volume Name:               smoo-ram-501\n')).toBe(
      'smoo-ram-501',
    );
    expect(volumeNameOf('   Device Identifier:         disk270\n')).toBeNull();
  });

  it('finds only the images whose backing file is on the volume', () => {
    const registry = [
      ...registryDevice('file:///private/tmp/smoo-ram-501/501-a1b2c3/store/main%20copy.asif', 'disk280'),
      ...registryDevice('file:///private/tmp/smoo-ram-5010/x.asif', 'disk281'),
      ...registryDevice('ram://134217728', 'disk270'),
    ].join('\n');
    expect(imagesBackedUnder(registry, '/private/tmp/smoo-ram-501/')).toEqual([
      '/private/tmp/smoo-ram-501/501-a1b2c3/store/main copy.asif (/dev/disk280)',
    ]);
  });

  it('keeps the mountpoint short enough to hold sockets', () => {
    // sun_path is 104 bytes; a lease directory leaves room for nested fixture paths.
    expect(ramTempPaths(501).mountpoint).toBe('/private/tmp/smoo-ram-501');
  });
});

describe('RAM temp volume on this host', () => {
  it('shares one volume between leases and detaches it when the last one ends', async () => {
    const parent = mkdtempSync(join(process.env.TMPDIR ?? '/private/tmp', 'rt-'));
    const name = `smoo-ram-test-${process.pid}`;
    const paths: RamTempPaths = {
      mountpoint: join(parent, 'v'),
      lockFile: join(parent, 'v.lock'),
      stateFile: join(parent, 'v.device'),
      volumeName: name,
    };
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
    const first = new RamTempVolume(paths, 256 * MIB, commands, 101, (pid) => alive.has(pid));
    const second = new RamTempVolume(paths, 256 * MIB, commands, 202, (pid) => alive.has(pid));
    try {
      const a = await first.acquire();
      if (!a.ok) {
        // A cowshed sandbox cannot attach images: the creation step says so instead of leasing.
        expect(a.error).toMatchObject({ kind: 'provision-failed', step: 'hdiutil attach' });
        return;
      }
      expect(a.value.kind).toBe('leased');
      const b = await second.acquire();
      if (a.value.kind !== 'leased' || !b.ok || b.value.kind !== 'leased') {
        throw new Error(`second lease failed: ${JSON.stringify(b)}`);
      }
      const leaseA = a.value.lease;
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
      const third = new RamTempVolume(paths, 256 * MIB, commands, 101, (pid) => alive.has(pid));
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
      const fourth = new RamTempVolume(paths, 256 * MIB, commands, 101, (pid) => alive.has(pid));
      const d = await fourth.acquire();
      if (!d.ok || d.value.kind !== 'leased') {
        throw new Error(`fourth lease failed: ${JSON.stringify(d)}`);
      }
      expect(d.value.held).toEqual([]);
      expect(existsSync(leaseB.directory)).toBe(false);
      expect(await fourth.release(d.value.lease)).toEqual({ ok: true, value: undefined });
      expect(existsSync(paths.mountpoint)).toBe(false);
      expect(existsSync(paths.stateFile)).toBe(false);
    } finally {
      // A failed assertion must not leave a RAM disk attached.
      const state = existsSync(paths.stateFile) ? readFileSync(paths.stateFile, 'utf8') : '';
      const physical = /"physical":"(\/dev\/disk\d+)"/.exec(state)?.[1];
      if (physical) {
        execFileSync('/usr/bin/hdiutil', ['detach', '-force', physical]);
      }
      rmSync(parent, { recursive: true, force: true });
    }
    // A RAM volume's attach/format/mount and its detach: ~3 s of DiskArbitration.
  }, 30_000);
});

/** One attached image as `ioreg -r -c AppleDiskImageDevice -l -w0` lists it. */
function registryDevice(url: string, disk: string): string[] {
  return [
    '+-o AppleDiskImageDevice@5d6  <class AppleDiskImageDevice, id 0x100659c38, registered, matched, active>',
    `  |   "DiskImageURL" = "${url}"`,
    '  +-o IOBlockStorageDriver  <class IOBlockStorageDriver, id 0x100659c3a, registered>',
    '    +-o Apple Disk Image Media  <class IOMedia, id 0x100659c3b, registered>',
    `      |   "BSD Name" = "${disk}"`,
    '        +-o Untitled 1@1  <class IOMedia, id 0x10037ce47, registered>',
    `          |   "BSD Name" = "${disk}s1"`,
  ];
}
