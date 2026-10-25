import { describe, expect, it } from 'bun:test';
import { diskCommand, RAM_TEMP_CAPACITY_BYTES, ramTempPaths, testNamespace } from './ram-temp.js';
import { fixtureRamVolume } from './ram-temp-fixtures.js';

const socket = '/tmp/fixture-gateway/gateway.sock';

describe('fixture gateway custody', () => {
  it('routes image and SAFE facade operations to the selected gateway', () => {
    expect(
      diskCommand('/usr/bin/hdiutil', ['detach', '/dev/disk7'], 300, { registryEntry: '0x17' }, socket)?.args,
    ).toEqual([
      'disk-command',
      'hdiutil',
      '--gateway-socket',
      socket,
      '--registry-entry',
      '0x17',
      '--',
      'detach',
      '/dev/disk7',
    ]);
    expect(diskCommand('/usr/sbin/diskutil', ['info', '/dev/disk7'], 300, undefined, socket)?.args).toEqual([
      'disk-command',
      'diskutil',
      '--gateway-socket',
      socket,
      '--deadline-ms',
      '300',
      '--',
      'info',
      '/dev/disk7',
    ]);
  });
  it("refuses the size of the user's own volume, the one name a test namespace does not derive", () => {
    const volume = {
      paths: ramTempPaths({ uid: 999999 }, testNamespace(process.pid, 0)),
      owned: '/tmp/fixture-owned.jsonl',
      gatewaySocket: socket,
    };
    expect(() => fixtureRamVolume({ ...volume, capacity: 64 * 1024 * 1024 }, undefined)).not.toThrow();
    expect(() => fixtureRamVolume({ ...volume, capacity: RAM_TEMP_CAPACITY_BYTES }, undefined)).toThrow(
      "user's own volume",
    );
  });
});
