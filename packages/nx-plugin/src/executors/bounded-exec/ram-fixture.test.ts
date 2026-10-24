import { describe, expect, it } from 'bun:test';
import { GATEWAY_SOCKET } from './gateway-lease.js';
import { diskCommand, type RamTempPaths } from './ram-temp.js';
import { fixtureRamVolume } from './ram-temp-fixtures.js';

const paths: RamTempPaths = {
  root: '/private/var/folders/cowshed/999999',
  uid: 999999,
  mountpoint: '/private/var/folders/cowshed/999999/t/fixture-volume',
  staging: '/private/var/folders/cowshed/999999/t/fixture-volume-new',
  lockFile: '/tmp/fixture-volume.lock',
  volumeName: 'fixture-volume',
};
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
  it('refuses the production gateway before creating a host command', () => {
    expect(() =>
      fixtureRamVolume(
        { paths, capacity: 64 * 1024 * 1024, owned: '/tmp/fixture-owned.jsonl', gatewaySocket: GATEWAY_SOCKET },
        undefined,
      ),
    ).toThrow('gatewaySocket');
  });
});
