import { spawn } from 'node:child_process';
import { appendFileSync, mkdirSync, mkdtempSync, readFileSync, rmdirSync, rmSync } from 'node:fs';
import { userInfo } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { GATEWAY_SOCKET } from './gateway-lease.js';
import {
  createHostCommands,
  describeRamTempError,
  detachRamDisk,
  type HostCommands,
  RAM_TEMP_CAPACITY_BYTES,
  type RamDisk,
  type RamTempPaths,
  RamTempVolume,
  ramDisksIn,
  ramTempPaths,
} from './ram-temp.js';

const MIB = 1024 * 1024;

/** One attached image as `ioreg -r -c AppleDiskImageDevice -l -w0` lists it. */
export function registryDevice(url: string, disk: string): string[] {
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

/** A host proof's RAM volume: where it lives, its size, and the record of the disks its attaches made. */
export interface ScratchVolume {
  readonly parent: string;
  readonly paths: RamTempPaths;
  readonly capacity: number;
  readonly owned: string;
  readonly gatewayRoot: string;
  readonly gatewaySocket: string;
}

/**
 * A volume of its own for one host proof: a lock beside it, and a name and a size no other test process uses, since the
 * size and the user are what identify a RAM disk as this volume's. It mounts in `t` below the per-user root `cowshed
 * setup` makes, as the user's own volume mounts below that root, so mds keeps no right for it
 * (specs/cowshed/01a_apfs.md, "Spotlight media references"). `index` is 0 to 3: the size spends 4 sectors per pid.
 * `owned` records the disks the volume's attaches made, by identity: the only disks its teardown may detach.
 */
export function scratchVolume(index: 0 | 1 | 2 | 3): ScratchVolume {
  const parent = mkdtempSync(join(process.env.TMPDIR ?? '/tmp', 'rt-'));
  const gatewayRoot = mkdtempSync(join(process.env.COWSHED_RUNTIME_LINK ?? '/tmp', 'csrg-'));
  const gatewaySocket = join(gatewayRoot, 'gateway.sock');
  if (gatewaySocket === GATEWAY_SOCKET) throw new Error('a fixture cannot own the host gateway');
  const { uid } = userInfo();
  const root = `/private/var/folders/cowshed/${uid}`;
  const fixtures = join(root, 't');
  try {
    mkdirSync(fixtures, { mode: 0o700 });
  } catch (error) {
    if (!(error instanceof Error && 'code' in error && error.code === 'EEXIST')) {
      throw error;
    }
  }
  const name = `smoo-ram-test-${process.pid}-${index}`;
  return {
    parent,
    gatewayRoot,
    gatewaySocket,
    paths: {
      root,
      uid,
      mountpoint: join(fixtures, name),
      lockFile: join(parent, 'v.lock'),
      volumeName: name,
      staging: join(fixtures, `${name}-new`),
    },
    capacity: 64 * MIB + (process.pid * 4 + index + 1) * 512,
    owned: join(parent, 'owned-disks.jsonl'),
  };
}

/** Start the prebuilt real gateway; readiness and exit are observed, never polled or faked. */
export async function startFixtureGateway(volume: ScratchVolume): Promise<() => Promise<void>> {
  const binary = fileURLToPath(new URL('../../../../cowshed/.cache/ram-fixture/ram_fixture', import.meta.url));
  const child = spawn(binary, [volume.gatewayRoot], { stdio: ['pipe', 'pipe', 'pipe'] });
  let stderr = '';
  let stdout = '';
  child.stderr.setEncoding('utf8').on('data', (text: string) => {
    stderr += text;
    process.stderr.write(text);
  });
  const exited = new Promise<void>((resolve, reject) => {
    child.once('error', reject);
    child.once('close', (code, signal) => {
      if (code === 0) resolve();
      else reject(new Error(`fixture gateway ended (${String(code ?? signal)}): ${stderr}`));
    });
  });
  // Install a rejection handler before readiness: early helper exit is the startup failure.
  const readiness = new Promise<void>((resolve, reject) => {
    child.stdout.setEncoding('utf8').on('data', (text: string) => {
      stdout += text;
      const newline = stdout.indexOf('\n');
      if (newline < 0) return;
      try {
        const answer: unknown = JSON.parse(stdout.slice(0, newline));
        if (
          typeof answer !== 'object' ||
          answer === null ||
          !('socket' in answer) ||
          answer.socket !== volume.gatewaySocket
        ) {
          throw new Error(`fixture gateway answered another socket: ${stdout}`);
        }
        process.stderr.write(`fixture gateway ready ${JSON.stringify(answer)}\n`);
        resolve();
      } catch (error) {
        reject(error);
      }
    });
    void exited.then(() => reject(new Error(`fixture gateway exited before readiness: ${stdout}${stderr}`)), reject);
  });
  try {
    await readiness;
  } catch (error) {
    child.stdin.end();
    await exited.catch((exitError: unknown) => process.stderr.write(`${String(exitError)}\n`));
    rmSync(volume.gatewayRoot, { recursive: true, force: true });
    throw error;
  }
  return async () => {
    child.stdin.end();
    await exited;
    rmSync(volume.gatewayRoot, { recursive: true, force: true });
  };
}

/**
 * Remove what a failed run left of the scratch volume's directories. Teardown must not hide the failure it follows,
 * so a directory `rmdirSync` refuses, one still mounted on, is said on stderr instead of thrown.
 */
export function removeScratchDirectories(paths: RamTempPaths): void {
  for (const directory of [paths.mountpoint, paths.staging]) {
    try {
      rmdirSync(directory);
    } catch (error) {
      if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) {
        process.stderr.write(`scratch volume teardown: rmdir ${directory}: ${String(error)}\n`);
      }
    }
  }
}

/**
 * This test's size of RAM disks, read from the I/O Registry as the module reads them: `hdiutil info` drops attached disks.
 * A guard for assertions only: a disk of the size is no proof it is the test's, so nothing detaches by it.
 */
export async function attachedDisks(paths: RamTempPaths, capacity: number, gatewaySocket: string): Promise<RamDisk[]> {
  const host = createHostCommands(undefined, gatewaySocket);
  const registry = await host.run('/usr/sbin/ioreg', ['-r', '-c', 'IOHDIXHDDriveOutKernel', '-l', '-w0']);
  const table = await host.run('/sbin/mount', []);
  if (registry.status !== 0 || table.status !== 0) {
    throw new Error(
      `inventory RAM disks: ioreg exit ${registry.status}: ${registry.stderr}; mount exit ${table.status}: ${table.stderr}`,
    );
  }
  const disks = ramDisksIn(registry.stdout, table.stdout);
  if (!disks.ok) {
    throw new Error(`inventory RAM disks: ${disks.error}`);
  }
  return disks.value.filter((disk) => disk.uid === paths.uid && disk.sectors === capacity / 512);
}

/** A disk a fixture attached, by the identity the I/O Registry gave it right after the attach. */
interface OwnedDisk {
  readonly entry: string;
  readonly device: string;
  readonly sectors: number;
}

/** The disks `owned` records; none when nothing was attached. A malformed record refuses the teardown. */
function ownedDisks(owned: string): OwnedDisk[] {
  let text: string;
  try {
    text = readFileSync(owned, 'utf8');
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
      return [];
    }
    throw error;
  }
  return text
    .split('\n')
    .filter((line) => line !== '')
    .map((line) => {
      const parsed: unknown = JSON.parse(line);
      if (
        typeof parsed === 'object' &&
        parsed !== null &&
        'entry' in parsed &&
        typeof parsed.entry === 'string' &&
        'device' in parsed &&
        typeof parsed.device === 'string' &&
        'sectors' in parsed &&
        typeof parsed.sectors === 'number'
      ) {
        return { entry: parsed.entry, device: parsed.device, sectors: parsed.sectors };
      }
      throw new Error(`${owned} holds a record that is no owned disk: ${line}`);
    });
}

/** Exact custody comes only from this fixture's attach receipts, never a size or name filter. */
export function ownsRecordedDisk(owned: string, disk: RamDisk): boolean {
  return ownedDisks(owned).some(
    (record) => record.entry === disk.entry && record.device === disk.physical && record.sectors === disk.sectors,
  );
}

/**
 * Detach the disks `owned` records, each only while a fresh registry read still lists its entry with the same whole disk,
 * size and owner `uid`; the detach is bound to that entry. A disk already gone needs nothing. Any other listing is
 * refused and reported on stderr, outside the fixture's directories, and never detached; the refusals are thrown once
 * every record has been seen, so the caller removes nothing that holds the evidence.
 */
export async function detachOwned(owned: string, uid: number, gatewaySocket: string): Promise<void> {
  const records = ownedDisks(owned);
  if (records.length === 0) {
    return;
  }
  const host = createHostCommands(undefined, gatewaySocket);
  const registry = await host.run('/usr/sbin/ioreg', ['-r', '-c', 'IOHDIXHDDriveOutKernel', '-l', '-w0']);
  const table = await host.run('/sbin/mount', []);
  if (registry.status !== 0 || table.status !== 0) {
    throw new Error(
      `inventory RAM disks: ioreg exit ${registry.status}: ${registry.stderr}; mount exit ${table.status}: ${table.stderr}`,
    );
  }
  const disks = ramDisksIn(registry.stdout, table.stdout);
  if (!disks.ok) {
    throw new Error(`inventory RAM disks: ${disks.error}`);
  }
  const refused: string[] = [];
  for (const record of records) {
    const disk = disks.value.find((candidate) => candidate.entry === record.entry);
    if (disk === undefined) {
      continue;
    }
    const { physical } = disk;
    if (physical === null || physical !== record.device || disk.sectors !== record.sectors || disk.uid !== uid) {
      refused.push(
        `registry entry ${record.entry} now lists ${physical ?? 'no disk'} of ${disk.sectors} sectors (uid ${disk.uid}), not the ${record.device} of ${record.sectors} sectors this fixture attached: not detached`,
      );
      continue;
    }
    const detached = await detachRamDisk(host, { ...disk, physical }, 'teardown detach');
    if (!detached.ok) {
      refused.push(describeRamTempError(detached.error));
    }
  }
  if (refused.length > 0) {
    process.stderr.write(`fixture teardown of ${owned}:\n${refused.join('\n')}\n`);
    throw new Error(refused.join('; '));
  }
}

/**
 * An attach through this fixture's real gateway. Right after the answer it reads the disk's registry entry and
 * appends the exact attached identity to `owned`,
 * the only record its teardown detaches from (`detachOwned`). The user's volume is attached by the gateway
 * (`ram-attach`), so that no job's end takes it from another job's lease.
 */
export function recordedRamAttach(
  attach: HostCommands['attachRam'],
  run: HostCommands['run'],
  owned: string,
): HostCommands['attachRam'] {
  return async (sectors) => {
    const attached = await attach(sectors);
    if (!attached.ok) return attached;
    const device = attached.value;
    const registry = await run('/usr/sbin/ioreg', ['-r', '-c', 'IOHDIXHDDriveOutKernel', '-l', '-w0']);
    const table = await run('/sbin/mount', []);
    const disks =
      registry.status === 0 && table.status === 0
        ? ramDisksIn(registry.stdout, table.stdout)
        : { ok: false as const, error: `ioreg exit ${registry.status}, mount exit ${table.status}` };
    const listed = disks.ok ? disks.value.filter((disk) => disk.physical === device) : [];
    const [disk] = listed;
    if (listed.length !== 1 || disk === undefined || disk.sectors !== sectors || disk.uid !== userInfo().uid) {
      return {
        ok: false,
        error: `hdiutil attached ${device}, but the I/O Registry lists ${disks.ok ? JSON.stringify(listed) : disks.error} for it: not recorded, so no teardown detaches it`,
      };
    }
    const identity = { entry: disk.entry, device, sectors };
    appendFileSync(owned, `${JSON.stringify(identity)}\n`, { mode: 0o600 });
    process.stderr.write(`fixture attached ${JSON.stringify(identity)}\n`);
    return { ok: true, value: device };
  };
}

/**
 * A host proof's RAM volume: `volume`'s paths and size, its disk commands through `cowshed`, and its disk attached by the
 * fixture's own gateway and recorded in `volume.owned`. It refuses the user's own volume, by path or by size: no test leases,
 * creates or detaches that one.
 */
export function fixtureRamVolume(
  volume: {
    readonly paths: RamTempPaths;
    readonly capacity: number;
    readonly owned: string;
    readonly gatewaySocket: string;
  },
  cowshed: string | undefined,
): RamTempVolume {
  const own = ramTempPaths({ uid: volume.paths.uid });
  const shared = (['mountpoint', 'staging', 'lockFile', 'volumeName'] as const).filter(
    (field) => volume.paths[field] === own[field],
  );
  if (shared.length > 0 || volume.capacity === RAM_TEMP_CAPACITY_BYTES || volume.gatewaySocket === GATEWAY_SOCKET) {
    throw new Error(
      `a fixture RAM volume names the user's own volume (${[...shared, ...(volume.capacity === RAM_TEMP_CAPACITY_BYTES ? ['capacity'] : []), ...(volume.gatewaySocket === GATEWAY_SOCKET ? ['gatewaySocket'] : [])].join(', ')}); a test owns its own`,
    );
  }
  const host = createHostCommands(undefined, volume.gatewaySocket, undefined, cowshed);
  return new RamTempVolume(volume.paths, volume.capacity, {
    run: host.run,
    detachRam: host.detachRam,
    ownsRamDisk: (disk) => ownsRecordedDisk(volume.owned, disk),
    attachRam: recordedRamAttach(host.attachRam, host.run, volume.owned),
  });
}
