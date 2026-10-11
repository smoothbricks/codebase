import { describe, expect, it } from 'bun:test';
import { lstatSync, mkdirSync, mkdtempSync, realpathSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  type CacheResult,
  captureHeldVerdict,
  type HeldCandidate,
  type HeldContext,
  type HeldVerdict,
  type HolderStats,
  heldProverOn,
  parseHeldVerdict,
  validateHeldVerdict,
} from './ram-temp-held.js';

const RAM_DEVICE = 77n;
const context: HeldContext = { volumeKey: 'root 77:2:100 marker 77:9:100:200', leaseKey: '77:40:1', ramDevice: '77' };

const workspace: Extract<HeldCandidate, { kind: 'mount' }> = {
  kind: 'mount',
  mountpoint: '/Volumes/ram/1-abcdef/workspace',
  description: '/Volumes/ram/1-abcdef/workspace (mounted)',
};
const cache: Extract<HeldCandidate, { kind: 'mount' }> = {
  kind: 'mount',
  mountpoint: '/Volumes/ram/1-abcdef/cache',
  description: '/Volumes/ram/1-abcdef/cache (mounted)',
};
const image: HeldCandidate = {
  kind: 'image',
  imagePath: '/Volumes/ram/1-abcdef/fixture.sparseimage',
  diskPath: '/dev/disk42',
  description: '/Volumes/ram/1-abcdef/fixture.sparseimage (/dev/disk42)',
};

type Entry = { shape: 'file' | 'directory'; dev: bigint; ino: bigint; birthtimeNs: bigint };
type Table = Map<string, Entry | Error>;

/**
 * A host whose paths are a table: a missing row is ENOENT, an `Error` row is thrown as read.
 * `reads` records every path read.
 */
function tableHost(table: Table, reads: string[] = []) {
  return heldProverOn(async (path): Promise<HolderStats> => {
    reads.push(path);
    const row = table.get(path);
    if (row instanceof Error) {
      throw row;
    }
    if (!row) {
      throw Object.assign(new Error(`ENOENT: no such file or directory, lstat '${path}'`), { code: 'ENOENT' });
    }
    return { dev: row.dev, ino: row.ino, birthtimeNs: row.birthtimeNs, isDirectory: () => row.shape === 'directory' };
  });
}

/** A lease held by two foreign volumes mounted below it. */
function heldTable(): Table {
  return new Map<string, Entry | Error>([
    [workspace.mountpoint, { shape: 'directory', dev: 88n, ino: 2n, birthtimeNs: 6_000n }],
    [cache.mountpoint, { shape: 'directory', dev: 89n, ino: 2n, birthtimeNs: 6_100n }],
  ]);
}

function changed(table: Table, path: string, change: Partial<Entry>): void {
  const row = table.get(path);
  if (!row || row instanceof Error) {
    throw new Error(`no entry for ${path}`);
  }
  table.set(path, { ...row, ...change });
}

async function capturedOn(table: Table): Promise<HeldVerdict> {
  const captured = await tableHost(table).capture(context, [workspace, cache]);
  if (!captured.ok || !captured.value) {
    throw new Error(`no verdict: ${JSON.stringify(captured)}`);
  }
  return captured.value;
}

describe('RAM lease held verdicts', () => {
  it('keys every mount and stays valid while each is still mounted', async () => {
    const table = heldTable();
    const verdict = await capturedOn(table);
    expect(verdict).toEqual({
      version: 1,
      context,
      holders: [
        { ...workspace, root: { dev: '88', ino: '2', birthtimeNs: '6000' } },
        { ...cache, root: { dev: '89', ino: '2', birthtimeNs: '6100' } },
      ],
    });
    expect(await tableHost(table).validate(verdict, { ...context })).toEqual({ ok: true, value: true });
  });

  it('caches no lease an image holds, without reading the image or its disk node', async () => {
    const reads: string[] = [];
    const host = tableHost(heldTable(), reads);
    expect(await host.capture(context, [image])).toEqual({ ok: true, value: undefined });
    expect(await host.capture(context, [workspace, image, cache])).toEqual({ ok: true, value: undefined });
    expect(reads).not.toContain(image.imagePath);
    expect(reads).not.toContain(image.diskPath);
  });

  it('caches nothing when there is no holder or a mount is gone, leaving the inventory result', async () => {
    const table = heldTable();
    const host = tableHost(table);
    expect(await host.capture(context, [])).toEqual({ ok: true, value: undefined });
    table.delete(cache.mountpoint);
    expect(await host.capture(context, [workspace, cache])).toEqual({ ok: true, value: undefined });
    // Unmounted before it was keyed: the RAM directory beneath proves nothing.
    table.set(cache.mountpoint, { shape: 'directory', dev: RAM_DEVICE, ino: 41n, birthtimeNs: 1n });
    expect(await host.capture(context, [workspace, cache])).toEqual({ ok: true, value: undefined });
  });

  it('asks for a fresh inventory once any mount is gone or changed', async () => {
    const enotdir = Object.assign(new Error('ENOTDIR: not a directory'), { code: 'ENOTDIR' });
    const changes: [string, (table: Table) => void][] = [
      ['unmounted externally: RAM shows beneath', (table) => changed(table, cache.mountpoint, { dev: RAM_DEVICE })],
      ['another volume mounted there', (table) => changed(table, cache.mountpoint, { dev: 90n, birthtimeNs: 6_500n })],
      ['the same device with another root', (table) => changed(table, cache.mountpoint, { ino: 3n })],
      ['the same root reborn', (table) => changed(table, cache.mountpoint, { birthtimeNs: 6_101n })],
      ['no longer a directory', (table) => changed(table, cache.mountpoint, { shape: 'file' })],
      ['mountpoint removed', (table) => table.delete(cache.mountpoint)],
      ['a parent replaced by a file', (table) => table.set(cache.mountpoint, enotdir)],
    ];
    for (const [what, change] of changes) {
      const table = heldTable();
      const verdict = await capturedOn(table);
      change(table);
      expect([what, await tableHost(table).validate(verdict, context)]).toEqual([what, { ok: true, value: false }]);
    }
  });

  it('is about one volume incarnation and lease: any other context asks for a fresh inventory', async () => {
    const table = heldTable();
    const verdict = await capturedOn(table);
    for (const other of [
      { ...context, volumeKey: 'root 77:2:100 marker 77:9:101:201' },
      { ...context, leaseKey: '77:40:2' },
      { ...context, ramDevice: '78' },
    ]) {
      expect(await tableHost(table).validate(verdict, other)).toEqual({ ok: true, value: false });
    }
  });

  it('reports an unreadable mount as an error, never as a verdict', async () => {
    const table = heldTable();
    const verdict = await capturedOn(table);
    table.set(
      cache.mountpoint,
      Object.assign(new Error(`EACCES: permission denied, lstat '${cache.mountpoint}'`), { code: 'EACCES' }),
    );
    const expected: CacheResult<never> = {
      ok: false,
      error: `lstat ${cache.mountpoint}: EACCES: permission denied, lstat '${cache.mountpoint}'`,
    };
    expect(await tableHost(table).capture(context, [workspace, cache])).toEqual(expected);
    expect(await tableHost(table).validate(verdict, context)).toEqual(expected);
  });

  it('reads mounts without following links, and their owners churning entries keeps them valid', async () => {
    const root = mkdtempSync(join(realpathSync(tmpdir()), 'ram-held-'));
    try {
      const mounted = join(root, 'workspace');
      mkdirSync(mounted);
      const link = join(root, 'link');
      symlinkSync(mounted, link);
      const device = String(lstatSync(mounted).dev);
      // Seen from a RAM volume on another device, the directory stands in for a mounted volume.
      const elsewhere: HeldContext = { ...context, ramDevice: device === '1' ? '2' : '1' };
      const candidate: HeldCandidate = { kind: 'mount', mountpoint: mounted, description: `${mounted} (mounted)` };
      const captured = await captureHeldVerdict(elsewhere, [candidate]);
      if (!captured.ok || !captured.value) {
        throw new Error(`no verdict: ${JSON.stringify(captured)}`);
      }
      // Its owner's files coming and going move the root's ctime and mtime, not its identity.
      writeFileSync(join(mounted, 'scratch'), 'x');
      mkdirSync(join(mounted, 'build'));
      rmSync(join(mounted, 'scratch'));
      expect(await validateHeldVerdict(captured.value, elsewhere)).toEqual({ ok: true, value: true });
      // On the RAM volume's own device it is a plain directory, which proves nothing.
      expect(await captureHeldVerdict({ ...context, ramDevice: device }, [candidate])).toEqual({
        ok: true,
        value: undefined,
      });
      // A link to a directory is not a mount: no-follow sees the link.
      expect(await captureHeldVerdict(elsewhere, [{ ...candidate, mountpoint: link }])).toEqual({
        ok: true,
        value: undefined,
      });
      rmSync(mounted, { recursive: true });
      expect(await validateHeldVerdict(captured.value, elsewhere)).toEqual({ ok: true, value: false });
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});

describe('RAM lease held verdict wire', () => {
  it('round-trips through JSON to an equal verdict that still validates', async () => {
    const table = heldTable();
    const verdict = await capturedOn(table);
    const parsed = parseHeldVerdict(JSON.parse(JSON.stringify(verdict)));
    expect(parsed).toEqual(verdict);
    if (!parsed) {
      throw new Error('round trip lost the verdict');
    }
    expect(await tableHost(table).validate(parsed, context)).toEqual({ ok: true, value: true });
  });

  it('keeps only the fields it knows', async () => {
    const verdict = await capturedOn(heldTable());
    const [first, ...rest] = verdict.holders;
    const wire = {
      ...verdict,
      extra: true,
      context: { ...verdict.context, extra: true },
      holders: [{ ...first, extra: true, root: { ...first.root, ctimeNs: '1' } }, ...rest],
    };
    expect(parseHeldVerdict(JSON.parse(JSON.stringify(wire)))).toEqual(verdict);
  });

  it('refuses every malformed wire, so the caller probes the inventory instead of trusting it', async () => {
    const verdict = await capturedOn(heldTable());
    const [first] = verdict.holders;
    // What a store written while image holders were keyed would carry.
    const legacyImage = {
      ...image,
      image: { dev: '77', ino: '501', birthtimeNs: '900' },
      disk: { dev: '3', ino: '655', birthtimeNs: '0', rdev: '16777258', ctimeNs: '5000' },
    };
    const malformed: [string, unknown][] = [
      ['not an object', 'held'],
      ['null', null],
      ['an array', [verdict]],
      ['another version', { ...verdict, version: 2 }],
      ['no context', { ...verdict, context: undefined }],
      ['an empty lease key', { ...verdict, context: { ...verdict.context, leaseKey: '' } }],
      ['a RAM device that is no integer', { ...verdict, context: { ...verdict.context, ramDevice: 'disk3' } }],
      ['no holders', { ...verdict, holders: [] }],
      ['holders that are no list', { ...verdict, holders: first }],
      ['an image holder', { ...verdict, holders: [legacyImage] }],
      ['a mount beside an image holder', { ...verdict, holders: [first, legacyImage] }],
      ['an unknown holder kind', { ...verdict, holders: [{ ...first, kind: 'socket' }] }],
      ['a holder without its description', { ...verdict, holders: [{ ...first, description: '' }] }],
      ['a holder without its mountpoint', { ...verdict, holders: [{ ...first, mountpoint: undefined }] }],
      ['a mount without its root', { ...verdict, holders: [{ ...first, root: undefined }] }],
      ['a root without its birth', { ...verdict, holders: [{ ...first, root: { ...first.root, birthtimeNs: '' } }] }],
      [
        'an identity as a number, losing precision',
        { ...verdict, holders: [{ ...first, root: { ...first.root, ino: 2 } }] },
      ],
      [
        'an identity in a spelling no read writes',
        { ...verdict, holders: [{ ...first, root: { ...first.root, dev: '08' } }] },
      ],
    ];
    for (const [what, wire] of malformed) {
      expect([what, parseHeldVerdict(JSON.parse(JSON.stringify(wire)))]).toEqual([what, undefined]);
    }
  });
});
