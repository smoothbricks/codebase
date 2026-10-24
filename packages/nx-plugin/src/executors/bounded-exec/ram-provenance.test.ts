import { describe, expect, it } from 'bun:test';
import { mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir, userInfo } from 'node:os';
import { join } from 'node:path';
import { type HostCommands, RamTempVolume, ramTempPaths } from './ram-temp.js';

const noSubtrees = {
  rootArgv(): never {
    throw new Error('no command admitted');
  },
  async retire(): Promise<never> {
    throw new Error('no subtree admitted');
  },
};
function fixture() {
  const parent = mkdtempSync(join(tmpdir(), 'ram-provenance-'));
  const root = join(parent, 'root');
  mkdirSync(root, { mode: 0o700 });
  const paths = {
    ...ramTempPaths(userInfo()),
    root,
    mountpoint: join(root, 'owned'),
    staging: join(root, 'owned-new'),
    lockFile: join(parent, 'v.lock'),
    volumeName: 'owned-provenance',
  };
  mkdirSync(paths.mountpoint);
  writeFileSync(join(paths.mountpoint, 'keep'), 'owned payload');
  let calls = 0;
  const commands: HostCommands = {
    async run() {
      calls++;
      throw new Error('legacy inspection runs no host mutation or inventory');
    },
    async attachRam() {
      calls++;
      throw new Error('legacy inspection attaches nothing');
    },
    async detachRam() {
      calls++;
      throw new Error('legacy inspection detaches nothing');
    },
  };
  return {
    parent,
    paths,
    commands,
    calls: () => calls,
    cleanup: () => rmSync(parent, { recursive: true, force: true }),
  };
}
describe('gateway creation provenance admission', () => {
  it('does not hide or delete unowned contents of an unmounted legacy mountpoint', async () => {
    const f = fixture();
    try {
      const answer = await new RamTempVolume(
        f.paths,
        64 * 1024 * 1024,
        f.commands,
        undefined,
        undefined,
        'optional',
      ).acquire(noSubtrees);
      expect(answer).toMatchObject({ ok: true, value: { kind: 'unavailable', reason: 'unproven-owner' } });
      expect(readFileSync(join(f.paths.mountpoint, 'keep'), 'utf8')).toBe('owned payload');
      expect(f.calls()).toBe(0);
    } finally {
      f.cleanup();
    }
  });
  it('leaves legacy marker, payload and ledger namespace unchanged, optional only for production policy', async () => {
    const f = fixture();
    const marker = join(f.paths.mountpoint, '.smoo-ram');
    writeFileSync(marker, `${f.paths.volumeName}\n`);
    const before = readdirSync(f.parent).sort();
    try {
      const optional = await new RamTempVolume(
        f.paths,
        64 * 1024 * 1024,
        f.commands,
        undefined,
        undefined,
        'optional',
      ).acquire(noSubtrees);
      expect(optional).toMatchObject({ ok: true, value: { kind: 'unavailable', reason: 'unproven-owner' } });
      const required = await new RamTempVolume(f.paths, 64 * 1024 * 1024, f.commands).acquire(noSubtrees);
      expect(required).toMatchObject({ ok: false, error: { kind: 'ram-unavailable', reason: 'unproven-owner' } });
      expect(readFileSync(marker, 'utf8')).toBe(`${f.paths.volumeName}\n`);
      expect(readFileSync(join(f.paths.mountpoint, 'keep'), 'utf8')).toBe('owned payload');
      expect(readdirSync(f.parent).sort()).toEqual(before);
      expect(f.calls()).toBe(0);
    } finally {
      f.cleanup();
    }
  });
  it('does not suppress malformed new provenance or unknown ledger authority', async () => {
    const f = fixture();
    const marker = join(f.paths.mountpoint, '.smoo-ram');
    try {
      for (const text of [
        '{broken',
        JSON.stringify({ version: 1, owner: 'job', registryEntry: '0x1', volumeName: f.paths.volumeName }),
      ]) {
        writeFileSync(marker, text);
        const answer = await new RamTempVolume(
          f.paths,
          64 * 1024 * 1024,
          f.commands,
          undefined,
          undefined,
          'optional',
        ).acquire(noSubtrees);
        expect(answer).toMatchObject({ ok: false, error: { kind: 'provision-failed', step: 'provenance' } });
        expect(readFileSync(marker, 'utf8')).toBe(text);
      }
      writeFileSync(marker, `${f.paths.volumeName}\n`);
      const ledger = JSON.stringify({ version: 99, lifecycle: null, claims: [], held: [] });
      writeFileSync(`${f.paths.lockFile}.ledger`, ledger);
      const answer = await new RamTempVolume(
        f.paths,
        64 * 1024 * 1024,
        f.commands,
        undefined,
        undefined,
        'optional',
      ).acquire(noSubtrees);
      expect(JSON.stringify(answer)).toContain('unsupported version 99');
      expect(readFileSync(`${f.paths.lockFile}.ledger`, 'utf8')).toBe(ledger);
      expect(f.calls()).toBe(0);
    } finally {
      f.cleanup();
    }
  });
});
