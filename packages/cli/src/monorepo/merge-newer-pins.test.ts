import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const script = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '..',
  '..',
  'managed/raw/tooling/direnv/merge-newer-pins.sh',
);

// Invoke the merge driver as git does: `driver %O %A %B %P`, result written to %A (ours).
function mergedOurs(oursContent: string, theirsContent: string): string {
  const dir = mkdtempSync(join(tmpdir(), 'mnp-'));
  try {
    const base = join(dir, 'base');
    const ours = join(dir, 'ours');
    const theirs = join(dir, 'theirs');
    writeFileSync(base, '');
    writeFileSync(ours, oursContent);
    writeFileSync(theirs, theirsContent);
    const r = spawnSync('bash', [script, base, ours, theirs, 'pins'], { encoding: 'utf8' });
    expect(r.status).toBe(0);
    return readFileSync(ours, 'utf8');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

describe('smoo-newer-pins merge driver', () => {
  it('nvfetcher JSON: takes theirs when it pins a newer semver', () => {
    expect(mergedOurs('{"bun":{"version": "1.3.13"}}', '{"bun":{"version": "1.3.14"}}')).toContain('1.3.14');
  });

  it('nvfetcher JSON: keeps ours when ours pins the newer semver', () => {
    expect(mergedOurs('{"bun":{"version": "1.3.14"}}', '{"bun":{"version": "1.3.13"}}')).toContain('1.3.14');
  });

  it('nvfetcher nix form: takes the newer semver', () => {
    expect(mergedOurs('version = "1.3.13";', 'version = "1.3.14";')).toContain('1.3.14');
  });

  it('flake lock: takes theirs when lastModified is newer', () => {
    const out = mergedOurs('{"nixpkgs":{"lastModified": 1777573317}}', '{"nixpkgs":{"lastModified": 1779717400}}');
    expect(out).toContain('1779717400');
  });

  it('flake lock: keeps ours when ours lastModified is newer', () => {
    const out = mergedOurs('{"nixpkgs":{"lastModified": 1779717400}}', '{"nixpkgs":{"lastModified": 1777573317}}');
    expect(out).toContain('1779717400');
  });

  it('equal versions: keeps ours unchanged (idempotent)', () => {
    expect(mergedOurs('{"v": "1.3.14", "note": "ours"}', '{"v": "1.3.14", "note": "theirs"}')).toContain('ours');
  });
});
