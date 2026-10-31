import { describe, expect, it } from 'bun:test';
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { Hash } from 'nx/src/hasher/task-hasher';
import { deployVersionTag } from '../../deploy-policy.js';
import { deployArtifactHash } from './deploy-hash.js';
import { deployHash, LIVE_VERSION_NODE, type LiveTag, probeLiveTag } from './hasher.js';

const ARTIFACT: Hash = {
  value: '16577780061662788004',
  details: { command: 'site:deploy', nodes: { 'site:ProjectConfiguration': '1' } },
};
const TAG = 'nx-16577780061662788004';

describe('the wrangler deploy hasher', () => {
  it("keys a deploy on its artifact and stage alone while the stage serves that artifact's version", () => {
    expect(deployVersionTag(ARTIFACT.value)).toBe(TAG);
    expect(deployHash(ARTIFACT, { kind: 'tag', tag: TAG }, '0123456789abcdef')).toEqual(ARTIFACT);
  });

  it("misses every cache while another version serves the stage, and the run deploys the artifact's version", () => {
    const lives: readonly LiveTag[] = [
      { kind: 'tag', tag: 'nx-1' },
      { kind: 'tag', tag: null },
      { kind: 'unknown', reason: 'smoo wrangler deployed-version exited 1: CLOUDFLARE_API_TOKEN is unset' },
    ];
    for (const live of lives) {
      const first = deployHash(ARTIFACT, live, '0123456789abcdef');
      const second = deployHash(ARTIFACT, live, 'fedcba9876543210');
      // A nonce per hash: no entry any earlier run stored can answer, rolled back to or not.
      expect(first.value).not.toBe(ARTIFACT.value);
      expect(second.value).not.toBe(first.value);
      // The run tags the artifact's version, so once it is live the next hash is the artifact's own.
      expect(deployArtifactHash(first.value)).toBe(ARTIFACT.value);
      expect(first.details.nodes[LIVE_VERSION_NODE]).toBe(
        live.kind === 'unknown' ? live.reason : `${live.tag ?? 'an untagged version'} is live, not ${TAG}`,
      );
      expect(first.details.nodes['site:ProjectConfiguration']).toBe('1');
    }
    expect(deployArtifactHash(ARTIFACT.value)).toBe(ARTIFACT.value);
  });

  it('reads the live tag with smoo wrangler deployed-version in the project, asking Cloudflare afresh', async () => {
    const root = await mkdtemp(join(tmpdir(), 'wrangler-deploy-hasher-'));
    try {
      const bin = join(root, 'node_modules', '.bin');
      await mkdir(bin, { recursive: true });
      await mkdir(join(root, 'packages', 'site'), { recursive: true });
      await writeFile(
        join(bin, 'smoo'),
        [
          '#!/bin/sh',
          'printf \'%s %s\\n\' "$PWD" "$*" >> "$(dirname "$0")/calls"',
          'if [ "$FAKE_LIVE" = fail ]; then echo "CLOUDFLARE_API_TOKEN is unset" >&2; exit 1; fi',
          'printf \'%s\\n\' "$FAKE_LIVE"',
          '',
        ].join('\n'),
      );
      await chmod(join(bin, 'smoo'), 0o755);
      const probe = (live: string) => probeLiveTag(root, 'packages/site', 'pr7', { ...process.env, FAKE_LIVE: live });

      expect(await probe(TAG)).toEqual({ kind: 'tag', tag: TAG });
      // A version deployed outside Nx carries no tag.
      expect(await probe('untagged')).toEqual({ kind: 'tag', tag: null });
      const failed = await probe('fail');
      expect(failed.kind).toBe('unknown');
      expect(failed.kind === 'unknown' ? failed.reason : '').toContain('CLOUDFLARE_API_TOKEN is unset');
      const site = join(root, 'packages', 'site');
      expect(await readFile(join(bin, 'calls'), 'utf8')).toBe(
        `${site} wrangler deployed-version --stage pr7 --refresh\n`.repeat(3),
      );
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
