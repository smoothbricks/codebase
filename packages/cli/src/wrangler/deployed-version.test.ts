import { afterEach, describe, expect, it } from 'bun:test';
import { existsSync } from 'node:fs';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { ProcessResult, ProcessRunner, ProcessRunOptions } from './deploy-stage.js';
import { deployedVersion } from './deployed-version.js';
import { LIVE_VERSION_CACHE_TTL_MS, liveVersionCachePath, writeCachedLiveVersion } from './live-version.js';

const HASH = '16577780061662788004';
const FIXTURE = `[env.staging]
name = "fixture-worker-staging"
workers_dev = false

[env.staging.vars]
ENVIRONMENT = "staging"
`;

const roots: string[] = [];

afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

class RecordingRunner implements ProcessRunner {
  readonly calls: string[][] = [];

  constructor(
    private readonly versions: unknown = [{ id: 'version-1', annotations: { 'workers/tag': `nx-${HASH}` } }],
    private readonly deployment: unknown = { versions: [{ version_id: 'version-1', percentage: 100 }] },
  ) {}

  async run(_command: string, args: string[], _options: ProcessRunOptions): Promise<ProcessResult> {
    this.calls.push(args);
    const value = args[0] === 'versions' && args[1] === 'list' ? this.versions : this.deployment;
    return { exitCode: 0, stdout: JSON.stringify(value), stderr: '' };
  }
}

async function fixture(): Promise<{ root: string; cacheDirectory: string }> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-deployed-version-'));
  roots.push(root);
  await writeFile(join(root, 'wrangler.toml'), FIXTURE);
  return { root, cacheDirectory: join(root, 'cache') };
}

const credentials = { CLOUDFLARE_ACCOUNT_ID: 'account-1', CLOUDFLARE_API_TOKEN: 'token' };

describe('smoo wrangler deployed-version', () => {
  it('reports the tag serving all traffic and remembers it', async () => {
    const { root, cacheDirectory } = await fixture();
    const runner = new RecordingRunner();

    const report = await deployedVersion(
      root,
      { stage: 'staging' },
      { runner, processEnv: credentials, cacheDirectory },
    );

    expect(report).toMatchObject({
      workerName: 'fixture-worker-staging',
      versionTag: `nx-${HASH}`,
      source: 'cloudflare',
    });
    expect(runner.calls.map((args) => args.slice(0, 2))).toEqual([
      ['deployments', 'status'],
      ['versions', 'list'],
    ]);
  });

  it('answers a second query from the cache without calling wrangler at all', async () => {
    const { root, cacheDirectory } = await fixture();
    const first = new RecordingRunner();
    await deployedVersion(root, { stage: 'staging' }, { runner: first, processEnv: credentials, cacheDirectory });

    const second = new RecordingRunner();
    const report = await deployedVersion(
      root,
      { stage: 'staging' },
      { runner: second, processEnv: credentials, cacheDirectory },
    );

    expect(report).toMatchObject({ versionTag: `nx-${HASH}`, source: 'cache' });
    expect(second.calls).toEqual([]);
  });

  it('asks Cloudflare again once the entry is older than the TTL', async () => {
    const { root, cacheDirectory } = await fixture();
    await writeCachedLiveVersion(
      cacheDirectory,
      { accountId: 'account-1', workerName: 'fixture-worker-staging', stage: 'staging' },
      { versionTag: 'nx-stale', versionId: 'version-0', fetchedAt: 0 },
    );
    const runner = new RecordingRunner();

    const report = await deployedVersion(
      root,
      { stage: 'staging' },
      { runner, processEnv: credentials, cacheDirectory, now: () => LIVE_VERSION_CACHE_TTL_MS + 1 },
    );

    expect(report).toMatchObject({ versionTag: `nx-${HASH}`, source: 'cloudflare' });
    expect(runner.calls.length).toBe(2);
  });

  it('refuses without a credential and writes no placeholder for a later run to trust', async () => {
    const { root, cacheDirectory } = await fixture();

    await expect(
      deployedVersion(
        root,
        { stage: 'staging' },
        { runner: new RecordingRunner(), processEnv: { CLOUDFLARE_ACCOUNT_ID: 'account-1' }, cacheDirectory },
      ),
    ).rejects.toThrow('CLOUDFLARE_API_TOKEN is required.');
    expect(
      existsSync(
        liveVersionCachePath(cacheDirectory, {
          accountId: 'account-1',
          workerName: 'fixture-worker-staging',
          stage: 'staging',
        }),
      ),
    ).toBe(false);
  });

  it('refuses to name a live version while traffic is split', async () => {
    const { root, cacheDirectory } = await fixture();
    const runner = new RecordingRunner([{ id: 'version-1', annotations: { 'workers/tag': `nx-${HASH}` } }], {
      versions: [
        { version_id: 'version-1', percentage: 50 },
        { version_id: 'version-2', percentage: 50 },
      ],
    });

    await expect(
      deployedVersion(root, { stage: 'staging' }, { runner, processEnv: credentials, cacheDirectory }),
    ).rejects.toThrow('fixture-worker-staging (staging): no single worker version is serving 100% of traffic');
  });

  it('names the pull-request worker the deploy would target, without provisioning anything', async () => {
    const { root, cacheDirectory } = await fixture();
    const runner = new RecordingRunner();

    const report = await deployedVersion(root, { stage: 'pr123' }, { runner, processEnv: credentials, cacheDirectory });

    expect(report.workerName).toBe('fixture-worker-pr123');
    expect(runner.calls.every((args) => args[0] === 'deployments' || args[0] === 'versions')).toBe(true);
  });
});
