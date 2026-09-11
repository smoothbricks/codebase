// `smoo wrangler deployed-version --stage <stage>` — what is serving this project's worker right now.
//
// Read-only by construction: it resolves the worker name from the committed config and asks
// Cloudflare. It never provisions, never derives a pull-request stage's resources, and never
// writes anything except its own cache entry.

import { readFile } from 'node:fs/promises';
import { parseJsonFileText } from '../lib/json.js';
import { BunProcessRunner, type ProcessRunner, type ProcessRunOptions, wranglerJson } from './deploy-stage.js';
import { parseFlatWranglerConfig } from './flat-config.js';
import {
  LIVE_VERSION_CACHE_TTL_MS,
  type LiveVersionProbe,
  liveVersionCacheDirectory,
  readCachedLiveVersion,
  readLiveVersion,
  writeCachedLiveVersion,
} from './live-version.js';
import { readWranglerSourceConfig } from './source-config.js';
import {
  type DeploymentStage,
  isPullRequestStage,
  parseDeploymentStage,
  planConfiguredStageResources,
  planStageResources,
  stageResourceName,
  stagingWorkerBaseName,
} from './stage.js';

export interface DeployedVersionOptions {
  /** `staging`, `production`, or `prN`. */
  stage: string;
  /** The build-generated flat wrangler.json the deploy would use, when the project deploys one. */
  config?: string;
  /** Ask Cloudflare even when a fresh cache entry exists. */
  refresh?: boolean;
}

export interface DeployedVersionDependencies {
  runner?: ProcessRunner;
  processEnv?: NodeJS.ProcessEnv;
  cacheDirectory?: string;
  ttlMs?: number;
  now?: () => number;
}

export interface DeployedVersionReport {
  workerName: string;
  stage: DeploymentStage;
  versionTag: string | null;
  versionId: string;
  source: 'cloudflare' | 'cache';
}

/**
 * The worker `stage` deploys to, from committed configuration alone.
 *
 * A pull-request stage's worker name is derived from the staging name the same way the deploy
 * derives it, but WITHOUT the account listing the deploy needs for its KV/D1/R2 isolation: a
 * question about what is live must not be able to create anything.
 */
export async function stageWorkerName(cwd: string, stage: DeploymentStage, configPath?: string): Promise<string> {
  if (configPath) {
    const flat = parseJsonFileText(configPath, await readFile(configPath, 'utf8'), parseFlatWranglerConfig);
    return isPullRequestStage(stage)
      ? stageResourceName(stagingWorkerBaseName(flat.name), stage)
      : planStageResources(flat, stage).workerName;
  }
  const { document } = readWranglerSourceConfig(cwd);
  return isPullRequestStage(stage)
    ? stageResourceName(stagingWorkerBaseName(planConfiguredStageResources(document, 'staging').workerName), stage)
    : planConfiguredStageResources(document, stage).workerName;
}

/**
 * Answers with the tag serving 100% of this worker's traffic, or refuses.
 *
 * Refusing is the whole contract. A missing credential, an API error, or a traffic split has no
 * honest answer, and returning a placeholder such as `unknown` would be worse than the error: a
 * later run comparing against that placeholder would read it as a match and conclude the desired
 * version is already live. For the same reason nothing here is ever written to the cache except a
 * real observation, and the answer never contains a timestamp or anything else that varies per
 * invocation.
 */
export async function deployedVersion(
  cwd: string,
  options: DeployedVersionOptions,
  dependencies: DeployedVersionDependencies = {},
): Promise<DeployedVersionReport> {
  const stage = parseDeploymentStage(options.stage);
  const processEnv = dependencies.processEnv ?? process.env;
  const accountId = processEnv.CLOUDFLARE_ACCOUNT_ID;
  if (!accountId) throw new Error('CLOUDFLARE_ACCOUNT_ID is required.');
  if (!processEnv.CLOUDFLARE_API_TOKEN) throw new Error('CLOUDFLARE_API_TOKEN is required.');
  const workerName = await stageWorkerName(cwd, stage, options.config);
  const cacheDirectory = dependencies.cacheDirectory ?? liveVersionCacheDirectory(cwd, processEnv);
  const key = { accountId, workerName, stage };
  const ttlMs = dependencies.ttlMs ?? LIVE_VERSION_CACHE_TTL_MS;
  const now = dependencies.now ?? Date.now;
  if (options.refresh !== true) {
    const cached = await readCachedLiveVersion(cacheDirectory, key, ttlMs, now);
    if (cached) {
      return { workerName, stage, versionTag: cached.versionTag, versionId: cached.versionId, source: 'cache' };
    }
  }
  const runner = dependencies.runner ?? new BunProcessRunner();
  const run: ProcessRunOptions = { cwd, unsetEnv: ['CLOUDFLARE_ENV'] };
  const probe: LiveVersionProbe = {
    deployments: () => wranglerJson(runner, ['deployments', 'status', '--name', workerName, '--json'], run),
    versions: () => wranglerJson(runner, ['versions', 'list', '--name', workerName, '--json'], run),
  };
  const live = await readLiveVersion(probe);
  if (!live.ok) throw new Error(`${workerName} (${stage}): ${live.error.message}`);
  await writeCachedLiveVersion(cacheDirectory, key, {
    versionTag: live.value.tag,
    versionId: live.value.versionId,
    fetchedAt: now(),
  });
  return {
    workerName,
    stage,
    versionTag: live.value.tag,
    versionId: live.value.versionId,
    source: 'cloudflare',
  };
}
