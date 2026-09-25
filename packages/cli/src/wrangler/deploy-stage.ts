import { randomUUID } from 'node:crypto';
import { readFile, rm, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { parseJsonFileText } from '../lib/json.js';
import { mergeEnv, printCommandOutput } from '../lib/run.js';
import {
  CloudflareApiError,
  type CloudflareClient,
  CloudflareRestClient,
  type D1DatabaseRecord,
  type DnsRecord,
  type WorkerDomain,
  type WorkerRoute,
} from './cloudflare.js';
import { type FlatWranglerConfig, parseFlatWranglerConfig } from './flat-config.js';
import {
  awaitLiveVersion,
  awaitVersionEndpoint,
  currentDeploymentVersionId,
  findVersionIdByTag,
  type LiveVersionProbe,
  liveVersionCacheDirectory,
  type VersionEndpointWaitOptions,
  writeCachedLiveVersion,
} from './live-version.js';
import { readWranglerSourceConfig } from './source-config.js';
import {
  type ConfiguredStageResourcePlan,
  type DeploymentStage,
  derivePullRequestDocument,
  derivePullRequestStageConfig,
  isPullRequestStage,
  type LiveKvNamespace,
  type PullRequestResourcePlan,
  parseDeploymentStage,
  planConfiguredStageResources,
  planPullRequestBindings,
  planPullRequestResources,
  planStageResources,
  pullRequestStage,
} from './stage.js';
import { wildcardDnsRecord } from './stage-labels.js';
import {
  parseStageRecordKey,
  plannedStageRecords,
  STAGE_RECORDS_BUCKET,
  type StageRecord,
  stageRecordKeys,
  stageRecordPrefix,
  stageRecordScope,
  zoneContaining,
} from './stage-records.js';
import {
  planStageSecrets,
  readDeclaredSecretNames,
  readSecretStageMap,
  type StageSecretPlan,
  stageSecretRefusal,
} from './stage-secrets.js';

export interface ProcessResult {
  exitCode: number;
  stdout: string;
  stderr: string;
}

export interface ProcessRunOptions {
  cwd: string;
  /** Overlaid on this process's environment, as `lib/run.ts` merges it; the child inherits everything else. */
  env?: Record<string, string>;
  /** Withheld from the child after the overlay: an overlay alone cannot unset a variable this process inherited. */
  unsetEnv?: string[];
}

export interface ProcessRunner {
  run(command: string, args: string[], options: ProcessRunOptions): Promise<ProcessResult>;
}

/** The child environment `options` describe; every runner, test doubles included, spawns with exactly this. */
export function childEnvironment(options: ProcessRunOptions): NodeJS.ProcessEnv {
  return mergeEnv(options.env, options.unsetEnv);
}

export class BunProcessRunner implements ProcessRunner {
  async run(command: string, args: string[], options: ProcessRunOptions): Promise<ProcessResult> {
    const child = Bun.spawn([command, ...args], {
      cwd: options.cwd,
      env: childEnvironment(options),
      stdin: 'inherit',
      stdout: 'pipe',
      stderr: 'pipe',
    });
    const [exitCode, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ]);
    return { exitCode, stdout, stderr };
  }
}

export interface WranglerCommandDependencies {
  runner?: ProcessRunner;
  cloudflare?: CloudflareClient;
  processEnv?: NodeJS.ProcessEnv;
  /** Bounds and clock for the post-deploy wait; tests drive it without sleeping. */
  wait?: VersionEndpointWaitOptions;
  /** Where a successful deploy records the tag it made live, for `smoo wrangler deployed-version`. */
  liveVersionCacheDirectory?: string;
}

export interface DeployStageResult {
  stage: DeploymentStage;
  workerName: string;
  action: 'deployed' | 'activated' | 'remote-cache-hit';
  versionTag?: string;
}

export interface DeployStageOptions {
  /** `staging`, `production`, or `prN`. */
  stage: string;
  /** The repository root; its package.json names the repository a `prN` stage is recorded under. */
  repositoryRoot: string;
  /** A build-generated flat wrangler.json to deploy instead of the project's own config (see `prepareFlatConfig`). */
  config?: string;
  /**
   * A URL, served by this worker, whose trimmed body is the running version tag. When given, the
   * deploy is not done until that URL answers with the tag it just made live — the control plane
   * accepting a traffic shift is not the edge serving it.
   */
  versionEndpoint?: string;
}

export async function deployStage(
  cwd: string,
  options: DeployStageOptions,
  dependencies: WranglerCommandDependencies = {},
): Promise<DeployStageResult> {
  const stage = parseDeploymentStage(options.stage);
  const processEnv = dependencies.processEnv ?? process.env;
  // Before any Cloudflare call: a stage whose records cannot be scoped must not start creating.
  const recordScope = isPullRequestStage(stage) ? stageRecordScope(options.repositoryRoot, processEnv) : undefined;
  const accountId = processEnv.CLOUDFLARE_ACCOUNT_ID;
  const apiToken = processEnv.CLOUDFLARE_API_TOKEN;
  if (!accountId) throw new Error('CLOUDFLARE_ACCOUNT_ID is required.');
  const cloudflare =
    dependencies.cloudflare ??
    new CloudflareRestClient(accountId, requiredEnvironmentValue(apiToken, 'CLOUDFLARE_API_TOKEN'));
  const runner = dependencies.runner ?? new BunProcessRunner();
  const secretPlan = planStageSecrets(readDeclaredSecretNames(cwd), stage, readSecretStageMap(cwd));
  // Only the stage's own secrets, and only the ones a value exists for. A secret scoped to other
  // stages is dropped here even when this shell exports it: that is the whole permission rule.
  const secretValues: Record<string, string> = {};
  for (const name of secretPlan.required) {
    const value = processEnv[name];
    if (value) secretValues[name] = value;
  }
  const gate = stageSecretGate(secretPlan, new Set(Object.keys(secretValues)), cloudflare);
  const record = stageRecorder(recordScope, cloudflare);
  let temporaryConfigPath: string | undefined;
  let temporarySecretsPath: string | undefined;
  try {
    const prepared = options.config
      ? await prepareFlatConfig(options.config, stage, accountId, gate, record, cloudflare)
      : await prepareSourceConfig(cwd, stage, accountId, gate, record, cloudflare);
    temporaryConfigPath = prepared.temporaryConfigPath;
    // The pull-request path already ran this before provisioning; the gate answers once per Worker.
    // Everything below here writes to Cloudflare.
    await gate(prepared.plan.workerName);
    const workerExists = await reconcileStageResources(prepared.plan, cloudflare);
    const versionTag = nxTaskVersionTag(processEnv);
    const envArgs = prepared.envFlag ? ['--env', prepared.envFlag] : [];
    // Without `--env`, wrangler falls back to `CLOUDFLARE_ENV` and, for a config with no env blocks,
    // renames the worker `<name>-<CLOUDFLARE_ENV>`. The build that produced the flat config is the
    // caller that sets the variable, so the deploy would silently succeed under a name neither
    // `reconcileStageResources` nor `versions list` looks at.
    //
    // A secret this stage is scoped out of goes with it. Leaving it out of the secrets payload is
    // what stops it being installed; withholding it from the child as well means the deploy cannot
    // read it at all, so "this stage never sees that capability" holds at the process boundary and
    // not merely in the file we happen to write.
    const unsetEnv = [...(prepared.envFlag ? [] : ['CLOUDFLARE_ENV']), ...secretPlan.withheld];
    const run: ProcessRunOptions = unsetEnv.length > 0 ? { cwd, unsetEnv } : { cwd };
    const workerName = prepared.plan.workerName;
    const probe: LiveVersionProbe = {
      deployments: () => wranglerJson(runner, ['deployments', 'status', '--name', workerName, '--json'], run),
      versions: () => wranglerJson(runner, ['versions', 'list', '--name', workerName, '--json'], run),
    };

    // The tagged-version lookup runs before the migrations: a cache hit means this exact build is
    // already live, so its migrations ran with it and re-applying them would touch the remote
    // database for nothing. The activation and upload paths below still migrate first.
    //
    // This reads Cloudflare every time and never the local cache. A cache hit here would mean
    // "we believe this hash is live" — exactly the belief a rollback falsifies — and believing it
    // would skip the deploy that repairs the rollback. Reading live state is what makes a
    // redundant deploy a proven no-op instead of an assumed one, which is in turn what makes a
    // cross-project `dependsOn: ["<other>:deploy"]` edge cheap enough to be the ordering mechanism.
    let taggedVersionId: string | null = null;
    if (versionTag && workerExists) {
      taggedVersionId = findVersionIdByTag(await probe.versions(), versionTag);
      if (taggedVersionId && currentDeploymentVersionId(await probe.deployments()) === taggedVersionId) {
        return { stage, workerName, action: 'remote-cache-hit', versionTag };
      }
    }

    for (const binding of prepared.d1MigrationBindings) {
      await wrangler(
        runner,
        ['d1', 'migrations', 'apply', binding, '--remote', '--config', prepared.deployConfigPath],
        run,
      );
    }

    if (versionTag && taggedVersionId) {
      await wrangler(
        runner,
        [
          'versions',
          'deploy',
          '--version-tag',
          versionTag,
          '--name',
          workerName,
          '--config',
          prepared.deployConfigPath,
          ...envArgs,
          '--yes',
        ],
        run,
      );
      await confirmVersionIsLive(probe, versionTag, { stage, workerName, accountId }, options, dependencies, cwd);
      return { stage, workerName, action: 'activated', versionTag };
    }

    const deployArgs = ['deploy', '--config', prepared.deployConfigPath, ...envArgs];
    if (versionTag) deployArgs.push('--tag', versionTag);
    if (Object.keys(secretValues).length > 0) {
      temporarySecretsPath = join(cwd, `.wrangler-secrets.smoo-${process.pid}-${randomUUID()}.json`);
      await writeFile(temporarySecretsPath, `${JSON.stringify(secretValues)}\n`, { mode: 0o600 });
      deployArgs.push('--secrets-file', temporarySecretsPath);
    }
    await wrangler(runner, deployArgs, run);
    if (versionTag) {
      await confirmVersionIsLive(probe, versionTag, { stage, workerName, accountId }, options, dependencies, cwd);
    }
    return { stage, workerName, action: 'deployed', ...(versionTag ? { versionTag } : {}) };
  } finally {
    if (temporaryConfigPath) {
      await rm(temporaryConfigPath, { force: true });
    }
    if (temporarySecretsPath) {
      await rm(temporarySecretsPath, { force: true });
    }
  }
}

interface PreparedConfig {
  deployConfigPath: string;
  temporaryConfigPath?: string;
  plan: ConfiguredStageResourcePlan;
  /** D1 bindings whose migrations run before the deploy (flat configs only; source configs keep migrations out of scope). */
  d1MigrationBindings: string[];
  /** `--env <stage>` for a source config with env blocks; a flat config is already resolved, so no flag. */
  envFlag?: DeploymentStage;
}

/**
 * The project's own `wrangler.jsonc`/`wrangler.json`/`wrangler.toml`, whichever it declares.
 * Format stops mattering at `readWranglerSourceConfig`: everything below derives from the model.
 */
async function prepareSourceConfig(
  cwd: string,
  stage: DeploymentStage,
  accountId: string,
  gate: SecretGate,
  record: StageRecorder,
  cloudflare: CloudflareClient,
): Promise<PreparedConfig> {
  const source = readWranglerSourceConfig(cwd);
  if (!isPullRequestStage(stage)) {
    return {
      deployConfigPath: source.path,
      plan: planConfiguredStageResources(source.document, stage),
      d1MigrationBindings: [],
      envFlag: stage,
    };
  }
  const liveNamespaces = await cloudflare.listKvNamespaces();
  const plan = planPullRequestResources(source.document, stage, liveNamespaces);
  const { kvNamespaceIds, d1DatabaseIds } = await provisionPullRequestResources(
    plan,
    gate,
    record,
    cloudflare,
    liveNamespaces,
  );
  const derived = derivePullRequestDocument(source.document, { stage, accountId, kvNamespaceIds, d1DatabaseIds });
  // JSON whatever the source format was: wrangler picks its parser by extension, and a file
  // written to be read once and deleted has no reader for the comments a TOML rewrite preserved.
  // Beside the original, because its main/assets/migrations paths are relative to the file.
  const temporaryConfigPath = join(dirname(source.path), `.wrangler.smoo-${process.pid}-${randomUUID()}.json`);
  await writeTemporaryConfig(temporaryConfigPath, `${JSON.stringify(derived, null, 2)}\n`);
  return {
    deployConfigPath: temporaryConfigPath,
    temporaryConfigPath,
    plan: planConfiguredStageResources(derived, stage),
    d1MigrationBindings: [],
    envFlag: stage,
  };
}

async function prepareFlatConfig(
  configPath: string,
  stage: DeploymentStage,
  accountId: string,
  gate: SecretGate,
  record: StageRecorder,
  cloudflare: CloudflareClient,
): Promise<PreparedConfig> {
  const flat = parseJsonFileText(configPath, await readFile(configPath, 'utf8'), parseFlatWranglerConfig);
  if (!isPullRequestStage(stage)) {
    return {
      deployConfigPath: configPath,
      plan: planStageResources(flat, stage),
      d1MigrationBindings: migrationBindings(flat),
    };
  }
  const liveNamespaces = await cloudflare.listKvNamespaces();
  const plan = planPullRequestBindings(flat, stage, liveNamespaces);
  const { kvNamespaceIds, d1DatabaseIds } = await provisionPullRequestResources(
    plan,
    gate,
    record,
    cloudflare,
    liveNamespaces,
  );
  const derived = derivePullRequestStageConfig(flat, { stage, accountId, kvNamespaceIds, d1DatabaseIds });
  // Beside the original: its main/assets/migrations paths are relative to the file.
  const temporaryConfigPath = join(dirname(configPath), `.wrangler.smoo-${process.pid}-${randomUUID()}.json`);
  await writeTemporaryConfig(temporaryConfigPath, `${JSON.stringify(derived, null, 2)}\n`);
  return {
    deployConfigPath: temporaryConfigPath,
    temporaryConfigPath,
    plan: planStageResources(derived, stage),
    d1MigrationBindings: migrationBindings(derived),
  };
}

/**
 * Writes a derived Wrangler config the deploy will delete afterwards. The caller only learns the
 * path from a successful return, so a write that fails midway (the file created, the content not
 * written) would leave it behind: remove it here before the failure propagates.
 */
async function writeTemporaryConfig(path: string, content: string): Promise<void> {
  try {
    await writeFile(path, content, { mode: 0o600 });
  } catch (error) {
    await rm(path, { force: true });
    throw error;
  }
}
export const writeTemporaryConfigForTest = writeTemporaryConfig;

function migrationBindings(config: FlatWranglerConfig): string[] {
  return (config.d1_databases ?? [])
    .filter((database) => typeof database.migrations_dir === 'string')
    .map((database) => database.binding);
}

/**
 * Asks, of one Worker, whether this stage may deploy at all. Called at each path's last read
 * before its first write, so a refusal costs nothing: the pull-request path provisions KV and D1
 * inside config preparation, and every path reconciles buckets, routes and DNS after it. A
 * refusal arriving later would strand resources created for a deploy that was never allowed.
 */
type SecretGate = (workerName: string) => Promise<void>;

/**
 * One answer per Worker, reused by both call sites so the read costs one round trip.
 *
 * What the Worker holds comes from the account's script listing first. A Worker absent from it
 * holds nothing — exactly the first-deployment case — and Cloudflare answers 404 rather than an
 * empty list when asked for a missing Worker's secrets. Reading that failure as "then nothing is
 * needed" would disarm the check at the one moment it matters most, so it is never asked.
 */
function stageSecretGate(
  plan: StageSecretPlan,
  exported: ReadonlySet<string>,
  cloudflare: CloudflareClient,
): SecretGate {
  const answered = new Map<string, Promise<void>>();
  return (workerName) => {
    let pending = answered.get(workerName);
    if (!pending) {
      pending = assertStageSecrets(plan, exported, cloudflare, workerName);
      answered.set(workerName, pending);
    }
    return pending;
  };
}

async function assertStageSecrets(
  plan: StageSecretPlan,
  exported: ReadonlySet<string>,
  cloudflare: CloudflareClient,
  workerName: string,
): Promise<void> {
  const live = (await cloudflare.listWorkerScripts()).some((script) => script.id === workerName);
  const held = new Set(live ? await cloudflare.listWorkerSecrets(workerName) : []);
  const refusal = stageSecretRefusal(plan, exported, held, workerName);
  if (refusal) throw new Error(refusal);
}

/**
 * Isolation refusals already ran in the plan; the gate below is the last one before the first write,
 * and the record the first write. Everything this deploy creates afterwards, here, in reconcile and
 * in `wrangler deploy`, is already recorded, so a deploy that dies halfway leaves nothing unrecorded.
 */
async function provisionPullRequestResources(
  plan: PullRequestResourcePlan,
  gate: SecretGate,
  record: StageRecorder,
  cloudflare: CloudflareClient,
  liveNamespaces: LiveKvNamespace[],
): Promise<{ kvNamespaceIds: Map<string, string>; d1DatabaseIds: Map<string, string> }> {
  await gate(plan.workerName);
  await record(plan);
  const kvNamespaceIds = await ensureKvNamespaces(plan.kvNamespaces, liveNamespaces, cloudflare);
  const d1DatabaseIds =
    plan.d1Databases.length === 0
      ? new Map<string, string>()
      : await ensureD1Databases(plan.d1Databases, await cloudflare.listD1Databases(), cloudflare);
  return { kvNamespaceIds, d1DatabaseIds };
}

/**
 * Writes down, in the account's `smoo-stage-records` bucket, every item a pull-request plan will
 * create (`stage-records.ts`). Items the stage finds already there are recorded too: they carry the
 * stage's name, so they are its own, and that is how a stage deployed before records existed gets
 * them on its next push.
 */
type StageRecorder = (plan: PullRequestResourcePlan) => Promise<void>;

function stageRecorder(scope: string | undefined, cloudflare: CloudflareClient): StageRecorder {
  return async (plan) => {
    if (scope === undefined) {
      throw new Error(`${plan.stage} was planned as a pull-request stage without a record scope.`);
    }
    let writing = false;
    try {
      const keys = stageRecordKeys(scope, plan.stage, await plannedStageRecords(plan, () => cloudflare.listZones()));
      writing = true;
      await ensureR2Bucket(STAGE_RECORDS_BUCKET, await r2BucketNames(cloudflare), cloudflare);
      // One after another: parallel writes would only trade a clear first failure for several.
      for (const key of keys) await cloudflare.putR2Object(STAGE_RECORDS_BUCKET, key, '');
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      // Only a refusal of the R2 calls names the permission: a 429, a 5xx or a network failure is
      // not one, and neither is a zone listing refused while planning the records.
      const refused = writing && error instanceof CloudflareApiError && error.status === 403;
      const reason = refused
        ? `${detail.replace(/\.$/, '')}. The deploy token needs R2 write (Workers R2 Storage: Edit).`
        : detail;
      const message = `Recording ${plan.stage} in R2 bucket ${STAGE_RECORDS_BUCKET} failed, so nothing was created: ${reason}`;
      throw new Error(message, { cause: error });
    }
  };
}

async function r2BucketNames(cloudflare: CloudflareClient): Promise<Set<string>> {
  return new Set((await cloudflare.listR2Buckets()).map((bucket) => bucket.name));
}

/**
 * Creates the bucket unless `existing` names it. A create that fails because a parallel deploy made
 * the bucket first is a success.
 */
async function ensureR2Bucket(name: string, existing: Set<string>, cloudflare: CloudflareClient): Promise<void> {
  if (existing.has(name)) return;
  try {
    await cloudflare.createR2Bucket(name);
  } catch (error) {
    if (!(await r2BucketNames(cloudflare)).has(name)) throw error;
  }
  existing.add(name);
}

/** Creates every planned namespace that is not live yet, mapping staging ids to the stage's own ids. */
async function ensureKvNamespaces(
  planned: { stagingId: string; title: string }[],
  liveNamespaces: LiveKvNamespace[],
  cloudflare: CloudflareClient,
): Promise<Map<string, string>> {
  const derivedIds = new Map<string, string>();
  const byTitle = new Map(liveNamespaces.map((namespace) => [namespace.title, namespace]));
  for (const namespace of planned) {
    let live = byTitle.get(namespace.title);
    if (!live) {
      try {
        live = await cloudflare.createKvNamespace(namespace.title);
      } catch (error) {
        live = (await cloudflare.listKvNamespaces()).find((candidate) => candidate.title === namespace.title);
        if (!live) throw error;
      }
      byTitle.set(live.title, live);
    }
    derivedIds.set(namespace.stagingId, live.id);
  }
  return derivedIds;
}

/** Creates every planned database that is not live yet, mapping staging ids to the stage's own ids. */
async function ensureD1Databases(
  planned: { stagingId: string; name: string }[],
  liveDatabases: D1DatabaseRecord[],
  cloudflare: CloudflareClient,
): Promise<Map<string, string>> {
  const derivedIds = new Map<string, string>();
  const byName = new Map(liveDatabases.map((database) => [database.name, database]));
  for (const database of planned) {
    let live = byName.get(database.name);
    if (!live) {
      try {
        live = await cloudflare.createD1Database(database.name);
      } catch (error) {
        live = (await cloudflare.listD1Databases()).find((candidate) => candidate.name === database.name);
        if (!live) throw error;
      }
      byName.set(live.name, live);
    }
    derivedIds.set(database.stagingId, live.uuid);
  }
  return derivedIds;
}

export interface CleanupCounts {
  workers: number;
  routes: number;
  domains: number;
  dnsRecords: number;
  kvNamespaces: number;
  r2Buckets: number;
  r2Objects: number;
  d1Databases: number;
}

export interface CleanupResult {
  stage: `pr${number}`;
  /** The repository whose records were read, as `host/owner/repo`. */
  scope: string;
  /** How many record keys the stage had. */
  recorded: number;
  deleted: CleanupCounts;
  /** Recorded items that no longer existed, each counted once. */
  alreadyGone: number;
  /** Recorded items another owner now holds, by name; they were not deleted. */
  leftInPlace: string[];
}

/**
 * Deletes what the deploys of this repository's `prN` stage recorded (`stage-records.ts`), and
 * nothing else: every item is resolved before the first delete, a route or custom domain a Worker
 * outside the stage now holds is left alone, and the records go last, so a cleanup that stops halfway is
 * finished by running it again.
 */
export async function cleanupPullRequest(
  root: string,
  prNumber: number,
  dependencies: WranglerCommandDependencies = {},
): Promise<CleanupResult> {
  const stage = pullRequestStage(prNumber);
  const processEnv = dependencies.processEnv ?? process.env;
  const scope = stageRecordScope(root, processEnv);
  const cloudflare =
    dependencies.cloudflare ??
    new CloudflareRestClient(
      requiredEnvironmentValue(processEnv.CLOUDFLARE_ACCOUNT_ID, 'CLOUDFLARE_ACCOUNT_ID'),
      requiredEnvironmentValue(processEnv.CLOUDFLARE_API_TOKEN, 'CLOUDFLARE_API_TOKEN'),
    );
  const result: CleanupResult = {
    stage,
    scope,
    recorded: 0,
    deleted: {
      workers: 0,
      routes: 0,
      domains: 0,
      dnsRecords: 0,
      kvNamespaces: 0,
      r2Buckets: 0,
      r2Objects: 0,
      d1Databases: 0,
    },
    alreadyGone: 0,
    leftInPlace: [],
  };
  const { keys, records, buckets } = await readStageRecords(cloudflare, scope, stage);
  result.recorded = keys.length;
  if (keys.length === 0) return result;
  const targets = await resolveCleanupTargets(cloudflare, records, buckets);
  result.alreadyGone = targets.alreadyGone;
  result.leftInPlace = targets.leftInPlace;
  try {
    await deleteCleanupTargets(cloudflare, targets, result.deleted);
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    throw new Error(
      `Cleaning ${stage} of ${scope} stopped: ${detail.replace(/\.?$/, '.')} Its records are kept, so running cleanup-pr again finishes it.`,
      { cause: error },
    );
  }
  for (const key of keys) await cloudflare.deleteR2Object(STAGE_RECORDS_BUCKET, key);
  return result;
}

/** The one line `smoo wrangler cleanup-pr` prints. */
export function describeCleanup(result: CleanupResult): string {
  const stage = `${result.stage} of ${result.scope}`;
  if (result.recorded === 0) {
    return `Nothing is recorded for ${stage}, so nothing was deleted (a pull request that deployed nothing, or a stage deployed before smoo recorded stages).`;
  }
  const { deleted } = result;
  const counts = [
    count(deleted.workers, 'Worker'),
    count(deleted.domains, 'custom domain'),
    count(deleted.routes, 'route'),
    count(deleted.dnsRecords, 'DNS record'),
    count(deleted.kvNamespaces, 'KV namespace'),
    `${count(deleted.r2Buckets, 'R2 bucket')} (${count(deleted.r2Objects, 'object')})`,
    count(deleted.d1Databases, 'D1 database'),
  ].join(', ');
  const gone =
    result.alreadyGone === 1
      ? '1 recorded item was already gone'
      : `${result.alreadyGone} recorded items were already gone`;
  const left = result.leftInPlace.length > 0 ? `; left in place: ${result.leftInPlace.join(', ')}` : '';
  return `Cleaned ${stage} from ${count(result.recorded, 'record')}: deleted ${counts}; ${gone}${left}.`;
}

function count(amount: number, noun: string): string {
  return `${amount} ${noun}${amount === 1 ? '' : 's'}`;
}

/**
 * The stage's record keys and what they say. No record bucket means nothing is recorded. One key
 * that is not the stage's own refuses the whole cleanup before anything is deleted.
 */
async function readStageRecords(
  cloudflare: CloudflareClient,
  scope: string,
  stage: `pr${number}`,
): Promise<{ keys: string[]; records: StageRecord[]; buckets: Set<string> }> {
  const buckets = await r2BucketNames(cloudflare);
  if (!buckets.has(STAGE_RECORDS_BUCKET)) return { keys: [], records: [], buckets };
  const keys = await cloudflare.listR2Objects(STAGE_RECORDS_BUCKET, stageRecordPrefix(scope, stage));
  return { keys, records: keys.map((key) => parseStageRecordKey(scope, stage, key)), buckets };
}

/** Every live item the records name, found before the first delete. */
interface CleanupTargets {
  domains: WorkerDomain[];
  routes: { zoneId: string; route: WorkerRoute }[];
  dnsRecords: { zoneId: string; record: DnsRecord }[];
  workers: string[];
  kvNamespaces: LiveKvNamespace[];
  r2Buckets: { name: string; keys: string[] }[];
  d1Databases: D1DatabaseRecord[];
  alreadyGone: number;
  leftInPlace: string[];
}

type RecordOf<K extends StageRecord['kind']> = Extract<StageRecord, { kind: K }>;

/**
 * Looks up each distinct recorded item; an item several Workers recorded (a shared KV namespace,
 * a wildcard DNS record, a route that moved between them) is one item. A route or custom domain is
 * the stage's own while no Worker outside the stage holds it. Only the kinds and zones the records
 * name are listed.
 */
async function resolveCleanupTargets(
  cloudflare: CloudflareClient,
  records: StageRecord[],
  buckets: Set<string>,
): Promise<CleanupTargets> {
  const distinct = [...new Map(records.map((record) => [recordIdentity(record), record])).values()];
  const of = <K extends StageRecord['kind']>(kind: K) =>
    distinct.filter((record): record is RecordOf<K> => record.kind === kind);
  const stageWorkers = new Set(records.map((record) => record.worker));
  /** Another owner's name, or undefined when the stage (or nobody) holds the item. */
  const otherOwner = (owner: string | undefined) =>
    owner !== undefined && !stageWorkers.has(owner) ? owner : undefined;
  const targets: CleanupTargets = {
    domains: [],
    routes: [],
    dnsRecords: [],
    workers: [],
    kvNamespaces: [],
    r2Buckets: [],
    d1Databases: [],
    alreadyGone: 0,
    leftInPlace: [],
  };
  /** Adds what was found, or counts it as already gone. */
  const found = <T>(list: T[], item: T | undefined) => {
    if (item === undefined) targets.alreadyGone += 1;
    else list.push(item);
  };

  const domains = of('domain');
  const liveDomains = domains.length > 0 ? await cloudflare.listWorkerDomains() : [];
  for (const record of domains) {
    const domain = liveDomains.find((candidate) => sameName(candidate.hostname, record.hostname));
    const owner = otherOwner(domain?.service);
    if (owner !== undefined) {
      targets.leftInPlace.push(`custom domain ${record.hostname} (bound to ${owner})`);
    } else {
      found(targets.domains, domain);
    }
  }

  await resolveZoneTargets(cloudflare, of('route'), of('dns'), otherOwner, targets, found);

  const workers = of('worker');
  const scripts = new Set(workers.length > 0 ? (await cloudflare.listWorkerScripts()).map((script) => script.id) : []);
  for (const record of workers) found(targets.workers, scripts.has(record.worker) ? record.worker : undefined);

  const kv = of('kv');
  const namespaces = kv.length > 0 ? await cloudflare.listKvNamespaces() : [];
  for (const record of kv)
    found(
      targets.kvNamespaces,
      namespaces.find((namespace) => namespace.title === record.title),
    );

  for (const record of of('r2')) {
    const exists = buckets.has(record.bucket);
    found(
      targets.r2Buckets,
      exists ? { name: record.bucket, keys: await cloudflare.listR2Objects(record.bucket) } : undefined,
    );
  }

  const d1 = of('d1');
  const databases = d1.length > 0 ? await cloudflare.listD1Databases() : [];
  for (const record of d1)
    found(
      targets.d1Databases,
      databases.find((database) => database.name === record.name),
    );
  return targets;
}

/**
 * Resolves the recorded routes and wildcard DNS records within their zones. A wildcard DNS record
 * that a route left in place still serves stays too, or that route's hosts would stop resolving.
 */
async function resolveZoneTargets(
  cloudflare: CloudflareClient,
  routes: RecordOf<'route'>[],
  dns: RecordOf<'dns'>[],
  otherOwner: (owner: string | undefined) => string | undefined,
  targets: CleanupTargets,
  found: <T>(list: T[], item: T | undefined) => void,
): Promise<void> {
  const zoneIds = await recordedZoneIds(cloudflare, [...routes, ...dns]);
  const routeTable = perZone((zoneId) => cloudflare.listWorkerRoutes(zoneId));
  /** The pattern of each route left in place, by the `zone/name` of the wildcard DNS record it needs. */
  const neededDns = new Map<string, string>();
  for (const record of routes) {
    const zoneId = zoneIds.get(record.zone);
    const route = (await routeTable(zoneId)).find(
      (candidate) => candidate.pattern.toLowerCase() === record.pattern.toLowerCase(),
    );
    const owner = otherOwner(route?.script);
    if (owner !== undefined) {
      targets.leftInPlace.push(`route ${record.pattern} (bound to ${owner})`);
      const wildcard = wildcardDnsRecord({ pattern: record.pattern, zoneName: record.zone });
      if (wildcard) neededDns.set(`${hostName(record.zone)}/${hostName(wildcard.name)}`, record.pattern);
    } else {
      found(targets.routes, zoneId !== undefined && route ? { zoneId, route } : undefined);
    }
  }
  const dnsTable = perZone((zoneId) => cloudflare.listDnsRecords(zoneId));
  for (const record of dns) {
    const zoneId = zoneIds.get(record.zone);
    // Only the proxied CNAME the deploy creates, `*.<host>` -> `<host>`; anything else under the
    // name is not the one it made.
    const target = record.name.replace(/^\*\./, '');
    const live = (await dnsTable(zoneId)).find(
      (candidate) =>
        candidate.type === 'CNAME' && sameName(candidate.name, record.name) && sameName(candidate.content, target),
    );
    const servedRoute = neededDns.get(`${hostName(record.zone)}/${hostName(record.name)}`);
    if (live && servedRoute !== undefined) {
      targets.leftInPlace.push(`DNS record ${record.name} (serves route ${servedRoute})`);
    } else {
      found(targets.dnsRecords, zoneId !== undefined && live ? { zoneId, record: live } : undefined);
    }
  }
}

/** Lists a zone's table at most once, however many records name the zone; a zone that is gone has none. */
function perZone<T>(list: (zoneId: string) => Promise<T[]>): (zoneId: string | undefined) => Promise<T[]> {
  const tables = new Map<string, Promise<T[]>>();
  return (zoneId) => {
    if (zoneId === undefined) return Promise.resolve([]);
    let table = tables.get(zoneId);
    if (table === undefined) {
      table = list(zoneId);
      tables.set(zoneId, table);
    }
    return table;
  };
}

/** Each recorded zone's id, looked up by name within the account; a zone that is gone has none. */
async function recordedZoneIds(
  cloudflare: CloudflareClient,
  records: (RecordOf<'route'> | RecordOf<'dns'>)[],
): Promise<Map<string, string | undefined>> {
  const ids = new Map<string, string | undefined>();
  for (const { zone } of records) {
    if (ids.has(zone)) continue;
    ids.set(zone, (await cloudflare.listZones(zone)).find((candidate) => sameName(candidate.name, zone))?.id);
  }
  return ids;
}

/**
 * Deletes in dependency order: what routes traffic to a Worker before the Worker, the Worker
 * before the storage it binds, a bucket's objects before the bucket. A custom domain takes its own
 * DNS record with it.
 */
async function deleteCleanupTargets(
  cloudflare: CloudflareClient,
  targets: CleanupTargets,
  deleted: CleanupCounts,
): Promise<void> {
  for (const domain of targets.domains) {
    await cloudflare.deleteWorkerDomain(domain.id);
    deleted.domains += 1;
  }
  for (const { zoneId, route } of targets.routes) {
    await cloudflare.deleteWorkerRoute(zoneId, route.id);
    deleted.routes += 1;
  }
  for (const { zoneId, record } of targets.dnsRecords) {
    await cloudflare.deleteDnsRecord(zoneId, record.id);
    deleted.dnsRecords += 1;
  }
  for (const worker of targets.workers) {
    await cloudflare.deleteWorkerScript(worker);
    deleted.workers += 1;
  }
  for (const namespace of targets.kvNamespaces) {
    await cloudflare.deleteKvNamespace(namespace.id);
    deleted.kvNamespaces += 1;
  }
  for (const bucket of targets.r2Buckets) {
    for (const key of bucket.keys) {
      await cloudflare.deleteR2Object(bucket.name, key);
      deleted.r2Objects += 1;
    }
    await cloudflare.deleteR2Bucket(bucket.name);
    deleted.r2Buckets += 1;
  }
  for (const database of targets.d1Databases) {
    await cloudflare.deleteD1Database(database.uuid);
    deleted.d1Databases += 1;
  }
}

/** One recorded item, whichever Worker of the stage recorded it. */
function recordIdentity(record: StageRecord): string {
  switch (record.kind) {
    case 'worker':
      return `worker/${record.worker}`;
    case 'kv':
      return `kv/${record.title}`;
    case 'd1':
      return `d1/${record.name}`;
    case 'r2':
      return `r2/${record.bucket}`;
    case 'domain':
      return `domain/${hostName(record.hostname)}`;
    case 'route':
      return `route/${hostName(record.zone)}/${record.pattern.toLowerCase()}`;
    case 'dns':
      return `dns/${hostName(record.zone)}/${hostName(record.name)}`;
  }
}

/** DNS names compare case-insensitively, and a trailing dot names the same host. */
function hostName(name: string): string {
  return name.toLowerCase().replace(/\.$/, '');
}

function sameName(left: string, right: string): boolean {
  return hostName(left) === hostName(right);
}

async function reconcileStageResources(
  plan: ConfiguredStageResourcePlan,
  cloudflare: CloudflareClient,
): Promise<boolean> {
  const namespaces = await cloudflare.listKvNamespaces();
  const namespaceIds = new Set(namespaces.map((namespace) => namespace.id));
  for (const binding of plan.kvNamespaces) {
    if (!namespaceIds.has(binding.id)) {
      throw new Error(`KV binding ${binding.binding} references missing namespace ${binding.id}.`);
    }
  }

  const buckets = await r2BucketNames(cloudflare);
  for (const binding of plan.r2Buckets) {
    await ensureR2Bucket(binding.bucketName, buckets, cloudflare);
  }

  const zones = await cloudflare.listZones();
  const zoneByName = new Map(zones.map((zone) => [zone.name, zone]));
  const dnsNamesByZone = new Map<string, Set<string>>();
  for (const route of plan.routes) {
    const wildcard = wildcardDnsRecord(route);
    if (!wildcard) continue;
    const zone = zoneByName.get(wildcard.zoneName);
    if (!zone) throw new Error(`Cloudflare zone ${wildcard.zoneName} is not available to the deployment token.`);
    let names = dnsNamesByZone.get(zone.id);
    if (!names) {
      names = new Set((await cloudflare.listDnsRecords(zone.id)).map((record) => record.name));
      dnsNamesByZone.set(zone.id, names);
    }
    if (!names.has(wildcard.name)) {
      try {
        await cloudflare.createDnsRecord(zone.id, wildcard.name, wildcard.content);
      } catch (error) {
        const exists = (await cloudflare.listDnsRecords(zone.id)).some((record) => record.name === wildcard.name);
        if (!exists) throw error;
      }
      names.add(wildcard.name);
    }
  }

  const scripts = await cloudflare.listWorkerScripts();
  const workerExists = scripts.some((script) => script.id === plan.workerName);
  if (!workerExists) return false;

  const domains = await cloudflare.listWorkerDomains();
  for (const route of plan.routes) {
    if (!route.customDomain) continue;
    const existing = domains.find((domain) => domain.hostname === route.pattern);
    if (existing?.service === plan.workerName) continue;
    if (existing) {
      throw new Error(`Custom domain ${route.pattern} is already attached to ${existing.service ?? 'another Worker'}.`);
    }
    const zone = route.zoneName ? zoneByName.get(route.zoneName) : zoneContaining(zones, route.pattern);
    if (!zone) throw new Error(`No accessible Cloudflare zone contains custom domain ${route.pattern}.`);
    await cloudflare.createWorkerDomain(route.pattern, plan.workerName, zone.id);
  }

  for (const zone of zones) {
    const desired = plan.routes.filter((route) => !route.customDomain && route.zoneName === zone.name);
    if (desired.length === 0) continue;
    const routes = await cloudflare.listWorkerRoutes(zone.id);
    for (const route of desired) {
      const existing = routes.find((candidate) => candidate.pattern === route.pattern);
      if (existing?.script === plan.workerName) continue;
      if (existing) {
        throw new Error(`Worker route ${route.pattern} is already attached to ${existing.script ?? 'another Worker'}.`);
      }
      await cloudflare.createWorkerRoute(zone.id, route.pattern, plan.workerName);
    }
  }
  return true;
}

export function nxTaskVersionTag(environment: NodeJS.ProcessEnv): string | undefined {
  const hash = environment.NX_TASK_HASH;
  const underNx =
    hash !== undefined ||
    environment.NX_TASK_TARGET_PROJECT !== undefined ||
    environment.NX_TASK_TARGET_TARGET !== undefined;
  if (!underNx) return undefined;
  if (!hash) throw new Error('NX_TASK_HASH is required when deploy-stage runs under Nx.');
  if (/^(?:0|[1-9][0-9]*)$/.test(hash)) {
    return `nx-${hash}`;
  }
  const normalized = hash.toLowerCase();
  if (/^[0-9a-f]{32,}$/.test(normalized)) {
    return `nx-${normalized.slice(0, 32)}`;
  }
  throw new Error('NX_TASK_HASH must be canonical decimal digits or at least 32 hexadecimal characters.');
}

/** What a freshly activated version has to be true of, and where the fact gets recorded. */
interface DeployedVersionIdentity {
  stage: DeploymentStage;
  workerName: string;
  accountId: string;
}

/**
 * Turns "Cloudflare accepted the traffic shift" into "the new version answers".
 *
 * Without this the deploy step goes green while the edge still serves the previous version — the
 * measured gap was about 20 s — so anything ordered after this deploy could call into code that
 * has not shipped yet. The wait is what a `dependsOn: ["<other>:deploy"]` edge actually buys; an
 * edge onto a step that returns early orders nothing.
 */
async function confirmVersionIsLive(
  probe: LiveVersionProbe,
  versionTag: string,
  identity: DeployedVersionIdentity,
  options: DeployStageOptions,
  dependencies: WranglerCommandDependencies,
  cwd: string,
): Promise<void> {
  const wait = dependencies.wait ?? {};
  const live = await awaitLiveVersion(probe, versionTag, wait);
  if (!live.ok) throw new Error(`${identity.workerName}: ${live.error.message}`);
  if (options.versionEndpoint) {
    const answered = await awaitVersionEndpoint(options.versionEndpoint, versionTag, wait);
    if (!answered.ok) throw new Error(`${identity.workerName}: ${answered.error.message}`);
  }
  const processEnv = dependencies.processEnv ?? process.env;
  const directory = dependencies.liveVersionCacheDirectory ?? liveVersionCacheDirectory(cwd, processEnv);
  await writeCachedLiveVersion(
    directory,
    { accountId: identity.accountId, workerName: identity.workerName, stage: identity.stage },
    { versionTag, versionId: live.value.versionId, fetchedAt: Date.now() },
  );
}

/** One wrangler invocation whose stdout must be JSON; shared by the deploy and the read-only query. */
export async function wranglerJson(runner: ProcessRunner, args: string[], run: ProcessRunOptions): Promise<unknown> {
  const result = await wrangler(runner, args, run);
  try {
    return JSON.parse(result.stdout);
  } catch {
    throw new Error(`wrangler ${args.join(' ')} returned invalid JSON.`);
  }
}

async function wrangler(runner: ProcessRunner, args: string[], run: ProcessRunOptions): Promise<ProcessResult> {
  const result = await runner.run('wrangler', args, run);
  if (result.exitCode !== 0) {
    printCommandOutput(result.stdout, result.stderr);
    throw new Error(`wrangler ${args.join(' ')} failed with exit code ${result.exitCode}`);
  }
  return result;
}

function requiredEnvironmentValue(value: string | undefined, name: string): string {
  if (!value) throw new Error(`${name} is required.`);
  return value;
}
