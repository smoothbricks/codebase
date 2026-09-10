import { randomUUID } from 'node:crypto';
import { readFile, rm, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { parseJsonFileText } from '../lib/json.js';
import { mergeEnv, printCommandOutput } from '../lib/run.js';
import { type CloudflareClient, CloudflareRestClient, type D1DatabaseRecord } from './cloudflare.js';
import { type FlatWranglerConfig, parseFlatWranglerConfig, planFlatStageResources } from './flat-config.js';
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
import {
  type ConfiguredStageResourcePlan,
  type DeploymentStage,
  derivePullRequestStageConfig,
  derivePullRequestWranglerConfig,
  hasExactStageSegment,
  isPullRequestStage,
  type LiveKvNamespace,
  type PullRequestResourcePlan,
  parseDeploymentStage,
  planConfiguredStageResources,
  planPullRequestBindings,
  planPullRequestResources,
  pullRequestStage,
} from './stage.js';
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
  /** A build-generated flat wrangler.json to deploy instead of `./wrangler.toml` (see `prepareFlatConfig`). */
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
  let temporaryConfigPath: string | undefined;
  let temporarySecretsPath: string | undefined;
  try {
    const prepared = options.config
      ? await prepareFlatConfig(options.config, stage, accountId, gate, cloudflare)
      : await prepareTomlConfig(cwd, stage, accountId, gate, cloudflare);
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
  /** D1 bindings whose migrations run before the deploy (flat configs only; TOML deploys keep migrations out of scope). */
  d1MigrationBindings: string[];
  /** `--env <stage>` for TOML configs with env blocks; a flat config is already resolved, so no flag. */
  envFlag?: DeploymentStage;
}

async function prepareTomlConfig(
  cwd: string,
  stage: DeploymentStage,
  accountId: string,
  gate: SecretGate,
  cloudflare: CloudflareClient,
): Promise<PreparedConfig> {
  const committedConfigPath = join(cwd, 'wrangler.toml');
  const committedToml = await readFile(committedConfigPath, 'utf8');
  if (!isPullRequestStage(stage)) {
    return {
      deployConfigPath: committedConfigPath,
      plan: planConfiguredStageResources(committedToml, stage),
      d1MigrationBindings: [],
      envFlag: stage,
    };
  }
  const liveNamespaces = await cloudflare.listKvNamespaces();
  const plan = planPullRequestResources(committedToml, stage, liveNamespaces);
  const { kvNamespaceIds, d1DatabaseIds } = await provisionPullRequestResources(plan, gate, cloudflare, liveNamespaces);
  const derivedToml = derivePullRequestWranglerConfig(committedToml, {
    stage,
    accountId,
    kvNamespaceIds,
    d1DatabaseIds,
  });
  const temporaryConfigPath = join(cwd, `.wrangler.smoo-${process.pid}-${randomUUID()}.toml`);
  await writeTemporaryConfig(temporaryConfigPath, derivedToml);
  return {
    deployConfigPath: temporaryConfigPath,
    temporaryConfigPath,
    plan: planConfiguredStageResources(derivedToml, stage),
    d1MigrationBindings: [],
    envFlag: stage,
  };
}

async function prepareFlatConfig(
  configPath: string,
  stage: DeploymentStage,
  accountId: string,
  gate: SecretGate,
  cloudflare: CloudflareClient,
): Promise<PreparedConfig> {
  const flat = parseJsonFileText(configPath, await readFile(configPath, 'utf8'), parseFlatWranglerConfig);
  if (!isPullRequestStage(stage)) {
    return {
      deployConfigPath: configPath,
      plan: planFlatStageResources(flat, stage),
      d1MigrationBindings: migrationBindings(flat),
    };
  }
  const liveNamespaces = await cloudflare.listKvNamespaces();
  const plan = planPullRequestBindings(flat, stage, liveNamespaces);
  const { kvNamespaceIds, d1DatabaseIds } = await provisionPullRequestResources(plan, gate, cloudflare, liveNamespaces);
  const derived = derivePullRequestStageConfig(flat, { stage, accountId, kvNamespaceIds, d1DatabaseIds });
  // Beside the original: its main/assets/migrations paths are relative to the file.
  const temporaryConfigPath = join(dirname(configPath), `.wrangler.smoo-${process.pid}-${randomUUID()}.json`);
  await writeTemporaryConfig(temporaryConfigPath, `${JSON.stringify(derived, null, 2)}\n`);
  return {
    deployConfigPath: temporaryConfigPath,
    temporaryConfigPath,
    plan: planFlatStageResources(derived, stage),
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

/** Isolation refusals already ran in the plan; the gate below is the last one before the first write. */
async function provisionPullRequestResources(
  plan: PullRequestResourcePlan,
  gate: SecretGate,
  cloudflare: CloudflareClient,
  liveNamespaces: LiveKvNamespace[],
): Promise<{ kvNamespaceIds: Map<string, string>; d1DatabaseIds: Map<string, string> }> {
  await gate(plan.workerName);
  const kvNamespaceIds = await ensureKvNamespaces(plan.kvNamespaces, liveNamespaces, cloudflare);
  const d1DatabaseIds =
    plan.d1Databases.length === 0
      ? new Map<string, string>()
      : await ensureD1Databases(plan.d1Databases, await cloudflare.listD1Databases(), cloudflare);
  return { kvNamespaceIds, d1DatabaseIds };
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

export interface CleanupResult {
  stage: `pr${number}`;
  deleted: {
    workers: number;
    routes: number;
    domains: number;
    kvNamespaces: number;
    r2Buckets: number;
    r2Objects: number;
    dnsRecords: number;
    d1Databases: number;
  };
}

export async function cleanupPullRequest(
  cwd: string,
  prNumber: number,
  dependencies: WranglerCommandDependencies = {},
): Promise<CleanupResult> {
  const stage = pullRequestStage(prNumber);
  const processEnv = dependencies.processEnv ?? process.env;
  const cloudflare =
    dependencies.cloudflare ??
    new CloudflareRestClient(
      requiredEnvironmentValue(processEnv.CLOUDFLARE_ACCOUNT_ID, 'CLOUDFLARE_ACCOUNT_ID'),
      requiredEnvironmentValue(processEnv.CLOUDFLARE_API_TOKEN, 'CLOUDFLARE_API_TOKEN'),
    );
  void cwd;
  const deleted = {
    workers: 0,
    routes: 0,
    domains: 0,
    kvNamespaces: 0,
    r2Buckets: 0,
    r2Objects: 0,
    dnsRecords: 0,
    d1Databases: 0,
  };

  // Every listing, D1 included, must succeed before the first delete: a missing D1
  // permission must not leave workers/KV/R2 already gone.
  const domains = await cloudflare.listWorkerDomains();
  const zones = await cloudflare.listZones();
  const routesByZone = [];
  const dnsByZone = [];
  for (const zone of zones) {
    routesByZone.push({ zone, routes: await cloudflare.listWorkerRoutes(zone.id) });
    dnsByZone.push({ zone, records: await cloudflare.listDnsRecords(zone.id) });
  }
  const scripts = await cloudflare.listWorkerScripts();
  const namespaces = await cloudflare.listKvNamespaces();
  const buckets = await cloudflare.listR2Buckets();
  const objectsByBucket = [];
  for (const bucket of buckets) {
    if (!hasExactStageSegment(bucket.name, stage)) continue;
    objectsByBucket.push({ bucket, keys: await cloudflare.listR2Objects(bucket.name) });
  }
  const databases = await cloudflare.listD1Databases();

  for (const domain of domains) {
    if (!hasExactStageSegment(domain.hostname, stage)) continue;
    await cloudflare.deleteWorkerDomain(domain.id);
    deleted.domains += 1;
  }
  for (const { zone, routes } of routesByZone) {
    for (const route of routes) {
      if (!hasExactStageSegment(route.pattern, stage)) continue;
      await cloudflare.deleteWorkerRoute(zone.id, route.id);
      deleted.routes += 1;
    }
  }
  for (const { zone, records } of dnsByZone) {
    for (const record of records) {
      if (!hasExactStageSegment(record.name, stage)) continue;
      await cloudflare.deleteDnsRecord(zone.id, record.id);
      deleted.dnsRecords += 1;
    }
  }
  for (const script of scripts) {
    if (!hasExactStageSegment(script.id, stage)) continue;
    await cloudflare.deleteWorkerScript(script.id);
    deleted.workers += 1;
  }
  for (const namespace of namespaces) {
    if (!hasExactStageSegment(namespace.title, stage)) continue;
    await cloudflare.deleteKvNamespace(namespace.id);
    deleted.kvNamespaces += 1;
  }
  for (const { bucket, keys } of objectsByBucket) {
    for (const key of keys) {
      await cloudflare.deleteR2Object(bucket.name, key);
      deleted.r2Objects += 1;
    }
    await cloudflare.deleteR2Bucket(bucket.name);
    deleted.r2Buckets += 1;
  }
  for (const database of databases) {
    if (!hasExactStageSegment(database.name, stage)) continue;
    await cloudflare.deleteD1Database(database.uuid);
    deleted.d1Databases += 1;
  }
  return { stage, deleted };
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

  const buckets = new Set((await cloudflare.listR2Buckets()).map((bucket) => bucket.name));
  for (const binding of plan.r2Buckets) {
    if (buckets.has(binding.bucketName)) continue;
    try {
      await cloudflare.createR2Bucket(binding.bucketName);
    } catch (error) {
      const exists = (await cloudflare.listR2Buckets()).some((bucket) => bucket.name === binding.bucketName);
      if (!exists) throw error;
    }
    buckets.add(binding.bucketName);
  }

  const zones = await cloudflare.listZones();
  const zoneByName = new Map(zones.map((zone) => [zone.name, zone]));
  const dnsNamesByZone = new Map<string, Set<string>>();
  for (const route of plan.routes) {
    if (!route.pattern.startsWith('*.') || !route.zoneName) continue;
    const zone = zoneByName.get(route.zoneName);
    if (!zone) throw new Error(`Cloudflare zone ${route.zoneName} is not available to the deployment token.`);
    const hostname = route.pattern.slice(0, route.pattern.indexOf('/')).replace(/^\*\./, '');
    const wildcard = `*.${hostname}`;
    let names = dnsNamesByZone.get(zone.id);
    if (!names) {
      names = new Set((await cloudflare.listDnsRecords(zone.id)).map((record) => record.name));
      dnsNamesByZone.set(zone.id, names);
    }
    if (!names.has(wildcard)) {
      try {
        await cloudflare.createDnsRecord(zone.id, wildcard, hostname);
      } catch (error) {
        const exists = (await cloudflare.listDnsRecords(zone.id)).some((record) => record.name === wildcard);
        if (!exists) throw error;
      }
      names.add(wildcard);
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
    const zone = route.zoneName
      ? zoneByName.get(route.zoneName)
      : zones
          .filter((candidate) => route.pattern === candidate.name || route.pattern.endsWith(`.${candidate.name}`))
          .sort((left, right) => right.name.length - left.name.length)[0];
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
