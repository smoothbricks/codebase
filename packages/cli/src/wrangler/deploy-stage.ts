import { randomUUID } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import { readFile, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import typia from 'typia';
import { printCommandOutput } from '../lib/run.js';
import { type CloudflareClient, CloudflareRestClient } from './cloudflare.js';
import { parseDevVarsExample } from './prepare-env.js';
import {
  type ConfiguredStageResourcePlan,
  type DeploymentStage,
  derivePullRequestWranglerConfig,
  hasExactStageSegment,
  isPullRequestStage,
  parseDeploymentStage,
  planConfiguredStageResources,
  planPullRequestResources,
  pullRequestStage,
  stageResourceName,
} from './stage.js';

export interface ProcessResult {
  exitCode: number;
  stdout: string;
  stderr: string;
}

export interface ProcessRunner {
  run(command: string, args: string[], options: { cwd: string; env?: Record<string, string> }): Promise<ProcessResult>;
}

export class BunProcessRunner implements ProcessRunner {
  async run(
    command: string,
    args: string[],
    options: { cwd: string; env?: Record<string, string> },
  ): Promise<ProcessResult> {
    const child = Bun.spawn([command, ...args], {
      cwd: options.cwd,
      env: { ...process.env, ...options.env },
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
}

export interface DeployStageResult {
  stage: DeploymentStage;
  workerName: string;
  action: 'deployed' | 'activated' | 'remote-cache-hit';
  versionTag?: string;
}

const isUnknownRecord = typia.createIs<Record<string, unknown>>();

export async function deployStage(
  cwd: string,
  stageValue: string,
  dependencies: WranglerCommandDependencies = {},
): Promise<DeployStageResult> {
  const stage = parseDeploymentStage(stageValue);
  const processEnv = dependencies.processEnv ?? process.env;
  const accountId = processEnv.CLOUDFLARE_ACCOUNT_ID;
  const apiToken = processEnv.CLOUDFLARE_API_TOKEN;
  if (!accountId) throw new Error('CLOUDFLARE_ACCOUNT_ID is required.');
  const cloudflare =
    dependencies.cloudflare ??
    new CloudflareRestClient(accountId, requiredEnvironmentValue(apiToken, 'CLOUDFLARE_API_TOKEN'));
  const runner = dependencies.runner ?? new BunProcessRunner();
  const committedConfigPath = join(cwd, 'wrangler.toml');
  const committedToml = await readFile(committedConfigPath, 'utf8');
  const secretNames = readSecretNames(cwd);
  const secretValues: Record<string, string> = {};
  for (const name of secretNames) {
    const value = processEnv[name];
    if (value) secretValues[name] = value;
  }
  const missingSecrets = secretNames.filter((name) => !processEnv[name]);
  let configPath = committedConfigPath;
  let temporaryConfigPath: string | undefined;
  let temporarySecretsPath: string | undefined;
  try {
    if (isPullRequestStage(stage)) {
      const staging = planConfiguredStageResources(committedToml, 'staging');
      if (!staging.workerName.endsWith('-staging')) {
        throw new Error('[env.staging].name must end with the exact suffix -staging.');
      }
      const workerName = stageResourceName(staging.workerName.slice(0, -'-staging'.length), stage);
      const firstDeployment = !(await cloudflare.listWorkerScripts()).some((script) => script.id === workerName);
      if (firstDeployment && missingSecrets.length > 0) {
        throw new Error(
          `First deployment of ${workerName} requires process environment values for: ${missingSecrets.join(', ')}.`,
        );
      }
      const liveNamespaces = await cloudflare.listKvNamespaces();
      const plan = planPullRequestResources(committedToml, stage, liveNamespaces);
      const derivedIds = new Map<string, string>();
      const byTitle = new Map(liveNamespaces.map((namespace) => [namespace.title, namespace]));
      for (const namespace of plan.kvNamespaces) {
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
      const derivedToml = derivePullRequestWranglerConfig(committedToml, {
        stage: stage,
        accountId,
        kvNamespaceIds: derivedIds,
      });
      temporaryConfigPath = join(cwd, `.wrangler.smoo-${process.pid}-${randomUUID()}.toml`);
      await writeFile(temporaryConfigPath, derivedToml, { mode: 0o600 });
      configPath = temporaryConfigPath;
    }

    const toml = configPath === committedConfigPath ? committedToml : await readFile(configPath, 'utf8');
    const plan = planConfiguredStageResources(toml, stage);
    const workerExists = await reconcileStageResources(plan, cloudflare);
    const versionTag = nxTaskVersionTag(processEnv);
    const commandContext = { cwd, configPath, stage, workerName: plan.workerName };

    if (versionTag && workerExists) {
      const versions = await wranglerJson(runner, ['versions', 'list', '--name', plan.workerName, '--json'], cwd);
      const deployments = await wranglerJson(
        runner,
        ['deployments', 'status', '--name', plan.workerName, '--json'],
        cwd,
      );
      const versionId = findVersionIdByTag(versions, versionTag);
      if (versionId && isFullCurrentDeployment(deployments, versionId)) {
        return { stage, workerName: plan.workerName, action: 'remote-cache-hit', versionTag };
      }
      if (versionId) {
        await wrangler(
          runner,
          [
            'versions',
            'deploy',
            '--version-tag',
            versionTag,
            '--name',
            plan.workerName,
            '--config',
            configPath,
            '--env',
            stage,
            '--yes',
          ],
          cwd,
        );
        return { stage, workerName: plan.workerName, action: 'activated', versionTag };
      }
    }

    const deployArgs = ['deploy', '--config', commandContext.configPath, '--env', commandContext.stage];
    if (versionTag) deployArgs.push('--tag', versionTag);
    if (Object.keys(secretValues).length > 0) {
      temporarySecretsPath = join(cwd, `.wrangler-secrets.smoo-${process.pid}-${randomUUID()}.json`);
      await writeFile(temporarySecretsPath, `${JSON.stringify(secretValues)}\n`, { mode: 0o600 });
      deployArgs.push('--secrets-file', temporarySecretsPath);
    }
    await wrangler(runner, deployArgs, cwd);
    return {
      stage,
      workerName: commandContext.workerName,
      action: 'deployed',
      ...(versionTag ? { versionTag } : {}),
    };
  } finally {
    if (temporaryConfigPath) {
      await rm(temporaryConfigPath, { force: true });
    }
    if (temporarySecretsPath) {
      await rm(temporarySecretsPath, { force: true });
    }
  }
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
  };

  for (const domain of await cloudflare.listWorkerDomains()) {
    if (!hasExactStageSegment(domain.hostname, stage)) continue;
    await cloudflare.deleteWorkerDomain(domain.id);
    deleted.domains += 1;
  }

  for (const zone of await cloudflare.listZones()) {
    for (const route of await cloudflare.listWorkerRoutes(zone.id)) {
      if (!hasExactStageSegment(route.pattern, stage)) continue;
      await cloudflare.deleteWorkerRoute(zone.id, route.id);
      deleted.routes += 1;
    }
    for (const record of await cloudflare.listDnsRecords(zone.id)) {
      if (!hasExactStageSegment(record.name, stage)) continue;
      await cloudflare.deleteDnsRecord(zone.id, record.id);
      deleted.dnsRecords += 1;
    }
  }

  for (const script of await cloudflare.listWorkerScripts()) {
    if (!hasExactStageSegment(script.id, stage)) continue;
    await cloudflare.deleteWorkerScript(script.id);
    deleted.workers += 1;
  }
  for (const namespace of await cloudflare.listKvNamespaces()) {
    if (!hasExactStageSegment(namespace.title, stage)) continue;
    await cloudflare.deleteKvNamespace(namespace.id);
    deleted.kvNamespaces += 1;
  }
  for (const bucket of await cloudflare.listR2Buckets()) {
    if (!hasExactStageSegment(bucket.name, stage)) continue;
    for (const key of await cloudflare.listR2Objects(bucket.name)) {
      await cloudflare.deleteR2Object(bucket.name, key);
      deleted.r2Objects += 1;
    }
    await cloudflare.deleteR2Bucket(bucket.name);
    deleted.r2Buckets += 1;
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

export function findVersionIdByTag(value: unknown, tag: string): string | null {
  if (Array.isArray(value)) {
    for (const entry of value) {
      const found = findVersionIdByTag(entry, tag);
      if (found) return found;
    }
    return null;
  }
  if (!isUnknownRecord(value)) return null;
  const annotations = isUnknownRecord(value.annotations) ? value.annotations : undefined;
  const metadata = isUnknownRecord(value.metadata) ? value.metadata : undefined;
  const annotationTag = annotations?.['workers/tag'];
  const candidateTag =
    typeof annotationTag === 'string' ? annotationTag : typeof value.tag === 'string' ? value.tag : metadata?.tag;
  if (candidateTag === tag) {
    if (typeof value.id === 'string') return value.id;
    if (typeof value.version_id === 'string') return value.version_id;
  }
  for (const nested of Object.values(value)) {
    const found = findVersionIdByTag(nested, tag);
    if (found) return found;
  }
  return null;
}

export function isFullCurrentDeployment(value: unknown, versionId: string): boolean {
  if (Array.isArray(value)) return value.some((entry) => isFullCurrentDeployment(entry, versionId));
  if (!isUnknownRecord(value)) return false;
  if (Array.isArray(value.versions)) {
    return (
      value.versions.length === 1 &&
      value.versions.some(
        (version) =>
          isUnknownRecord(version) &&
          (version.version_id === versionId || version.id === versionId) &&
          Number(version.percentage) === 100,
      )
    );
  }
  return Object.values(value).some((entry) => isFullCurrentDeployment(entry, versionId));
}

async function wranglerJson(runner: ProcessRunner, args: string[], cwd: string): Promise<unknown> {
  const result = await wrangler(runner, args, cwd);
  try {
    return JSON.parse(result.stdout);
  } catch {
    throw new Error(`wrangler ${args.join(' ')} returned invalid JSON.`);
  }
}

async function wrangler(runner: ProcessRunner, args: string[], cwd: string): Promise<ProcessResult> {
  const result = await runner.run('wrangler', args, { cwd });
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

function readSecretNames(cwd: string): string[] {
  const path = join(cwd, '.dev.vars.example');
  return existsSync(path) ? parseDevVarsExample(readFileSync(path, 'utf8')) : [];
}
