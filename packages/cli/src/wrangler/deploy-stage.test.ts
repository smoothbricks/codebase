import { afterEach, describe, expect, it } from 'bun:test';
import { existsSync, readFileSync, statSync } from 'node:fs';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type {
  CloudflareClient,
  CloudflareZone,
  DnsRecord,
  R2Bucket,
  WorkerDomain,
  WorkerRoute,
  WorkerScript,
} from './cloudflare.js';
import { cleanupPullRequest, deployStage, type ProcessResult, type ProcessRunner } from './deploy-stage.js';
import type { LiveKvNamespace } from './stage.js';

const HASH = '16577780061662788004';
const FIXTURE = `[env.staging]
name = "fixture-worker-staging"
workers_dev = false

[env.staging.vars]
ENVIRONMENT = "staging"
`;

const ROUTED_FIXTURE = `${FIXTURE}
[[env.staging.routes]]
pattern = "*.staging.conloca.com/*"
zone_name = "conloca.com"
`;

const roots: string[] = [];

function requiredTestValue<T>(value: T | undefined, name: string): T {
  if (value === undefined) throw new Error(`${name} was not captured.`);
  return value;
}

afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

class FakeRunner implements ProcessRunner {
  readonly calls: string[][] = [];
  configPathSeen: string | undefined;
  secretsPathSeen: string | undefined;
  secretsMode: number | undefined;
  secretsJson: string | undefined;

  constructor(
    private readonly versions: unknown,
    private readonly deployment: unknown,
  ) {}

  async run(_command: string, args: string[]): Promise<ProcessResult> {
    this.calls.push(args);
    if (args[0] === 'versions' && args[1] === 'list') {
      return success(this.versions);
    }
    if (args[0] === 'deployments' && args[1] === 'status') {
      return success(this.deployment);
    }
    const configIndex = args.indexOf('--config');
    if (configIndex >= 0) {
      this.configPathSeen = args[configIndex + 1];
      expect(this.configPathSeen && existsSync(this.configPathSeen)).toBe(true);
    }
    const secretsIndex = args.indexOf('--secrets-file');
    if (secretsIndex >= 0) {
      this.secretsPathSeen = args[secretsIndex + 1];
      const secretsPath = requiredTestValue(this.secretsPathSeen, 'secrets path');
      this.secretsMode = statSync(secretsPath).mode & 0o777;
      this.secretsJson = readFileSync(secretsPath, 'utf8');
    }
    return success({});
  }
}

class FakeCloudflare implements CloudflareClient {
  namespaces: LiveKvNamespace[] = [];
  buckets: R2Bucket[] = [];
  scripts: WorkerScript[] = [{ id: 'fixture-worker-pr123' }];
  domains: WorkerDomain[] = [];
  zones: CloudflareZone[] = [];
  routes: Record<string, WorkerRoute[]> = {};
  records: Record<string, DnsRecord[]> = {};
  objects: Record<string, string[]> = {};
  mutations: string[] = [];

  async listKvNamespaces(): Promise<LiveKvNamespace[]> {
    return this.namespaces;
  }
  async createKvNamespace(title: string): Promise<LiveKvNamespace> {
    this.mutations.push(`create-kv:${title}`);
    const namespace = { id: `id-${title}`, title };
    this.namespaces.push(namespace);
    return namespace;
  }
  async deleteKvNamespace(id: string): Promise<void> {
    this.mutations.push(`delete-kv:${id}`);
  }
  async listR2Buckets(): Promise<R2Bucket[]> {
    return this.buckets;
  }
  async createR2Bucket(name: string): Promise<void> {
    this.mutations.push(`create-r2:${name}`);
    this.buckets.push({ name });
  }
  async listR2Objects(bucket: string): Promise<string[]> {
    return this.objects[bucket] ?? [];
  }
  async deleteR2Object(bucket: string, key: string): Promise<void> {
    this.mutations.push(`delete-object:${bucket}:${key}`);
  }
  async deleteR2Bucket(name: string): Promise<void> {
    this.mutations.push(`delete-r2:${name}`);
  }
  async listWorkerScripts(): Promise<WorkerScript[]> {
    return this.scripts;
  }
  async deleteWorkerScript(name: string): Promise<void> {
    this.mutations.push(`delete-worker:${name}`);
  }
  async listWorkerDomains(): Promise<WorkerDomain[]> {
    return this.domains;
  }
  async createWorkerDomain(hostname: string, workerName: string): Promise<void> {
    this.mutations.push(`create-domain:${hostname}:${workerName}`);
  }
  async deleteWorkerDomain(id: string): Promise<void> {
    this.mutations.push(`delete-domain:${id}`);
  }
  async listZones(): Promise<CloudflareZone[]> {
    return this.zones;
  }
  async listWorkerRoutes(zoneId: string): Promise<WorkerRoute[]> {
    return this.routes[zoneId] ?? [];
  }
  async createWorkerRoute(zoneId: string, pattern: string, workerName: string): Promise<void> {
    this.mutations.push(`create-route:${zoneId}:${pattern}:${workerName}`);
  }
  async deleteWorkerRoute(zoneId: string, routeId: string): Promise<void> {
    this.mutations.push(`delete-route:${zoneId}:${routeId}`);
  }
  async listDnsRecords(zoneId: string): Promise<DnsRecord[]> {
    return this.records[zoneId] ?? [];
  }
  async createDnsRecord(zoneId: string, name: string, content: string): Promise<void> {
    this.mutations.push(`create-dns:${zoneId}:${name}:${content}`);
  }
  async deleteDnsRecord(zoneId: string, recordId: string): Promise<void> {
    this.mutations.push(`delete-dns:${zoneId}:${recordId}`);
  }
}

describe('deploy-stage remote version fallback', () => {
  it('returns a remote cache hit for the active tagged 100% version', async () => {
    const root = await fixtureRoot();
    const runner = new FakeRunner([{ id: 'version-1', annotations: { 'workers/tag': `nx-${HASH}` } }], {
      versions: [{ version_id: 'version-1', percentage: 100 }],
    });

    const result = await deployStage(root, 'pr123', dependencies(runner, new FakeCloudflare()));

    expect(result.action).toBe('remote-cache-hit');
    expect(runner.calls.map((args) => args.slice(0, 2))).toEqual([
      ['versions', 'list'],
      ['deployments', 'status'],
    ]);
  });

  it('activates an existing tagged version that is not current', async () => {
    const root = await fixtureRoot();
    const runner = new FakeRunner([{ id: 'version-1', annotations: { 'workers/tag': `nx-${HASH}` } }], {
      versions: [{ version_id: 'version-2', percentage: 100 }],
    });

    const result = await deployStage(root, 'pr123', dependencies(runner, new FakeCloudflare()));

    expect(result.action).toBe('activated');
    expect(runner.calls.at(-1)?.slice(0, 3)).toEqual(['versions', 'deploy', '--version-tag']);
    expect(runner.configPathSeen).toBeDefined();
    expect(existsSync(requiredTestValue(runner.configPathSeen, 'config path'))).toBe(false);
  });

  it('uploads a missing tag with a temporary config and secure secrets file, then removes both', async () => {
    const root = await fixtureRoot();
    await writeFile(join(root, '.dev.vars.example'), 'FIXTURE_SECRET=""\nFIXTURE_TOKEN=""\n');
    const runner = new FakeRunner([], { versions: [{ version_id: 'version-2', percentage: 100 }] });

    const result = await deployStage(root, 'pr123', {
      ...dependencies(runner, new FakeCloudflare()),
      processEnv: {
        CLOUDFLARE_ACCOUNT_ID: 'account-1',
        CLOUDFLARE_API_TOKEN: 'token',
        NX_TASK_HASH: HASH,
        NX_TASK_TARGET_PROJECT: 'fixture',
        FIXTURE_SECRET: 'shared-secret',
        FIXTURE_TOKEN: 'encryption-secret',
      },
    });

    expect(result.action).toBe('deployed');
    expect(runner.calls.at(-1)?.[0]).toBe('deploy');
    expect(runner.calls.at(-1)).toContain('--tag');
    expect(runner.calls.at(-1)).toContain(`nx-${HASH}`);
    expect(runner.secretsMode).toBe(0o600);
    expect(JSON.parse(requiredTestValue(runner.secretsJson, 'secrets JSON'))).toEqual({
      FIXTURE_SECRET: 'shared-secret',
      FIXTURE_TOKEN: 'encryption-secret',
    });
    expect(existsSync(requiredTestValue(runner.configPathSeen, 'config path'))).toBe(false);
    expect(existsSync(requiredTestValue(runner.secretsPathSeen, 'secrets path'))).toBe(false);
  });

  it('passes only present manifest values for fixed stages and preserves absent remote secrets', async () => {
    const root = await fixtureRoot();
    await writeFile(join(root, '.dev.vars.example'), 'FIXTURE_SECRET=""\nFIXTURE_TOKEN=""\n');
    const runner = new FakeRunner([], {});
    const cloudflare = new FakeCloudflare();

    const result = await deployStage(root, 'staging', {
      runner,
      cloudflare,
      processEnv: {
        CLOUDFLARE_ACCOUNT_ID: 'account-1',
        CLOUDFLARE_API_TOKEN: 'token',
        FIXTURE_SECRET: 'shared-secret',
      },
    });

    expect(result.action).toBe('deployed');
    expect(JSON.parse(requiredTestValue(runner.secretsJson, 'secrets JSON'))).toEqual({
      FIXTURE_SECRET: 'shared-secret',
    });
    expect(existsSync(requiredTestValue(runner.secretsPathSeen, 'secrets path'))).toBe(false);
  });

  it('rejects a first PR Worker with missing manifest secrets before mutating Cloudflare', async () => {
    const root = await fixtureRoot();
    await writeFile(join(root, '.dev.vars.example'), 'FIXTURE_SECRET=""\n');
    const cloudflare = new FakeCloudflare();
    cloudflare.scripts = [];

    await expect(
      deployStage(root, 'pr123', {
        ...dependencies(new FakeRunner([], {}), cloudflare),
        processEnv: {
          CLOUDFLARE_ACCOUNT_ID: 'account-1',
          CLOUDFLARE_API_TOKEN: 'token',
          NX_TASK_HASH: HASH,
          NX_TASK_TARGET_PROJECT: 'fixture',
        },
      }),
    ).rejects.toThrow(/FIXTURE_SECRET/);
    expect(cloudflare.mutations).toEqual([]);
  });

  it('recovers when a parallel deployment creates the wildcard DNS record first', async () => {
    const root = await fixtureRoot(ROUTED_FIXTURE);
    const cloudflare = new FakeCloudflare();
    cloudflare.zones = [{ id: 'zone', name: 'conloca.com' }];
    let createAttempts = 0;
    cloudflare.createDnsRecord = async (zoneId, name, content) => {
      createAttempts += 1;
      cloudflare.records[zoneId] = [{ id: 'raced', name, type: 'CNAME', content, proxied: true }];
      throw new Error('record already exists');
    };

    const result = await deployStage(root, 'pr123', dependencies(new FakeRunner([], {}), cloudflare));

    expect(result.action).toBe('deployed');
    expect(createAttempts).toBe(1);
    expect(cloudflare.records.zone?.map((record) => record.name)).toEqual(['*.pr123.conloca.com']);
  });
});

describe('cleanup-pr exact stage matching', () => {
  it('rejects an invalid PR before touching the client', async () => {
    const cloudflare = new FakeCloudflare();
    let calls = 0;
    cloudflare.listWorkerDomains = async () => {
      calls += 1;
      return [];
    };

    await expect(cleanupPullRequest('/unused', 0, { cloudflare })).rejects.toThrow(/1 through 999999999/);
    expect(calls).toBe(0);
  });

  it('deletes only exact hyphen/dot-delimited pr123 resources and is idempotent for missing resources', async () => {
    const cloudflare = new FakeCloudflare();
    cloudflare.domains = [
      { id: 'domain-123', hostname: 'app.pr123.conloca.com' },
      { id: 'domain-1234', hostname: 'app.pr1234.conloca.com' },
    ];
    cloudflare.zones = [{ id: 'zone', name: 'conloca.com' }];
    cloudflare.routes.zone = [
      { id: 'route-123', pattern: '*.pr123.conloca.com/*' },
      { id: 'route-1234', pattern: '*.pr1234.conloca.com/*' },
    ];
    cloudflare.records.zone = [
      { id: 'dns-123', name: '*.pr123.conloca.com', type: 'CNAME', content: 'pr123.conloca.com' },
      { id: 'dns-staging', name: '*.staging.conloca.com', type: 'CNAME', content: 'staging.conloca.com' },
    ];
    cloudflare.scripts = [{ id: 'conloca-app-pr123' }, { id: 'conloca-app-pr1234' }, { id: 'conloca-app-staging' }];
    cloudflare.namespaces = [
      { id: 'kv-123', title: 'org-profiles-pr123' },
      { id: 'kv-1234', title: 'org-profiles-pr1234' },
    ];
    cloudflare.buckets = [{ name: 'conloca-media-pr123' }, { name: 'conloca-media-pr1234' }];
    cloudflare.objects['conloca-media-pr123'] = ['one', 'nested/two'];

    const result = await cleanupPullRequest('/unused', 123, { cloudflare });

    expect(result.deleted).toEqual({
      workers: 1,
      routes: 1,
      domains: 1,
      kvNamespaces: 1,
      r2Buckets: 1,
      r2Objects: 2,
      dnsRecords: 1,
    });
    expect(cloudflare.mutations.join('\n')).toContain('delete-worker:conloca-app-pr123');
    expect(cloudflare.mutations.join('\n')).not.toContain('pr1234');
    expect(cloudflare.mutations.join('\n')).not.toContain('staging');
  });
});

async function fixtureRoot(toml = FIXTURE): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-wrangler-test-'));
  roots.push(root);
  await writeFile(join(root, 'wrangler.toml'), toml);
  return root;
}

function dependencies(runner: ProcessRunner, cloudflare: CloudflareClient) {
  return {
    runner,
    cloudflare,
    processEnv: {
      CLOUDFLARE_ACCOUNT_ID: 'account-1',
      CLOUDFLARE_API_TOKEN: 'token',
      NX_TASK_HASH: HASH,
      NX_TASK_TARGET_PROJECT: 'fixture',
    },
  };
}

function success(value: unknown): ProcessResult {
  return { exitCode: 0, stdout: JSON.stringify(value), stderr: '' };
}
