import { afterEach, describe, expect, it } from 'bun:test';
import { existsSync, mkdtempSync, readFileSync, statSync } from 'node:fs';
import { mkdir, mkdtemp, readdir, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type {
  CloudflareClient,
  CloudflareZone,
  D1DatabaseRecord,
  DnsRecord,
  R2Bucket,
  WorkerDomain,
  WorkerRoute,
  WorkerScript,
} from './cloudflare.js';
import {
  childEnvironment,
  cleanupPullRequest,
  deployStage,
  type ProcessResult,
  type ProcessRunner,
  type ProcessRunOptions,
  writeTemporaryConfigForTest,
} from './deploy-stage.js';
import {
  type FetchLike,
  findVersionIdByTag,
  LIVE_VERSION_CACHE_TTL_MS,
  readCachedLiveVersion,
} from './live-version.js';
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
pattern = "*.staging.example.test/*"
zone_name = "example.test"
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
  readonly calls: { command: string; args: string[]; cwd: string; env: NodeJS.ProcessEnv }[] = [];
  configPathSeen: string | undefined;
  secretsPathSeen: string | undefined;
  secretsMode: number | undefined;
  secretsJson: string | undefined;
  /** Runs before the fake reports success, so a test can read a temporary file the command was handed. */
  onCall: ((args: string[], cwd: string) => Promise<void>) | undefined;
  /**
   * How many `deployments status` reads still answer with the OLD version after a traffic shift.
   * This is the whole point of the fake: Cloudflare accepts the shift before it serves it.
   */
  propagationPolls = 0;

  private pendingVersionId: string | null = null;

  constructor(
    private versions: unknown = [],
    private deployment: unknown = {},
  ) {}

  /** The versions payload as a growable list, so an upload can add the version it just created. */
  private versionList(): unknown[] {
    const list: unknown[] = Array.isArray(this.versions) ? this.versions : [this.versions];
    this.versions = list;
    return list;
  }

  private shiftTrafficTo(versionId: string): void {
    this.pendingVersionId = versionId;
  }

  private readDeployment(): unknown {
    if (this.pendingVersionId !== null) {
      if (this.propagationPolls > 0) {
        this.propagationPolls -= 1;
      } else {
        this.deployment = { versions: [{ version_id: this.pendingVersionId, percentage: 100 }] };
        this.pendingVersionId = null;
      }
    }
    return this.deployment;
  }

  async run(command: string, args: string[], options: ProcessRunOptions): Promise<ProcessResult> {
    this.calls.push({ command, args, cwd: options.cwd, env: childEnvironment(options) });
    if (args[0] === 'versions' && args[1] === 'list') {
      return success(this.versions);
    }
    if (args[0] === 'deployments' && args[1] === 'status') {
      return success(this.readDeployment());
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
    if (args[0] === 'versions' && args[1] === 'deploy') {
      // Resolved with the same reader the deploy uses, so the fake cannot agree with a lookup
      // production would miss.
      const tag = args[args.indexOf('--version-tag') + 1];
      const existing = tag ? findVersionIdByTag(this.versions, tag) : null;
      if (existing) this.shiftTrafficTo(existing);
    }
    if (args[0] === 'deploy') {
      const tagIndex = args.indexOf('--tag');
      if (tagIndex >= 0) {
        const versionId = `uploaded-${args[tagIndex + 1]}`;
        this.versionList().push({ id: versionId, annotations: { 'workers/tag': args[tagIndex + 1] } });
        this.shiftTrafficTo(versionId);
      }
    }
    await this.onCall?.(args, options.cwd);
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
  d1Databases: D1DatabaseRecord[] = [];
  mutations: string[] = [];

  async listKvNamespaces(): Promise<LiveKvNamespace[]> {
    return this.namespaces;
  }
  async createKvNamespace(title: string): Promise<LiveKvNamespace> {
    this.mutations.push(`create-kv:${title}`);
    const namespace = { id: `kv-${title}`, title };
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
  async listD1Databases(): Promise<D1DatabaseRecord[]> {
    return this.d1Databases;
  }
  async createD1Database(name: string): Promise<D1DatabaseRecord> {
    const record = { uuid: `d1-${name}`, name };
    this.d1Databases.push(record);
    this.mutations.push(`create-d1:${name}`);
    return record;
  }
  async deleteD1Database(uuid: string): Promise<void> {
    this.mutations.push(`delete-d1:${uuid}`);
  }
}

describe('deploy-stage against live state', () => {
  it('uploads nothing and shifts no traffic when the live version already is the task hash', async () => {
    const root = await fixtureRoot();
    const runner = new FakeRunner([{ id: 'version-1', annotations: { 'workers/tag': `nx-${HASH}` } }], {
      versions: [{ version_id: 'version-1', percentage: 100 }],
    });

    const result = await deployStage(root, { stage: 'pr123' }, dependencies(runner, new FakeCloudflare()));

    expect(result.action).toBe('remote-cache-hit');
    // Two reads and nothing else. This is what makes a redundant deploy — the one a cross-project
    // `dependsOn` edge adds — provably free instead of assumed free.
    expect(runner.calls.map((call) => call.args.slice(0, 2))).toEqual([
      ['versions', 'list'],
      ['deployments', 'status'],
    ]);
  });

  it('redeploys after a rollback, when the live version is no longer the task hash', async () => {
    const root = await fixtureRoot();
    // The build for this hash is still uploaded; someone rolled traffic back to the older version.
    const runner = new FakeRunner([{ id: 'version-1', annotations: { 'workers/tag': `nx-${HASH}` } }], {
      versions: [{ version_id: 'version-2', percentage: 100 }],
    });

    const result = await deployStage(root, { stage: 'pr123' }, dependencies(runner, new FakeCloudflare()));

    expect(result.action).toBe('activated');
    const shiftIndex = runner.calls.findIndex((call) => call.args[0] === 'versions' && call.args[1] === 'deploy');
    expect(runner.calls[shiftIndex]?.args.slice(0, 4)).toEqual(['versions', 'deploy', '--version-tag', `nx-${HASH}`]);
    // And the step does not return until it has read live state again after that shift.
    expect(runner.calls.slice(shiftIndex + 1).some((call) => call.args[0] === 'deployments')).toBe(true);
    expect(existsSync(requiredTestValue(runner.configPathSeen, 'config path'))).toBe(false);
  });

  it('waits out propagation before reporting the deploy done', async () => {
    const root = await fixtureRoot();
    const runner = new FakeRunner([{ id: 'version-1', annotations: { 'workers/tag': `nx-${HASH}` } }], {
      versions: [{ version_id: 'version-2', percentage: 100 }],
    });
    runner.propagationPolls = 3;

    const result = await deployStage(root, { stage: 'pr123' }, dependencies(runner, new FakeCloudflare()));

    expect(result.action).toBe('activated');
    const statusReads = runner.calls.filter((call) => call.args[0] === 'deployments').length;
    expect(statusReads).toBe(5);
  });

  it('fails with what it expected and what it saw when the version never becomes live', async () => {
    const root = await fixtureRoot();
    const runner = new FakeRunner([{ id: 'version-1', annotations: { 'workers/tag': `nx-${HASH}` } }], {
      versions: [{ version_id: 'version-2', percentage: 100 }],
    });
    runner.propagationPolls = Number.POSITIVE_INFINITY;

    await expect(deployStage(root, { stage: 'pr123' }, dependencies(runner, new FakeCloudflare()))).rejects.toThrow(
      `fixture-worker-pr123: deployed version nx-${HASH} was not serving traffic after 10s; live is an untagged version (version-2)`,
    );
  });

  it('leaves the cache holding the tag it made live, so the next query needs no API call', async () => {
    const root = await fixtureRoot();
    const cacheDirectory = await mkdtemp(join(tmpdir(), 'smoo-live-version-'));
    roots.push(cacheDirectory);
    const runner = new FakeRunner([{ id: 'version-1', annotations: { 'workers/tag': `nx-${HASH}` } }], {
      versions: [{ version_id: 'version-2', percentage: 100 }],
    });

    await deployStage(
      root,
      { stage: 'pr123' },
      { ...dependencies(runner, new FakeCloudflare()), liveVersionCacheDirectory: cacheDirectory },
    );

    const cached = await readCachedLiveVersion(
      cacheDirectory,
      { accountId: 'account-1', workerName: 'fixture-worker-pr123', stage: 'pr123' },
      LIVE_VERSION_CACHE_TTL_MS,
    );
    expect(cached).toMatchObject({ versionTag: `nx-${HASH}`, versionId: 'version-1' });
  });

  it('is not done until a declared version endpoint answers with the new tag', async () => {
    const root = await fixtureRoot();
    const runner = new FakeRunner([{ id: 'version-1', annotations: { 'workers/tag': `nx-${HASH}` } }], {
      versions: [{ version_id: 'version-2', percentage: 100 }],
    });
    const bodies = [`nx-${'0'.repeat(20)}`, `nx-${'0'.repeat(20)}`, `nx-${HASH}`];
    const requested: string[] = [];
    const stubFetch: FetchLike = async (input) => {
      requested.push(String(input));
      return new Response(bodies.shift() ?? `nx-${HASH}`);
    };

    const result = await deployStage(
      root,
      { stage: 'pr123', versionEndpoint: 'https://fixture.example.test/__version' },
      {
        ...dependencies(runner, new FakeCloudflare()),
        wait: { ...fakeWait(), fetch: stubFetch },
      },
    );

    expect(result.action).toBe('activated');
    expect(requested).toEqual([
      'https://fixture.example.test/__version',
      'https://fixture.example.test/__version',
      'https://fixture.example.test/__version',
    ]);
  });

  it('fails naming the endpoint answer when the edge never reports the new tag', async () => {
    const root = await fixtureRoot();
    const runner = new FakeRunner([{ id: 'version-1', annotations: { 'workers/tag': `nx-${HASH}` } }], {
      versions: [{ version_id: 'version-2', percentage: 100 }],
    });
    const stubFetch: FetchLike = async () => new Response('nx-previous');

    await expect(
      deployStage(
        root,
        { stage: 'pr123', versionEndpoint: 'https://fixture.example.test/__version' },
        { ...dependencies(runner, new FakeCloudflare()), wait: { ...fakeWait(), fetch: stubFetch } },
      ),
    ).rejects.toThrow(
      `fixture-worker-pr123: https://fixture.example.test/__version did not report version nx-${HASH} after 10s; it reported nx-previous`,
    );
  });

  it('uploads a missing tag with a temporary config and secure secrets file, then removes both', async () => {
    const root = await fixtureRoot();
    await writeFile(join(root, '.dev.vars.example'), 'FIXTURE_SECRET=""\nFIXTURE_TOKEN=""\n');
    const runner = new FakeRunner([], { versions: [{ version_id: 'version-2', percentage: 100 }] });

    const result = await deployStage(
      root,
      { stage: 'pr123' },
      {
        ...dependencies(runner, new FakeCloudflare()),
        processEnv: {
          CLOUDFLARE_ACCOUNT_ID: 'account-1',
          CLOUDFLARE_API_TOKEN: 'token',
          NX_TASK_HASH: HASH,
          NX_TASK_TARGET_PROJECT: 'fixture',
          FIXTURE_SECRET: 'shared-secret',
          FIXTURE_TOKEN: 'encryption-secret',
        },
      },
    );

    expect(result.action).toBe('deployed');
    const upload = requiredTestValue(
      runner.calls.find((call) => call.args[0] === 'deploy'),
      'upload call',
    );
    expect(upload.args).toContain('--tag');
    expect(upload.args).toContain(`nx-${HASH}`);
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

    const result = await deployStage(
      root,
      { stage: 'staging' },
      {
        runner,
        cloudflare,
        processEnv: {
          CLOUDFLARE_ACCOUNT_ID: 'account-1',
          CLOUDFLARE_API_TOKEN: 'token',
          FIXTURE_SECRET: 'shared-secret',
        },
      },
    );

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
      deployStage(
        root,
        { stage: 'pr123' },
        {
          ...dependencies(new FakeRunner([], {}), cloudflare),
          processEnv: {
            CLOUDFLARE_ACCOUNT_ID: 'account-1',
            CLOUDFLARE_API_TOKEN: 'token',
            NX_TASK_HASH: HASH,
            NX_TASK_TARGET_PROJECT: 'fixture',
          },
        },
      ),
    ).rejects.toThrow(/FIXTURE_SECRET/);
    expect(cloudflare.mutations).toEqual([]);
  });

  it('recovers when a parallel deployment creates the wildcard DNS record first', async () => {
    const root = await fixtureRoot(ROUTED_FIXTURE);
    const cloudflare = new FakeCloudflare();
    cloudflare.zones = [{ id: 'zone', name: 'example.test' }];
    let createAttempts = 0;
    cloudflare.createDnsRecord = async (zoneId, name, content) => {
      createAttempts += 1;
      cloudflare.records[zoneId] = [{ id: 'raced', name, type: 'CNAME', content, proxied: true }];
      throw new Error('record already exists');
    };

    const result = await deployStage(root, { stage: 'pr123' }, dependencies(new FakeRunner([], {}), cloudflare));

    expect(result.action).toBe('deployed');
    expect(createAttempts).toBe(1);
    expect(cloudflare.records.zone?.map((record) => record.name)).toEqual(['*.pr123.example.test']);
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
      { id: 'domain-123', hostname: 'app.pr123.example.test' },
      { id: 'domain-1234', hostname: 'app.pr1234.example.test' },
    ];
    cloudflare.zones = [{ id: 'zone', name: 'example.test' }];
    cloudflare.routes.zone = [
      { id: 'route-123', pattern: '*.pr123.example.test/*' },
      { id: 'route-1234', pattern: '*.pr1234.example.test/*' },
    ];
    cloudflare.records.zone = [
      { id: 'dns-123', name: '*.pr123.example.test', type: 'CNAME', content: 'pr123.example.test' },
      { id: 'dns-staging', name: '*.staging.example.test', type: 'CNAME', content: 'staging.example.test' },
    ];
    cloudflare.scripts = [{ id: 'app-pr123' }, { id: 'app-pr1234' }, { id: 'app-staging' }];
    cloudflare.namespaces = [
      { id: 'kv-123', title: 'org-profiles-pr123' },
      { id: 'kv-1234', title: 'org-profiles-pr1234' },
    ];
    cloudflare.buckets = [{ name: 'app-media-pr123' }, { name: 'app-media-pr1234' }];
    cloudflare.objects['app-media-pr123'] = ['one', 'nested/two'];

    const result = await cleanupPullRequest('/unused', 123, { cloudflare });

    expect(result.deleted).toEqual({
      workers: 1,
      routes: 1,
      domains: 1,
      kvNamespaces: 1,
      r2Buckets: 1,
      r2Objects: 2,
      dnsRecords: 1,
      d1Databases: 0,
    });
    expect(cloudflare.mutations.join('\n')).toContain('delete-worker:app-pr123');
    expect(cloudflare.mutations.join('\n')).not.toContain('pr1234');
    expect(cloudflare.mutations.join('\n')).not.toContain('staging');
  });

  it('deletes D1 databases carrying the stage segment and reports them', async () => {
    const cloudflare = new FakeCloudflare();
    cloudflare.d1Databases = [
      { uuid: 'd1-keep', name: 'site-staging-db' },
      { uuid: 'd1-gone', name: 'site-pr7-db' },
      { uuid: 'd1-other', name: 'site-pr70-db' },
    ];

    const result = await cleanupPullRequest('/unused', 7, { cloudflare });

    expect(cloudflare.mutations).toContain('delete-d1:d1-gone');
    expect(cloudflare.mutations).not.toContain('delete-d1:d1-keep');
    expect(cloudflare.mutations).not.toContain('delete-d1:d1-other');
    expect(result.deleted.d1Databases).toBe(1);
  });

  it('lists D1 before deleting anything so a listing failure cannot partial-clean', async () => {
    const cloudflare = new FakeCloudflare();
    cloudflare.scripts = [{ id: 'app-pr7' }];
    cloudflare.listD1Databases = async () => {
      throw new Error('D1 listing forbidden');
    };

    await expect(cleanupPullRequest('/unused', 7, { cloudflare })).rejects.toThrow(/D1 listing forbidden/);
    expect(cloudflare.mutations).toEqual([]);
  });
});

// What the Cloudflare Astro adapter writes under the build output: one flat
// document, already resolved for staging, with its paths relative to itself.
const FLAT_FIXTURE = JSON.stringify(
  {
    name: 'fixture-website-preview-staging',
    main: 'entry.mjs',
    targetEnvironment: 'staging',
    assets: { binding: 'ASSETS', directory: '../client' },
    routes: [
      { pattern: 'next.example.com', custom_domain: true },
      { pattern: 'site.staging.example.test/*', zone_name: 'example.test' },
    ],
    vars: { SITE_URL: 'https://app.staging.example.test' },
    kv_namespaces: [{ binding: 'SESSION', id: 'kv-staging' }],
    d1_databases: [
      {
        binding: 'DB',
        database_name: 'fixture-website-staging-db',
        database_id: 'd1-staging',
        migrations_dir: '../../migrations',
      },
    ],
    services: [{ binding: 'BACKEND', service: 'fixture-backend-staging' }],
  },
  null,
  2,
);

// A production build output: it carries its own name, with no -staging suffix to derive from.
const FLAT_PRODUCTION_FIXTURE = JSON.stringify(
  {
    name: 'fixture-website',
    main: 'entry.mjs',
    assets: { binding: 'ASSETS', directory: '../client' },
    routes: [{ pattern: 'www.example.test/*', zone_name: 'example.test' }],
    d1_databases: [
      {
        binding: 'DB',
        database_name: 'fixture-website-db',
        database_id: 'd1-production',
        migrations_dir: '../../migrations',
      },
    ],
  },
  null,
  2,
);

async function flatFixtureRoot(fixture = FLAT_FIXTURE): Promise<{ root: string; configPath: string }> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-wrangler-flat-'));
  roots.push(root);
  const serverDir = join(root, '.out', 'server');
  await mkdir(serverDir, { recursive: true });
  const configPath = join(serverDir, 'wrangler.json');
  await writeFile(configPath, fixture);
  return { root, configPath };
}

describe('deployStage with a flat JSON config', () => {
  it('deploys staging as-is without an --env flag and applies D1 migrations', async () => {
    const { root, configPath } = await flatFixtureRoot();
    const runner = new FakeRunner();
    const cloudflare = new FakeCloudflare();
    cloudflare.namespaces = [{ id: 'kv-staging', title: 'fixture-SESSION-staging' }];
    cloudflare.d1Databases = [{ uuid: 'd1-staging', name: 'fixture-website-staging-db' }];
    cloudflare.zones = [{ id: 'zone-1', name: 'example.test' }];

    // The build that produced the config runs with CLOUDFLARE_ENV set, so the deploy process inherits it.
    const result = await withProcessEnv({ CLOUDFLARE_ENV: 'staging', CLOUDFLARE_ACCOUNT_ID: 'account-1' }, () =>
      deployStage(root, { stage: 'staging', config: configPath }, dependencies(runner, cloudflare)),
    );

    expect(result).toEqual({
      stage: 'staging',
      workerName: 'fixture-website-preview-staging',
      action: 'deployed',
      versionTag: `nx-${HASH}`,
    });
    const migrate = runner.calls.find((call) => call.args[0] === 'd1');
    expect(migrate?.args).toEqual(['d1', 'migrations', 'apply', 'DB', '--remote', '--config', configPath]);
    const deploy = runner.calls.find((call) => call.args[0] === 'deploy');
    expect(deploy?.args).toEqual(['deploy', '--config', configPath, '--tag', `nx-${HASH}`]);
    // Without --env, wrangler would fall back to CLOUDFLARE_ENV and rename the worker after it; the
    // rest of the inherited environment, the account id included, still reaches wrangler.
    for (const call of runner.calls) {
      expect(call.env.CLOUDFLARE_ENV).toBeUndefined();
      expect(call.env.CLOUDFLARE_ACCOUNT_ID).toBe('account-1');
      expect(call.env.PATH).toBe(process.env.PATH);
    }
  });

  it('leaves a D1 binding without a migrations directory unmigrated', async () => {
    const { root, configPath } = await flatFixtureRoot(
      JSON.stringify({
        ...JSON.parse(FLAT_FIXTURE),
        d1_databases: [{ binding: 'DB', database_name: 'fixture-website-staging-db', database_id: 'd1-staging' }],
      }),
    );
    const runner = new FakeRunner();
    const cloudflare = new FakeCloudflare();
    cloudflare.namespaces = [{ id: 'kv-staging', title: 'fixture-SESSION-staging' }];
    cloudflare.d1Databases = [{ uuid: 'd1-staging', name: 'fixture-website-staging-db' }];
    cloudflare.zones = [{ id: 'zone-1', name: 'example.test' }];

    const result = await deployStage(root, { stage: 'staging', config: configPath }, dependencies(runner, cloudflare));

    expect(result.action).toBe('deployed');
    expect(runner.calls.map((call) => call.args[0])).not.toContain('d1');
  });

  it('derives a pull-request stage beside the original, creating KV and D1 and rewriting the service', async () => {
    const { root, configPath } = await flatFixtureRoot();
    const runner = new FakeRunner();
    const cloudflare = new FakeCloudflare();
    cloudflare.namespaces = [{ id: 'kv-staging', title: 'fixture-SESSION-staging' }];
    cloudflare.d1Databases = [{ uuid: 'd1-staging', name: 'fixture-website-staging-db' }];
    cloudflare.zones = [{ id: 'zone-1', name: 'example.test' }];
    let derivedConfig: Record<string, unknown> | undefined;
    runner.onCall = async (args) => {
      if (args[0] !== 'deploy') return;
      const index = args.indexOf('--config');
      derivedConfig = JSON.parse(await readFile(args[index + 1] ?? '', 'utf8'));
    };

    const result = await deployStage(root, { stage: 'pr7', config: configPath }, dependencies(runner, cloudflare));

    expect(result.workerName).toBe('fixture-website-preview-pr7');
    expect(cloudflare.mutations).toContain('create-kv:fixture-SESSION-pr7');
    expect(cloudflare.mutations).toContain('create-d1:fixture-website-pr7-db');
    expect(derivedConfig).toMatchObject({
      name: 'fixture-website-preview-pr7',
      routes: [{ pattern: 'site.pr7.example.test/*', zone_name: 'example.test' }],
      vars: { SITE_URL: 'https://app.pr7.example.test' },
      kv_namespaces: [{ binding: 'SESSION', id: 'kv-fixture-SESSION-pr7' }],
      d1_databases: [
        { binding: 'DB', database_name: 'fixture-website-pr7-db', database_id: 'd1-fixture-website-pr7-db' },
      ],
      services: [{ binding: 'BACKEND', service: 'fixture-backend-pr7' }],
    });
    const deploy = runner.calls.find((call) => call.args[0] === 'deploy');
    expect(deploy?.args).not.toContain('--env');
    expect(deploy?.args[2]?.startsWith(join(root, '.out', 'server', '.wrangler.smoo-'))).toBe(true);
    // The migrations must run against the derived config, not the staging template beside it.
    const migrate = runner.calls.find((call) => call.args[0] === 'd1');
    expect(migrate?.args.slice(0, 5)).toEqual(['d1', 'migrations', 'apply', 'DB', '--remote']);
    expect(migrate?.args.at(-1)).toBe(deploy?.args[2]);
    // The derived file is removed afterwards.
    const leftovers = (await readdir(join(root, '.out', 'server'))).filter((name) =>
      name.startsWith('.wrangler.smoo-'),
    );
    expect(leftovers).toEqual([]);
  });

  it('deploys a production config as-is, with no derivation and no --env flag', async () => {
    const { root, configPath } = await flatFixtureRoot(FLAT_PRODUCTION_FIXTURE);
    const runner = new FakeRunner();
    const cloudflare = new FakeCloudflare();
    cloudflare.zones = [{ id: 'zone-1', name: 'example.test' }];

    const result = await deployStage(
      root,
      { stage: 'production', config: configPath },
      dependencies(runner, cloudflare),
    );

    expect(result).toEqual({
      stage: 'production',
      workerName: 'fixture-website',
      action: 'deployed',
      versionTag: `nx-${HASH}`,
    });
    const migrate = runner.calls.find((call) => call.args[0] === 'd1');
    expect(migrate?.args).toEqual(['d1', 'migrations', 'apply', 'DB', '--remote', '--config', configPath]);
    const deploy = runner.calls.find((call) => call.args[0] === 'deploy');
    expect(deploy?.args).toEqual(['deploy', '--config', configPath, '--tag', `nx-${HASH}`]);
    const leftovers = (await readdir(join(root, '.out', 'server'))).filter((name) =>
      name.startsWith('.wrangler.smoo-'),
    );
    expect(leftovers).toEqual([]);
  });

  it('skips the D1 migrations when the tagged build is already the live deployment', async () => {
    const { root, configPath } = await flatFixtureRoot();
    const runner = new FakeRunner([{ id: 'version-1', annotations: { 'workers/tag': `nx-${HASH}` } }], {
      versions: [{ version_id: 'version-1', percentage: 100 }],
    });
    const cloudflare = new FakeCloudflare();
    cloudflare.namespaces = [{ id: 'kv-staging', title: 'fixture-SESSION-staging' }];
    cloudflare.d1Databases = [{ uuid: 'd1-staging', name: 'fixture-website-staging-db' }];
    cloudflare.zones = [{ id: 'zone-1', name: 'example.test' }];
    cloudflare.scripts = [{ id: 'fixture-website-preview-staging' }];
    cloudflare.domains = [{ id: 'domain-1', hostname: 'next.example.com', service: 'fixture-website-preview-staging' }];

    const result = await deployStage(root, { stage: 'staging', config: configPath }, dependencies(runner, cloudflare));

    expect(result.action).toBe('remote-cache-hit');
    expect(runner.calls.map((call) => call.args.slice(0, 2))).toEqual([
      ['versions', 'list'],
      ['deployments', 'status'],
    ]);
  });

  it('applies D1 migrations before activating a tagged version that is not current', async () => {
    const { root, configPath } = await flatFixtureRoot();
    const runner = new FakeRunner([{ id: 'version-1', annotations: { 'workers/tag': `nx-${HASH}` } }], {
      versions: [{ version_id: 'version-2', percentage: 100 }],
    });
    const cloudflare = new FakeCloudflare();
    cloudflare.namespaces = [{ id: 'kv-staging', title: 'fixture-SESSION-staging' }];
    cloudflare.d1Databases = [{ uuid: 'd1-staging', name: 'fixture-website-staging-db' }];
    cloudflare.scripts = [{ id: 'fixture-website-preview-staging' }];
    cloudflare.domains = [{ id: 'domain-1', hostname: 'next.example.com', service: 'fixture-website-preview-staging' }];

    const result = await deployStage(root, { stage: 'staging', config: configPath }, dependencies(runner, cloudflare));

    expect(result.action).toBe('activated');
    expect(runner.calls.map((call) => call.args.slice(0, 2))).toEqual([
      ['versions', 'list'],
      ['deployments', 'status'],
      ['d1', 'migrations'],
      ['versions', 'deploy'],
      // The traffic shift is not the end of the deploy: the step re-reads live state and only
      // returns once the tag it activated is the one being served.
      ['deployments', 'status'],
      ['versions', 'list'],
    ]);
  });

  it('removes the derived config when wrangler fails after writing it', async () => {
    const { root, configPath } = await flatFixtureRoot();
    const runner = new FakeRunner();
    runner.onCall = async () => {
      throw new Error('wrangler exploded');
    };
    const cloudflare = new FakeCloudflare();
    cloudflare.namespaces = [{ id: 'kv-staging', title: 'fixture-SESSION-staging' }];
    cloudflare.d1Databases = [{ uuid: 'd1-staging', name: 'fixture-website-staging-db' }];

    await expect(
      deployStage(root, { stage: 'pr7', config: configPath }, dependencies(runner, cloudflare)),
    ).rejects.toThrow(/wrangler exploded/);
    const leftovers = (await readdir(join(root, '.out', 'server'))).filter((name) =>
      name.startsWith('.wrangler.smoo-'),
    );
    expect(leftovers).toEqual([]);
  });

  it('refuses an all-pinned template before mutating Cloudflare', async () => {
    const { root, configPath } = await flatFixtureRoot(
      JSON.stringify({
        ...JSON.parse(FLAT_FIXTURE),
        routes: [{ pattern: 'next.example.com', custom_domain: true }],
      }),
    );
    const cloudflare = new FakeCloudflare();
    cloudflare.namespaces = [{ id: 'kv-staging', title: 'fixture-SESSION-staging' }];

    await expect(
      deployStage(root, { stage: 'pr7', config: configPath }, dependencies(new FakeRunner(), cloudflare)),
    ).rejects.toThrow(/pinned/);
    expect(cloudflare.mutations).toEqual([]);
  });

  it('refuses an R2 bucket the stage would share with staging, before mutating Cloudflare', async () => {
    const { root, configPath } = await flatFixtureRoot(
      JSON.stringify({
        ...JSON.parse(FLAT_FIXTURE),
        r2_buckets: [{ binding: 'MEDIA', bucket_name: 'shared-media' }],
      }),
    );
    const cloudflare = new FakeCloudflare();
    cloudflare.namespaces = [{ id: 'kv-staging', title: 'fixture-SESSION-staging' }];

    await expect(
      deployStage(root, { stage: 'pr7', config: configPath }, dependencies(new FakeRunner(), cloudflare)),
    ).rejects.toThrow(/no exact staging segment/);
    expect(cloudflare.mutations).toEqual([]);
  });

  it('rejects a first flat PR Worker with missing manifest secrets before mutating Cloudflare', async () => {
    const { root, configPath } = await flatFixtureRoot();
    // The manifest is read from the working directory, not from beside the --config file.
    await writeFile(join(root, '.dev.vars.example'), 'FIXTURE_SECRET=""\n');
    const cloudflare = new FakeCloudflare();
    cloudflare.namespaces = [{ id: 'kv-staging', title: 'fixture-SESSION-staging' }];
    cloudflare.scripts = [];

    await expect(
      deployStage(root, { stage: 'pr7', config: configPath }, dependencies(new FakeRunner(), cloudflare)),
    ).rejects.toThrow(/requires process environment values/);
    expect(cloudflare.mutations).toEqual([]);
  });

  it('refuses a config with env blocks', async () => {
    const { root, configPath } = await flatFixtureRoot();
    await writeFile(configPath, JSON.stringify({ name: 'x-staging', env: { staging: {} } }));

    await expect(
      deployStage(root, { stage: 'staging', config: configPath }, dependencies(new FakeRunner(), new FakeCloudflare())),
    ).rejects.toThrow(/env blocks/);
  });

  it('names the config file when it is not JSON', async () => {
    const { root, configPath } = await flatFixtureRoot('{');

    await expect(
      deployStage(root, { stage: 'staging', config: configPath }, dependencies(new FakeRunner(), new FakeCloudflare())),
    ).rejects.toThrow(`${configPath} is not valid JSON: `);
  });
});

describe('writeTemporaryConfig', () => {
  it('removes the file it created when the write itself fails', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-wrangler-write-'));
    roots.push(root);
    const path = join(root, '.wrangler.smoo-blocked.json');
    // A path that exists but cannot be written to: the write fails with the file already on disk.
    await writeFile(path, 'stale', { mode: 0o400 });

    await expect(writeTemporaryConfigForTest(path, '{}\n')).rejects.toThrow();

    expect(await readdir(root)).toEqual([]);
  });
});

/** Runs `action` with the variables set on this process, restoring the previous values afterwards. */
async function withProcessEnv<T>(variables: Record<string, string>, action: () => Promise<T>): Promise<T> {
  const previous = Object.fromEntries(Object.keys(variables).map((name) => [name, process.env[name]]));
  Object.assign(process.env, variables);
  try {
    return await action();
  } finally {
    for (const [name, value] of Object.entries(previous)) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
}

async function fixtureRoot(toml = FIXTURE): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-wrangler-test-'));
  roots.push(root);
  await writeFile(join(root, 'wrangler.toml'), toml);
  return root;
}

/**
 * A wait whose clock only advances when the code under test sleeps: real bounds, no real seconds,
 * and a deterministic poll count a test can assert on.
 */
function fakeWait(): { budgetMs: number; intervalMs: number; now: () => number; sleep: (ms: number) => Promise<void> } {
  let clock = 0;
  return {
    budgetMs: 10_000,
    intervalMs: 1_000,
    now: () => clock,
    sleep: async (ms: number) => {
      clock += ms;
    },
  };
}

function dependencies(runner: ProcessRunner, cloudflare: CloudflareClient) {
  const cacheDirectory = mkdtempSync(join(tmpdir(), 'smoo-live-version-'));
  roots.push(cacheDirectory);
  return {
    runner,
    cloudflare,
    wait: fakeWait(),
    liveVersionCacheDirectory: cacheDirectory,
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
