import { afterEach, describe, expect, it } from 'bun:test';
import { existsSync, readFileSync, statSync } from 'node:fs';
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

  constructor(
    private readonly versions: unknown = [],
    private readonly deployment: unknown = {},
  ) {}

  async run(command: string, args: string[], options: ProcessRunOptions): Promise<ProcessResult> {
    this.calls.push({ command, args, cwd: options.cwd, env: childEnvironment(options) });
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

describe('deploy-stage remote version fallback', () => {
  it('returns a remote cache hit for the active tagged 100% version', async () => {
    const root = await fixtureRoot();
    const runner = new FakeRunner([{ id: 'version-1', annotations: { 'workers/tag': `nx-${HASH}` } }], {
      versions: [{ version_id: 'version-1', percentage: 100 }],
    });

    const result = await deployStage(root, { stage: 'pr123' }, dependencies(runner, new FakeCloudflare()));

    expect(result.action).toBe('remote-cache-hit');
    expect(runner.calls.map((call) => call.args.slice(0, 2))).toEqual([
      ['versions', 'list'],
      ['deployments', 'status'],
    ]);
  });

  it('activates an existing tagged version that is not current', async () => {
    const root = await fixtureRoot();
    const runner = new FakeRunner([{ id: 'version-1', annotations: { 'workers/tag': `nx-${HASH}` } }], {
      versions: [{ version_id: 'version-2', percentage: 100 }],
    });

    const result = await deployStage(root, { stage: 'pr123' }, dependencies(runner, new FakeCloudflare()));

    expect(result.action).toBe('activated');
    expect(runner.calls.at(-1)?.args.slice(0, 3)).toEqual(['versions', 'deploy', '--version-tag']);
    expect(runner.configPathSeen).toBeDefined();
    expect(existsSync(requiredTestValue(runner.configPathSeen, 'config path'))).toBe(false);
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
    expect(runner.calls.at(-1)?.args[0]).toBe('deploy');
    expect(runner.calls.at(-1)?.args).toContain('--tag');
    expect(runner.calls.at(-1)?.args).toContain(`nx-${HASH}`);
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
    vars: { EXAMPLE_SAAS_ENDPOINT: 'https://app.staging.example.test' },
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
      vars: { EXAMPLE_SAAS_ENDPOINT: 'https://app.pr7.example.test' },
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
