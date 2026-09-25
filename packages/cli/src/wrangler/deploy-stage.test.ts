import { afterEach, describe, expect, it } from 'bun:test';
import { existsSync, mkdtempSync, readFileSync, statSync } from 'node:fs';
import { mkdir, mkdtemp, readdir, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  CloudflareApiError,
  type CloudflareClient,
  CloudflareRestClient,
  type CloudflareZone,
  type D1DatabaseRecord,
  type DnsRecord,
  type R2Bucket,
  type WorkerDomain,
  type WorkerRoute,
  type WorkerScript,
} from './cloudflare.js';
import {
  childEnvironment,
  cleanupPullRequest,
  deployStage,
  describeCleanup,
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
import { STAGE_RECORDS_BUCKET, type StageRecord, stageRecordKey } from './stage-records.js';

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

/** The record keys of `fixture-worker-pr123`, as the fake logs their writes. */
const RECORDED_PR123 = 'put-record:v1/github.com%2Facme%2Fapp/pr123/fixture-worker-pr123';

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
  /** Secret names each Worker already holds, by Worker name. */
  secrets: Record<string, string[]> = {};
  /** Every Worker whose secrets were queried, in order, so a test can prove one was never asked. */
  secretQueries: string[] = [];
  domains: WorkerDomain[] = [];
  zones: CloudflareZone[] = [];
  routes: Record<string, WorkerRoute[]> = {};
  records: Record<string, DnsRecord[]> = {};
  objects: Record<string, string[]> = {};
  d1Databases: D1DatabaseRecord[] = [];
  /** Every listing asked for, with its argument, so a test can prove what was never read. */
  reads: string[] = [];
  mutations: string[] = [];

  async listKvNamespaces(): Promise<LiveKvNamespace[]> {
    this.reads.push('kv');
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
    this.namespaces = this.namespaces.filter((namespace) => namespace.id !== id);
  }
  async listR2Buckets(): Promise<R2Bucket[]> {
    this.reads.push('r2');
    return this.buckets;
  }
  async createR2Bucket(name: string): Promise<void> {
    this.mutations.push(`create-r2:${name}`);
    this.buckets.push({ name });
  }
  async listR2Objects(bucket: string, prefix = ''): Promise<string[]> {
    this.reads.push(`objects:${bucket}:${prefix}`);
    return (this.objects[bucket] ?? []).filter((key) => key.startsWith(prefix));
  }
  async putR2Object(bucket: string, key: string): Promise<void> {
    this.mutations.push(`put-record:${key}`);
    const keys = this.objects[bucket] ?? [];
    if (!keys.includes(key)) keys.push(key);
    this.objects[bucket] = keys;
  }
  async deleteR2Object(bucket: string, key: string): Promise<void> {
    this.mutations.push(`delete-object:${bucket}:${key}`);
    this.objects[bucket] = (this.objects[bucket] ?? []).filter((candidate) => candidate !== key);
  }
  async deleteR2Bucket(name: string): Promise<void> {
    this.mutations.push(`delete-r2:${name}`);
    this.buckets = this.buckets.filter((bucket) => bucket.name !== name);
  }
  async listWorkerScripts(): Promise<WorkerScript[]> {
    this.reads.push('workers');
    return this.scripts;
  }
  async listWorkerSecrets(workerName: string): Promise<string[]> {
    this.secretQueries.push(workerName);
    // Cloudflare answers 404 for a Worker that is not there. Wrangler used to hand that back as an
    // empty list; it no longer does, and code that reads the failure as "no secrets" inverts the
    // one check a first deployment depends on.
    if (!this.scripts.some((script) => script.id === workerName)) {
      throw new CloudflareApiError(
        `Cloudflare API /workers/scripts/${workerName}/secrets failed: not found`,
        404,
        [10007],
      );
    }
    return this.secrets[workerName] ?? [];
  }
  async deleteWorkerScript(name: string): Promise<void> {
    this.mutations.push(`delete-worker:${name}`);
    this.scripts = this.scripts.filter((script) => script.id !== name);
  }
  async listWorkerDomains(): Promise<WorkerDomain[]> {
    this.reads.push('domains');
    return this.domains;
  }
  async createWorkerDomain(hostname: string, workerName: string): Promise<void> {
    this.mutations.push(`create-domain:${hostname}:${workerName}`);
  }
  async deleteWorkerDomain(id: string): Promise<void> {
    this.mutations.push(`delete-domain:${id}`);
    this.domains = this.domains.filter((domain) => domain.id !== id);
  }
  async listZones(name?: string): Promise<CloudflareZone[]> {
    this.reads.push(name === undefined ? 'zones' : `zones:${name}`);
    return name === undefined ? this.zones : this.zones.filter((zone) => zone.name === name);
  }
  async listWorkerRoutes(zoneId: string): Promise<WorkerRoute[]> {
    this.reads.push(`routes:${zoneId}`);
    return this.routes[zoneId] ?? [];
  }
  async createWorkerRoute(zoneId: string, pattern: string, workerName: string): Promise<void> {
    this.mutations.push(`create-route:${zoneId}:${pattern}:${workerName}`);
  }
  async deleteWorkerRoute(zoneId: string, routeId: string): Promise<void> {
    this.mutations.push(`delete-route:${zoneId}:${routeId}`);
    this.routes[zoneId] = (this.routes[zoneId] ?? []).filter((route) => route.id !== routeId);
  }
  async listDnsRecords(zoneId: string): Promise<DnsRecord[]> {
    this.reads.push(`dns:${zoneId}`);
    return this.records[zoneId] ?? [];
  }
  async createDnsRecord(zoneId: string, name: string, content: string): Promise<void> {
    this.mutations.push(`create-dns:${zoneId}:${name}:${content}`);
  }
  async deleteDnsRecord(zoneId: string, recordId: string): Promise<void> {
    this.mutations.push(`delete-dns:${zoneId}:${recordId}`);
    this.records[zoneId] = (this.records[zoneId] ?? []).filter((record) => record.id !== recordId);
  }
  async listD1Databases(): Promise<D1DatabaseRecord[]> {
    this.reads.push('d1');
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
    this.d1Databases = this.d1Databases.filter((database) => database.uuid !== uuid);
  }
}

describe('deploy-stage against live state', () => {
  it('uploads nothing and shifts no traffic when the live version already is the task hash', async () => {
    const root = await fixtureRoot();
    const runner = new FakeRunner([{ id: 'version-1', annotations: { 'workers/tag': `nx-${HASH}` } }], {
      versions: [{ version_id: 'version-1', percentage: 100 }],
    });

    const result = await deployStage(
      root,
      { stage: 'pr123', repositoryRoot: root },
      dependencies(runner, new FakeCloudflare()),
    );

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

    const result = await deployStage(
      root,
      { stage: 'pr123', repositoryRoot: root },
      dependencies(runner, new FakeCloudflare()),
    );

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

    const result = await deployStage(
      root,
      { stage: 'pr123', repositoryRoot: root },
      dependencies(runner, new FakeCloudflare()),
    );

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

    await expect(
      deployStage(root, { stage: 'pr123', repositoryRoot: root }, dependencies(runner, new FakeCloudflare())),
    ).rejects.toThrow(
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
      { stage: 'pr123', repositoryRoot: root },
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
      { stage: 'pr123', repositoryRoot: root, versionEndpoint: 'https://fixture.example.test/__version' },
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
        { stage: 'pr123', repositoryRoot: root, versionEndpoint: 'https://fixture.example.test/__version' },
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
      { stage: 'pr123', repositoryRoot: root },
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

  it('sends only the values it has and leaves a secret the Worker already holds untouched', async () => {
    const root = await fixtureRoot();
    await writeFile(join(root, '.dev.vars.example'), 'FIXTURE_SECRET=""\nFIXTURE_TOKEN=""\n');
    const runner = new FakeRunner([], {});
    const cloudflare = new FakeCloudflare();
    cloudflare.scripts = [{ id: 'fixture-worker-staging' }];
    cloudflare.secrets['fixture-worker-staging'] = ['FIXTURE_TOKEN'];

    const result = await deployStage(
      root,
      { stage: 'staging', repositoryRoot: root },
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

    // `--secrets-file` applies additively, so naming only what this shell has leaves FIXTURE_TOKEN
    // exactly as the Worker holds it. That silence is only safe because the gate proved it is held.
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
        { stage: 'pr123', repositoryRoot: root },
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

    const result = await deployStage(
      root,
      { stage: 'pr123', repositoryRoot: root },
      dependencies(new FakeRunner([], {}), cloudflare),
    );

    expect(result.action).toBe('deployed');
    expect(createAttempts).toBe(1);
    expect(cloudflare.records.zone?.map((record) => record.name)).toEqual(['*.pr123.example.test']);
  });

  it('points the wildcard DNS record of a route without a path at the whole host', async () => {
    const root = await fixtureRoot(`${FIXTURE}
[[env.staging.routes]]
pattern = "*.staging.example.test"
zone_name = "example.test"
`);
    const cloudflare = new FakeCloudflare();
    cloudflare.zones = [{ id: 'zone', name: 'example.test' }];

    await deployStage(root, { stage: 'pr123', repositoryRoot: root }, dependencies(new FakeRunner([], {}), cloudflare));

    expect(cloudflare.mutations).toEqual([
      'create-r2:smoo-stage-records',
      `${RECORDED_PR123}/worker`,
      `${RECORDED_PR123}/route/example.test/*.pr123.example.test`,
      `${RECORDED_PR123}/dns/example.test/*.pr123.example.test`,
      'create-dns:zone:*.pr123.example.test:pr123.example.test',
      'create-route:zone:*.pr123.example.test:fixture-worker-pr123',
    ]);
  });
});

/**
 * `smoo.wrangler.secretStages` answers two questions from one declaration, and both are load
 * bearing: which secrets a stage must have before it may deploy, and which secrets it is allowed
 * to receive at all. The second direction is the security-relevant one — a capability CI exports
 * for preview stages must not ride a production deploy just because the variable is set.
 */
describe('stage-scoped deployment secrets', () => {
  const STAGED_FIXTURE = `${FIXTURE}
[env.production]
name = "fixture-worker-production"
workers_dev = false

[env.production.vars]
ENVIRONMENT = "production"
`;

  async function scopedRoot(secrets: string[], secretStages?: Record<string, string[]>): Promise<string> {
    const root = await fixtureRoot(STAGED_FIXTURE);
    await writeFile(join(root, '.dev.vars.example'), secrets.map((name) => `${name}=""\n`).join(''));
    if (secretStages) {
      await writeRepositoryRoot(root, { name: '@acme/api', smoo: { wrangler: { secretStages } } });
    }
    return root;
  }

  function environment(values: Record<string, string> = {}): NodeJS.ProcessEnv {
    return { CLOUDFLARE_ACCOUNT_ID: 'account-1', CLOUDFLARE_API_TOKEN: 'token', ...values };
  }

  it('refuses a first deploy whose stage-required secret is exported nowhere, naming every one', async () => {
    const root = await scopedRoot(['SESSION_SECRET', 'OAUTH_STATE_KEY'], {
      OAUTH_STATE_KEY: ['staging', 'production'],
    });
    const runner = new FakeRunner([], {});
    const cloudflare = new FakeCloudflare();

    await expect(
      deployStage(
        root,
        { stage: 'production', repositoryRoot: root },
        { runner, cloudflare, processEnv: environment() },
      ),
    ).rejects.toThrow(
      /Refusing to deploy fixture-worker-production to production[\s\S]*SESSION_SECRET[\s\S]*OAUTH_STATE_KEY/,
    );
    expect(cloudflare.mutations).toEqual([]);
    expect(runner.calls).toEqual([]);
  });

  it('treats a Worker that is not there as holding nothing, never as needing nothing', async () => {
    const root = await scopedRoot(['OAUTH_STATE_KEY']);
    const cloudflare = new FakeCloudflare();

    // The double answers 404 for an absent Worker, exactly as Cloudflare does. Reading that error
    // as "no secrets" would let this deploy through; asking at all would surface the 404 instead
    // of the refusal. Neither happens: the script listing already said the Worker holds nothing.
    await expect(
      deployStage(
        root,
        { stage: 'production', repositoryRoot: root },
        { runner: new FakeRunner([], {}), cloudflare, processEnv: environment() },
      ),
    ).rejects.toThrow(/Refusing to deploy fixture-worker-production to production[\s\S]*OAUTH_STATE_KEY/);
    expect(cloudflare.secretQueries).toEqual([]);
    expect(cloudflare.mutations).toEqual([]);
  });

  it('requires an unscoped secret of every stage, including a pull-request stage', async () => {
    const root = await scopedRoot(['SESSION_SECRET'], { OTHER_SECRET: ['staging'] });
    await writeFile(join(root, '.dev.vars.example'), 'SESSION_SECRET=""\nOTHER_SECRET=""\n');
    const cloudflare = new FakeCloudflare();

    await expect(
      deployStage(root, { stage: 'pr77', repositoryRoot: root }, dependencies(new FakeRunner([], {}), cloudflare)),
    ).rejects.toThrow(/fixture-worker-pr77[\s\S]*SESSION_SECRET — unscoped, so every stage requires it/);
    expect(cloudflare.mutations).toEqual([]);
  });

  it('does not require a preview-scoped secret of production', async () => {
    const root = await scopedRoot(['E2E_CONTROL_TOKEN'], { E2E_CONTROL_TOKEN: ['staging', 'preview'] });
    const runner = new FakeRunner([], {});
    const cloudflare = new FakeCloudflare();

    const result = await deployStage(
      root,
      { stage: 'production', repositoryRoot: root },
      { runner, cloudflare, processEnv: environment() },
    );

    expect(result).toMatchObject({ action: 'deployed', workerName: 'fixture-worker-production' });
    expect(runner.secretsPathSeen).toBeUndefined();
  });

  it('withholds a preview-scoped secret from production even when the deploying shell exports it', async () => {
    const root = await scopedRoot(['API_TOKEN', 'E2E_CONTROL_TOKEN'], { E2E_CONTROL_TOKEN: ['staging', 'preview'] });
    const runner = new FakeRunner([], {});
    const cloudflare = new FakeCloudflare();
    // The real environment, because that is what a child process inherits and what the shell of a
    // laptop deploy actually looks like when CI has exported a preview capability.
    process.env.E2E_CONTROL_TOKEN = 'teardown-value';

    try {
      const result = await deployStage(
        root,
        { stage: 'production', repositoryRoot: root },
        {
          runner,
          cloudflare,
          processEnv: environment({ API_TOKEN: 'api-value', E2E_CONTROL_TOKEN: 'teardown-value' }),
        },
      );

      expect(result.action).toBe('deployed');
      expect(JSON.parse(requiredTestValue(runner.secretsJson, 'secrets JSON'))).toEqual({ API_TOKEN: 'api-value' });
      // Not in the payload, and not readable by the deploy either: the capability is absent from
      // this stage at the process boundary, not merely left out of a file.
      for (const call of runner.calls) {
        expect(call.env.E2E_CONTROL_TOKEN).toBeUndefined();
      }
      expect(runner.calls.length).toBeGreaterThan(0);
    } finally {
      delete process.env.E2E_CONTROL_TOKEN;
    }
  });

  it('sends a preview-scoped secret to the stages it is scoped to', async () => {
    const scopes = { E2E_CONTROL_TOKEN: ['staging', 'preview'] };
    const values = { API_TOKEN: 'api-value', E2E_CONTROL_TOKEN: 'teardown-value' };

    for (const stage of ['staging', 'pr77'] as const) {
      const root = await scopedRoot(['API_TOKEN', 'E2E_CONTROL_TOKEN'], scopes);
      const runner = new FakeRunner([], {});

      await deployStage(
        root,
        { stage, repositoryRoot: root },
        { runner, cloudflare: new FakeCloudflare(), processEnv: environment(values) },
      );

      expect(JSON.parse(requiredTestValue(runner.secretsJson, `secrets JSON for ${stage}`))).toEqual(values);
    }
  });

  it('treats an empty scope as no stage at all, not as every stage', async () => {
    const root = await scopedRoot(['API_TOKEN', 'LOCAL_ONLY_KEY'], { LOCAL_ONLY_KEY: [] });
    const runner = new FakeRunner([], {});

    // `[]` and "absent from the map" are opposite declarations. A check written as
    // `!scope?.length` would collapse them and ship a local-development value to every stage.
    const result = await deployStage(
      root,
      { stage: 'production', repositoryRoot: root },
      {
        runner,
        cloudflare: new FakeCloudflare(),
        processEnv: environment({ API_TOKEN: 'api-value', LOCAL_ONLY_KEY: 'laptop-value' }),
      },
    );

    expect(result.action).toBe('deployed');
    expect(JSON.parse(requiredTestValue(runner.secretsJson, 'secrets JSON'))).toEqual({ API_TOKEN: 'api-value' });
  });

  it('refuses when the Worker holds a secret this stage is scoped out of, since a deploy cannot remove it', async () => {
    const root = await scopedRoot(['E2E_CONTROL_TOKEN'], { E2E_CONTROL_TOKEN: ['staging', 'preview'] });
    const cloudflare = new FakeCloudflare();
    cloudflare.scripts = [{ id: 'fixture-worker-production' }];
    cloudflare.secrets['fixture-worker-production'] = ['E2E_CONTROL_TOKEN'];

    await expect(
      deployStage(
        root,
        { stage: 'production', repositoryRoot: root },
        { runner: new FakeRunner([], {}), cloudflare, processEnv: environment() },
      ),
    ).rejects.toThrow(/keeps out of production[\s\S]*E2E_CONTROL_TOKEN — scoped to staging, preview/);
    expect(cloudflare.mutations).toEqual([]);
  });

  it('refuses a scope written against a name no secret has, rather than scoping nothing', async () => {
    const root = await scopedRoot(['E2E_CONTROL_TOKEN'], { E2E_CONTROL_TOEKN: ['staging'] });

    await expect(
      deployStage(
        root,
        { stage: 'production', repositoryRoot: root },
        { runner: new FakeRunner([], {}), cloudflare: new FakeCloudflare(), processEnv: environment() },
      ),
    ).rejects.toThrow(/secretStages scopes E2E_CONTROL_TOEKN, which .dev.vars.example does not declare/);
  });

  it('names a malformed scope declaration by its path in the manifest', async () => {
    const root = await scopedRoot(['E2E_CONTROL_TOKEN'], { E2E_CONTROL_TOKEN: ['pr7'] });

    await expect(
      deployStage(
        root,
        { stage: 'production', repositoryRoot: root },
        { runner: new FakeRunner([], {}), cloudflare: new FakeCloudflare(), processEnv: environment() },
      ),
    ).rejects.toThrow(
      /package\.json declares an invalid smoo\.wrangler block: smoo\.wrangler\.secretStages\.E2E_CONTROL_TOKEN\[0\]/,
    );
  });

  it('keeps every secret value out of the refusal and off every command line', async () => {
    const root = await scopedRoot(['API_TOKEN', 'SESSION_SECRET']);
    const runner = new FakeRunner([], {});
    const cloudflare = new FakeCloudflare();

    const refused = await deployStage(
      root,
      { stage: 'production', repositoryRoot: root },
      { runner, cloudflare, processEnv: environment({ API_TOKEN: 'api-value' }) },
    ).catch((error: unknown) => (error instanceof Error ? error.message : String(error)));

    expect(refused).toContain('SESSION_SECRET');
    expect(refused).not.toContain('api-value');

    // And the same on the path that does deploy: values reach wrangler through a 0600 file only.
    cloudflare.scripts = [{ id: 'fixture-worker-production' }];
    cloudflare.secrets['fixture-worker-production'] = ['SESSION_SECRET'];
    await deployStage(
      root,
      { stage: 'production', repositoryRoot: root },
      { runner, cloudflare, processEnv: environment({ API_TOKEN: 'api-value' }) },
    );

    expect(runner.calls.flatMap((call) => call.args).join('\u0000')).not.toContain('api-value');
    expect(JSON.parse(requiredTestValue(runner.secretsJson, 'secrets JSON'))).toEqual({ API_TOKEN: 'api-value' });
  });
});

const SCOPE = 'github.com/acme/app';
const PR7_PREFIX = 'v1/github.com%2Facme%2Fapp/pr7/';

/** Writes `records` into the fake's record bucket the way a deploy of `stage` under `scope` would. */
function recordStage(
  cloudflare: FakeCloudflare,
  records: StageRecord[],
  { scope = SCOPE, stage = 'pr7' }: { scope?: string; stage?: `pr${number}` } = {},
): string[] {
  if (!cloudflare.buckets.some((bucket) => bucket.name === STAGE_RECORDS_BUCKET)) {
    cloudflare.buckets.push({ name: STAGE_RECORDS_BUCKET });
  }
  const keys = records.map((record) => stageRecordKey(scope, stage, record));
  cloudflare.objects[STAGE_RECORDS_BUCKET] = [...(cloudflare.objects[STAGE_RECORDS_BUCKET] ?? []), ...keys];
  return keys;
}

/** A workspace root naming `github.com/acme/app`; no `GITHUB_REPOSITORY`, so no CI cross-check. */
async function cleanupRoot(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-cleanup-test-'));
  roots.push(root);
  await writeRepositoryRoot(root);
  return root;
}

function cleanup(root: string, prNumber: number, cloudflare: CloudflareClient) {
  return cleanupPullRequest(root, prNumber, { cloudflare, processEnv: {} });
}

/** Everything two Workers of one stage recorded: each binds the shared KV namespace and DNS record. */
const FULL_STAGE: StageRecord[] = [
  { kind: 'worker', worker: 'api-pr7' },
  { kind: 'kv', worker: 'api-pr7', title: 'sessions-pr7' },
  { kind: 'd1', worker: 'api-pr7', name: 'site-pr7-db' },
  { kind: 'dns', worker: 'api-pr7', zone: 'example.test', name: '*.pr7.example.test' },
  { kind: 'worker', worker: 'web-pr7' },
  { kind: 'kv', worker: 'web-pr7', title: 'sessions-pr7' },
  { kind: 'r2', worker: 'web-pr7', bucket: 'media-pr7' },
  { kind: 'domain', worker: 'web-pr7', hostname: 'app.pr7.example.test' },
  { kind: 'route', worker: 'web-pr7', zone: 'example.test', pattern: '*.pr7.example.test/*' },
  { kind: 'dns', worker: 'web-pr7', zone: 'example.test', name: '*.pr7.example.test' },
];

/** The live account a FULL_STAGE deploy leaves behind, next to items that are not the stage's. */
function liveFullStage(cloudflare: FakeCloudflare): void {
  cloudflare.scripts = [{ id: 'api-pr7' }, { id: 'web-pr7' }, { id: 'other-pr7' }, { id: 'web-staging' }];
  cloudflare.namespaces = [
    { id: 'kv-sessions', title: 'sessions-pr7' },
    { id: 'kv-other', title: 'other-pr7' },
  ];
  cloudflare.d1Databases = [
    { uuid: 'd1-site', name: 'site-pr7-db' },
    { uuid: 'd1-other', name: 'other-pr7-db' },
  ];
  cloudflare.buckets.push({ name: 'media-pr7' }, { name: 'other-pr7' });
  cloudflare.objects['media-pr7'] = ['one', 'nested/two'];
  cloudflare.objects['other-pr7'] = ['keep'];
  // Mixed case and a trailing dot, as Cloudflare may answer them.
  cloudflare.domains = [
    { id: 'domain-app', hostname: 'App.PR7.example.test', service: 'web-pr7' },
    { id: 'domain-other', hostname: 'other.pr7.example.test', service: 'other-pr7' },
  ];
  cloudflare.zones = [
    { id: 'zone-example', name: 'example.test' },
    { id: 'zone-unrelated', name: 'unrelated.test' },
  ];
  cloudflare.routes['zone-example'] = [
    { id: 'route-web', pattern: '*.PR7.example.test/*', script: 'web-pr7' },
    { id: 'route-other', pattern: 'other.pr7.example.test/*', script: 'other-pr7' },
  ];
  cloudflare.records['zone-example'] = [
    { id: 'dns-wildcard', name: '*.pr7.Example.test.', type: 'CNAME', content: 'PR7.example.test' },
    { id: 'dns-other', name: '*.other.pr7.example.test', type: 'CNAME', content: 'other.pr7.example.test' },
  ];
}

describe('cleanup-pr from stage records', () => {
  it('rejects an invalid PR before touching the client', async () => {
    const cloudflare = new FakeCloudflare();

    await expect(cleanup(await cleanupRoot(), 0, cloudflare)).rejects.toThrow(/1 through 999999999/);
    expect(cloudflare.reads).toEqual([]);
  });

  it('refuses a root without a repository before any Cloudflare call', async () => {
    const root = await cleanupRoot();
    await writeFile(join(root, 'package.json'), '{ "name": "@acme/app" }\n');
    const cloudflare = new FakeCloudflare();

    await expect(cleanup(root, 7, cloudflare)).rejects.toThrow(`${join(root, 'package.json')} declares no repository`);
    expect(cloudflare.reads).toEqual([]);
  });

  it('reads nothing else and deletes nothing when the account has no record bucket', async () => {
    const cloudflare = new FakeCloudflare();
    cloudflare.scripts = [{ id: 'web-pr7' }];

    const result = await cleanup(await cleanupRoot(), 7, cloudflare);

    expect(result).toMatchObject({ stage: 'pr7', scope: SCOPE, recorded: 0, alreadyGone: 0, leftInPlace: [] });
    expect(cloudflare.reads).toEqual(['r2']);
    expect(cloudflare.mutations).toEqual([]);
  });

  it('reads nothing else and deletes nothing when the stage has no keys, whatever other stages and repositories hold', async () => {
    const cloudflare = new FakeCloudflare();
    recordStage(cloudflare, [{ kind: 'worker', worker: 'web-pr70' }], { stage: 'pr70' });
    recordStage(cloudflare, [{ kind: 'worker', worker: 'web-pr7' }], { scope: 'github.com/acme/other' });
    cloudflare.scripts = [{ id: 'web-pr7' }, { id: 'web-pr70' }];

    const result = await cleanup(await cleanupRoot(), 7, cloudflare);

    expect(result.recorded).toBe(0);
    expect(cloudflare.reads).toEqual(['r2', `objects:${STAGE_RECORDS_BUCKET}:${PR7_PREFIX}`]);
    expect(cloudflare.mutations).toEqual([]);
  });

  it('deletes exactly the recorded items, each once, in dependency order, and the records last', async () => {
    const cloudflare = new FakeCloudflare();
    const keys = recordStage(cloudflare, FULL_STAGE);
    const otherScope = recordStage(cloudflare, [{ kind: 'worker', worker: 'other-pr7' }], {
      scope: 'github.com/acme/other',
    });
    liveFullStage(cloudflare);

    const result = await cleanup(await cleanupRoot(), 7, cloudflare);

    expect(cloudflare.mutations).toEqual([
      'delete-domain:domain-app',
      'delete-route:zone-example:route-web',
      'delete-dns:zone-example:dns-wildcard',
      'delete-worker:api-pr7',
      'delete-worker:web-pr7',
      'delete-kv:kv-sessions',
      'delete-object:media-pr7:one',
      'delete-object:media-pr7:nested/two',
      'delete-r2:media-pr7',
      'delete-d1:d1-site',
      ...keys.map((key) => `delete-object:${STAGE_RECORDS_BUCKET}:${key}`),
    ]);
    expect(result).toEqual({
      stage: 'pr7',
      scope: SCOPE,
      recorded: FULL_STAGE.length,
      deleted: {
        workers: 2,
        routes: 1,
        domains: 1,
        dnsRecords: 1,
        kvNamespaces: 1,
        r2Buckets: 1,
        r2Objects: 2,
        d1Databases: 1,
      },
      alreadyGone: 0,
      leftInPlace: [],
    });
    expect(cloudflare.objects[STAGE_RECORDS_BUCKET]).toEqual(otherScope);
    // Only the recorded zone, found by its name; never a listing of every zone.
    expect(cloudflare.reads.filter((read) => read.startsWith('zones'))).toEqual(['zones:example.test']);
    expect(cloudflare.reads).not.toContain('routes:zone-unrelated');
    expect(cloudflare.reads).not.toContain('dns:zone-unrelated');
  });

  it('lists only what a record names a kind of', async () => {
    const cloudflare = new FakeCloudflare();
    recordStage(cloudflare, [{ kind: 'worker', worker: 'web-pr7' }]);
    cloudflare.scripts = [{ id: 'web-pr7' }];

    await cleanup(await cleanupRoot(), 7, cloudflare);

    expect(cloudflare.reads).toEqual(['r2', `objects:${STAGE_RECORDS_BUCKET}:${PR7_PREFIX}`, 'workers']);
  });

  it('leaves a recorded route or custom domain another Worker now holds in place and says so', async () => {
    const cloudflare = new FakeCloudflare();
    recordStage(cloudflare, [
      { kind: 'domain', worker: 'web-pr7', hostname: 'app.pr7.example.test' },
      { kind: 'route', worker: 'web-pr7', zone: 'example.test', pattern: '*.pr7.example.test/*' },
    ]);
    cloudflare.domains = [{ id: 'domain-app', hostname: 'app.pr7.example.test', service: 'other-worker' }];
    cloudflare.zones = [{ id: 'zone-example', name: 'example.test' }];
    cloudflare.routes['zone-example'] = [{ id: 'route-web', pattern: '*.pr7.example.test/*', script: 'other-worker' }];

    const result = await cleanup(await cleanupRoot(), 7, cloudflare);

    expect(cloudflare.mutations.filter((mutation) => !mutation.includes(STAGE_RECORDS_BUCKET))).toEqual([]);
    expect(result.leftInPlace).toEqual([
      'custom domain app.pr7.example.test (bound to other-worker)',
      'route *.pr7.example.test/* (bound to other-worker)',
    ]);
    expect(result.alreadyGone).toBe(0);
  });

  it('leaves in place the wildcard DNS record a route left in place still needs', async () => {
    const cloudflare = new FakeCloudflare();
    recordStage(cloudflare, [
      { kind: 'route', worker: 'web-pr7', zone: 'example.test', pattern: '*.pr7.example.test/*' },
      { kind: 'dns', worker: 'web-pr7', zone: 'example.test', name: '*.pr7.example.test' },
      { kind: 'route', worker: 'web-pr7', zone: 'example.test', pattern: '*.api.pr7.example.test/*' },
      { kind: 'dns', worker: 'web-pr7', zone: 'example.test', name: '*.api.pr7.example.test' },
    ]);
    cloudflare.zones = [{ id: 'zone-example', name: 'example.test' }];
    cloudflare.routes['zone-example'] = [
      { id: 'route-web', pattern: '*.pr7.example.test/*', script: 'other-worker' },
      { id: 'route-api', pattern: '*.api.pr7.example.test/*', script: 'web-pr7' },
    ];
    cloudflare.records['zone-example'] = [
      { id: 'dns-web', name: '*.pr7.example.test', type: 'CNAME', content: 'pr7.example.test' },
      { id: 'dns-api', name: '*.api.pr7.example.test', type: 'CNAME', content: 'api.pr7.example.test' },
    ];

    const result = await cleanup(await cleanupRoot(), 7, cloudflare);

    expect(cloudflare.mutations.filter((mutation) => !mutation.includes(STAGE_RECORDS_BUCKET))).toEqual([
      'delete-route:zone-example:route-api',
      'delete-dns:zone-example:dns-api',
    ]);
    expect(result.leftInPlace).toEqual([
      'route *.pr7.example.test/* (bound to other-worker)',
      'DNS record *.pr7.example.test (serves route *.pr7.example.test/*)',
    ]);
    expect(result.alreadyGone).toBe(0);
  });

  it("deletes a route and custom domain that moved between Workers of the stage, with the route's DNS record", async () => {
    const cloudflare = new FakeCloudflare();
    recordStage(cloudflare, [
      { kind: 'domain', worker: 'web-pr7', hostname: 'app.pr7.example.test' },
      { kind: 'route', worker: 'web-pr7', zone: 'example.test', pattern: '*.pr7.example.test/*' },
      { kind: 'dns', worker: 'web-pr7', zone: 'example.test', name: '*.pr7.example.test' },
      { kind: 'domain', worker: 'api-pr7', hostname: 'app.pr7.example.test' },
      { kind: 'route', worker: 'api-pr7', zone: 'example.test', pattern: '*.pr7.example.test/*' },
      { kind: 'dns', worker: 'api-pr7', zone: 'example.test', name: '*.pr7.example.test' },
    ]);
    cloudflare.domains = [{ id: 'domain-app', hostname: 'app.pr7.example.test', service: 'api-pr7' }];
    cloudflare.zones = [{ id: 'zone-example', name: 'example.test' }];
    cloudflare.routes['zone-example'] = [{ id: 'route-web', pattern: '*.pr7.example.test/*', script: 'api-pr7' }];
    cloudflare.records['zone-example'] = [
      { id: 'dns-web', name: '*.pr7.example.test', type: 'CNAME', content: 'pr7.example.test' },
    ];

    const result = await cleanup(await cleanupRoot(), 7, cloudflare);

    expect(cloudflare.mutations.filter((mutation) => !mutation.includes(STAGE_RECORDS_BUCKET))).toEqual([
      'delete-domain:domain-app',
      'delete-route:zone-example:route-web',
      'delete-dns:zone-example:dns-web',
    ]);
    expect(result.leftInPlace).toEqual([]);
    expect(result.alreadyGone).toBe(0);
  });

  it('deletes a recorded route bound to no Worker, with its DNS record', async () => {
    const cloudflare = new FakeCloudflare();
    recordStage(cloudflare, [
      { kind: 'route', worker: 'web-pr7', zone: 'example.test', pattern: '*.pr7.example.test/*' },
      { kind: 'dns', worker: 'web-pr7', zone: 'example.test', name: '*.pr7.example.test' },
    ]);
    cloudflare.zones = [{ id: 'zone-example', name: 'example.test' }];
    cloudflare.routes['zone-example'] = [{ id: 'route-web', pattern: '*.pr7.example.test/*' }];
    cloudflare.records['zone-example'] = [
      { id: 'dns-web', name: '*.pr7.example.test', type: 'CNAME', content: 'pr7.example.test' },
    ];

    const result = await cleanup(await cleanupRoot(), 7, cloudflare);

    expect(cloudflare.mutations.filter((mutation) => !mutation.includes(STAGE_RECORDS_BUCKET))).toEqual([
      'delete-route:zone-example:route-web',
      'delete-dns:zone-example:dns-web',
    ]);
    expect(result.leftInPlace).toEqual([]);
  });

  it('counts a recorded item that no longer exists as already gone', async () => {
    const cloudflare = new FakeCloudflare();
    const keys = recordStage(cloudflare, [
      { kind: 'worker', worker: 'web-pr7' },
      { kind: 'kv', worker: 'web-pr7', title: 'sessions-pr7' },
      { kind: 'd1', worker: 'web-pr7', name: 'site-pr7-db' },
      { kind: 'r2', worker: 'web-pr7', bucket: 'media-pr7' },
      { kind: 'domain', worker: 'web-pr7', hostname: 'app.pr7.example.test' },
      // A zone the account no longer has: its routes and records went with it.
      { kind: 'route', worker: 'web-pr7', zone: 'gone.test', pattern: '*.pr7.gone.test/*' },
      { kind: 'dns', worker: 'web-pr7', zone: 'example.test', name: '*.pr7.example.test' },
    ]);
    cloudflare.scripts = [];
    cloudflare.zones = [{ id: 'zone-example', name: 'example.test' }];

    const result = await cleanup(await cleanupRoot(), 7, cloudflare);

    expect(result.alreadyGone).toBe(7);
    expect(Object.values(result.deleted).every((count) => count === 0)).toBe(true);
    expect(cloudflare.mutations).toEqual(keys.map((key) => `delete-object:${STAGE_RECORDS_BUCKET}:${key}`));
  });

  it('keeps every record when a delete fails, and a re-run finishes what is left', async () => {
    const cloudflare = new FakeCloudflare();
    const keys = recordStage(cloudflare, FULL_STAGE);
    liveFullStage(cloudflare);
    const deleteWorkerScript = cloudflare.deleteWorkerScript.bind(cloudflare);
    cloudflare.deleteWorkerScript = async () => {
      throw new Error('Worker delete refused');
    };
    const root = await cleanupRoot();

    await expect(cleanup(root, 7, cloudflare)).rejects.toThrow(/Worker delete refused/);
    expect(cloudflare.objects[STAGE_RECORDS_BUCKET]).toEqual(keys);
    expect(cloudflare.mutations.some((mutation) => mutation.includes(STAGE_RECORDS_BUCKET))).toBe(false);

    cloudflare.deleteWorkerScript = deleteWorkerScript;
    const rerun = await cleanup(root, 7, cloudflare);

    // The domain, route and DNS record went in the first run.
    expect(rerun.alreadyGone).toBe(3);
    expect(rerun.deleted).toMatchObject({ workers: 2, kvNamespaces: 1, r2Buckets: 1, d1Databases: 1, domains: 0 });
    expect(cloudflare.objects[STAGE_RECORDS_BUCKET]).toEqual([]);
  });

  it('refuses the whole stage before any delete when one key is not its own', async () => {
    for (const stray of [`${PR7_PREFIX}web-pr8/worker`, `${PR7_PREFIX}web-pr7/junk`, `${PR7_PREFIX}web-pr7`]) {
      const cloudflare = new FakeCloudflare();
      recordStage(cloudflare, [{ kind: 'worker', worker: 'web-pr7' }]);
      cloudflare.objects[STAGE_RECORDS_BUCKET]?.push(stray);
      cloudflare.scripts = [{ id: 'web-pr7' }, { id: 'web-pr8' }];

      await expect(cleanup(await cleanupRoot(), 7, cloudflare)).rejects.toThrow(stray);
      expect(cloudflare.mutations).toEqual([]);
    }
  });

  it('deletes two recorded Workers that bind each other', async () => {
    // Cloudflare refuses to delete a Worker another Worker still binds unless the delete is forced;
    // this account answers exactly that way, so whichever Worker went first would stop an unforced cleanup.
    const keys = [
      stageRecordKey(SCOPE, 'pr7', { kind: 'worker', worker: 'api-pr7' }),
      stageRecordKey(SCOPE, 'pr7', { kind: 'worker', worker: 'web-pr7' }),
    ];
    const bindings: Record<string, string> = { 'api-pr7': 'web-pr7', 'web-pr7': 'api-pr7' };
    const scripts = new Set(['api-pr7', 'web-pr7']);
    const deleted: string[] = [];
    const answer = (result: unknown, status = 200) =>
      new Response(JSON.stringify({ success: status < 400, result, errors: [] }), { status });
    const client = new CloudflareRestClient('account-1', 'token', async (input, init) => {
      const url = new URL(input);
      const path = url.pathname.replace('/client/v4/accounts/account-1', '');
      const method = init?.method ?? 'GET';
      if (method === 'GET' && path === '/r2/buckets') return answer([{ name: STAGE_RECORDS_BUCKET }]);
      if (method === 'GET' && path === `/r2/buckets/${STAGE_RECORDS_BUCKET}/objects`) {
        return answer(keys.map((key) => ({ key })));
      }
      if (method === 'GET' && path === '/workers/scripts') return answer([...scripts].map((id) => ({ id })));
      const worker = /^\/workers\/scripts\/([^/]+)$/.exec(path)?.[1];
      if (method === 'DELETE' && worker) {
        const boundBy = bindings[worker];
        if (boundBy && scripts.has(boundBy) && url.searchParams.get('force') !== 'true') {
          return answer(null, 400);
        }
        scripts.delete(worker);
        deleted.push(worker);
        return answer(null);
      }
      if (method === 'DELETE' && path.startsWith(`/r2/buckets/${STAGE_RECORDS_BUCKET}/objects/`)) return answer(null);
      throw new Error(`unexpected ${method} ${path}`);
    });

    const result = await cleanupPullRequest(await cleanupRoot(), 7, { cloudflare: client, processEnv: {} });

    expect(deleted).toEqual(['api-pr7', 'web-pr7']);
    expect(result.deleted.workers).toBe(2);
  });
});

describe('describeCleanup', () => {
  const nothingDeleted = {
    workers: 0,
    routes: 0,
    domains: 0,
    dnsRecords: 0,
    kvNamespaces: 0,
    r2Buckets: 0,
    r2Objects: 0,
    d1Databases: 0,
  };

  it('names every count, what was already gone and what was left in place', () => {
    expect(
      describeCleanup({
        stage: 'pr7',
        scope: SCOPE,
        recorded: 23,
        deleted: {
          workers: 3,
          domains: 2,
          routes: 4,
          dnsRecords: 1,
          kvNamespaces: 2,
          r2Buckets: 1,
          r2Objects: 15,
          d1Databases: 0,
        },
        alreadyGone: 2,
        leftInPlace: ['route *.pr7.example.com/* (bound to other-worker)'],
      }),
    ).toBe(
      'Cleaned pr7 of github.com/acme/app from 23 records: deleted 3 Workers, 2 custom domains, 4 routes, 1 DNS record, 2 KV namespaces, 1 R2 bucket (15 objects), 0 D1 databases; 2 recorded items were already gone; left in place: route *.pr7.example.com/* (bound to other-worker).',
    );
  });

  it('leaves out the left-in-place clause when nothing was left, and counts in the singular', () => {
    expect(
      describeCleanup({
        stage: 'pr7',
        scope: SCOPE,
        recorded: 1,
        deleted: { ...nothingDeleted, workers: 1, r2Buckets: 2, r2Objects: 1, d1Databases: 1 },
        alreadyGone: 1,
        leftInPlace: [],
      }),
    ).toBe(
      'Cleaned pr7 of github.com/acme/app from 1 record: deleted 1 Worker, 0 custom domains, 0 routes, 0 DNS records, 0 KV namespaces, 2 R2 buckets (1 object), 1 D1 database; 1 recorded item was already gone.',
    );
  });

  it('says why a stage without records deleted nothing', () => {
    expect(
      describeCleanup({
        stage: 'pr7',
        scope: SCOPE,
        recorded: 0,
        deleted: nothingDeleted,
        alreadyGone: 0,
        leftInPlace: [],
      }),
    ).toBe(
      'Nothing is recorded for pr7 of github.com/acme/app, so nothing was deleted (a pull request that deployed nothing, or a stage deployed before smoo recorded stages).',
    );
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
  await writeRepositoryRoot(root);
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
      deployStage(
        root,
        { stage: 'staging', repositoryRoot: root, config: configPath },
        dependencies(runner, cloudflare),
      ),
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

    const result = await deployStage(
      root,
      { stage: 'staging', repositoryRoot: root, config: configPath },
      dependencies(runner, cloudflare),
    );

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

    const result = await deployStage(
      root,
      { stage: 'pr7', repositoryRoot: root, config: configPath },
      dependencies(runner, cloudflare),
    );

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
      { stage: 'production', repositoryRoot: root, config: configPath },
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

    const result = await deployStage(
      root,
      { stage: 'staging', repositoryRoot: root, config: configPath },
      dependencies(runner, cloudflare),
    );

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

    const result = await deployStage(
      root,
      { stage: 'staging', repositoryRoot: root, config: configPath },
      dependencies(runner, cloudflare),
    );

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
      deployStage(root, { stage: 'pr7', repositoryRoot: root, config: configPath }, dependencies(runner, cloudflare)),
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
      deployStage(
        root,
        { stage: 'pr7', repositoryRoot: root, config: configPath },
        dependencies(new FakeRunner(), cloudflare),
      ),
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
      deployStage(
        root,
        { stage: 'pr7', repositoryRoot: root, config: configPath },
        dependencies(new FakeRunner(), cloudflare),
      ),
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
      deployStage(
        root,
        { stage: 'pr7', repositoryRoot: root, config: configPath },
        dependencies(new FakeRunner(), cloudflare),
      ),
    ).rejects.toThrow(/Refusing to deploy fixture-website-preview-pr7 to pr7[\s\S]*FIXTURE_SECRET/);
    expect(cloudflare.mutations).toEqual([]);
  });

  it('refuses a config with env blocks', async () => {
    const { root, configPath } = await flatFixtureRoot();
    await writeFile(configPath, JSON.stringify({ name: 'x-staging', env: { staging: {} } }));

    await expect(
      deployStage(
        root,
        { stage: 'staging', repositoryRoot: root, config: configPath },
        dependencies(new FakeRunner(), new FakeCloudflare()),
      ),
    ).rejects.toThrow(/env blocks/);
  });

  it('names the config file when it is not JSON', async () => {
    const { root, configPath } = await flatFixtureRoot('{');

    await expect(
      deployStage(
        root,
        { stage: 'staging', repositoryRoot: root, config: configPath },
        dependencies(new FakeRunner(), new FakeCloudflare()),
      ),
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

// The same project as FIXTURE/ROUTED_FIXTURE, in the format Cloudflare now recommends. The
// deploy has to reach the same worker, the same routes and the same derived stage from it.
const JSONC_FIXTURE = `{
  // A worker whose stages are derived, written as JSONC.
  "env": {
    "staging": {
      "name": "fixture-worker-staging",
      "workers_dev": false,
      "routes": [{ "pattern": "*.staging.example.test/*", "zone_name": "example.test" }],
      "vars": { "ENVIRONMENT": "staging" },
    },
  },
}
`;

describe('deployStage with a JSONC source config', () => {
  it('deploys staging from wrangler.jsonc itself, with --env and no temporary file', async () => {
    const root = await fixtureRoot(JSONC_FIXTURE, 'wrangler.jsonc');
    const runner = new FakeRunner();
    const cloudflare = new FakeCloudflare();
    cloudflare.scripts = [{ id: 'fixture-worker-staging' }];
    cloudflare.zones = [{ id: 'zone-1', name: 'example.test' }];

    const result = await deployStage(
      root,
      { stage: 'staging', repositoryRoot: root },
      dependencies(runner, cloudflare),
    );

    expect(result).toMatchObject({ workerName: 'fixture-worker-staging', action: 'deployed' });
    const deploy = requiredTestValue(
      runner.calls.find((call) => call.args[0] === 'deploy'),
      'deploy call',
    );
    expect(deploy.args.slice(0, 5)).toEqual(['deploy', '--config', join(root, 'wrangler.jsonc'), '--env', 'staging']);
    expect(cloudflare.mutations).toEqual([
      'create-dns:zone-1:*.staging.example.test:staging.example.test',
      'create-route:zone-1:*.staging.example.test/*:fixture-worker-staging',
    ]);
    expect((await readdir(root)).filter((name) => name.startsWith('.wrangler.smoo-'))).toEqual([]);
  });

  it('derives a pull-request stage into a temporary JSON config beside the source', async () => {
    const root = await fixtureRoot(JSONC_FIXTURE, 'wrangler.jsonc');
    const runner = new FakeRunner();
    const cloudflare = new FakeCloudflare();
    cloudflare.zones = [{ id: 'zone-1', name: 'example.test' }];
    let derivedConfig: Record<string, unknown> | undefined;
    let derivedPath: string | undefined;
    runner.onCall = async (args) => {
      if (args[0] !== 'deploy') return;
      derivedPath = args[args.indexOf('--config') + 1];
      derivedConfig = JSON.parse(await readFile(derivedPath ?? '', 'utf8'));
    };

    const result = await deployStage(root, { stage: 'pr123', repositoryRoot: root }, dependencies(runner, cloudflare));

    expect(result).toMatchObject({ workerName: 'fixture-worker-pr123', action: 'deployed' });
    // Wrangler picks its parser by extension, so the derived config is JSON whatever the source was.
    expect(derivedPath?.startsWith(join(root, '.wrangler.smoo-'))).toBe(true);
    expect(derivedPath?.endsWith('.json')).toBe(true);
    expect(derivedConfig?.env).toEqual({
      staging: {
        name: 'fixture-worker-staging',
        workers_dev: false,
        routes: [{ pattern: '*.staging.example.test/*', zone_name: 'example.test' }],
        vars: { ENVIRONMENT: 'staging' },
      },
      pr123: {
        name: 'fixture-worker-pr123',
        workers_dev: false,
        routes: [{ pattern: '*.pr123.example.test/*', zone_name: 'example.test' }],
        vars: { ENVIRONMENT: 'pr123' },
      },
    });
    const deploy = requiredTestValue(
      runner.calls.find((call) => call.args[0] === 'deploy'),
      'deploy call',
    );
    expect(deploy.args.slice(3, 5)).toEqual(['--env', 'pr123']);
    // The committed source is never rewritten, and the derived copy does not outlive the deploy.
    expect(await readFile(join(root, 'wrangler.jsonc'), 'utf8')).toBe(JSONC_FIXTURE);
    expect((await readdir(root)).filter((name) => name.startsWith('.wrangler.smoo-'))).toEqual([]);
  });

  it('refuses a project carrying two source configs before it reads either', async () => {
    const root = await fixtureRoot(JSONC_FIXTURE, 'wrangler.jsonc');
    await writeFile(join(root, 'wrangler.toml'), FIXTURE);
    const cloudflare = new FakeCloudflare();

    await expect(
      deployStage(root, { stage: 'staging', repositoryRoot: root }, dependencies(new FakeRunner(), cloudflare)),
    ).rejects.toThrow(`${root} declares more than one Wrangler configuration: wrangler.jsonc, wrangler.toml.`);
    expect(cloudflare.mutations).toEqual([]);
  });
});

/**
 * A pull-request stage writes down everything it will create before it creates any of it, so the
 * cleanup can delete exactly what this repository's stage made and nothing another repository did.
 */
describe('deployStage records a pull-request stage before creating it', () => {
  /** Puts the wrangler commands that change something into the same log as the Cloudflare mutations. */
  function logWranglerInto(runner: FakeRunner, cloudflare: FakeCloudflare): void {
    runner.onCall = async (args) => {
      cloudflare.mutations.push(`wrangler ${args[0]}`);
    };
  }

  it('records every item of a TOML stage, each key once, before it creates any of them', async () => {
    const root = await fixtureRoot(`${ROUTED_FIXTURE}
[[env.staging.routes]]
pattern = "*.staging.example.test/api/*"
zone_name = "example.test"

[[env.staging.kv_namespaces]]
binding = "SESSIONS"
id = "kv-staging"
`);
    const runner = new FakeRunner([], {});
    const cloudflare = new FakeCloudflare();
    cloudflare.namespaces = [{ id: 'kv-staging', title: 'fixture-sessions-staging' }];
    cloudflare.zones = [{ id: 'zone', name: 'example.test' }];
    logWranglerInto(runner, cloudflare);

    await deployStage(root, { stage: 'pr123', repositoryRoot: root }, dependencies(runner, cloudflare));

    // Both `*.` routes serve one host, so they share one DNS record and one key.
    expect(cloudflare.mutations).toEqual([
      'create-r2:smoo-stage-records',
      `${RECORDED_PR123}/worker`,
      `${RECORDED_PR123}/kv/fixture-sessions-pr123`,
      `${RECORDED_PR123}/route/example.test/*.pr123.example.test%2F*`,
      `${RECORDED_PR123}/dns/example.test/*.pr123.example.test`,
      `${RECORDED_PR123}/route/example.test/*.pr123.example.test%2Fapi%2F*`,
      'create-kv:fixture-sessions-pr123',
      'create-dns:zone:*.pr123.example.test:pr123.example.test',
      'create-route:zone:*.pr123.example.test/*:fixture-worker-pr123',
      'create-route:zone:*.pr123.example.test/api/*:fixture-worker-pr123',
      'wrangler deploy',
    ]);
  });

  it('records a JSONC stage before it creates any of it', async () => {
    const root = await fixtureRoot(JSONC_FIXTURE, 'wrangler.jsonc');
    const runner = new FakeRunner([], {});
    const cloudflare = new FakeCloudflare();
    cloudflare.zones = [{ id: 'zone-1', name: 'example.test' }];
    logWranglerInto(runner, cloudflare);

    await deployStage(root, { stage: 'pr123', repositoryRoot: root }, dependencies(runner, cloudflare));

    expect(cloudflare.mutations).toEqual([
      'create-r2:smoo-stage-records',
      `${RECORDED_PR123}/worker`,
      `${RECORDED_PR123}/route/example.test/*.pr123.example.test%2F*`,
      `${RECORDED_PR123}/dns/example.test/*.pr123.example.test`,
      'create-dns:zone-1:*.pr123.example.test:pr123.example.test',
      'create-route:zone-1:*.pr123.example.test/*:fixture-worker-pr123',
      'wrangler deploy',
    ]);
  });

  it('records a stage derived from a flat config before it creates any of it', async () => {
    const { root, configPath } = await flatFixtureRoot();
    const runner = new FakeRunner([], {});
    const cloudflare = new FakeCloudflare();
    cloudflare.namespaces = [{ id: 'kv-staging', title: 'fixture-SESSION-staging' }];
    cloudflare.d1Databases = [{ uuid: 'd1-staging', name: 'fixture-website-staging-db' }];
    logWranglerInto(runner, cloudflare);

    await deployStage(
      root,
      { stage: 'pr7', repositoryRoot: root, config: configPath },
      dependencies(runner, cloudflare),
    );

    const recorded = 'put-record:v1/github.com%2Facme%2Fapp/pr7/fixture-website-preview-pr7';
    expect(cloudflare.mutations).toEqual([
      'create-r2:smoo-stage-records',
      `${recorded}/worker`,
      `${recorded}/kv/fixture-SESSION-pr7`,
      `${recorded}/d1/fixture-website-pr7-db`,
      `${recorded}/route/example.test/site.pr7.example.test%2F*`,
      'create-kv:fixture-SESSION-pr7',
      'create-d1:fixture-website-pr7-db',
      'wrangler d1',
      'wrangler deploy',
    ]);
  });

  it('records again on a push whose build is already live', async () => {
    const root = await fixtureRoot();
    const runner = new FakeRunner([{ id: 'version-1', annotations: { 'workers/tag': `nx-${HASH}` } }], {
      versions: [{ version_id: 'version-1', percentage: 100 }],
    });
    const cloudflare = new FakeCloudflare();
    cloudflare.buckets = [{ name: 'smoo-stage-records' }];

    const result = await deployStage(root, { stage: 'pr123', repositoryRoot: root }, dependencies(runner, cloudflare));

    // A stage deployed before records existed gets them on its next push, build unchanged or not.
    expect(result.action).toBe('remote-cache-hit');
    expect(cloudflare.mutations).toEqual([`${RECORDED_PR123}/worker`]);
  });

  it('continues when a parallel deploy creates the record bucket first', async () => {
    const root = await fixtureRoot();
    const cloudflare = new FakeCloudflare();
    cloudflare.createR2Bucket = async (name) => {
      cloudflare.buckets.push({ name });
      throw new Error('bucket already exists');
    };

    const result = await deployStage(
      root,
      { stage: 'pr123', repositoryRoot: root },
      dependencies(new FakeRunner([], {}), cloudflare),
    );

    expect(result.action).toBe('deployed');
    expect(cloudflare.mutations).toEqual([`${RECORDED_PR123}/worker`]);
  });

  it('creates nothing when a record cannot be written, and names R2 write only for a 403', async () => {
    for (const [status, namesPermission] of [
      [403, true],
      [429, false],
    ] as const) {
      const root = await fixtureRoot(ROUTED_FIXTURE);
      const runner = new FakeRunner([], {});
      const cloudflare = new FakeCloudflare();
      cloudflare.zones = [{ id: 'zone', name: 'example.test' }];
      const refusal = new CloudflareApiError('Cloudflare API /r2 failed: refused', status, [10000]);
      cloudflare.putR2Object = async () => {
        throw refusal;
      };

      const error = await deployStage(
        root,
        { stage: 'pr123', repositoryRoot: root },
        dependencies(runner, cloudflare),
      ).catch((thrown: unknown) => thrown);

      expect(error).toBeInstanceOf(Error);
      expect(error).toMatchObject({ cause: refusal });
      const message = error instanceof Error ? error.message : '';
      expect(message).toStartWith(
        'Recording pr123 in R2 bucket smoo-stage-records failed, so nothing was created: Cloudflare API /r2 failed: refused',
      );
      expect(message.includes('The deploy token needs R2 write (Workers R2 Storage: Edit).')).toBe(namesPermission);
      // The record bucket is the one thing that exists; nothing the stage would use does.
      expect(cloudflare.mutations).toEqual(['create-r2:smoo-stage-records']);
      expect(runner.calls).toEqual([]);
    }
  });

  it('does not name R2 write when the zone listing behind a record is what was refused', async () => {
    const root = await fixtureRoot(`${FIXTURE}
[[env.staging.routes]]
pattern = "site.staging.example.test/*"
`);
    const cloudflare = new FakeCloudflare();
    cloudflare.listZones = async () => {
      throw new CloudflareApiError('Cloudflare API /zones failed: refused', 403, [10000]);
    };

    const error = await deployStage(
      root,
      { stage: 'pr123', repositoryRoot: root },
      dependencies(new FakeRunner([], {}), cloudflare),
    ).catch((thrown: unknown) => thrown);

    expect(error instanceof Error ? error.message : '').toBe(
      'Recording pr123 in R2 bucket smoo-stage-records failed, so nothing was created: Cloudflare API /zones failed: refused',
    );
    expect(cloudflare.mutations).toEqual([]);
  });

  it('refuses a pull-request stage whose root manifest names no repository, before any Cloudflare call', async () => {
    const root = await fixtureRoot();
    await writeFile(join(root, 'package.json'), '{ "name": "@acme/app" }\n');
    const touched: string[] = [];
    const cloudflare = new Proxy(new FakeCloudflare(), {
      get(target, property, receiver) {
        touched.push(String(property));
        return Reflect.get(target, property, receiver);
      },
    });

    await expect(
      deployStage(root, { stage: 'pr123', repositoryRoot: root }, dependencies(new FakeRunner([], {}), cloudflare)),
    ).rejects.toThrow(`${join(root, 'package.json')} declares no repository`);
    expect(touched).toEqual([]);

    // Staging and production are never recorded, so they never read the root manifest.
    const staging = await deployStage(
      root,
      { stage: 'staging', repositoryRoot: root },
      dependencies(new FakeRunner([], {}), new FakeCloudflare()),
    );
    expect(staging.action).toBe('deployed');
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

/**
 * A project directory that is also its repository's root: `nx.json`, and a root manifest naming the
 * repository a pull-request stage is recorded under.
 */
async function fixtureRoot(config = FIXTURE, file = 'wrangler.toml'): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-wrangler-test-'));
  roots.push(root);
  await writeRepositoryRoot(root);
  await writeFile(join(root, file), config);
  return root;
}

async function writeRepositoryRoot(root: string, manifest: Record<string, unknown> = {}): Promise<void> {
  await writeFile(join(root, 'nx.json'), '{}\n');
  await writeFile(
    join(root, 'package.json'),
    `${JSON.stringify({ name: '@acme/app', repository: { type: 'git', url: 'https://github.com/acme/app.git' }, ...manifest }, null, 2)}\n`,
  );
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
