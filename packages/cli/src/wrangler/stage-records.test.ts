import { afterEach, describe, expect, it } from 'bun:test';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { CloudflareZone } from './cloudflare.js';
import { parseWranglerConfig } from './source-config.js';
import { type PullRequestResourcePlan, planPullRequestResources } from './stage.js';
import {
  parseStageRecordKey,
  plannedStageRecords,
  type StageRecord,
  stageRecordKey,
  stageRecordKeys,
  stageRecordPrefix,
  stageRecordScope,
} from './stage-records.js';

const SCOPE = 'github.com/acme/app';
const PREFIX = 'v1/github.com%2Facme%2Fapp/pr7/';

const roots: string[] = [];

afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

/** A repository root: `nx.json` unless told otherwise, and a root manifest carrying `repository` when given one. */
async function repositoryRoot(repository: unknown, options: { nx?: boolean } = {}): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-stage-records-'));
  roots.push(root);
  if (options.nx !== false) await writeFile(join(root, 'nx.json'), '{}\n');
  const manifest = repository === undefined ? { name: '@acme/app' } : { name: '@acme/app', repository };
  await writeFile(join(root, 'package.json'), `${JSON.stringify(manifest)}\n`);
  return root;
}

function plan(routes: PullRequestResourcePlan['routes']): PullRequestResourcePlan {
  return {
    stage: 'pr7',
    workerName: 'web-pr7',
    workerBaseName: 'web',
    kvNamespaces: [],
    d1Databases: [],
    r2Buckets: [],
    routes,
  };
}

/** A zone listing that counts how often it was asked. */
function zoneListing(zones: CloudflareZone[]): { list: () => Promise<CloudflareZone[]>; calls: () => number } {
  let calls = 0;
  return {
    list: async () => {
      calls += 1;
      return zones;
    },
    calls: () => calls,
  };
}

describe('stage record keys', () => {
  const records: StageRecord[] = [
    { kind: 'worker', worker: 'web-pr7' },
    { kind: 'kv', worker: 'web-pr7', title: 'web-sessions-pr7' },
    { kind: 'd1', worker: 'web-pr7', name: 'web-pr7-db' },
    { kind: 'r2', worker: 'web-pr7', bucket: 'web-media-pr7' },
    { kind: 'domain', worker: 'web-pr7', hostname: 'pr7.example.com' },
    { kind: 'route', worker: 'web-pr7', zone: 'example.com', pattern: 'web.pr7.example.com/100%/*' },
    { kind: 'dns', worker: 'web-pr7', zone: 'example.com', name: '*.pr7.example.com' },
  ];

  it('round-trips every kind of record', () => {
    for (const record of records) {
      expect(parseStageRecordKey(SCOPE, 'pr7', stageRecordKey(SCOPE, 'pr7', record))).toEqual(record);
    }
  });

  it('encodes every field, so a pattern carrying / * and % stays one segment', () => {
    const route: StageRecord = {
      kind: 'route',
      worker: 'web-pr7',
      zone: 'example.com',
      pattern: 'web.pr7.example.com/100%/*',
    };
    expect(stageRecordKey(SCOPE, 'pr7', route)).toBe(
      `${PREFIX}web-pr7/route/example.com/web.pr7.example.com%2F100%25%2F*`,
    );
    expect(stageRecordKey(SCOPE, 'pr7', { kind: 'worker', worker: 'web-pr7' })).toBe(`${PREFIX}web-pr7/worker`);
  });

  it('refuses a key outside the grammar', () => {
    for (const key of [
      'v2/github.com%2Facme%2Fapp/pr7/web-pr7/worker',
      `${PREFIX}web-pr7/worker/extra`,
      `${PREFIX}web-pr7/kv`,
      `${PREFIX}web-pr7/kv/`,
      `${PREFIX}web-pr7/queue/web-jobs-pr7`,
      `${PREFIX}web-pr7/route/example.com`,
      `${PREFIX}web-pr7/dns/example.com/%2A.pr7.example.com`,
      `${PREFIX}web-pr7/kv/web-sessions-pr7%`,
      'v1/github.com%2Facme%2Fother/pr7/web-pr7/worker',
    ]) {
      expect(() => parseStageRecordKey(SCOPE, 'pr7', key)).toThrow(key);
    }
  });

  it("refuses, on write and on read, a record that is not the stage's own", () => {
    const foreign: StageRecord[] = [
      { kind: 'worker', worker: 'web-pr8' },
      { kind: 'worker', worker: 'web-staging' },
      { kind: 'dns', worker: 'web-pr7', zone: 'example.com', name: '*.example.com' },
      { kind: 'r2', worker: 'web-pr7', bucket: 'smoo-stage-records' },
      { kind: 'kv', worker: 'web-pr7', title: 'web-sessions-pr70' },
      { kind: 'd1', worker: 'web-pr7', name: 'web-staging-db' },
      { kind: 'domain', worker: 'web-pr7', hostname: 'next.example.com' },
    ];
    const keys = [
      `${PREFIX}web-pr8/worker`,
      `${PREFIX}web-staging/worker`,
      `${PREFIX}web-pr7/dns/example.com/*.example.com`,
      `${PREFIX}web-pr7/r2/smoo-stage-records`,
      `${PREFIX}web-pr7/kv/web-sessions-pr70`,
      `${PREFIX}web-pr7/d1/web-staging-db`,
      `${PREFIX}web-pr7/domain/next.example.com`,
    ];
    for (const record of foreign) {
      expect(() => stageRecordKey(SCOPE, 'pr7', record)).toThrow(/pr7/);
    }
    for (const key of keys) {
      expect(() => parseStageRecordKey(SCOPE, 'pr7', key)).toThrow(/pr7/);
    }
  });

  it('lists a stage under a prefix no other stage number shares', () => {
    expect(stageRecordPrefix(SCOPE, 'pr7')).toBe(PREFIX);
    expect(stageRecordKey(SCOPE, 'pr70', { kind: 'worker', worker: 'web-pr70' }).startsWith(PREFIX)).toBe(false);
  });

  it('refuses a key past the 1024 bytes R2 allows', () => {
    const pattern = `web.pr7.example.com/${'a'.repeat(1000)}`;
    expect(() =>
      stageRecordKey(SCOPE, 'pr7', { kind: 'route', worker: 'web-pr7', zone: 'example.com', pattern }),
    ).toThrow(/1024 bytes/);
  });

  it('writes each distinct key once', () => {
    const dns: StageRecord = { kind: 'dns', worker: 'web-pr7', zone: 'example.com', name: '*.pr7.example.com' };
    expect(stageRecordKeys(SCOPE, 'pr7', [{ kind: 'worker', worker: 'web-pr7' }, dns, { ...dns }])).toEqual([
      `${PREFIX}web-pr7/worker`,
      `${PREFIX}web-pr7/dns/example.com/*.pr7.example.com`,
    ]);
  });
});

describe('stageRecordScope', () => {
  it('names the repository by host and path, whatever form package.json writes it in', async () => {
    const forms: Array<[unknown, string]> = [
      ['https://github.com/Acme/App.git', 'github.com/acme/app'],
      [{ type: 'git', url: 'git+https://github.com/acme/app.git' }, 'github.com/acme/app'],
      ['git@github.com:acme/app.git', 'github.com/acme/app'],
      ['ssh://git@git.example.com:2222/Acme/App.git', 'git.example.com/acme/app'],
      [{ url: 'git+ssh://git@github.com/acme/app.git' }, 'github.com/acme/app'],
      ['acme/app', 'github.com/acme/app'],
      ['github:acme/app', 'github.com/acme/app'],
      ['gitlab:acme/app', 'gitlab.com/acme/app'],
      ['bitbucket:acme/app', 'bitbucket.org/acme/app'],
      ['https://code.example.com/forgejo/acme/app/#main', 'code.example.com/forgejo/acme/app'],
    ];
    for (const [repository, scope] of forms) {
      expect(stageRecordScope(await repositoryRoot(repository), {})).toBe(scope);
    }
  });

  it('refuses a root package.json without a repository, naming the file', async () => {
    const root = await repositoryRoot(undefined);
    expect(() => stageRecordScope(root, {})).toThrow(`${join(root, 'package.json')} declares no repository`);
  });

  it('refuses a repository that names no owner and repository', async () => {
    const root = await repositoryRoot('https://github.com/acme');
    expect(() => stageRecordScope(root, {})).toThrow(join(root, 'package.json'));
  });

  it('refuses a directory that is not the Nx workspace root', async () => {
    const root = await repositoryRoot('https://github.com/acme/app', { nx: false });
    expect(() => stageRecordScope(root, {})).toThrow(`${root} has no nx.json`);
  });

  it('refuses in CI when the repository CI runs for is another one', async () => {
    const root = await repositoryRoot('https://github.com/acme/app');
    expect(() => stageRecordScope(root, { GITHUB_REPOSITORY: 'fork/app' })).toThrow(
      `repository.url in ${join(root, 'package.json')} names acme/app, but CI runs for fork/app`,
    );
    expect(stageRecordScope(root, { GITHUB_REPOSITORY: 'Acme/App' })).toBe('github.com/acme/app');
  });
});

describe('plannedStageRecords', () => {
  const TOML = `[env.staging]
name = "web-staging"

[[env.staging.routes]]
pattern = "*.staging.example.com/*"
zone_name = "example.com"

[[env.staging.routes]]
pattern = "staging.example.com"
custom_domain = true

[[env.staging.routes]]
pattern = "api.staging.example.com/*"
zone_id = "zone-apex"

[[env.staging.routes]]
pattern = "next.example.com"
custom_domain = true

[[env.staging.kv_namespaces]]
binding = "SESSIONS"
id = "kv-sessions-staging"

[[env.staging.d1_databases]]
binding = "DB"
database_name = "web-staging-db"
database_id = "d1-staging"

[[env.staging.r2_buckets]]
binding = "MEDIA"
bucket_name = "web-media-staging"
`;

  it('records every item the plan derives, and nothing it pins', async () => {
    const resources = planPullRequestResources(parseWranglerConfig('wrangler.toml', TOML, 'toml'), 'pr7', [
      { id: 'kv-sessions-staging', title: 'web-sessions-staging' },
    ]);
    // The more specific zone would win for `api.pr7.example.com` if the plan dropped its `zone_id`.
    const zones = zoneListing([
      { id: 'zone-apex', name: 'example.com' },
      { id: 'zone-sub', name: 'pr7.example.com' },
    ]);

    expect(await plannedStageRecords(resources, zones.list)).toEqual([
      { kind: 'worker', worker: 'web-pr7' },
      { kind: 'kv', worker: 'web-pr7', title: 'web-sessions-pr7' },
      { kind: 'd1', worker: 'web-pr7', name: 'web-pr7-db' },
      { kind: 'r2', worker: 'web-pr7', bucket: 'web-media-pr7' },
      { kind: 'route', worker: 'web-pr7', zone: 'example.com', pattern: '*.pr7.example.com/*' },
      { kind: 'dns', worker: 'web-pr7', zone: 'example.com', name: '*.pr7.example.com' },
      { kind: 'domain', worker: 'web-pr7', hostname: 'pr7.example.com' },
      { kind: 'route', worker: 'web-pr7', zone: 'example.com', pattern: 'api.pr7.example.com/*' },
    ]);
    expect(zones.calls()).toBe(1);
  });

  it('gives two wildcard routes on one host one DNS key', async () => {
    const records = await plannedStageRecords(
      plan([
        { pattern: '*.pr7.example.com/*', zoneName: 'example.com', customDomain: false },
        { pattern: '*.pr7.example.com/api/*', zoneName: 'example.com', customDomain: false },
      ]),
      zoneListing([]).list,
    );

    expect(stageRecordKeys(SCOPE, 'pr7', records).filter((key) => key.includes('/dns/'))).toEqual([
      `${PREFIX}web-pr7/dns/example.com/*.pr7.example.com`,
    ]);
  });

  describe('the zone a route is recorded under', () => {
    const zones: CloudflareZone[] = [
      { id: 'zone-apex', name: 'example.com' },
      { id: 'zone-sub', name: 'pr7.example.com' },
    ];

    async function routeZone(route: PullRequestResourcePlan['routes'][number]): Promise<string | undefined> {
      const records = await plannedStageRecords(plan([route]), zoneListing(zones).list);
      const recorded = records.find((record) => record.kind === 'route');
      return recorded?.kind === 'route' ? recorded.zone : undefined;
    }

    it('is the declared zone_name, lowercased, without listing zones', async () => {
      const listing = zoneListing(zones);
      const records = await plannedStageRecords(
        plan([{ pattern: 'api.pr7.example.com/*', zoneName: 'Example.com', zoneId: 'zone-sub', customDomain: false }]),
        listing.list,
      );
      expect(records[1]).toEqual({
        kind: 'route',
        worker: 'web-pr7',
        zone: 'example.com',
        pattern: 'api.pr7.example.com/*',
      });
      expect(listing.calls()).toBe(0);
    });

    it('is the zone zone_id names, even when a more specific zone contains the host', async () => {
      expect(await routeZone({ pattern: 'api.pr7.example.com/*', zoneId: 'zone-apex', customDomain: false })).toBe(
        'example.com',
      );
    });

    it('is otherwise the most specific account zone containing the host', async () => {
      expect(await routeZone({ pattern: 'API.pr7.example.com/*', customDomain: false })).toBe('pr7.example.com');
    });

    it('refuses a route no account zone contains, asking for zone_name', async () => {
      await expect(routeZone({ pattern: 'api.pr7.example.org/*', customDomain: false })).rejects.toThrow(/zone_name/);
      await expect(
        routeZone({ pattern: 'api.pr7.example.com/*', zoneId: 'zone-gone', customDomain: false }),
      ).rejects.toThrow(/zone_name/);
    });

    it('lists the zones at most once per plan', async () => {
      const listing = zoneListing(zones);
      await plannedStageRecords(
        plan([
          { pattern: 'api.pr7.example.com/*', customDomain: false },
          { pattern: 'web.pr7.example.com/*', customDomain: false },
        ]),
        listing.list,
      );
      expect(listing.calls()).toBe(1);
    });
  });
});
