import { describe, expect, it } from 'bun:test';
import { parseWranglerConfig } from './source-config.js';
import {
  derivePullRequestDocument,
  derivePullRequestStageConfig,
  isPullRequestStage,
  planPullRequestBindings,
  planPullRequestResources,
  pullRequestStage,
  rateLimitNamespaceId,
  stageDomain,
  stageResourceName,
  type WranglerDocument,
} from './stage.js';

/** The fixtures below are TOML; the derivation only ever sees the model a parser produces. */
function document(toml: string): WranglerDocument {
  return parseWranglerConfig('wrangler.toml', toml, 'toml');
}

const APP_FIXTURE = `name = "app"
compatibility_date = "2026-05-06"

[env.staging]
name = "app-staging"
workers_dev = false

[env.staging.assets]
directory = "./dist"
not_found_handling = "single-page-application"

[[env.staging.routes]]
pattern = "*.staging.example.test/*"
zone_name = "example.test"

[[env.staging.routes]]
pattern = "staging.example.test"
custom_domain = true

[[env.staging.routes]]
pattern = "next.example.test"
custom_domain = true
`;

const BACKEND_FIXTURE = `name = "app-backend"
main = "dist/worker.js"

[[migrations]]
tag = "v1"
new_sqlite_classes = ["QueueDO", "StoreDO"]

[env.staging]
name = "app-backend-staging"
workers_dev = false

[[env.staging.routes]]
pattern = "*.staging.example.test/auth/*"
zone_name = "example.test"

[[env.staging.durable_objects.bindings]]
name = "QUEUE"
class_name = "QueueDO"

[[env.staging.kv_namespaces]]
binding = "ALIAS_INDEX"
id = "kv-alias-staging-id"

[[env.staging.kv_namespaces]]
binding = "ORG_PROFILES"
id = "kv-org-staging-id"

[[env.staging.d1_databases]]
binding = "DB"
database_name = "app-staging-db"
database_id = "d1-staging-id"

[[env.staging.services]]
binding = "BACKEND"
service = "app-backend-staging"

[[env.staging.services]]
binding = "SHARED"
service = "auth-service"

[[env.staging.send_email]]
name = "EMAIL"
allowed_sender_addresses = ["login@mail.staging.example.test"]

[[env.staging.ratelimits]]
name = "SIGNUP"
namespace_id = "2026071701"
simple = { limit = 5, period = 60 }

[[env.staging.ratelimits]]
name = "LOGIN"
namespace_id = "2026071702"
simple = { limit = 20, period = 60 }

[[env.staging.r2_buckets]]
binding = "MEDIA"
bucket_name = "app-media-staging"

[env.staging.vars]
ENVIRONMENT = "staging"
TLD_DOMAIN = "staging.example.test"
BUILD_ID = "staging-20260716-2"
MAIL_RECIPIENTS = "login-test@staging.example.test,invite-test@staging.example.test"
EMAIL_FROM_ADDRESS = "login@mail.staging.example.test"
APP_ORIGIN = "https://app.staging.example.test"
`;

const LIVE_NAMESPACES = [
  { id: 'kv-alias-staging-id', title: 'alias-index-staging' },
  { id: 'kv-org-staging-id', title: 'org-profiles-staging' },
];

const DERIVED_IDS = new Map([
  ['kv-alias-staging-id', 'kv-alias-pr123-id'],
  ['kv-org-staging-id', 'kv-org-pr123-id'],
]);

const DERIVED_D1 = new Map([['d1-staging-id', 'd1-pr123-id']]);

describe('Wrangler deployment stage convention', () => {
  it('validates pull-request numbers and derives generic names', () => {
    expect(pullRequestStage(123)).toBe('pr123');
    expect(() => pullRequestStage(0)).toThrow(/1 through 999999999/);
    expect(() => pullRequestStage(1.5)).toThrow(/integer/);
    expect(() => pullRequestStage(1_000_000_000)).toThrow(/1 through 999999999/);
    expect(stageDomain('pr123', 'example.test')).toBe('pr123.example.test');
    expect(stageDomain('production', 'example.test')).toBe('example.test');
    expect(stageResourceName('app', 'staging')).toBe('app-staging');
    expect(stageResourceName('app', 'production')).toBe('app');
  });

  it('counts only numbered pr stages as pull requests, not production', () => {
    expect(isPullRequestStage('pr123')).toBe(true);
    expect(isPullRequestStage('production')).toBe(false);
    expect(isPullRequestStage('staging')).toBe(false);
  });

  it('derives the app staging block without changing inherited/static semantics', () => {
    const source = document(APP_FIXTURE);
    const derived = derivePullRequestDocument(source, {
      stage: 'pr123',
      accountId: 'account-1',
      kvNamespaceIds: new Map<string, string>(),
      d1DatabaseIds: new Map<string, string>(),
    });

    expect(derived.env?.pr123).toEqual({
      name: 'app-pr123',
      workers_dev: false,
      assets: { directory: './dist', not_found_handling: 'single-page-application' },
      routes: [
        { pattern: '*.pr123.example.test/*', zone_name: 'example.test' },
        { pattern: 'pr123.example.test', custom_domain: true },
      ],
    });
    // Inherited top-level keys and the staging block the stage was derived from are untouched.
    expect(derived.compatibility_date).toBe('2026-05-06');
    expect(derived.env?.staging).toEqual(source.env?.staging);
    expect(JSON.stringify(derived)).not.toContain('pr456');
  });

  it('refuses an all-pinned template before any resource is named', () => {
    const pinned = document(`name = "app"
[env.staging]
name = "app-staging"
[[env.staging.routes]]
pattern = "next.example.test"
custom_domain = true
`);
    expect(() => planPullRequestResources(pinned, 'pr123', [])).toThrow(/pinned/);
    expect(() => planPullRequestResources(pinned, 'pr123', [])).toThrow(/next\.example\.test/);
  });

  it('derives backend resources from staging while preserving provider and DO identities', () => {
    const source = document(BACKEND_FIXTURE);
    const plan = planPullRequestResources(source, 'pr123', LIVE_NAMESPACES);
    expect(plan.workerName).toBe('app-backend-pr123');
    expect(plan.kvNamespaces.map(({ title }) => title)).toEqual(['alias-index-pr123', 'org-profiles-pr123']);
    expect(plan.d1Databases).toEqual([{ binding: 'DB', stagingId: 'd1-staging-id', name: 'app-pr123-db' }]);
    expect(plan.r2Buckets).toEqual([{ binding: 'MEDIA', bucketName: 'app-media-pr123' }]);

    const derived = derivePullRequestDocument(source, {
      stage: 'pr123',
      accountId: 'account-1',
      kvNamespaceIds: DERIVED_IDS,
      d1DatabaseIds: DERIVED_D1,
    });
    const signupId = rateLimitNamespaceId('account-1', 'app-backend', 'pr123', 'SIGNUP');
    const loginId = rateLimitNamespaceId('account-1', 'app-backend', 'pr123', 'LOGIN');

    expect(Number(signupId)).toBeGreaterThan(0);
    expect(Number(signupId)).toBeLessThanOrEqual(0x7fff_ffff);
    expect(loginId).not.toBe(signupId);
    expect(derived.env?.pr123).toEqual({
      name: 'app-backend-pr123',
      workers_dev: false,
      routes: [{ pattern: '*.pr123.example.test/auth/*', zone_name: 'example.test' }],
      durable_objects: { bindings: [{ name: 'QUEUE', class_name: 'QueueDO' }] },
      kv_namespaces: [
        { binding: 'ALIAS_INDEX', id: 'kv-alias-pr123-id' },
        { binding: 'ORG_PROFILES', id: 'kv-org-pr123-id' },
      ],
      d1_databases: [{ binding: 'DB', database_name: 'app-pr123-db', database_id: 'd1-pr123-id' }],
      services: [
        { binding: 'BACKEND', service: 'app-backend-pr123' },
        { binding: 'SHARED', service: 'auth-service' },
      ],
      send_email: [{ name: 'EMAIL', allowed_sender_addresses: ['login@mail.pr123.example.test'] }],
      ratelimits: [
        { name: 'SIGNUP', namespace_id: signupId, simple: { limit: 5, period: 60 } },
        { name: 'LOGIN', namespace_id: loginId, simple: { limit: 20, period: 60 } },
      ],
      r2_buckets: [{ binding: 'MEDIA', bucket_name: 'app-media-pr123' }],
      vars: {
        ENVIRONMENT: 'pr123',
        TLD_DOMAIN: 'pr123.example.test',
        BUILD_ID: 'staging-20260716-2',
        MAIL_RECIPIENTS: 'login-test@pr123.example.test,invite-test@pr123.example.test',
        EMAIL_FROM_ADDRESS: 'login@mail.pr123.example.test',
        APP_ORIGIN: 'https://app.pr123.example.test',
      },
    });
    // The Durable Object migrations are a top-level, stage-independent declaration.
    expect(derived.migrations).toEqual([{ tag: 'v1', new_sqlite_classes: ['QueueDO', 'StoreDO'] }]);
    expect(JSON.stringify(derived)).not.toContain('pr456');
  });

  it('refuses unlabelled mutable KV, R2 and D1 before a pull-request stage can share them', () => {
    expect(() =>
      planPullRequestBindings(
        {
          name: 'app-staging',
          routes: [{ pattern: 'site.staging.example.test/*' }],
          kv_namespaces: [{ binding: 'SESSION', id: 'kv-1' }],
        },
        'pr7',
        [{ id: 'kv-1', title: 'shared' }],
      ),
    ).toThrow(/no exact staging segment/);
    expect(() =>
      planPullRequestBindings(
        {
          name: 'app-staging',
          routes: [{ pattern: 'site.staging.example.test/*' }],
          r2_buckets: [{ binding: 'MEDIA', bucket_name: 'shared-media' }],
        },
        'pr7',
        [],
      ),
    ).toThrow(/no exact staging segment/);
    expect(() =>
      planPullRequestBindings(
        {
          name: 'app-staging',
          routes: [{ pattern: 'site.staging.example.test/*' }],
          d1_databases: [{ binding: 'DB', database_name: 'shared-db', database_id: 'd1-x' }],
        },
        'pr7',
        [],
      ),
    ).toThrow(/no exact staging segment/);
  });

  it('leaves an unlabelled service binding shared', () => {
    const derived = derivePullRequestStageConfig(
      {
        name: 'app-staging',
        services: [{ binding: 'SHARED', service: 'auth-service' }],
      },
      {
        stage: 'pr7',
        accountId: 'account-1',
        kvNamespaceIds: new Map(),
        d1DatabaseIds: new Map(),
      },
    );
    expect(derived.services).toEqual([{ binding: 'SHARED', service: 'auth-service' }]);
  });
});
