// One worker, written twice: as `wrangler.toml` and as `wrangler.jsonc`. Everything a deploy
// derives from it — the parsed model, every stage plan, the derived pull-request document and
// every refusal — has to come out deep-equal, because past the parser there is one model and one
// implementation. A divergence here means a repo that renamed its config deploys something else.

import { describe, expect, it } from 'bun:test';
import { parseWranglerConfig } from './source-config.js';
import {
  derivePullRequestDocument,
  type LiveKvNamespace,
  planConfiguredStageResources,
  planPullRequestResources,
  rateLimitNamespaceId,
  type WranglerDocument,
} from './stage.js';

const TOML = `name = "acme-site"
main = "src/index.ts"
compatibility_date = "2026-05-06"
compatibility_flags = ["nodejs_compat"]

[observability]
enabled = true

[[migrations]]
tag = "v1"
new_sqlite_classes = ["QueueDO"]

[env.staging]
name = "acme-site-staging"
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

[[env.staging.durable_objects.bindings]]
name = "QUEUE"
class_name = "QueueDO"

[[env.staging.kv_namespaces]]
binding = "SESSIONS"
id = "kv-sessions-staging"
preview_id = "kv-sessions-preview"

[[env.staging.r2_buckets]]
binding = "MEDIA"
bucket_name = "acme-media-staging"

[[env.staging.d1_databases]]
binding = "DB"
database_name = "acme-staging-db"
database_id = "d1-staging"
migrations_dir = "./migrations"

[[env.staging.services]]
binding = "BACKEND"
service = "acme-backend-staging"

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

[env.staging.vars]
ENVIRONMENT = "staging"
APP_ORIGIN = "https://app.staging.example.test"
BUILD_CHANNEL = "staging-weekly"

[env.production]
name = "acme-site"
workers_dev = false

[[env.production.routes]]
pattern = "example.test"
custom_domain = true

[[env.production.kv_namespaces]]
binding = "SESSIONS"
id = "kv-sessions-production"

[[env.production.r2_buckets]]
binding = "MEDIA"
bucket_name = "acme-media"

[env.production.vars]
ENVIRONMENT = "production"
`;

// The same worker in the format Cloudflare now recommends, with the comments and trailing commas
// that are the whole reason for the `.jsonc` spelling.
const JSONC = `{
  // The worker every stage of this repo deploys.
  "name": "acme-site",
  "main": "src/index.ts",
  "compatibility_date": "2026-05-06",
  "compatibility_flags": ["nodejs_compat"],
  "observability": { "enabled": true },
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["QueueDO"] }],
  "env": {
    "staging": {
      "name": "acme-site-staging",
      "workers_dev": false,
      "assets": { "directory": "./dist", "not_found_handling": "single-page-application" },
      "routes": [
        { "pattern": "*.staging.example.test/*", "zone_name": "example.test" },
        { "pattern": "staging.example.test", "custom_domain": true },
        /* pinned: this one has no stage label and is dropped from a pull-request stage */
        { "pattern": "next.example.test", "custom_domain": true },
      ],
      "durable_objects": { "bindings": [{ "name": "QUEUE", "class_name": "QueueDO" }] },
      "kv_namespaces": [
        { "binding": "SESSIONS", "id": "kv-sessions-staging", "preview_id": "kv-sessions-preview" },
      ],
      "r2_buckets": [{ "binding": "MEDIA", "bucket_name": "acme-media-staging" }],
      "d1_databases": [
        {
          "binding": "DB",
          "database_name": "acme-staging-db",
          "database_id": "d1-staging",
          "migrations_dir": "./migrations",
        },
      ],
      "services": [
        { "binding": "BACKEND", "service": "acme-backend-staging" },
        { "binding": "SHARED", "service": "auth-service" }, // unlabelled: stays shared
      ],
      "send_email": [{ "name": "EMAIL", "allowed_sender_addresses": ["login@mail.staging.example.test"] }],
      "ratelimits": [{ "name": "SIGNUP", "namespace_id": "2026071701", "simple": { "limit": 5, "period": 60 } }],
      "vars": {
        "ENVIRONMENT": "staging",
        "APP_ORIGIN": "https://app.staging.example.test",
        "BUILD_CHANNEL": "staging-weekly",
      },
    },
    "production": {
      "name": "acme-site",
      "workers_dev": false,
      "routes": [{ "pattern": "example.test", "custom_domain": true }],
      "kv_namespaces": [{ "binding": "SESSIONS", "id": "kv-sessions-production" }],
      "r2_buckets": [{ "binding": "MEDIA", "bucket_name": "acme-media" }],
      "vars": { "ENVIRONMENT": "production" },
    },
  },
}
`;

const LIVE_NAMESPACES: LiveKvNamespace[] = [{ id: 'kv-sessions-staging', title: 'acme-sessions-staging' }];

const PR42 = {
  stage: 'pr42',
  accountId: 'account-1',
  kvNamespaceIds: new Map([['kv-sessions-staging', 'kv-sessions-pr42']]),
  d1DatabaseIds: new Map([['d1-staging', 'd1-pr42']]),
} as const;

const fromToml = parseWranglerConfig('wrangler.toml', TOML, 'toml');
const fromJsonc = parseWranglerConfig('wrangler.jsonc', JSONC, 'jsonc');

/**
 * What `act` refused with, for each spelling. Returning the message rather than asserting inside
 * keeps the two halves comparable: a refusal only reaches parity when both sides produce the same
 * sentence, not merely when both throw something.
 */
function refusals(
  toml: string,
  jsonc: string,
  act: (document: WranglerDocument) => unknown,
): { toml: string; jsonc: string } {
  const message = (text: string, file: 'wrangler.toml' | 'wrangler.jsonc'): string => {
    try {
      act(parseWranglerConfig(file, text, file === 'wrangler.toml' ? 'toml' : 'jsonc'));
    } catch (error) {
      return error instanceof Error ? error.message : String(error);
    }
    return 'did not refuse';
  };
  return { toml: message(toml, 'wrangler.toml'), jsonc: message(jsonc, 'wrangler.jsonc') };
}

describe('a worker written as TOML and as JSONC', () => {
  it('parses to one model, comments and trailing commas included', () => {
    expect(fromJsonc).toEqual(fromToml);
  });

  it('plans staging identically', () => {
    const plan = planConfiguredStageResources(fromToml, 'staging');
    expect(planConfiguredStageResources(fromJsonc, 'staging')).toEqual(plan);
    expect(plan).toEqual({
      stage: 'staging',
      workerName: 'acme-site-staging',
      kvNamespaces: [{ binding: 'SESSIONS', id: 'kv-sessions-staging' }],
      r2Buckets: [{ binding: 'MEDIA', bucketName: 'acme-media-staging' }],
      routes: [
        { pattern: '*.staging.example.test/*', zoneName: 'example.test', customDomain: false },
        { pattern: 'staging.example.test', customDomain: true },
        { pattern: 'next.example.test', customDomain: true },
      ],
    });
  });

  it('plans production identically', () => {
    const plan = planConfiguredStageResources(fromToml, 'production');
    expect(planConfiguredStageResources(fromJsonc, 'production')).toEqual(plan);
    expect(plan).toEqual({
      stage: 'production',
      workerName: 'acme-site',
      kvNamespaces: [{ binding: 'SESSIONS', id: 'kv-sessions-production' }],
      r2Buckets: [{ binding: 'MEDIA', bucketName: 'acme-media' }],
      routes: [{ pattern: 'example.test', customDomain: true }],
    });
  });

  it('plans a pull-request stage identically, down to the resources it would create', () => {
    const plan = planPullRequestResources(fromToml, 'pr42', LIVE_NAMESPACES);
    expect(planPullRequestResources(fromJsonc, 'pr42', LIVE_NAMESPACES)).toEqual(plan);
    expect(plan).toEqual({
      stage: 'pr42',
      workerName: 'acme-site-pr42',
      workerBaseName: 'acme-site',
      kvNamespaces: [
        {
          binding: 'SESSIONS',
          stagingId: 'kv-sessions-staging',
          stagingTitle: 'acme-sessions-staging',
          title: 'acme-sessions-pr42',
        },
      ],
      d1Databases: [{ binding: 'DB', stagingId: 'd1-staging', name: 'acme-pr42-db' }],
      r2Buckets: [{ binding: 'MEDIA', bucketName: 'acme-media-pr42' }],
      // `staging.example.test` carries the label too, so the stage gets its own custom domain;
      // only the pinned `next.example.test` is dropped.
      routes: [
        { pattern: '*.pr42.example.test/*', zoneName: 'example.test', customDomain: false },
        { pattern: 'pr42.example.test', customDomain: true },
      ],
    });
  });

  it('derives the same pull-request document, carrying keys it does not model', () => {
    const derived = derivePullRequestDocument(fromToml, PR42);
    expect(derivePullRequestDocument(fromJsonc, PR42)).toEqual(derived);
    expect(derived.env?.pr42).toEqual({
      name: 'acme-site-pr42',
      workers_dev: false,
      assets: { directory: './dist', not_found_handling: 'single-page-application' },
      routes: [
        { pattern: '*.pr42.example.test/*', zone_name: 'example.test' },
        { pattern: 'pr42.example.test', custom_domain: true },
      ],
      durable_objects: { bindings: [{ name: 'QUEUE', class_name: 'QueueDO' }] },
      // `preview_id` is not a field this derivation models, and it survives anyway.
      kv_namespaces: [{ binding: 'SESSIONS', id: 'kv-sessions-pr42', preview_id: 'kv-sessions-preview' }],
      r2_buckets: [{ binding: 'MEDIA', bucket_name: 'acme-media-pr42' }],
      d1_databases: [
        {
          binding: 'DB',
          database_name: 'acme-pr42-db',
          database_id: 'd1-pr42',
          migrations_dir: './migrations',
        },
      ],
      services: [
        { binding: 'BACKEND', service: 'acme-backend-pr42' },
        { binding: 'SHARED', service: 'auth-service' },
      ],
      send_email: [{ name: 'EMAIL', allowed_sender_addresses: ['login@mail.pr42.example.test'] }],
      ratelimits: [
        {
          name: 'SIGNUP',
          namespace_id: rateLimitNamespaceId('account-1', 'acme-site', 'pr42', 'SIGNUP'),
          simple: { limit: 5, period: 60 },
        },
      ],
      vars: {
        ENVIRONMENT: 'pr42',
        APP_ORIGIN: 'https://app.pr42.example.test',
        BUILD_CHANNEL: 'staging-weekly',
      },
    });
    // Top-level declarations and the block the stage was derived from are left exactly as read.
    expect(derived.migrations).toEqual([{ tag: 'v1', new_sqlite_classes: ['QueueDO'] }]);
    expect(derived.observability).toEqual({ enabled: true });
    expect(derived.env?.staging).toEqual(fromToml.env?.staging);
    expect(derived.env?.production).toEqual(fromToml.env?.production);
  });

  it('leaves the parsed source document untouched by a derivation', () => {
    const before = structuredClone(fromToml);
    derivePullRequestDocument(fromToml, PR42);
    expect(fromToml).toEqual(before);
  });
});

describe('a refusal reads the same whichever format declared it', () => {
  it('refuses a template whose every route is pinned', () => {
    const { toml, jsonc } = refusals(
      `[env.staging]
name = "acme-site-staging"
[[env.staging.routes]]
pattern = "next.example.test"
custom_domain = true
`,
      `{
  // no stage-derivable route anywhere
  "env": {
    "staging": {
      "name": "acme-site-staging",
      "routes": [{ "pattern": "next.example.test", "custom_domain": true }],
    },
  },
}`,
      (document) => planPullRequestResources(document, 'pr42', []),
    );
    expect(jsonc).toBe(toml);
    expect(toml).toBe(
      'Every route of acme-site-staging is pinned (no staging label): next.example.test. A pull-request stage would deploy unrouted and Wrangler would expose it on workers.dev; add a stage-derivable route such as site.staging.<zone>/*.',
    );
  });

  it('refuses an R2 bucket the pull-request stage would share with staging', () => {
    const { toml, jsonc } = refusals(
      `[env.staging]
name = "acme-site-staging"
[[env.staging.routes]]
pattern = "site.staging.example.test/*"
[[env.staging.r2_buckets]]
binding = "MEDIA"
bucket_name = "acme-shared-media"
`,
      `{
  "env": {
    "staging": {
      "name": "acme-site-staging",
      "routes": [{ "pattern": "site.staging.example.test/*" }],
      "r2_buckets": [{ "binding": "MEDIA", "bucket_name": "acme-shared-media" }], // no staging segment
    },
  },
}`,
      (document) => planPullRequestResources(document, 'pr42', []),
    );
    expect(jsonc).toBe(toml);
    expect(toml).toBe('Staging R2 bucket acme-shared-media has no exact staging segment.');
  });

  it('refuses a D1 database the pull-request stage would share with staging', () => {
    const { toml, jsonc } = refusals(
      `[env.staging]
name = "acme-site-staging"
[[env.staging.routes]]
pattern = "site.staging.example.test/*"
[[env.staging.d1_databases]]
binding = "DB"
database_name = "acme-shared-db"
database_id = "d1-shared"
`,
      `{
  "env": {
    "staging": {
      "name": "acme-site-staging",
      "routes": [{ "pattern": "site.staging.example.test/*" }],
      "d1_databases": [{ "binding": "DB", "database_name": "acme-shared-db", "database_id": "d1-shared" }],
    },
  },
}`,
      (document) => planPullRequestResources(document, 'pr42', []),
    );
    expect(jsonc).toBe(toml);
    expect(toml).toBe('Staging D1 database acme-shared-db has no exact staging segment.');
  });

  it('refuses a stage the configuration does not declare', () => {
    const { toml, jsonc } = refusals(
      `name = "acme-site"
[env.staging]
name = "acme-site-staging"
`,
      `{
  "name": "acme-site",
  "env": { "staging": { "name": "acme-site-staging" } },
}`,
      (document) => planConfiguredStageResources(document, 'production'),
    );
    expect(jsonc).toBe(toml);
    expect(toml).toBe('Wrangler configuration must declare [env.production].');
  });

  it('refuses a pull-request stage the repo committed itself, rather than replacing it', () => {
    const { toml, jsonc } = refusals(
      `[env.staging]
name = "acme-site-staging"
[[env.staging.routes]]
pattern = "site.staging.example.test/*"
[env.pr42]
name = "hand-written"
`,
      `{
  "env": {
    "staging": {
      "name": "acme-site-staging",
      "routes": [{ "pattern": "site.staging.example.test/*" }],
    },
    "pr42": { "name": "hand-written" },
  },
}`,
      (document) => derivePullRequestDocument(document, PR42),
    );
    expect(jsonc).toBe(toml);
    expect(toml).toBe(
      'Wrangler configuration already declares [env.pr42]; a pull-request stage is derived from [env.staging] and must not be committed.',
    );
  });
});
