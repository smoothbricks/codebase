import { describe, expect, it } from 'bun:test';
import {
  derivePullRequestWranglerConfig,
  planPullRequestResources,
  pullRequestStage,
  rateLimitNamespaceId,
  stageDomain,
  stageResourceName,
} from './stage.js';

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
`;

const BACKEND_FIXTURE = `name = "app-backend"
main = "dist/worker.js"

[[migrations]]
tag = "v1"
new_sqlite_classes = ["AuthKeysDO", "SaasGitDO"]

[env.staging]
name = "app-backend-staging"
workers_dev = false

[[env.staging.routes]]
pattern = "*.staging.example.test/auth/*"
zone_name = "example.test"

[[env.staging.durable_objects.bindings]]
name = "AUTH_KEYS"
class_name = "AuthKeysDO"

[[env.staging.kv_namespaces]]
binding = "ALIAS_INDEX"
id = "kv-alias-staging-id"

[[env.staging.kv_namespaces]]
binding = "ORG_PROFILES"
id = "kv-org-staging-id"

[[env.staging.kv_namespaces]]
binding = "MAIL_CAPTURE"
id = "kv-mail-staging-id"

[[env.staging.send_email]]
name = "EMAIL"
allowed_sender_addresses = ["login@mail.staging.example.test"]

[[env.staging.ratelimits]]
name = "MAGIC_LINK_EMAIL_RATE_LIMIT"
namespace_id = "2026071701"
simple = { limit = 5, period = 60 }

[[env.staging.ratelimits]]
name = "MAGIC_LINK_SOURCE_RATE_LIMIT"
namespace_id = "2026071702"
simple = { limit = 20, period = 60 }

[[env.staging.r2_buckets]]
binding = "MEDIA"
bucket_name = "app-media-staging"

[env.staging.vars]
ENVIRONMENT = "staging"
TLD_DOMAIN = "staging.example.test"
AUTH_TLD_DOMAIN = "staging.example.test"
GITHUB_WEBHOOK_INGRESS_URL = "https://staging.example.test/webhooks/github"
GITHUB_APP_ID = "4077531"
GITHUB_APP_SLUG = "app-staging"
GITHUB_CLIENT_ID = "Iv23liD6EDsBZ8kJGU3f"
AUTH_KEYS_INSTANCE_NAME = "staging-20260716-2"
MAIL_CAPTURE_RECIPIENTS = "login-test@staging.example.test,invite-test@staging.example.test"
EMAIL_FROM_ADDRESS = "login@mail.staging.example.test"
INVITATION_REDEEM_ORIGIN = "https://app.staging.example.test"
`;

const LIVE_NAMESPACES = [
  { id: 'kv-alias-staging-id', title: 'alias-index-staging' },
  { id: 'kv-org-staging-id', title: 'org-profiles-staging' },
  { id: 'kv-mail-staging-id', title: 'mail-capture-staging' },
];

const DERIVED_IDS = new Map([
  ['kv-alias-staging-id', 'kv-alias-pr123-id'],
  ['kv-org-staging-id', 'kv-org-pr123-id'],
  ['kv-mail-staging-id', 'kv-mail-pr123-id'],
]);

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

  it('derives the app staging block without changing inherited/static semantics', () => {
    const derived = derivePullRequestWranglerConfig(APP_FIXTURE, {
      stage: 'pr123',
      accountId: 'account-1',
      kvNamespaceIds: new Map<string, string>(),
    });

    expect(derived).toContain('[env.pr123]\nname = "app-pr123"');
    expect(derived).toContain('pattern = "*.pr123.example.test/*"');
    expect(derived).toContain('pattern = "pr123.example.test"');
    expect(derived).toContain('[env.pr123.assets]\ndirectory = "./dist"');
    expect(derived).toContain('compatibility_date = "2026-05-06"');
    expect(derived).not.toContain('pr456');
  });

  it('derives backend resources from staging while preserving provider and DO identities', () => {
    const plan = planPullRequestResources(BACKEND_FIXTURE, 'pr123', LIVE_NAMESPACES);
    expect(plan.workerName).toBe('app-backend-pr123');
    expect(plan.kvNamespaces.map(({ title }) => title)).toEqual([
      'alias-index-pr123',
      'org-profiles-pr123',
      'mail-capture-pr123',
    ]);
    expect(plan.r2Buckets).toEqual([{ binding: 'MEDIA', bucketName: 'app-media-pr123' }]);

    const derived = derivePullRequestWranglerConfig(BACKEND_FIXTURE, {
      stage: 'pr123',
      accountId: 'account-1',
      kvNamespaceIds: DERIVED_IDS,
    });
    const emailId = rateLimitNamespaceId('account-1', 'app-backend', 'pr123', 'MAGIC_LINK_EMAIL_RATE_LIMIT');
    const sourceId = rateLimitNamespaceId('account-1', 'app-backend', 'pr123', 'MAGIC_LINK_SOURCE_RATE_LIMIT');

    expect(Number(emailId)).toBeGreaterThan(0);
    expect(Number(emailId)).toBeLessThanOrEqual(0x7fff_ffff);
    expect(sourceId).not.toBe(emailId);
    expect(derived).toContain(`namespace_id = "${emailId}"`);
    expect(derived).toContain(`namespace_id = "${sourceId}"`);
    expect(derived).toContain('id = "kv-alias-pr123-id"');
    expect(derived).toContain('id = "kv-org-pr123-id"');
    expect(derived).toContain('id = "kv-mail-pr123-id"');
    expect(derived).toContain('bucket_name = "app-media-pr123"');
    expect(derived).toContain('ENVIRONMENT = "pr123"');
    expect(derived).toContain('TLD_DOMAIN = "pr123.example.test"');
    expect(derived).toContain('AUTH_KEYS_INSTANCE_NAME = "pr123-20260716-2"');
    expect(derived).toContain('allowed_sender_addresses = ["login@mail.pr123.example.test"]');
    expect(derived).toContain(
      'MAIL_CAPTURE_RECIPIENTS = "login-test@pr123.example.test,invite-test@pr123.example.test"',
    );
    expect(derived).toContain('INVITATION_REDEEM_ORIGIN = "https://app.pr123.example.test"');
    expect(derived).toContain('GITHUB_APP_ID = "4077531"');
    expect(derived).toContain('GITHUB_APP_SLUG = "app-staging"');
    expect(derived).toContain('GITHUB_CLIENT_ID = "Iv23liD6EDsBZ8kJGU3f"');
    expect(derived).toContain('class_name = "AuthKeysDO"');
    expect(derived).toContain('new_sqlite_classes = ["AuthKeysDO", "SaasGitDO"]');
    expect(derived).not.toContain('pr456');
  });
});
