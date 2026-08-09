import { describe, expect, it } from 'bun:test';
import {
  derivePullRequestWranglerConfig,
  environmentDomain,
  environmentResourceName,
  planPullRequestResources,
  pullRequestEnvironment,
  rateLimitNamespaceId,
} from './environment.js';

const APP_FIXTURE = `name = "conloca-app"
compatibility_date = "2026-05-06"

[env.staging]
name = "conloca-app-staging"
workers_dev = false

[env.staging.assets]
directory = "./dist"
not_found_handling = "single-page-application"

[[env.staging.routes]]
pattern = "*.staging.conloca.com/*"
zone_name = "conloca.com"

[[env.staging.routes]]
pattern = "staging.conloca.com"
custom_domain = true
`;

const BACKEND_FIXTURE = `name = "conloca-app-backend"
main = "dist/worker.js"

[[migrations]]
tag = "v1"
new_sqlite_classes = ["ConlocaAuthKeysDO", "SaasGitDO"]

[env.staging]
name = "conloca-app-backend-staging"
workers_dev = false

[[env.staging.routes]]
pattern = "*.staging.conloca.com/auth/*"
zone_name = "conloca.com"

[[env.staging.durable_objects.bindings]]
name = "AUTH_KEYS"
class_name = "ConlocaAuthKeysDO"

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
allowed_sender_addresses = ["login@mail.staging.conloca.com"]

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
bucket_name = "conloca-media-staging"

[env.staging.vars]
ENVIRONMENT = "staging"
TLD_DOMAIN = "staging.conloca.com"
AUTH_TLD_DOMAIN = "staging.conloca.com"
GITHUB_WEBHOOK_INGRESS_URL = "https://staging.conloca.com/webhooks/github"
GITHUB_APP_ID = "4077531"
GITHUB_APP_SLUG = "conloca-staging"
GITHUB_CLIENT_ID = "Iv23liD6EDsBZ8kJGU3f"
AUTH_KEYS_INSTANCE_NAME = "staging-20260716-2"
MAIL_CAPTURE_RECIPIENTS = "login-test@staging.conloca.com,invite-test@staging.conloca.com"
EMAIL_FROM_ADDRESS = "login@mail.staging.conloca.com"
INVITATION_REDEEM_ORIGIN = "https://app.staging.conloca.com"
`;

const LIVE_NAMESPACES = [
  { id: 'kv-alias-staging-id', title: 'alias-index-staging' },
  { id: 'kv-org-staging-id', title: 'org-profiles-staging' },
  { id: 'kv-mail-staging-id', title: 'conloca-mail-capture-staging' },
];

const DERIVED_IDS = new Map([
  ['kv-alias-staging-id', 'kv-alias-pr123-id'],
  ['kv-org-staging-id', 'kv-org-pr123-id'],
  ['kv-mail-staging-id', 'kv-mail-pr123-id'],
]);

describe('Wrangler environment convention', () => {
  it('validates pull-request numbers and derives generic names', () => {
    expect(pullRequestEnvironment(123)).toBe('pr123');
    expect(() => pullRequestEnvironment(0)).toThrow(/1 through 999999999/);
    expect(() => pullRequestEnvironment(1.5)).toThrow(/integer/);
    expect(() => pullRequestEnvironment(1_000_000_000)).toThrow(/1 through 999999999/);
    expect(environmentDomain('pr123', 'conloca.com')).toBe('pr123.conloca.com');
    expect(environmentDomain('production', 'conloca.com')).toBe('conloca.com');
    expect(environmentResourceName('conloca-app', 'staging')).toBe('conloca-app-staging');
    expect(environmentResourceName('conloca-app', 'production')).toBe('conloca-app');
  });

  it('derives the app staging block without changing inherited/static semantics', () => {
    const derived = derivePullRequestWranglerConfig(APP_FIXTURE, {
      environment: 'pr123',
      accountId: 'account-1',
      kvNamespaceIds: new Map<string, string>(),
    });

    expect(derived).toContain('[env.pr123]\nname = "conloca-app-pr123"');
    expect(derived).toContain('pattern = "*.pr123.conloca.com/*"');
    expect(derived).toContain('pattern = "pr123.conloca.com"');
    expect(derived).toContain('[env.pr123.assets]\ndirectory = "./dist"');
    expect(derived).toContain('compatibility_date = "2026-05-06"');
    expect(derived).not.toContain('pr456');
  });

  it('derives backend resources from staging while preserving provider and DO identities', () => {
    const plan = planPullRequestResources(BACKEND_FIXTURE, 'pr123', LIVE_NAMESPACES);
    expect(plan.workerName).toBe('conloca-app-backend-pr123');
    expect(plan.kvNamespaces.map(({ title }) => title)).toEqual([
      'alias-index-pr123',
      'org-profiles-pr123',
      'conloca-mail-capture-pr123',
    ]);
    expect(plan.r2Buckets).toEqual([{ binding: 'MEDIA', bucketName: 'conloca-media-pr123' }]);

    const derived = derivePullRequestWranglerConfig(BACKEND_FIXTURE, {
      environment: 'pr123',
      accountId: 'account-1',
      kvNamespaceIds: DERIVED_IDS,
    });
    const emailId = rateLimitNamespaceId('account-1', 'conloca-app-backend', 'pr123', 'MAGIC_LINK_EMAIL_RATE_LIMIT');
    const sourceId = rateLimitNamespaceId('account-1', 'conloca-app-backend', 'pr123', 'MAGIC_LINK_SOURCE_RATE_LIMIT');

    expect(Number(emailId)).toBeGreaterThan(0);
    expect(Number(emailId)).toBeLessThanOrEqual(0x7fff_ffff);
    expect(sourceId).not.toBe(emailId);
    expect(derived).toContain(`namespace_id = "${emailId}"`);
    expect(derived).toContain(`namespace_id = "${sourceId}"`);
    expect(derived).toContain('id = "kv-alias-pr123-id"');
    expect(derived).toContain('id = "kv-org-pr123-id"');
    expect(derived).toContain('id = "kv-mail-pr123-id"');
    expect(derived).toContain('bucket_name = "conloca-media-pr123"');
    expect(derived).toContain('ENVIRONMENT = "pr123"');
    expect(derived).toContain('TLD_DOMAIN = "pr123.conloca.com"');
    expect(derived).toContain('AUTH_KEYS_INSTANCE_NAME = "pr123-20260716-2"');
    expect(derived).toContain('allowed_sender_addresses = ["login@mail.pr123.conloca.com"]');
    expect(derived).toContain('MAIL_CAPTURE_RECIPIENTS = "login-test@pr123.conloca.com,invite-test@pr123.conloca.com"');
    expect(derived).toContain('INVITATION_REDEEM_ORIGIN = "https://app.pr123.conloca.com"');
    expect(derived).toContain('GITHUB_APP_ID = "4077531"');
    expect(derived).toContain('GITHUB_APP_SLUG = "conloca-staging"');
    expect(derived).toContain('GITHUB_CLIENT_ID = "Iv23liD6EDsBZ8kJGU3f"');
    expect(derived).toContain('class_name = "ConlocaAuthKeysDO"');
    expect(derived).toContain('new_sqlite_classes = ["ConlocaAuthKeysDO", "SaasGitDO"]');
    expect(derived).not.toContain('pr456');
  });
});
