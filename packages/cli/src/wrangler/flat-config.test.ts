import { describe, expect, it } from 'bun:test';
import {
  type DeriveFlatPullRequestOptions,
  deriveFlatPullRequestConfig,
  type FlatWranglerConfig,
  parseFlatWranglerConfig,
  planFlatPullRequestResources,
  planFlatStageResources,
} from './flat-config.js';

// The shape the Cloudflare Vite adapter emits for a site built with
// CLOUDFLARE_ENV=staging: one flat document, ids already resolved.
const WEBSITE = {
  name: 'site-preview-staging',
  main: 'entry.mjs',
  compatibility_date: '2026-03-15',
  targetEnvironment: 'staging',
  assets: { binding: 'ASSETS', directory: '../client' },
  routes: [
    { pattern: 'next.example.com', custom_domain: true },
    { pattern: 'site.staging.example.com/*', zone_name: 'example.com' },
  ],
  vars: {
    EXAMPLE_SAAS_ENDPOINT: 'https://app.staging.example.com',
    ENVIRONMENT: 'staging',
    ISSUER: 'https://saas.example.app',
  },
  kv_namespaces: [{ binding: 'SESSION', id: 'kv-staging' }],
  r2_buckets: [{ binding: 'MEDIA', bucket_name: 'site-media-staging' }],
  d1_databases: [
    { binding: 'DB', database_name: 'site-staging-db', database_id: 'd1-staging', migrations_dir: '../../migrations' },
  ],
  services: [{ binding: 'BACKEND', service: 'backend-staging' }],
  ratelimits: [{ name: 'SIGNUP', namespace_id: '1001', simple: { limit: 5, period: 60 } }],
};

const CONFIG = parseFlatWranglerConfig(JSON.stringify(WEBSITE));
const LIVE_KV = [{ id: 'kv-staging', title: 'site-SESSION-staging' }];

const PR7: DeriveFlatPullRequestOptions = {
  stage: 'pr7',
  accountId: 'account-1',
  kvNamespaceIds: new Map([['kv-staging', 'kv-pr7']]),
  d1DatabaseIds: new Map([['d1-staging', 'd1-pr7']]),
};

/** The fixture with sections replaced, re-parsed so the guard sees it the way a build output arrives. */
function configWith(overrides: Record<string, unknown>): FlatWranglerConfig {
  return parseFlatWranglerConfig(JSON.stringify({ ...WEBSITE, ...overrides }));
}

describe('parseFlatWranglerConfig', () => {
  it('accepts the adapter output', () => {
    expect(CONFIG.name).toBe('site-preview-staging');
  });

  it('rejects env blocks: a flat config is already resolved for one stage', () => {
    expect(() => parseFlatWranglerConfig(JSON.stringify({ ...WEBSITE, env: { staging: {} } }))).toThrow(/env blocks/);
  });

  it('names the offending path when a binding section is malformed', () => {
    expect(() => parseFlatWranglerConfig(JSON.stringify({ ...WEBSITE, kv_namespaces: [{ binding: 'A' }] }))).toThrow(
      /malformed .*kv_namespaces/,
    );
  });

  it('rejects invalid JSON', () => {
    expect(() => parseFlatWranglerConfig('{')).toThrow(SyntaxError);
  });
});

describe('planFlatPullRequestResources', () => {
  it('derives KV titles from the live staging titles and D1 names from the staging names', () => {
    const plan = planFlatPullRequestResources(CONFIG, 'pr7', LIVE_KV);
    expect(plan.workerName).toBe('site-preview-pr7');
    expect(plan.kvNamespaces).toEqual([
      { binding: 'SESSION', stagingId: 'kv-staging', stagingTitle: 'site-SESSION-staging', title: 'site-SESSION-pr7' },
    ]);
    expect(plan.d1Databases).toEqual([{ binding: 'DB', stagingId: 'd1-staging', name: 'site-pr7-db' }]);
    expect(plan.r2Buckets).toEqual([{ binding: 'MEDIA', bucketName: 'site-media-pr7' }]);
  });

  it('refuses a staging KV namespace whose live title has no staging segment', () => {
    expect(() => planFlatPullRequestResources(CONFIG, 'pr7', [{ id: 'kv-staging', title: 'shared' }])).toThrow(
      /no exact staging segment/,
    );
  });

  it('refuses a staging D1 database whose name has no staging segment', () => {
    const config = configWith({ d1_databases: [{ binding: 'DB', database_name: 'shared-db', database_id: 'd1-x' }] });
    expect(() => planFlatPullRequestResources(config, 'pr7', LIVE_KV)).toThrow(/no exact staging segment/);
  });

  it('refuses a staging R2 bucket whose name has no staging segment', () => {
    const config = configWith({ r2_buckets: [{ binding: 'MEDIA', bucket_name: 'shared-media' }] });
    expect(() => planFlatPullRequestResources(config, 'pr7', LIVE_KV)).toThrow(/no exact staging segment/);
  });

  it('refuses an all-pinned template, so the caller creates no resources for a stage it cannot route', () => {
    const config = configWith({ routes: [{ pattern: 'next.example.com', custom_domain: true }] });
    expect(() => planFlatPullRequestResources(config, 'pr7', LIVE_KV)).toThrow(/pinned/);
    expect(() => planFlatPullRequestResources(config, 'pr7', LIVE_KV)).toThrow(/next\.example\.com/);
  });

  it('refuses a template whose name does not end in -staging, since it is not the staging build', () => {
    const config = configWith({ name: 'site-preview' });
    expect(() => planFlatPullRequestResources(config, 'pr7', LIVE_KV)).toThrow(/-staging/);
  });
});

describe('deriveFlatPullRequestConfig', () => {
  const derived = deriveFlatPullRequestConfig(CONFIG, PR7);

  it('renames the worker and keeps everything it does not derive', () => {
    expect(derived.name).toBe('site-preview-pr7');
    expect(derived.main).toBe('entry.mjs');
    expect(derived.assets).toEqual({ binding: 'ASSETS', directory: '../client' });
    expect(derived.compatibility_date).toBe('2026-03-15');
  });

  it('derives routes with a stage label and drops pinned hosts', () => {
    expect(derived.routes).toEqual([{ pattern: 'site.pr7.example.com/*', zone_name: 'example.com' }]);
  });

  it('refuses a template whose name does not end in -staging, since it is not the staging build', () => {
    expect(() => deriveFlatPullRequestConfig(configWith({ name: 'site-preview' }), PR7)).toThrow(/-staging/);
  });

  it('leaves an explicitly routeless template routeless', () => {
    expect(deriveFlatPullRequestConfig(configWith({ routes: [] }), PR7).routes).toEqual([]);
  });

  it('derives vars by hostname label and ENVIRONMENT verbatim', () => {
    expect(derived.vars).toEqual({
      EXAMPLE_SAAS_ENDPOINT: 'https://app.pr7.example.com',
      ENVIRONMENT: 'pr7',
      ISSUER: 'https://saas.example.app',
    });
  });

  it('does not add an ENVIRONMENT var the template does not declare', () => {
    const config = configWith({ vars: { ISSUER: 'https://saas.example.app' } });
    expect(deriveFlatPullRequestConfig(config, PR7).vars).toEqual({ ISSUER: 'https://saas.example.app' });
  });

  it('substitutes KV and D1 ids, and derives R2, service and D1 names', () => {
    expect(derived.kv_namespaces).toEqual([{ binding: 'SESSION', id: 'kv-pr7' }]);
    expect(derived.r2_buckets).toEqual([{ binding: 'MEDIA', bucket_name: 'site-media-pr7' }]);
    expect(derived.d1_databases).toEqual([
      { binding: 'DB', database_name: 'site-pr7-db', database_id: 'd1-pr7', migrations_dir: '../../migrations' },
    ]);
    expect(derived.services).toEqual([{ binding: 'BACKEND', service: 'backend-pr7' }]);
  });

  it('derives rate-limit namespace ids deterministically per stage and binding', () => {
    const [limit] = derived.ratelimits ?? [];
    expect(limit?.namespace_id).toMatch(/^[1-9][0-9]*$/);
    expect(limit?.namespace_id).not.toBe('1001');
  });

  it('materialises no section a minimal template does not declare', () => {
    expect(deriveFlatPullRequestConfig(parseFlatWranglerConfig('{"name":"x-staging"}'), PR7)).toEqual({
      name: 'x-pr7',
    });
  });

  it('refuses a KV binding with no derived counterpart', () => {
    expect(() => deriveFlatPullRequestConfig(CONFIG, { ...PR7, kvNamespaceIds: new Map() })).toThrow(/KV namespace id/);
  });

  it('refuses a D1 binding with no derived counterpart', () => {
    expect(() => deriveFlatPullRequestConfig(CONFIG, { ...PR7, d1DatabaseIds: new Map() })).toThrow(/D1 database id/);
  });

  it('refuses an R2 bucket with no staging segment, which the stage would otherwise share', () => {
    const config = configWith({ r2_buckets: [{ binding: 'MEDIA', bucket_name: 'shared-media' }] });
    expect(() => deriveFlatPullRequestConfig(config, PR7)).toThrow(/no exact staging segment/);
  });
});

describe('planFlatStageResources', () => {
  it('reads the bindings a reconcile needs from a flat config', () => {
    const plan = planFlatStageResources(CONFIG, 'staging');
    expect(plan.workerName).toBe('site-preview-staging');
    expect(plan.kvNamespaces).toEqual([{ binding: 'SESSION', id: 'kv-staging' }]);
    expect(plan.r2Buckets).toEqual([{ binding: 'MEDIA', bucketName: 'site-media-staging' }]);
    expect(plan.routes).toEqual([
      { pattern: 'next.example.com', customDomain: true },
      { pattern: 'site.staging.example.com/*', zoneName: 'example.com', customDomain: false },
    ]);
  });
});
