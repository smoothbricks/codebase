import { createHash } from 'node:crypto';
import type { DeploymentStage } from '@smoothbricks/nx-plugin/deploy-policy';
import typia from 'typia';
import { derivedStagingName, hasStageLabel, replaceExactToken, replaceHostnameLabel } from './stage-labels.js';

export type { DeploymentStage } from '@smoothbricks/nx-plugin/deploy-policy';

const MAX_PULL_REQUEST_NUMBER = 999_999_999;
const STAGE_PATTERN = /^(?:staging|production|pr[1-9][0-9]{0,8})$/;

export function pullRequestStage(prNumber: number): `pr${number}` {
  if (!Number.isInteger(prNumber) || prNumber < 1 || prNumber > MAX_PULL_REQUEST_NUMBER) {
    throw new Error(`Pull request number must be an integer from 1 through ${MAX_PULL_REQUEST_NUMBER}.`);
  }
  return `pr${prNumber}`;
}

export function parseDeploymentStage(value: string): DeploymentStage {
  if (!STAGE_PATTERN.test(value)) {
    throw new Error(
      'Deployment stage must be exactly staging, production, or pr followed by an integer from 1 through 999999999.',
    );
  }
  if (value === 'staging' || value === 'production') return value;
  return pullRequestStage(Number(value.slice(2)));
}

export function isPullRequestStage(stage: DeploymentStage): stage is `pr${number}` {
  // After parseDeploymentStage the only values are staging, production, or prN.
  // `production` starts with `pr`, so a prefix check is not a stage check.
  return stage !== 'staging' && stage !== 'production';
}

export function stageDomain(stage: string, zone: string): string {
  const token = parseDeploymentStage(stage);
  if (!zone || zone.startsWith('.') || zone.endsWith('.')) {
    throw new Error('Zone must be a non-empty DNS name without leading or trailing dots.');
  }
  return token === 'production' ? zone : `${token}.${zone}`;
}

export function stageResourceName(base: string, stage: string): string {
  const token = parseDeploymentStage(stage);
  if (!base) {
    throw new Error('Resource base name must not be empty.');
  }
  return token === 'production' ? base : `${base}-${token}`;
}

export function hasExactStageSegment(value: string, stage: `pr${number}`): boolean {
  const escaped = stage.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(`(?:^|[-.])${escaped}(?=$|[-.])`).test(value);
}

/**
 * A whole Wrangler configuration as data — the one model both source formats parse into
 * (`source-config.ts` owns the parsers). Nothing in this module reads configuration text, which
 * is what makes a stage derive identically from `wrangler.toml` and from `wrangler.jsonc`.
 */
export interface WranglerDocument {
  env?: Record<string, WranglerEnvironment | undefined>;
  [key: string]: unknown;
}

/**
 * One `env` block, still unparsed. `StageConfigFields` is the typed statement of what this
 * module reads out of it; every other key is carried into a derived stage, never interpreted.
 */
export interface WranglerEnvironment {
  [key: string]: unknown;
}

/** One reconciled binding of the plan: exactly what Cloudflare is asked about, and nothing else. */
export interface KvBinding {
  binding: string;
  id: string;
}
export interface R2Binding {
  binding: string;
  bucketName: string;
}

// Configuration rows, as opposed to plan rows: the fields the derivation proves and rewrites,
// plus every other key of the row carried verbatim. A route's `zone_id`, a KV binding's
// `preview_id` and anything Cloudflare adds next survive into the derived config untouched.

export interface StageRoute {
  pattern: string;
  zone_name?: string;
  custom_domain?: boolean;
  [key: string]: unknown;
}

export interface StageKvBinding {
  binding: string;
  id: string;
  [key: string]: unknown;
}

export interface StageR2Binding {
  binding: string;
  bucket_name: string;
  [key: string]: unknown;
}

export interface StageD1Binding {
  binding: string;
  database_name: string;
  database_id: string;
  migrations_dir?: string;
  [key: string]: unknown;
}

export interface StageServiceBinding {
  binding: string;
  service: string;
  environment?: string;
  [key: string]: unknown;
}

export interface StageRateLimit {
  name: string;
  namespace_id: string;
  [key: string]: unknown;
}

/** One-stage bindings that an env block and a flat JSON config both present. */
export interface StageConfigFields {
  name: string;
  routes?: StageRoute[];
  vars?: Record<string, unknown>;
  kv_namespaces?: StageKvBinding[];
  r2_buckets?: StageR2Binding[];
  d1_databases?: StageD1Binding[];
  services?: StageServiceBinding[];
  ratelimits?: StageRateLimit[];
  send_email?: Record<string, unknown>[];
  [key: string]: unknown;
}

const isWranglerEnvironment = typia.createIs<WranglerEnvironment>();
const isUnknownRecord = typia.createIs<Record<string, unknown>>();
const isUnknownRows = typia.createIs<Record<string, unknown>[]>();
const isRouteRows = typia.createIs<Array<string | Record<string, unknown>>>();

export interface LiveKvNamespace {
  id: string;
  title: string;
}

export interface PullRequestKvResource {
  binding: string;
  stagingId: string;
  stagingTitle: string;
  title: string;
}

export interface PullRequestD1Resource {
  binding: string;
  stagingId: string;
  name: string;
}

export interface PullRequestResourcePlan {
  stage: `pr${number}`;
  workerName: string;
  workerBaseName: string;
  kvNamespaces: PullRequestKvResource[];
  d1Databases: PullRequestD1Resource[];
  r2Buckets: R2Binding[];
  routes: Array<{ pattern: string; zoneName?: string; customDomain: boolean }>;
}
export interface ConfiguredStageResourcePlan {
  stage: DeploymentStage;
  workerName: string;
  kvNamespaces: KvBinding[];
  r2Buckets: R2Binding[];
  routes: Array<{ pattern: string; zoneName?: string; customDomain: boolean }>;
}

/** The reconcile plan (`reconcileStageResources`) read off one resolved stage's bindings. */
export function planStageResources(config: StageConfigFields, stage: DeploymentStage): ConfiguredStageResourcePlan {
  return {
    stage,
    workerName: config.name,
    kvNamespaces: (config.kv_namespaces ?? []).map(({ binding, id }) => ({ binding, id })),
    r2Buckets: (config.r2_buckets ?? []).map((bucket) => ({ binding: bucket.binding, bucketName: bucket.bucket_name })),
    routes: (config.routes ?? []).map((route) => ({
      pattern: route.pattern,
      ...(typeof route.zone_name === 'string' ? { zoneName: route.zone_name } : {}),
      customDomain: route.custom_domain === true,
    })),
  };
}

export function planConfiguredStageResources(
  document: WranglerDocument,
  stage: DeploymentStage,
): ConfiguredStageResourcePlan {
  return planStageResources(stageEnvironmentConfig(document, stage), stage);
}

/** The `env.<stage>` block as the stage model, or the refusal that it is not declared. */
function stageEnvironmentConfig(document: WranglerDocument, stage: DeploymentStage): StageConfigFields {
  const block = document.env?.[stage];
  if (!isWranglerEnvironment(block)) {
    throw new Error(`Wrangler configuration must declare [env.${stage}].`);
  }
  return environmentStageConfig(block, `[env.${stage}]`);
}

/**
 * One env block as the stage model: every field the derivation reads proven present and typed,
 * every other key carried verbatim. A field absent from the source stays absent, so the same
 * worker written as TOML and as JSONC produces the same model down to the key set.
 */
function environmentStageConfig(environment: WranglerEnvironment, label: string): StageConfigFields {
  const config: StageConfigFields = { ...environment, name: requiredString(environment, 'name', label) };
  if (environment.routes !== undefined) config.routes = readStageRoutes(environment.routes);
  if (isUnknownRecord(environment.vars)) config.vars = environment.vars;
  if (environment.kv_namespaces !== undefined) config.kv_namespaces = readKvBindings(environment.kv_namespaces);
  if (environment.r2_buckets !== undefined) config.r2_buckets = readR2Buckets(environment.r2_buckets);
  if (environment.d1_databases !== undefined) config.d1_databases = readD1Bindings(environment.d1_databases);
  if (environment.services !== undefined) config.services = readServices(environment.services);
  if (environment.ratelimits !== undefined) config.ratelimits = readRateLimits(environment.ratelimits);
  if (environment.send_email !== undefined) config.send_email = readRows(environment.send_email);
  return config;
}

/** The staging worker name without its required `-staging` suffix. */
export function stagingWorkerBaseName(name: string): string {
  if (!name.endsWith('-staging')) {
    throw new Error(`Worker name ${name} must end with the exact suffix -staging.`);
  }
  return name.slice(0, -'-staging'.length);
}

/**
 * A template whose routes are all pinned (no derivable stage label) cannot serve a
 * pull-request stage: it would deploy unrouted and Wrangler would expose it on workers.dev.
 */
export function assertPullRequestRoutable(name: string, routes: StageRoute[] | undefined): void {
  const list = routes ?? [];
  if (list.length === 0 || list.some((route) => hasStageLabel(route.pattern))) return;
  const patterns = list.map((route) => route.pattern).join(', ');
  throw new Error(
    `Every route of ${name} is pinned (no staging label): ${patterns}. A pull-request stage would deploy unrouted and Wrangler would expose it on workers.dev; add a stage-derivable route such as site.staging.<zone>/*.`,
  );
}

export function planPullRequestResources(
  document: WranglerDocument,
  stage: `pr${number}`,
  liveNamespaces: LiveKvNamespace[],
): PullRequestResourcePlan {
  return planPullRequestBindings(stageEnvironmentConfig(document, 'staging'), stage, liveNamespaces);
}

/** KV/R2/D1 isolation and route checks for a pull-request stage, shared by every source format. */
export function planPullRequestBindings(
  config: StageConfigFields,
  stage: `pr${number}`,
  liveNamespaces: LiveKvNamespace[],
): PullRequestResourcePlan {
  parseDeploymentStage(stage);
  assertPullRequestRoutable(config.name, config.routes);
  const workerBaseName = stagingWorkerBaseName(config.name);
  const namespaceById = new Map(liveNamespaces.map((namespace) => [namespace.id, namespace]));
  const kvNamespaces = (config.kv_namespaces ?? []).map(({ binding, id }) => {
    const stagingNamespace = namespaceById.get(id);
    if (!stagingNamespace) {
      throw new Error(
        `Staging KV binding ${binding} references namespace ${id}, which is absent from the account listing.`,
      );
    }
    const title = derivedStagingName(stagingNamespace.title, stage, 'Staging KV namespace title');
    return { binding, stagingId: id, stagingTitle: stagingNamespace.title, title };
  });
  const d1Databases = (config.d1_databases ?? []).map((database) => ({
    binding: database.binding,
    stagingId: database.database_id,
    name: derivedStagingName(database.database_name, stage, 'Staging D1 database'),
  }));
  // Planned, not created: the deploy refuses an underivable bucket here, before it mutates the account.
  const r2Buckets = (config.r2_buckets ?? []).map((bucket) => ({
    binding: bucket.binding,
    bucketName: derivedStagingName(bucket.bucket_name, stage, 'Staging R2 bucket'),
  }));
  const routes = (config.routes ?? [])
    .filter((route) => hasStageLabel(route.pattern))
    .map((route) => ({
      pattern: replaceHostnameLabel(route.pattern, stage),
      ...(typeof route.zone_name === 'string' ? { zoneName: route.zone_name } : {}),
      customDomain: route.custom_domain === true,
    }));
  return {
    stage,
    workerName: stageResourceName(workerBaseName, stage),
    workerBaseName,
    kvNamespaces,
    d1Databases,
    r2Buckets,
    routes,
  };
}

export interface DerivePullRequestConfigOptions {
  stage: `pr${number}`;
  accountId: string;
  kvNamespaceIds: ReadonlyMap<string, string>;
  d1DatabaseIds: ReadonlyMap<string, string>;
}

interface DeriveContext {
  stage: `pr${number}`;
  accountId: string;
  workerBaseName: string;
  kvNamespaceIds: ReadonlyMap<string, string>;
  d1DatabaseIds: ReadonlyMap<string, string>;
}

function deriveContext(name: string, options: DerivePullRequestConfigOptions): DeriveContext {
  parseDeploymentStage(options.stage);
  if (!options.accountId) {
    throw new Error('Cloudflare account id is required to derive rate-limit namespaces.');
  }
  return {
    stage: options.stage,
    accountId: options.accountId,
    workerBaseName: stagingWorkerBaseName(name),
    kvNamespaceIds: options.kvNamespaceIds,
    d1DatabaseIds: options.d1DatabaseIds,
  };
}

/**
 * The one rewrite policy for a pull-request stage. `section` is the wrangler
 * array/table name (`routes`, `vars`, …); a missing section is the worker root.
 * `nearbyName` is the rate-limit binding's `name` when rewriting `namespace_id`.
 */
function deriveStageField(
  section: string | undefined,
  key: string,
  current: unknown,
  ctx: DeriveContext,
  nearbyName?: string,
): unknown {
  if (section === undefined && key === 'name') {
    return stageResourceName(ctx.workerBaseName, ctx.stage);
  }
  if (section === 'routes' && key === 'pattern' && typeof current === 'string') {
    return replaceHostnameLabel(current, ctx.stage);
  }
  if (section === 'vars' && typeof current === 'string') {
    if (key === 'ENVIRONMENT') return ctx.stage;
    return replaceHostnameLabel(current, ctx.stage);
  }
  if (section === 'send_email' && key === 'allowed_sender_addresses' && Array.isArray(current)) {
    return current.map((value) => (typeof value === 'string' ? replaceHostnameLabel(value, ctx.stage) : value));
  }
  if (section === 'kv_namespaces' && key === 'id' && typeof current === 'string') {
    const derived = ctx.kvNamespaceIds.get(current);
    if (!derived) {
      throw new Error(`No derived KV namespace id was supplied for staging namespace ${current}.`);
    }
    return derived;
  }
  if (section === 'r2_buckets' && key === 'bucket_name' && typeof current === 'string') {
    return derivedStagingName(current, ctx.stage, 'Staging R2 bucket');
  }
  if (section === 'd1_databases' && key === 'database_name' && typeof current === 'string') {
    return derivedStagingName(current, ctx.stage, 'Staging D1 database');
  }
  if (section === 'd1_databases' && key === 'database_id' && typeof current === 'string') {
    const derived = ctx.d1DatabaseIds.get(current);
    if (!derived) {
      throw new Error(`No derived D1 database id was supplied for staging database ${current}.`);
    }
    return derived;
  }
  if (section === 'services' && key === 'service' && typeof current === 'string') {
    // Unlabelled services stay shared; replaceExactToken is a no-op without an exact staging segment.
    return replaceExactToken(current, 'staging', ctx.stage);
  }
  if (section === 'ratelimits' && key === 'namespace_id') {
    if (!nearbyName) {
      throw new Error('Rate-limit binding must declare a non-empty name.');
    }
    return rateLimitNamespaceId(ctx.accountId, ctx.workerBaseName, ctx.stage, nearbyName);
  }
  return current;
}

/** Staging bindings rewritten for a pull-request stage; keys this module does not model are left to the caller. */
export function derivePullRequestStageConfig<T extends StageConfigFields>(
  config: T,
  options: DerivePullRequestConfigOptions,
): T {
  const ctx = deriveContext(config.name, options);
  const derived: T = { ...config, name: stageResourceName(ctx.workerBaseName, ctx.stage) };
  if (config.routes) {
    derived.routes = config.routes
      .filter((route) => hasStageLabel(route.pattern))
      .map((route) => ({
        ...route,
        pattern: replaceHostnameLabel(route.pattern, ctx.stage),
      }));
  }
  if (config.vars) {
    derived.vars = Object.fromEntries(
      Object.entries(config.vars).map(([key, value]) => [key, deriveStageField('vars', key, value, ctx)]),
    );
  }
  if (config.kv_namespaces) {
    derived.kv_namespaces = config.kv_namespaces.map((namespace) => ({
      ...namespace,
      id: requiredDerivedString(deriveStageField('kv_namespaces', 'id', namespace.id, ctx)),
    }));
  }
  if (config.r2_buckets) {
    derived.r2_buckets = config.r2_buckets.map((bucket) => ({
      ...bucket,
      bucket_name: requiredDerivedString(deriveStageField('r2_buckets', 'bucket_name', bucket.bucket_name, ctx)),
    }));
  }
  if (config.d1_databases) {
    derived.d1_databases = config.d1_databases.map((database) => ({
      ...database,
      database_name: requiredDerivedString(
        deriveStageField('d1_databases', 'database_name', database.database_name, ctx),
      ),
      database_id: requiredDerivedString(deriveStageField('d1_databases', 'database_id', database.database_id, ctx)),
    }));
  }
  if (config.services) {
    derived.services = config.services.map((service) => ({
      ...service,
      service: requiredDerivedString(deriveStageField('services', 'service', service.service, ctx)),
    }));
  }
  if (config.ratelimits) {
    derived.ratelimits = config.ratelimits.map((limit) => ({
      ...limit,
      namespace_id: requiredDerivedString(
        deriveStageField('ratelimits', 'namespace_id', limit.namespace_id, ctx, limit.name),
      ),
    }));
  }
  if (config.send_email) {
    derived.send_email = config.send_email.map((row) => ({
      ...row,
      allowed_sender_addresses: deriveStageField(
        'send_email',
        'allowed_sender_addresses',
        row.allowed_sender_addresses,
        ctx,
      ),
    }));
  }
  return derived;
}

function requiredDerivedString(value: unknown): string {
  if (typeof value !== 'string' || !value) {
    throw new Error('Derived pull-request field must be a non-empty string.');
  }
  return value;
}

/**
 * The whole document with `env.<prN>` added, derived from `env.staging`. Nothing read from disk
 * is mutated — every level the derivation touches is rebuilt — so the committed source config is
 * still exactly what the repo wrote once the deploy is done with it.
 */
export function derivePullRequestDocument(
  document: WranglerDocument,
  options: DerivePullRequestConfigOptions,
): WranglerDocument {
  if (document.env?.[options.stage] !== undefined) {
    throw new Error(
      `Wrangler configuration already declares [env.${options.stage}]; a pull-request stage is derived from [env.staging] and must not be committed.`,
    );
  }
  const derived = derivePullRequestStageConfig(stageEnvironmentConfig(document, 'staging'), options);
  return { ...document, env: { ...document.env, [options.stage]: derived } };
}

export function rateLimitNamespaceId(
  accountId: string,
  workerBaseName: string,
  stage: string,
  bindingName: string,
): string {
  const token = parseDeploymentStage(stage);
  const digest = createHash('sha256').update(`${accountId}:${workerBaseName}:${token}:${bindingName}`).digest();
  const value = digest.readUInt32BE(0) & 0x7fff_ffff;
  return String(value === 0 ? 1 : value);
}

/**
 * Wrangler accepts a route as an object or as a bare pattern string. Both become the object form
 * here, so one derivation rewrites the hostname label either way — a string route used to be
 * carried into the pull-request stage verbatim, pointing it at staging's own hostname.
 */
function readStageRoutes(value: unknown): StageRoute[] {
  if (!isRouteRows(value)) return [];
  return value.map((row) =>
    typeof row === 'string' ? { pattern: row } : { ...row, pattern: requiredString(row, 'pattern', 'route') },
  );
}

function readKvBindings(value: unknown): StageKvBinding[] {
  return readRows(value).map((row) => ({
    ...row,
    binding: requiredString(row, 'binding', 'KV namespace'),
    id: requiredString(row, 'id', 'KV namespace'),
  }));
}

function readR2Buckets(value: unknown): StageR2Binding[] {
  return readRows(value).map((row) => {
    const binding = requiredString(row, 'binding', 'R2 binding');
    return { ...row, binding, bucket_name: requiredString(row, 'bucket_name', `R2 binding ${binding}`) };
  });
}

function readD1Bindings(value: unknown): StageD1Binding[] {
  return readRows(value).map((row) => {
    const binding = requiredString(row, 'binding', 'D1 database');
    return {
      ...row,
      binding,
      database_name: requiredString(row, 'database_name', `D1 binding ${binding}`),
      database_id: requiredString(row, 'database_id', `D1 binding ${binding}`),
    };
  });
}

function readServices(value: unknown): StageServiceBinding[] {
  return readRows(value).map((row) => {
    const binding = requiredString(row, 'binding', 'Service binding');
    return { ...row, binding, service: requiredString(row, 'service', `Service binding ${binding}`) };
  });
}

function readRateLimits(value: unknown): StageRateLimit[] {
  return readRows(value).map((row) => {
    const name = requiredString(row, 'name', 'Rate-limit binding');
    return { ...row, name, namespace_id: requiredString(row, 'namespace_id', `Rate-limit binding ${name}`) };
  });
}

function readRows(value: unknown): Record<string, unknown>[] {
  return isUnknownRows(value) ? value : [];
}

function requiredString(row: Record<string, unknown>, key: string, context: string): string {
  const value = row[key];
  if (typeof value !== 'string' || !value) {
    throw new Error(`${context} must declare a non-empty ${key}.`);
  }
  return value;
}
