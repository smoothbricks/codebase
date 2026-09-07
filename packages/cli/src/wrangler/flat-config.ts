import typia from 'typia';
import { formatValidationErrors } from '../lib/json.js';
import {
  type ConfiguredStageResourcePlan,
  type DeploymentStage,
  type LiveKvNamespace,
  parseDeploymentStage,
  type R2Binding,
  rateLimitNamespaceId,
  stageResourceName,
} from './stage.js';
import { derivedStagingName, hasStageLabel, replaceExactToken, replaceHostnameLabel } from './stage-labels.js';

/**
 * A Wrangler configuration with no `env` blocks: the shape build tools emit
 * once they have resolved an environment (the Cloudflare Vite and Astro
 * adapters write one under the build output). Unknown keys are carried
 * through verbatim so a deploy never loses a binding this module does not
 * model.
 */
export interface FlatWranglerConfig {
  name: string;
  routes?: { pattern: string; zone_name?: string; custom_domain?: boolean }[];
  vars?: Record<string, unknown>;
  kv_namespaces?: { binding: string; id: string }[];
  r2_buckets?: { binding: string; bucket_name: string }[];
  d1_databases?: { binding: string; database_name: string; database_id: string; migrations_dir?: string }[];
  // `environment` is deliberately not derived: name-suffixed services are the supported shape.
  services?: { binding: string; service: string; environment?: string }[];
  ratelimits?: { name: string; namespace_id: string; simple?: unknown }[];
  env?: unknown;
  [key: string]: unknown;
}

const parseFlatWranglerConfigText = typia.json.createValidateParse<FlatWranglerConfig>();

/** Reads a build-generated Wrangler JSON, refusing anything that is not already resolved for one stage. */
export function parseFlatWranglerConfig(json: string): FlatWranglerConfig {
  const result = parseFlatWranglerConfigText(json);
  if (!result.success) {
    throw new Error(`Flat Wrangler configuration is malformed at ${formatValidationErrors(result.errors)}.`);
  }
  if (result.data.env !== undefined) {
    throw new Error('A flat Wrangler configuration must not declare env blocks; it is already resolved for one stage.');
  }
  return result.data;
}

/** The pull-request template is the staging build, so its name carries the exact `-staging` suffix. */
function flatConfigWorkerBaseName(config: FlatWranglerConfig): string {
  if (!config.name.endsWith('-staging')) {
    throw new Error(
      `A flat configuration used as the pull-request template must have a name ending in -staging, but it is ${config.name}.`,
    );
  }
  return config.name.slice(0, -'-staging'.length);
}

/**
 * A template whose routes are all pinned to staging (no derivable stage label) cannot serve a
 * pull-request stage: it would deploy unrouted and Wrangler would expose it on workers.dev. The
 * check runs before any Cloudflare resource is created, so a refused template leaves no strays.
 */
function assertPullRequestRoutable(config: FlatWranglerConfig): void {
  const routes = config.routes ?? [];
  if (routes.length === 0 || routes.some((route) => hasStageLabel(route.pattern))) return;
  const patterns = routes.map((route) => route.pattern).join(', ');
  throw new Error(
    `Every route of ${config.name} is pinned (no staging label): ${patterns}. A pull-request stage would deploy unrouted and Wrangler would expose it on workers.dev; add a stage-derivable route such as site.staging.<zone>/*.`,
  );
}

interface FlatPullRequestPlan {
  stage: `pr${number}`;
  workerName: string;
  kvNamespaces: { binding: string; stagingId: string; stagingTitle: string; title: string }[];
  d1Databases: { binding: string; stagingId: string; name: string }[];
  r2Buckets: R2Binding[];
}

/** The KV namespaces, D1 databases and R2 buckets a pull-request stage needs, named after their staging counterparts. */
export function planFlatPullRequestResources(
  config: FlatWranglerConfig,
  stage: `pr${number}`,
  liveNamespaces: LiveKvNamespace[],
): FlatPullRequestPlan {
  // The `pr${number}` type still admits pr0 and pr-1; the runtime check refuses them, as the TOML path does.
  parseDeploymentStage(stage);
  assertPullRequestRoutable(config);
  const base = flatConfigWorkerBaseName(config);
  const namespaceById = new Map(liveNamespaces.map((namespace) => [namespace.id, namespace]));
  const kvNamespaces = (config.kv_namespaces ?? []).map(({ binding, id }) => {
    const live = namespaceById.get(id);
    if (!live) {
      throw new Error(
        `Staging KV binding ${binding} references namespace ${id}, which is absent from the account listing.`,
      );
    }
    return {
      binding,
      stagingId: id,
      stagingTitle: live.title,
      title: derivedStagingName(live.title, stage, 'Staging KV namespace title'),
    };
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
  return { stage, workerName: stageResourceName(base, stage), kvNamespaces, d1Databases, r2Buckets };
}

export interface DeriveFlatPullRequestOptions {
  stage: `pr${number}`;
  accountId: string;
  kvNamespaceIds: ReadonlyMap<string, string>;
  d1DatabaseIds: ReadonlyMap<string, string>;
}

/** The staging configuration rewritten for a pull-request stage; everything not derived below is carried through verbatim, and the id maps come from `planFlatPullRequestResources`. */
export function deriveFlatPullRequestConfig(
  config: FlatWranglerConfig,
  options: DeriveFlatPullRequestOptions,
): FlatWranglerConfig {
  const { stage } = options;
  // The `pr${number}` type still admits pr0 and pr-1; the runtime check refuses them, as the TOML path does.
  parseDeploymentStage(stage);
  if (!options.accountId) {
    throw new Error('Cloudflare account id is required to derive rate-limit namespaces.');
  }
  const base = flatConfigWorkerBaseName(config);
  const derived: FlatWranglerConfig = { ...config, name: stageResourceName(base, stage) };
  if (config.routes) {
    // A host without the staging label (next.example.com) is pinned to
    // staging: it cannot be derived, so a pull-request stage does not claim it.
    derived.routes = config.routes
      .filter((route) => hasStageLabel(route.pattern))
      .map((route) => ({ ...route, pattern: replaceHostnameLabel(route.pattern, stage) }));
  }
  if (config.vars) {
    derived.vars = Object.fromEntries(
      Object.entries(config.vars).map(([key, value]) => {
        if (key === 'ENVIRONMENT') return [key, stage];
        return [key, typeof value === 'string' ? replaceHostnameLabel(value, stage) : value];
      }),
    );
  }
  if (config.kv_namespaces) {
    derived.kv_namespaces = config.kv_namespaces.map((namespace) => {
      const id = options.kvNamespaceIds.get(namespace.id);
      if (!id) throw new Error(`No derived KV namespace id was supplied for staging namespace ${namespace.id}.`);
      return { ...namespace, id };
    });
  }
  if (config.r2_buckets) {
    derived.r2_buckets = config.r2_buckets.map((bucket) => ({
      ...bucket,
      bucket_name: derivedStagingName(bucket.bucket_name, stage, 'Staging R2 bucket'),
    }));
  }
  if (config.d1_databases) {
    derived.d1_databases = config.d1_databases.map((database) => {
      const id = options.d1DatabaseIds.get(database.database_id);
      if (!id) throw new Error(`No derived D1 database id was supplied for staging database ${database.database_id}.`);
      return {
        ...database,
        database_name: derivedStagingName(database.database_name, stage, 'Staging D1 database'),
        database_id: id,
      };
    });
  }
  if (config.services) {
    derived.services = config.services.map((service) => ({
      ...service,
      service: replaceExactToken(service.service, 'staging', stage),
    }));
  }
  if (config.ratelimits) {
    derived.ratelimits = config.ratelimits.map((limit) => ({
      ...limit,
      namespace_id: rateLimitNamespaceId(options.accountId, base, stage, limit.name),
    }));
  }
  return derived;
}

/** The reconcile plan (`reconcileStageResources`) read off a flat config for any stage. */
export function planFlatStageResources(
  config: FlatWranglerConfig,
  stage: DeploymentStage,
): ConfiguredStageResourcePlan {
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
