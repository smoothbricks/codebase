import { createHash } from 'node:crypto';
import { getStaticTOMLValue, parseTOML } from 'toml-eslint-parser';
import typia from 'typia';
import { cloneEnvBlock } from './prepare-env.js';

export type EnvironmentToken = 'staging' | 'production' | `pr${number}`;

const MAX_PULL_REQUEST_NUMBER = 999_999_999;
const ENVIRONMENT_PATTERN = /^(?:staging|production|pr[1-9][0-9]{0,8})$/;

export function pullRequestEnvironment(prNumber: number): `pr${number}` {
  if (!Number.isInteger(prNumber) || prNumber < 1 || prNumber > MAX_PULL_REQUEST_NUMBER) {
    throw new Error(`Pull request number must be an integer from 1 through ${MAX_PULL_REQUEST_NUMBER}.`);
  }
  return `pr${prNumber}`;
}

export function parseEnvironmentToken(value: string): EnvironmentToken {
  if (!ENVIRONMENT_PATTERN.test(value)) {
    throw new Error(
      'Environment must be exactly staging, production, or pr followed by an integer from 1 through 999999999.',
    );
  }
  if (value === 'staging' || value === 'production') return value;
  return pullRequestEnvironment(Number(value.slice(2)));
}

export function isPullRequestEnvironment(environment: EnvironmentToken): environment is `pr${number}` {
  return environment.startsWith('pr');
}

export function environmentDomain(environment: string, zone: string): string {
  const token = parseEnvironmentToken(environment);
  if (!zone || zone.startsWith('.') || zone.endsWith('.')) {
    throw new Error('Zone must be a non-empty DNS name without leading or trailing dots.');
  }
  return token === 'production' ? zone : `${token}.${zone}`;
}

export function environmentResourceName(base: string, environment: string): string {
  const token = parseEnvironmentToken(environment);
  if (!base) {
    throw new Error('Resource base name must not be empty.');
  }
  return token === 'production' ? base : `${base}-${token}`;
}

export function hasExactEnvironmentSegment(value: string, environment: `pr${number}`): boolean {
  const escaped = environment.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(`(?:^|[-.])${escaped}(?=$|[-.])`).test(value);
}

interface WranglerRoot {
  env?: Record<string, WranglerEnvironment | undefined>;
}

interface WranglerEnvironment {
  name?: unknown;
  routes?: unknown;
  kv_namespaces?: unknown;
  r2_buckets?: unknown;
  ratelimits?: unknown;
  vars?: unknown;
  [key: string]: unknown;
}

export interface KvBinding {
  binding: string;
  id: string;
}
export interface R2Binding {
  binding: string;
  bucketName: string;
}
const isWranglerRoot = typia.createIs<WranglerRoot>();
const isWranglerEnvironment = typia.createIs<WranglerEnvironment>();
const isUnknownRecord = typia.createIs<Record<string, unknown>>();
const isUnknownRows = typia.createIs<Record<string, unknown>[]>();

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

export interface PullRequestResourcePlan {
  environment: `pr${number}`;
  workerName: string;
  workerBaseName: string;
  kvNamespaces: PullRequestKvResource[];
  r2Buckets: R2Binding[];
  routes: Array<{ pattern: string; zoneName?: string; customDomain: boolean }>;
}
export interface ConfiguredEnvironmentResourcePlan {
  environment: EnvironmentToken;
  workerName: string;
  kvNamespaces: KvBinding[];
  r2Buckets: R2Binding[];
  routes: Array<{ pattern: string; zoneName?: string; customDomain: boolean }>;
}

export function planConfiguredEnvironmentResources(
  toml: string,
  environment: EnvironmentToken,
): ConfiguredEnvironmentResourcePlan {
  const block = parseRoot(toml).env?.[environment];
  if (!isWranglerEnvironment(block)) {
    throw new Error(`Wrangler configuration must declare [env.${environment}].`);
  }
  const workerName = requiredString(block, 'name', `[env.${environment}]`);
  return {
    environment,
    workerName,
    kvNamespaces: readKvBindings(block.kv_namespaces),
    r2Buckets: readRows(block.r2_buckets).map((row) => {
      const binding = requiredString(row, 'binding', 'R2 binding');
      return { binding, bucketName: requiredString(row, 'bucket_name', `R2 binding ${binding}`) };
    }),
    routes: readRows(block.routes).map((row) => ({
      pattern: requiredString(row, 'pattern', 'route'),
      ...(typeof row.zone_name === 'string' ? { zoneName: row.zone_name } : {}),
      customDomain: row.custom_domain === true,
    })),
  };
}

function parseRoot(toml: string): WranglerRoot {
  const value: unknown = getStaticTOMLValue(parseTOML(toml));
  if (!isWranglerRoot(value)) {
    throw new Error('Wrangler configuration is not a valid TOML environment document.');
  }
  return value;
}

function stagingEnvironment(toml: string): WranglerEnvironment {
  const staging = parseRoot(toml).env?.staging;
  if (!isWranglerEnvironment(staging)) {
    throw new Error('Wrangler configuration must declare [env.staging].');
  }
  return staging;
}

function stagingWorkerName(staging: WranglerEnvironment): { workerName: string; workerBaseName: string } {
  if (typeof staging.name !== 'string' || !staging.name.endsWith('-staging')) {
    throw new Error('[env.staging].name must end with the exact suffix -staging.');
  }
  return { workerName: staging.name, workerBaseName: staging.name.slice(0, -'-staging'.length) };
}

export function planPullRequestResources(
  toml: string,
  environment: `pr${number}`,
  liveNamespaces: LiveKvNamespace[],
): PullRequestResourcePlan {
  parseEnvironmentToken(environment);
  const staging = stagingEnvironment(toml);
  const { workerBaseName } = stagingWorkerName(staging);
  const namespaceById = new Map(liveNamespaces.map((namespace) => [namespace.id, namespace]));
  const kvNamespaces = readKvBindings(staging.kv_namespaces).map(({ binding, id }) => {
    const stagingNamespace = namespaceById.get(id);
    if (!stagingNamespace) {
      throw new Error(
        `Staging KV binding ${binding} references namespace ${id}, which is absent from the account listing.`,
      );
    }
    const title = replaceExactToken(stagingNamespace.title, 'staging', environment);
    if (title === stagingNamespace.title) {
      throw new Error(`Staging KV namespace title ${stagingNamespace.title} has no exact staging segment.`);
    }
    return { binding, stagingId: id, stagingTitle: stagingNamespace.title, title };
  });
  const r2Buckets = readRows(staging.r2_buckets).map((row) => {
    const binding = requiredString(row, 'binding', 'R2 binding');
    const stagingBucket = requiredString(row, 'bucket_name', `R2 binding ${binding}`);
    const bucketName = replaceExactToken(stagingBucket, 'staging', environment);
    if (bucketName === stagingBucket) {
      throw new Error(`Staging R2 bucket ${stagingBucket} has no exact staging segment.`);
    }
    return { binding, bucketName };
  });
  const routes = readRows(staging.routes).map((row) => ({
    pattern: replaceHostnameLabel(requiredString(row, 'pattern', 'route'), environment),
    ...(typeof row.zone_name === 'string' ? { zoneName: row.zone_name } : {}),
    customDomain: row.custom_domain === true,
  }));
  return {
    environment,
    workerName: environmentResourceName(workerBaseName, environment),
    workerBaseName,
    kvNamespaces,
    r2Buckets,
    routes,
  };
}

export interface DerivePullRequestConfigOptions {
  environment: `pr${number}`;
  accountId: string;
  kvNamespaceIds: ReadonlyMap<string, string>;
}

export function derivePullRequestWranglerConfig(toml: string, options: DerivePullRequestConfigOptions): string {
  const environment = options.environment;
  parseEnvironmentToken(environment);
  if (!options.accountId) {
    throw new Error('Cloudflare account id is required to derive rate-limit namespaces.');
  }
  const staging = stagingEnvironment(toml);
  const { workerBaseName } = stagingWorkerName(staging);
  const cloned = cloneEnvBlock(toml, 'staging', environment);
  const program = parseTOML(cloned);
  const rootValue: unknown = getStaticTOMLValue(program);
  if (!isWranglerRoot(rootValue)) {
    throw new Error('Derived Wrangler configuration is not a valid environment document.');
  }
  const root = rootValue;
  const edits: Array<{ start: number; end: number; value: string }> = [];

  for (const table of program.body[0].body) {
    if (table.type !== 'TOMLTable' || table.resolvedKey[0] !== 'env' || table.resolvedKey[1] !== environment) {
      continue;
    }
    const tableValue = valueAtPath(root, table.resolvedKey);
    if (!isUnknownRecord(tableValue)) {
      continue;
    }
    for (const keyValue of table.body) {
      const key = cloned.slice(keyValue.key.range[0], keyValue.key.range[1]).trim();
      const current = tableValue[key];
      const next = deriveFieldValue(
        table.resolvedKey.slice(2),
        tableValue,
        key,
        current,
        environment,
        workerBaseName,
        options.accountId,
        options.kvNamespaceIds,
      );
      if (next !== current) {
        edits.push({ start: keyValue.value.range[0], end: keyValue.value.range[1], value: tomlLiteral(next) });
      }
    }
  }

  let derived = cloned;
  for (const edit of edits.sort((left, right) => right.start - left.start)) {
    derived = derived.slice(0, edit.start) + edit.value + derived.slice(edit.end);
  }
  parseTOML(derived);
  return derived;
}

function deriveFieldValue(
  path: Array<string | number>,
  table: Record<string, unknown>,
  key: string,
  current: unknown,
  environment: `pr${number}`,
  workerBaseName: string,
  accountId: string,
  kvNamespaceIds: ReadonlyMap<string, string>,
): unknown {
  const section = path[0];
  if (path.length === 0 && key === 'name') {
    return environmentResourceName(workerBaseName, environment);
  }
  if (section === 'routes' && key === 'pattern' && typeof current === 'string') {
    return replaceHostnameLabel(current, environment);
  }
  if (section === 'vars' && typeof current === 'string') {
    if (key === 'ENVIRONMENT') {
      return environment;
    }
    if (key === 'AUTH_KEYS_INSTANCE_NAME') {
      return replaceExactToken(current, 'staging', environment);
    }
    return replaceHostnameLabel(current, environment);
  }
  if (section === 'send_email' && key === 'allowed_sender_addresses' && Array.isArray(current)) {
    return current.map((value) => (typeof value === 'string' ? replaceHostnameLabel(value, environment) : value));
  }
  if (section === 'kv_namespaces' && key === 'id' && typeof current === 'string') {
    const derived = kvNamespaceIds.get(current);
    if (!derived) {
      throw new Error(`No derived KV namespace id was supplied for staging namespace ${current}.`);
    }
    return derived;
  }
  if (section === 'r2_buckets' && key === 'bucket_name' && typeof current === 'string') {
    return replaceExactToken(current, 'staging', environment);
  }
  if (section === 'ratelimits' && key === 'namespace_id') {
    const bindingName = requiredString(table, 'name', 'Rate-limit binding');
    return rateLimitNamespaceId(accountId, workerBaseName, environment, bindingName);
  }
  return current;
}

export function rateLimitNamespaceId(
  accountId: string,
  workerBaseName: string,
  environment: string,
  bindingName: string,
): string {
  const token = parseEnvironmentToken(environment);
  const digest = createHash('sha256').update(`${accountId}:${workerBaseName}:${token}:${bindingName}`).digest();
  const value = digest.readUInt32BE(0) & 0x7fff_ffff;
  return String(value === 0 ? 1 : value);
}

function replaceHostnameLabel(value: string, environment: `pr${number}`): string {
  return value.replace(/(^|[.@/])staging(?=\.)/g, `$1${environment}`);
}

function replaceExactToken(value: string, from: string, to: string): string {
  const escaped = from.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return value.replace(new RegExp(`(^|[-.])${escaped}(?=$|[-.])`, 'g'), `$1${to}`);
}

function readKvBindings(value: unknown): KvBinding[] {
  return readRows(value).map((row) => ({
    binding: requiredString(row, 'binding', 'KV namespace'),
    id: requiredString(row, 'id', 'KV namespace'),
  }));
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

function valueAtPath(root: unknown, path: Array<string | number>): unknown {
  let value = root;
  for (const segment of path) {
    if (typeof segment === 'number') {
      if (!Array.isArray(value)) return undefined;
      value = value[segment];
    } else {
      if (!isUnknownRecord(value)) return undefined;
      value = value[segment];
    }
  }
  return value;
}

function tomlLiteral(value: unknown): string {
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean' || Array.isArray(value)) {
    const literal = JSON.stringify(value);
    if (literal !== undefined) return literal;
  }
  throw new Error(`Cannot materialize Wrangler TOML value of type ${typeof value}.`);
}
