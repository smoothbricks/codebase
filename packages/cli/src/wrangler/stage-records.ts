// What a pull-request stage deploy writes down before it creates anything, so that cleanup can
// delete exactly what that repository's stage made. Internal to the package.
//
// One R2 bucket per Cloudflare account holds one empty object per recorded item. The key carries
// the whole record, so nothing ever reads an object's body, and R2 lists are strongly consistent,
// so a cleanup that starts seconds after it cancelled a deploy still sees every key that deploy
// wrote. The grammar (version 1), every field `encodeURIComponent`-encoded so none contains `/`:
//
//   v1/<scope>/<stage>/<worker>/worker
//   v1/<scope>/<stage>/<worker>/kv/<title>
//   v1/<scope>/<stage>/<worker>/d1/<name>
//   v1/<scope>/<stage>/<worker>/r2/<bucket>
//   v1/<scope>/<stage>/<worker>/domain/<hostname>
//   v1/<scope>/<stage>/<worker>/route/<zoneName>/<pattern>
//   v1/<scope>/<stage>/<worker>/dns/<zoneName>/<recordName>
//
// The worker is part of every key, so the parallel deploys of one stage's projects never write the
// same key; an item two of them share gets two keys. Deploys only ever add keys.

import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { readPackageJsonObject, repositoryInfo } from '../lib/workspace.js';
import type { CloudflareZone } from './cloudflare.js';
import { hasExactStageSegment, type PullRequestResourcePlan } from './stage.js';
import { routeHostname, wildcardDnsRecord } from './stage-labels.js';

export const STAGE_RECORDS_BUCKET = 'smoo-stage-records';

export type StageRecord =
  | { kind: 'worker'; worker: string }
  // The title, not the id: the record is written before the namespace exists.
  | { kind: 'kv'; worker: string; title: string }
  | { kind: 'd1'; worker: string; name: string }
  | { kind: 'r2'; worker: string; bucket: string }
  // A Worker custom domain.
  | { kind: 'domain'; worker: string; hostname: string }
  | { kind: 'route'; worker: string; zone: string; pattern: string }
  // The proxied CNAME `*.<host>` -> `<host>` a `*.` route gets.
  | { kind: 'dns'; worker: string; zone: string; name: string };

type PlannedRoute = PullRequestResourcePlan['routes'][number];

const KEY_VERSION = 'v1';
/** R2 refuses a longer object key. */
const MAX_KEY_BYTES = 1024;
const SHORTHAND_HOSTS = new Map([
  ['github', 'github.com'],
  ['gitlab', 'gitlab.com'],
  ['bitbucket', 'bitbucket.org'],
]);

/**
 * The repository a stage's records belong to: `host/owner/repo` from the root package.json's
 * `repository`. Two repositories deploying into one account therefore never see each other's
 * records, and in CI a manifest naming another repository than the one CI runs for is refused.
 */
export function stageRecordScope(root: string, env: NodeJS.ProcessEnv): string {
  // Without git, the repository root falls back to the working directory, which under Nx is a
  // project whose own package.json may name a repository as well.
  if (!existsSync(join(root, 'nx.json'))) {
    throw new Error(
      `${root} has no nx.json, so it is not the workspace root whose package.json names the repository pull-request stages are recorded under.`,
    );
  }
  const file = join(root, 'package.json');
  const manifest = readPackageJsonObject(file);
  const url = manifest ? repositoryInfo(manifest)?.url : undefined;
  if (!url) {
    throw new Error(`${file} declares no repository; pull-request stages are recorded under the repository it names.`);
  }
  const scope = repositoryScope(url);
  if (!scope) {
    throw new Error(`${file} declares repository ${url}, which names no host, owner and repository.`);
  }
  // Only owner/repo, never the host: a Forgejo runner may know its forge by an internal address.
  const named = scope.split('/').slice(-2).join('/');
  const ci = env.GITHUB_REPOSITORY;
  if (ci && ci.toLowerCase() !== named) {
    throw new Error(
      `repository.url in ${file} names ${named}, but CI runs for ${ci}. A fork, a template copy or a copied manifest keeps another repository's URL, and its stages would be recorded, and cleaned up, as that repository's.`,
    );
  }
  return scope;
}

/**
 * `host/path`, lowercased, from the forms npm accepts for `repository`: a URL (with `git+`, user
 * info or a port), scp-style `git@host:path`, the `github:`/`gitlab:`/`bitbucket:` shorthands and
 * bare `owner/repo`. Nothing unless it names a host plus at least owner and repository.
 */
function repositoryScope(url: string): string | undefined {
  const value = url
    .trim()
    .replace(/^git\+/i, '')
    .replace(/#.*$/, '');
  const shorthand = /^([a-z]+):(.*)$/i.exec(value);
  const shorthandHost = shorthand ? SHORTHAND_HOSTS.get(shorthand[1].toLowerCase()) : undefined;
  const withScheme = /^[a-z][a-z0-9+.-]*:\/\/([^/]*)(.*)$/i.exec(value);
  const scp = /^(?:[^@/]+@)?([^/:]+):(.*)$/.exec(value);
  let host: string;
  let path: string;
  if (shorthand && shorthandHost) {
    host = shorthandHost;
    path = shorthand[2];
  } else if (withScheme) {
    host = withScheme[1].replace(/^.*@/, '').replace(/:\d*$/, '');
    path = withScheme[2];
  } else if (scp) {
    host = scp[1];
    path = scp[2];
  } else if (value.split('/').length === 2) {
    host = 'github.com';
    path = value;
  } else {
    return undefined;
  }
  const segments = path
    .replace(/^\/+|\/+$/g, '')
    .replace(/\.git$/i, '')
    .split('/');
  if (!host || segments.length < 2 || segments.some((segment) => !segment)) return undefined;
  return `${host}/${segments.join('/')}`.toLowerCase();
}

/** Every key of `stage` starts with this; the trailing slash keeps `pr7` from listing `pr70`. */
export function stageRecordPrefix(scope: string, stage: `pr${number}`): string {
  return `${KEY_VERSION}/${encodeURIComponent(scope)}/${stage}/`;
}

/** Refuses, before the deploy changes anything, a record that is not the stage's own or a key R2 cannot hold. */
export function stageRecordKey(scope: string, stage: `pr${number}`, record: StageRecord): string {
  const refusal = stageRecordRefusal(record, stage);
  if (refusal) throw new Error(`Refusing to record ${record.kind} for ${stage}: it ${refusal}.`);
  const key = `${stageRecordPrefix(scope, stage)}${[record.worker, record.kind, ...recordFields(record)].map(encodeURIComponent).join('/')}`;
  if (Buffer.byteLength(key) > MAX_KEY_BYTES) {
    throw new Error(`Stage record key ${key} is longer than the ${MAX_KEY_BYTES} bytes R2 allows in a key.`);
  }
  return key;
}

/**
 * The distinct keys of `records`, in order. Several `*.` routes on one host share one DNS record,
 * and R2 refuses more than about one write per second to one key.
 */
export function stageRecordKeys(scope: string, stage: `pr${number}`, records: StageRecord[]): string[] {
  return [...new Set(records.map((record) => stageRecordKey(scope, stage, record)))];
}

/** Throws on anything outside the grammar or the stage; cleanup then deletes nothing. */
export function parseStageRecordKey(scope: string, stage: `pr${number}`, key: string): StageRecord {
  const prefix = stageRecordPrefix(scope, stage);
  const record = key.startsWith(prefix) ? readRecord(key.slice(prefix.length).split('/')) : undefined;
  if (!record) throw new Error(`Stage record ${key} is not a ${KEY_VERSION} record key under ${prefix}.`);
  const refusal = stageRecordRefusal(record, stage);
  if (refusal) throw new Error(`Stage record ${key} ${refusal}.`);
  // What a deploy would write for this record, byte for byte; anything else was not written by one.
  if (stageRecordKey(scope, stage, record) !== key) {
    throw new Error(`Stage record ${key} is not encoded the way a deploy writes it.`);
  }
  return record;
}

/** The records one pull-request plan implies; `zones` is read once, and only for a route without `zone_name`. */
export async function plannedStageRecords(
  plan: PullRequestResourcePlan,
  zones: () => Promise<CloudflareZone[]>,
): Promise<StageRecord[]> {
  const worker = plan.workerName;
  const records: StageRecord[] = [{ kind: 'worker', worker }];
  for (const { title } of plan.kvNamespaces) records.push({ kind: 'kv', worker, title });
  for (const { name } of plan.d1Databases) records.push({ kind: 'd1', worker, name });
  for (const { bucketName } of plan.r2Buckets) records.push({ kind: 'r2', worker, bucket: bucketName });
  let listed: Promise<CloudflareZone[]> | undefined;
  const accountZones = () => {
    listed ??= zones();
    return listed;
  };
  for (const route of plan.routes) {
    if (route.customDomain) {
      records.push({ kind: 'domain', worker, hostname: route.pattern.toLowerCase() });
    } else {
      records.push({ kind: 'route', worker, zone: await routeZone(route, accountZones), pattern: route.pattern });
    }
    // The one derivation reconcile creates the record from, so what is recorded is what is created.
    const dns = wildcardDnsRecord(route);
    if (dns) records.push({ kind: 'dns', worker, zone: dns.zoneName.toLowerCase(), name: dns.name });
  }
  return records;
}

/** The most specific zone whose name is `hostname` or one of its parents. */
export function zoneContaining(zones: CloudflareZone[], hostname: string): CloudflareZone | undefined {
  const host = hostname.toLowerCase();
  return zones
    .filter((zone) => {
      const name = zone.name.toLowerCase();
      return host === name || host.endsWith(`.${name}`);
    })
    .sort((left, right) => right.name.length - left.name.length)[0];
}

/**
 * The name of the zone a route binds: declared, else the one its `zone_id` names (even when a more
 * specific zone contains the host), else the most specific one containing its host.
 */
async function routeZone(route: PlannedRoute, zones: () => Promise<CloudflareZone[]>): Promise<string> {
  if (route.zoneName) return route.zoneName.toLowerCase();
  const listed = await zones();
  const zone = route.zoneId
    ? listed.find((candidate) => candidate.id === route.zoneId)
    : zoneContaining(listed, routeHostname(route.pattern));
  if (!zone) {
    const missing = route.zoneId ? `has the zone_id ${route.zoneId}` : 'contains its host';
    throw new Error(`No Cloudflare zone of the account ${missing} for route ${route.pattern}; declare its zone_name.`);
  }
  return zone.name.toLowerCase();
}

/** The fields after `<worker>/<kind>`, in key order. */
function recordFields(record: StageRecord): string[] {
  switch (record.kind) {
    case 'worker':
      return [];
    case 'kv':
      return [record.title];
    case 'd1':
      return [record.name];
    case 'r2':
      return [record.bucket];
    case 'domain':
      return [record.hostname];
    case 'route':
      return [record.zone, record.pattern];
    case 'dns':
      return [record.zone, record.name];
  }
}

/**
 * Why `record` is not the stage's own, or nothing when it is. The Worker and every name the stage
 * derived must carry `stage` as an exact segment, which is what keeps a stray record from ever
 * naming a shared, staging or production item. A zone carries no stage, and a route's pattern
 * need not: a route belongs to the Worker its script names, and the record's Worker carries it.
 */
function stageRecordRefusal(record: StageRecord, stage: `pr${number}`): string | undefined {
  if ([record.worker, ...recordFields(record)].some((field) => !field)) return 'has an empty field';
  const names = [record.worker];
  if (record.kind === 'kv') names.push(record.title);
  if (record.kind === 'd1') names.push(record.name);
  if (record.kind === 'r2') names.push(record.bucket);
  if (record.kind === 'domain') names.push(record.hostname);
  if (record.kind === 'dns') names.push(record.name);
  const foreign = names.find((name) => !hasExactStageSegment(name, stage));
  return foreign === undefined ? undefined : `names ${foreign}, which has no exact ${stage} segment`;
}

/** `<worker>/<kind>/<fields…>` decoded, or nothing when it is outside the grammar. */
function readRecord(segments: string[]): StageRecord | undefined {
  const fields: string[] = [];
  for (const segment of segments) {
    try {
      fields.push(decodeURIComponent(segment));
    } catch {
      return undefined;
    }
  }
  const [worker = '', kind, first = '', second = ''] = fields;
  switch (`${kind}/${fields.length}`) {
    case 'worker/2':
      return { kind: 'worker', worker };
    case 'kv/3':
      return { kind: 'kv', worker, title: first };
    case 'd1/3':
      return { kind: 'd1', worker, name: first };
    case 'r2/3':
      return { kind: 'r2', worker, bucket: first };
    case 'domain/3':
      return { kind: 'domain', worker, hostname: first };
    case 'route/4':
      return { kind: 'route', worker, zone: first, pattern: second };
    case 'dns/4':
      return { kind: 'dns', worker, zone: first, name: second };
    default:
      return undefined;
  }
}
