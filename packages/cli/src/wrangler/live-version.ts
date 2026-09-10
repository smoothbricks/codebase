// What is live right now, as opposed to what we last uploaded.
//
// `wrangler versions deploy` returns when Cloudflare ACCEPTS the traffic shift, not when the edge
// serves it. A deploy that returns is therefore not yet a deploy that answers: the site that signs
// in to its stage's backend measured the previous backend version's response about 20 s after the
// backend's deploy step had already gone green. Everything here exists to turn "we asked" into
// "we observed".

import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import typia from 'typia';

/** Same shape as the release tooling's Result: a known operational failure is a value, not a throw. */
export type Result<T, E> = { ok: true; value: T } | { ok: false; error: E };

/** The version Cloudflare is serving to 100% of traffic for one worker. */
export interface LiveVersion {
  versionId: string;
  /** `wrangler versions deploy --version-tag` matches on this; a version deployed outside Nx has none. */
  tag: string | null;
}

export type LiveVersionFailure =
  | { kind: 'never-deployed'; message: string }
  | { kind: 'split-traffic'; message: string };

/** Reads the two `wrangler` JSON payloads the live version is derived from. */
export interface LiveVersionProbe {
  /** `wrangler deployments status --name <worker> --json` */
  deployments(): Promise<unknown>;
  /** `wrangler versions list --name <worker> --json` */
  versions(): Promise<unknown>;
}

const isUnknownRecord = typia.createIs<Record<string, unknown>>();

/**
 * Every `{ id -> tag }` pair the payload carries, in document order.
 *
 * Wrangler nests versions differently per subcommand and per version of itself, spells the tag
 * three ways (`annotations['workers/tag']`, a bare `tag`, `metadata.tag`) and the id two (`id`,
 * `version_id`). One walk collects the relation once; both directions of the lookup are then a Map
 * read rather than a second recursive scan that could disagree with the first.
 */
export function collectVersionTags(value: unknown): Map<string, string> {
  const pairs = new Map<string, string>();
  collectInto(value, pairs);
  return pairs;
}

function collectInto(value: unknown, pairs: Map<string, string>): void {
  if (Array.isArray(value)) {
    for (const entry of value) collectInto(entry, pairs);
    return;
  }
  if (!isUnknownRecord(value)) return;
  const annotations = isUnknownRecord(value.annotations) ? value.annotations : undefined;
  const metadata = isUnknownRecord(value.metadata) ? value.metadata : undefined;
  const annotationTag = annotations?.['workers/tag'];
  const candidateTag =
    typeof annotationTag === 'string' ? annotationTag : typeof value.tag === 'string' ? value.tag : metadata?.tag;
  const candidateId =
    typeof value.id === 'string' ? value.id : typeof value.version_id === 'string' ? value.version_id : undefined;
  if (typeof candidateTag === 'string' && candidateId !== undefined && !pairs.has(candidateId)) {
    pairs.set(candidateId, candidateTag);
  }
  for (const nested of Object.values(value)) collectInto(nested, pairs);
}

/** The version id whose tag is `tag`, or null. */
export function findVersionIdByTag(value: unknown, tag: string): string | null {
  for (const [id, candidate] of collectVersionTags(value)) {
    if (candidate === tag) return id;
  }
  return null;
}

/** One version's share of the current deployment's traffic. */
export interface DeployedVersionShare {
  versionId: string | null;
  percentage: number;
}

/**
 * The current deployment's traffic split, or null when the payload names no deployment at all.
 *
 * "Nothing is deployed" and "two versions share the traffic" are different facts with different
 * remedies, and a caller that collapses them cannot tell an operator which one it hit — so the
 * split is returned whole and both questions are answered from it.
 */
export function currentDeploymentVersions(value: unknown): DeployedVersionShare[] | null {
  if (Array.isArray(value)) {
    for (const entry of value) {
      const found = currentDeploymentVersions(entry);
      if (found) return found;
    }
    return null;
  }
  if (!isUnknownRecord(value)) return null;
  if (Array.isArray(value.versions)) {
    return value.versions.map((version) => {
      if (!isUnknownRecord(version)) return { versionId: null, percentage: 0 };
      const id = version.version_id ?? version.id;
      return { versionId: typeof id === 'string' ? id : null, percentage: Number(version.percentage) };
    });
  }
  for (const nested of Object.values(value)) {
    const found = currentDeploymentVersions(nested);
    if (found) return found;
  }
  return null;
}

/**
 * The one version serving 100% of traffic, or null when there is none or the deployment is split.
 *
 * A split is not "close enough": with two versions live, "is our version live" has no single
 * answer, and answering it optimistically is how a deploy reports success while part of the
 * traffic still reaches the old code.
 */
export function currentDeploymentVersionId(value: unknown): string | null {
  const shares = currentDeploymentVersions(value);
  if (shares?.length !== 1) return null;
  const [only] = shares;
  return only.percentage === 100 ? only.versionId : null;
}

/** The version serving all traffic for this worker, with the tag it was uploaded under. */
export async function readLiveVersion(probe: LiveVersionProbe): Promise<Result<LiveVersion, LiveVersionFailure>> {
  const deployments = await probe.deployments();
  const shares = currentDeploymentVersions(deployments);
  const versionId = currentDeploymentVersionId(deployments);
  if (!versionId) {
    return shares && shares.length > 0
      ? {
          ok: false,
          error: {
            kind: 'split-traffic',
            message: 'no single worker version is serving 100% of traffic; refusing to name one as live',
          },
        }
      : { ok: false, error: { kind: 'never-deployed', message: 'this worker has no active deployment' } };
  }
  const tag = collectVersionTags(await probe.versions()).get(versionId) ?? null;
  return { ok: true, value: { versionId, tag } };
}

/**
 * How long a deploy waits for the version it just activated to become the one being served.
 *
 * The bound is the point: an unbounded loop turns a stuck propagation into a hung CI job nobody
 * reads, and a fixed sleep turns it into a green job that lied. Both are worse than a red job
 * naming what it expected and what it saw.
 */
export const LIVE_VERSION_WAIT_BUDGET_MS = 120_000;
/** Cloudflare's control plane converges in seconds; polling faster than this only burns API quota. */
export const LIVE_VERSION_POLL_INTERVAL_MS = 2_000;

export interface LiveVersionWaitOptions {
  budgetMs?: number;
  intervalMs?: number;
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
}

export interface LiveVersionNotObserved {
  kind: 'not-observed';
  message: string;
  expected: string;
  observed: string;
}

/** One poll's verdict: the value that ends the wait, or how the world looked this time round. */
type Attempt<T> = { done: true; value: T } | { done: false; observed: string };

/**
 * Runs `attempt` until it succeeds or the budget expires, and reports the LAST thing it saw.
 *
 * Both waits below need the same deadline arithmetic and the same "what did you see instead"
 * record; sharing it keeps the two from drifting into disagreeing about when a wait is over.
 */
async function pollWithinBudget<T>(
  attempt: () => Promise<Attempt<T>>,
  options: LiveVersionWaitOptions,
): Promise<Result<T, { observed: string; waitedMs: number }>> {
  const budgetMs = options.budgetMs ?? LIVE_VERSION_WAIT_BUDGET_MS;
  const intervalMs = options.intervalMs ?? LIVE_VERSION_POLL_INTERVAL_MS;
  const now = options.now ?? Date.now;
  const sleep = options.sleep ?? sleepFor;
  const deadline = now() + budgetMs;
  for (;;) {
    const result = await attempt();
    if (result.done) return { ok: true, value: result.value };
    if (now() >= deadline) return { ok: false, error: { observed: result.observed, waitedMs: budgetMs } };
    await sleep(intervalMs);
  }
}

function sleepFor(ms: number): Promise<void> {
  const { promise, resolve } = Promise.withResolvers<void>();
  setTimeout(resolve, ms);
  return promise;
}

/**
 * Polls until the worker's live version carries `expectedTag`, or the budget runs out.
 *
 * The tag is the match, not the version id: the tag IS the desired-state identity (`nx-<task
 * hash>`), it is what both `wrangler deploy --tag` and `wrangler versions deploy --version-tag`
 * were told to make live, and it is the one name the caller knows before the upload has produced
 * a version id.
 *
 * Returns the failure rather than throwing it: a deploy that cannot be observed is an operational
 * outcome the caller has to report, not a broken invariant.
 */
export async function awaitLiveVersion(
  probe: LiveVersionProbe,
  expectedTag: string,
  options: LiveVersionWaitOptions = {},
): Promise<Result<LiveVersion, LiveVersionNotObserved>> {
  const polled = await pollWithinBudget<LiveVersion>(async () => {
    const live = await readLiveVersion(probe);
    if (!live.ok) return { done: false, observed: live.error.message };
    const { tag, versionId } = live.value;
    if (tag === expectedTag) return { done: true, value: live.value };
    return { done: false, observed: tag ? `${tag} (${versionId})` : `an untagged version (${versionId})` };
  }, options);
  if (polled.ok) return polled;
  return {
    ok: false,
    error: {
      kind: 'not-observed',
      expected: expectedTag,
      observed: polled.error.observed,
      message:
        `deployed version ${expectedTag} was not serving traffic after ` +
        `${Math.round(polled.error.waitedMs / 1000)}s; live is ${polled.error.observed}`,
    },
  };
}

/**
 * Exactly the call the wait makes. Demanding all of `typeof fetch` would force every caller — the
 * tests included — to also supply `preconnect`, which nothing here uses.
 */
export type FetchLike = (input: string, init?: RequestInit) => Promise<Response>;

export interface VersionEndpointWaitOptions extends LiveVersionWaitOptions {
  fetch?: FetchLike;
}

/**
 * Polls a worker-served endpoint until it answers with `expectedTag`.
 *
 * The control plane agreeing is necessary and not sufficient — that gap is exactly what this
 * change's predecessor measured — so a project that can prove the edge serves the new code says so
 * with an endpoint echoing its own version tag (Workers read it from the version-metadata
 * binding). The contract is deliberately one shape: the trimmed response body IS the tag.
 */
export async function awaitVersionEndpoint(
  url: string,
  expectedTag: string,
  options: VersionEndpointWaitOptions = {},
): Promise<Result<void, LiveVersionNotObserved>> {
  const request = options.fetch ?? fetch;
  const polled = await pollWithinBudget<void>(async () => {
    const observed = await probeVersionEndpoint(request, url);
    return observed === expectedTag ? { done: true, value: undefined } : { done: false, observed };
  }, options);
  if (polled.ok) return polled;
  return {
    ok: false,
    error: {
      kind: 'not-observed',
      expected: expectedTag,
      observed: polled.error.observed,
      message:
        `${url} did not report version ${expectedTag} after ` +
        `${Math.round(polled.error.waitedMs / 1000)}s; it reported ${polled.error.observed}`,
    },
  };
}

async function probeVersionEndpoint(request: FetchLike, url: string): Promise<string> {
  try {
    const response = await request(url, { headers: { accept: 'text/plain' } });
    const body = (await response.text()).trim();
    return response.ok ? body : `HTTP ${response.status} ${body}`.trim();
  } catch (error) {
    // A worker mid-rollout refuses connections; that is a poll result, not a reason to abandon the
    // wait. It only becomes the failure when it is still the answer at the deadline.
    return error instanceof Error ? error.message : String(error);
  }
}

/**
 * How long `smoo wrangler deployed-version` trusts its own last answer.
 *
 * This TTL is a convenience for operators and repeated local queries, NOT a correctness mechanism:
 * nothing that decides whether to deploy may read it. The deploy's own liveness check always calls
 * Cloudflare, because a cached "live already equals the desired tag" is precisely the belief a
 * rollback falsifies, and acting on it would skip the deploy that repairs the rollback.
 */
export const LIVE_VERSION_CACHE_TTL_MS = 45_000;

export interface LiveVersionCacheKey {
  accountId: string;
  workerName: string;
  stage: string;
}

export interface CachedLiveVersion {
  versionTag: string | null;
  versionId: string;
  fetchedAt: number;
}

const parseCachedLiveVersion = typia.json.createValidateParse<CachedLiveVersion>();

/**
 * One file per key, so two deploys running in parallel cannot clobber each other's answer; a
 * shared map would need a lock to say the same thing.
 */
export function liveVersionCachePath(cacheDirectory: string, key: LiveVersionCacheKey): string {
  const name = [key.accountId, key.workerName, key.stage].map(encodeURIComponent).join('_');
  return join(cacheDirectory, 'smoo-live-version', `${name}.json`);
}

export async function readCachedLiveVersion(
  cacheDirectory: string,
  key: LiveVersionCacheKey,
  ttlMs: number,
  now: () => number = Date.now,
): Promise<CachedLiveVersion | null> {
  let text: string;
  try {
    text = await readFile(liveVersionCachePath(cacheDirectory, key), 'utf8');
  } catch {
    return null;
  }
  const parsed = parseCachedLiveVersion(text);
  if (!parsed.success) return null;
  return now() - parsed.data.fetchedAt < ttlMs ? parsed.data : null;
}

export async function writeCachedLiveVersion(
  cacheDirectory: string,
  key: LiveVersionCacheKey,
  entry: CachedLiveVersion,
): Promise<void> {
  const path = liveVersionCachePath(cacheDirectory, key);
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(entry)}\n`);
}

/**
 * Where the cache lives. Nx's workspace data directory already exists, is already gitignored, and
 * is already wiped when the workspace is reset — three properties a hand-rolled directory would
 * each have to re-earn, and one ($HOME) that would leak one checkout's answers into another's.
 */
export function liveVersionCacheDirectory(workspaceRoot: string, environment: NodeJS.ProcessEnv): string {
  return environment.NX_WORKSPACE_DATA_DIRECTORY ?? join(workspaceRoot, '.nx', 'workspace-data');
}
