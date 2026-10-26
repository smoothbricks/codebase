/**
 * The scheduled safety net behind `cleanup-pr`: a close-time cleanup that failed, was cancelled or
 * never ran leaves a closed pull request's stage behind, and its Workers keep their Durable Object
 * namespaces against the account's limit of 500, past which every stage deploy fails (Cloudflare
 * 10067). The sweep finds every stage this repository holds records for, collects the ones whose pull
 * request is closed, and reports how close the account is to that limit.
 */
import typia from 'typia';
import { ciApiContext, ciApiRequest } from '../github-ci/api.js';
import { type CloudflareClient, CloudflareRestClient } from './cloudflare.js';
import { type CleanupResult, cleanupPullRequest, type WranglerCommandDependencies } from './deploy-stage.js';
import { recordedPullRequests, STAGE_RECORDS_BUCKET, scopeRecordPrefix, stageRecordScope } from './stage-records.js';

/** Cloudflare's per-account Durable Object namespace limit; the next namespace fails with 10067. */
export const DURABLE_OBJECT_NAMESPACE_LIMIT = 500;
/** The share of that limit at which the sweep fails loudly instead of waiting for deploys to fail. */
export const DURABLE_OBJECT_ALERT_RATIO = 0.8;

/** What the forge says about one pull request, reduced to what decides whether its stage may go. */
export interface PullRequestState {
  state: 'open' | 'closed';
  /** `owner/repo` of the head; null when the head repository was deleted. */
  headRepository: string | null;
  baseRepository: string;
}

/**
 * Only a closed pull request whose head and base are both this repository: an open one is in use, and a
 * stage is only ever deployed for a same-repository head, so anything else is not this repository's to
 * delete. Staging, e2e and production are never pull-request stages and never reach this question.
 */
export function isCollectable(pr: PullRequestState, repository: string): boolean {
  const own = repository.toLowerCase();
  return pr.state === 'closed' && pr.headRepository?.toLowerCase() === own && pr.baseRepository.toLowerCase() === own;
}

export interface NamespaceCapacity {
  count: number;
  limit: number;
  /** The count above which `over` is true. */
  alertAbove: number;
  over: boolean;
}

export function namespaceCapacity(count: number): NamespaceCapacity {
  const alertAbove = Math.floor(DURABLE_OBJECT_NAMESPACE_LIMIT * DURABLE_OBJECT_ALERT_RATIO);
  return { count, limit: DURABLE_OBJECT_NAMESPACE_LIMIT, alertAbove, over: count > alertAbove };
}

export interface StageSweepResult {
  scope: string;
  /** Every pull request with recorded stage items, ascending. */
  recorded: number[];
  collected: CleanupResult[];
  /** Pull requests left alone, with the state that kept them. */
  kept: { pr: number; state: PullRequestState['state'] }[];
  failed: { pr: number; error: string }[];
  /** Counted after the collection, so it reflects what the sweep freed. */
  namespaces: NamespaceCapacity;
}

export interface StageSweepDependencies {
  /** Only the reads the sweep makes itself; deleting is `cleanup`'s. */
  cloudflare: Pick<CloudflareClient, 'listR2Buckets' | 'listR2Objects' | 'listDurableObjectNamespaces'>;
  scope: string;
  /** `owner/repo` this run belongs to. */
  repository: string;
  pullRequestState(pr: number): Promise<PullRequestState>;
  cleanup(pr: number): Promise<CleanupResult>;
  /** Restricts the sweep to this one pull request, still collected only if closed. */
  only?: number;
}

/**
 * Collects every recorded stage whose pull request is closed. One stage's failure does not stop the
 * others; it is reported, and the next sweep retries it, since `cleanup-pr` keeps the records of a
 * cleanup that stopped halfway. A state the forge cannot answer is a failure, never read as closed.
 */
export async function sweepPullRequestStages(dependencies: StageSweepDependencies): Promise<StageSweepResult> {
  const { cloudflare, scope, repository } = dependencies;
  const buckets = await cloudflare.listR2Buckets();
  const keys = buckets.some((bucket) => bucket.name === STAGE_RECORDS_BUCKET)
    ? await cloudflare.listR2Objects(STAGE_RECORDS_BUCKET, scopeRecordPrefix(scope))
    : [];
  const recorded = recordedPullRequests(scope, keys).filter(
    (pr) => dependencies.only === undefined || pr === dependencies.only,
  );
  const result: Omit<StageSweepResult, 'namespaces'> = { scope, recorded, collected: [], kept: [], failed: [] };
  for (const pr of recorded) {
    try {
      const state = await dependencies.pullRequestState(pr);
      if (!isCollectable(state, repository)) {
        result.kept.push({ pr, state: state.state });
        continue;
      }
      result.collected.push(await dependencies.cleanup(pr));
    } catch (error) {
      result.failed.push({ pr, error: error instanceof Error ? error.message : String(error) });
    }
  }
  const namespaces = namespaceCapacity((await cloudflare.listDurableObjectNamespaces()).length);
  return { ...result, namespaces };
}

/** True when the run must fail: a stage could not be collected, or the account is near its limit. */
export function sweepNeedsAttention(result: StageSweepResult): boolean {
  return result.failed.length > 0 || result.namespaces.over;
}

/** A Markdown report for a job summary and the log. */
export function describeSweep(result: StageSweepResult): string {
  const { namespaces } = result;
  const lines = [
    `## PR stage sweep for ${result.scope}`,
    '',
    `Durable Object namespaces: **${namespaces.count} / ${namespaces.limit}** (alert above ${namespaces.alertAbove}).`,
    `Recorded pull-request stages: ${result.recorded.length === 0 ? 'none' : result.recorded.map((pr) => `pr${pr}`).join(', ')}.`,
    `Collected (closed): ${result.collected.length === 0 ? 'none' : result.collected.map((cleanup) => cleanup.stage).join(', ')}.`,
    `Kept (open): ${result.kept.length === 0 ? 'none' : result.kept.map(({ pr }) => `pr${pr}`).join(', ')}.`,
  ];
  for (const { pr, error } of result.failed) lines.push(`- **pr${pr} failed:** ${error}`);
  if (namespaces.over) {
    lines.push(
      '',
      `**The account holds ${namespaces.count} of ${namespaces.limit} Durable Object namespaces.** Past ${namespaces.limit} every stage deploy fails with Cloudflare error 10067. What is left is held by open pull requests or by Workers deployed without stage records; find them by namespace in the Cloudflare dashboard.`,
    );
  }
  return `${lines.join('\n')}\n`;
}

const isPullRequestReply = typia.createIs<{
  state: 'open' | 'closed';
  head: { repo: { full_name: string } | null };
  base: { repo: { full_name: string } };
}>();

/** The pull request as the CI forge's API answers it; any failure throws, never "closed". */
export async function fetchPullRequestState(
  pr: number,
  environment: NodeJS.ProcessEnv = process.env,
): Promise<PullRequestState> {
  const body: unknown = JSON.parse(await ciApiRequest(`/pulls/${pr}`, 'GET', undefined, environment));
  if (!isPullRequestReply(body)) throw new Error(`The forge answered pull request ${pr} in an unexpected shape.`);
  return {
    state: body.state,
    headRepository: body.head.repo?.full_name ?? null,
    baseRepository: body.base.repo.full_name,
  };
}

/** `smoo wrangler sweep-pr-stages`, wired to the real forge and Cloudflare account. */
export async function sweepRepositoryStages(
  root: string,
  dependencies: WranglerCommandDependencies & { only?: number } = {},
): Promise<StageSweepResult> {
  const processEnv = dependencies.processEnv ?? process.env;
  const scope = stageRecordScope(root, processEnv);
  const accountId = processEnv.CLOUDFLARE_ACCOUNT_ID;
  const apiToken = processEnv.CLOUDFLARE_API_TOKEN;
  if (!accountId) throw new Error('CLOUDFLARE_ACCOUNT_ID is required.');
  if (!apiToken) throw new Error('CLOUDFLARE_API_TOKEN is required.');
  const cloudflare =
    dependencies.cloudflare ??
    new CloudflareRestClient(accountId, apiToken, undefined, processEnv.CLOUDFLARE_API_BASE_URL);
  // In CI the forge names the repository (and stageRecordScope has checked it against the manifest). A
  // local run may infer it only for a github.com repository: asking GitHub about another forge's
  // owner/repo would read an unrelated pull request's state and could delete a live stage.
  const forgeNamed = Boolean(processEnv.GITHUB_REPOSITORY || processEnv.FORGEJO_REPOSITORY);
  const github = /^github\.com\/([^/]+\/[^/]+)$/.exec(scope);
  // Inference is for a plain local shell only: any forge setting means the shell already speaks to some
  // forge, and guessing which one (or whose token GH_TOKEN holds) could read the wrong pull requests.
  const forgeSettings = [
    'FORGEJO_ACTIONS',
    'FORGEJO_SERVER_URL',
    'FORGEJO_API_URL',
    'FORGEJO_TOKEN',
    'GITHUB_SERVER_URL',
    'GITHUB_API_URL',
  ];
  if (!forgeNamed && (!github || forgeSettings.some((name) => processEnv[name]))) {
    throw new Error(
      `Cannot tell which forge ${scope} is on; set GITHUB_REPOSITORY (or FORGEJO_SERVER_URL and FORGEJO_REPOSITORY) to name it.`,
    );
  }
  const forgeEnv = forgeNamed || !github ? processEnv : { ...processEnv, GITHUB_REPOSITORY: github[1] };
  // Whichever variable named it, the repository whose pull requests are read must be the one whose
  // records are cleaned: another repository's closed #N says nothing about this repository's prN. In CI
  // the runner may know its forge by an internal address, so the forge's host and path are compared only
  // outside CI.
  const { repository, htmlBase, apiBase } = ciApiContext(forgeEnv);
  const forge = new URL(htmlBase);
  const forgeScope = `${forge.hostname}${forge.pathname.replace(/\/+$/, '')}/${repository}`.toLowerCase();
  // The API the lookups reach must be the forge's own, not a separately configured installation.
  const forgeApi = forge.hostname === 'github.com' ? 'https://api.github.com' : `${htmlBase}/api/v1`;
  const inCi = Boolean(processEnv.GITHUB_ACTIONS || processEnv.FORGEJO_ACTIONS);
  const sameRepository = inCi
    ? repository.toLowerCase() === scope.split('/').slice(-2).join('/')
    : forgeScope === scope && apiBase === forgeApi;
  if (!sameRepository) {
    throw new Error(
      `The forge names ${htmlBase}/${repository}, but the stage records belong to ${scope}; set FORGEJO_SERVER_URL or GITHUB_SERVER_URL and GITHUB_API_URL to its forge. Refusing to read one repository's pull requests to clean another's stages.`,
    );
  }
  return sweepPullRequestStages({
    cloudflare,
    scope,
    repository,
    ...(dependencies.only === undefined ? {} : { only: dependencies.only }),
    pullRequestState: (pr) => fetchPullRequestState(pr, forgeEnv),
    cleanup: (pr) => cleanupPullRequest(root, pr, { ...dependencies, cloudflare }),
  });
}
