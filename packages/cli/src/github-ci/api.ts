import typia from 'typia';
import {
  parseRetryAfter,
  processRetryShell,
  type RetryPolicy,
  type RetryShell,
  type RetryStep,
  retryTransient,
} from '../lib/retry.js';
import { transportFailureDetail } from '../release/durable-state.js';
import { forgejoAuthHeaders } from '../release/source-release.js';

export function ciApiContext(environment: NodeJS.ProcessEnv = process.env) {
  const server = new URL(environment.FORGEJO_SERVER_URL || environment.GITHUB_SERVER_URL || 'https://github.com');
  if (
    !['https:', 'http:'].includes(server.protocol) ||
    server.username ||
    server.password ||
    server.search ||
    server.hash
  ) {
    throw new Error('CI server URL must be an HTTP(S) URL without credentials, query, or fragment.');
  }
  const htmlBase = server.href.replace(/\/$/, '');
  const configuredApi = environment.FORGEJO_API_URL || environment.GITHUB_API_URL;
  const forgejo =
    Boolean(environment.FORGEJO_ACTIONS || environment.FORGEJO_SERVER_URL) ||
    (configuredApi ? /\/api\/v1\/?$/.test(configuredApi) : server.hostname !== 'github.com');
  const apiBase = (configuredApi || (forgejo ? `${htmlBase}/api/v1` : 'https://api.github.com')).replace(/\/$/, '');
  const api = new URL(apiBase);
  if (
    api.username ||
    api.password ||
    api.search ||
    api.hash ||
    (api.origin !== server.origin && !(server.hostname === 'github.com' && api.origin === 'https://api.github.com'))
  ) {
    throw new Error('CI API URL must belong to the configured CI server.');
  }
  const repository = environment.FORGEJO_REPOSITORY || environment.GITHUB_REPOSITORY;
  if (
    !repository ||
    !/^[^/\s?#]+\/[^/\s?#]+$/.test(repository) ||
    repository.split('/').some((part) => part === '.' || part === '..')
  ) {
    throw new Error('CI repository must identify an owner and repository.');
  }
  return { forgejo, apiBase, htmlBase, repository };
}

/** Opt-in retrying of transient failures. */
export interface CiApiRetry {
  policy: RetryPolicy;
  shell: RetryShell;
}

/** What a call without `retry` gets: one attempt, so the first failure is thrown as it is. */
const SEND_ONCE: CiApiRetry = {
  policy: { attempts: 1, baseDelayMs: 0, maxDelayMs: 0, maxRetryAfterMs: 0 },
  shell: processRetryShell,
};

/**
 * One request. With `retry`, a 5xx answer (honouring its Retry-After) and a request that died in transport are sent
 * again within the policy; the caller opts in because repeating is only safe for a read or an idempotent write.
 * Everything else, 4xx included, is the forge's verdict and fails at once.
 */
export async function ciApiRequest(
  path: string,
  method: 'GET' | 'POST',
  body?: unknown,
  environment: NodeJS.ProcessEnv = process.env,
  retry: CiApiRetry = SEND_ONCE,
): Promise<Response> {
  const context = ciApiContext(environment);
  const token = environment.FORGEJO_TOKEN || environment.GH_TOKEN || environment.GITHUB_TOKEN;
  if (!token) throw new Error('CI API access requires FORGEJO_TOKEN, GH_TOKEN, or GITHUB_TOKEN.');
  const attempt = async (): Promise<RetryStep<Response>> => {
    let response: Response;
    try {
      response = await fetch(`${context.apiBase}/repos/${context.repository}${path}`, {
        method,
        headers: context.forgejo
          ? forgejoAuthHeaders(token)
          : {
              Authorization: `Bearer ${token}`,
              Accept: 'application/vnd.github+json',
              'Content-Type': 'application/json',
            },
        body: body === undefined ? undefined : JSON.stringify(body),
        redirect: 'error',
      });
    } catch (error) {
      // Only a request that died in transport can be repeated; anything else (a refused redirect, a bad URL) is a bug.
      const detail = transportFailureDetail(error);
      if (detail === null) throw error;
      return {
        kind: 'transient',
        error: error instanceof Error ? error : new Error(detail, { cause: error }),
        reason: detail,
      };
    }
    if (response.ok) return { kind: 'done', value: response };
    // Do not print response bodies: a proxy can echo credentials or request headers.
    const failure = new Error(`CI API ${method} ${path} failed with HTTP ${response.status}.`);
    if (response.status < 500) throw failure;
    // Release the socket before the retry; a body nobody reads would hold the connection.
    await response.body?.cancel().catch(() => undefined);
    return {
      kind: 'transient',
      error: failure,
      reason: `HTTP ${response.status}`,
      retryAfterMs: parseRetryAfter(response.headers.get('Retry-After'), Date.now()),
    };
  };
  return retryTransient(attempt, retry.policy, retry.shell);
}

export async function dispatchCiWorkflow(workflow: string, ref: string): Promise<void> {
  if (!workflow || !ref) throw new Error('Workflow dispatch requires a workflow and ref.');
  await ciApiRequest(`/actions/workflows/${encodeURIComponent(workflow)}/dispatches`, 'POST', { ref });
}

export interface CiCommitStatus {
  state: 'pending' | 'success' | 'failure' | 'error';
  context: string;
  description: string;
  target_url?: string;
}

/**
 * Five attempts, waiting 1, 2, 4 and 8 s between them (15 s in all): long enough to outlast the forge's seconds-long
 * 5xx blips, short enough that a real outage still ends the job quickly. A Retry-After beyond 15 s ends the retrying.
 */
const CI_STATUS_RETRY_POLICY: RetryPolicy = {
  attempts: 5,
  baseDelayMs: 1000,
  maxDelayMs: 8000,
  maxRetryAfterMs: 15_000,
};

/**
 * Records a commit status, repeating it over a 5xx or a dropped connection. Safe to repeat: a status is a log entry
 * whose latest state per context wins, so an attempt whose answer was lost after the forge stored it only adds an
 * identical entry. A 4xx is the forge's verdict (bad token, unknown commit, invalid state) and is never repeated.
 *
 * A failure names the status that could not be recorded, so it reads as a status-reporting failure rather than as the
 * work's own.
 */
export async function postCiStatus(
  sha: string,
  status: CiCommitStatus,
  environment: NodeJS.ProcessEnv = process.env,
  shell: RetryShell = processRetryShell,
): Promise<void> {
  try {
    await ciApiRequest(`/statuses/${sha}`, 'POST', status, environment, { policy: CI_STATUS_RETRY_POLICY, shell });
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    throw new Error(`Could not record commit status "${status.context}" as ${status.state}: ${reason}`, {
      cause: error,
    });
  }
}

export async function ensureCiPullRequest(options: {
  head: string;
  base: string;
  title: string;
  body: string;
}): Promise<void> {
  const { repository } = ciApiContext();
  for (let page = 1; ; page += 1) {
    const response = await ciApiRequest(`/pulls?state=open&limit=50&per_page=50&page=${page}`, 'GET');
    const pulls = typia.json.assertParse<
      Array<{
        head: { ref: string; repo: { full_name: string } | null };
        base: { ref: string };
      }>
    >(await response.text());
    if (
      pulls.some(
        (pull) =>
          pull.head.ref === options.head && pull.head.repo?.full_name === repository && pull.base.ref === options.base,
      )
    ) {
      console.log(`Review pull request already exists for ${options.head}.`);
      return;
    }
    if (pulls.length === 0) break;
  }
  await ciApiRequest('/pulls', 'POST', options);
}
