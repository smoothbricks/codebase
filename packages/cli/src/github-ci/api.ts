import typia from 'typia';
import { RemoteHttpTimeoutError, remoteRequest } from '../lib/remote-http.js';
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

/**
 * One CI forge API request, answered with its whole body, within the remote-http bound. A request past the bound
 * throws `RemoteHttpTimeoutError`; an answer that is not 2xx throws its status.
 */
export async function ciApiRequest(
  path: string,
  method: 'GET' | 'POST',
  body?: unknown,
  environment: NodeJS.ProcessEnv = process.env,
): Promise<string> {
  const context = ciApiContext(environment);
  const token = environment.FORGEJO_TOKEN || environment.GH_TOKEN || environment.GITHUB_TOKEN;
  if (!token) throw new Error('CI API access requires FORGEJO_TOKEN, GH_TOKEN, or GITHUB_TOKEN.');
  const answer = await remoteRequest(
    `CI API ${method} ${path}`,
    `${context.apiBase}/repos/${context.repository}${path}`,
    {
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
    },
  );
  if (!answer.ok) throw new RemoteHttpTimeoutError(answer.error);
  if (!answer.value.ok) {
    // Do not print response bodies: a proxy can echo credentials or request headers.
    throw new Error(`CI API ${method} ${path} failed with HTTP ${answer.value.status}.`);
  }
  return answer.value.body;
}

export async function dispatchCiWorkflow(workflow: string, ref: string): Promise<void> {
  if (!workflow || !ref) throw new Error('Workflow dispatch requires a workflow and ref.');
  await ciApiRequest(`/actions/workflows/${encodeURIComponent(workflow)}/dispatches`, 'POST', { ref });
}

export async function ensureCiPullRequest(options: {
  head: string;
  base: string;
  title: string;
  body: string;
}): Promise<void> {
  const { repository } = ciApiContext();
  for (let page = 1; ; page += 1) {
    const listing = await ciApiRequest(`/pulls?state=open&limit=50&per_page=50&page=${page}`, 'GET');
    const pulls =
      typia.json.assertParse<
        Array<{
          head: { ref: string; repo: { full_name: string } | null };
          base: { ref: string };
        }>
      >(listing);
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
