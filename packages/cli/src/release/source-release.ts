import { join } from 'node:path';
import { readPackageJson, readPackageJsonObject, repositoryInfo } from '../lib/workspace.js';
import { resolvePrivateNpmRegistry } from './private-npm.js';

export type SourceRepository =
  | { kind: 'github'; owner: string; repo: string }
  | { kind: 'forgejo'; apiBase: string; htmlBase: string; owner: string; repo: string };

export function parseSourceRepository(url: string): SourceRepository {
  const original = url;
  let value = url.trim().replace(/^git\+/i, '');
  const fragment = value.indexOf('#');
  if (fragment >= 0) {
    value = value.slice(0, fragment);
  }
  value = value.replace(/\.git$/i, '');

  const githubShorthand = /^github:([^/]+)\/([^/]+)$/i.exec(value);
  if (githubShorthand) {
    return { kind: 'github', owner: githubShorthand[1], repo: githubShorthand[2] };
  }

  const scp = /^git@([^:/?#]+):(.+)$/.exec(value);
  if (scp) {
    const segments = repositoryPathSegments(scp[2]);
    if (/^(?:www\.)?github\.com$/i.test(scp[1])) {
      if (segments.length === 2) {
        return { kind: 'github', owner: segments[0], repo: segments[1] };
      }
      throw new Error(`Unable to parse source repository URL: ${original}`);
    }
    if (segments.length >= 2) {
      return forgejoRepository('https', scp[1], segments);
    }
    throw new Error(`Unable to parse source repository URL: ${original}`);
  }

  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error(`Unable to parse source repository URL: ${original}`);
  }

  const segments = repositoryPathSegments(parsed.pathname);
  if (/^(?:www\.)?github\.com$/i.test(parsed.hostname)) {
    if (segments.length === 2) {
      return { kind: 'github', owner: segments[0], repo: segments[1] };
    }
    throw new Error(`Unable to parse source repository URL: ${original}`);
  }
  if ((parsed.protocol === 'http:' || parsed.protocol === 'https:') && segments.length >= 2) {
    return forgejoRepository(parsed.protocol.slice(0, -1), parsed.host, segments);
  }
  throw new Error(`Unable to parse source repository URL: ${original}`);
}

export function resolveSourceRepository(root: string): SourceRepository {
  const packageInfo = readPackageJson(join(root, 'package.json'));
  const repository = packageInfo ? repositoryInfo(packageInfo.json) : null;
  if (!repository?.url?.trim()) {
    throw new Error(`Source repository is missing from ${join(root, 'package.json')}.`);
  }
  return parseSourceRepository(repository.url);
}

export function sourceReleaseUrl(repo: SourceRepository, tag: string): string {
  const encodedTag = encodeURIComponent(tag);
  if (repo.kind === 'github') {
    return `https://github.com/${repo.owner}/${repo.repo}/releases/tag/${encodedTag}`;
  }
  return `${repo.htmlBase}/${repo.owner}/${repo.repo}/releases/tag/${encodedTag}`;
}

/**
 * The CI forge's own identity, exactly as `github-ci/api.ts::ciApiContext`
 * resolves it: that function owns server/API/repository resolution and already
 * refuses an API URL outside the CI server's origin. Passed in as data so this
 * module stays free of the import cycle api.ts -> source-release.ts.
 */
export interface CiForgeContext {
  forgejo: boolean;
  /** API base inside the CI server's own origin; on a runner this is often an internal address. */
  apiBase: string;
  /** `owner/repo` the ambient credential was issued for. */
  repository: string;
}

/** Where source-release API calls go, plus the credential for that one origin. */
export interface SourceReleaseEndpoint {
  /** `<apiBase>/repos/<owner>/<repo>`; release paths append to this. */
  repositoryApi: string;
  /** Variable that supplied the token: safe to log. The token is not. */
  envName: string;
  token: string;
}

/**
 * Ambient credential names, in the order `github-ci/api.ts::ciApiRequest`
 * consumes them. They are only ever paired with an endpoint taken from the
 * same CI context, so the credential cannot leave the origin that issued it.
 */
const CI_TOKEN_ENVS = ['FORGEJO_TOKEN', 'GH_TOKEN', 'GITHUB_TOKEN'] as const;

/**
 * Every credential that may authenticate a source release, each already paired
 * with the one endpoint it is allowed to reach, in resolution order. The
 * pairing is the security property: a credential is never offered an origin
 * other than the one that issued it.
 *
 * - In CI for this very repository, the ambient credential is used against the
 *   CI server's own API base. A runner reaches its instance through whatever
 *   address its network gives it -- the observed Forgejo GARM runner checks out
 *   `http://10.89.0.1:3000/<owner>/<repo>` while the repository's public URL is
 *   the https host -- and no environment signal proves those are one host. So
 *   the ambient token is never sent to the public URL on the strength of a
 *   guess: it goes back to the origin that minted it, for the repository it
 *   was minted for. A foreign instance serving a same-named repository can
 *   therefore only ever be handed its own credential, never ours.
 * - The declared private-npm publisher variable is that forge's own
 *   credential, so it pairs with the canonical public API and only when the
 *   declared registry lives on the source host. It remains available in CI
 *   too: a runner whose forge issued no token still releases, through the
 *   public host it was declared for.
 */
function sourceReleaseCandidates(
  repo: SourceRepository,
  root: string,
  ci: CiForgeContext | null,
): Array<{ envName: string; repositoryApi: string }> {
  const candidates: Array<{ envName: string; repositoryApi: string }> = [];
  if (ciHostsSource(repo, ci)) {
    const repositoryApi = `${ci.apiBase}/repos/${ci.repository}`;
    for (const envName of CI_TOKEN_ENVS) {
      candidates.push({ envName, repositoryApi });
    }
  }
  const declared = readPackageJsonObject(join(root, 'package.json'))?.smoo?.privateNpm?.publishTokenEnv;
  if (repo.kind === 'forgejo' && declared && declaredRegistryHostsSource(new URL(repo.htmlBase).host, root)) {
    candidates.push({ envName: declared, repositoryApi: forgejoApiUrl(repo, '') });
  }
  return candidates;
}

/** Candidate variable names in resolution order; for diagnostics, never values. */
export function sourceReleaseTokenEnvNames(repo: SourceRepository, root: string, ci: CiForgeContext | null): string[] {
  return sourceReleaseCandidates(repo, root, ci).map((candidate) => candidate.envName);
}

/**
 * The endpoint and credential for source-release API calls, or null when
 * nothing is configured. Release links stay canonical and public regardless of
 * which endpoint served the call: see sourceReleaseUrl.
 */
export function resolveSourceReleaseEndpoint(
  repo: SourceRepository,
  root: string,
  ci: CiForgeContext | null,
  env: Record<string, string | undefined> = process.env,
): SourceReleaseEndpoint | null {
  for (const { envName, repositoryApi } of sourceReleaseCandidates(repo, root, ci)) {
    const token = env[envName];
    if (token) {
      return { repositoryApi, envName, token };
    }
  }
  return null;
}

/**
 * Whether the CI context is this repository's own forge: same forge family as
 * the configured source and the same `owner/repo`. The repository match is
 * what makes the ambient credential the right one to use; the addressing
 * guarantee comes from taking the API base from this same context, never from
 * comparing an internal address to a public host.
 */
function ciHostsSource(repo: SourceRepository, ci: CiForgeContext | null): ci is CiForgeContext {
  return (
    ci !== null &&
    ci.forgejo === (repo.kind === 'forgejo') &&
    ci.repository.toLowerCase() === `${repo.owner}/${repo.repo}`.toLowerCase()
  );
}

function declaredRegistryHostsSource(sourceHost: string, root: string): boolean {
  const resolved = resolvePrivateNpmRegistry(root);
  return resolved.ok && new URL(resolved.value.registry).host === sourceHost;
}

export function forgejoReleaseLookupExists(status: number, body: string, tag: string): boolean {
  if (status === 200) {
    return true;
  }
  if (status === 404) {
    return false;
  }
  const snippet = body.trim().slice(0, 500);
  throw new Error(`Unable to inspect source release ${tag} (HTTP ${status}).${snippet ? `\n${snippet}` : ''}`);
}

export function forgejoApiUrl(repo: Extract<SourceRepository, { kind: 'forgejo' }>, path: string): string {
  return `${repo.apiBase}/repos/${repo.owner}/${repo.repo}${path}`;
}

export function forgejoAuthHeaders(token: string): Record<string, string> {
  return { 'Content-Type': 'application/json', Authorization: `token ${token}` };
}

function repositoryPathSegments(path: string): string[] {
  const segments = path.split('/').filter(Boolean);
  if (segments.length > 0) {
    segments[segments.length - 1] = segments[segments.length - 1].replace(/\.git$/i, '');
  }
  return segments.filter(Boolean);
}

function forgejoRepository(scheme: string, host: string, segments: string[]): SourceRepository {
  const baseSegments = segments.slice(0, -2);
  const base = `${scheme}://${host}${baseSegments.length > 0 ? `/${baseSegments.join('/')}` : ''}`;
  return {
    kind: 'forgejo',
    apiBase: `${base}/api/v1`,
    htmlBase: base,
    owner: segments[segments.length - 2],
    repo: segments[segments.length - 1],
  };
}
