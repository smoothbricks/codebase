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

/** Which environment variable supplied a source-release token, plus its value. */
export interface SourceReleaseCredential {
  /** Variable name: safe to log. The token is not. */
  envName: string;
  token: string;
}

/**
 * Environment variables that may authenticate source-release API calls for
 * `repo`, in resolution order.
 *
 * The ambient forge credentials (GH_TOKEN, GITHUB_TOKEN) count only when the
 * ambient forge *is* the source forge. Forgejo Actions exports GITHUB_TOKEN
 * and GITHUB_SERVER_URL exactly as GitHub Actions does, and a developer shell
 * normally holds a GitHub PAT in GH_TOKEN, so an ungated ambient token sends
 * one forge's credential to another forge's host: an unexplainable 401 in the
 * good case, credential disclosure in the bad one. GITHUB_SERVER_URL names
 * the forge that issued the ambient token.
 *
 * The declared private-npm publisher variable comes last, and only for a
 * source forge that also hosts the declared registry: it is that host's
 * credential, not a general-purpose one.
 */
export function sourceReleaseTokenEnvNames(
  repo: SourceRepository,
  root: string,
  env: Record<string, string | undefined> = process.env,
): string[] {
  const sourceHost = repo.kind === 'github' ? 'github.com' : new URL(repo.htmlBase).host;
  const names = ambientForgeIsSource(sourceHost, env) ? ['GH_TOKEN', 'GITHUB_TOKEN'] : [];
  const declared = readPackageJsonObject(join(root, 'package.json'))?.smoo?.privateNpm?.publishTokenEnv;
  if (declared && !names.includes(declared) && declaredRegistryHostsSource(sourceHost, root)) {
    names.push(declared);
  }
  return names;
}

/**
 * First configured credential for `repo`, naming the variable it came from so
 * a release log can say which credential authenticated without printing it.
 */
export function resolveSourceReleaseToken(
  repo: SourceRepository,
  root: string,
  env: Record<string, string | undefined> = process.env,
): SourceReleaseCredential | null {
  for (const envName of sourceReleaseTokenEnvNames(repo, root, env)) {
    const token = env[envName];
    if (token) {
      return { envName, token };
    }
  }
  return null;
}

function ambientForgeIsSource(sourceHost: string, env: Record<string, string | undefined>): boolean {
  const serverUrl = env.GITHUB_SERVER_URL;
  if (!serverUrl) {
    // No CI forge identity in the environment: ambient GitHub credentials are
    // conventionally GitHub's own (gh CLI, developer PATs), so they
    // authenticate a github.com source and nothing else.
    return sourceHost === 'github.com';
  }
  try {
    return new URL(serverUrl).host === sourceHost;
  } catch {
    return false;
  }
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
