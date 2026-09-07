import { join } from 'node:path';
import { readPackageJson, readPackageJsonObject, repositoryInfo } from '../lib/workspace.js';

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

export function resolveSourceReleaseToken(
  root: string,
  env: Record<string, string | undefined> = process.env,
): string | null {
  const declared = readPackageJsonObject(join(root, 'package.json'))?.smoo?.privateNpm?.publishTokenEnv;
  return env.GH_TOKEN || env.GITHUB_TOKEN || (declared ? env[declared] : undefined) || null;
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
