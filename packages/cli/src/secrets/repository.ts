/**
 * Which GitHub repository `smoo secrets` reads and writes.
 *
 * `gh` refuses on a checkout with more than one remote ("multiple remotes
 * detected"), which is exactly the public/private mirror layout this CLI's own
 * users run — so the repository has to be resolved here, from the checkout,
 * before `gh` is spawned. The current branch's upstream decides it: a branch
 * that pushes to the private mirror is a branch whose secrets live there. That
 * is a fact the checkout already records, not a preference to configure.
 */

import { spawnSync } from 'node:child_process';

export interface RepositoryChoice {
  /** `owner/name`, the form `gh --repo` takes. */
  readonly repo: string;
  /** Why this repository, for the operator to read back. */
  readonly source: string;
}

/** `owner/name` from any git remote URL form, or null when it names no repository. */
export function repositorySlugFromUrl(url: string): string | null {
  const withoutProtocol = url
    .trim()
    .replace(/^[a-z+]+:\/\//i, '')
    .replace(/^[^@/]+@/, '');
  // A remainder starting with `/` or `.` carries no host, so it is a path on
  // this machine - a valid git remote, never a GitHub repository.
  if (withoutProtocol.startsWith('/') || withoutProtocol.startsWith('.')) return null;
  const path = withoutProtocol.replace(/^[^/:]+(?::\d+)?[/:]/, '');
  const segments = path
    .replace(/\.git$/i, '')
    .split('/')
    .filter((segment) => segment.length > 0);
  if (segments.length < 2) return null;
  return `${segments[segments.length - 2]}/${segments[segments.length - 1]}`;
}

/**
 * Pure resolution, so the precedence is testable without a checkout.
 *
 * A requested value is either a remote name or an `owner/name` slug; naming a
 * remote is what an operator reaches for first (`-R private-repo`), and
 * refusing it because it lacks a slash would be pedantry.
 *
 * With no request and no upstream - a branch pushed by explicit refspec, which
 * is how the mirror layout is driven - the remote-tracking refs still record
 * where this branch has been pushed. One remote carrying it is an answer; two
 * is a genuine ambiguity worth refusing.
 */
export function chooseRepository(inputs: {
  readonly remotes: ReadonlyMap<string, string>;
  readonly upstreamRemote: string | null;
  /** Remotes that already carry the current branch, from its remote-tracking refs. */
  readonly remotesCarryingBranch?: readonly string[];
  /** Current branch name, for the refusal to say which branch decided nothing. */
  readonly branch?: string | null;
  readonly requested: string | undefined;
}): { ok: true; choice: RepositoryChoice } | { ok: false; reason: string } {
  const { remotes, upstreamRemote, requested } = inputs;
  const remotesCarryingBranch = inputs.remotesCarryingBranch ?? [];
  if (requested !== undefined && requested.length > 0) {
    const url = remotes.get(requested);
    if (url !== undefined) {
      const slug = repositorySlugFromUrl(url);
      if (slug === null) return { ok: false, reason: `remote ${requested} (${url}) names no owner/name` };
      return { ok: true, choice: { repo: slug, source: `remote ${requested}` } };
    }
    if (requested.includes('/')) return { ok: true, choice: { repo: requested, source: 'requested' } };
    return {
      ok: false,
      reason:
        `${requested} is neither a remote of this checkout nor an owner/name. ` +
        `Remotes: ${[...remotes.keys()].join(', ') || 'none'}`,
    };
  }
  if (upstreamRemote !== null) {
    const url = remotes.get(upstreamRemote);
    const slug = url === undefined ? null : repositorySlugFromUrl(url);
    if (slug !== null) {
      return { ok: true, choice: { repo: slug, source: `upstream of the current branch (${upstreamRemote})` } };
    }
  }
  if (remotesCarryingBranch.length === 1) {
    const name = remotesCarryingBranch[0] ?? '';
    const url = remotes.get(name);
    const slug = url === undefined ? null : repositorySlugFromUrl(url);
    if (slug !== null) {
      return { ok: true, choice: { repo: slug, source: `the only remote carrying this branch (${name})` } };
    }
  }
  if (remotes.size === 1) {
    const [name, url] = [...remotes][0];
    const slug = repositorySlugFromUrl(url);
    if (slug !== null) return { ok: true, choice: { repo: slug, source: `the only remote (${name})` } };
  }
  const origin = remotes.get('origin');
  const originSlug = origin === undefined ? null : repositorySlugFromUrl(origin);
  if (originSlug !== null) return { ok: true, choice: { repo: originSlug, source: 'remote origin' } };
  const candidates = [...remotes].map(([name, url]) => `${name} -> ${repositorySlugFromUrl(url) ?? url}`).join(', ');
  const branchClause =
    inputs.branch === undefined || inputs.branch === null
      ? 'this checkout has no current branch'
      : `branch ${inputs.branch} has no upstream and no remote carries it`;
  return {
    ok: false,
    reason:
      `${branchClause}, and there is no origin, so the repository is ambiguous. ` +
      `Pass -R with one of: ${candidates || 'no remotes at all'}`,
  };
}

function git(root: string, args: readonly string[]): string | null {
  const result = spawnSync('git', [...args], { cwd: root, encoding: 'utf8' });
  return result.status === 0 ? result.stdout.trim() : null;
}

/** Remote name -> URL, in config order. */
export function readRemotes(root: string): Map<string, string> {
  const remotes = new Map<string, string>();
  const config = git(root, ['config', '--get-regexp', '^remote\\..*\\.url']);
  if (config === null) return remotes;
  for (const line of config.split('\n')) {
    const match = /^remote\.(.+)\.url\s+(.+)$/.exec(line.trim());
    if (match !== null && match[1] !== undefined && match[2] !== undefined) remotes.set(match[1], match[2]);
  }
  return remotes;
}

/** Remotes with a remote-tracking ref for the current branch: where it has been pushed. */
export function readRemotesCarryingBranch(root: string, remotes: ReadonlyMap<string, string>): string[] {
  const branch = git(root, ['rev-parse', '--abbrev-ref', 'HEAD']);
  if (branch === null || branch === 'HEAD') return [];
  const carrying: string[] = [];
  for (const name of remotes.keys()) {
    if (git(root, ['rev-parse', '--verify', '--quiet', `refs/remotes/${name}/${branch}`]) !== null) {
      carrying.push(name);
    }
  }
  return carrying;
}

/** The remote the current branch tracks, or null when it tracks nothing. */
export function readUpstreamRemote(root: string): string | null {
  const branch = git(root, ['rev-parse', '--abbrev-ref', 'HEAD']);
  if (branch === null || branch === 'HEAD') return null;
  return git(root, ['config', '--get', `branch.${branch}.remote`]);
}

/** The repository to operate on, resolved from the checkout. */
export function resolveRepository(
  root: string,
  requested: string | undefined,
): { ok: true; choice: RepositoryChoice } | { ok: false; reason: string } {
  const remotes = readRemotes(root);
  const branch = git(root, ['rev-parse', '--abbrev-ref', 'HEAD']);
  return chooseRepository({
    remotes,
    upstreamRemote: readUpstreamRemote(root),
    remotesCarryingBranch: readRemotesCarryingBranch(root, remotes),
    branch: branch === 'HEAD' ? null : branch,
    requested,
  });
}
