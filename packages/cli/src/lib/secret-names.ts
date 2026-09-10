/**
 * One rule for naming the repository secret behind an environment variable, so
 * the workflow renderer, the reconciler and an operator all derive the same
 * name instead of maintaining a map.
 *
 * GitHub refuses to create a secret whose name starts with `GITHUB_` (hence
 * `RepositorySecretName`'s pattern), which is why a Worker's
 * `GITHUB_CLIENT_SECRET` cannot be stored under its own name. Prefixing with
 * the repository owner is the workaround operators already reach for by hand
 * (`ACME_GITHUB_CLIENT_SECRET` for `acme/app`); making it the convention means
 * nothing has to declare it.
 */

/** Names GitHub reserves: it rejects `gh secret set GITHUB_*` outright. */
export function isReservedRepositorySecretName(envName: string): boolean {
  return /^GITHUB_/i.test(envName);
}

/**
 * The repository secret an env name lives in. Identity for every name GitHub
 * accepts; owner-prefixed for the reserved ones. `owner` comes from the
 * repository URL, so a fork or a rename derives its own names rather than
 * inheriting a hardcoded prefix.
 */
export function conventionalRepositorySecretName(envName: string, owner: string): string {
  if (!isReservedRepositorySecretName(envName)) return envName;
  const prefix = owner
    .replace(/[^A-Za-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .toUpperCase();
  if (prefix.length === 0) {
    throw new Error(
      `Cannot derive a repository secret name for ${envName}: GitHub reserves the GITHUB_ prefix and the repository owner is empty.`,
    );
  }
  return `${prefix}_${envName}`;
}

/**
 * Resolve the whole env -> secret mapping for a set of declared env names.
 * Explicit entries win: an exception stays expressible, but a repository that
 * follows the convention declares nothing.
 */
export function repositorySecretMapping(
  envNames: readonly string[],
  owner: string,
  declared: Readonly<Record<string, string>> = {},
): Record<string, string> {
  const mapping: Record<string, string> = {};
  for (const envName of [...envNames].sort((left, right) => left.localeCompare(right))) {
    mapping[envName] = declared[envName] ?? conventionalRepositorySecretName(envName, owner);
  }
  for (const [envName, secretName] of Object.entries(declared)) {
    mapping[envName] = secretName;
  }
  return mapping;
}

/**
 * Repository owner from a repository URL — `acme` from
 * `https://github.com/acme/app.git`, and the same for an ssh or forge
 * URL. The owner is derived rather than configured so a fork or a rename
 * carries its own prefix instead of inheriting one.
 */
export function repositoryOwnerFromUrl(url: string): string | null {
  const withoutProtocol = url.replace(/^[a-z+]+:\/\//i, '').replace(/^git@/i, '');
  const path = withoutProtocol.replace(/^[^/:]+[/:]/, '');
  const owner = path.split('/')[0];
  return owner && owner.length > 0 ? owner.replace(/\.git$/i, '') : null;
}
