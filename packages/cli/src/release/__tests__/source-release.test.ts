import { describe, expect, it } from 'bun:test';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  forgejoApiUrl,
  forgejoAuthHeaders,
  forgejoReleaseLookupStatus,
  parseSourceRepository,
  resolveSourceReleaseEndpoint,
  resolveSourceRepository,
  sourceReleaseTokenEnvNames,
  sourceReleaseUrl,
} from '../source-release.js';

describe('source repository parsing', () => {
  it('parses GitHub HTTPS, SSH, and shorthand repositories', () => {
    expect(parseSourceRepository(' https://github.com/owner/repo.git#source ')).toEqual({
      kind: 'github',
      owner: 'owner',
      repo: 'repo',
    });
    expect(parseSourceRepository('git@github.com:owner/repo')).toEqual({
      kind: 'github',
      owner: 'owner',
      repo: 'repo',
    });
    expect(parseSourceRepository('github:owner/repo')).toEqual({
      kind: 'github',
      owner: 'owner',
      repo: 'repo',
    });
  });

  it('parses a Forgejo source repository', () => {
    expect(parseSourceRepository('https://forge.example.com/acme/widgets.git')).toEqual({
      kind: 'forgejo',
      apiBase: 'https://forge.example.com/api/v1',
      htmlBase: 'https://forge.example.com',
      owner: 'acme',
      repo: 'widgets',
    });
  });

  it('preserves a Forgejo base path in API and HTML URLs', () => {
    expect(parseSourceRepository('https://host/base/o/r.git')).toEqual({
      kind: 'forgejo',
      apiBase: 'https://host/base/api/v1',
      htmlBase: 'https://host/base',
      owner: 'o',
      repo: 'r',
    });
  });

  it('rejects unparseable source repository URLs while naming the input', () => {
    expect(() => parseSourceRepository('not a repository')).toThrow('not a repository');
  });
});

describe('source release URLs and Forgejo protocol helpers', () => {
  it('builds release URLs for GitHub and Forgejo', () => {
    expect(sourceReleaseUrl({ kind: 'github', owner: 'owner', repo: 'repo' }, 'v1/preview')).toBe(
      'https://github.com/owner/repo/releases/tag/v1%2Fpreview',
    );
    expect(
      sourceReleaseUrl(
        {
          kind: 'forgejo',
          apiBase: 'https://host/base/api/v1',
          htmlBase: 'https://host/base',
          owner: 'o',
          repo: 'r',
        },
        'v1',
      ),
    ).toBe('https://host/base/o/r/releases/tag/v1');
  });

  it('builds Forgejo API URLs without dropping the base path', () => {
    expect(
      forgejoApiUrl(
        {
          kind: 'forgejo',
          apiBase: 'https://host/base/api/v1',
          htmlBase: 'https://host/base',
          owner: 'o',
          repo: 'r',
        },
        '/releases/tags/v1',
      ),
    ).toBe('https://host/base/api/v1/repos/o/r/releases/tags/v1');
  });

  it('builds the Forgejo authorization headers', () => {
    expect(forgejoAuthHeaders('fixture-token')).toEqual({
      'Content-Type': 'application/json',
      Authorization: 'token fixture-token',
    });
  });

  it('treats only 200 as found and 404 as absent', () => {
    expect(forgejoReleaseLookupStatus(200, '{"id":1}', 'v1')).toEqual({ kind: 'exists' });
    expect(forgejoReleaseLookupStatus(404, '{"message":"not found"}', 'v1')).toEqual({ kind: 'absent' });
  });

  it('throws with status and response details for an unauthorized response', () => {
    expect(() => forgejoReleaseLookupStatus(401, '{"message":"unauthorized"}', 'v1')).toThrow(
      'Unable to inspect source release v1 (HTTP 401)',
    );
    expect(() => forgejoReleaseLookupStatus(401, '{"message":"unauthorized"}', 'v1')).toThrow('unauthorized');
  });

  it('reports an unavailable forge as undetermined rather than a verdict', () => {
    // 503 is the server saying it produced no answer. Read as absent it would
    // recreate a release that exists; read as a refusal it ends a release the
    // next attempt would have completed.
    expect(forgejoReleaseLookupStatus(503, 'service unavailable', 'v1')).toMatchObject({
      kind: 'undetermined',
      detail: expect.stringContaining('service unavailable'),
    });
  });
});

/**
 * A source-release token is a forge credential, and the only sound rule is
 * that it goes back to the origin that issued it. A runner reaches its own
 * instance through whatever address its network gives it (observed: the
 * Forgejo GARM runner checks out http://10.89.0.1:3000/<owner>/<repo> while
 * the repository's public URL is the https host), so no environment signal
 * proves an internal alias and a public host are the same forge. The endpoint
 * is therefore taken from the same CI context as the credential, never chosen
 * by comparing hosts.
 */
describe('source release endpoint selection', () => {
  const FORGEJO_REPO = { type: 'git', url: 'https://forge.example.test/fixture-owner/fixture-repo.git' };
  const PUBLIC_REPOSITORY_API = 'https://forge.example.test/api/v1/repos/fixture-owner/fixture-repo';
  const SAME_FORGE_NPMRC =
    '@fixture.test:registry=https://forge.example.test/api/packages/fixture-owner/npm/\n' +
    '//forge.example.test/api/packages/fixture-owner/npm/:_authToken=${DECLARED_PUBLISH_TOKEN}\n';
  const OTHER_FORGE_NPMRC =
    '@fixture.test:registry=https://other.example.test/api/packages/fixture-owner/npm/\n' +
    '//other.example.test/api/packages/fixture-owner/npm/:_authToken=${DECLARED_PUBLISH_TOKEN}\n';
  const manifest = {
    name: 'fixture-source',
    version: '1.0.0',
    repository: FORGEJO_REPO,
    smoo: { privateNpm: { scope: '@fixture.test', publishTokenEnv: 'DECLARED_PUBLISH_TOKEN' } },
  };
  /** What ciApiContext reports on the observed GARM runner: its own internal address. */
  const aliasCi = {
    forgejo: true,
    apiBase: 'http://10.89.0.1:3000/api/v1',
    repository: 'fixture-owner/fixture-repo',
  };

  it('sends the instance credential back to the instance that issued it, not to the public host', async () => {
    await withRoot(manifest, (root) => {
      const repo = parseSourceRepository(FORGEJO_REPO.url);

      const endpoint = resolveSourceReleaseEndpoint(repo, root, aliasCi, {
        FORGEJO_TOKEN: undefined,
        GITHUB_TOKEN: 'instance-issued-token',
      });

      expect(endpoint).toEqual({
        repositoryApi: 'http://10.89.0.1:3000/api/v1/repos/fixture-owner/fixture-repo',
        envName: 'GITHUB_TOKEN',
        token: 'instance-issued-token',
      });
      // The public host never receives the ambient credential.
      expect(endpoint?.repositoryApi).not.toContain('forge.example.test');
      expect(sourceReleaseTokenEnvNames(repo, root, aliasCi)).toEqual(['FORGEJO_TOKEN', 'GH_TOKEN', 'GITHUB_TOKEN']);
    });
  });

  it('prefers FORGEJO_TOKEN over the GitHub-named aliases, as the CI API path does', async () => {
    await withRoot(manifest, (root) => {
      const repo = parseSourceRepository(FORGEJO_REPO.url);

      expect(
        resolveSourceReleaseEndpoint(repo, root, aliasCi, {
          FORGEJO_TOKEN: 'forgejo-token',
          GH_TOKEN: 'gh-token',
          GITHUB_TOKEN: 'github-token',
        })?.envName,
      ).toBe('FORGEJO_TOKEN');
    });
  });

  it('refuses to spend an ambient credential on the source host when CI is for another repository', async () => {
    await withRoot(manifest, (root) => {
      const repo = parseSourceRepository(FORGEJO_REPO.url);
      const foreignCi = { ...aliasCi, repository: 'other-owner/other-repo' };

      // A same-named repository on a foreign instance cannot redirect our
      // credential, and a foreign instance's credential is not offered to our
      // source host either: no ambient variable is even a candidate.
      expect(sourceReleaseTokenEnvNames(repo, root, foreignCi)).toEqual([]);
      expect(
        resolveSourceReleaseEndpoint(repo, root, foreignCi, { GITHUB_TOKEN: 'foreign-instance-token' }),
      ).toBeNull();
    });
  });

  it('falls back to the declared publisher credential in CI, against the host it was declared for', async () => {
    await withRoot(
      manifest,
      (root) => {
        const repo = parseSourceRepository(FORGEJO_REPO.url);

        // A runner whose forge issued no token still releases; the declared
        // credential belongs to the public host, so it goes there and never to
        // the runner's internal address.
        expect(resolveSourceReleaseEndpoint(repo, root, aliasCi, { DECLARED_PUBLISH_TOKEN: 'declared-token' })).toEqual(
          {
            repositoryApi: PUBLIC_REPOSITORY_API,
            envName: 'DECLARED_PUBLISH_TOKEN',
            token: 'declared-token',
          },
        );
        expect(sourceReleaseTokenEnvNames(repo, root, aliasCi)).toEqual([
          'FORGEJO_TOKEN',
          'GH_TOKEN',
          'GITHUB_TOKEN',
          'DECLARED_PUBLISH_TOKEN',
        ]);
      },
      SAME_FORGE_NPMRC,
    );
  });

  it('refuses an ambient credential from a different forge family', async () => {
    await withRoot(manifest, (root) => {
      const repo = parseSourceRepository(FORGEJO_REPO.url);
      const githubCi = { forgejo: false, apiBase: 'https://api.github.com', repository: 'fixture-owner/fixture-repo' };

      // GitHub Actions running against a Forgejo source: the repository name
      // matches a mirror, which is exactly how a GitHub credential would
      // otherwise be posted to a third-party forge.
      expect(resolveSourceReleaseEndpoint(repo, root, githubCi, { GITHUB_TOKEN: 'github-issued-token' })).toBeNull();
    });
  });

  it('offers no ambient credential outside CI, whatever the shell holds', async () => {
    await withRoot(manifest, (root) => {
      const repo = parseSourceRepository(FORGEJO_REPO.url);

      // Candidate names do not depend on which variables happen to be set:
      // outside CI no ambient variable is a candidate at all, so an operator
      // shell's GitHub PAT can never authenticate this forge.
      expect(sourceReleaseTokenEnvNames(repo, root, null)).toEqual([]);
      expect(
        resolveSourceReleaseEndpoint(repo, root, null, {
          GH_TOKEN: 'developer-github-pat',
          GITHUB_TOKEN: 'developer-github-pat',
        }),
      ).toBeNull();
    });
  });

  it('uses the declared publisher credential against the public host when it belongs to that forge', async () => {
    await withRoot(
      manifest,
      (root) => {
        const repo = parseSourceRepository(FORGEJO_REPO.url);

        expect(resolveSourceReleaseEndpoint(repo, root, null, { DECLARED_PUBLISH_TOKEN: 'declared-token' })).toEqual({
          repositoryApi: PUBLIC_REPOSITORY_API,
          envName: 'DECLARED_PUBLISH_TOKEN',
          token: 'declared-token',
        });
      },
      SAME_FORGE_NPMRC,
    );
  });

  it('refuses a declared publisher credential issued by a different host than the source forge', async () => {
    await withRoot(
      manifest,
      (root) => {
        const repo = parseSourceRepository(FORGEJO_REPO.url);

        expect(sourceReleaseTokenEnvNames(repo, root, null)).toEqual([]);
        expect(
          resolveSourceReleaseEndpoint(repo, root, null, { DECLARED_PUBLISH_TOKEN: 'other-forge-token' }),
        ).toBeNull();
      },
      OTHER_FORGE_NPMRC,
    );
  });

  it('has no endpoint for a GitHub source, which releases through the gh CLI', async () => {
    await withRoot({ ...manifest, repository: { type: 'git', url: 'github:owner/repo' } }, (root) => {
      const github = parseSourceRepository('github:owner/repo');

      expect(
        resolveSourceReleaseEndpoint(github, root, null, { GH_TOKEN: 'gh', DECLARED_PUBLISH_TOKEN: 'declared' }),
      ).toBeNull();
    });
  });
});

describe('source repository resolution', () => {
  it('resolves the repository URL from the root package manifest', async () => {
    await withRoot(
      { name: 'fixture-source', version: '1.0.0', repository: { type: 'git', url: 'github:owner/repo' } },
      (root) => {
        expect(resolveSourceRepository(root)).toEqual({ kind: 'github', owner: 'owner', repo: 'repo' });
      },
    );
  });

  it('throws when the root package has no repository URL', async () => {
    await withRoot({ name: 'fixture-source', version: '1.0.0' }, (root) => {
      expect(() => resolveSourceRepository(root)).toThrow('Source repository is missing');
    });
  });
});

async function withRoot(
  packageJson: Record<string, unknown>,
  fn: (root: string) => void,
  npmrc?: string,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-source-release-'));
  try {
    await writeFile(join(root, 'package.json'), `${JSON.stringify(packageJson)}\n`);
    if (npmrc) {
      await writeFile(join(root, '.npmrc'), npmrc);
    }
    fn(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}
