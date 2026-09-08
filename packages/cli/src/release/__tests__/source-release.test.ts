import { describe, expect, it } from 'bun:test';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  forgejoApiUrl,
  forgejoAuthHeaders,
  forgejoReleaseLookupExists,
  parseSourceRepository,
  resolveSourceReleaseToken,
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
    expect(forgejoReleaseLookupExists(200, '{"id":1}', 'v1')).toBe(true);
    expect(forgejoReleaseLookupExists(404, '{"message":"not found"}', 'v1')).toBe(false);
  });

  it('throws with status and response details for unauthorized and unavailable responses', () => {
    expect(() => forgejoReleaseLookupExists(401, '{"message":"unauthorized"}', 'v1')).toThrow(
      'Unable to inspect source release v1 (HTTP 401)',
    );
    expect(() => forgejoReleaseLookupExists(401, '{"message":"unauthorized"}', 'v1')).toThrow('unauthorized');
    expect(() => forgejoReleaseLookupExists(503, 'service unavailable', 'v1')).toThrow(
      'Unable to inspect source release v1 (HTTP 503)',
    );
    expect(() => forgejoReleaseLookupExists(503, 'service unavailable', 'v1')).toThrow('service unavailable');
  });
});

/**
 * A source-release token is a forge credential. Forgejo Actions exports
 * GITHUB_TOKEN/GITHUB_SERVER_URL exactly as GitHub Actions does and developer
 * shells hold GitHub PATs, so "is this token even for this host" is the whole
 * question: an ambient token from another forge produces an unexplainable 401
 * and discloses the credential to a third party.
 */
describe('source release token resolution', () => {
  const FORGEJO_REPO = { type: 'git', url: 'https://forge.example.test/fixture-owner/fixture-repo.git' };
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

  it('uses GH_TOKEN, then GITHUB_TOKEN, then the declared publisher token env for a GitHub source', async () => {
    await withRoot({ ...manifest, repository: { type: 'git', url: 'github:owner/repo' } }, (root) => {
      const github = parseSourceRepository('github:owner/repo');
      const env = {
        GITHUB_SERVER_URL: 'https://github.com',
        GH_TOKEN: 'fixture-gh-token',
        GITHUB_TOKEN: 'fixture-github-token',
        DECLARED_PUBLISH_TOKEN: 'fixture-declared-token',
      };

      expect(resolveSourceReleaseToken(github, root, env)).toEqual({
        envName: 'GH_TOKEN',
        token: 'fixture-gh-token',
      });
      expect(resolveSourceReleaseToken(github, root, { ...env, GH_TOKEN: undefined })).toEqual({
        envName: 'GITHUB_TOKEN',
        token: 'fixture-github-token',
      });
      // The declared publisher variable belongs to the Forgejo registry host,
      // not to github.com, so it is not a candidate for a GitHub source.
      expect(resolveSourceReleaseToken(github, root, { DECLARED_PUBLISH_TOKEN: 'fixture-declared-token' })).toBeNull();
    });
  });

  it('accepts the ambient CI token when the ambient forge is the source forge', async () => {
    await withRoot(manifest, (root) => {
      const repo = parseSourceRepository(FORGEJO_REPO.url);

      expect(
        resolveSourceReleaseToken(repo, root, {
          GITHUB_SERVER_URL: 'https://forge.example.test',
          GITHUB_TOKEN: 'fixture-forgejo-actions-token',
        }),
      ).toEqual({ envName: 'GITHUB_TOKEN', token: 'fixture-forgejo-actions-token' });
    });
  });

  it('never sends another forge ambient token to the source forge', async () => {
    await withRoot(
      manifest,
      (root) => {
        const repo = parseSourceRepository(FORGEJO_REPO.url);

        // Running on GitHub Actions against a Forgejo source.
        expect(
          resolveSourceReleaseToken(repo, root, {
            GITHUB_SERVER_URL: 'https://github.com',
            GH_TOKEN: 'github-issued-token',
            GITHUB_TOKEN: 'github-issued-token',
          }),
        ).toBeNull();
        // A developer shell: GH_TOKEN there is a GitHub PAT, not a Forgejo token.
        expect(resolveSourceReleaseToken(repo, root, { GH_TOKEN: 'developer-github-pat' })).toBeNull();
        expect(sourceReleaseTokenEnvNames(repo, root, { GH_TOKEN: 'developer-github-pat' })).not.toContain('GH_TOKEN');
      },
      SAME_FORGE_NPMRC,
    );
  });

  it('uses the declared publisher token when it belongs to the source forge', async () => {
    await withRoot(
      manifest,
      (root) => {
        const repo = parseSourceRepository(FORGEJO_REPO.url);

        expect(resolveSourceReleaseToken(repo, root, { DECLARED_PUBLISH_TOKEN: 'fixture-declared-token' })).toEqual({
          envName: 'DECLARED_PUBLISH_TOKEN',
          token: 'fixture-declared-token',
        });
      },
      SAME_FORGE_NPMRC,
    );
  });

  it('refuses a declared publisher token issued by a different host than the source forge', async () => {
    await withRoot(
      manifest,
      (root) => {
        const repo = parseSourceRepository(FORGEJO_REPO.url);

        expect(sourceReleaseTokenEnvNames(repo, root, {})).toEqual([]);
        expect(resolveSourceReleaseToken(repo, root, { DECLARED_PUBLISH_TOKEN: 'other-forge-token' })).toBeNull();
      },
      OTHER_FORGE_NPMRC,
    );
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
