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

describe('source release token resolution', () => {
  it('uses GH_TOKEN, then GITHUB_TOKEN, then the declared publisher token env', async () => {
    await withRoot(
      {
        smoo: { privateNpm: { scope: '@fixture', publishTokenEnv: 'DECLARED_PUBLISH_TOKEN' } },
      },
      (root) => {
        expect(
          resolveSourceReleaseToken(root, {
            GH_TOKEN: 'fixture-gh-token',
            GITHUB_TOKEN: 'fixture-github-token',
            DECLARED_PUBLISH_TOKEN: 'fixture-declared-token',
          }),
        ).toBe('fixture-gh-token');
        expect(
          resolveSourceReleaseToken(root, {
            GITHUB_TOKEN: 'fixture-github-token',
            DECLARED_PUBLISH_TOKEN: 'fixture-declared-token',
          }),
        ).toBe('fixture-github-token');
        expect(
          resolveSourceReleaseToken(root, {
            DECLARED_PUBLISH_TOKEN: 'fixture-declared-token',
          }),
        ).toBe('fixture-declared-token');
        expect(resolveSourceReleaseToken(root, {})).toBeNull();
      },
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

async function withRoot(packageJson: Record<string, unknown>, fn: (root: string) => void): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-source-release-'));
  try {
    await writeFile(join(root, 'package.json'), `${JSON.stringify(packageJson)}\n`);
    fn(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}
