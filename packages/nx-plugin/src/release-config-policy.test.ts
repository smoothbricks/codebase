import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import {
  applyReleaseConfigPolicy,
  checkReleaseConfigPolicy,
  SMOO_NX_RELEASE_TAG_PATTERN,
  SMOO_NX_VERSION_ACTIONS,
} from './release-config-policy.js';

describe('release config policy', () => {
  it('detects missing/wrong release config and reports issues', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-release-policy-'));
    try {
      await writeJson(join(root, 'nx.json'), {});

      const issues = checkReleaseConfigPolicy(root);
      expect(issues.length).toBeGreaterThan(0);
      expect(issues.some((i) => i.message.includes('release config is missing'))).toBe(true);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('detects wrong release config values', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-release-policy-'));
    try {
      await writeJson(join(root, 'nx.json'), {
        release: {
          projectsRelationship: 'fixed',
          version: {
            specifierSource: 'prompt',
            currentVersionResolver: 'disk',
            fallbackCurrentVersionResolver: 'registry',
            versionActions: 'wrong/path',
            preVersionCommand: 'nx run-many -t build',
          },
          releaseTag: {
            pattern: 'v{version}',
          },
          changelog: {
            workspaceChangelog: true,
            projectChangelogs: {
              createRelease: 'github',
              file: '{projectRoot}/CHANGELOG.md',
              renderOptions: {},
            },
          },
        },
      });

      const issues = checkReleaseConfigPolicy(root);
      const messages = issues.map((i) => i.message);
      expect(messages).toContainEqual('release.projectsRelationship must be independent');
      expect(messages).toContainEqual('release.version.specifierSource must be conventional-commits');
      expect(messages).toContainEqual('release.version.currentVersionResolver must be git-tag');
      expect(messages).toContainEqual('release.version.fallbackCurrentVersionResolver must be disk');
      expect(messages).toContainEqual(`release.version.versionActions must be ${SMOO_NX_VERSION_ACTIONS}`);
      expect(messages).toContainEqual(
        'release.version.preVersionCommand must not be defined; smoo builds npm-missing packages before publish',
      );
      expect(messages).toContainEqual(`release.releaseTag.pattern must be ${SMOO_NX_RELEASE_TAG_PATTERN}`);
      expect(messages).toContainEqual('release.changelog.workspaceChangelog must be false');
      expect(messages).toContainEqual('release.changelog.projectChangelogs.createRelease must be false');
      expect(messages).toContainEqual('release.changelog.projectChangelogs.file must be false');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('applies full release config defaults', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-release-policy-'));
    try {
      await writeJson(join(root, 'nx.json'), {});

      expect(applyReleaseConfigPolicy(root)).toBe(true);

      const nxJson = JSON.parse(await readFile(join(root, 'nx.json'), 'utf8'));
      const release = nxJson.release;
      expect(release.projectsRelationship).toBe('independent');
      expect(release.version.specifierSource).toBe('conventional-commits');
      expect(release.version.currentVersionResolver).toBe('git-tag');
      expect(release.version.fallbackCurrentVersionResolver).toBe('disk');
      expect(release.version.versionActions).toBe(SMOO_NX_VERSION_ACTIONS);
      expect(release.version.preVersionCommand).toBeUndefined();
      expect(release.releaseTag.pattern).toBe(SMOO_NX_RELEASE_TAG_PATTERN);
      expect(release.changelog.workspaceChangelog).toBe(false);
      expect(release.changelog.projectChangelogs.createRelease).toBe(false);
      expect(release.changelog.projectChangelogs.file).toBe(false);
      expect(release.changelog.projectChangelogs.renderOptions.authors).toBe(true);
      expect(release.changelog.projectChangelogs.renderOptions.applyUsernameToAuthors).toBe(true);

      // No issues after fix
      expect(checkReleaseConfigPolicy(root)).toEqual([]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('removes preVersionCommand', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-release-policy-'));
    try {
      await writeJson(join(root, 'nx.json'), validReleaseNxJson());
      // Add a preVersionCommand to the valid config
      const nxJson = JSON.parse(await readFile(join(root, 'nx.json'), 'utf8'));
      nxJson.release.version.preVersionCommand = 'nx run-many -t build';
      await writeJson(join(root, 'nx.json'), nxJson);

      const issues = checkReleaseConfigPolicy(root);
      expect(issues.some((i) => i.message.includes('preVersionCommand'))).toBe(true);

      expect(applyReleaseConfigPolicy(root)).toBe(true);

      const fixed = JSON.parse(await readFile(join(root, 'nx.json'), 'utf8'));
      expect(fixed.release.version.preVersionCommand).toBeUndefined();

      expect(checkReleaseConfigPolicy(root)).toEqual([]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('accepts valid release config with no issues', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-release-policy-'));
    try {
      await writeJson(join(root, 'nx.json'), validReleaseNxJson());

      expect(checkReleaseConfigPolicy(root)).toEqual([]);
      expect(applyReleaseConfigPolicy(root)).toBe(false);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('reports issue when nx.json is missing', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-release-policy-'));
    try {
      const issues = checkReleaseConfigPolicy(root);
      expect(issues).toHaveLength(1);
      expect(issues[0].message).toBe('nx.json not found or invalid');

      expect(applyReleaseConfigPolicy(root)).toBe(false);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

function validReleaseNxJson(): Record<string, unknown> {
  return {
    release: {
      projectsRelationship: 'independent',
      version: {
        specifierSource: 'conventional-commits',
        currentVersionResolver: 'git-tag',
        fallbackCurrentVersionResolver: 'disk',
        versionActions: SMOO_NX_VERSION_ACTIONS,
      },
      releaseTag: {
        pattern: SMOO_NX_RELEASE_TAG_PATTERN,
      },
      changelog: {
        workspaceChangelog: false,
        projectChangelogs: {
          createRelease: false,
          file: false,
          renderOptions: {
            authors: true,
            applyUsernameToAuthors: true,
          },
        },
      },
    },
  };
}

async function writeJson(path: string, value: unknown): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}
