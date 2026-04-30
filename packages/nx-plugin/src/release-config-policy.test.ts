import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import { readJson, writeJson } from 'nx/src/devkit-exports.js';
import { createTreeWithEmptyWorkspace } from 'nx/src/devkit-testing-exports.js';

import {
  applyReleaseConfig,
  applyReleaseConfigPolicy,
  applyReleaseConfigTree,
  checkReleaseConfig,
  checkReleaseConfigPolicy,
  checkReleaseConfigTree,
  SMOO_NX_RELEASE_TAG_PATTERN,
  SMOO_NX_VERSION_ACTIONS,
} from './release-config-policy.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Layer 1: Pure core function tests
// ---------------------------------------------------------------------------

describe('pure core: checkReleaseConfig', () => {
  it('returns no issues for valid config', () => {
    expect(checkReleaseConfig(validReleaseNxJson())).toEqual([]);
  });

  it('uses nx.json as path in issues', () => {
    const issues = checkReleaseConfig({});
    for (const issue of issues) {
      expect(issue.path).toBe('nx.json');
    }
  });

  it('detects missing release config', () => {
    const issues = checkReleaseConfig({});
    expect(issues.some((i) => i.message.includes('release config is missing'))).toBe(true);
  });

  it('detects wrong release config values', () => {
    const issues = checkReleaseConfig({
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
  });
});

describe('pure core: applyReleaseConfig', () => {
  it('returns false for already-valid config', () => {
    const nxJson = validReleaseNxJson();
    expect(applyReleaseConfig(nxJson)).toBe(false);
  });

  it('applies full release config defaults', () => {
    const nxJson: Record<string, unknown> = {};
    expect(applyReleaseConfig(nxJson)).toBe(true);

    const release = nxJson.release as Record<string, unknown>;
    expect(release.projectsRelationship).toBe('independent');
    const version = release.version as Record<string, unknown>;
    expect(version.specifierSource).toBe('conventional-commits');
    expect(version.currentVersionResolver).toBe('git-tag');
    expect(version.fallbackCurrentVersionResolver).toBe('disk');
    expect(version.versionActions).toBe(SMOO_NX_VERSION_ACTIONS);
    expect(version.preVersionCommand).toBeUndefined();
    const releaseTag = release.releaseTag as Record<string, unknown>;
    expect(releaseTag.pattern).toBe(SMOO_NX_RELEASE_TAG_PATTERN);
    const changelog = release.changelog as Record<string, unknown>;
    expect(changelog.workspaceChangelog).toBe(false);
    const projectChangelogs = changelog.projectChangelogs as Record<string, unknown>;
    expect(projectChangelogs.createRelease).toBe(false);
    expect(projectChangelogs.file).toBe(false);
    const renderOptions = projectChangelogs.renderOptions as Record<string, unknown>;
    expect(renderOptions.authors).toBe(true);
    expect(renderOptions.applyUsernameToAuthors).toBe(true);
  });

  it('removes preVersionCommand', () => {
    const nxJson = validReleaseNxJson();
    const version = (nxJson.release as Record<string, unknown>).version as Record<string, unknown>;
    version.preVersionCommand = 'nx run-many -t build';
    expect(applyReleaseConfig(nxJson)).toBe(true);
    expect(version.preVersionCommand).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Layer 2: Tree-based function tests
// ---------------------------------------------------------------------------

describe('Tree: checkReleaseConfigTree', () => {
  it('returns issue when nx.json missing', () => {
    const tree = createTreeWithEmptyWorkspace();
    tree.delete('nx.json');
    const issues = checkReleaseConfigTree(tree);
    expect(issues).toHaveLength(1);
    expect(issues[0].message).toBe('nx.json not found');
  });

  it('detects missing release config', () => {
    const tree = createTreeWithEmptyWorkspace();
    writeJson(tree, 'nx.json', {});

    const issues = checkReleaseConfigTree(tree);
    expect(issues.some((i) => i.message.includes('release config is missing'))).toBe(true);
  });

  it('returns no issues for valid config', () => {
    const tree = createTreeWithEmptyWorkspace();
    writeJson(tree, 'nx.json', validReleaseNxJson());

    expect(checkReleaseConfigTree(tree)).toEqual([]);
  });
});

describe('Tree: applyReleaseConfigTree', () => {
  it('returns false when nx.json missing', () => {
    const tree = createTreeWithEmptyWorkspace();
    tree.delete('nx.json');
    expect(applyReleaseConfigTree(tree)).toBe(false);
  });

  it('applies release config defaults and writes back to tree', () => {
    const tree = createTreeWithEmptyWorkspace();
    writeJson(tree, 'nx.json', {});

    expect(applyReleaseConfigTree(tree)).toBe(true);

    const nxJson = readJson(tree, 'nx.json');
    const release = nxJson.release as Record<string, unknown>;
    expect(release.projectsRelationship).toBe('independent');
    const version = release.version as Record<string, unknown>;
    expect(version.specifierSource).toBe('conventional-commits');

    // Tree version now passes check
    expect(checkReleaseConfigTree(tree)).toEqual([]);
  });

  it('returns false when config already valid', () => {
    const tree = createTreeWithEmptyWorkspace();
    writeJson(tree, 'nx.json', validReleaseNxJson());

    expect(applyReleaseConfigTree(tree)).toBe(false);
  });

  it('removes preVersionCommand via tree', () => {
    const tree = createTreeWithEmptyWorkspace();
    const nxJson = validReleaseNxJson();
    const version = (nxJson.release as Record<string, unknown>).version as Record<string, unknown>;
    version.preVersionCommand = 'nx run-many -t build';
    writeJson(tree, 'nx.json', nxJson);

    expect(applyReleaseConfigTree(tree)).toBe(true);

    const fixed = readJson(tree, 'nx.json');
    const fixedVersion = (fixed.release as Record<string, unknown>).version as Record<string, unknown>;
    expect(fixedVersion.preVersionCommand).toBeUndefined();

    expect(checkReleaseConfigTree(tree)).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// Layer 3: Filesystem wrapper integration test
// ---------------------------------------------------------------------------

describe('filesystem: checkReleaseConfigPolicy / applyReleaseConfigPolicy', () => {
  it('round-trips check/apply on real temp directory', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoothbricks-release-policy-'));
    try {
      await writeJsonFile(join(root, 'nx.json'), {});

      const issues = checkReleaseConfigPolicy(root);
      expect(issues.length).toBeGreaterThan(0);
      // Filesystem wrapper uses absolute paths
      expect(issues[0].path).toBe(join(root, 'nx.json'));

      expect(applyReleaseConfigPolicy(root)).toBe(true);

      const nxJson = JSON.parse(await readFile(join(root, 'nx.json'), 'utf8'));
      const release = nxJson.release;
      expect(release.projectsRelationship).toBe('independent');
      expect(release.version.specifierSource).toBe('conventional-commits');
      expect(release.version.versionActions).toBe(SMOO_NX_VERSION_ACTIONS);

      // No issues after fix
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

// ---------------------------------------------------------------------------
// Test-local file helper (for filesystem integration tests only)
// ---------------------------------------------------------------------------

async function writeJsonFile(path: string, value: unknown): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}
