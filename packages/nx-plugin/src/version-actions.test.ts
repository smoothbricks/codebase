import { describe, expect, it } from 'bun:test';
// The published entry point is what `nx.json` names in `release.version.versionActions`, so it is
// the artifact whose behaviour the release depends on.
import SmoothbricksVersionActions from '@smoothbricks/nx-plugin/version-actions';
import type { ProjectGraphDependency } from 'nx/src/config/project-graph';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function edge(target: string): ProjectGraphDependency {
  return { source: 'consumer', target, type: 'static' };
}

/** Every fixture project's Nx name is its unscoped package name. */
function packageNameOf(projectName: string): string | null {
  return projectName.startsWith('npm:') ? null : `@smoothbricks/${projectName}`;
}

// ---------------------------------------------------------------------------
// publishedDependencies
// ---------------------------------------------------------------------------

describe('publishedDependencies', () => {
  it('keeps every section an installer resolves', () => {
    const manifest = {
      dependencies: { '@smoothbricks/runtime': 'workspace:*' },
      peerDependencies: { '@smoothbricks/host': '^1.0.0' },
      optionalDependencies: { '@smoothbricks/native': 'workspace:*' },
    };

    const kept = SmoothbricksVersionActions.publishedDependencies(
      [edge('runtime'), edge('host'), edge('native')],
      manifest,
      packageNameOf,
    );

    expect(kept.map((dependency) => dependency.target)).toEqual(['runtime', 'host', 'native']);
  });

  it('drops a dev-only edge, because a devDependency bump changes nothing an installer sees', () => {
    const manifest = {
      dependencies: { typia: '13.2.0' },
      devDependencies: { '@smoothbricks/lmao': 'workspace:*' },
    };

    expect(SmoothbricksVersionActions.publishedDependencies([edge('lmao')], manifest, packageNameOf)).toEqual([]);
  });

  it('keeps a package declared in both, because the published section is what ships', () => {
    const manifest = {
      dependencies: { '@smoothbricks/lmao': 'workspace:*' },
      devDependencies: { '@smoothbricks/lmao': 'workspace:*' },
    };

    expect(
      SmoothbricksVersionActions.publishedDependencies([edge('lmao')], manifest, packageNameOf).map(
        (dependency) => dependency.target,
      ),
    ).toEqual(['lmao']);
  });

  it('keeps an edge whose target has no resolvable package name', () => {
    expect(
      SmoothbricksVersionActions.publishedDependencies([edge('npm:typia')], {}, packageNameOf).map(
        (dependency) => dependency.target,
      ),
    ).toEqual(['npm:typia']);
  });

  it('drops every workspace edge of a manifest with no published sections', () => {
    expect(
      SmoothbricksVersionActions.publishedDependencies(
        [edge('lmao'), edge('validation')],
        { devDependencies: { '@smoothbricks/lmao': 'workspace:*', '@smoothbricks/validation': 'workspace:*' } },
        packageNameOf,
      ),
    ).toEqual([]);
  });
});
