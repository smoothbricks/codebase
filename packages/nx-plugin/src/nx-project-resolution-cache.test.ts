import { expect, test } from 'bun:test';
import type { FileMap, ProjectGraphProjectNode } from 'nx/src/config/project-graph';
import type { ProjectConfiguration } from 'nx/src/config/workspace-json-project-json';
import { testOnlyTransferFileMap } from 'nx/src/native/index.js';
import { TargetProjectLocator } from 'nx/src/plugins/js/project-graph/build-dependencies/target-project-locator.js';
import { buildProjectGraphUsingProjectFileMap } from 'nx/src/project-graph/build-project-graph.js';
import {
  createProjectFileMapCache,
  type FileMapCache,
  shouldRecomputeWholeGraph,
} from 'nx/src/project-graph/nx-deps-cache.js';

// The installed Nx owns this cache. Unchanged importing bytes are not proof
// of unchanged resolution: another project's inferred package metadata can
// change the destination of their imports and manifest dependencies.
function projects(): { consumer: ProjectConfiguration; library: ProjectConfiguration } {
  return {
    consumer: { name: 'consumer', root: 'packages/consumer' },
    library: {
      name: 'library',
      root: 'packages/library',
      metadata: {
        js: {
          packageName: '@fixture/library',
          packageVersion: '1.0.0',
          packageExports: { '.': './src/index.ts' },
          packageMain: './src/index.ts',
          isInPackageManagerWorkspaces: true,
        },
      },
    },
  };
}
const files: FileMap = {
  nonProjectFiles: [],
  projectFileMap: {
    consumer: [{ file: 'packages/consumer/package.json', hash: 'unchanged-importing-manifest', deps: [] }],
    library: [{ file: 'packages/library/package.json', hash: 'unchanged-library-file' }],
  },
};
const tsConfig = { compilerOptions: { paths: {} } };
const external = 'unchanged-external-nodes';
const snapshot = (current: Record<string, ProjectConfiguration>, config = tsConfig) =>
  createProjectFileMapCache({}, {}, files, config, external, current);
const invalid = (cached: FileMapCache, current: Record<string, ProjectConfiguration>, config = tsConfig) =>
  shouldRecomputeWholeGraph(cached, {}, current, {}, config, external);

function resolves(current: Record<string, ProjectConfiguration>) {
  const nodes: Record<string, ProjectGraphProjectNode> = {};
  for (const [name, data] of Object.entries(current)) nodes[name] = { name, type: 'lib', data };
  return new TargetProjectLocator(nodes).findDependencyInWorkspaceProjects(
    'packages/consumer/package.json',
    '@fixture/library',
    'workspace:*',
  );
}

test('a renamed package reprocesses unchanged importing manifests instead of replaying their missing edge', () => {
  const before = projects();
  before.library.metadata = { js: { packageName: '@fixture/former-library' } };
  const cache = snapshot(before);
  const after = projects();
  expect(resolves(before)).toBeNull();
  expect(resolves(after)).toBe('library');
  expect(cache.fileMap.projectFileMap.consumer).toEqual(files.projectFileMap.consumer);
  expect(invalid(cache, after)).toBe(true);
  expect(invalid(snapshot(after), after)).toBe(false);
});

const metadataChanges: readonly [string, (project: ProjectConfiguration) => void][] = [
  [
    'root',
    (project) => {
      project.root = 'moved/library';
    },
  ],
  [
    'version',
    (project) => {
      project.metadata = { js: { ...project.metadata?.js, packageName: '@fixture/library', packageVersion: '2.0.0' } };
    },
  ],
  [
    'exports',
    (project) => {
      project.metadata = {
        js: {
          ...project.metadata?.js,
          packageName: '@fixture/library',
          packageExports: { './extra': './src/extra.ts' },
        },
      };
    },
  ],
  [
    'main',
    (project) => {
      project.metadata = {
        js: { ...project.metadata?.js, packageName: '@fixture/library', packageMain: './other.js' },
      };
    },
  ],
  [
    'workspace membership',
    (project) => {
      project.metadata = {
        js: { ...project.metadata?.js, packageName: '@fixture/library', isInPackageManagerWorkspaces: false },
      };
    },
  ],
  [
    'metadata removal',
    (project) => {
      delete project.metadata;
    },
  ],
];
for (const [name, mutate] of metadataChanges) {
  test(`changed package ${name} invalidates cached dependency resolution`, () => {
    const before = projects();
    const cache = snapshot(before);
    const after = projects();
    mutate(after.library);
    expect(invalid(cache, after)).toBe(true);
  });
}

test('added and removed projects can change resolution of unchanged files', () => {
  const before = projects();
  const added = { ...before, extra: { name: 'extra', root: 'packages/extra' } };
  expect(invalid(snapshot(before), added)).toBe(true);
  expect(invalid(snapshot(added), before)).toBe(true);
});

test('unrelated target/tag changes retain dependency cache entries', () => {
  const before = projects();
  const cache = snapshot(before);
  const after = projects();
  after.library.targets = { test: { executor: 'nx:noop', cache: true } };
  after.library.tags = ['changed-tag'];
  expect(invalid(cache, after)).toBe(false);
});

test('package-name collision order is a real resolution input', () => {
  const before = projects();
  before.consumer.metadata = before.library.metadata;
  const after = { library: before.library, consumer: before.consumer };
  expect(resolves(before)).toBe('library');
  expect(resolves(after)).toBe('consumer');
  expect(invalid(snapshot(before), after)).toBe(true);
});

test('a legacy cache without package-resolution metadata is recomputed, not deleted', () => {
  const current = projects();
  const { projectResolution, ...legacy } = snapshot(current);
  expect(projectResolution).toBeDefined();
  expect(invalid(legacy, current)).toBe(true);
  expect(legacy.fileMap).toBe(files);
});

test('normal graph construction records the resolution identity it will reuse', async () => {
  const current = projects();
  const roots = Object.fromEntries(Object.values(current).map((project) => [project.root, project]));
  const refs = testOnlyTransferFileMap(files.projectFileMap, files.nonProjectFiles);
  const { projectFileMapCache } = await buildProjectGraphUsingProjectFileMap(roots, {}, files, refs, null, [], {});
  expect(projectFileMapCache.projectResolution).toBe(snapshot(current).projectResolution);
});

test('added path aliases invalidate unchanged imports; map enumeration order does not', () => {
  const current = projects();
  const paths = { compilerOptions: { paths: { '@new/*': ['packages/library/*'] } } };
  expect(invalid(snapshot(current), current, paths)).toBe(true);
  const first = { compilerOptions: { paths: { a: ['a'], b: ['b'] } } };
  const reordered = { compilerOptions: { paths: { b: ['b'], a: ['a'] } } };
  expect(invalid(snapshot(current, first), current, reordered)).toBe(false);
});
