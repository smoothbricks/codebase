import { createProjectGraphAsync, getProjects, joinPathFragments, readJson, type Tree } from 'nx/src/devkit-exports.js';
import type { PackageJson } from '../workspace-manifest.js';
import { deriveManagedFileContext, type WorkspaceProjects } from './context.js';
import { renderManagedFiles } from './files.js';
import { inspectManagedPaths } from './paths.js';
import { assertNoManagedConflicts, type FileResult, type ManagedPathInfo, stageManagedFiles } from './tree.js';

/**
 * All workspace content comes from Tree. The supplied graph is Nx's resolved
 * graph, not getProjects(Tree), which intentionally omits plugin-inferred targets.
 * This generator changes no graph inputs, installs nothing, and resolves no secrets.
 */
export function generateManagedFiles(
  tree: Tree,
  projects: WorkspaceProjects,
  pathInfo?: ReadonlyMap<string, ManagedPathInfo>,
): FileResult[] {
  const manifest = tree.exists('package.json') ? readJson<PackageJson>(tree, 'package.json') : {};
  const packageRoots = new Set([...getProjects(tree).values()].map((project) => project.root));
  const packages = [...packageRoots]
    .filter((root) => root !== '.' && root !== '')
    .flatMap((root) => {
      const path = joinPathFragments(root, 'package.json');
      return tree.exists(path) ? [readJson<PackageJson>(tree, path)] : [];
    });
  const files = renderManagedFiles(
    deriveManagedFileContext(manifest, packages, projects, tree.exists('bun.lock'), tree.read('.npmrc', 'utf8') ?? ''),
  );
  const paths =
    pathInfo ??
    inspectManagedPaths(
      tree.root,
      files.filter((file) => file.content !== null).map((file) => file.target),
    );
  return stageManagedFiles(tree, files, paths);
}

/** Native Nx generate/sync entry point. CLI calls the same transformation with its already-loaded graph. */
export default async function generator(tree: Tree): Promise<void> {
  const graph = await createProjectGraphAsync();
  const projects = Object.fromEntries(Object.entries(graph.nodes).map(([name, node]) => [name, node.data]));
  assertNoManagedConflicts(generateManagedFiles(tree, projects));
}
