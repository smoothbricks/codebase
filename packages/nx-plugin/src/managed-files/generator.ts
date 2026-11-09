import { createHash } from 'node:crypto';
import { createProjectGraphAsync, getProjects, joinPathFragments, readJson, type Tree } from 'nx/src/devkit-exports.js';
import { renderTypecheckTestFiles, treeSourceListing } from '../typecheck-test-policy.js';
import type { PackageJson } from '../workspace-manifest.js';
import { deriveManagedFileContext, type WorkspaceProjects } from './context.js';
import { type ManagedFileContext, renderManagedFiles } from './files.js';
import { formatManagedContent } from './managed-format.js';
import { inspectManagedPaths } from './paths.js';
import { renderedOnChange } from './render-memo.js';
import { assertNoManagedConflicts, type FileResult, type ManagedPathInfo, stageManagedFiles } from './tree.js';

/**
 * All workspace content comes from Tree. The supplied graph is Nx's resolved
 * graph, not getProjects(Tree), which intentionally omits plugin-inferred targets.
 * The shared test-config renderer reads staged manifests too; no generation
 * installs dependencies or resolves secrets.
 */
export async function generateManagedFiles(
  tree: Tree,
  projects: WorkspaceProjects,
  pathInfo?: ReadonlyMap<string, ManagedPathInfo>,
): Promise<FileResult[]> {
  const manifest = tree.exists('package.json') ? readJson<PackageJson>(tree, 'package.json') : {};
  const packageRoots = new Set([...getProjects(tree).values()].map((project) => project.root));
  const packages = [...packageRoots]
    .filter((root) => root !== '.' && root !== '')
    .flatMap((root) => {
      const path = joinPathFragments(root, 'package.json');
      return tree.exists(path) ? [readJson<PackageJson>(tree, path)] : [];
    });
  return generateManagedFilesForContext(
    tree,
    deriveManagedFileContext(manifest, packages, projects, tree.exists('bun.lock'), tree.read('.npmrc', 'utf8') ?? ''),
    pathInfo,
  );
}

/**
 * Shared rendering boundary; stage only after every consumer formatter has succeeded. The render is a function of the
 * context, the workspace's source listing and what it reads through the Tree, so it runs again only when one of those
 * changed (`renderedOnChange`); staging always compares against the Tree as it is.
 */
export async function generateManagedFilesForContext(
  tree: Tree,
  context: ManagedFileContext,
  pathInfo?: ReadonlyMap<string, ManagedPathInfo>,
): Promise<FileResult[]> {
  const listing = treeSourceListing(tree);
  const key = createHash('sha256')
    .update(JSON.stringify(context))
    .update('\0')
    .update(listing.join('\0'))
    .digest('hex');
  const files = await renderedOnChange(tree, key, async (reading) => {
    const rendered = [...renderManagedFiles(context), ...renderTypecheckTestFiles(reading, listing)];
    for (const file of rendered) {
      if (file.content !== null) file.content = await formatManagedContent(reading.root, file.target, file.content);
    }
    return rendered;
  });
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
  assertNoManagedConflicts(await generateManagedFiles(tree, projects));
}
