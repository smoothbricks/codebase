import { existsSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import type JsVersionActions from '@nx/js/src/release/version-actions';
import type { AfterAllProjectsVersioned } from 'nx/release';
import type { ProjectGraph, ProjectGraphDependency, ProjectGraphProjectNode } from 'nx/src/config/project-graph';
import type { Tree } from 'nx/src/generators/tree';

// The runtime `require` is what Nx's own loader does with this path, and the concrete
// `JsVersionActions` type is what makes subclassing it typecheck: `nx/release` only exports the
// abstract `VersionActions`, whose members this class does not reimplement.
type JsVersionActionsClass = typeof JsVersionActions & {
  afterAllProjectsVersioned: AfterAllProjectsVersioned;
};

/** The exported shape: the base class plus the statics this module adds. */
type SmoothbricksVersionActionsClass = JsVersionActionsClass & {
  publishedDependencies(
    dependencies: readonly ProjectGraphDependency[],
    manifest: Record<string, unknown>,
    packageNameOf: (projectName: string) => string | null,
  ): ProjectGraphDependency[];
};

const nxJsVersionActions = require('@nx/js/src/release/version-actions') as JsVersionActionsClass & {
  default?: JsVersionActionsClass;
};
const baseVersionActions: JsVersionActionsClass = nxJsVersionActions.default ?? nxJsVersionActions;

// The manifest sections that survive `npm publish` as a consumer's problem. A devDependency is
// build-time only: it never reaches an installer, so a new version of it changes nothing about
// the published artifact and is no reason to cut a release.
const PUBLISHED_DEPENDENCY_FIELDS = ['dependencies', 'peerDependencies', 'optionalDependencies'] as const;

const afterAllProjectsVersioned: AfterAllProjectsVersioned = async (cwd, options) => {
  const result = await nxJsVersionActions.afterAllProjectsVersioned(cwd, options);

  // Temporary Bun workaround. Delete this hook together with the CLI lockfile
  // sync/validation code once supported Bun versions fix all three issues:
  // - https://github.com/oven-sh/bun/issues/18906
  // - https://github.com/oven-sh/bun/issues/20477
  // - https://github.com/oven-sh/bun/issues/20829
  //
  // After ANY version bump (release or prepare-next), bun.lock workspace versions
  // must match package.json exactly — including unpublished -next. Day-to-day CI
  // and frozen installs require that invariant.
  //
  // Publish-time pack rewrites (-next → last stable tag) happen only in
  // `smoo release publish` around `bun pm pack`, never here. Doing publish-mode
  // rewrite on prepare-next is what left lock at 0.x.y while package.json said
  // 0.x.y-next.0 and blew up monorepo validate / packed-package-manifest.
  const updated = syncBunLockfileVersionsToPackageJson(cwd);
  if (updated === 0) {
    return result;
  }

  return {
    changedFiles: Array.from(new Set([...result.changedFiles, 'bun.lock'])),
    deletedFiles: result.deletedFiles,
  };
};

/**
 * Version only the dependents a release can actually reach.
 *
 * Nx expands a bump to every reverse edge of the project graph, and that graph cannot answer the
 * question that matters: `@nx/js` merges `dependencies`, `devDependencies`, `peerDependencies`,
 * and `optionalDependencies` into one edge set before the graph is built, so a package that only
 * uses another for its own tests is indistinguishable from one that ships it. Left alone, a fix
 * to a test-only library republishes every package that tests with it — and on macOS that means
 * rebuilding and re-testing native binaries nothing in the release actually changed.
 *
 * `readDependencies` is Nx's sanctioned override for exactly this: `ReleaseGraph`'s reverse-edge
 * map is built from its return value alone, so dropping the dev-only edges here removes those
 * packages from the release, from the platform builds, and from the version data the CLI reads
 * back. Specifier rewriting is unaffected: `updateProjectDependencies` reads the project graph
 * directly, so a package that is versioned on its own merit still gets accurate devDependency
 * pins.
 */
class SmoothbricksVersionActions extends baseVersionActions {
  // Nx reads this off the exported module, not off an instance (`loaded.afterAllProjectsVersioned`).
  static override afterAllProjectsVersioned: AfterAllProjectsVersioned = afterAllProjectsVersioned;

  /**
   * The policy itself: keep an edge only when the source manifest declares the target in a
   * section that survives publication.
   *
   * `packageNameOf` returns `null` for anything that is not a workspace project with a readable
   * name — an external `npm:` node, most commonly. Those are kept exactly as the project graph
   * reported them: they are not release projects, so they expand nothing, and keeping them makes
   * this override strictly subtractive on the set that matters.
   */
  static publishedDependencies(
    dependencies: readonly ProjectGraphDependency[],
    manifest: Record<string, unknown>,
    packageNameOf: (projectName: string) => string | null,
  ): ProjectGraphDependency[] {
    return dependencies.filter((dependency) => {
      const packageName = packageNameOf(dependency.target);
      if (packageName === null) {
        return true;
      }
      return PUBLISHED_DEPENDENCY_FIELDS.some((field) => {
        const section = manifest[field];
        return isRecord(section) && packageName in section;
      });
    });
  }

  override async readDependencies(tree: Tree, projectGraph: ProjectGraph): Promise<ProjectGraphDependency[]> {
    const dependencies = await super.readDependencies(tree, projectGraph);
    const manifest = readManifest(tree, this.projectGraphNode);
    if (!manifest) {
      return dependencies;
    }
    return SmoothbricksVersionActions.publishedDependencies(dependencies, manifest, (projectName) => {
      const node = projectGraph.nodes[projectName];
      const packageName = node ? readManifest(tree, node)?.name : undefined;
      return typeof packageName === 'string' ? packageName : null;
    });
  }
}

// Exported as a typed value, not as the class declaration. `export =` of a class publishes every
// static as an ESM named import TypeScript can see but Node's CJS lexer cannot, which crashes an
// `import { publishedDependencies }` at runtime; a const has no members to publish.
const versionActions: SmoothbricksVersionActionsClass = SmoothbricksVersionActions;

export = versionActions;

/**
 * Read a project's `package.json` from the version tree, which carries writes earlier projects in
 * this run have already staged.
 */
function readManifest(tree: Tree, node: ProjectGraphProjectNode): Record<string, unknown> | null {
  const contents = tree.read(join(node.data.root, 'package.json'), 'utf-8');
  if (contents === null) {
    return null;
  }
  const parsed: unknown = JSON.parse(contents);
  return isRecord(parsed) ? parsed : null;
}

function syncBunLockfileVersionsToPackageJson(root: string): number {
  const lockfilePath = join(root, 'bun.lock');
  if (!existsSync(lockfilePath)) {
    throw new Error('bun.lock not found');
  }
  const packages = workspacePackages(root);
  let lockfile = readFileSync(lockfilePath, 'utf8');
  let updated = 0;
  for (const pkg of packages) {
    // Install/CI invariant: lockfile version ≡ package.json version (including -next).
    const targetVersion = pkg.version;
    const relativePath = pkg.path.replaceAll('\\', '/');
    const escaped = escapeRegex(relativePath);
    const pattern = new RegExp(`("${escaped}":\\s*\\{[^}]*"version":\\s*")([^"]+)(")`);
    const match = lockfile.match(pattern);
    if (!match) {
      console.log(`skip: ${relativePath} (not found in lockfile)`);
      continue;
    }
    const lockVersion = match[2];
    if (lockVersion === targetVersion) {
      console.log(`ok:   ${relativePath} = ${targetVersion}`);
      continue;
    }
    lockfile = lockfile.replace(pattern, `$1${targetVersion}$3`);
    console.log(`fix:  ${relativePath}: ${lockVersion} -> ${targetVersion}`);
    updated++;
  }
  if (updated > 0) {
    writeFileSync(lockfilePath, lockfile);
  }
  console.log(
    updated > 0 ? `Updated ${updated} workspace version(s) in bun.lock` : 'All workspace versions already in sync.',
  );
  return updated;
}

interface WorkspacePackage {
  path: string;
  name: string;
  projectName: string;
  version: string;
}

function workspacePackages(root: string): WorkspacePackage[] {
  const packagesRoot = join(root, 'packages');
  return readdirSync(packagesRoot, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => {
      const path = `packages/${entry.name}`;
      const packageJsonPath = join(root, path, 'package.json');
      if (!existsSync(packageJsonPath)) {
        return null;
      }
      const parsed: unknown = JSON.parse(readFileSync(packageJsonPath, 'utf8'));
      if (!isRecord(parsed) || typeof parsed.name !== 'string' || typeof parsed.version !== 'string') {
        return null;
      }
      const json = parsed;
      const nx = isRecord(json.nx) ? json.nx : null;
      const projectName = (nx ? (typeof nx.name === 'string' ? nx.name : null) : null) ?? json.name;
      return { path, name: json.name, projectName, version: json.version };
    })
    .filter((pkg): pkg is WorkspacePackage => pkg !== null);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
