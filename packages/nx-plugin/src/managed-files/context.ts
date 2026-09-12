import { cargoCrossTestArchiveFile, cargoCrossTestTripleFromArchiveTarget } from '../cross-check-policy.js';
import { type DeploymentStage, PRODUCTION_PUSH_DEPLOY_TAG, stageDeploysProject } from '../deploy-policy.js';
import { MACOS_PLATFORM_TARGET_GLOBS, PLATFORM_TARGET_GLOBS } from '../platform-targets.js';
import { ciPushBranches, type NxProjectJson, type PackageJson, type PackageSmooGithub } from '../workspace-manifest.js';
import { isPublishablePackage, repositoryInfo } from '../workspace-package-policy.js';
import type { CiCrossTestArchive } from './ci-workflow.js';
import type { ManagedFileContext } from './files.js';
import { privateNpmWorkflowConfig } from './private-npm-policy.js';

export type WorkspaceProjects = Record<string, NxProjectJson>;
interface DeployTargetInfo {
  exists: boolean;
  provider?: 'cloudflare';
}

/** Pure derivation from the caller's resolved Nx graph and manifest contents. */
export function deriveManagedFileContext(
  manifest: PackageJson,
  packages: readonly PackageJson[],
  nxProjects: WorkspaceProjects,
  hasBunLock: boolean,
  npmrc: string,
): ManagedFileContext {
  const rootRepository = repositoryInfo(manifest);
  const github = manifest?.smoo?.github;
  const macosRunsOn = getMacosRunsOn(manifest);
  const sourceCheckouts = github?.sourceCheckouts;
  const cargoCredentials = github?.cargoCredentials;
  const stagingDeploy = deployTargetInfoFromProjects(nxProjects, 'staging');
  const productionDeploy = deployTargetInfoFromProjects(nxProjects, 'production');
  const targetNames = Object.values(nxProjects).flatMap((project) => Object.keys(project.targets ?? {}));
  const platformTargetGlobs = platformTargetGlobsForTest(targetNames);
  // The release workflow produces every platform family the graph carries
  // EXCEPT the ones the repository excludes. An excluded family renders no job
  // at all rather than a job whose failure blocks every release; the workflow
  // that does own it says so itself.
  const releasePlatformTargetGlobs = releasePlatformTargetGlobsFor(
    platformTargetGlobs,
    github?.releasePlatformFamiliesExcluded,
  );
  const privateNpm = privateNpmWorkflowConfig(manifest.smoo?.privateNpm, manifest, packages, npmrc);
  // Cache registry identity plus the lockfile, never token values: a scope
  // URL change in the committed .npmrc must invalidate the dependency cache,
  // and the cache key must stay free of secrets. Shell entry never requires
  // registry configuration; only actual registry operations resolve one.
  const nodeModulesCacheKey = hasBunLock
    ? `$${"{{ hashFiles('.npmrc', 'bun.lock', 'package.json', 'packages/*/package.json') }}"}`
    : `$${"{{ hashFiles('.npmrc', 'bun.lockb', 'package.json', 'packages/*/package.json') }}"}`;
  return {
    hasReleasePackages: packages.some(
      (pkg) =>
        !!pkg.name &&
        !!pkg.version &&
        isPublishablePackage({ private: pkg.private === true, tags: pkg.nx?.tags ?? [] }) &&
        rootRepository !== null &&
        repositoryInfo(pkg)?.url === rootRepository.url,
    ),
    hasStagingDeployTargets: stagingDeploy.exists,
    hasProductionDeployTargets: productionDeploy.exists,
    hasBrowserTestTargets: hasExactTargetForTest(targetNames, 'test-browser'),
    hasE2eDeploymentTargets: hasExactTargetForTest(targetNames, 'e2e-deployment'),
    hasProductionPushDeployTargets: anyProjectHasTagForTest(nxProjects, PRODUCTION_PUSH_DEPLOY_TAG),
    stagingDeployProvider: stagingDeploy.provider,
    productionDeployProvider: productionDeploy.provider,
    ciPushBranches: ciPushBranches(github),
    ciRunsOn: getCiRunsOn(github),
    macosRunsOn,
    platformProducer: github?.platformProducer,
    actionsProvider: github?.actionsProvider,
    ciEnvironments: github?.environments,
    ciDeploySecrets: github?.deploySecrets ?? {},
    ciE2eSecrets: github?.e2eSecrets ?? {},
    nodeModulesCacheKey,
    repoName: manifest.name ?? 'monorepo',
    platformTargetGlobs,
    releasePlatformTargetGlobs,
    sourceCheckouts,
    cargoCredentials,
    remoteCache: manifest?.smoo?.remoteCache,
    macosPlatformArchitectures: MACOS_PLATFORM_TARGET_GLOBS.some((glob) => releasePlatformTargetGlobs.includes(glob))
      ? macosPlatformArchitecturesForTest(targetNames)
      : [],
    privateNpm,
    crossTestArchives: crossTestArchivesForTest(nxProjects),
  };
}

export function hasExactTargetForTest(targetNames: Iterable<string>, target: string): boolean {
  return [...targetNames].includes(target);
}

/**
 * Test seam: the cross-built test archives this graph can produce, read back
 * out of the inferred `cargo-cross-test-archive-<triple>` targets.
 *
 * Derived, not configured. The triple is declared exactly once — in the cargo
 * workspace's own `[workspace.metadata.smoothbricks.test] cross-targets`, where
 * a Rust target triple belongs — and both halves of the workflow are rendered
 * from the targets that declaration produced. A second copy in `smoo.github`
 * could name a triple the graph cannot build, and CI would discover that only
 * on the runner.
 *
 * The path is the target's own output location, so the artifact the Linux job
 * uploads and the file the macOS job's runner opens are the same string
 * computed once.
 */
export function crossTestArchivesForTest(projects: WorkspaceProjects): CiCrossTestArchive[] {
  const archives: CiCrossTestArchive[] = [];
  for (const project of Object.values(projects)) {
    const projectRoot = project.root;
    for (const targetName of Object.keys(project.targets ?? {})) {
      const triple = cargoCrossTestTripleFromArchiveTarget(targetName);
      if (triple === null) {
        continue;
      }
      const file = cargoCrossTestArchiveFile(triple);
      archives.push({
        triple,
        path: projectRoot === undefined || projectRoot === '.' ? file : `${projectRoot}/${file}`,
      });
    }
  }
  return archives.sort((left, right) => left.triple.localeCompare(right.triple));
}

/**
 * The platform families the release workflow produces: the graph's families
 * minus the ones the repository excludes. Excluding a family the graph does not
 * have is not an error — a repository states what it does not release, and the
 * statement stays true when the target is added later.
 */
export function releasePlatformTargetGlobsFor(
  platformTargetGlobs: readonly string[],
  excluded: readonly string[] | undefined,
): string[] {
  if (!excluded || excluded.length === 0) return [...platformTargetGlobs];
  const drop = new Set(excluded);
  return platformTargetGlobs.filter((glob) => !drop.has(glob));
}

export function platformTargetGlobsForTest(targetNames: Iterable<string>): string[] {
  const names = [...targetNames];
  return PLATFORM_TARGET_GLOBS.filter((glob) => {
    const suffix = glob.startsWith('*') ? glob.slice(1) : glob;
    return names.some((name) => name.endsWith(suffix));
  });
}

/**
 * Test seam: the architectures the macOS platform job must fan out over,
 * derived from the target names themselves so a repository that ships one
 * architecture renders one matrix leg. Each leg owns one architecture's
 * targets, which is what lets them build on separate runners: cargo takes an
 * exclusive lock on a package's whole `target/` directory, so same-runner
 * platform builds serialize no matter how many workers Nx is given.
 */
export function macosPlatformArchitecturesForTest(targetNames: Iterable<string>): string[] {
  const families = MACOS_PLATFORM_TARGET_GLOBS.map((glob) => glob.slice(1));
  const architectures = new Set<string>();
  for (const name of targetNames) {
    for (const family of families) {
      if (!name.endsWith(family)) {
        continue;
      }
      const architecture = name.slice(0, -family.length).split('-').pop();
      if (architecture) {
        architectures.add(architecture);
      }
    }
  }
  return [...architectures].sort((left, right) => left.localeCompare(right));
}

/** Test seam: what the stage flow deploys, and with which provider, from graph nodes. */
export function deployTargetInfoFromProjects(projects: WorkspaceProjects, stage: DeploymentStage): DeployTargetInfo {
  let exists = false;
  let provider: DeployTargetInfo['provider'];
  for (const project of Object.values(projects)) {
    const info = deployTargetInfoFromProject(project, stage);
    if (!info.exists) {
      continue;
    }
    exists = true;
    provider ??= info.provider;
  }
  return { exists, provider };
}

/** Test seam: whether any project in the graph carries the Nx tag. */
export function anyProjectHasTagForTest(projects: WorkspaceProjects, tag: string): boolean {
  return Object.values(projects).some((project) => project.tags?.includes(tag));
}

/**
 * A generated deploy job exists for the projects CI would actually deploy, which
 * is `stageDeploysProject` and nothing else — the same rule `smoo github-ci
 * nx-deploy` selects by, so the job and its selection cannot disagree. Reading
 * the target's presence instead would render a deploy job, with cloud
 * credentials, into a repository whose only wrangler manifests document a
 * Durable Object binding for a published library's consumers.
 *
 * The command is read for the PROVIDER only: which cloud CLI runs says which
 * credentials the job needs, and says nothing about whether CI runs it.
 */
function deployTargetInfoFromProject(project: NxProjectJson, stage: DeploymentStage): DeployTargetInfo {
  const deploy = project.targets?.deploy;
  if (!deploy || !stageDeploysProject(project.tags, stage)) {
    return { exists: false };
  }
  const command = deploy.options?.command ?? deploy.command;
  return { exists: true, provider: deployProvider(typeof command === 'string' ? command : '') };
}

function deployProvider(command: string): DeployTargetInfo['provider'] {
  return command.includes('wrangler ') ? 'cloudflare' : undefined;
}

function getCiRunsOn(github: PackageSmooGithub | undefined): string | string[] {
  const configured = github?.runsOn;
  if (configured === undefined) {
    return 'ubuntu-latest';
  }
  if (typeof configured === 'string') {
    return configured.length > 0 ? configured : 'ubuntu-latest';
  }
  const labels = configured.filter((label) => label.length > 0);
  return labels.length > 0 ? labels : 'ubuntu-latest';
}

function getMacosRunsOn(packageJson: PackageJson | null | undefined): string | string[] {
  const configured = packageJson?.smoo?.github?.macosRunsOn;
  if (configured === undefined) {
    return 'macos-latest';
  }
  if (typeof configured === 'string') {
    return configured.length > 0 ? configured : 'macos-latest';
  }
  const labels = configured.filter((label) => label.length > 0);
  return labels.length > 0 ? labels : 'macos-latest';
}
