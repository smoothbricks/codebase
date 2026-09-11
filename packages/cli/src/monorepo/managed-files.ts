import { appendFileSync, existsSync, readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  cargoCrossTestArchiveFile,
  cargoCrossTestTripleFromArchiveTarget,
} from '@smoothbricks/nx-plugin/cross-check-policy';
import { MACOS_PLATFORM_TARGET_GLOBS, PLATFORM_TARGET_GLOBS } from '@smoothbricks/nx-plugin/workspace-config-policy';
import { PRODUCTION_PUSH_DEPLOY_TAG, stageDeploysProject } from '../lib/deploy-tags.js';
import {
  ciPushBranches,
  type NonEmptyArray,
  type NxProjectJson,
  type PackageCargoCredentialsConfig,
  type PackageJson,
  type PackagePrivateNpmConfig,
  type PackageRemoteCacheConfig,
  type PackageSmooGithub,
  type PackageSmooGithubEnvironments,
  type PackageSourceCheckoutConfig,
  readValidatedPackageJson,
} from '../lib/json.js';
import { listReleasePackages, packageInfo } from '../lib/workspace.js';
import {
  loadNxProjects,
  type NxProjects,
  projectRootFromNxProjectJson,
  targetNamesFromNxProjectJson,
  targetNamesFromProjects,
} from '../nx/index.js';
import { resolvePrivateNpmWorkflowConfig } from '../release/private-npm.js';
import type { DeploymentStage } from '../wrangler/stage.js';
import { type CiCrossTestArchive, privateNpmReadTokenJobEnv, renderCiWorkflowYaml } from './ci-workflow.js';
import { renderRunsOnLine } from './github-runs-on.js';
import { syncManagedFiles } from './managed-fs.js';
import type { FileResult } from './managed-plan.js';
import { renderPrPreviewCleanupWorkflowYaml } from './pr-preview-cleanup-workflow.js';
import { renderPublishWorkflowYaml } from './publish-workflow.js';

export { INLINE_LOCAL_BEGIN, INLINE_LOCAL_END, LOCAL_SECTION_MARKER } from './managed-content.js';
export type { FileResult } from './managed-plan.js';

type ManagedKind = 'raw' | 'template' | 'generated';

interface ManagedFile {
  kind: ManagedKind;
  source: string;
  target: string;
  executable?: boolean;
  releasePackagesOnly?: boolean;
  cloudflareDeployOnly?: boolean;
}

export interface ManagedFileContext {
  hasReleasePackages: boolean;
  hasStagingDeployTargets: boolean;
  hasProductionDeployTargets: boolean;
  hasBrowserTestTargets: boolean;
  hasE2eDeploymentTargets: boolean;
  hasProductionPushDeployTargets: boolean;
  stagingDeployProvider?: 'cloudflare';
  productionDeployProvider?: 'cloudflare';
  ciPushBranches: NonEmptyArray<string>;
  ciRunsOn: string | string[];
  /** macOS platform job runs-on labels from the root smoo config; default macos-latest. */
  macosRunsOn: string | string[];
  platformProducer?: PackageSmooGithub['platformProducer'];
  actionsProvider?: PackageSmooGithub['actionsProvider'];
  ciEnvironments?: PackageSmooGithubEnvironments;
  ciDeploySecrets: Record<string, string>;
  ciE2eSecrets: Record<string, string>;
  nodeModulesCacheKey: string;
  repoName: string;
  platformTargetGlobs: string[];
  /** Platform families the release workflow produces; excluded families are absent. */
  releasePlatformTargetGlobs: string[];
  macosPlatformArchitectures: string[];
  /** Declared private-npm opt-in from the root smoo config; absent means fully public. */
  privateNpm?: PackagePrivateNpmConfig;
  /** Declared sibling source checkouts from the root smoo config; absent means none. */
  sourceCheckouts?: PackageSourceCheckoutConfig[];
  /** Declared Cargo private-dependency credentials from the root smoo config; absent means none. */
  cargoCredentials?: PackageCargoCredentialsConfig;
  /** Declared self-hosted Nx remote cache from the root smoo config; absent means local caching only. */
  remoteCache?: PackageRemoteCacheConfig;
  /**
   * Cross-built test archives the graph can produce, one per foreign target
   * triple the cargo workspace declares. Empty for a repository that declares
   * none, which renders a CI workflow with no cross steps and no macOS job.
   */
  crossTestArchives: CiCrossTestArchive[];
}

interface DeployTargetInfo {
  exists: boolean;
  provider?: 'cloudflare';
}

const managedFiles: ManagedFile[] = [
  {
    kind: 'raw',
    source: 'envrc',
    target: '.envrc',
  },
  {
    kind: 'raw',
    source: 'tooling/devenv',
    target: 'tooling/devenv',
    executable: true,
  },
  {
    kind: 'raw',
    source: 'tooling/direnv/repo-path',
    target: 'tooling/direnv/repo-path',
    executable: true,
  },
  {
    kind: 'raw',
    source: 'tooling/direnv/github-actions-bootstrap.sh',
    target: 'tooling/direnv/github-actions-bootstrap.sh',
    executable: true,
  },
  {
    kind: 'raw',
    source: 'tooling/direnv/setup-environment.ts',
    target: 'tooling/direnv/setup-environment.ts',
    executable: true,
  },
  {
    kind: 'raw',
    source: 'tooling/direnv/secret-references.ts',
    target: 'tooling/direnv/secret-references.ts',
  },
  {
    kind: 'raw',
    source: 'tooling/direnv/toolchain-stamp.ts',
    target: 'tooling/direnv/toolchain-stamp.ts',
    executable: true,
  },
  {
    kind: 'raw',
    source: 'tooling/direnv/devenv.smoo.nix',
    target: 'tooling/direnv/devenv.smoo.nix',
  },
  {
    kind: 'raw',
    source: 'tooling/git-hooks/pre-commit.sh',
    target: 'tooling/git-hooks/pre-commit.sh',
    executable: true,
  },
  {
    kind: 'raw',
    source: 'tooling/git-hooks/commit-msg.sh',
    target: 'tooling/git-hooks/commit-msg.sh',
    executable: true,
  },
  {
    kind: 'raw',
    source: 'tooling/git-hooks/pre-push.sh',
    target: 'tooling/git-hooks/pre-push.sh',
    executable: true,
  },
  {
    kind: 'raw',
    source: 'git-format-staged.yml',
    target: '.git-format-staged.yml',
  },
  {
    kind: 'raw',
    source: 'gitattributes',
    target: '.gitattributes',
  },
  {
    kind: 'raw',
    source: 'tooling/direnv/merge-newer-pins.sh',
    target: 'tooling/direnv/merge-newer-pins.sh',
    executable: true,
  },
  {
    kind: 'generated',
    source: 'ci-workflow',
    target: '.github/workflows/ci.yml',
  },
  {
    kind: 'generated',
    source: 'pr-preview-cleanup-workflow',
    target: '.github/workflows/pr-preview-cleanup.yml',
    cloudflareDeployOnly: true,
  },
  {
    kind: 'generated',
    source: 'publish-workflow',
    target: '.github/workflows/publish.yml',
    releasePackagesOnly: true,
  },
  {
    kind: 'template',
    source: 'github/workflows/managed-files.yml',
    target: '.github/workflows/managed-files.yml',
  },
  {
    kind: 'template',
    source: 'github/actions/cache-nix-devenv/action.yml',
    target: '.github/actions/cache-nix-devenv/action.yml',
  },
  {
    kind: 'template',
    source: 'github/actions/setup-devenv/action.yml',
    target: '.github/actions/setup-devenv/action.yml',
  },
  {
    kind: 'template',
    source: 'github/actions/save-nix-devenv/action.yml',
    target: '.github/actions/save-nix-devenv/action.yml',
  },
  {
    kind: 'template',
    source: 'github/actions/cache-node-modules/action.yml',
    target: '.github/actions/cache-node-modules/action.yml',
  },
  {
    kind: 'template',
    source: 'github/actions/cache-ttsc-plugins/action.yml',
    target: '.github/actions/cache-ttsc-plugins/action.yml',
  },
  {
    kind: 'template',
    source: 'github/actions/cache-nx/action.yml',
    target: '.github/actions/cache-nx/action.yml',
  },
];

export const managedFileTargetsForTest = managedFiles.map(({ target, executable }) => ({ target, executable }));

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');

/**
 * Whether a repository of this shape has the file at all. A repository that owns
 * no packages has nothing to publish, and one the stage flow does not deploy to
 * Cloudflare has no preview stage to clean up.
 */
function managedFileApplies(file: ManagedFile, context: ManagedFileContext): boolean {
  if (file.releasePackagesOnly === true && !context.hasReleasePackages && !context.hasProductionDeployTargets) {
    return false;
  }
  return file.cloudflareDeployOnly !== true || context.stagingDeployProvider === 'cloudflare';
}

/** Test seam: the managed targets a repository of this shape gets, in file order. */
export function managedFileTargetsForContext(context: ManagedFileContext): string[] {
  return managedFiles.filter((file) => managedFileApplies(file, context)).map((file) => file.target);
}

export async function applyManagedFiles(root: string, mode: 'update' | 'check' | 'diff'): Promise<FileResult[]> {
  const context = await getManagedFileContext(root);
  // Render every template before any write. A late renderer or ownership
  // conflict must not leave the checkout half-updated.
  return syncManagedFiles(
    root,
    managedFiles.map((file) => ({
      target: file.target,
      desired: managedFileApplies(file, context)
        ? { content: getManagedContent(file, context), executable: file.executable === true }
        : null,
    })),
    mode,
  );
}

/** Renders a generated workflow through its real managed-file descriptor, so the context wiring is covered. */
export function renderManagedWorkflowForTest(
  source: 'ci-workflow' | 'publish-workflow',
  context: ManagedFileContext,
): string {
  const file = managedFiles.find((candidate) => candidate.kind === 'generated' && candidate.source === source);
  if (!file) {
    throw new Error(`${source} is no longer a managed file`);
  }
  return getManagedContent(file, context);
}

function getManagedContent(file: ManagedFile, context: ManagedFileContext): string {
  if (file.kind === 'generated') {
    if (file.source === 'ci-workflow') {
      return renderCiWorkflowYaml({
        actionsProvider: context.actionsProvider,
        deploy: context.hasStagingDeployTargets,
        // The production job's Cloudflare env pair keys off this too: it can only select stage-derived projects,
        // whose provider is computed identically for staging and production.
        deployProvider: context.stagingDeployProvider,
        browserTests: context.hasBrowserTestTargets,
        e2eDeployment: context.hasStagingDeployTargets && context.hasE2eDeploymentTargets,
        pushBranches: context.ciPushBranches,
        runsOn: context.ciRunsOn,
        privateNpm: context.privateNpm,
        sourceCheckouts: context.sourceCheckouts,
        cargoCredentials: context.cargoCredentials,
        remoteCache: context.remoteCache,
        environments: context.ciEnvironments,
        deploySecrets: context.ciDeploySecrets,
        e2eSecrets: context.ciE2eSecrets,
        productionOnPush: context.hasProductionPushDeployTargets,
        crossTestArchives: context.crossTestArchives,
        macosRunsOn: context.macosRunsOn,
        platformProducer: context.platformProducer,
      });
    }
    if (file.source === 'publish-workflow') {
      return renderPublishWorkflowYaml({
        deploy: context.hasProductionDeployTargets,
        // A repo reaches this file on deploy targets alone. Without owned
        // packages every release step throws, so render the deploy-only shape.
        release: context.hasReleasePackages,
        deployProvider: context.productionDeployProvider,
        repoName: context.repoName,
        platformTargetGlobs: context.releasePlatformTargetGlobs,
        macosPlatformArchitectures: context.macosPlatformArchitectures,
        runsOn: context.ciRunsOn,
        macosRunsOn: context.macosRunsOn,
        platformProducer: context.platformProducer,
        actionsProvider: context.actionsProvider,
        privateNpm: context.privateNpm,
        sourceCheckouts: context.sourceCheckouts,
        cargoCredentials: context.cargoCredentials,
        remoteCache: context.remoteCache,
        deploySecrets: context.ciDeploySecrets,
      });
    }
    if (file.source === 'pr-preview-cleanup-workflow') {
      return renderPrPreviewCleanupWorkflowYaml({ runsOn: context.ciRunsOn });
    }
    throw new Error(`Unknown generated managed file source ${file.source}`);
  }
  const sourceRoot = file.kind === 'raw' ? 'managed/raw' : 'managed/templates';
  const sourcePath = join(packageRoot, sourceRoot, file.source);
  const content = readFileSync(sourcePath, 'utf8');
  if (file.kind === 'raw') {
    return content;
  }
  return renderTemplate(context, content);
}

async function getManagedFileContext(root: string): Promise<ManagedFileContext> {
  const manifestPath = join(root, 'package.json');
  const manifest = readValidatedPackageJson(manifestPath);
  const packageJson = manifest && packageInfo(manifestPath, manifest);
  const repoName = packageJson?.name ?? 'monorepo';
  const github = manifest?.smoo?.github;
  const macosRunsOn = getMacosRunsOn(manifest);
  const sourceCheckouts = github?.sourceCheckouts;
  const cargoCredentials = github?.cargoCredentials;
  // In-process Nx API → daemon socket (no second Node/`nx` CLI process).
  const nxProjects = await loadNxProjects(root);
  const stagingDeploy = deployTargetInfoFromProjects(nxProjects, 'staging');
  const productionDeploy = deployTargetInfoFromProjects(nxProjects, 'production');
  const targetNames = targetNamesFromProjects(nxProjects);
  const platformTargetGlobs = platformTargetGlobsForTest(targetNames);
  // The release workflow produces every platform family the graph carries
  // EXCEPT the ones the repository excludes. An excluded family renders no job
  // at all rather than a job whose failure blocks every release; the workflow
  // that does own it says so itself.
  const releasePlatformTargetGlobs = releasePlatformTargetGlobsFor(
    platformTargetGlobs,
    github?.releasePlatformFamiliesExcluded,
  );
  const privateNpm = resolvePrivateNpmWorkflowConfig(root);
  // Cache registry identity plus the lockfile, never token values: a scope
  // URL change in the committed .npmrc must invalidate the dependency cache,
  // and the cache key must stay free of secrets. Shell entry never requires
  // registry configuration; only actual registry operations resolve one.
  const nodeModulesCacheKey = existsSync(join(root, 'bun.lock'))
    ? `$${"{{ hashFiles('.npmrc', 'bun.lock', 'package.json', 'packages/*/package.json') }}"}`
    : `$${"{{ hashFiles('.npmrc', 'bun.lockb', 'package.json', 'packages/*/package.json') }}"}`;
  return {
    hasReleasePackages: listReleasePackages(root, packageJson).length > 0,
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
    repoName,
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
export function crossTestArchivesForTest(projects: NxProjects): CiCrossTestArchive[] {
  const archives: CiCrossTestArchive[] = [];
  for (const project of Object.values(projects)) {
    const projectRoot = projectRootFromNxProjectJson(project);
    for (const targetName of targetNamesFromNxProjectJson(project)) {
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
export function deployTargetInfoFromProjects(projects: NxProjects, stage: DeploymentStage): DeployTargetInfo {
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
export function anyProjectHasTagForTest(projects: NxProjects, tag: string): boolean {
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

function renderTemplate(context: ManagedFileContext, template: string): string {
  return template
    .replaceAll('{{REPO_NAME}}', context.repoName)
    .replaceAll('__SMOO_CI_PUSH_BRANCHES__', renderYamlFlowList(context.ciPushBranches))
    .replaceAll('__SMOO_CI_RUNS_ON__', renderRunsOnLine(context.ciRunsOn))
    .replaceAll(
      '__SMOO_PRIVATE_INSTALL_ENV__',
      context.privateNpm ? `    env:\n${privateNpmReadTokenJobEnv(context)}` : '',
    )
    .replaceAll('{{NODE_MODULES_CACHE_KEY}}', context.nodeModulesCacheKey);
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

function renderYamlFlowList(values: string[]): string {
  return JSON.stringify(values);
}

export function printResults(results: FileResult[]): void {
  for (const result of results) {
    console.log(`${result.action.padEnd(15)} ${result.target}${result.reason ? ` — ${result.reason}` : ''}`);
  }
}

export async function validateManagedFiles(root: string): Promise<number> {
  const results = await applyManagedFiles(root, 'check');
  printResults(results);
  const failures = results.filter((result) => result.action === 'drifted').length;
  if (failures > 0) {
    console.error('Managed monorepo files are out of date. Run: smoo monorepo update');
  }
  return failures;
}

/** The devenv module is inert until the repo-owned devenv.nix imports it. */
export const DEVENV_MODULE_IMPORT = './devenv.smoo.nix';

/**
 * Report, never rewrite: the import belongs to a repo-owned Nix file whose
 * shape this tool has no business editing. A missing import would otherwise be
 * silent — the managed module simply would not apply.
 */
export function validateDevenvModuleImport(root: string): number {
  const target = join(root, 'tooling/direnv/devenv.nix');
  if (!existsSync(target)) {
    return 0;
  }
  if (readFileSync(target, 'utf8').includes(DEVENV_MODULE_IMPORT)) {
    return 0;
  }
  console.error(
    `tooling/direnv/devenv.nix does not import ${DEVENV_MODULE_IMPORT}: add "imports = [${DEVENV_MODULE_IMPORT}];" so the managed shell contract applies`,
  );
  return 1;
}

/**
 * Non-blocking drift report: prints the per-file table and surfaces drift as
 * GitHub Actions warning annotations (plain stderr elsewhere) without failing
 * the run. Managed-file drift is derived state with its own remediation flow
 * (the persistent managed-files PR); only `monorepo check` without --warn and
 * PR-scoped gates treat it as an error.
 *
 * Under GitHub Actions the drifted-file count is published as the step output
 * `drifted`, so downstream steps gate declaratively instead of parsing logs.
 */
export async function warnOnManagedFileDrift(root: string): Promise<void> {
  const results = await applyManagedFiles(root, 'check');
  printResults(results);
  const drifted = results.filter((result) => result.action === 'drifted');
  if (process.env.GITHUB_OUTPUT) {
    appendFileSync(process.env.GITHUB_OUTPUT, `drifted=${drifted.length}\n`);
  }
  if (drifted.length === 0) {
    return;
  }
  if (process.env.GITHUB_ACTIONS === 'true') {
    for (const result of drifted) {
      console.log(
        `::warning title=Managed file drift::${result.target} drifted from the @smoothbricks/cli template; run 'smoo monorepo update'`,
      );
    }
  }
  console.error(`${drifted.length} managed monorepo file(s) drifted (non-blocking). Run: smoo monorepo update`);
}
