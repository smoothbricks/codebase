import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { managedAssetsRoot } from '../managed-assets.js';
import type {
  NonEmptyArray,
  PackageCargoCredentialsConfig,
  PackagePrivateNpmConfig,
  PackageRemoteCacheConfig,
  PackageSmooGithub,
  PackageSmooGithubEnvironments,
  PackageSourceCheckoutConfig,
} from '../workspace-manifest.js';
import { type CiCrossTestArchive, privateNpmReadTokenJobEnv, renderCiWorkflowYaml } from './ci-workflow.js';
import { renderRunsOnLine } from './github-runs-on.js';
import { renderPrPreviewCleanupWorkflowYaml } from './pr-preview-cleanup-workflow.js';
import { renderPublishWorkflowYaml } from './publish-workflow.js';
import type { ManagedFile } from './tree.js';

type ManagedKind = 'raw' | 'template' | 'generated';

interface ManagedFileDescriptor {
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

const managedFiles: ManagedFileDescriptor[] = [
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

/**
 * Whether a repository of this shape has the file at all. A repository that owns
 * no packages has nothing to publish, and one the stage flow does not deploy to
 * Cloudflare has no preview stage to clean up.
 */
function managedFileApplies(file: ManagedFileDescriptor, context: ManagedFileContext): boolean {
  if (file.releasePackagesOnly === true && !context.hasReleasePackages && !context.hasProductionDeployTargets) {
    return false;
  }
  return file.cloudflareDeployOnly !== true || context.stagingDeployProvider === 'cloudflare';
}

/** Test seam: the managed targets a repository of this shape gets, in file order. */
export function managedFileTargetsForContext(context: ManagedFileContext): string[] {
  return managedFiles.filter((file) => managedFileApplies(file, context)).map((file) => file.target);
}

/** Render all packaged templates before staging any workspace changes. */
export function renderManagedFiles(context: ManagedFileContext): ManagedFile[] {
  return managedFiles.map((file) => ({
    target: file.target,
    content: managedFileApplies(file, context) ? getManagedContent(file, context) : null,
    executable: file.executable,
  }));
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

function getManagedContent(file: ManagedFileDescriptor, context: ManagedFileContext): string {
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
  const sourcePath = join(managedAssetsRoot, file.kind === 'raw' ? 'raw' : 'templates', file.source);
  const content = readFileSync(sourcePath, 'utf8');
  if (file.kind === 'raw') {
    return content;
  }
  return renderTemplate(context, content);
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

function renderYamlFlowList(values: string[]): string {
  return JSON.stringify(values);
}
