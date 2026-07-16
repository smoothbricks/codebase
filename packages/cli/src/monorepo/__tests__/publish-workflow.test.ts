/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: Assertions cover literal GitHub Actions expressions. */

import { describe, expect, it } from 'bun:test';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import {
  LINUX_PLATFORM_TARGET_GLOBS,
  MACOS_PLATFORM_TARGET_GLOBS,
  PLATFORM_TARGET_GLOBS,
} from '@smoothbricks/nx-plugin/workspace-config-policy';
import {
  definePublishWorkflow,
  type PublishWorkflowBump,
  type PublishWorkflowCallbacks,
  type PublishWorkflowNxTarget,
  type PublishWorkflowVersionOutputs,
  renderPublishWorkflowYaml,
  runPublishWorkflow,
} from '../publish-workflow.js';

describe('publish workflow definition', () => {
  it('renders the checked-in local publish workflow copy', async () => {
    const rendered = renderPublishWorkflowYaml({
      repoName: '@smoothbricks/codebase',
      platformTargetGlobs: PLATFORM_TARGET_GLOBS,
    });
    const packageRoot = join(import.meta.dir, '..', '..', '..');
    await expect(readFile(join(packageRoot, '..', '..', '.github/workflows/publish.yml'), 'utf8')).resolves.toBe(
      rendered,
    );
  });

  it('preserves the byte-equivalent single-job workflow when no platform targets exist', () => {
    const current = renderPublishWorkflowYaml({ repoName: '@smoothbricks/codebase' });
    const explicitlyEmpty = renderPublishWorkflowYaml({
      repoName: '@smoothbricks/codebase',
      platformTargetGlobs: [],
    });

    expect(explicitlyEmpty).toBe(current);
    expect(current).toContain('jobs:\n  publish:\n    runs-on: ubuntu-latest');
    expect(current).not.toContain('linux-release-candidate:');
    expect(current).not.toContain('macos-platform:');
    expect(current).not.toContain('publish-on-linux:');
  });

  it('renders a macOS release candidate and verified artifact transfer only when macOS targets exist', () => {
    const linuxOnly = renderPublishWorkflowYaml({
      repoName: '@smoothbricks/codebase',
      platformTargetGlobs: LINUX_PLATFORM_TARGET_GLOBS,
    });
    const native = renderPublishWorkflowYaml({
      repoName: '@smoothbricks/codebase',
      platformTargetGlobs: PLATFORM_TARGET_GLOBS,
    });

    expect(linuxOnly).not.toContain('macos-release-candidate:');
    expect(linuxOnly).toContain(
      `smoo github-ci nx-run-many --targets "${LINUX_PLATFORM_TARGET_GLOBS.join(',')}" --projects`,
    );
    expect(native).toContain('  macos-release-candidate:\n    runs-on: macos-latest');
    expect(native).toContain('  publish-on-linux:\n    needs: macos-release-candidate');
    expect(native).not.toContain('linux-release-candidate:');
    expect(native).not.toContain('macos-platform:');
    expect(native).toContain('uses: ./.github/actions/setup-devenv');
    expect(native).toContain(
      `smoo github-ci nx-run-many --targets "${MACOS_PLATFORM_TARGET_GLOBS.join(',')}" --projects`,
    );
    expect(native).toContain(
      `smoo github-ci nx-run-many --targets "${LINUX_PLATFORM_TARGET_GLOBS.join(',')}" --projects`,
    );
    expect(native).toContain('uses: actions/upload-artifact@v7.0.1');
    expect(native).toContain('uses: actions/download-artifact@v8.0.1');
    expect(native).toContain('name: publish-release-state-${{ github.run_id }}');
    expect(native).toContain('name: publish-macos-outputs-${{ github.run_id }}');
    expect(native).not.toContain('name: publish-release-outputs-${{ github.run_id }}');
    expect(native).not.toContain('name: publish-linux-outputs-${{ github.run_id }}');
    expect(native).toContain('git bundle create');
    expect(native).toContain('git fetch "${{ runner.temp }}/publish-artifacts/publish-release-state-');
    expect(native).toContain(
      'smoo github-ci apply-outputs --source-sha "${{ needs.macos-release-candidate.outputs.release-sha }}"',
    );
  });

  it('versions on macOS, restores before Linux setup, and preserves mode, deploy, and dry-run gates', () => {
    const rendered = renderPublishWorkflowYaml({
      deploy: true,
      deployProvider: 'cloudflare',
      repoName: '@smoothbricks/codebase',
      platformTargetGlobs: PLATFORM_TARGET_GLOBS,
    });
    const candidate = rendered.slice(
      rendered.indexOf('  macos-release-candidate:'),
      rendered.indexOf('  publish-on-linux:'),
    );
    const finalJob = rendered.slice(rendered.indexOf('  publish-on-linux:'));

    expect(candidate.indexOf('smoo release version')).toBeLessThan(candidate.indexOf('🍎 Build macOS and iOS targets'));
    expect(finalJob.indexOf('♻️ Restore validated release state')).toBeLessThan(finalJob.indexOf('🧱 Setup Nix/devenv'));
    expect(rendered).toContain('GITHUB_SHA: ${{ steps.release-state.outputs.sha }}');
    expect(finalJob).not.toContain('smoo release version');
    expect(finalJob).toContain("needs.macos-release-candidate.outputs.mode != 'none'");
    expect(finalJob).toContain('smoo github-ci nx-run-many --targets build --projects');
    expect(finalJob).toContain('smoo github-ci nx-run-many --targets lint --projects');
    expect(finalJob).toContain('smoo github-ci nx-run-many --targets test --projects');
    expect(finalJob).toContain("inputs.deploy_environment == 'production'");
    expect(finalJob).toContain("inputs.dry_run != 'true'");
    expect(finalJob).toContain('smoo release publish --bump "${{ inputs.bump }}" --dry-run "${{ inputs.dry_run }}"');
    expect(finalJob).toContain('CLOUDFLARE_API_TOKEN: ${{ secrets.CLOUDFLARE_API_TOKEN }}');
    expect(finalJob).toContain('CLOUDFLARE_ACCOUNT_ID: ${{ secrets.CLOUDFLARE_ACCOUNT_ID }}');
  });

  it('does not wire npm token secrets into repair or publish steps', () => {
    const rendered = renderPublishWorkflowYaml({ repoName: '@smoothbricks/codebase' });

    expect(rendered).not.toContain('NODE_AUTH_TOKEN');
    expect(rendered).not.toContain('secrets.NPM_TOKEN');
    expect(rendered).toContain('packages must already exist on npm and use trusted publishing/OIDC');
  });

  it('omits production deploy controls when no production deploy target exists', () => {
    const rendered = renderPublishWorkflowYaml({ repoName: '@smoothbricks/codebase' });

    expect(rendered).not.toContain('deploy_environment');
    expect(rendered).not.toContain('Deploy production');
    expect(rendered).not.toContain('nx-deploy');
  });

  it('renders production deploy controls for repos with production deploy targets', () => {
    const rendered = renderPublishWorkflowYaml({ deploy: true, repoName: '@smoothbricks/codebase' });

    expect(rendered).toContain('deploy_environment:');
    expect(rendered).toContain('- name: 🚀 Deploy production');
    expect(rendered).not.toContain('CLOUDFLARE_API_TOKEN');
    expect(rendered).not.toContain('CLOUDFLARE_ACCOUNT_ID');
    expect(rendered).toContain(
      'smoo github-ci nx-deploy --configuration production --mode run-many --verify --name "Deploy Production"',
    );
  });

  it('adds Cloudflare credentials for Wrangler-backed production deploys', () => {
    const rendered = renderPublishWorkflowYaml({
      deploy: true,
      deployProvider: 'cloudflare',
      repoName: '@smoothbricks/codebase',
    });

    expect(rendered).toContain('CLOUDFLARE_API_TOKEN: ${{ secrets.CLOUDFLARE_API_TOKEN }}');
    expect(rendered).toContain('CLOUDFLARE_ACCOUNT_ID: ${{ secrets.CLOUDFLARE_ACCOUNT_ID }}');
  });

  it('deploys production only after a real publish when requested', async () => {
    const deployed = await publishWorkflowScenario({
      deploy: true,
      repairs: [],
      current: [],
      bump: 'auto',
      deployEnvironment: 'production',
      dryRun: false,
      version: { mode: 'new', projects: ['app'] },
    }).run();
    const dryRun = await publishWorkflowScenario({
      deploy: true,
      repairs: [],
      current: [],
      bump: 'auto',
      deployEnvironment: 'production',
      dryRun: true,
      version: { mode: 'new', projects: ['app'] },
    }).run();

    expect(deployed.productionDeployed).toBe(true);
    expect(dryRun.productionDeployed).toBe(false);
  });

  it('bootstraps the self-hosted CLI before release commands only for the SmoothBricks repo', async () => {
    const smoothbricks = await publishWorkflowScenario({
      repoName: '@smoothbricks/codebase',
      repairs: [],
      current: [],
      bump: 'patch',
      dryRun: false,
      version: { mode: 'none', projects: [] },
    }).run();
    const downstream = await publishWorkflowScenario({
      repoName: '@other/repo',
      repairs: [],
      current: [],
      bump: 'patch',
      dryRun: false,
      version: { mode: 'none', projects: [] },
    }).run();

    expect(smoothbricks.selfHostedCliBuilt).toBe(true);
    expect(smoothbricks.repairSawSelfHostedCli).toBe(true);
    expect(smoothbricks.versionSawSelfHostedCli).toBe(true);
    expect(downstream.selfHostedCliBuilt).toBe(false);
    expect(downstream.repairSawSelfHostedCli).toBe(false);
    expect(downstream.versionSawSelfHostedCli).toBe(false);
  });

  it('repairs older gaps, skips validation for mode none, and still completes current HEAD publish', async () => {
    const scenario = publishWorkflowScenario({
      repairs: [
        { tag: '@scope/a@1.1.0', npmMissing: false, githubMissing: true },
        { tag: '@scope/b@2.0.0-beta.1', npmMissing: true, githubMissing: true },
      ],
      current: [{ tag: '@scope/a@1.2.0', npmMissing: true, githubMissing: false }],
      bump: 'auto',
      dryRun: false,
      version: { mode: 'none', projects: [] },
    });

    const outcome = await scenario.run();

    expect(outcome.fixtureRepoSetup).toBe(true);
    expect(outcome.releaseAuthorConfigured).toBe(true);
    expect(outcome.repairedTags).toEqual(['@scope/a@1.1.0', '@scope/b@2.0.0-beta.1']);
    expect(outcome.repairBuildArtifacts).toEqual(['@scope/b']);
    expect(outcome.validation).toEqual({ checks: 0, builds: [], lints: [], tests: [], validates: 0 });
    expect(outcome.publishRan).toBe(true);
    expect(outcome.publishCompletedTags).toEqual(['@scope/a@1.2.0']);
    expect(outcome.remainingDurableGaps).toEqual([]);
  });

  it('validates a new release before publishing current HEAD gaps', async () => {
    const scenario = publishWorkflowScenario({
      repairs: [],
      current: [
        { tag: '@scope/a@1.2.0', npmMissing: true, githubMissing: true },
        { tag: '@scope/b@2.0.0', npmMissing: false, githubMissing: true },
      ],
      bump: 'patch',
      dryRun: false,
      version: { mode: 'new', projects: ['a', 'b'] },
    });

    const outcome = await scenario.run();

    expect(outcome.validation).toEqual({
      checks: 1,
      builds: ['a', 'b'],
      lints: ['a', 'b'],
      tests: ['a', 'b'],
      validates: 1,
    });
    expect(outcome.publishSawValidatedRelease).toBe(true);
    expect(outcome.publishCompletedTags).toEqual(['@scope/a@1.2.0', '@scope/b@2.0.0']);
    expect(outcome.remainingDurableGaps).toEqual([]);
  });

  it('dry-run walks the workflow outcomes without mutating durable release state', async () => {
    const scenario = publishWorkflowScenario({
      repairs: [{ tag: '@scope/a@1.1.0', npmMissing: true, githubMissing: true }],
      current: [{ tag: '@scope/a@1.2.0', npmMissing: true, githubMissing: true }],
      bump: 'prerelease',
      dryRun: true,
      version: { mode: 'new', projects: ['a'] },
    });

    const outcome = await scenario.run();

    expect(outcome.fixtureRepoSetup).toBe(true);
    expect(outcome.repairedTags).toEqual([]);
    expect(outcome.repairBuildArtifacts).toEqual([]);
    expect(outcome.validation).toEqual({
      checks: 1,
      builds: ['a'],
      lints: ['a'],
      tests: ['a'],
      validates: 1,
    });
    expect(outcome.publishRan).toBe(true);
    expect(outcome.publishCompletedTags).toEqual([]);
    expect(outcome.remainingDurableGaps).toEqual([
      '@scope/a@1.1.0:github',
      '@scope/a@1.1.0:npm',
      '@scope/a@1.2.0:github',
      '@scope/a@1.2.0:npm',
    ]);
  });
});

interface ReleaseGap {
  tag: string;
  npmMissing: boolean;
  githubMissing: boolean;
}

interface WorkflowScenarioConfig {
  deploy?: boolean;
  repoName?: string;
  repairs: ReleaseGap[];
  current: ReleaseGap[];
  bump: PublishWorkflowBump;
  deployEnvironment?: 'none' | 'production';
  dryRun: boolean;
  version: PublishWorkflowVersionOutputs;
}

interface WorkflowOutcome {
  fixtureRepoSetup: boolean;
  releaseAuthorConfigured: boolean;
  selfHostedCliBuilt: boolean;
  repairSawSelfHostedCli: boolean;
  versionSawSelfHostedCli: boolean;
  repairedTags: string[];
  repairBuildArtifacts: string[];
  validation: { checks: number; builds: string[]; lints: string[]; tests: string[]; validates: number };
  publishRan: boolean;
  productionDeployed: boolean;
  publishSawValidatedRelease: boolean;
  publishCompletedTags: string[];
  remainingDurableGaps: string[];
}

function publishWorkflowScenario(config: WorkflowScenarioConfig): { run(): Promise<WorkflowOutcome> } {
  return {
    async run() {
      const state = new WorkflowScenarioState(config);
      await runPublishWorkflow(definePublishWorkflow({ deploy: config.deploy, repoName: config.repoName }), {
        inputs: { bump: config.bump, deployEnvironment: config.deployEnvironment ?? 'none', dryRun: config.dryRun },
        callbacks: state.callbacks(),
      });
      return state.outcome();
    },
  };
}

class WorkflowScenarioState {
  private fixtureSetup = false;
  private authorConfigured = false;
  private selfHostedCli = false;
  private repairObservedSelfHostedCli = false;
  private versionObservedSelfHostedCli = false;
  private publishReleaseRan = false;
  private productionDeployRan = false;
  private publishSawValidation = false;
  private readonly repaired = new Set<string>();
  private readonly repairBuilds = new Set<string>();
  private readonly publishedCurrent = new Set<string>();
  private readonly durableGaps = new Set<string>();
  private readonly validationState: {
    checks: number;
    builds: string[];
    lints: string[];
    tests: string[];
    validates: number;
  } = {
    checks: 0,
    builds: [],
    lints: [],
    tests: [],
    validates: 0,
  };

  constructor(private readonly config: WorkflowScenarioConfig) {
    for (const gap of [...config.repairs, ...config.current]) {
      if (gap.githubMissing) {
        this.durableGaps.add(`${gap.tag}:github`);
      }
      if (gap.npmMissing) {
        this.durableGaps.add(`${gap.tag}:npm`);
      }
    }
  }

  callbacks(): PublishWorkflowCallbacks {
    return {
      checkout: async () => {},
      setupDevenv: async () => {
        this.fixtureSetup = true;
        return { nixCacheHit: 'false', devenvCacheHit: 'false' };
      },
      configureReleaseAuthor: async () => {
        this.authorConfigured = true;
      },
      buildSelfHostedCli: async () => {
        this.selfHostedCli = true;
      },
      repairPendingReleases: async ({ dryRun }) => {
        this.repairObservedSelfHostedCli = this.selfHostedCli;
        if (dryRun) {
          return;
        }
        for (const gap of this.config.repairs) {
          if (gap.npmMissing) {
            this.repairBuilds.add(packageNameFromTag(gap.tag));
            this.durableGaps.delete(`${gap.tag}:npm`);
          }
          if (gap.githubMissing) {
            this.durableGaps.delete(`${gap.tag}:github`);
          }
          this.repaired.add(gap.tag);
        }
      },
      versionRelease: async ({ bump, dryRun }) => {
        this.versionObservedSelfHostedCli = this.selfHostedCli;
        expect(bump).toBe(this.config.bump);
        expect(dryRun).toBe(this.config.dryRun);
        return this.config.version;
      },
      checkManagedMonorepoFiles: async () => {
        this.validationState.checks += 1;
      },
      nxRunMany: async ({ target, projects }) => {
        this.validationProjects(target).push(...projects);
      },
      uploadTraceDbs: async () => {},
      validateMonorepoConfig: async () => {
        this.validationState.validates += 1;
      },
      publishRelease: async ({ bump, dryRun }) => {
        expect(bump).toBe(this.config.bump);
        expect(dryRun).toBe(this.config.dryRun);
        this.publishReleaseRan = true;
        this.publishSawValidation = this.config.version.mode === 'none' || this.validationState.validates > 0;
        if (dryRun) {
          return;
        }
        for (const gap of this.config.current) {
          if (gap.npmMissing) {
            this.durableGaps.delete(`${gap.tag}:npm`);
          }
          if (gap.githubMissing) {
            this.durableGaps.delete(`${gap.tag}:github`);
          }
          if (gap.npmMissing || gap.githubMissing) {
            this.publishedCurrent.add(gap.tag);
          }
        }
      },
      deployProduction: async () => {
        this.productionDeployRan = true;
      },
      saveNixDevenv: async () => {},
    };
  }

  outcome(): WorkflowOutcome {
    return {
      fixtureRepoSetup: this.fixtureSetup,
      releaseAuthorConfigured: this.authorConfigured,
      selfHostedCliBuilt: this.selfHostedCli,
      repairSawSelfHostedCli: this.repairObservedSelfHostedCli,
      versionSawSelfHostedCli: this.versionObservedSelfHostedCli,
      repairedTags: [...this.repaired].sort(),
      repairBuildArtifacts: [...this.repairBuilds].sort(),
      validation: {
        checks: this.validationState.checks,
        builds: [...this.validationState.builds],
        lints: [...this.validationState.lints],
        tests: [...this.validationState.tests],
        validates: this.validationState.validates,
      },
      publishRan: this.publishReleaseRan,
      productionDeployed: this.productionDeployRan,
      publishSawValidatedRelease: this.publishSawValidation,
      publishCompletedTags: [...this.publishedCurrent].sort(),
      remainingDurableGaps: [...this.durableGaps].sort(),
    };
  }

  private validationProjects(target: PublishWorkflowNxTarget): string[] {
    if (target === 'build') {
      return this.validationState.builds;
    }
    if (target === 'lint') {
      return this.validationState.lints;
    }
    return this.validationState.tests;
  }
}

function packageNameFromTag(tag: string): string {
  const versionSeparator = tag.lastIndexOf('@');
  if (versionSeparator <= 0) {
    throw new Error(`Invalid release tag fixture ${tag}.`);
  }
  return tag.slice(0, versionSeparator);
}
