import { describe, expect, it } from 'bun:test';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
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
    const rendered = renderPublishWorkflowYaml({ repoName: '@smoothbricks/codebase' });
    const packageRoot = join(import.meta.dir, '..', '..', '..');
    await expect(readFile(join(packageRoot, '..', '..', '.github/workflows/publish.yml'), 'utf8')).resolves.toBe(
      rendered,
    );
  });

  it('does not wire npm token secrets into repair or publish steps', () => {
    const rendered = renderPublishWorkflowYaml({ repoName: '@smoothbricks/codebase' });

    expect(rendered).not.toContain('NODE_AUTH_TOKEN');
    expect(rendered).not.toContain('secrets.NPM_TOKEN');
    expect(rendered).toContain('packages must already exist on npm and use trusted publishing/OIDC');
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
      version: { mode: 'new', projects: ['@scope/a', '@scope/b'] },
    });

    const outcome = await scenario.run();

    expect(outcome.validation).toEqual({
      checks: 1,
      builds: ['@scope/a', '@scope/b'],
      lints: ['@scope/a', '@scope/b'],
      tests: ['@scope/a', '@scope/b'],
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
      version: { mode: 'new', projects: ['@scope/a'] },
    });

    const outcome = await scenario.run();

    expect(outcome.fixtureRepoSetup).toBe(true);
    expect(outcome.repairedTags).toEqual([]);
    expect(outcome.repairBuildArtifacts).toEqual([]);
    expect(outcome.validation).toEqual({
      checks: 1,
      builds: ['@scope/a'],
      lints: ['@scope/a'],
      tests: ['@scope/a'],
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
  repoName?: string;
  repairs: ReleaseGap[];
  current: ReleaseGap[];
  bump: PublishWorkflowBump;
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
  publishSawValidatedRelease: boolean;
  publishCompletedTags: string[];
  remainingDurableGaps: string[];
}

function publishWorkflowScenario(config: WorkflowScenarioConfig): { run(): Promise<WorkflowOutcome> } {
  return {
    async run() {
      const state = new WorkflowScenarioState(config);
      await runPublishWorkflow(definePublishWorkflow({ repoName: config.repoName }), {
        inputs: { bump: config.bump, dryRun: config.dryRun },
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
  private publishSawValidation = false;
  private readonly repaired = new Set<string>();
  private readonly repairBuilds = new Set<string>();
  private readonly publishedCurrent = new Set<string>();
  private readonly durableGaps = new Set<string>();
  private readonly validationState = {
    checks: 0,
    builds: [] as string[],
    lints: [] as string[],
    tests: [] as string[],
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
