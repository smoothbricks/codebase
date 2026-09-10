import { spawn } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { appendFile } from 'node:fs/promises';
import { PLATFORM_TARGET_GLOBS } from '@smoothbricks/nx-plugin/workspace-config-policy';
import { $ } from 'bun';
import typia from 'typia';
import { isStageDerivedDeploy, LATE_DEPLOY_TAG, PERMANENT_DEPLOY_TAG, STAGING_DEPLOY_TAG } from '../lib/deploy-tags.js';
import {
  ciPushBranches,
  isNonEmpty,
  type NonEmptyArray,
  type PackageSmooGithub,
  parseStringArrayText,
  readSmooGithub,
} from '../lib/json.js';
import { decode, printCommandOutput, run, runStatus, runText } from '../lib/run.js';
import { type ProjectTargets, readProjectTargets } from '../nx/index.js';
import { type DeploymentStage, isPullRequestStage, parseDeploymentStage, pullRequestStage } from '../wrangler/stage.js';
import { ciApiContext, ciApiRequest } from './api.js';
import type { NxTargetRun } from './outputs.js';

export interface GithubActionsEventPayload {
  action?: string;
  repository?: {
    default_branch?: string;
    full_name?: string;
  };
  pull_request?: {
    number?: number;
    head?: {
      repo?: {
        full_name?: string;
      };
    };
  };
}

interface NxProjectDeployTarget {
  tags?: unknown;
  targets?: {
    deploy?: {
      command?: unknown;
      options?: {
        command?: unknown;
      };
    };
  };
}

const parseGithubActionsEvent = typia.json.createIsParse<GithubActionsEventPayload>();
const isGithubDeployment = typia.createIs<{ id: number }>();
const isNxProjectDeployTarget = typia.createIs<NxProjectDeployTarget>();
const parseNxProjectDeployTarget = typia.json.createIsParse<NxProjectDeployTarget>();

type NxSmartMode = 'auto' | 'affected' | 'run-many';

/** Nx workers = host cores (CI runners sized for full machine use). */
const NX_PARALLEL = '100%';

export function nxSmartArgs(
  target: string,
  mode: 'affected' | 'run-many',
  configuration?: string,
  stage?: string,
  streamOutput = false,
): string[] {
  const args = [mode, '-t', target];
  if (configuration) {
    args.push(`--configuration=${configuration}`);
  }
  if (stage) {
    args.push(`--stage=${stage}`);
  }
  if (streamOutput) {
    args.push('--outputStyle=stream-without-prefixes');
  }
  args.push(`--exclude=tag:ci:skip:${target}`, `--parallel=${NX_PARALLEL}`);
  return args;
}

export async function githubCiNxSmart(
  root: string,
  options: {
    target: string;
    name?: string;
    step?: string;
    mode?: NxSmartMode;
    configuration?: string;
    stage?: string;
    streamOutput?: boolean;
  },
): Promise<void> {
  const name = options.name ?? options.target;
  const step = options.step ?? '';
  await createGithubStatus(name, step);
  const mode = resolveNxSmartMode(options.mode ?? 'auto');
  const nxArgs = nxSmartArgs(options.target, mode, options.configuration, options.stage, options.streamOutput);
  const status = await runStatus('nx', nxArgs, root);
  await updateGithubStatus(name, status === 0 ? 'success' : 'failure', step);
  if (status !== 0) {
    throw new Error(`nx ${nxArgs.join(' ')} failed with exit code ${status}`);
  }
}

export interface NxRunManyOptions {
  targets: string;
  projects?: string;
  configuration?: string;
  collectOutputs?: string;
  collectOutputsSourceSha?: string;
  allowEmptyProjects?: boolean;
  projectsWithTargets?: string;
}

export interface ExpandedNxTargetRuns {
  runs: NxTargetRun[];
  unmatchedGlobs: string[];
}

export function expandNxTargetRuns(projects: ProjectTargets[], options: NxRunManyOptions): ExpandedNxTargetRuns {
  const namedProjects = selectProjects(projects, options.projects);
  if (options.projects !== undefined && namedProjects.length === 0 && options.allowEmptyProjects !== true) {
    throw new Error(`No Nx projects matched --projects ${options.projects}.`);
  }
  const selectedProjects = selectProjectsWithTargets(namedProjects, options.projectsWithTargets);
  if (
    options.projectsWithTargets !== undefined &&
    selectedProjects.length === 0 &&
    options.allowEmptyProjects !== true
  ) {
    throw new Error(`No Nx projects own targets matching --projects-with-targets ${options.projectsWithTargets}.`);
  }
  const selectedTargetNames = [...new Set(selectedProjects.flatMap((project) => project.targets))].sort((a, b) =>
    a.localeCompare(b),
  );
  const runs: NxTargetRun[] = [];
  const unmatchedGlobs: string[] = [];
  const addedTargets = new Set<string>();
  for (const targetPattern of commaSeparatedValues(options.targets)) {
    const isGlob = isGlobPattern(targetPattern);
    const targets = isGlob
      ? selectedTargetNames.filter((target) => new Bun.Glob(targetPattern).match(target))
      : [targetPattern];
    if (isGlob && targets.length === 0) {
      unmatchedGlobs.push(targetPattern);
    }
    for (const target of targets) {
      if (!isGlob && options.projectsWithTargets !== undefined) {
        const missingProjects = selectedProjects
          .filter((project) => !project.targets.includes(target))
          .map((project) => project.project);
        if (missingProjects.length > 0) {
          throw new Error(
            `Nx target ${target} is missing for project(s) selected by --projects-with-targets ${options.projectsWithTargets}: ${missingProjects.join(', ')}.`,
          );
        }
      }
      if (addedTargets.has(target)) {
        continue;
      }
      const owners = selectedProjects.filter((project) => project.targets.includes(target));
      const runProjects = owners.length > 0 ? owners : selectedProjects;
      if (runProjects.length === 0) {
        continue;
      }
      addedTargets.add(target);
      runs.push({ target, projects: runProjects });
    }
  }
  return { runs, unmatchedGlobs };
}

/**
 * Outputs a collection tree owns: the selected targets plus their dependency
 * closure, minus platform-suffixed dependencies.
 *
 * A platform artifact belongs to the step that selects its platform target, so
 * the same file never lands in two collected trees (`apply-outputs` rejects
 * that, and would have to trust two producers of one path). The aggregate
 * `build` reaches host-platform targets because the runner happens to be that
 * platform - an accident of scheduling, not release-artifact ownership.
 */
export function expandNxTargetDependencyRuns(runs: NxTargetRun[]): NxTargetRun[] {
  const expanded: NxTargetRun[] = [];
  const added = new Set<string>();
  const visiting = new Set<string>();

  const visit = (project: ProjectTargets, target: string): void => {
    const key = `${project.project}:${target}`;
    if (added.has(key)) {
      return;
    }
    if (visiting.has(key)) {
      throw new Error(`Nx target dependency cycle detected at ${key}.`);
    }
    visiting.add(key);
    for (const dependency of project.targetDependencies?.get(target) ?? []) {
      if (dependency.startsWith('^')) {
        continue;
      }
      const dependencyTargets = isGlobPattern(dependency)
        ? project.targets.filter((candidate) => new Bun.Glob(dependency).match(candidate))
        : project.targets.includes(dependency)
          ? [dependency]
          : [];
      for (const dependencyTarget of dependencyTargets
        .filter((candidate) => !isPlatformTargetName(candidate))
        .sort((left, right) => left.localeCompare(right))) {
        visit(project, dependencyTarget);
      }
    }
    visiting.delete(key);
    added.add(key);
    if ((project.targetOutputs?.get(target)?.length ?? 0) > 0) {
      expanded.push({ target, projects: [project] });
    }
  };

  for (const run of runs) {
    for (const project of run.projects) {
      visit(project, run.target);
    }
  }
  return expanded;
}

export function nxRunManyArgs(run: NxTargetRun, configuration?: string): string[] {
  return nxRunManyBatchArgs([run], configuration);
}

/** One `nx run-many` for every selected target so independent rustc graphs overlap. */
export function nxRunManyBatchArgs(runs: NxTargetRun[], configuration?: string): string[] {
  if (runs.length === 0) {
    throw new Error('Nx run-many has no selected targets.');
  }
  const targets = [...new Set(runs.map((run) => run.target))];
  const projects = [...new Set(runs.flatMap((run) => run.projects.map((project) => project.project)))].sort(
    (left, right) => left.localeCompare(right),
  );
  if (projects.length === 0) {
    throw new Error(`Nx targets ${targets.join(',')} have no selected projects.`);
  }
  const nxArgs = ['run-many', '-t', targets.join(','), `--projects=${projects.join(',')}`];
  if (configuration) {
    nxArgs.push(`--configuration=${configuration}`);
  }
  nxArgs.push(`--parallel=${NX_PARALLEL}`);
  return nxArgs;
}

export async function readGitHeadSha(root: string): Promise<string> {
  // GitHub CI requires a valid repository root; runText keeps Git's diagnostics visible.
  return (await runText('git', ['rev-parse', 'HEAD'], root)).trim();
}

export async function githubCiNxRunMany(root: string, options: NxRunManyOptions): Promise<ExpandedNxTargetRuns> {
  const expanded = expandNxTargetRuns(await readProjectTargets(root), options);
  if (expanded.unmatchedGlobs.length > 0) {
    console.log(`No Nx targets matched target glob(s): ${expanded.unmatchedGlobs.join(', ')}; skipping.`);
  }
  if (expanded.runs.length > 0) {
    await run('nx', nxRunManyBatchArgs(expanded.runs, options.configuration), root);
  }
  if (options.collectOutputs) {
    const { collectNxOutputs } = await loadOutputBoundary();
    // Ambient HEAD identifies the source only when the collecting job is also the
    // job that publishes it. A job that versions independently commits its own
    // bump, so its HEAD is a coordinate no other job can name; such callers must
    // declare the shared coordinate their consumer validates against instead.
    const sourceSha = options.collectOutputsSourceSha ?? (await readGitHeadSha(root));
    await collectNxOutputs(root, options.collectOutputs, expandNxTargetDependencyRuns(expanded.runs), sourceSha);
  }
  return expanded;
}

export async function githubCiApplyOutputs(
  root: string,
  directories: string[],
  expectedSourceSha: string,
): Promise<void> {
  const { applyCollectedOutputs } = await loadOutputBoundary();
  await applyCollectedOutputs(root, directories, expectedSourceSha);
}

async function loadOutputBoundary() {
  if (import.meta.url.endsWith('/src/github-ci/index.ts')) {
    // The source self-hosting shim has no Typia transform; register it before loading manifest validators.
    await import('@smoothbricks/cli/bun/preload');
  }
  // This boundary must stay lazy because the transformed dist and source self-hosting paths initialize Typia differently.
  return import('./outputs.js');
}

function isGlobPattern(value: string): boolean {
  return /[*?{[]/.test(value);
}

function isPlatformTargetName(target: string): boolean {
  return PLATFORM_TARGET_GLOBS.some((glob) => target.endsWith(glob.slice(1)));
}

function selectProjects(projects: ProjectTargets[], selectors: string | undefined): ProjectTargets[] {
  if (selectors === undefined) {
    return projects.slice().sort((left, right) => left.project.localeCompare(right.project));
  }
  const patterns = commaSeparatedValues(selectors);
  return projects
    .filter((project) => patterns.some((pattern) => new Bun.Glob(pattern).match(project.project)))
    .sort((left, right) => left.project.localeCompare(right.project));
}

function selectProjectsWithTargets(projects: ProjectTargets[], selectors: string | undefined): ProjectTargets[] {
  if (selectors === undefined) {
    return projects;
  }
  const patterns = commaSeparatedValues(selectors);
  return projects.filter((project) =>
    project.targets.some((target) => patterns.some((pattern) => new Bun.Glob(pattern).match(target))),
  );
}

function commaSeparatedValues(value: string): string[] {
  return value
    .split(',')
    .map((entry) => entry.trim())
    .filter(Boolean);
}

export interface GithubCiNxDeployOptions {
  stage?: string;
  mode?: NxSmartMode;
  name?: string;
  step?: string;
  verify?: boolean;
  selectTag?: string;
}

/** A project `nx-deploy` will deploy, and whether it waits for the rest of the selection (LATE_DEPLOY_TAG). */
export interface DeployProject {
  name: string;
  late: boolean;
}

export interface GithubCiNxDeployDependencies {
  listProjects?: (
    root: string,
    mode: 'affected' | 'run-many',
    stage: DeploymentStage,
    selectTag?: string,
  ) => Promise<DeployProject[]>;
  runNx?: (args: string[], root: string) => Promise<number>;
  appendSummary?: (summaryPath: string, content: string) => Promise<void>;
  appendOutput?: (outputPath: string, content: string) => Promise<void>;
  publishDeployment?: (stage: `pr${number}`, url: string) => Promise<void>;
  setStatus?: (state: 'pending' | 'success' | 'failure') => Promise<void>;
  processEnv?: NodeJS.ProcessEnv;
  eventPayload?: GithubActionsEventPayload;
  /** The root manifest's `smoo.github` block; read from `root` when absent. */
  github?: PackageSmooGithub;
}

export async function githubCiNxDeploy(
  root: string,
  options: GithubCiNxDeployOptions,
  dependencies: GithubCiNxDeployDependencies = {},
): Promise<void> {
  const processEnv = dependencies.processEnv ?? process.env;
  const eventPayload = dependencies.eventPayload ?? readGithubActionsEvent(processEnv);
  const github = dependencies.github ?? readSmooGithub(root);
  const [stagingPushBranch] = ciPushBranches(github);
  const stage = resolveDeploymentStage(options.stage, processEnv, eventPayload, stagingPushBranch);
  const name = options.name ?? 'Deploy Stage';
  const step = options.step ?? '';
  const setStatus =
    dependencies.setStatus ??
    ((state: 'pending' | 'success' | 'failure') =>
      state === 'pending' ? createGithubStatus(name, step) : updateGithubStatus(name, state, step));
  const mode = resolveNxSmartMode(options.mode ?? 'run-many');
  const listProjects = dependencies.listProjects ?? listNxDeployProjects;
  const runNx = dependencies.runNx ?? ((args: string[], commandRoot: string) => runStatus('nx', args, commandRoot));
  const projects = await listProjects(root, mode, stage, options.selectTag);
  if (projects.length === 0) {
    console.log(`No ${mode} deploy projects; skipping ${stage}.`);
    await setStatus('pending');
    await setStatus('success');
    return;
  }
  // Resolved only for a non-empty pull-request selection, before any status mutation or deploy: a missing or bad
  // template must fail here, not after the deploy with the status left pending.
  const preview = isPullRequestStage(stage)
    ? { stage, urls: previewUrlsForStage(github?.previewUrls, stage) }
    : undefined;
  await setStatus('pending');

  const targets = options.verify === true ? ['build', 'lint', 'test', 'deploy'] : ['deploy'];
  const names = projects.map((project) => project.name);
  for (const target of targets) {
    // Build, lint and test run over the whole selection at once; only the deploy waits for its late round.
    const rounds = target === 'deploy' ? deployRounds(projects) : [names];
    for (const round of rounds) {
      const nxArgs = [
        'run-many',
        '-t',
        target,
        `--projects=${round.join(',')}`,
        `--exclude=${deployExclusions(stage)}`,
        `--parallel=${NX_PARALLEL}`,
      ];
      if (target === 'deploy') {
        nxArgs.push(`--stage=${stage}`);
      }
      const status = await runNx(nxArgs, root);
      if (status !== 0) {
        await setStatus('failure');
        throw new Error(`nx ${nxArgs.join(' ')} failed with exit code ${status}`);
      }
    }
  }

  if (preview) {
    const summaryPath = processEnv.GITHUB_STEP_SUMMARY;
    if (summaryPath) {
      const appendSummary = dependencies.appendSummary ?? appendFile;
      await appendSummary(
        summaryPath,
        `## ${stage} deployment\n\n${preview.urls.map((url) => `- [${url}](${url})`).join('\n')}\n`,
      );
    }
    const publishDeployment =
      dependencies.publishDeployment ??
      ((token: `pr${number}`, deploymentUrl: string) => publishGithubDeployment(token, deploymentUrl, processEnv));
    // GitHub shows one environment URL per deployment, so the first template is the PR's entry point.
    await publishDeployment(preview.stage, preview.urls[0]);
  }

  const outputPath = processEnv.GITHUB_OUTPUT;
  if (outputPath) {
    try {
      const appendOutput = dependencies.appendOutput ?? appendFile;
      await appendOutput(
        outputPath,
        `stage=${stage}
`,
      );
    } catch (error) {
      await setStatus('failure');
      throw error;
    }
  }
  await setStatus('success');
}

function resolveNxSmartMode(mode: NxSmartMode): 'affected' | 'run-many' {
  if (mode === 'affected' || mode === 'run-many') {
    return mode;
  }
  // No successful-workflow baseline is resolved on Forgejo. A previous commit
  // is not equivalent: it can omit changes from an earlier failed CI run.
  if ((process.env.FORGEJO_REPOSITORY || process.env.GITHUB_REPOSITORY) && ciApiContext().forgejo) {
    return 'run-many';
  }
  const defaultBranch = eventDefaultBranch() ?? 'main';
  if (process.env.GITHUB_EVENT_NAME === 'push') {
    return process.env.GITHUB_REF_NAME === defaultBranch ? 'run-many' : 'affected';
  }
  // A PR into a NON-default branch is an integration surface (e.g. a mirror-sync
  // review branch): its base moves outside the default-branch workflow that
  // affected scoping is calibrated against, so under-selection can pass PR CI
  // and only fail after merge. Validate those PRs in full.
  if (process.env.GITHUB_EVENT_NAME === 'pull_request') {
    const base = process.env.GITHUB_BASE_REF;
    return base && base !== defaultBranch ? 'run-many' : 'affected';
  }
  return 'affected';
}

/** The repository default branch from the Actions event payload. */
function eventDefaultBranch(): string | undefined {
  const eventPath = process.env.GITHUB_EVENT_PATH;
  if (!eventPath) return undefined;
  try {
    const payload = parseGithubActionsEvent(readFileSync(eventPath, 'utf8'));
    return payload?.repository?.default_branch || undefined;
  } catch {
    return undefined;
  }
}

export function resolveDeploymentStage(
  explicit: string | undefined,
  environment: NodeJS.ProcessEnv,
  event: GithubActionsEventPayload | undefined,
  stagingPushBranch: string,
): DeploymentStage {
  if (explicit !== undefined) return parseDeploymentStage(explicit);
  if (environment.GITHUB_EVENT_NAME === 'pull_request') {
    if (!event?.action || !['opened', 'reopened', 'synchronize'].includes(event.action)) {
      throw new Error('Pull-request deployment runs only for opened, reopened, or synchronize events.');
    }
    const repository = event.repository?.full_name;
    const headRepository = event.pull_request?.head?.repo?.full_name;
    if (!repository || !headRepository || repository !== headRepository) {
      throw new Error('Pull-request deployment is restricted to same-repository pull requests.');
    }
    const number = event.pull_request?.number;
    if (number === undefined) throw new Error('GitHub pull_request event is missing its PR number.');
    return pullRequestStage(number);
  }
  if (environment.GITHUB_EVENT_NAME === 'push' && environment.GITHUB_REF_NAME === stagingPushBranch) {
    return 'staging';
  }
  if (environment.GITHUB_EVENT_NAME === 'release') {
    return 'production';
  }
  throw new Error('Cannot resolve a deployment stage from this GitHub event; pass --stage explicitly.');
}

function readGithubActionsEvent(environment: NodeJS.ProcessEnv): GithubActionsEventPayload | undefined {
  const eventPath = environment.GITHUB_EVENT_PATH;
  if (!eventPath) return undefined;
  try {
    return parseGithubActionsEvent(readFileSync(eventPath, 'utf8')) || undefined;
  } catch {
    return undefined;
  }
}

function deployExclusions(stage: DeploymentStage): string {
  const tags = [`tag:${PERMANENT_DEPLOY_TAG}`];
  if (stage !== 'staging') tags.push(`tag:${STAGING_DEPLOY_TAG}`);
  return tags.join(',');
}

/** Preview URLs for a pull-request stage from the configured templates; `{stage}` is the only placeholder. */
export function previewUrlsForStage(templates: string[] | undefined, stage: `pr${number}`): NonEmptyArray<string> {
  if (!templates || !isNonEmpty(templates)) {
    throw new Error(
      'smoo.github.previewUrls is not configured; refusing to invent a preview hostname. ' +
        'SMOO_PREVIEW_ZONE is no longer read: set previewUrls to one or more URL templates containing {stage}.',
    );
  }
  const [first, ...rest] = templates;
  return [previewUrlFromTemplate(first, stage), ...rest.map((template) => previewUrlFromTemplate(template, stage))];
}

/** A template without `{stage}` would publish one URL for every pull request, so it is refused. */
function previewUrlFromTemplate(template: string, stage: string): string {
  if (!template.includes('{stage}')) throw new Error(`preview URL template "${template}" must contain {stage}`);
  return template.replaceAll('{stage}', stage);
}

async function listNxDeployProjects(
  root: string,
  mode: 'affected' | 'run-many',
  stage: DeploymentStage,
  selectTag?: string,
): Promise<DeployProject[]> {
  const listArgs = ['show', 'projects'];
  if (mode === 'affected') listArgs.push('--affected');
  listArgs.push('--withTarget', 'deploy', `--exclude=${deployExclusions(stage)}`, '--json');
  const candidates = nxProjectList(await runText('nx', listArgs, root)).sort((left, right) =>
    left.localeCompare(right),
  );
  return selectStageDeployProjects(candidates, stage, selectTag, async (project) => {
    const parsed = parseNxProjectDeployTarget(await runText('nx', ['show', 'project', project, '--json'], root));
    if (!parsed) throw new Error(`nx show project ${project} returned invalid JSON.`);
    return parsed;
  });
}

export async function selectStageDeployProjects(
  candidates: string[],
  stage: DeploymentStage,
  requireTag: string | undefined,
  loadProject: (project: string) => Promise<unknown>,
): Promise<DeployProject[]> {
  const selected: DeployProject[] = [];
  for (const project of candidates) {
    const definition = await loadProject(project);
    if (!isNxProjectDeployTarget(definition)) continue;
    const deploy = definition.targets?.deploy;
    const commandValue = deploy?.options?.command ?? deploy?.command;
    const tags = Array.isArray(definition.tags)
      ? definition.tags.filter((tag): tag is string => typeof tag === 'string')
      : [];
    const isStageDerived = isStageDerivedDeploy(tags, typeof commandValue === 'string' ? commandValue : undefined);
    const isStagingOnly = tags.includes(STAGING_DEPLOY_TAG);
    // A required tag narrows the stage rules; it never selects a project they exclude.
    if (requireTag && !tags.includes(requireTag)) continue;
    if (isStageDerived || (stage === 'staging' && isStagingOnly)) {
      selected.push({ name: project, late: tags.includes(LATE_DEPLOY_TAG) });
    }
  }
  return selected;
}

/** The deploy's `nx run-many` rounds: everything else first, then the late projects; an empty round is dropped. */
export function deployRounds(projects: DeployProject[]): string[][] {
  const early: string[] = [];
  const late: string[] = [];
  for (const project of projects) {
    (project.late ? late : early).push(project.name);
  }
  return [early, late].filter((round) => round.length > 0);
}

export interface GithubApiProcessResult {
  exitCode: number;
  stdout: string;
  stderr: string;
}

export interface GithubApiProcessRunner {
  run(args: string[], input: string, cwd: string): Promise<GithubApiProcessResult>;
}

export class NodeGithubApiProcessRunner implements GithubApiProcessRunner {
  run(args: string[], input: string, cwd: string): Promise<GithubApiProcessResult> {
    const { promise, resolve, reject } = Promise.withResolvers<GithubApiProcessResult>();
    const child = spawn('gh', args, { cwd, stdio: ['pipe', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    child.stdout.setEncoding('utf8');
    child.stderr.setEncoding('utf8');
    child.stdout.on('data', (chunk: string) => {
      stdout += chunk;
    });
    child.stderr.on('data', (chunk: string) => {
      stderr += chunk;
    });
    child.once('error', (error) => {
      reject(new Error(`gh ${args.join(' ')} failed to start`, { cause: error }));
    });
    child.once('close', (code) => {
      resolve({ exitCode: code ?? -1, stdout, stderr });
    });
    child.stdin.end(input);
    return promise;
  }
}

export async function publishGithubDeployment(
  environment: `pr${number}`,
  url: string,
  processEnvironment: NodeJS.ProcessEnv,
  runner: GithubApiProcessRunner = new NodeGithubApiProcessRunner(),
): Promise<void> {
  const repository = processEnvironment.GITHUB_REPOSITORY;
  const sha = processEnvironment.GITHUB_SHA;
  if (!repository || !sha) throw new Error('GITHUB_REPOSITORY and GITHUB_SHA are required to publish a deployment.');
  if (ciApiContext(processEnvironment).forgejo) {
    await ciApiRequest(
      `/statuses/${sha}`,
      'POST',
      {
        state: 'success',
        context: `deployment/${environment}`,
        description: `Deployed ${environment}`,
        target_url: url,
      },
      processEnvironment,
    );
    return;
  }
  const createBody = JSON.stringify({
    ref: sha,
    environment,
    auto_merge: false,
    required_contexts: [],
    transient_environment: true,
    production_environment: false,
  });
  const createArgs = [
    'api',
    '--method',
    'POST',
    '-H',
    'Accept: application/vnd.github+json',
    `/repos/${repository}/deployments`,
    '--input',
    '-',
  ];
  const createResult = await runner.run(createArgs, createBody, process.cwd());
  if (createResult.exitCode !== 0) {
    printCommandOutput(createResult.stdout, createResult.stderr);
    throw new Error(`gh ${createArgs.join(' ')} failed with exit code ${createResult.exitCode}`);
  }
  let deployment: unknown;
  try {
    deployment = JSON.parse(createResult.stdout);
  } catch {
    printCommandOutput(createResult.stdout, createResult.stderr);
    throw new Error(`gh ${createArgs.join(' ')} returned invalid JSON while creating a deployment.`);
  }
  if (!isGithubDeployment(deployment)) {
    printCommandOutput(createResult.stdout, createResult.stderr);
    throw new Error(`gh ${createArgs.join(' ')} returned an invalid deployment response.`);
  }
  const statusBody = JSON.stringify({
    state: 'success',
    environment,
    environment_url: url,
    auto_inactive: false,
  });
  const statusArgs = [
    'api',
    '--method',
    'POST',
    '-H',
    'Accept: application/vnd.github+json',
    `/repos/${repository}/deployments/${deployment.id}/statuses`,
    '--input',
    '-',
  ];
  const statusResult = await runner.run(statusArgs, statusBody, process.cwd());
  if (statusResult.exitCode !== 0) {
    printCommandOutput(statusResult.stdout, statusResult.stderr);
    throw new Error(`gh ${statusArgs.join(' ')} failed with exit code ${statusResult.exitCode}`);
  }
}

function nxProjectList(output: string): string[] {
  return parseStringArrayText(output) ?? [];
}

async function createGithubStatus(name: string, step: string): Promise<void> {
  await postGithubStatus(name, 'pending', `Running ${name}...`, step);
}

async function updateGithubStatus(name: string, state: 'success' | 'failure' | 'error', step: string): Promise<void> {
  const suffix = state === 'success' ? 'passed' : state === 'failure' ? 'failed' : 'errored';
  await postGithubStatus(name, state, `${name} ${suffix}`, step);
}

async function postGithubStatus(name: string, state: string, description: string, step: string): Promise<void> {
  const repository = process.env.GITHUB_REPOSITORY;
  const sha = process.env.GITHUB_SHA;
  if (!repository || !sha) {
    return;
  }
  if (!githubCommitStatusesWritable(process.env, readGithubActionsEvent(process.env))) {
    console.log(`Skipping GitHub commit status ${name}=${state}: fork pull requests receive a read-only GITHUB_TOKEN.`);
    return;
  }
  const targetUrl = await getGithubStepUrl(step);
  await ciApiRequest(`/statuses/${sha}`, 'POST', {
    state,
    context: name,
    description,
    ...(targetUrl ? { target_url: targetUrl } : {}),
  });
}

async function getGithubStepUrl(step: string): Promise<string | null> {
  const repository = process.env.GITHUB_REPOSITORY;
  const runId = process.env.GITHUB_RUN_ID;
  const job = process.env.GITHUB_JOB;
  if (!repository || !runId || !job) {
    return null;
  }
  const { forgejo, htmlBase } = ciApiContext();
  if (forgejo) return `${htmlBase}/${repository}/actions/runs/${runId}`;
  const result =
    await $`gh api -H ${'Accept: application/vnd.github+json'} ${`/repos/${repository}/actions/runs/${runId}/jobs`} --jq ${`.jobs[] | select(.name == "${job}") | .id`}`
      .quiet()
      .nothrow();
  const jobId = decode(result.stdout).trim();
  if (!jobId) {
    return `${htmlBase}/${repository}/actions/runs/${runId}`;
  }
  return step
    ? `${htmlBase}/${repository}/actions/runs/${runId}/job/${jobId}#step:${step}:1`
    : `${htmlBase}/${repository}/actions/runs/${runId}/job/${jobId}`;
}

export function githubCommitStatusesWritable(
  environment: NodeJS.ProcessEnv,
  event: GithubActionsEventPayload | undefined,
): boolean {
  if (environment.GITHUB_EVENT_NAME !== 'pull_request') {
    return true;
  }
  const repository = environment.GITHUB_REPOSITORY;
  const headRepository = event?.pull_request?.head?.repo?.full_name;
  // GitHub downgrades GITHUB_TOKEN to read-only for fork pull_request jobs. Only a
  // positively identified fork is skipped: incomplete payloads still attempt the
  // write and preserve the API error instead of silently hiding a permissions fault.
  return !repository || !headRepository || headRepository === repository;
}
