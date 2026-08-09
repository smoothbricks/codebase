import { spawn } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { appendFile, mkdtemp, realpath, rename, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { $ } from 'bun';
import typia from 'typia';
import { parseStringArrayText } from '../lib/json.js';
import { decode, run, runStatus } from '../lib/run.js';
import { type ProjectTargets, readProjectTargets } from '../nx/index.js';
import {
  type EnvironmentToken,
  isPullRequestEnvironment,
  parseEnvironmentToken,
  pullRequestEnvironment,
} from '../wrangler/environment.js';
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

export async function cleanupGithubCiCache(root: string): Promise<void> {
  const githubOutput = process.env.GITHUB_OUTPUT;
  const markCacheReady = async (ready: boolean): Promise<void> => {
    if (githubOutput) {
      await appendFile(githubOutput, `cache-ready=${ready ? 'true' : 'false'}\n`);
    }
  };

  const nar = process.env.NIX_STORE_NAR;
  if (!nar) {
    console.warn('NIX_STORE_NAR is not set; skipping Nix cache save.');
    await markCacheReady(false);
    return;
  }
  const nixStore = '/nix/var/nix/profiles/default/bin/nix-store';
  const devenvProfile = `${root}/tooling/direnv/.devenv/profile`;
  if (!existsSync(devenvProfile)) {
    console.warn(`${devenvProfile} is missing; skipping Nix cache save.`);
    await markCacheReady(false);
    return;
  }
  await runStatus(nixStore, ['--verify', '--check-contents', '--repair'], root);
  await exportNixStoreCache(root, nar, nixStore, devenvProfile);
  await markCacheReady(true);
}

async function exportNixStoreCache(root: string, nar: string, nixStore: string, devenvProfile: string): Promise<void> {
  const gcRootDir = '/nix/var/nix/gcroots/smoothbricks-cache-roots';
  const tmpDir = await mkdtemp(join(dirname(nar), '.smoo-nix-cache-'));
  const tmpNar = join(tmpDir, 'nix-store.nar');
  const roots = new Set<string>();

  try {
    await $`rm -f ${nar}`.cwd(root);
    await $`sudo rm -rf ${gcRootDir}`.cwd(root);
    await $`sudo mkdir -p ${gcRootDir}`.cwd(root);

    // The Nix cache must include every live store path referenced by the
    // restored shell state, not just the devenv profile. .direnv stores paths to
    // derivations like devenv-shell.drv; omitting those makes a cache hit restore
    // metadata that points at missing store paths.
    await addRoot(roots, devenvProfile);
    const home = process.env.HOME;
    if (home) {
      const nixProfile = join(home, '.nix-profile');
      await addRoot(roots, nixProfile);
      await addReferencesFrom(roots, nixProfile, root);
    }
    await addReferencesFrom(roots, join(root, 'tooling/direnv/.devenv'), root);
    await addReferencesFrom(roots, join(root, 'tooling/direnv/.direnv'), root);

    const rootLinks: string[] = [];
    let index = 0;
    for (const target of roots) {
      const link = `${gcRootDir}/root-${index}`;
      await $`sudo ln -s ${target} ${link}`.cwd(root);
      rootLinks.push(link);
      index += 1;
    }

    if (rootLinks.length === 0) {
      throw new Error('No live Nix store roots found; skipping Nix cache save.');
    }

    await $`nix-collect-garbage --quiet`.cwd(root);
    const closureOutput = await $`sudo ${nixStore} -qR ${rootLinks}`.cwd(root).quiet();
    const closure = decode(closureOutput.stdout)
      .split('\n')
      .map((line) => line.trim())
      .filter(Boolean);
    if (closure.length === 0) {
      throw new Error('No Nix store closure paths found; skipping Nix cache save.');
    }

    await $`sudo ${nixStore} --export --quiet ${closure} > ${tmpNar}`.cwd(root);
    await $`test -s ${tmpNar}`.cwd(root);
    await rename(tmpNar, nar);
  } finally {
    await $`sudo rm -rf ${gcRootDir}`.cwd(root).nothrow();
    await rm(tmpDir, { recursive: true, force: true });
  }
}

async function addRoot(roots: Set<string>, candidate: string): Promise<void> {
  if (!existsSync(candidate)) {
    return;
  }
  const target = await realpath(candidate);
  if (existsSync(target)) {
    roots.add(target);
  }
}

async function addReferencesFrom(roots: Set<string>, path: string, cwd: string): Promise<void> {
  if (!existsSync(path)) {
    return;
  }
  const storePathPattern = '/nix/store/[a-z0-9]{32}-[A-Za-z0-9+._?=-]+';
  const result = await $`grep -rahoE ${storePathPattern} ${path}`.cwd(cwd).quiet().nothrow();
  for (const line of decode(result.stdout).split('\n')) {
    const candidate = line.trim();
    if (candidate && existsSync(candidate)) {
      roots.add(candidate);
    }
  }
}

export function nxSmartArgs(target: string, mode: 'affected' | 'run-many', configuration?: string): string[] {
  const args = [mode, '-t', target];
  if (configuration) {
    args.push(`--configuration=${configuration}`);
  }
  args.push(`--exclude=tag:ci:skip:${target}`, `--parallel=${NX_PARALLEL}`);
  return args;
}

export async function githubCiNxSmart(
  root: string,
  options: { target: string; name?: string; step?: string; mode?: NxSmartMode; configuration?: string },
): Promise<void> {
  const name = options.name ?? options.target;
  const step = options.step ?? '';
  await createGithubStatus(name, step);
  const mode = resolveNxSmartMode(options.mode ?? 'auto');
  const nxArgs = nxSmartArgs(options.target, mode, options.configuration);
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
      for (const dependencyTarget of dependencyTargets.sort((left, right) => left.localeCompare(right))) {
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
  if (run.projects.length === 0) {
    throw new Error(`Nx target ${run.target} has no selected projects.`);
  }
  const nxArgs = [
    'run-many',
    '-t',
    run.target,
    `--projects=${run.projects.map((project) => project.project).join(',')}`,
  ];
  if (configuration) {
    nxArgs.push(`--configuration=${configuration}`);
  }
  nxArgs.push(`--parallel=${NX_PARALLEL}`);
  return nxArgs;
}

export async function readGitHeadSha(root: string): Promise<string> {
  // invariant throw: GitHub CI commands require a valid repository root.
  return decode((await $`git rev-parse HEAD`.cwd(root).quiet()).stdout).trim();
}

export async function githubCiNxRunMany(root: string, options: NxRunManyOptions): Promise<ExpandedNxTargetRuns> {
  const expanded = expandNxTargetRuns(await readProjectTargets(root), options);
  if (expanded.unmatchedGlobs.length > 0) {
    console.log(`No Nx targets matched target glob(s): ${expanded.unmatchedGlobs.join(', ')}; skipping.`);
  }
  for (const targetRun of expanded.runs) {
    await run('nx', nxRunManyArgs(targetRun, options.configuration), root);
  }
  if (options.collectOutputs) {
    const { collectNxOutputs } = await loadOutputBoundary();
    const sourceSha = await readGitHeadSha(root);
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
  environment?: string;
  mode?: NxSmartMode;
  name?: string;
  step?: string;
  verify?: boolean;
}

export interface GithubCiNxDeployDependencies {
  listProjects?: (root: string, target: string, mode: 'affected' | 'run-many') => Promise<string[]>;
  runNx?: (args: string[], root: string) => Promise<number>;
  appendSummary?: (summaryPath: string, content: string) => Promise<void>;
  publishDeployment?: (environment: `pr${number}`, url: string) => Promise<void>;
  setStatus?: (state: 'pending' | 'success' | 'failure') => Promise<void>;
  processEnv?: NodeJS.ProcessEnv;
  eventPayload?: GithubActionsEventPayload;
}

export async function githubCiNxDeploy(
  root: string,
  options: GithubCiNxDeployOptions,
  dependencies: GithubCiNxDeployDependencies = {},
): Promise<void> {
  const processEnv = dependencies.processEnv ?? process.env;
  const eventPayload = dependencies.eventPayload ?? readGithubActionsEvent(processEnv);
  const environment = resolveDeploymentEnvironment(options.environment, processEnv, eventPayload);
  const name = options.name ?? 'Deploy Environment';
  const step = options.step ?? '';
  const setStatus =
    dependencies.setStatus ??
    ((state: 'pending' | 'success' | 'failure') =>
      state === 'pending' ? createGithubStatus(name, step) : updateGithubStatus(name, state, step));
  await setStatus('pending');
  const mode = resolveNxSmartMode(options.mode ?? 'run-many');
  const listProjects = dependencies.listProjects ?? listNxProjectsWithTarget;
  const runNx = dependencies.runNx ?? ((args: string[], commandRoot: string) => runStatus('nx', args, commandRoot));
  const projects = await listProjects(root, 'deploy', mode);
  if (projects.length === 0) {
    console.log(`No ${mode} deploy projects; skipping ${environment}.`);
    await setStatus('success');
    return;
  }

  const projectList = projects.join(',');
  const targets = options.verify === true ? ['build', 'lint', 'test', 'deploy'] : ['deploy'];
  for (const target of targets) {
    const nxArgs = [
      'run-many',
      '-t',
      target,
      `--projects=${projectList}`,
      '--exclude=tag:permanent-deploy-target',
      `--parallel=${NX_PARALLEL}`,
    ];
    if (target === 'deploy') {
      nxArgs.push(`--environment=${environment}`);
    }
    const status = await runNx(nxArgs, root);
    if (status !== 0) {
      await setStatus('failure');
      throw new Error(`nx ${nxArgs.join(' ')} failed with exit code ${status}`);
    }
  }

  if (isPullRequestEnvironment(environment)) {
    const url = `https://app.${environment}.conloca.com`;
    const summaryPath = processEnv.GITHUB_STEP_SUMMARY;
    if (summaryPath) {
      const appendSummary = dependencies.appendSummary ?? appendFile;
      await appendSummary(summaryPath, `## ${environment} deployment\n\n[View deployment](${url})\n`);
    }
    const publishDeployment =
      dependencies.publishDeployment ??
      ((token: `pr${number}`, deploymentUrl: string) => publishGithubDeployment(token, deploymentUrl, processEnv));
    await publishDeployment(environment, url);
  }
  if (environment !== 'production') {
    const e2eProjects = await listProjects(root, 'e2e-deployed', 'run-many');
    if (e2eProjects.length > 0) {
      const nxArgs = [
        'run-many',
        '-t',
        'e2e-deployed',
        `--projects=${e2eProjects.join(',')}`,
        `--parallel=${NX_PARALLEL}`,
        `--environment=${environment}`,
      ];
      const status = await runNx(nxArgs, root);
      if (status !== 0) {
        await setStatus('failure');
        throw new Error(`nx ${nxArgs.join(' ')} failed with exit code ${status}`);
      }
    }
  }

  await setStatus('success');
}

function resolveNxSmartMode(mode: NxSmartMode): 'affected' | 'run-many' {
  if (mode === 'affected' || mode === 'run-many') {
    return mode;
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

export function resolveDeploymentEnvironment(
  explicit: string | undefined,
  environment: NodeJS.ProcessEnv,
  event: GithubActionsEventPayload | undefined,
): EnvironmentToken {
  if (explicit !== undefined) return parseEnvironmentToken(explicit);
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
    return pullRequestEnvironment(number);
  }
  if (environment.GITHUB_EVENT_NAME === 'push' && environment.GITHUB_REF_NAME === 'private') {
    return 'staging';
  }
  if (environment.GITHUB_EVENT_NAME === 'release') {
    return 'production';
  }
  throw new Error('Cannot resolve a deployment environment from this GitHub event; pass --environment explicitly.');
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

async function listNxProjectsWithTarget(
  root: string,
  target: string,
  mode: 'affected' | 'run-many',
): Promise<string[]> {
  const listArgs = ['show', 'projects'];
  if (mode === 'affected') listArgs.push('--affected');
  listArgs.push('--withTarget', target);
  if (target === 'deploy') listArgs.push('--exclude=tag:permanent-deploy-target');
  listArgs.push('--json');
  const result = await $`nx ${listArgs}`.cwd(root).quiet();
  const candidates = nxProjectList(decode(result.stdout)).sort((left, right) => left.localeCompare(right));
  if (target !== 'deploy') return candidates;
  return selectEnvironmentDeployProjects(candidates, async (project) => {
    const projectResult = await $`nx show project ${project} --json`.cwd(root).quiet();
    const parsed = parseNxProjectDeployTarget(decode(projectResult.stdout));
    if (!parsed) throw new Error(`nx show project ${project} returned invalid JSON.`);
    return parsed;
  });
}

export async function selectEnvironmentDeployProjects(
  candidates: string[],
  loadProject: (project: string) => Promise<unknown>,
): Promise<string[]> {
  const selected: string[] = [];
  for (const project of candidates) {
    const definition = await loadProject(project);
    if (!isNxProjectDeployTarget(definition)) continue;
    const deploy = definition.targets?.deploy;
    const commandValue = deploy?.options?.command ?? deploy?.command;
    if (typeof commandValue === 'string' && commandValue.includes('smoo wrangler deploy-environment')) {
      selected.push(project);
    }
  }
  return selected;
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
    child.once('error', reject);
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
  const createBody = JSON.stringify({
    ref: sha,
    environment,
    auto_merge: false,
    required_contexts: [],
    transient_environment: true,
    production_environment: false,
  });
  const createResult = await runner.run(
    [
      'api',
      '--method',
      'POST',
      '-H',
      'Accept: application/vnd.github+json',
      `/repos/${repository}/deployments`,
      '--input',
      '-',
    ],
    createBody,
    process.cwd(),
  );
  if (createResult.exitCode !== 0) {
    throw new Error(
      `GitHub deployment creation failed with exit code ${createResult.exitCode}: ${createResult.stderr.trim()}`,
    );
  }
  let deployment: unknown;
  try {
    deployment = JSON.parse(createResult.stdout);
  } catch {
    throw new Error('GitHub returned invalid JSON while creating a deployment.');
  }
  if (!isGithubDeployment(deployment)) throw new Error('GitHub returned an invalid deployment response.');
  const statusBody = JSON.stringify({
    state: 'success',
    environment,
    environment_url: url,
    auto_inactive: false,
  });
  const statusResult = await runner.run(
    [
      'api',
      '--method',
      'POST',
      '-H',
      'Accept: application/vnd.github+json',
      `/repos/${repository}/deployments/${deployment.id}/statuses`,
      '--input',
      '-',
    ],
    statusBody,
    process.cwd(),
  );
  if (statusResult.exitCode !== 0) {
    throw new Error(
      `GitHub deployment status failed with exit code ${statusResult.exitCode}: ${statusResult.stderr.trim()}`,
    );
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
  const targetUrl = await getGithubStepUrl(step);
  const args = [
    'api',
    '--method',
    'POST',
    '-H',
    'Accept: application/vnd.github+json',
    `/repos/${repository}/statuses/${sha}`,
    '-f',
    `state=${state}`,
    '-f',
    `context=${name}`,
    '-f',
    `description=${description}`,
  ];
  if (targetUrl) {
    args.push('-f', `target_url=${targetUrl}`);
  }
  await run('gh', args, process.cwd());
}

async function getGithubStepUrl(step: string): Promise<string | null> {
  const repository = process.env.GITHUB_REPOSITORY;
  const runId = process.env.GITHUB_RUN_ID;
  const job = process.env.GITHUB_JOB;
  if (!repository || !runId || !job) {
    return null;
  }
  const result =
    await $`gh api -H ${'Accept: application/vnd.github+json'} ${`/repos/${repository}/actions/runs/${runId}/jobs`} --jq ${`.jobs[] | select(.name == "${job}") | .id`}`
      .quiet()
      .nothrow();
  const jobId = decode(result.stdout).trim();
  if (!jobId) {
    return `https://github.com/${repository}/actions/runs/${runId}`;
  }
  return step
    ? `https://github.com/${repository}/actions/runs/${runId}/job/${jobId}#step:${step}:1`
    : `https://github.com/${repository}/actions/runs/${runId}/job/${jobId}`;
}
