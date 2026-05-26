import { existsSync } from 'node:fs';
import { appendFile, mkdtemp, realpath, rename, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { $ } from 'bun';
import { decode, run, runStatus } from '../lib/run.js';

type NxSmartMode = 'auto' | 'affected' | 'run-many';

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

export async function githubCiNxSmart(
  root: string,
  options: { target: string; name?: string; step?: string; mode?: NxSmartMode; configuration?: string },
): Promise<void> {
  const name = options.name ?? options.target;
  const step = options.step ?? '';
  await createGithubStatus(name, step);
  const mode = resolveNxSmartMode(options.mode ?? 'auto');
  const nxArgs = mode === 'run-many' ? ['run-many', '-t', options.target] : ['affected', '-t', options.target];
  if (options.configuration) {
    nxArgs.push(`--configuration=${options.configuration}`);
  }
  nxArgs.push('--parallel=5');
  const status = await runStatus('nx', nxArgs, root);
  await updateGithubStatus(name, status === 0 ? 'success' : 'failure', step);
  if (status !== 0) {
    throw new Error(`nx ${nxArgs.join(' ')} failed with exit code ${status}`);
  }
}

export async function githubCiNxRunMany(
  root: string,
  options: { targets: string; projects?: string; configuration?: string },
): Promise<void> {
  const nxArgs = ['run-many', '-t', options.targets, '--parallel=5'];
  if (options.projects) {
    nxArgs.push(`--projects=${options.projects}`);
  }
  if (options.configuration) {
    nxArgs.push(`--configuration=${options.configuration}`);
  }
  await run('nx', nxArgs, root);
}

export async function githubCiNxDeploy(
  root: string,
  options: { configuration: string; mode?: NxSmartMode; name?: string; step?: string; verify?: boolean },
): Promise<void> {
  const name = options.name ?? `Deploy ${options.configuration}`;
  const step = options.step ?? '';
  await createGithubStatus(name, step);
  const mode = resolveNxSmartMode(options.mode ?? 'run-many');
  const projects = await deployProjectsWithConfiguration(root, options.configuration, mode);
  if (projects.length === 0) {
    console.log(`No ${mode} deploy projects with configuration ${options.configuration}; skipping.`);
    await updateGithubStatus(name, 'success', step);
    return;
  }

  const projectList = projects.join(',');
  const targets = options.verify === true ? ['build', 'lint', 'test', 'deploy'] : ['deploy'];
  for (const target of targets) {
    const nxArgs = ['run-many', '-t', target, `--projects=${projectList}`, '--parallel=5'];
    if (target === 'deploy') {
      nxArgs.push(`--configuration=${options.configuration}`);
    }
    const status = await runStatus('nx', nxArgs, root);
    if (status !== 0) {
      await updateGithubStatus(name, 'failure', step);
      throw new Error(`nx ${nxArgs.join(' ')} failed with exit code ${status}`);
    }
  }
  await updateGithubStatus(name, 'success', step);
}

function resolveNxSmartMode(mode: NxSmartMode): 'affected' | 'run-many' {
  if (mode === 'affected' || mode === 'run-many') {
    return mode;
  }
  return process.env.GITHUB_EVENT_NAME === 'push' && process.env.GITHUB_REF_NAME === 'main' ? 'run-many' : 'affected';
}

async function deployProjectsWithConfiguration(
  root: string,
  configuration: string,
  mode: 'affected' | 'run-many',
): Promise<string[]> {
  const listArgs =
    mode === 'affected'
      ? ['show', 'projects', '--affected', '--withTarget', 'deploy', '--json']
      : ['show', 'projects', '--withTarget', 'deploy', '--json'];
  const result = await $`nx ${listArgs}`.cwd(root).quiet();
  const candidates = nxProjectList(decode(result.stdout));
  const projects: string[] = [];
  for (const project of candidates) {
    if (await deployTargetHasConfiguration(root, project, configuration)) {
      projects.push(project);
    }
  }
  return projects.sort((a, b) => a.localeCompare(b));
}

async function deployTargetHasConfiguration(root: string, project: string, configuration: string): Promise<boolean> {
  const result = await $`nx show project ${project} --json`.cwd(root).quiet();
  const parsed: unknown = JSON.parse(decode(result.stdout));
  const targets = recordValue(parsed)?.targets;
  const deploy = recordValue(targets)?.deploy;
  const configurations = recordValue(deploy)?.configurations;
  return recordValue(configurations)?.[configuration] !== undefined;
}

function nxProjectList(output: string): string[] {
  const parsed: unknown = JSON.parse(output);
  return Array.isArray(parsed) ? parsed.filter((project): project is string => typeof project === 'string') : [];
}

function recordValue(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
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
