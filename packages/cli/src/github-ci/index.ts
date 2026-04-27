import { $ } from 'bun';
import { decode, run, runStatus } from '../lib/run.js';

export async function cleanupGithubCiCache(root: string): Promise<void> {
  await run('nix-collect-garbage', ['--quiet'], root);
  const nar = process.env.NIX_STORE_NAR;
  if (!nar) {
    return;
  }
  await runStatus('/nix/var/nix/profiles/default/bin/nix-store', ['--verify', '--check-contents', '--repair'], root);
  const rootsResult = await $`sudo find /nix/var/nix/gcroots -type l -exec readlink {} ;`.cwd(root).quiet().nothrow();
  const roots = decode(rootsResult.stdout)
    .split('\n')
    .map((entry) => entry.trim())
    .filter(Boolean)
    .sort()
    .join(' ');
  if (!roots) {
    return;
  }
  await $`bash -lc ${`sudo /nix/var/nix/profiles/default/bin/nix-store --export --quiet $(sudo /nix/var/nix/profiles/default/bin/nix-store -qR ${roots} 2>/dev/null) > "${nar}" || true`}`.cwd(
    root,
  );
}

export async function githubCiNxSmart(
  root: string,
  options: { target: string; name?: string; step?: string },
): Promise<void> {
  const name = options.name ?? options.target;
  const step = options.step ?? '';
  await createGithubStatus(name, step);
  const mode =
    process.env.GITHUB_EVENT_NAME === 'push' && process.env.GITHUB_REF_NAME === 'main' ? 'run-many' : 'affected';
  const nxArgs =
    mode === 'run-many'
      ? ['run-many', '-t', options.target, '--parallel=5']
      : ['affected', '-t', options.target, '--parallel=5'];
  const status = await runStatus('nx', nxArgs, root);
  await updateGithubStatus(name, status === 0 ? 'success' : 'failure', step);
  if (status !== 0) {
    throw new Error(`nx ${nxArgs.join(' ')} failed with exit code ${status}`);
  }
}

export async function githubCiNxRunMany(root: string, options: { targets: string; projects?: string }): Promise<void> {
  const nxArgs = ['run-many', '-t', options.targets, '--parallel=5'];
  if (options.projects) {
    nxArgs.push(`--projects=${options.projects}`);
  }
  await run('nx', nxArgs, root);
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
