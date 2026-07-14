import { mkdir, mkdtemp, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { $ } from 'bun';
import type { GitReleaseTagInfo } from '../../core.js';

const GIT_TIMEOUT_MS = 10_000;

// Isolate fixture git from the runner environment and skip fsync. These are
// throwaway temp repos (deleted in withFixtureRepo's finally) so durability is
// irrelevant — fsync is pure cost. On GitHub Actions the ~20 sequential git
// spawns in a push test otherwise sum past the 30s cap: fsync on the runner's
// disk is the dominant cost, and inheriting the runner's global/system config
// (credential helpers, url.*.insteadOf rewrites, fsmonitor) slows every spawn
// and risks a credential-prompt hang. Unknown core.* keys are ignored by older
// git, so this is a harmless speedup locally and the real fix on CI.
const GIT_FIXTURE_ENV: Record<string, string> = {
  GIT_CONFIG_GLOBAL: '/dev/null',
  GIT_CONFIG_SYSTEM: '/dev/null',
  GIT_TERMINAL_PROMPT: '0',
  GIT_OPTIONAL_LOCKS: '0',
  GIT_CONFIG_COUNT: '1',
  GIT_CONFIG_KEY_0: 'core.fsync',
  GIT_CONFIG_VALUE_0: 'none',
};

export async function withFixtureRepo(fn: (root: string) => Promise<void>): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-release-test-'));
  try {
    await git(root, ['init', '-b', 'main']);
    await git(root, ['config', 'user.name', 'Test User']);
    await git(root, ['config', 'user.email', 'test@example.com']);
    await fn(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

export async function writeWorkspace(root: string): Promise<void> {
  await symlink(join(import.meta.dir, '../../../../../../node_modules'), join(root, 'node_modules'), 'dir');
  await writeFile(
    join(root, 'package.json'),
    `${JSON.stringify({ name: 'fixture', private: true, workspaces: ['packages/*'] }, null, 2)}\n`,
  );
  await writeFile(
    join(root, 'nx.json'),
    `${JSON.stringify({ targetDefaults: { build: { cache: true, outputs: ['{projectRoot}/dist'] } } }, null, 2)}\n`,
  );
}

export async function writePackage(root: string, name: string, packagePath: string, version: string): Promise<void> {
  await mkdir(join(root, packagePath), { recursive: true });
  await writeFile(join(root, packagePath, 'package.json'), `${JSON.stringify({ name, version }, null, 2)}\n`);
}

export async function writeBuildablePackage(
  root: string,
  name: string,
  packagePath: string,
  version = '1.0.0',
): Promise<void> {
  await mkdir(join(root, packagePath), { recursive: true });
  await writeFile(
    join(root, packagePath, 'package.json'),
    `${JSON.stringify(
      {
        name,
        version,
        nx: {
          name: name.startsWith('@') ? name.slice(name.indexOf('/') + 1) : name,
          targets: {
            build: {
              executor: 'nx:run-commands',
              options: { command: "mkdir -p dist && echo '{}' > dist/index.js", cwd: packagePath },
            },
          },
        },
      },
      null,
      2,
    )}\n`,
  );
}

export async function git(
  root: string,
  args: string[],
  env?: Record<string, string>,
  timeoutMs = GIT_TIMEOUT_MS,
): Promise<void> {
  const result = await gitResult(root, args, env, timeoutMs);
  if (result.timedOut || result.exitCode !== 0) {
    throw new Error(gitErrorMessage(root, args, result));
  }
}

export async function gitOutput(root: string, args: string[]): Promise<string> {
  const result = await gitResult(root, args);
  if (result.exitCode !== 0) {
    throw new Error(gitErrorMessage(root, args, result));
  }
  return new TextDecoder().decode(result.stdout).trim();
}

export async function gitSucceeds(root: string, args: string[]): Promise<boolean> {
  return (await gitResult(root, args)).exitCode === 0;
}

async function gitResult(
  root: string,
  args: string[],
  env?: Record<string, string>,
  timeoutMs = GIT_TIMEOUT_MS,
): Promise<GitResult> {
  const proc = Bun.spawn(['git', ...args], {
    cwd: root,
    detached: true,
    env: { ...process.env, ...GIT_FIXTURE_ENV, ...env },
    stdout: 'pipe',
    stderr: 'pipe',
  });
  let timedOut = false;
  const timeout = setTimeout(() => {
    timedOut = true;
    try {
      // `git` may exit after spawning a transport or hook which still owns its
      // pipes. Killing the detached process group closes every inherited pipe.
      process.kill(-proc.pid, 'SIGKILL');
    } catch (error) {
      if (!(error instanceof Error && 'code' in error && error.code === 'ESRCH')) {
        proc.kill('SIGKILL');
      }
    }
  }, timeoutMs);
  try {
    const [exitCode, stdout, stderr] = await Promise.all([
      proc.exited,
      streamBytes(proc.stdout),
      streamBytes(proc.stderr),
    ]);
    return { exitCode, stdout, stderr, timedOut, timeoutMs };
  } finally {
    clearTimeout(timeout);
  }
}

async function streamBytes(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

export async function runFixtureNx(root: string, args: string[]): Promise<void> {
  await $`nx ${args}`
    .cwd(root)
    .env({ ...definedProcessEnv(), NX_DAEMON: 'false' })
    .quiet();
}

function definedProcessEnv(): Record<string, string> {
  const env: Record<string, string> = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (value !== undefined) {
      env[key] = value;
    }
  }
  return env;
}

interface GitResult {
  exitCode: number;
  stdout: Uint8Array;
  stderr: Uint8Array;
  timedOut: boolean;
  timeoutMs: number;
}

function gitErrorMessage(root: string, args: string[], result: GitResult): string {
  const stderr = new TextDecoder().decode(result.stderr).trim();
  const timeoutText = result.timedOut
    ? ` timed out after ${result.timeoutMs}ms`
    : ` failed with exit code ${result.exitCode}`;
  return [`git ${args.join(' ')}${timeoutText}`, `cwd: ${root}`, stderr].filter(Boolean).join('\n');
}

export async function tag(root: string, tagName: string, date: string): Promise<void> {
  await git(root, ['tag', '-a', tagName, '-m', tagName], { GIT_COMMITTER_DATE: date });
}

export async function gitReleaseTagsByCreatorDate(root: string): Promise<GitReleaseTagInfo[]> {
  const result = await gitResult(root, [
    'for-each-ref',
    '--sort=-creatordate',
    '--format=%(refname:short)%09%(creatordate:unix)%09%(*objectname)%09%(objectname)',
    'refs/tags',
  ]);
  if (result.exitCode !== 0) {
    throw new Error(gitErrorMessage(root, ['for-each-ref', 'refs/tags'], result));
  }
  return new TextDecoder()
    .decode(result.stdout)
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const [name, timestampText, peeledSha, objectSha] = line.split('\t');
      const timestamp = Number(timestampText);
      const sha = peeledSha || objectSha;
      if (!name || !sha || !Number.isSafeInteger(timestamp)) {
        throw new Error(`Unable to parse release tag ref: ${line}`);
      }
      return { name, sha, timestamp };
    });
}

export async function gitIsAncestor(root: string, ancestor: string, descendant: string): Promise<boolean> {
  return gitSucceeds(root, ['merge-base', '--is-ancestor', ancestor, descendant]);
}

export async function packageVersionAtRef(root: string, packagePath: string, ref: string): Promise<string | null> {
  const result = await gitResult(root, ['show', `${ref}:${packagePath}/package.json`]);
  if (result.exitCode !== 0) {
    return null;
  }
  const parsed = JSON.parse(new TextDecoder().decode(result.stdout));
  return typeof parsed.version === 'string' ? parsed.version : null;
}
