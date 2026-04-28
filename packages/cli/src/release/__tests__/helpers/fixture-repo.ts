import { mkdir, mkdtemp, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { $ } from 'bun';
import type { GitReleaseTagInfo } from '../../core.js';

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

export async function git(root: string, args: string[], env?: Record<string, string>): Promise<void> {
  await $`git ${args}`
    .cwd(root)
    .env(env ?? {})
    .quiet();
}

export async function gitOutput(root: string, args: string[]): Promise<string> {
  const result = await $`git ${args}`.cwd(root).quiet();
  return new TextDecoder().decode(result.stdout).trim();
}

export async function tag(root: string, tagName: string, date: string): Promise<void> {
  await git(root, ['tag', '-a', tagName, '-m', tagName], { GIT_COMMITTER_DATE: date });
}

export async function gitReleaseTagsByCreatorDate(root: string): Promise<GitReleaseTagInfo[]> {
  const result =
    await $`git for-each-ref --sort=-creatordate --format=${'%(refname:short)%09%(creatordate:unix)%09%(*objectname)%09%(objectname)'} refs/tags`
      .cwd(root)
      .quiet();
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
  const result = await $`git merge-base --is-ancestor ${ancestor} ${descendant}`.cwd(root).quiet().nothrow();
  return result.exitCode === 0;
}

export async function packageVersionAtRef(root: string, packagePath: string, ref: string): Promise<string | null> {
  const result = await $`git show ${`${ref}:${packagePath}/package.json`}`.cwd(root).quiet().nothrow();
  if (result.exitCode !== 0) {
    return null;
  }
  const parsed = JSON.parse(new TextDecoder().decode(result.stdout));
  return typeof parsed.version === 'string' ? parsed.version : null;
}
