import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { ChangelogOptions } from 'nx/src/command-line/release/command-object.js';
import type { NxReleaseConfiguration } from 'nx/src/config/nx-json.js';
import { type ReleasePackageInfo, releaseTag } from './core.js';

const nxRenderOnlyReleaseConfig = {
  changelog: {
    workspaceChangelog: false,
    projectChangelogs: {
      createRelease: false,
      file: false,
    },
  },
} satisfies NxReleaseConfiguration;

interface ProjectChangelogLookupResult {
  projectChangelogs?: Record<string, { contents: string }>;
}

export interface RenderNxProjectChangelogInput<Package extends ReleasePackageInfo = ReleasePackageInfo> {
  root: string;
  pkg: Package;
  previousTag: string | null;
  dryRun: boolean;
}

export interface GithubReleaseWriteShell {
  githubReleaseExists(tag: string): Promise<boolean>;
  runGhRelease(args: string[]): Promise<void>;
  log(message: string): void;
}

export async function renderNxProjectChangelogContents(input: RenderNxProjectChangelogInput): Promise<string> {
  return withNxWorkspaceRoot(input.root, async () => {
    const { createAPI } = await import('nx/src/command-line/release/changelog.js');
    const result = await createAPI(
      nxRenderOnlyReleaseConfig,
      false,
    )(nxProjectChangelogArgs(input.pkg, input.previousTag, input.dryRun));
    return projectChangelogContents(result, input.pkg.name);
  });
}

export async function createOrUpdateGithubRelease(
  pkg: ReleasePackageInfo,
  contents: string,
  shell: GithubReleaseWriteShell,
): Promise<void> {
  const tag = releaseTag(pkg);
  const releaseExists = await shell.githubReleaseExists(tag);
  shell.log(`${pkg.name}@${pkg.version}: ${releaseExists ? 'updating' : 'creating'} GitHub Release for ${tag}.`);

  const tempDir = await mkdtemp(join(tmpdir(), 'smoo-github-release-'));
  const notesFile = join(tempDir, 'notes.md');
  try {
    await writeFile(notesFile, contents);
    await shell.runGhRelease(githubReleaseCommandArgs(tag, notesFile, releaseExists, pkg.version));
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
}

export function nxProjectChangelogArgs(pkg: ReleasePackageInfo, previousTag: string | null, dryRun: boolean) {
  const base = {
    version: pkg.version,
    projects: [pkg.name],
    gitCommit: false,
    gitTag: false,
    gitPush: false,
    stageChanges: false,
    createRelease: false,
    deleteVersionPlans: false,
    dryRun,
  } satisfies ChangelogOptions;
  return previousTag ? { ...base, from: previousTag } : { ...base, firstRelease: true };
}

export function projectChangelogContents(result: ProjectChangelogLookupResult, projectName: string): string {
  const changelog = result.projectChangelogs?.[projectName];
  if (!changelog) {
    throw new Error(`Nx did not generate a project changelog for ${projectName}.`);
  }
  return changelog.contents;
}

export function githubReleaseCommandArgs(
  tag: string,
  notesFile: string,
  releaseExists: boolean,
  version: string,
): string[] {
  const args = [
    'release',
    releaseExists ? 'edit' : 'create',
    tag,
    '--title',
    tag,
    '--notes-file',
    notesFile,
    '--verify-tag',
    `--latest=${isPrereleaseVersion(version) ? 'false' : 'true'}`,
  ];
  if (isPrereleaseVersion(version)) {
    args.push('--prerelease');
  }
  return args;
}

function isPrereleaseVersion(version: string): boolean {
  return version.includes('-');
}

async function withNxWorkspaceRoot<T>(root: string, run: () => Promise<T>): Promise<T> {
  const workspaceRootModule = await import('nx/src/utils/workspace-root.js');
  const previousWorkspaceRoot = workspaceRootModule.workspaceRoot;
  const previousEnvWorkspaceRoot = process.env.NX_WORKSPACE_ROOT_PATH;
  const previousCwd = process.cwd();
  process.env.NX_WORKSPACE_ROOT_PATH = root;
  process.chdir(root);
  workspaceRootModule.setWorkspaceRoot(root);
  try {
    return await run();
  } finally {
    workspaceRootModule.setWorkspaceRoot(previousWorkspaceRoot);
    if (previousEnvWorkspaceRoot === undefined) {
      delete process.env.NX_WORKSPACE_ROOT_PATH;
    } else {
      process.env.NX_WORKSPACE_ROOT_PATH = previousEnvWorkspaceRoot;
    }
    process.chdir(previousCwd);
  }
}
