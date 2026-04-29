import { describe, expect, it } from 'bun:test';
import { readFile } from 'node:fs/promises';
import type { ReleasePackageInfo } from '../core.js';
import {
  createOrUpdateGithubRelease,
  type GithubReleaseWriteShell,
  nxProjectChangelogArgs,
  projectChangelogContents,
} from '../github-release.js';

const stable: ReleasePackageInfo = { name: '@scope/pkg', projectName: 'pkg', path: 'packages/pkg', version: '1.2.3' };
const prerelease: ReleasePackageInfo = {
  name: '@scope/pkg',
  projectName: 'pkg',
  path: 'packages/pkg',
  version: '2.0.0-beta.1',
};

describe('GitHub release helpers', () => {
  it('configures the Nx changelog API for render-only project changelogs', () => {
    expect(nxProjectChangelogArgs(stable, '@scope/pkg@1.2.2', false)).toEqual({
      version: '1.2.3',
      projects: ['pkg'],
      gitCommit: false,
      gitTag: false,
      gitPush: false,
      stageChanges: false,
      createRelease: false,
      deleteVersionPlans: false,
      dryRun: false,
      from: '@scope/pkg@1.2.2',
    });
  });

  it('marks the Nx changelog request as a first release when there is no previous tag', () => {
    expect(nxProjectChangelogArgs(stable, null, true)).toEqual({
      version: '1.2.3',
      projects: ['pkg'],
      gitCommit: false,
      gitTag: false,
      gitPush: false,
      stageChanges: false,
      createRelease: false,
      deleteVersionPlans: false,
      dryRun: true,
      firstRelease: true,
    });
  });

  it('extracts the generated Nx project changelog body', () => {
    expect(
      projectChangelogContents(
        {
          projectChangelogs: {
            pkg: { contents: 'generated release notes' },
          },
        },
        'pkg',
      ),
    ).toBe('generated release notes');
  });

  it('fails when Nx omits the requested project changelog', () => {
    expect(() => projectChangelogContents({ projectChangelogs: {} }, 'pkg')).toThrow(
      'Nx did not generate a project changelog for pkg.',
    );
  });

  it('creates a missing stable GitHub Release from the generated notes file', async () => {
    const shell = new RecordingGithubReleaseShell(false);

    await createOrUpdateGithubRelease(stable, 'stable notes', shell);

    expect(shell.existsQueries).toEqual(['@scope/pkg@1.2.3']);
    expect(shell.notes).toEqual(['stable notes']);
    expect(shell.logs).toEqual(['@scope/pkg@1.2.3: creating GitHub Release for @scope/pkg@1.2.3.']);
    const command = onlyCommand(shell.commands);
    expect(command.slice(0, 6)).toEqual([
      'release',
      'create',
      '@scope/pkg@1.2.3',
      '--title',
      '@scope/pkg@1.2.3',
      '--notes-file',
    ]);
    expect(command).toContain('--verify-tag');
    expect(command).toContain('--latest=true');
    expect(command).not.toContain('--prerelease');
  });

  it('edits an existing prerelease and prevents it from becoming latest', async () => {
    const shell = new RecordingGithubReleaseShell(true);

    await createOrUpdateGithubRelease(prerelease, 'prerelease notes', shell);

    expect(shell.existsQueries).toEqual(['@scope/pkg@2.0.0-beta.1']);
    expect(shell.notes).toEqual(['prerelease notes']);
    expect(shell.logs).toEqual(['@scope/pkg@2.0.0-beta.1: updating GitHub Release for @scope/pkg@2.0.0-beta.1.']);
    const command = onlyCommand(shell.commands);
    expect(command.slice(0, 6)).toEqual([
      'release',
      'edit',
      '@scope/pkg@2.0.0-beta.1',
      '--title',
      '@scope/pkg@2.0.0-beta.1',
      '--notes-file',
    ]);
    expect(command).toContain('--verify-tag');
    expect(command).toContain('--latest=false');
    expect(command).toContain('--prerelease');
  });
});

class RecordingGithubReleaseShell implements GithubReleaseWriteShell {
  readonly existsQueries: string[] = [];
  readonly commands: string[][] = [];
  readonly notes: string[] = [];
  readonly logs: string[] = [];

  constructor(private readonly releaseExists: boolean) {}

  async githubReleaseExists(tag: string): Promise<boolean> {
    this.existsQueries.push(tag);
    return this.releaseExists;
  }

  async runGhRelease(args: string[]): Promise<void> {
    this.commands.push(args);
    const notesFileIndex = args.indexOf('--notes-file') + 1;
    const notesFile = args[notesFileIndex];
    if (!notesFile) {
      throw new Error(`Missing --notes-file in gh release command: ${args.join(' ')}`);
    }
    this.notes.push(await readFile(notesFile, 'utf8'));
  }

  log(message: string): void {
    this.logs.push(message);
  }
}

function onlyCommand(commands: string[][]): string[] {
  expect(commands).toHaveLength(1);
  const command = commands[0];
  if (!command) {
    throw new Error('Expected one gh command.');
  }
  return command;
}
