import { describe, expect, it } from 'bun:test';
import type { ReleasePackageInfo } from '../core.js';
import {
  type RetagUnpublishedOptions,
  type RetagUnpublishedShell,
  type RetagUnpublishedTagUpdate,
  retagUnpublished,
} from '../retag-unpublished.js';

const pkg: ReleasePackageInfo = { name: '@scope/pkg', projectName: 'pkg', path: 'packages/pkg', version: '1.2.3' };

describe('retag unpublished releases', () => {
  it('moves unpublished owned tags, pushes with a remote lease, and dispatches publish auto', async () => {
    const shell = new RecordingRetagShell({
      versionAtRef: '1.2.3',
      remoteTagObjects: new Map([['@scope/pkg@1.2.3', 'old-tag-object']]),
      dispatchSha: 'target-sha',
    });

    const updates = await retagUnpublished(shell, retagOptions({ push: true, dispatch: true }));

    expect(updates).toEqual([{ tag: '@scope/pkg@1.2.3', pkg, expectedRemoteObject: 'old-tag-object' }]);
    expect(shell.movedTags).toEqual([{ tag: '@scope/pkg@1.2.3', ref: 'HEAD' }]);
    expect(shell.pushed).toEqual([[{ tag: '@scope/pkg@1.2.3', expectedRemoteObject: 'old-tag-object' }]]);
    expect(shell.dispatched).toEqual([{ workflow: 'publish.yml', branch: 'main' }]);
    expect(shell.dispatchRefLookups).toEqual(['main']);
    expect(shell.remoteTagLookups).toEqual(['@scope/pkg@1.2.3']);
  });

  it('rejects tags whose package version already exists on npm', async () => {
    const shell = new RecordingRetagShell({ versionAtRef: '1.2.3', npmPublished: true });

    await expect(retagUnpublished(shell, retagOptions())).rejects.toThrow(
      'Cannot retag @scope/pkg@1.2.3: @scope/pkg@1.2.3 already exists on npm.',
    );

    expect(shell.movedTags).toEqual([]);
    expect(shell.pushed).toEqual([]);
  });

  it('rejects tags whose version does not match the target ref package manifest', async () => {
    const shell = new RecordingRetagShell({ versionAtRef: '1.2.4' });

    await expect(retagUnpublished(shell, retagOptions())).rejects.toThrow(
      'Release tag @scope/pkg@1.2.3 cannot move to HEAD: packages/pkg/package.json has version 1.2.4, expected 1.2.3.',
    );
  });

  it('rejects workflow dispatch when the target ref is not the remote branch head', async () => {
    const shell = new RecordingRetagShell({ versionAtRef: '1.2.3', dispatchSha: 'other-sha' });

    await expect(retagUnpublished(shell, retagOptions({ push: true, dispatch: true }))).rejects.toThrow(
      'Cannot dispatch publish.yml: HEAD resolves to target-sha, but main resolves to other-sha.',
    );

    expect(shell.movedTags).toEqual([]);
    expect(shell.pushed).toEqual([]);
    expect(shell.dispatched).toEqual([]);
  });

  it('dry-runs without moving, pushing, or dispatching tags', async () => {
    const shell = new RecordingRetagShell({ versionAtRef: '1.2.3', dispatchSha: 'target-sha' });

    await retagUnpublished(shell, retagOptions({ push: true, dispatch: true, dryRun: true }));

    expect(shell.movedTags).toEqual([]);
    expect(shell.pushed).toEqual([]);
    expect(shell.dispatched).toEqual([]);
    expect(shell.logs).toContain('Would move @scope/pkg@1.2.3 to HEAD (target-sha).');
    expect(shell.logs).toContain('Would push 1 retagged release tag.');
    expect(shell.logs).toContain('Would dispatch publish.yml on main with bump=auto.');
  });
});

function retagOptions(overrides: Partial<RetagUnpublishedOptions> = {}): RetagUnpublishedOptions {
  return {
    tags: ['@scope/pkg@1.2.3'],
    toRef: 'HEAD',
    push: false,
    dispatch: false,
    dryRun: false,
    branch: 'main',
    workflow: 'publish.yml',
    ...overrides,
  };
}

class RecordingRetagShell implements RetagUnpublishedShell<ReleasePackageInfo> {
  readonly movedTags: Array<{ tag: string; ref: string }> = [];
  readonly pushed: Array<Array<{ tag: string; expectedRemoteObject: string | null }>> = [];
  readonly dispatched: Array<{ workflow: string; branch: string }> = [];
  readonly dispatchRefLookups: string[] = [];
  readonly remoteTagLookups: string[] = [];
  readonly logs: string[] = [];
  private readonly versionAtRef: string | null;
  private readonly npmPublished: boolean;
  private readonly githubReleaseCreated: boolean;
  private readonly remoteTagObjects: Map<string, string>;
  private readonly dispatchSha: string | null;

  constructor(options: {
    versionAtRef: string | null;
    npmPublished?: boolean;
    githubReleaseCreated?: boolean;
    remoteTagObjects?: Map<string, string>;
    dispatchSha?: string | null;
  }) {
    this.versionAtRef = options.versionAtRef;
    this.npmPublished = options.npmPublished === true;
    this.githubReleaseCreated = options.githubReleaseCreated === true;
    this.remoteTagObjects = options.remoteTagObjects ?? new Map();
    this.dispatchSha = options.dispatchSha ?? null;
  }

  listReleasePackages(): ReleasePackageInfo[] {
    return [pkg];
  }

  async resolveRef(): Promise<string> {
    return 'target-sha';
  }

  async resolveDispatchRef(branch: string): Promise<string | null> {
    this.dispatchRefLookups.push(branch);
    return this.dispatchSha;
  }

  async packageVersionAtRef(): Promise<string | null> {
    return this.versionAtRef;
  }

  async npmVersionExists(): Promise<boolean> {
    return this.npmPublished;
  }

  async githubReleaseExists(): Promise<boolean> {
    return this.githubReleaseCreated;
  }

  async remoteTagObject(tag: string): Promise<string | null> {
    this.remoteTagLookups.push(tag);
    return this.remoteTagObjects.get(tag) ?? null;
  }

  async createOrMoveTag(tag: string, ref: string): Promise<void> {
    this.movedTags.push({ tag, ref });
  }

  async pushTags(updates: Array<RetagUnpublishedTagUpdate<ReleasePackageInfo>>): Promise<void> {
    this.pushed.push(updates.map((update) => ({ tag: update.tag, expectedRemoteObject: update.expectedRemoteObject })));
  }

  async dispatchPublishWorkflow(workflow: string, branch: string): Promise<void> {
    this.dispatched.push({ workflow, branch });
  }

  log(message: string): void {
    this.logs.push(message);
  }
}
