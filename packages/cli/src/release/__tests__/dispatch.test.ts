import { describe, expect, it } from 'bun:test';
import {
  dispatchRelease,
  type PublishRun,
  type PublishWorkflowInputs,
  publishRunFromDispatchOutput,
  type ReleaseDispatchShell,
} from '../dispatch.js';

describe('a held publish dispatch', () => {
  it('reads the run gh workflow run created from its output', () => {
    expect(
      publishRunFromDispatchOutput(
        '✓ Created workflow_dispatch event for publish.yml at main\nhttps://github.com/smoothbricks/codebase/actions/runs/42752601857\n\nTo see the created workflow run, try: gh run view 42752601857\n',
      ),
    ).toEqual({ id: '42752601857', url: 'https://github.com/smoothbricks/codebase/actions/runs/42752601857' });
    expect(publishRunFromDispatchOutput('✓ Created workflow_dispatch event for publish.yml at main\n')).toBeNull();
  });

  it('fails with the run when the run did not conclude success', async () => {
    const shell = new RecordingDispatchShell('failure');

    await expect(dispatchRelease(shell, options())).rejects.toThrow(
      'publish.yml run https://github.com/smoothbricks/codebase/actions/runs/7 concluded failure.',
    );
    expect(shell.dispatched).toEqual([{ workflow: 'publish.yml', branch: 'main', inputs: options().inputs }]);
  });

  it('refuses a remote without the release branch before dispatching', async () => {
    const shell = new RecordingDispatchShell('success', null);

    await expect(dispatchRelease(shell, options())).rejects.toThrow(
      'Cannot dispatch publish.yml: origin has no branch main.',
    );
    expect(shell.dispatched).toEqual([]);
  });
});

function options() {
  return {
    workflow: 'publish.yml',
    remote: 'origin',
    branch: 'main',
    inputs: { bump: 'patch', projects: 'cli', dryRun: true } satisfies PublishWorkflowInputs,
  };
}

class RecordingDispatchShell implements ReleaseDispatchShell {
  readonly dispatched: Array<{ workflow: string; branch: string; inputs: PublishWorkflowInputs }> = [];
  readonly logs: string[] = [];

  constructor(
    private readonly conclusion: string,
    private readonly remoteHead: string | null = 'head-sha',
  ) {}

  async head(): Promise<string> {
    return 'head-sha';
  }

  async fetchRemoteHead(): Promise<string | null> {
    return this.remoteHead;
  }

  async isAncestor(ancestor: string, descendant: string): Promise<boolean> {
    return ancestor === descendant;
  }

  async dispatchPublishWorkflow(workflow: string, branch: string, inputs: PublishWorkflowInputs): Promise<PublishRun> {
    this.dispatched.push({ workflow, branch, inputs });
    return { id: '7', url: 'https://github.com/smoothbricks/codebase/actions/runs/7' };
  }

  async awaitPublishRun(): Promise<string> {
    return this.conclusion;
  }

  log(message: string): void {
    this.logs.push(message);
  }
}
