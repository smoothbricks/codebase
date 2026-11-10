import type { ReleaseBump } from './core.js';

/** The publish workflow's dispatch inputs, as `publish.yml` declares them under `on.workflow_dispatch.inputs`. */
export interface PublishWorkflowInputs {
  readonly bump: ReleaseBump;
  /** Comma-separated Nx projects, `all`, or blank for the package-local changed set. */
  readonly projects: string;
  readonly dryRun: boolean;
}

/** The run a dispatch created, as the forge names it. */
export interface PublishRun {
  readonly id: string;
  readonly url: string;
}

export interface ReleaseDispatchShell {
  /** The commit `HEAD` names in this checkout. */
  head(): Promise<string>;
  /** The release branch's head on its remote, fetched into this checkout; null when the remote has no such branch. */
  fetchRemoteHead(branch: string): Promise<string | null>;
  isAncestor(ancestor: string, descendant: string): Promise<boolean>;
  /** Start `workflow` on `branch` and answer the run the forge created for it. */
  dispatchPublishWorkflow(workflow: string, branch: string, inputs: PublishWorkflowInputs): Promise<PublishRun>;
  /** Wait for `run` to complete and answer its conclusion: `success` for a run that passed. */
  awaitPublishRun(run: PublishRun): Promise<string>;
  log(message: string): void;
}

export interface ReleaseDispatchOptions {
  readonly workflow: string;
  readonly remote: string;
  readonly branch: string;
  readonly inputs: PublishWorkflowInputs;
}

/**
 * Start the publish workflow and hold until its run completes (specs/cowshed/05_gateway.md "A publish holds the land
 * turn"). The run versions, tags and pushes a release commit onto the branch it was dispatched on, and that push
 * refuses a branch that moved while the run built (`classifyReleaseBranchPush`). Run as the command of the
 * repository's land turn, this process keeps the turn until the run has ended, so no land moves the branch under it.
 * One push stays outside every turn: a land pushes main after its turn ends. So the dispatch first refuses a checkout
 * whose HEAD the remote's branch does not hold, since that commit's push would land while the run builds.
 */
export async function dispatchRelease(shell: ReleaseDispatchShell, options: ReleaseDispatchOptions): Promise<void> {
  const { workflow, remote, branch } = options;
  const head = await shell.head();
  const remoteHead = await shell.fetchRemoteHead(branch);
  if (remoteHead === null) {
    throw new Error(`Cannot dispatch ${workflow}: ${remote} has no branch ${branch}.`);
  }
  if (!(await shell.isAncestor(head, remoteHead))) {
    throw new Error(
      `Cannot dispatch ${workflow}: HEAD is at ${head.slice(0, 12)}, which ${remote}/${branch} at ${remoteHead.slice(0, 12)} does not hold. ` +
        `Its push would move ${branch} while the release builds, and the release's own push would then refuse the diverged branch. ` +
        `Wait for the pending push (a land pushes ${branch} after its turn ends) or push ${branch}, then dispatch again.`,
    );
  }
  const run = await shell.dispatchPublishWorkflow(workflow, branch, options.inputs);
  shell.log(
    `Dispatched ${workflow} on ${branch} at ${remoteHead.slice(0, 12)}: ${run.url}. Holding until it completes.`,
  );
  const conclusion = await shell.awaitPublishRun(run);
  if (conclusion !== 'success') {
    throw new Error(`${workflow} run ${run.url} concluded ${conclusion}.`);
  }
  shell.log(`${workflow} run ${run.url} succeeded.`);
}

/** The run `gh workflow run` names in its output, which prints the created run's URL; null when it names none. */
export function publishRunFromDispatchOutput(output: string): PublishRun | null {
  const match = /(https?:\/\/\S+\/actions\/runs\/(\d+))/.exec(output);
  const url = match?.[1];
  const id = match?.[2];
  return url && id ? { id, url } : null;
}
