import { execFile } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { delimiter, join } from 'node:path';
import { promisify } from 'node:util';
import type { HasherContext } from 'nx/src/config/misc-interfaces';
import type { Task } from 'nx/src/config/task-graph';
import type { Hash } from 'nx/src/hasher/task-hasher';
import { workspaceRoot } from 'nx/src/utils/workspace-root';
import { deployVersionTag } from '../../deploy-policy.js';
import { isRecord } from '../../type-guards.js';
import { unconfirmedDeployHash } from './deploy-hash.js';

const execFileAsync = promisify(execFile);

/**
 * `smoo wrangler deployed-version` asks Cloudflare twice through wrangler, in seconds. The bound is for one that never
 * answers: the live version is then unknown, the hash misses, and the deploy reads Cloudflare itself.
 */
const LIVE_TAG_DEADLINE_MS = 120_000;

/** What serves a stage: the tag of the version taking all its traffic (null for a version deployed untagged), or why that is not known. */
export type LiveTag =
  | { readonly kind: 'tag'; readonly tag: string | null }
  | { readonly kind: 'unknown'; readonly reason: string };

/** The hash detail that says what the stage served when it was not the artifact's version. */
export const LIVE_VERSION_NODE = 'wrangler:live-version';

/**
 * A deploy's hash: `artifact`, Nx's hash of what the deploy uploads and the stage it names, while the stage serves the
 * version tagged with it, so a cache hit replays a deploy that would change nothing. Any other live version, or none
 * known, makes it `unconfirmedDeployHash`, which no cache holds, so the run deploys; a rollback is never answered
 * from the cache. Hashing the live tag in instead would never converge: the deployed version is tagged with the hash.
 */
export function deployHash(artifact: Hash, live: LiveTag, nonce: string): Hash {
  const expected = deployVersionTag(artifact.value);
  if (live.kind === 'tag' && expected !== null && live.tag === expected) return artifact;
  const served =
    expected === null
      ? `the artifact hash ${artifact.value} names no version tag`
      : live.kind === 'unknown'
        ? live.reason
        : `${live.tag ?? 'an untagged version'} is live, not ${expected}`;
  return {
    ...artifact,
    value: unconfirmedDeployHash(artifact.value, nonce),
    details: { ...artifact.details, nodes: { ...artifact.details.nodes, [LIVE_VERSION_NODE]: served } },
  };
}

/** `env`'s PATH with the workspace's own bins first, as nx:run-commands runs a command. */
export function workspaceBinPath(root: string, env: NodeJS.ProcessEnv): string {
  const bins = join(root, 'node_modules', '.bin');
  return env.PATH === undefined || env.PATH === '' ? bins : `${bins}${delimiter}${env.PATH}`;
}

/** The tag serving `stage` for the project at `projectRoot`, read from Cloudflare now, never from a cached answer. */
export async function probeLiveTag(
  root: string,
  projectRoot: string,
  stage: string,
  env: NodeJS.ProcessEnv = process.env,
): Promise<LiveTag> {
  const args = ['wrangler', 'deployed-version', '--stage', stage, '--refresh'];
  const command = `smoo ${args.join(' ')}`;
  try {
    const { stdout } = await execFileAsync('smoo', args, {
      cwd: join(root, projectRoot),
      env: { ...env, PATH: workspaceBinPath(root, env) },
      encoding: 'utf8',
      timeout: LIVE_TAG_DEADLINE_MS,
      killSignal: 'SIGKILL',
      maxBuffer: 64 * 1024,
    });
    const tag = stdout.trim();
    if (tag === '' || /\s/.test(tag)) {
      return { kind: 'unknown', reason: `\`${command}\` printed no single tag: ${JSON.stringify(stdout)}` };
    }
    // `deployed-version` says `untagged` for a version deployed outside Nx.
    return { kind: 'tag', tag: tag === 'untagged' ? null : tag };
  } catch (error) {
    if (error instanceof Error && 'killed' in error && error.killed === true) {
      return { kind: 'unknown', reason: `\`${command}\` gave no answer within ${LIVE_TAG_DEADLINE_MS} ms` };
    }
    const said =
      error instanceof Error && 'stderr' in error && typeof error.stderr === 'string' ? error.stderr.trim() : '';
    const reason = said !== '' ? said : error instanceof Error ? error.message : String(error);
    return { kind: 'unknown', reason: `\`${command}\` failed: ${reason}` };
  }
}

/** The stage the task deploys: its `--stage` override, else its target's `stage` option. */
function deployStage(task: Task, options: unknown): string | null {
  const overrides: unknown = task.overrides;
  if (isRecord(overrides) && typeof overrides.stage === 'string') return overrides.stage;
  return isRecord(options) && typeof options.stage === 'string' ? options.stage : null;
}

/** Nx's custom hasher of `@smoothbricks/nx-plugin:wrangler-deploy`: `deployHash` of the task's own hash and its stage's live tag. */
export default async function wranglerDeployHasher(task: Task, context: HasherContext): Promise<Hash> {
  const artifact = await context.hasher.hashTask(task, context.taskGraph);
  const project = context.projectsConfigurations.projects[task.target.project];
  const stage = deployStage(task, project?.targets?.[task.target.target]?.options);
  const live: LiveTag =
    project === undefined
      ? { kind: 'unknown', reason: `Nx names no project ${task.target.project}` }
      : stage === null
        ? { kind: 'unknown', reason: 'the task names no --stage' }
        : await probeLiveTag(workspaceRoot, project.root, stage);
  return deployHash(artifact, live, randomBytes(8).toString('hex'));
}
