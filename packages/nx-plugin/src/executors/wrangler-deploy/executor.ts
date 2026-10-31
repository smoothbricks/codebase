import { spawn } from 'node:child_process';
import { join } from 'node:path';
import { deployArtifactHash } from './deploy-hash.js';
import { workspaceBinPath } from './hasher.js';
import type { WranglerDeployOptions } from './schema.js';

/** What the executor reads of Nx's context: the workspace and the project's root. */
interface WranglerDeployContext {
  readonly root: string;
  readonly projectName?: string | undefined;
  readonly projectsConfigurations: { readonly projects: Readonly<Record<string, { readonly root: string }>> };
}

/**
 * `smoo wrangler deploy-stage --stage <stage>` in the project, as the workspace's own `smoo`, under the artifact hash
 * of the hash the task ran under: `deploy-stage` tags the version it deploys with that hash, and the hasher compares
 * the stage's live tag with it.
 */
export function runWranglerDeploy(
  options: WranglerDeployOptions,
  context: WranglerDeployContext,
  env: NodeJS.ProcessEnv,
): Promise<{ success: boolean }> {
  const project =
    context.projectName === undefined ? undefined : context.projectsConfigurations.projects[context.projectName];
  if (project === undefined) {
    throw new Error('@smoothbricks/nx-plugin:wrangler-deploy requires a project context.');
  }
  const hash = env.NX_TASK_HASH;
  const deployEnv: NodeJS.ProcessEnv = {
    ...env,
    PATH: workspaceBinPath(context.root, env),
    ...(hash === undefined ? {} : { NX_TASK_HASH: deployArtifactHash(hash) }),
  };
  const command = `smoo wrangler deploy-stage --stage ${options.stage}`;
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a deploy lasts as long as wrangler's upload and the live-version wait deploy-stage bounds itself, as it did under nx:run-commands
  return new Promise((resolve) => {
    const child = spawn('smoo', ['wrangler', 'deploy-stage', '--stage', options.stage], {
      cwd: join(context.root, project.root),
      env: deployEnv,
      stdio: 'inherit',
    });
    child.on('error', (error) => {
      console.error(`wrangler-deploy: \`${command}\` could not start: ${error.message}`);
      resolve({ success: false });
    });
    child.on('close', (code, signal) => {
      if (code !== 0) {
        console.error(
          `wrangler-deploy: \`${command}\` ${signal === null ? `exited ${code}` : `was ended by ${signal}`}`,
        );
      }
      resolve({ success: code === 0 });
    });
  });
}

export default function wranglerDeployExecutor(
  options: WranglerDeployOptions,
  context: WranglerDeployContext,
): Promise<{ success: boolean }> {
  return runWranglerDeploy(options, context, process.env);
}
