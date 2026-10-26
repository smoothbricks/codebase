import { spawnSync } from 'node:child_process';
import { createRequire } from 'node:module';
import { constants } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { requiredJsonObject } from './json.js';

/**
 * Every `smoo-nx-*` bin of the SmoothBricks packages this CLI depends on, by its smoo command (`nx-failures` for
 * `smoo-nx-failures`), to the JavaScript file it runs. Those packages' own manifests are the registry: a tool package
 * added to the CLI's dependencies is dispatched with no change here.
 */
export function nxTools(): Map<string, string> {
  const cliManifest = fileURLToPath(new URL('../../package.json', import.meta.url));
  const resolve = createRequire(cliManifest).resolve;
  const tools = new Map<string, string>();
  for (const name of Object.keys(requiredJsonObject(cliManifest).dependencies ?? {})) {
    if (!name.startsWith('@smoothbricks/')) continue;
    const manifest = resolve(`${name}/package.json`);
    const bin = requiredJsonObject(manifest).bin;
    if (bin === undefined || typeof bin === 'string') continue;
    for (const [command, script] of Object.entries(bin)) {
      if (command.startsWith('smoo-nx-')) tools.set(command.slice('smoo-'.length), join(dirname(manifest), script));
    }
  }
  return tools;
}

/** `smoo <tool> <args...>`: the tool's bin under this process's runtime; the tool's exit status is smoo's. */
export function runNxTool(tool: string, args: readonly string[]): number {
  const tools = nxTools();
  const script = tools.get(tool);
  if (script === undefined) {
    process.stderr.write(`smoo: no tool ${tool}; the tools are ${[...tools.keys()].sort().join(', ')}\n`);
    return 64;
  }
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: smoo nx-<tool> is the tool, run for as long as its work (an archive extraction, a summary of any log); the operator's interrupt or the enclosing runner's bound ends a hung one
  const run = spawnSync(process.execPath, [script, ...args], { stdio: 'inherit' });
  if (run.error !== undefined) {
    process.stderr.write(`smoo: could not run ${script}: ${run.error.message}\n`);
    return 1;
  }
  return toolExitStatus(run.status, run.signal);
}

/** A child's ending as a shell reports it: its status, or 128 plus the number of the signal that ended it. */
export function toolExitStatus(status: number | null, signal: NodeJS.Signals | null): number {
  return signal === null ? (status ?? 1) : 128 + constants.signals[signal];
}
