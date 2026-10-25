import { posix } from 'node:path';

/** The workspace root's link to this plugin, which every workspace that uses it has. */
const PLUGIN_LINK = 'node_modules/@smoothbricks/nx-plugin';

/**
 * The command that runs a tool package's CLI `file` (a path inside the package) from the directory `cwd` (relative to
 * the workspace root) an inferred target runs in.
 *
 * A tool package is a dependency of this plugin, never of the workspace root, so the root `node_modules` has no link to
 * it. Its CLI is named through the plugin's root link, as the plugin's own `nextest.toml` is and for the same reason (never the
 * plugin's real location, whose depth from each workspace differs), and then one step up to the package beside the
 * plugin: every install layout puts a package's dependencies next to it, the workspace's `packages/` directory for a
 * linked checkout, a hoisted `node_modules/@smoothbricks/`, a bun or pnpm store's per-package `node_modules`. The kernel
 * resolves that `..` from where the link really points, so a native executable runs from the path as written. Node
 * resolves its main module lexically, which would drop the link before the `..`, so a JavaScript CLI is named through
 * the link's `realpath` first.
 */
export function toolCommand(cwd: string, tool: string, file: string, runtime: 'native' | 'node'): string {
  const link = posix.join(posix.relative(cwd, '.'), PLUGIN_LINK);
  return runtime === 'native' ? posix.join(link, '..', tool, file) : `node "$(realpath ${link})/../${tool}/${file}"`;
}
