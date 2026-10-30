import { readFile, realpath } from 'node:fs/promises';
import { dirname, join, posix, relative } from 'node:path';

import type { TargetConfiguration } from 'nx/src/devkit-exports.js';

/** The workspace root's link to this plugin, which every workspace that uses it has. */
const PLUGIN_LINK = 'node_modules/@smoothbricks/nx-plugin';

/**
 * The command that runs a tool package's executable `file` (a path inside the package) from the directory `cwd`
 * (relative to the workspace root) an inferred target runs in.
 *
 * A tool package is a dependency of this plugin, never of the workspace root, so the root `node_modules` has no link to
 * it. Its executable is named through the plugin's root link, as the plugin's own `nextest.toml` is and for the same
 * reason (never the plugin's real location, whose depth from each workspace differs), and then one step up to the
 * package beside the plugin: every install layout puts a package's dependencies next to it, the workspace's `packages/`
 * directory for a linked checkout, a hoisted `node_modules/@smoothbricks/`, a bun or pnpm store's per-package
 * `node_modules`. The kernel resolves that `..` from where the link really points, so the executable runs from the
 * path as written, which is why it is concatenated rather than joined: a lexical join would drop the link before the
 * `..`.
 */
function toolCommand(cwd: string, tool: string, file: string): string {
  return `${posix.join(posix.relative(cwd, '.'), PLUGIN_LINK)}/../${tool}/${file}`;
}

/** The tool packages whose native artifacts inferred commands consume. */
const NATIVE_TOOL_PACKAGES = ['build-inputs', 'cargo-retain', 'nextest-extract', 'cowshed'] as const;

export type NativeToolPackage = (typeof NATIVE_TOOL_PACKAGES)[number];

type TargetDependency = NonNullable<TargetConfiguration['dependsOn']>[number];

/**
 * Native tools as the inferring host consumes them. Installed packages already ship their artifacts; a workspace
 * package needs its declared producer. Scriptc tools use the host platform target, cowshed its napi-native addon.
 */
export interface NativeTools {
  /** The host's directory under a tool package's `dist/bin`. */
  readonly platform: string;
  /** The host's scriptc target, the one that writes `dist/bin/<platform>/`. */
  readonly target: string;
  /** Each tool package that is a project of this workspace, to its project name. */
  readonly projects: ReadonlyMap<NativeToolPackage, string>;
}

/**
 * The tools of the workspace at `workspaceRoot`, whose project manifests are `projectManifests` (workspace-relative):
 * a tool package is a project when the plugin's link resolves beside one of them.
 */
export async function resolveNativeTools(
  workspaceRoot: string,
  projectManifests: readonly string[],
  platform: string,
  target: string,
): Promise<NativeTools> {
  const projects = new Map<NativeToolPackage, string>();
  const plugin = await realpath(join(workspaceRoot, PLUGIN_LINK)).catch((error: unknown) => {
    // A workspace whose install has not linked the plugin yet has no tool package of its own either.
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return null;
    throw error;
  });
  if (plugin !== null) {
    const root = await realpath(workspaceRoot);
    for (const tool of NATIVE_TOOL_PACKAGES) {
      const manifest = relative(root, join(dirname(plugin), tool, 'package.json')).replaceAll('\\', '/');
      if (!projectManifests.includes(manifest)) continue;
      const parsed: unknown = JSON.parse(await readFile(join(root, manifest), 'utf8'));
      const name = projectName(parsed);
      if (name === undefined) throw new Error(`${manifest}: a tool package names its Nx project in name or nx.name`);
      projects.set(tool, name);
    }
  }
  return { platform, target, projects };
}

function projectName(manifest: unknown): string | undefined {
  if (typeof manifest !== 'object' || manifest === null) return undefined;
  const nx = 'nx' in manifest && typeof manifest.nx === 'object' && manifest.nx !== null ? manifest.nx : undefined;
  const nxName = nx !== undefined && 'name' in nx && typeof nx.name === 'string' ? nx.name : undefined;
  const name = 'name' in manifest && typeof manifest.name === 'string' ? manifest.name : undefined;
  return nxName ?? name;
}

/**
 * The command running `tool`'s executable `bin` from `cwd`. `tools` is null on a host no scriptc platform covers,
 * where no tool package carries an executable to run.
 */
export function nativeToolCommand(
  tools: Pick<NativeTools, 'platform'> | null,
  cwd: string,
  tool: NativeToolPackage,
  bin: string,
): string {
  if (tools === null) {
    throw new Error(
      `@smoothbricks/${tool} ships ${bin} for darwin-arm64, darwin-x64, linux-arm64 and linux-x64 only, and Nx is inferring this workspace for another platform`,
    );
  }
  return toolCommand(cwd, tool, `dist/bin/${tools.platform}/${bin}`);
}

/** The task producing a tool artifact, only when this workspace owns its package. */
export function nativeToolDependsOn(
  tools: NativeTools | null,
  tool: NativeToolPackage,
  target?: string,
): TargetDependency[] {
  const project = tools?.projects.get(tool);
  return project === undefined || tools === null
    ? []
    : [{ projects: [project], target: target ?? (tool === 'cowshed' ? 'napi-native' : tools.target) }];
}
