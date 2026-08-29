import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, renameSync, rmSync } from 'node:fs';
import { createRequire } from 'node:module';
import { homedir } from 'node:os';
import path from 'node:path';

import type { NapiCrossToolchainOptions } from './schema.js';

interface NapiCrossToolchainContext {
  root: string;
  projectName?: string;
  projectsConfigurations?: {
    projects: Record<string, { root?: string }>;
  };
}

/** Package-name arch segment of `@napi-rs/cross-toolchain-<host>-target-<arch>`. */
const TARGET_PACKAGE_ARCH: Readonly<Record<string, string>> = {
  'aarch64-unknown-linux-gnu': 'aarch64',
  'x86_64-unknown-linux-gnu': 'x86_64',
};

const HOST_PACKAGE_ARCH: Readonly<Record<string, string>> = {
  arm64: 'arm64',
  x64: 'x64',
};

export type PrewarmOutcome = 'ready' | 'extracted' | 'unsupported-host';

export default async function napiCrossToolchainExecutor(
  options: NapiCrossToolchainOptions,
  context: NapiCrossToolchainContext,
): Promise<{ success: boolean }> {
  const projectName = context.projectName;
  if (!projectName) {
    throw new Error('@smoothbricks/nx-plugin:napi-cross-toolchain requires a project context.');
  }
  const project = context.projectsConfigurations?.projects[projectName];
  if (!project) {
    throw new Error(`Project ${projectName} was not found in the Nx project graph.`);
  }
  const projectRoot = path.resolve(context.root, project.root ?? '.');
  const triple = options.triple;

  console.log(`${triple}: ${prewarmCrossToolchain({ projectRoot, triple })}`);

  return { success: true };
}

/**
 * Extract the pinned cross toolchain into the directory
 * `napi build --use-napi-cross` probes, so its own download never runs.
 *
 * WHY this exists: `@napi-rs/cross-toolchain@1.0.3`'s `download()` uses its own
 * package directory as scratch space — `npm pack` there, unpack, then a
 * non-`force` `rmSync` of the tarball. Under `linker = "isolated"` with
 * `globalStore = true` that directory is the shared Bun store (in CI a
 * host-wide mutable bind mount), so two concurrent `--use-napi-cross` builds of
 * one triple pack the same filename into the same directory and the loser dies
 * with `ENOENT ... lstat '<tarball>'`. Nx runs `cli-x64-linux` and
 * `napi-x64-linux` in one wave, which is exactly that race.
 *
 * Extraction stages into a private sibling directory and lands with `rename`,
 * so racing callers — and repeated runs after a crash — cannot corrupt the
 * result: `rename` replaces an empty directory (napi itself creates one before
 * probing) and fails with ENOTEMPTY against a populated one, which means
 * another caller already won.
 */
export function prewarmCrossToolchain(options: {
  projectRoot: string;
  triple: string;
  platform?: string;
  hostArch?: string;
  home?: string;
}): PrewarmOutcome {
  const { projectRoot, triple } = options;
  const targetArch = TARGET_PACKAGE_ARCH[triple];
  if (!targetArch) {
    throw new Error(
      `Unsupported --use-napi-cross triple ${triple}. Supported: ${Object.keys(TARGET_PACKAGE_ARCH).join(', ')}.`,
    );
  }
  const hostArch = HOST_PACKAGE_ARCH[options.hostArch ?? process.arch];
  // napi-rs ships these toolchains for Linux x64 and Linux arm64 hosts only,
  // and refuses `--use-napi-cross` anywhere else. Extracting on a host that can
  // never use the result would only demand pins nothing needs, so leave the
  // unsupported-host diagnostic to the build itself.
  if (!hostArch || (options.platform ?? process.platform) !== 'linux') {
    return 'unsupported-host';
  }

  const requireFromProject = createRequire(path.join(projectRoot, 'package.json'));
  const targetPackage = `@napi-rs/cross-toolchain-${hostArch}-target-${targetArch}`;
  let targetModule: unknown;
  try {
    targetModule = requireFromProject(targetPackage);
  } catch (error) {
    throw new Error(
      `${targetPackage} is not installed for ${projectRoot}. Add it to optionalDependencies so ${triple} ` +
        'builds extract the pinned toolchain instead of downloading into the shared Bun store.',
      { cause: error },
    );
  }
  if (
    typeof targetModule !== 'object' ||
    targetModule === null ||
    !('toolchainPath' in targetModule) ||
    typeof targetModule.toolchainPath !== 'string'
  ) {
    throw new Error(`${targetPackage} no longer exports a toolchainPath string.`);
  }
  const tarball = targetModule.toolchainPath;

  const home = options.home ?? homedir();
  const destination = path.join(home, '.napi-rs', 'cross-toolchain', crossToolchainVersion(requireFromProject), triple);
  const marker = path.join(destination, 'package.json');
  if (existsSync(marker)) {
    return 'ready';
  }

  mkdirSync(path.dirname(destination), { recursive: true });
  const staging = mkdtempSync(`${destination}.staging-`);
  const command = `tar -xJf ${tarball} -C ${staging}`;
  try {
    execFileSync('tar', ['-xJf', tarball, '-C', staging], { stdio: 'inherit' });
    if (!existsSync(path.join(staging, 'package.json'))) {
      throw new Error(
        `${tarball} did not contain the package.json marker napi probes; the toolchain archive layout changed.`,
      );
    }
    renameSync(staging, destination);
  } catch (error) {
    rmSync(staging, { force: true, recursive: true });
    const code = error instanceof Error && 'code' in error ? error.code : undefined;
    if ((code === 'ENOTEMPTY' || code === 'EEXIST') && existsSync(marker)) {
      return 'ready';
    }
    const status = error instanceof Error && 'status' in error ? error.status : undefined;
    if (status !== undefined) {
      if (error instanceof Error) {
        console.error(error.message);
      }
      throw new Error(`${command} failed with exit code ${typeof status === 'number' ? status : 1}`, { cause: error });
    }
    throw error;
  }
  return 'extracted';
}

/**
 * `napi` derives the extraction directory from `@napi-rs/cross-toolchain`'s own
 * version, so read it from the package the CLI resolves — not from the
 * per-target package, whose version only happens to match today.
 */
function crossToolchainVersion(requireFromProject: NodeJS.Require): string {
  const cliRequire = createRequire(requireFromProject.resolve('@napi-rs/cli/package.json'));
  const indexPath = cliRequire.resolve('@napi-rs/cross-toolchain');
  const manifest: unknown = JSON.parse(readFileSync(path.join(path.dirname(indexPath), 'package.json'), 'utf-8'));
  const version =
    typeof manifest === 'object' && manifest !== null && 'version' in manifest ? manifest.version : undefined;
  if (typeof version !== 'string' || version.length === 0) {
    throw new Error(`@napi-rs/cross-toolchain resolved at ${indexPath} has no version.`);
  }
  return version;
}
