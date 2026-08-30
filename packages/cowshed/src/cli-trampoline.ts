/// <reference types="node" />

import { spawn } from 'node:child_process';
import { chmodSync, existsSync } from 'node:fs';
import { homedir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { platformDirectory } from './platform.js';
import { CowshedError } from './types.js';

const FORWARDED_SIGNALS = ['SIGINT', 'SIGTERM', 'SIGHUP', 'SIGQUIT'] as const;

export interface CliResolutionOptions {
  packageRoot: string;
  platform?: NodeJS.Platform;
  arch?: string;
  exists?: (path: string) => boolean;
  /** Invoked argv; the first non-flag token selects service-verb routing. */
  argv?: readonly string[];
  /** Home directory root for the host-stable install; defaults to os.homedir(). */
  home?: string;
}

export type CliBackend =
  | { kind: 'native'; path: string; source: 'stable' | 'package' | 'workspace' }
  /** Every candidate path that was checked, so the failure can name what is missing. */
  | { kind: 'missing'; searched: readonly string[]; reason: string };

export interface RunCliOptions extends CliResolutionOptions {
  spawnBinary?: (executable: string, argv: readonly string[]) => Promise<number>;
}

export function packageRootFromModule(moduleUrl: string): string {
  const moduleDirectory = dirname(fileURLToPath(moduleUrl));
  if (basename(moduleDirectory) === 'ts' && basename(dirname(moduleDirectory)) === 'dist') {
    return resolve(moduleDirectory, '..', '..');
  }
  return resolve(moduleDirectory, '..');
}

// Exactly the daemon-control verbs, deliberately narrower than the host-management set (gateway,
// sccache, skill, setup): only launchd-managed daemons make the installed copy authoritative, and
// `setup` must run the invoking binary because it is what WRITES the stable install — routing it
// through a stale install would have the old binary reinstall itself.
const SERVICE_VERBS: Record<string, true> = { gateway: true, sccache: true };

function isServiceVerb(argv: readonly string[] | undefined): boolean {
  // The verb is the first non-flag token: `--json gateway status` routes like `gateway status`.
  const verb = argv?.find((token) => !token.startsWith('-'));
  return verb !== undefined && SERVICE_VERBS[verb] === true;
}

export function resolveCliBackend(options: CliResolutionOptions): CliBackend {
  const platform = options.platform ?? process.platform;
  const arch = options.arch ?? process.arch;
  const fileExists = options.exists ?? existsSync;
  const binaryDirectory = platformDirectory(platform, arch);
  const searched: string[] = [];

  if (isServiceVerb(options.argv)) {
    // launchd keeps running the installed copy, so for daemon verbs it is authoritative even when
    // stale; gateway start refreshes its bytes from whichever allowed binary invoked it. A missing
    // install falls through so the first-ever start can bootstrap from the invoking binary — the
    // Rust side refuses a workspace copy with exit 5 and the install path, never installing a
    // dangling agent.
    const stableBinary = join(
      options.home ?? homedir(),
      'Library',
      'Application Support',
      'dev.cowshed',
      'bin',
      'cowshed',
    );
    searched.push(stableBinary);
    if (fileExists(stableBinary)) {
      return { kind: 'native', path: stableBinary, source: 'stable' };
    }
  }

  if (binaryDirectory !== null) {
    const packagedBinary = join(options.packageRoot, 'dist', 'bin', binaryDirectory, 'cowshed');
    searched.push(packagedBinary);
    if (fileExists(packagedBinary)) {
      return { kind: 'native', path: packagedBinary, source: 'package' };
    }
  }

  const workspaceBinary = join(options.packageRoot, 'target', 'release', 'cowshed');
  searched.push(workspaceBinary);
  if (fileExists(workspaceBinary)) {
    return { kind: 'native', path: workspaceBinary, source: 'workspace' };
  }

  // There is no in-process fallback to reach for: the Node addon deliberately does not link the
  // CLI, so nothing else implements these verbs. Reporting the paths that were searched is the
  // difference between a diagnosable failure and "cowshed did nothing".
  return {
    kind: 'missing',
    searched,
    reason:
      binaryDirectory === null
        ? `cowshed ships no CLI binary for ${platform}-${arch}`
        : `no cowshed CLI binary found; looked in ${searched.join(', ')}`,
  };
}

export async function runCli(argv: readonly string[], options: RunCliOptions): Promise<number> {
  const backend = resolveCliBackend({ ...options, argv });
  if (backend.kind === 'missing') {
    throw new CowshedError(
      'environment-missing',
      backend.reason,
      'build this platform with `nx build cowshed`, or install a published @smoothbricks/cowshed',
    );
  }
  return (options.spawnBinary ?? spawnBinary)(backend.path, argv);
}

function spawnBinary(executable: string, argv: readonly string[]): Promise<number> {
  const { promise, resolve: resolveExit, reject } = Promise.withResolvers<number>();
  // The first EACCES is kept as the surfaced outcome when the heal cannot
  // revive the spawn: the retry's failure (often ENOEXEC on a healed but
  // non-binary file) would only restate that the permission was the cause.
  let permissionError: Error | null = null;

  const startChild = () => {
    const child = spawn(executable, [...argv], { stdio: 'inherit' });
    const signalHandlers = FORWARDED_SIGNALS.map((signal) => {
      const handler = () => {
        child.kill(signal);
      };
      process.once(signal, handler);
      return [signal, handler] as const;
    });

    const cleanup = () => {
      for (const [signal, handler] of signalHandlers) {
        process.off(signal, handler);
      }
      child.off('error', onError);
      child.off('exit', onExit);
    };
    const onError = (error: Error) => {
      cleanup();
      if (permissionError === null && (error as NodeJS.ErrnoException).code === 'EACCES') {
        // Packaged binaries can ship without the exec bit; restore it once and retry.
        permissionError = error;
        try {
          chmodSync(executable, 0o755);
        } catch {
          // Unwritable target: the retry fails with EACCES and surfaces the original error.
        }
        startChild();
        return;
      }
      reject(permissionError ?? error);
    };
    const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
      cleanup();
      if (signal !== null) {
        try {
          process.kill(process.pid, signal);
        } catch (error) {
          reject(error);
        }
        return;
      }
      resolveExit(code ?? 1);
    };

    child.once('error', onError);
    child.once('exit', onExit);
  };

  startChild();
  return promise;
}
