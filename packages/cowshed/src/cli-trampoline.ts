/// <reference types="node" />

import { spawn } from 'node:child_process';
import { chmodSync, existsSync } from 'node:fs';
import { homedir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

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
  | { kind: 'napi' };

export interface RunCliOptions extends CliResolutionOptions {
  spawnBinary?: (executable: string, argv: readonly string[]) => Promise<number>;
  runNapi?: (argv: readonly string[]) => Promise<number>;
}

export function packageRootFromModule(moduleUrl: string): string {
  const moduleDirectory = dirname(fileURLToPath(moduleUrl));
  if (basename(moduleDirectory) === 'ts' && basename(dirname(moduleDirectory)) === 'dist') {
    return resolve(moduleDirectory, '..', '..');
  }
  return resolve(moduleDirectory, '..');
}

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
  const platformDirectory = binaryPlatformDirectory(platform, arch);

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
    if (fileExists(stableBinary)) {
      return { kind: 'native', path: stableBinary, source: 'stable' };
    }
  }

  if (platformDirectory !== null) {
    const packagedBinary = join(options.packageRoot, 'dist', 'bin', platformDirectory, 'cowshed');
    if (fileExists(packagedBinary)) {
      return { kind: 'native', path: packagedBinary, source: 'package' };
    }
  }

  const workspaceBinary = join(options.packageRoot, 'target', 'release', 'cowshed');
  if (fileExists(workspaceBinary)) {
    return { kind: 'native', path: workspaceBinary, source: 'workspace' };
  }

  return { kind: 'napi' };
}

export async function runCli(argv: readonly string[], options: RunCliOptions): Promise<number> {
  const backend = resolveCliBackend({ ...options, argv });
  if (backend.kind === 'native') {
    return (options.spawnBinary ?? spawnBinary)(backend.path, argv);
  }
  return (options.runNapi ?? runNapiFallback)(argv);
}

function binaryPlatformDirectory(platform: NodeJS.Platform, arch: string): string | null {
  if (platform === 'darwin' && (arch === 'arm64' || arch === 'x64')) {
    return `darwin-${arch}`;
  }
  if (platform === 'linux' && (arch === 'arm64' || arch === 'x64')) {
    return `linux-${arch}-gnu`;
  }
  return null;
}

async function runNapiFallback(argv: readonly string[]): Promise<number> {
  // Loading the known module lazily is load-bearing: native binaries must bypass Node-API and typia startup entirely.
  const { loadNativeModule } = await import('./native.js');
  return loadNativeModule().runCli(argv);
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
