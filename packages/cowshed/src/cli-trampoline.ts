/// <reference types="node" />

import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { basename, dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const FORWARDED_SIGNALS = ['SIGINT', 'SIGTERM', 'SIGHUP', 'SIGQUIT'] as const;

export interface CliResolutionOptions {
  packageRoot: string;
  platform?: NodeJS.Platform;
  arch?: string;
  exists?: (path: string) => boolean;
}

export type CliBackend = { kind: 'native'; path: string; source: 'package' | 'workspace' } | { kind: 'napi' };

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

export function resolveCliBackend(options: CliResolutionOptions): CliBackend {
  const platform = options.platform ?? process.platform;
  const arch = options.arch ?? process.arch;
  const fileExists = options.exists ?? existsSync;
  const platformDirectory = binaryPlatformDirectory(platform, arch);

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
  const backend = resolveCliBackend(options);
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
    reject(error);
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
  return promise;
}
