/// <reference types="node" />

/**
 * Startup configuration, read once from the environment and refused loudly when it cannot describe a
 * server that is safe to run.
 *
 * Every refusal below names one wrong thing and the fix, and the problems are collected rather than
 * thrown one at a time so a fresh operator sees the whole list in one start attempt.
 */

export interface CacheServerConfig {
  /** Absolute directory holding one file per cache key. */
  readonly directory: string;
  /** `0` asks the OS for a free port; the bound port is reported by `startCacheServer`. */
  readonly port: number;
  /** Largest artifact this server accepts, in bytes. */
  readonly maxBytes: number;
  /** Published archive retention cap, independent of the per-upload limit. */
  readonly capBytes: number;
}

export type CacheServerConfigResult =
  | { readonly ok: true; readonly config: CacheServerConfig }
  | { readonly ok: false; readonly problems: readonly string[] };

/**
 * The cache is per user and unauthenticated: it binds loopback only, never a configurable address.
 */
export const HOST = '127.0.0.1';
export const DEFAULT_PORT = 8765;
/**
 * 1 GiB. The nx client builds the whole gzipped tarball in memory before it sends
 * (`packages/nx/src/native/cache/http_remote_cache.rs`, `store`), so artifacts are already bounded by
 * client RAM; this bound protects the server's disk from a client that is not nx.
 */
export const DEFAULT_MAX_BYTES = 1024 * 1024 * 1024;
export const DEFAULT_CAP_BYTES = 10 * 1024 * 1024 * 1024;

const MAX_PORT = 65_535;

type Env = Readonly<Record<string, string | undefined>>;

export function cacheServerConfigFromEnv(env: Env): CacheServerConfigResult {
  const problems: string[] = [];
  const directory = readDirectory(env, problems);
  const port = readInteger(env, 'NX_CACHE_SERVER_PORT', DEFAULT_PORT, 0, MAX_PORT, problems);
  const maxBytes = readInteger(
    env,
    'NX_CACHE_SERVER_MAX_BYTES',
    DEFAULT_MAX_BYTES,
    1,
    Number.MAX_SAFE_INTEGER,
    problems,
  );
  const capBytes = readInteger(
    env,
    'NX_CACHE_SERVER_CAP_BYTES',
    DEFAULT_CAP_BYTES,
    1,
    Number.MAX_SAFE_INTEGER,
    problems,
  );
  if (maxBytes !== undefined && capBytes !== undefined && maxBytes > capBytes) {
    problems.push('NX_CACHE_SERVER_MAX_BYTES must not exceed NX_CACHE_SERVER_CAP_BYTES.');
  }

  if (
    problems.length > 0 ||
    directory === undefined ||
    port === undefined ||
    maxBytes === undefined ||
    capBytes === undefined
  ) {
    return { ok: false, problems };
  }
  return { ok: true, config: { directory, port, maxBytes, capBytes } };
}

function readDirectory(env: Env, problems: string[]): string | undefined {
  const raw = env.NX_CACHE_SERVER_DIR?.trim();
  if (raw === undefined || raw === '') {
    problems.push(
      'NX_CACHE_SERVER_DIR is not set. Name the directory that holds cache entries; there is no default because a wrong guess would start an empty cache that looks healthy.',
    );
    return undefined;
  }
  if (!raw.startsWith('/')) {
    problems.push(
      `NX_CACHE_SERVER_DIR=${JSON.stringify(raw)} must be an absolute path: a supervisor starts this process, and its working directory is not the operator's.`,
    );
    return undefined;
  }
  return raw;
}

function readInteger(
  env: Env,
  name: string,
  fallback: number,
  min: number,
  max: number,
  problems: string[],
): number | undefined {
  const raw = env[name]?.trim();
  if (raw === undefined || raw === '') return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < min || value > max) {
    problems.push(`${name}=${JSON.stringify(raw)} must be an integer between ${min} and ${max}.`);
    return undefined;
  }
  return value;
}
