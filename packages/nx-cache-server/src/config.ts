/// <reference types="node" />

/**
 * Startup configuration, read once from the environment and refused loudly when it cannot describe a
 * server that is safe to run.
 *
 * A token is a `label:secret` pair, not a bare secret, because every audit line names the principal
 * that read or published an artifact and no line may ever carry the secret itself. Capability rides
 * with the token: `NX_CACHE_SERVER_WRITE_TOKENS` may publish, `NX_CACHE_SERVER_READ_TOKENS` may only
 * read. That split is the CREEP mitigation (see the module header of `server.ts`): an untrusted
 * context gets a read token and cannot reach the write path at all.
 *
 * Every refusal below names one wrong thing and the fix, and the problems are collected rather than
 * thrown one at a time so a fresh operator sees the whole list in one start attempt.
 */

export type CacheCapability = 'read' | 'write';

export interface CachePrincipal {
  /** Audit name for this token. Appears in every log line the token produces; the secret never does. */
  readonly label: string;
  /** The exact `Authorization` header value this principal must present, precomputed at startup. */
  readonly bearer: string;
  readonly capability: CacheCapability;
}

export interface CacheServerConfig {
  /** Absolute directory holding one file per cache key. */
  readonly directory: string;
  readonly host: string;
  /** `0` asks the OS for a free port; the bound port is reported by `startCacheServer`. */
  readonly port: number;
  /** Largest artifact this server accepts, in bytes. */
  readonly maxBytes: number;
  /** Published archive retention cap, independent of the per-upload limit. */
  readonly capBytes: number;
  readonly principals: readonly CachePrincipal[];
}

export type CacheServerConfigResult =
  | { readonly ok: true; readonly config: CacheServerConfig }
  | { readonly ok: false; readonly problems: readonly string[] };

/**
 * Loopback by default: this server authenticates with a shared bearer token, so the address it binds
 * is the difference between "the sheds on this host" and "the network". Widening it is an explicit
 * act (`NX_CACHE_SERVER_HOST=0.0.0.0` or an interface address), never an accident of the default.
 */
export const DEFAULT_HOST = '127.0.0.1';
export const DEFAULT_PORT = 8765;
/**
 * 1 GiB. The nx client builds the whole gzipped tarball in memory before it sends
 * (`packages/nx/src/native/cache/http_remote_cache.rs`, `store`), so artifacts are already bounded by
 * client RAM; this bound protects the server's disk from a client that is not nx.
 */
export const DEFAULT_MAX_BYTES = 1024 * 1024 * 1024;
export const DEFAULT_CAP_BYTES = 10 * 1024 * 1024 * 1024;

const MAX_PORT = 65_535;
/** Lowercase, digits and dashes: a label is read by humans in logs and typed into a systemd unit. */
const LABEL = /^[a-z0-9][a-z0-9-]{0,31}$/;

const TOKEN_SOURCES: readonly (readonly [envName: string, capability: CacheCapability])[] = [
  ['NX_CACHE_SERVER_WRITE_TOKENS', 'write'],
  ['NX_CACHE_SERVER_READ_TOKENS', 'read'],
];

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
  const principals = readPrincipals(env, problems);
  const configuredHost = env.NX_CACHE_SERVER_HOST?.trim();
  const host = configuredHost === undefined || configuredHost === '' ? DEFAULT_HOST : configuredHost;

  if (
    problems.length > 0 ||
    directory === undefined ||
    port === undefined ||
    maxBytes === undefined ||
    capBytes === undefined ||
    principals === undefined
  ) {
    return { ok: false, problems };
  }
  return { ok: true, config: { directory, host, port, maxBytes, capBytes, principals } };
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

function readPrincipals(env: Env, problems: string[]): readonly CachePrincipal[] | undefined {
  const principals: CachePrincipal[] = [];
  const labels = new Set<string>();
  const secrets = new Set<string>();

  for (const [envName, capability] of TOKEN_SOURCES) {
    const raw = env[envName];
    if (raw === undefined) continue;
    const entries = raw.split(',');
    entries.forEach((rawEntry, index) => {
      const entry = rawEntry.trim();
      if (entry === '') return;
      // The position, never the entry: an entry without a colon is entirely secret.
      const at = `${envName} entry ${index + 1}`;
      const colon = entry.indexOf(':');
      if (colon < 1) {
        problems.push(
          `${at} is not "label:secret". Every token carries a label so an audit line can name the principal that published an artifact without printing the token.`,
        );
        return;
      }
      const label = entry.slice(0, colon);
      const secret = entry.slice(colon + 1);
      if (!LABEL.test(label)) {
        problems.push(
          `${at} label ${JSON.stringify(label)} must match ${LABEL.source} — lowercase, digits and dashes.`,
        );
        return;
      }
      if (secret === '') {
        problems.push(`${at} (label ${label}) has an empty secret.`);
        return;
      }
      if (labels.has(label)) {
        problems.push(`${at} reuses label ${label}. One label names one principal, or the audit log lies.`);
        return;
      }
      if (secrets.has(secret)) {
        problems.push(
          `${at} (label ${label}) repeats a secret already configured for another principal. One secret maps to exactly one label and one capability.`,
        );
        return;
      }
      labels.add(label);
      secrets.add(secret);
      principals.push({ label, bearer: `Bearer ${secret}`, capability });
    });
  }

  if (principals.length === 0) {
    problems.push(
      'No tokens configured. Set NX_CACHE_SERVER_WRITE_TOKENS and/or NX_CACHE_SERVER_READ_TOKENS to comma-separated "label:secret" entries; the write list may publish artifacts, the read list may only fetch them.',
    );
    return undefined;
  }
  return principals;
}
