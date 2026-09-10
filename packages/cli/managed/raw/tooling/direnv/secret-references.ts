/**
 * Provider-neutral local secret resolution for smoo-managed repositories.
 *
 * Reads the root package.json `smoo.secrets` map and `smoo.remoteCache` block
 * — the bootstrap twins of `PackageSecretCommand`, `PackageSmooConfig.secrets`
 * and `PackageRemoteCacheConfig` in packages/cli/src/lib/json.ts. This file is
 * a managed raw script: it runs from `tooling/direnv/setup-environment.ts`
 * BEFORE `bun install`, when no workspace package and no Typia transform exist
 * yet, so both shapes are hand-validated here against exactly what json.ts
 * declares. Keep them aligned; smoo's Typia validation fails the manifest at
 * generation time for anything this file would reject at runtime.
 *
 * Routing per declared variable, in first-match order:
 *
 *  1. An existing nonempty environment value wins; no command runs.
 *  2. CI (`CI` set): variables must be injected from the CI secret store;
 *     provider commands never run there. Missing variables refuse with
 *     injected-secret guidance instead of a confusing auth failure later.
 *  3. Inside a cowshed workspace (`COWSHED_WORKSPACE_TOKEN` set), a variable
 *     referenced from `.npmrc` is registry routing intent: it is excluded
 *     from local provider resolution and refuses with names-only
 *     gateway-enrollment guidance. Generic variables still use env/provider.
 *  4. Otherwise the command argv executes directly, without a shell; stdout
 *     with one terminal newline trimmed becomes the value.
 *
 * Failures aggregate so a single direnv reload surfaces every problem.
 * Error text names variables and exit codes only: secret values, provider
 * stdout/stderr, and command arguments are never echoed.
 * A resolved value still reaches the environment of the install its caller
 * runs next, so `maskSecretValues` redacts those values out of any captured
 * child output that caller replays.
 *
 * The variable `smoo.remoteCache.tokenSecret` names is never resolved here,
 * nor anywhere at shell entry. A remote cache is an optimization, and shell
 * entry happens on every direnv reload and every `devenv shell -- <command>`:
 * a provider command run there is a credential prompt on each of them. Nx
 * reads NX_SELF_HOSTED_REMOTE_CACHE_SERVER and _ACCESS_TOKEN from the
 * environment it runs in — CI injects them, a developer exports the token
 * once in the terminal that wants the cache — and every nested shell inherits
 * them. Absent, Nx keeps to its local cache.
 */
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

export interface SecretSpec {
  /** Executed directly, without a shell; stdout supplies the secret value. */
  readonly command: readonly [string, ...string[]];
}

export type SecretCommandRunner = (argv: readonly string[]) => Promise<string>;

export interface SecretResolutionRequest {
  readonly secrets: Readonly<Record<string, SecretSpec>>;
  /** Env names referenced from `.npmrc`; registry routing intent inside a cowshed workspace. */
  readonly registryIntentEnvs: ReadonlySet<string>;
  /** Environment consulted for existing values and mode flags; read, never written. */
  readonly env: Readonly<Record<string, string | undefined>>;
  /** Overrides the default direct-argv runner (test seam). */
  readonly runCommand?: SecretCommandRunner;
}

export interface SecretResolutionOptions {
  /** Repository root holding package.json and .npmrc. */
  readonly root: string;
  /** Environment to consult; defaults to process.env. Read, never written. */
  readonly env?: Record<string, string | undefined>;
  /** Overrides the default direct-argv runner (test seam). */
  readonly runCommand?: SecretCommandRunner;
}

/**
 * The env name pattern every shell accepts, so the same regex classifies
 * declared names and `.npmrc` references without echoing values.
 */
const ENV_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;

/** The `.npmrc` interpolations Bun substitutes while reading auth and registry config. */
const NPMRC_ENV_REFERENCE = /\$\{([A-Za-z_][A-Za-z0-9_]*)\}/g;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isNonemptyEnvValue(value: string | undefined): value is string {
  return value !== undefined && value.length > 0;
}

/**
 * Validates the `smoo.secrets` map out of an already-parsed package.json.
 * Absent `smoo` or `smoo.secrets` is an empty map; a malformed declaration
 * throws naming the offending variable — never a value.
 */
export function parseSmooSecrets(packageJson: unknown): Readonly<Record<string, SecretSpec>> {
  if (!isRecord(packageJson)) return {};
  const smoo = packageJson.smoo;
  if (!isRecord(smoo)) return {};
  const secrets = smoo.secrets;
  if (secrets === undefined) return {};
  if (!isRecord(secrets)) {
    throw new Error('smoo.secrets must map environment variable names to { command: [string, ...] }');
  }
  const parsed: Record<string, SecretSpec> = {};
  for (const [name, spec] of Object.entries(secrets)) {
    parsed[name] = parseSecretSpec(name, spec);
  }
  return parsed;
}

function parseSecretSpec(name: string, spec: unknown): SecretSpec {
  if (!ENV_NAME.test(name)) {
    throw new Error(
      `smoo.secrets: "${name}" is not a valid environment variable name (expected [A-Za-z_][A-Za-z0-9_]*)`,
    );
  }
  if (!isRecord(spec)) {
    throw new Error(`smoo.secrets.${name}: each entry must be an object with a command array`);
  }
  const raw = spec.command;
  if (!Array.isArray(raw) || raw.length === 0) {
    throw new Error(`smoo.secrets.${name}.command: must be an array of at least one string`);
  }
  for (const part of raw) {
    if (typeof part !== 'string') {
      throw new Error(`smoo.secrets.${name}.command: must be an array of at least one string`);
    }
  }
  const [first, ...rest] = raw;
  return { command: [first, ...rest] };
}

/**
 * The declared remote cache as a local shell needs it. `internalServer` is
 * deliberately absent: it is the address a managed runner reaches inside its
 * own network, and a shell that loads this file is by definition outside it,
 * so a developer machine always takes the public `server`.
 */
export interface RemoteCacheSpec {
  readonly server: string;
  /** Variable holding the cache token: an ambient value, or a `smoo.secrets` entry resolved here. */
  readonly tokenSecret: string;
}

/**
 * Validates the `smoo.remoteCache` block out of an already-parsed
 * package.json. Absent `smoo` or `smoo.remoteCache` is no cache; a malformed
 * declaration throws naming the field.
 */
export function parseSmooRemoteCache(packageJson: unknown): RemoteCacheSpec | null {
  if (!isRecord(packageJson)) return null;
  const smoo = packageJson.smoo;
  if (!isRecord(smoo)) return null;
  const remoteCache = smoo.remoteCache;
  if (remoteCache === undefined) return null;
  if (!isRecord(remoteCache)) {
    throw new Error('smoo.remoteCache must be an object with { server, tokenSecret }');
  }
  const server = remoteCache.server;
  const tokenSecret = remoteCache.tokenSecret;
  // A trailing slash is refused rather than trimmed: Nx appends
  // `/v1/cache/<hash>`, so the doubled slash is a route that answers 404
  // forever, and silently repairing the manifest here would leave managed CI
  // — which reads the same field — disagreeing with this shell.
  if (typeof server !== 'string' || server.length === 0 || server.endsWith('/')) {
    throw new Error(
      'smoo.remoteCache.server must be an origin with no trailing slash, e.g. https://nx-cache.example.net',
    );
  }
  if (typeof tokenSecret !== 'string' || !ENV_NAME.test(tokenSecret)) {
    throw new Error('smoo.remoteCache.tokenSecret must name the environment variable holding the cache token');
  }
  return { server, tokenSecret };
}

/** Every `${VAR}` referenced from `.npmrc` text; an absent file contributes nothing. */
export function registryAuthEnvNames(npmrcText: string | null): ReadonlySet<string> {
  const names = new Set<string>();
  if (npmrcText === null) return names;
  for (const match of npmrcText.matchAll(NPMRC_ENV_REFERENCE)) {
    names.add(match[1]);
  }
  return names;
}

type SecretOutcome =
  | { readonly name: string; readonly kind: 'env-wins' }
  | { readonly name: string; readonly kind: 'resolved'; readonly value: string }
  | { readonly name: string; readonly kind: 'failed'; readonly guidance: string };

/**
 * One variable's routing, the four rules in the header applied in order. It is
 * a value rather than a throw so each caller presents a failure in its own
 * terms: an install refuses, the remote cache turns itself off.
 */
async function routeSecret(
  name: string,
  spec: SecretSpec,
  context: {
    readonly env: Readonly<Record<string, string | undefined>>;
    readonly registryIntentEnvs: ReadonlySet<string>;
    readonly run: SecretCommandRunner;
  },
): Promise<SecretOutcome> {
  const { env } = context;
  if (isNonemptyEnvValue(env[name])) {
    return { name, kind: 'env-wins' };
  }
  if (isNonemptyEnvValue(env.CI)) {
    return {
      name,
      kind: 'failed',
      guidance:
        'CI does not run secret provider commands — inject this variable into the job environment from the CI secret store',
    };
  }
  if (isNonemptyEnvValue(env.COWSHED_WORKSPACE_TOKEN) && context.registryIntentEnvs.has(name)) {
    return {
      name,
      kind: 'failed',
      guidance:
        'referenced from .npmrc and absent in this cowshed workspace — enroll registry credentials through the cowshed gateway; gateway-managed workspaces never resolve .npmrc registry variables via local provider commands',
    };
  }
  try {
    const value = (await context.run(spec.command)).replace(/\r?\n$/, '');
    if (value.length === 0) {
      return {
        name,
        kind: 'failed',
        guidance:
          'provider command produced no output — run it locally to see why (its arguments and output are never logged here)',
      };
    }
    return { name, kind: 'resolved', value };
  } catch (error) {
    return { name, kind: 'failed', guidance: describeCommandFailure(error) };
  }
}

export async function resolveSecrets(request: SecretResolutionRequest): Promise<Readonly<Record<string, string>>> {
  const context = {
    env: request.env,
    registryIntentEnvs: request.registryIntentEnvs,
    run: request.runCommand ?? runSecretCommand,
  };
  const outcomes = await Promise.all(
    Object.entries(request.secrets).map(async ([name, spec]) => routeSecret(name, spec, context)),
  );
  const failures = outcomes.filter(
    (outcome): outcome is Extract<SecretOutcome, { kind: 'failed' }> => outcome.kind === 'failed',
  );
  if (failures.length > 0) {
    throw new Error(
      [
        'smoo secret resolution failed; dependencies were not installed:',
        ...failures.map((failure) => `- ${failure.name}: ${failure.guidance}`),
      ].join('\n'),
    );
  }
  const resolved: Record<string, string> = {};
  for (const outcome of outcomes) {
    if (outcome.kind === 'resolved') {
      resolved[outcome.name] = outcome.value;
    }
  }
  return resolved;
}

class SecretCommandExitError extends Error {
  constructor(readonly exitCode: number) {
    super('secret provider command failed');
  }
}

class SecretCommandStartError extends Error {
  constructor() {
    super('secret provider command could not be started');
  }
}

function describeCommandFailure(error: unknown): string {
  if (error instanceof SecretCommandExitError) {
    return `provider command exited with code ${error.exitCode}; its arguments and output are suppressed — run it locally to debug`;
  }
  if (error instanceof SecretCommandStartError) {
    return 'provider command could not be started — check that its program exists and is executable, then run it locally to debug';
  }
  // Foreign runner errors may carry argv or provider output; only the generic
  // suppression note survives.
  return 'provider command failed — its arguments and output are suppressed; run it locally to debug';
}

/** The program name is committed config, but an argument may embed a credential, so argv is never echoed. */
function startSecretCommand(argv: readonly string[]) {
  try {
    return Bun.spawn({
      cmd: [...argv],
      stdin: 'ignore',
      stdout: 'pipe',
      // Provider stderr can quote arguments or the secret itself; it is never surfaced.
      stderr: 'ignore',
    });
  } catch {
    throw new SecretCommandStartError();
  }
}

const runSecretCommand: SecretCommandRunner = async (argv) => {
  const child = startSecretCommand(argv);
  const stdout = await new Response(child.stdout).text();
  const exitCode = await child.exited;
  if (exitCode !== 0) {
    throw new SecretCommandExitError(exitCode);
  }
  return stdout;
};

/** '*', the byte a redacted secret leaves behind. */
const MASK_BYTE = 0x2a;

/**
 * A copy of captured child output with every resolved secret value replaced
 * by the same number of `*` bytes.
 *
 * setup-environment.ts puts the values this file resolves into the
 * environment of the install it then runs, and replays that child's captured
 * stdout/stderr verbatim when it fails. Whatever the child echoes — a prepare
 * script printing its environment, a registry URL carrying an inline
 * credential — would otherwise walk straight past the suppression the rest of
 * this file maintains. Redaction is byte-exact and same-length, so the
 * failure being reported stays legible and every offset in it still lines up.
 * Output with nothing to redact is returned as it came in, so the path that
 * has no secrets to hide copies nothing.
 */
export function maskSecretValues(output: Uint8Array, values: Iterable<string>): Uint8Array {
  const encoder = new TextEncoder();
  let masked: Uint8Array | undefined;
  for (const value of values) {
    const needle = encoder.encode(value);
    if (needle.length === 0 || needle.length > output.length) {
      continue;
    }
    for (let from = 0; from + needle.length <= output.length; ) {
      const at = indexOfBytes(masked ?? output, needle, from);
      if (at < 0) {
        break;
      }
      // `new Uint8Array`, not `.slice()`: a captured child's output arrives as
      // a node Buffer, whose `slice` is `subarray` — a view. Masking through
      // one would rewrite the caller's own captured bytes.
      masked ??= new Uint8Array(output);
      masked.fill(MASK_BYTE, at, at + needle.length);
      from = at + needle.length;
    }
  }
  return masked ?? output;
}

/** First occurrence of `needle` in `haystack` at or after `from`, or -1. */
function indexOfBytes(haystack: Uint8Array, needle: Uint8Array, from: number): number {
  const first = needle[0];
  if (first === undefined) {
    return -1;
  }
  const last = haystack.length - needle.length;
  for (let at = haystack.indexOf(first, from); at >= 0 && at <= last; at = haystack.indexOf(first, at + 1)) {
    let matches = true;
    for (let offset = 1; offset < needle.length; offset += 1) {
      if (haystack[at + offset] !== needle[offset]) {
        matches = false;
        break;
      }
    }
    if (matches) {
      return at;
    }
  }
  return -1;
}

/**
 * The declared secrets an install needs, resolved before it runs. The cache
 * token is deliberately not among them: a remote cache is an optimization, so
 * its credential is resolved by the shell's cache export — where an
 * unreachable secret provider costs a stderr line — while every secret an
 * install actually depends on still refuses loudly here. Which variable that
 * is comes from `smoo.remoteCache.tokenSecret` rather than a second flag, so
 * the two declarations cannot disagree.
 */
export async function resolveSecretEnvironment(
  options: SecretResolutionOptions,
): Promise<Readonly<Record<string, string>>> {
  const packageJson = readPackageJson(options.root);
  const cacheToken = parseSmooRemoteCache(packageJson)?.tokenSecret;
  const required: Record<string, SecretSpec> = {};
  for (const [name, spec] of Object.entries(parseSmooSecrets(packageJson))) {
    if (name !== cacheToken) {
      required[name] = spec;
    }
  }
  return resolveSecrets({
    secrets: required,
    registryIntentEnvs: registryAuthEnvNames(readNpmrcText(options.root)),
    env: options.env ?? process.env,
    runCommand: options.runCommand,
  });
}

function readPackageJson(root: string): unknown {
  const packageJsonPath = join(root, 'package.json');
  let text: string;
  try {
    text = readFileSync(packageJsonPath, 'utf8');
  } catch (error) {
    throw new Error(
      `smoo secrets: cannot read ${packageJsonPath} (${error instanceof Error ? error.message : String(error)})`,
    );
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (error) {
    throw new Error(
      `smoo secrets: ${packageJsonPath} is not valid JSON (${error instanceof Error ? error.message : String(error)})`,
    );
  }
  return parsed;
}
function readNpmrcText(root: string): string | null {
  const npmrcPath = join(root, '.npmrc');
  if (!existsSync(npmrcPath)) return null;
  return readFileSync(npmrcPath, 'utf8');
}
