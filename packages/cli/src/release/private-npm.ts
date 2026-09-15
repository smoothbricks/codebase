import { existsSync, readFileSync } from 'node:fs';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { homedir, tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { privateNpmWorkflowConfig } from '@smoothbricks/nx-plugin/managed-files/private-npm-policy';
import type { PackagePrivateNpmConfig } from '../lib/json.js';
import { runResult } from '../lib/run.js';
import {
  getWorkspacePackageManifests,
  listPrivatePackages,
  type PackageInfo,
  readPackageJson,
  readPackageJsonObject,
} from '../lib/workspace.js';
import {
  type DurableState,
  isTransportFailure,
  probeWithTransportRetry,
  requireDurableState,
  undetermined,
} from './durable-state.js';

/** Minimal typed-result convention for private-registry resolution. */
export type Result<T, E> = { ok: true; value: T } | { ok: false; error: E };

export interface PrivateNpmRegistry {
  scope: string;
  registry: string;
  /** `//host/path:_authToken` key; the secret itself stays in the named env var. */
  authKey: string;
  readTokenEnv: string;
  publishTokenEnv?: string;
}

export type PrivateNpmRegistryConfigErrorKind =
  | 'MissingConfiguration'
  | 'MissingRegistry'
  | 'InvalidRegistry'
  | 'ScopeMismatch';

export interface PrivateNpmRegistryConfigError {
  kind: PrivateNpmRegistryConfigErrorKind;
  /** Names the variable/package/scope involved; never carries a token. */
  message: string;
}

type RegistryResult = Result<PrivateNpmRegistry, PrivateNpmRegistryConfigError>;

function configError(kind: PrivateNpmRegistryConfigErrorKind, message: string): RegistryResult {
  return { ok: false, error: { kind, message } };
}

/**
 * Resolve the Forgejo npm destination for the root's declared private scope.
 * The URL is the scoped `@scope:registry` entry in the committed .npmrc
 * (project file first, then the user's) — npm's own source of truth, never a
 * required environment variable. Validation (HTTPS, credential-free,
 * /api/packages/<owner>/npm/ owner path) runs here, and this only runs when
 * an actual registry operation needs a destination: shell entry never
 * requires private configuration. Diagnostics name the file, scope, or
 * missing credential variable — never a token value.
 *
 * ScopeMismatch is not produced here (a bare root carries no package scope to
 * compare); per-package scope/registry-conflict checks live in
 * assertPrivatePackage, which returns the same error shape.
 */
export function resolvePrivateNpmRegistry(root: string): RegistryResult {
  const rootPackage = readPackageJson(join(root, 'package.json'));
  const config = rootPackage?.json.smoo?.privateNpm;
  if (!config) {
    return configError(
      'MissingConfiguration',
      'No private npm destination is configured: package.json smoo.privateNpm is missing. Refusing private operation without declared configuration.',
    );
  }
  if (!config.scope.startsWith('@')) {
    return configError(
      'InvalidRegistry',
      `smoo.privateNpm.scope must be a scope such as @scope, got ${JSON.stringify(config.scope)}.`,
    );
  }
  const rawUrl = npmrcScopeRegistry(root, config.scope);
  if (!rawUrl) {
    return configError(
      'MissingRegistry',
      `Private npm registry URL is not configured: no ${config.scope}:registry entry in ${join(root, '.npmrc')} (or the user .npmrc). Refusing private operation.`,
    );
  }
  let url: URL;
  try {
    url = new URL(rawUrl);
  } catch {
    return configError('InvalidRegistry', `The ${config.scope}:registry entry in .npmrc is not a valid URL.`);
  }
  if (url.protocol !== 'https:') {
    return configError(
      'InvalidRegistry',
      `The ${config.scope}:registry entry in .npmrc must be an HTTPS URL ending in /api/packages/<owner>/npm/.`,
    );
  }
  if (url.username || url.password || url.search || url.hash) {
    return configError(
      'InvalidRegistry',
      `The ${config.scope}:registry entry in .npmrc must be a credential-free URL with no query or fragment; tokens belong in //host/path/:_authToken lines referencing an environment variable.`,
    );
  }
  // Forgejo npm endpoints are scoped under /api/packages/<owner>/npm/, with an
  // optional server base path ahead of it. Anchor the FINAL four segments so a
  // base path containing api/packages cannot shadow the owner path, and reject
  // duplicate slashes rather than filtering them away.
  if (url.pathname.includes('//')) {
    return configError(
      'InvalidRegistry',
      `The ${config.scope}:registry entry in .npmrc must not contain duplicate slashes.`,
    );
  }
  const segments = url.pathname.split('/').filter((segment) => segment.length > 0);
  const tail = segments.slice(-4);
  if (segments.length < 4 || tail[0] !== 'api' || tail[1] !== 'packages' || !tail[2] || tail[3] !== 'npm') {
    return configError(
      'InvalidRegistry',
      `The ${config.scope}:registry entry in .npmrc must end in /api/packages/<owner>/npm/ (Forgejo npm owner path).`,
    );
  }
  const pathname = url.pathname.endsWith('/') ? url.pathname : `${url.pathname}/`;
  // Token env names come from the explicit declaration first, then the
  // committed .npmrc `${VAR}` auth line. A publish credential is never
  // *inferred* for a workspace that does not publish the scope: a consumer
  // must refuse publication rather than reach for its read credential.
  // Reading with a declared publisher credential is the producer's normal
  // case, so the read name may fall back to it. Only the publish inference
  // consults the workspace graph, so ordinary status resolution costs no
  // manifest scan.
  const npmrcTokenEnv = NPMRC_TOKEN_ENV.exec(npmrcValue(root, `//${url.host}${pathname}:_authToken`) ?? '')?.[1];
  const publishTokenEnv =
    config.publishTokenEnv ??
    (npmrcTokenEnv && workspacePublishesPrivateScope(root, config.scope) ? npmrcTokenEnv : undefined);
  const readTokenEnv = config.readTokenEnv ?? npmrcTokenEnv ?? publishTokenEnv;
  if (!readTokenEnv) {
    return configError(
      'MissingConfiguration',
      'Private npm authentication must reference a token environment variable in .npmrc or smoo.privateNpm.',
    );
  }
  return {
    ok: true,
    value: {
      scope: config.scope,
      registry: `https://${url.host}${pathname}`,
      authKey: `//${url.host}${pathname}:_authToken`,
      readTokenEnv,
      publishTokenEnv,
    },
  };
}

function npmrcScopeRegistry(root: string, scope: string): string | null {
  return npmrcValue(root, `${scope}:registry`);
}

const NPMRC_TOKEN_ENV = /^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$/;

function npmrcValue(root: string, key: string): string | null {
  const candidates = [join(root, '.npmrc'), join(homedir(), '.npmrc')];
  for (const path of candidates) {
    let text: string;
    try {
      text = readFileSync(path, 'utf8');
    } catch {
      continue;
    }
    for (const line of text.split(/\r?\n/)) {
      const trimmed = line.trim();
      if (trimmed.length === 0 || trimmed.startsWith('#') || trimmed.startsWith(';')) {
        continue;
      }
      const separator = trimmed.indexOf('=');
      if (separator <= 0) {
        continue;
      }
      if (trimmed.slice(0, separator).trim() === key) {
        return trimmed.slice(separator + 1).trim();
      }
    }
  }
  return null;
}

function nameInPrivateScope(name: string, scope: string): boolean {
  return name === scope || name.startsWith(`${scope}/`);
}

function workspacePublishesPrivateScope(root: string, scope: string): boolean {
  return listPrivatePackages(root).some((pkg) => nameInPrivateScope(pkg.name, scope));
}

/**
 * Tokens the managed CI/publish workflows should expose. Derived from the
 * declared scope plus the workspace graph and `.npmrc` `${VAR}` auth line —
 * never from a hardcoded namespace. A consumer that does not publish the
 * scope does not receive a publish token; a producer that never installs the
 * scope from the registry does not receive a read token.
 */
export function resolvePrivateNpmWorkflowConfig(root: string): PackagePrivateNpmConfig | undefined {
  const manifest = readPackageJsonObject(join(root, 'package.json')) ?? {};
  const npmrcPath = join(root, '.npmrc');
  return privateNpmWorkflowConfig(
    manifest.smoo?.privateNpm,
    manifest,
    getWorkspacePackageManifests(root).map((pkg) => pkg.json),
    existsSync(npmrcPath) ? readFileSync(npmrcPath, 'utf8') : '',
  );
}

/** Throwing variant for publish/status paths: refuses with zero network I/O. */
export function requirePrivateNpmRegistry(root: string): PrivateNpmRegistry {
  const resolved = resolvePrivateNpmRegistry(root);
  if (!resolved.ok) {
    throw new Error(resolved.error.message);
  }
  return resolved.value;
}

export function isPrivatePackage(pkg: Pick<{ name: string }, 'name'>, registry: PrivateNpmRegistry): boolean {
  return pkg.name === registry.scope || pkg.name.startsWith(`${registry.scope}/`);
}

/**
 * Select the registry for a package name: null for public packages, the
 * resolved private registry for in-scope packages. Throws (no network) when a
 * private-scoped package lacks its URL/token configuration.
 */
export function selectRegistryForPackage(root: string, name: string): PrivateNpmRegistry | null {
  const rootPackage = readPackageJson(join(root, 'package.json'));
  const config = rootPackage?.json.smoo?.privateNpm;
  if (!config || (!name.startsWith(`${config.scope}/`) && name !== config.scope)) {
    return null;
  }
  return requirePrivateNpmRegistry(root);
}

/**
 * Enforce the ownership policy for one private package: scope must match and an
 * explicit publishConfig.registry must not conflict with the resolved
 * destination. Returns a ScopeMismatch error instead of throwing so pack
 * closure validation can collect failures.
 */
export function checkPrivatePackage(
  pkg: Pick<PackageInfo, 'name' | 'json'>,
  registry: PrivateNpmRegistry,
): PrivateNpmRegistryConfigError | null {
  if (!isPrivatePackage(pkg, registry)) {
    return {
      kind: 'ScopeMismatch',
      message: `${pkg.name}: package scope does not match private npm scope ${registry.scope}.`,
    };
  }
  const explicit = pkg.json.publishConfig?.registry;
  if (explicit && normalizeRegistryUrl(explicit) !== normalizeRegistryUrl(registry.registry)) {
    // Name the package and config field only: the explicit value is unvalidated
    // operator input and could carry credentials or query secrets.
    return {
      kind: 'ScopeMismatch',
      message: `${pkg.name}: publishConfig.registry does not match the private npm destination declared by smoo.privateNpm; remove it or align it with the declared destination.`,
    };
  }
  return null;
}

/** Throwing variant used on the publish path. */
export function assertPrivatePackage(pkg: Pick<PackageInfo, 'name' | 'json'>, registry: PrivateNpmRegistry): void {
  const failure = checkPrivatePackage(pkg, registry);
  if (failure) {
    throw new Error(failure.message);
  }
}

export type PublishDestination = { kind: 'public' } | { kind: 'private'; registry: PrivateNpmRegistry };

/**
 * Decide where a publishable package must go. Private-scoped names resolve to
 * the declared Forgejo destination; anything else is public. An npm:private
 * tag with no resolvable private registry throws fail-closed (naming the
 * variable/scope, never a token) so the caller can never fall through to
 * npmjs. Reads configuration only; makes no network requests.
 */
export function selectPublishDestination(
  root: string,
  pkg: Pick<{ name: string; tags: string[] }, 'name' | 'tags'>,
): PublishDestination {
  const registry = selectRegistryForPackage(root, pkg.name);
  if (registry) {
    return { kind: 'private', registry };
  }
  if (pkg.tags.includes('npm:private')) {
    const resolved = resolvePrivateNpmRegistry(root);
    const detail = resolved.ok
      ? `${pkg.name}: package scope does not match private npm scope ${resolved.value.scope}.`
      : resolved.error.message;
    throw new Error(`${pkg.name}: refusing to publish an npm:private package to the public registry: ${detail}`);
  }
  return { kind: 'public' };
}

function normalizeRegistryUrl(url: string): string {
  return url.endsWith('/') ? url : `${url}/`;
}

/**
 * Render a temporary npm userconfig binding the private scope to the resolved
 * registry. Secrets stay as `${TOKEN_ENV}` references; no token bytes are
 * written. Accepts any registry string (including http loopback fixtures):
 * URL validation lives in resolvePrivateNpmRegistry, not here.
 */
export function privateNpmUserconfigContent(
  resolved: PrivateNpmRegistry,
  options: { mode: 'read' | 'publish' },
): string {
  // One owner of mode -> credential: a publish userconfig authenticated with
  // the read credential would either 401 mid-publication or spend a read-only
  // credential on a write, so the mode resolver refuses instead of falling back.
  const tokenEnv = privateNpmTokenEnvForMode(resolved, options.mode);
  return `${resolved.scope}:registry=${resolved.registry}\n${resolved.authKey}=\${${tokenEnv}}\n`;
}

/** Resolve which token env a mode needs; throws naming the variable, never the token. */
export function privateNpmTokenEnvForMode(resolved: PrivateNpmRegistry, mode: 'read' | 'publish'): string {
  if (mode === 'read') {
    return resolved.readTokenEnv;
  }
  if (!resolved.publishTokenEnv) {
    throw new Error(
      `Private npm publish token is not configured: ${resolved.readTokenEnv} covers reads; no publish token env is declared. Refusing private publication.`,
    );
  }
  return resolved.publishTokenEnv;
}

/**
 * Run `fn` with a mode0600 temporary NPM_CONFIG_USERCONFIG holding only the
 * resolved scope registry and a path-scoped `${TOKEN_ENV}` reference. Actual
 * secrets remain environment values; the file is removed afterwards.
 */
export async function withPrivateNpmUserconfig<T>(
  resolved: PrivateNpmRegistry,
  mode: 'read' | 'publish',
  fn: (userconfigPath: string) => Promise<T>,
): Promise<T> {
  const tokenEnv = privateNpmTokenEnvForMode(resolved, mode);
  if (!process.env[tokenEnv]) {
    throw new Error(
      `Private npm ${mode} credential is not configured: environment variable ${tokenEnv} is unset or empty. Refusing private operation without contacting a registry.`,
    );
  }
  const dir = await mkdtemp(join(tmpdir(), 'smoo-private-npm-'));
  const userconfigPath = join(dir, 'userconfig');
  try {
    await writeFile(userconfigPath, privateNpmUserconfigContent(resolved, { mode }), { mode: 0o600 });
    return await fn(userconfigPath);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

export interface NpmStatusOptions {
  registry?: string;
  userconfig?: string;
  /** Path-scoped auth key and token env, so the credential can ride env config. */
  credential?: { authKey: string; tokenEnv: string };
}

/**
 * npm config precedence is cli > env > project `.npmrc` > userconfig. A
 * repository that commits its own `//host/path:_authToken=${SOME_ENV}` line
 * therefore OVERRIDES the userconfig this module writes, and when that env is
 * unset npm sends the unexpanded value: the registry answers 401 on reads with
 * a perfectly good read credential, naming nothing. So the credential also
 * rides as env config, which the project file cannot outrank, and the token
 * value stays out of every file on disk.
 */
function npmStatusEnv(
  userconfig: string | undefined,
  credential?: { authKey: string; tokenEnv: string },
): Record<string, string> | undefined {
  if (!userconfig) return undefined;
  const env: Record<string, string> = { NPM_CONFIG_USERCONFIG: userconfig };
  const token = credential ? process.env[credential.tokenEnv] : undefined;
  if (credential && token) {
    env[`npm_config_${credential.authKey}`] = token;
  }
  return env;
}

function npmCommandFailedMessage(npmArgs: string[], exitCode: number, stdout = '', stderr = ''): string {
  const detail = `${stdout}\n${stderr}`.trim();
  return `npm ${npmArgs.join(' ')} failed with exit code ${exitCode}${detail ? `: ${detail}` : ''}`;
}

export interface NpmCommandResult {
  exitCode: number;
  stdout: string;
  stderr: string;
}

/**
 * How a status probe reaches npm and how it waits between attempts. Injected
 * so the three outcomes are pinned without a registry: the probe's verdict is
 * a pure function of npm's exit code and output, and this is the seam that
 * supplies them.
 */
export interface NpmStatusShell {
  /** Runs `npm <args>`; a non-zero exit is reported, never thrown. */
  run(args: string[], env: Record<string, string> | undefined): Promise<NpmCommandResult>;
  /** Backoff between transport retries. */
  sleep(ms: number): Promise<void>;
}

export function npmProcessStatusShell(root: string): NpmStatusShell {
  return {
    run: (args, env) => runResult('npm', args, root, env),
    sleep: (ms) => delay(ms),
  };
}

/** A genuine not-found verdict: the one failure that means "not published". */
const NPM_NOT_FOUND = /\bE404\b|404 Not Found/i;

function npmViewArgs(target: string, field: string, options: NpmStatusOptions): string[] {
  // --fetch-retries=0: npm's own retry ladder is off because a 404 is the
  // common answer on the publish path. Transport retries happen above instead,
  // where a 404 does not pay for them.
  const args = ['view', target, field, '--json', '--fetch-retries=0'];
  if (options.registry) {
    args.push('--registry', options.registry);
  }
  return args;
}

/**
 * One `npm view` reduced to the three outcomes. Exit 0 is a verdict, E404 is a
 * verdict, a dead connection is not -- and the difference between the last two
 * is the whole point: both used to leave here as "failed", so a reset packet
 * ended a release that had nothing wrong with it.
 */
async function npmViewStatus(args: string[], options: NpmStatusOptions, shell: NpmStatusShell): Promise<DurableState> {
  return probeWithTransportRetry(async () => {
    const result = await shell.run(args, npmStatusEnv(options.userconfig, options.credential));
    if (result.exitCode === 0) {
      return { kind: 'exists' };
    }
    const output = `${result.stdout}\n${result.stderr}`;
    if (NPM_NOT_FOUND.test(output)) {
      return { kind: 'absent' };
    }
    const failure = npmCommandFailedMessage(args, result.exitCode, result.stdout, result.stderr);
    if (isTransportFailure(output)) {
      return undetermined(failure);
    }
    // A verdict this probe must not interpret: unauthorized, forbidden,
    // unparseable. Retrying it is pointless and reading it as absent would
    // publish over a version the registry refused to describe.
    throw new Error(failure);
  }, shell);
}

/**
 * Whether the registry holds this exact version: the durable-state probe the
 * publish and repair paths plan from.
 */
export async function npmPublishedVersionStatus(
  root: string,
  name: string,
  version: string,
  options: NpmStatusOptions = {},
  shell: NpmStatusShell = npmProcessStatusShell(root),
): Promise<DurableState> {
  return npmViewStatus(npmViewArgs(`${name}@${version}`, 'version', options), options, shell);
}

/** Package-level existence on the same path. */
export async function npmPackageStatus(
  root: string,
  name: string,
  options: NpmStatusOptions = {},
  shell: NpmStatusShell = npmProcessStatusShell(root),
): Promise<DurableState> {
  return npmViewStatus(npmViewArgs(name, 'name', options), options, shell);
}

/**
 * Boolean view for callers that must decide now: absent is false, and an
 * undetermined probe refuses the release rather than guessing. A caller that
 * can do something better with a non-answer takes `npmPublishedVersionStatus`
 * instead.
 */
export async function npmPublishedVersionExists(
  root: string,
  name: string,
  version: string,
  options: NpmStatusOptions = {},
  shell: NpmStatusShell = npmProcessStatusShell(root),
): Promise<boolean> {
  return requireDurableState(
    await npmPublishedVersionStatus(root, name, version, options, shell),
    `${name}@${version}`,
    'whether this version is published on the npm registry',
  );
}

export async function npmPackageExists(
  root: string,
  name: string,
  options: NpmStatusOptions = {},
  shell: NpmStatusShell = npmProcessStatusShell(root),
): Promise<boolean> {
  return requireDurableState(
    await npmPackageStatus(root, name, options, shell),
    name,
    'whether this package exists on the npm registry',
  );
}

export interface PrivateNpmPublishDiagnosticShell {
  publish(): Promise<void>;
  versionExists(): Promise<boolean>;
  log(message: string): void;
  error(message: string): void;
  appendSummary(markdown: string): Promise<void>;
}

/** Explicit publish args for Forgejo: restricted access, resolved registry, no npmjs provenance. */
export function privateNpmPublishArgs(tarball: string, tag: string, registry: PrivateNpmRegistry): string[] {
  return ['publish', tarball, '--access', 'restricted', '--tag', tag, '--registry', registry.registry];
}

/**
 * Private counterpart to publishWithAuthDiagnostics: reuses the run/publish +
 * version-check shape, but diagnoses against the configured token env names and
 * never invokes npmjs bootstrap/trusted-publisher repair. Authentication or
 * network failures refuse the operation; only a version already visible on the
 * private registry continues.
 */
export async function publishPrivateWithDiagnostics(
  pkg: Pick<{ name: string; version: string }, 'name' | 'version'>,
  shell: PrivateNpmPublishDiagnosticShell,
  registry: PrivateNpmRegistry,
): Promise<void> {
  try {
    await shell.publish();
  } catch (error) {
    const packageVersion = `${pkg.name}@${pkg.version}`;
    // The status probe is failure-aware, so it throws on 401/403/5xx. Letting
    // that throw escape from here would replace the publish diagnostic with a
    // status diagnostic and skip the operator summary entirely: the run would
    // report "cannot read the registry" for what is actually a failed publish.
    // Classify instead -- visible / absent / unknown -- and only a visible
    // version continues.
    let visible: boolean;
    let statusFailure: unknown;
    try {
      visible = await shell.versionExists();
    } catch (probeError) {
      visible = false;
      statusFailure = probeError;
    }
    if (visible) {
      shell.log(`${packageVersion}: publish result already visible on the private registry; continuing.`);
      return;
    }
    const statusDetail =
      statusFailure === undefined
        ? ''
        : ` The follow-up status query also failed, so publication state is unknown: ${
            statusFailure instanceof Error ? statusFailure.message : String(statusFailure)
          }`;
    const message =
      `${packageVersion}: private npm publish failed. ` +
      `Verify ${registry.readTokenEnv} (status) and ${registry.publishTokenEnv ?? registry.readTokenEnv} (publish) ` +
      `are set and the Forgejo namespace/package ACLs grant access to ${registry.registry}.${statusDetail}`;
    shell.error(message);
    await shell.appendSummary(`## Private npm publish failed\n\nPackage: \`${packageVersion}\`\n\n${message}\n`);
    throw new Error(`${packageVersion}: private npm publish failed; refusing instead of treating it as unpublished.`, {
      cause: error,
    });
  }
}
