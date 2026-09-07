import { readFileSync } from 'node:fs';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { homedir, tmpdir } from 'node:os';
import { join } from 'node:path';
import type { PackageJson, PackagePrivateNpmConfig } from '../lib/json.js';
import { runResult } from '../lib/run.js';
import {
  getWorkspacePackageManifests,
  listPrivatePackages,
  type PackageInfo,
  readPackageJson,
  readPackageJsonObject,
  workspaceDependencyFields,
} from '../lib/workspace.js';

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
  const readTokenEnv = config.readTokenEnv ?? config.publishTokenEnv;
  if (!readTokenEnv) {
    return configError(
      'MissingConfiguration',
      'smoo.privateNpm must name a token environment variable (readTokenEnv or publishTokenEnv).',
    );
  }
  return {
    ok: true,
    value: {
      scope: config.scope,
      registry: `https://${url.host}${pathname}`,
      authKey: `//${url.host}${pathname}:_authToken`,
      readTokenEnv,
      publishTokenEnv: config.publishTokenEnv,
    },
  };
}

function npmrcScopeRegistry(root: string, scope: string): string | null {
  return npmrcValue(root, `${scope}:registry`);
}

const NPMRC_TOKEN_ENV = /^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$/;

function npmrcAuthTokenEnv(root: string, scope: string): string | undefined {
  const registry = npmrcScopeRegistry(root, scope);
  if (!registry) {
    return undefined;
  }
  let url: URL;
  try {
    url = new URL(registry);
  } catch {
    return undefined;
  }
  const pathname = url.pathname.endsWith('/') ? url.pathname : `${url.pathname}/`;
  const value = npmrcValue(root, `//${url.host}${pathname}:_authToken`);
  if (!value) {
    return undefined;
  }
  return NPMRC_TOKEN_ENV.exec(value)?.[1];
}

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

function workspaceConsumesPrivateScope(root: string, scope: string): boolean {
  const rootJson = readPackageJsonObject(join(root, 'package.json'));
  const manifests: PackageJson[] = [
    ...(rootJson ? [rootJson] : []),
    ...getWorkspacePackageManifests(root).map((pkg) => pkg.json),
  ];
  for (const json of manifests) {
    for (const field of workspaceDependencyFields) {
      const deps = json[field];
      if (!deps) {
        continue;
      }
      for (const [name, spec] of Object.entries(deps)) {
        if (
          nameInPrivateScope(name, scope) &&
          !spec.startsWith('workspace:') &&
          !spec.startsWith('link:') &&
          !spec.startsWith('file:')
        ) {
          return true;
        }
      }
    }
  }
  return false;
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
  const declared = readPackageJsonObject(join(root, 'package.json'))?.smoo?.privateNpm;
  if (!declared) {
    return undefined;
  }
  const consumes = workspaceConsumesPrivateScope(root, declared.scope);
  const publishes = workspacePublishesPrivateScope(root, declared.scope);
  if (!consumes && !publishes) {
    return undefined;
  }
  const npmrcEnv = npmrcAuthTokenEnv(root, declared.scope);
  const readTokenEnv = consumes ? (declared.readTokenEnv ?? npmrcEnv) : undefined;
  const publishTokenEnv = publishes ? (declared.publishTokenEnv ?? (consumes ? undefined : npmrcEnv)) : undefined;
  if (!readTokenEnv && !publishTokenEnv) {
    return undefined;
  }
  return {
    scope: declared.scope,
    ...(readTokenEnv ? { readTokenEnv } : {}),
    ...(publishTokenEnv ? { publishTokenEnv } : {}),
  };
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
  const tokenEnv =
    options.mode === 'publish' ? (resolved.publishTokenEnv ?? resolved.readTokenEnv) : resolved.readTokenEnv;
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
}

function npmStatusEnv(userconfig: string | undefined): Record<string, string> | undefined {
  return userconfig ? { NPM_CONFIG_USERCONFIG: userconfig } : undefined;
}

function npmCommandFailedMessage(npmArgs: string[], exitCode: number, stdout = '', stderr = ''): string {
  const detail = `${stdout}\n${stderr}`.trim();
  return `npm ${npmArgs.join(' ')} failed with exit code ${exitCode}${detail ? `: ${detail}` : ''}`;
}

/**
 * Failure-aware registry status: only a genuine not-found result means absent.
 * Unauthorized/forbidden/unavailable (and any other failure) throw instead of
 * reading as unpublished. Passes --fetch-retries=0 so 5xx responses fail fast
 * instead of hanging CI in npm's retry loop. No fallback to another registry.
 */
export async function npmPublishedVersionExists(
  root: string,
  name: string,
  version: string,
  options: NpmStatusOptions = {},
): Promise<boolean> {
  const args = ['view', `${name}@${version}`, 'version', '--json', '--fetch-retries=0'];
  if (options.registry) {
    args.push('--registry', options.registry);
  }
  const result = await runResult('npm', args, root, npmStatusEnv(options.userconfig));
  if (result.exitCode === 0) {
    return true;
  }
  if (/\bE404\b|404 Not Found/i.test(`${result.stdout}\n${result.stderr}`)) {
    return false;
  }
  throw new Error(npmCommandFailedMessage(args, result.exitCode, result.stdout, result.stderr));
}

/** Failure-aware package-level existence on the same path (E404-only absent). */
export async function npmPackageExists(root: string, name: string, options: NpmStatusOptions = {}): Promise<boolean> {
  const args = ['view', name, 'name', '--json', '--fetch-retries=0'];
  if (options.registry) {
    args.push('--registry', options.registry);
  }
  const result = await runResult('npm', args, root, npmStatusEnv(options.userconfig));
  if (result.exitCode === 0) {
    return true;
  }
  if (/\bE404\b|404 Not Found/i.test(`${result.stdout}\n${result.stderr}`)) {
    return false;
  }
  throw new Error(npmCommandFailedMessage(args, result.exitCode, result.stdout, result.stderr));
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
    if (await shell.versionExists()) {
      shell.log(`${packageVersion}: publish result already visible on the private registry; continuing.`);
      return;
    }
    const message =
      `${packageVersion}: private npm publish failed. ` +
      `Verify ${registry.readTokenEnv} (status) and ${registry.publishTokenEnv ?? registry.readTokenEnv} (publish) ` +
      `are set and the Forgejo namespace/package ACLs grant access to ${registry.registry}.`;
    shell.error(message);
    await shell.appendSummary(`## Private npm publish failed\n\nPackage: \`${packageVersion}\`\n\n${message}\n`);
    throw new Error(`${packageVersion}: private npm publish failed; refusing instead of treating it as unpublished.`, {
      cause: error,
    });
  }
}
