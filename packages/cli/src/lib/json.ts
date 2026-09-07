import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import typia, { type IValidation } from 'typia';

/** String-keyed dependency / script / engine maps. */
export type StringMap = Record<string, string>;

export interface PackageRepository {
  type?: string;
  url?: string;
  directory?: string;
}

export interface PackageNxConfig {
  name?: string;
  tags?: string[];
  includedScripts?: string[];
  targets?: Record<string, NxTargetConfig>;
}

export interface PackagePublishConfig {
  access?: string;
  /** Real registry URL for publication metadata. Resolved configuration only — never an environment placeholder in a packed package.json. */
  registry?: string;
}

export interface PackageSmooGithub {
  /** Action repository provider selected while generating workflows. Default: GitHub. */
  actionsProvider?: 'github' | 'forgejo';
  pushBranches?: string[];
  /** GitHub Actions runs-on for managed CI (string or label list). Default: ubuntu-latest. */
  runsOn?: string | string[];
  /** macOS platform job runs-on labels (string or label list). Default: macos-latest. */
  macosRunsOn?: string | string[];
  /** Opt in to building foreign platform artifacts on Linux without native-host runtime tests. */
  platformProducer?: {
    kind: 'linux-cross';
    /** Required shell preflight, run from the repository root before toolchain setup or builds. */
    preflight: string;
    /** Toolchain environment on the Linux foreign-platform producer, such as an SDK path. */
    env?: StringMap;
  };
  /**
   * Sibling source checkouts a repo path-depends on (Cargo path
   * dependencies, nix path inputs). The CI workflow clones each entry beside
   * the main checkout before any build step runs, so the workspace layout on
   * CI matches the developer workspace. Private sources need tokenEnv naming
   * the repo-scoped read secret; fork PRs receive no secrets and skip the
   * step entirely.
   */
  sourceCheckouts?: PackageSourceCheckoutConfig[];
  /** Declared Cargo private-dependency credentials; absent means no private Cargo fetch. */
  cargoCredentials?: PackageCargoCredentialsConfig;
}

/** One declared sibling source checkout for managed CI. */
export interface PackageSourceCheckoutConfig {
  /** Destination relative to the CI workspace root, e.g. `../smoothbricks`. */
  path: string;
  /** HTTPS clone URL of the source repository. */
  repository: string;
  /** Exact commit or ref to check out; the remote default branch when omitted. */
  ref?: string;
  /** Secret name holding the source-read credential; required for private sources. */
  tokenEnv?: string;
}

/**
 * Declared opt-in for Cargo fetching private dependencies in managed CI. The
 * generator wires the named secrets into the job environment and, for private
 * git origins, installs a host-gated credential helper through `GIT_CONFIG_*`
 * process environment: the token is read at helper-call time, never embedded
 * in a URL, argv, or any stored git/cargo configuration, and an empty
 * `credential.helper` reset keeps an ambient credential store from capturing
 * it. Registry tokens are plain `CARGO_REGISTRIES_<NAME>_TOKEN` mappings read
 * by Cargo's own credential provider.
 */
export interface PackageCargoCredentialsConfig {
  /** Private git origins Cargo fetches dependencies from. */
  gitOrigins?: PackageCargoGitOrigin[];
  /** Credential env names Cargo's `cargo:token` provider reads for private registries. */
  registryTokenEnvs?: string[];
}

/** One private git origin with the secret that reads it. */
export interface PackageCargoGitOrigin {
  /** Credential-free https origin, e.g. `https://git.example.net`. */
  origin: string;
  /** Secret name holding the source-read credential for this origin. */
  tokenEnv: string;
}

/**
 * Declared opt-in for private npm distribution. The registry URL itself is
 * the scoped `@scope:registry` entry in the committed .npmrc — never a
 * required environment variable and never a shell-entry check. Token env
 * names are taken from `.npmrc` `${VAR}` auth lines when omitted. A consumer
 * that never publishes packages in this scope omits publishTokenEnv; a
 * producer whose install is fully `workspace:`/`link:`/`file:` omits
 * readTokenEnv.
 */
export interface PackagePrivateNpmConfig {
  scope: string;
  /** Environment variable holding the package-read token, referenced from .npmrc. */
  readTokenEnv?: string;
  /** Environment variable holding the separate publisher token; publishers only. */
  publishTokenEnv?: string;
}

export interface PackageSmooConfig {
  github?: PackageSmooGithub;
  privateNpm?: PackagePrivateNpmConfig;
}

export interface PackageWorkspacesObject {
  packages?: string[];
}

/**
 * package.json shape used by smoo. Optional fields stay optional so partial
 * manifests parse; mutation helpers create nested objects on demand.
 */
export interface PackageJson {
  name?: string;
  version?: string;
  private?: boolean;
  license?: string;
  types?: string;
  typings?: string;
  packageManager?: string;
  files?: string[];
  bin?: string | StringMap;
  exports?: PackageExports;
  workspaces?: string[] | PackageWorkspacesObject;
  dependencies?: StringMap;
  devDependencies?: StringMap;
  peerDependencies?: StringMap;
  optionalDependencies?: StringMap;
  /** Bun patchedDependencies map (package@version → patch path). */
  patchedDependencies?: StringMap;
  engines?: StringMap;
  scripts?: StringMap;
  publishConfig?: PackagePublishConfig;
  repository?: string | PackageRepository;
  nx?: PackageNxConfig;
  smoo?: PackageSmooConfig;
}

/** Recursive package exports map (conditions nest arbitrarily). */
export type PackageExports = string | PackageExportMap | null | undefined;
export interface PackageExportMap {
  [condition: string]: PackageExports;
}

export interface NxDependsOnObject {
  target: string;
  projects?: string | string[];
}

export type NxDependsOn = string | NxDependsOnObject;

/** Nx target options are executor-specific open bags. */
export type NxTargetOptions = Record<string, unknown>;

export type NxInput = string | object;

export interface NxTargetConfig {
  executor?: string;
  dependsOn?: NxDependsOn[];
  outputs?: string[];
  inputs?: NxInput[];
  options?: NxTargetOptions;
  configurations?: Record<string, NxTargetConfig>;
  cache?: boolean;
  command?: string;
  parallelism?: boolean;
}

export interface NxProjectJson {
  name?: string;
  root?: string;
  targets?: Record<string, NxTargetConfig>;
}

export interface NxJson {
  namedInputs?: Record<string, Array<string | Record<string, unknown>> | string>;
}

/** Parse package.json text. Invalid JSON throws; wrong shape returns null. */
export const parsePackageJsonText = typia.json.createIsParse<PackageJson>();

/** Parse `nx show project --json` output. Invalid JSON throws; wrong shape returns null. */
export const parseNxProjectJsonText = typia.json.createIsParse<NxProjectJson>();

/** Parse nx.json text. Invalid JSON throws; wrong shape returns null. */
export const parseNxJsonText = typia.json.createIsParse<NxJson>();

/** Parse a JSON string array. Invalid JSON throws; non-arrays return null. */
export const parseStringArrayText = typia.json.createIsParse<string[]>();

/** typia's failures as `path: expected type` items, the `$input` root stripped so paths read as the document's own. */
export function formatValidationErrors(errors: IValidation.IError[]): string {
  return errors.map((error) => `${error.path.replace(/^\$input\.?/, '')}: expected ${error.expected}`).join(', ');
}

/**
 * A file's text through a typia JSON parser. Text that is not JSON at all makes the parser throw a bare
 * SyntaxError that names no file; the rethrow names the one the text came from.
 */
export function parseJsonFileText<T>(path: string, text: string, parse: (text: string) => T): T {
  try {
    return parse(text);
  } catch (error) {
    if (error instanceof SyntaxError) throw new Error(`${path} is not valid JSON: ${error.message}`);
    throw error;
  }
}

const isPackageJsonValue = typia.createIs<PackageJson>();

export function readJsonObject(path: string): PackageJson | null {
  if (!existsSync(path)) {
    return null;
  }
  try {
    return parsePackageJsonText(readFileSync(path, 'utf8'));
  } catch {
    return null;
  }
}

export function requiredJsonObject(path: string): PackageJson {
  const json = readJsonObject(path);
  if (!json) {
    throw new Error(`${path} not found or invalid`);
  }
  return json;
}

export function writeJsonObject(path: string, value: object): void {
  writeFileSync(path, jsonObjectText(value));
}

export function jsonObjectText(value: object): string {
  return `${JSON.stringify(value, null, 2)}\n`;
}

export function readJson(path: string): unknown {
  if (!existsSync(path)) {
    return null;
  }
  return JSON.parse(readFileSync(path, 'utf8'));
}

export function isPackageJson(value: unknown): value is PackageJson {
  return isPackageJsonValue(value);
}

/** Ensure package.json.scripts exists. */
export function ensureScripts(pkg: PackageJson): StringMap {
  if (isStringMap(pkg.scripts)) {
    return pkg.scripts;
  }
  const next: StringMap = {};
  pkg.scripts = next;
  return next;
}

/** Ensure package.json.dependencies (or other string-map dep field) exists. */
export function ensureDependencyMap(
  pkg: PackageJson,
  key: 'dependencies' | 'devDependencies' | 'peerDependencies' | 'optionalDependencies' | 'patchedDependencies',
): StringMap {
  const current = pkg[key];
  if (isStringMap(current)) {
    return current;
  }
  const next: StringMap = {};
  pkg[key] = next;
  return next;
}

/** Ensure package.json.engines exists. */
export function ensureEngines(pkg: PackageJson): StringMap {
  if (isStringMap(pkg.engines)) {
    return pkg.engines;
  }
  const next: StringMap = {};
  pkg.engines = next;
  return next;
}

/** Ensure package.json.nx exists. */
export function ensureNx(pkg: PackageJson): PackageNxConfig {
  if (isPackageNxConfig(pkg.nx)) {
    return pkg.nx;
  }
  const next: PackageNxConfig = {};
  pkg.nx = next;
  return next;
}

/** Ensure package.json.nx.targets exists. */
export function ensureNxTargets(nx: PackageNxConfig): Record<string, NxTargetConfig> {
  if (isNxTargets(nx.targets)) {
    return nx.targets;
  }
  const next: Record<string, NxTargetConfig> = {};
  nx.targets = next;
  return next;
}

/** Ensure package.json.publishConfig exists. */
export function ensurePublishConfig(pkg: PackageJson): PackagePublishConfig {
  if (isPublishConfig(pkg.publishConfig)) {
    return pkg.publishConfig;
  }
  const next: PackagePublishConfig = {};
  pkg.publishConfig = next;
  return next;
}

/** Ensure package.json.repository is an object (not a string URL). */
export function ensureRepositoryObject(pkg: PackageJson): PackageRepository {
  if (isRepositoryObject(pkg.repository)) {
    return pkg.repository;
  }
  const next: PackageRepository = {};
  pkg.repository = next;
  return next;
}

export function setStringProperty(record: StringMap, key: string, value: string): boolean {
  if (record[key] === value) {
    return false;
  }
  record[key] = value;
  return true;
}

export function setMissingStringProperty(record: StringMap, key: string, value: string): boolean {
  if (typeof record[key] === 'string') {
    return false;
  }
  record[key] = value;
  return true;
}

export function setPackageStringField(
  pkg: PackageJson,
  key: 'name' | 'version' | 'license' | 'types' | 'packageManager',
  value: string,
): boolean {
  if (pkg[key] === value) {
    return false;
  }
  pkg[key] = value;
  return true;
}

export function setMissingPackageStringField(
  pkg: PackageJson,
  key: 'name' | 'version' | 'license' | 'types' | 'packageManager',
  value: string,
): boolean {
  if (typeof pkg[key] === 'string') {
    return false;
  }
  pkg[key] = value;
  return true;
}

export function setNxName(nx: PackageNxConfig, value: string): boolean {
  if (nx.name === value) {
    return false;
  }
  nx.name = value;
  return true;
}

export function setPublishAccess(publishConfig: PackagePublishConfig, value: string): boolean {
  if (publishConfig.access === value) {
    return false;
  }
  publishConfig.access = value;
  return true;
}

export function setRepositoryField(
  repository: PackageRepository,
  key: 'type' | 'url' | 'directory',
  value: string,
): boolean {
  if (repository[key] === value) {
    return false;
  }
  repository[key] = value;
  return true;
}

export function setMissingRepositoryField(
  repository: PackageRepository,
  key: 'type' | 'url' | 'directory',
  value: string,
): boolean {
  if (typeof repository[key] === 'string') {
    return false;
  }
  repository[key] = value;
  return true;
}

function isStringMap(value: unknown): value is StringMap {
  return value !== null && value !== undefined && typeof value === 'object' && !Array.isArray(value);
}

function isPackageNxConfig(value: unknown): value is PackageNxConfig {
  return value !== null && value !== undefined && typeof value === 'object' && !Array.isArray(value);
}

function isNxTargets(value: unknown): value is Record<string, NxTargetConfig> {
  return value !== null && value !== undefined && typeof value === 'object' && !Array.isArray(value);
}

function isPublishConfig(value: unknown): value is PackagePublishConfig {
  return value !== null && value !== undefined && typeof value === 'object' && !Array.isArray(value);
}

function isRepositoryObject(value: unknown): value is PackageRepository {
  return value !== null && value !== undefined && typeof value === 'object' && !Array.isArray(value);
}
