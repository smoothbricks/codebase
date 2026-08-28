import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import typia from 'typia';
import { cliPackageVersion, isSmoothBricksCodebasePackageName } from '../lib/cli-package.js';
import {
  ensureDependencyMap,
  readJsonObject,
  setPackageStringField,
  setStringProperty,
  writeJsonObject,
} from '../lib/json.js';
import { type PackageJson, readPackageJsonObject } from '../lib/workspace.js';

interface RequiredDependency {
  name: string;
  /**
   * Seed used only when the repository has no bun.lock entry yet (a fresh
   * bootstrap). For `pinnedFromLockfile` dependencies the lock, not this value,
   * decides what package.json must say.
   */
  fallbackVersion: string;
  minimumVersion?: string;
  prefix?: string;
  useWorkspaceRangeInCodebase?: boolean;
  /**
   * Exact pin whose value the repository owns. bun.lock is the source of truth:
   * this CLI must not carry a second copy of a version the repo has already
   * resolved, or `smoo monorepo update` silently reverts a deliberate bump.
   */
  pinnedFromLockfile?: boolean;
}

export interface ToolPolicy {
  isSmoothBricksCodebase: boolean;
  toolingPackageName: string;
  cliDependencyRange: string;
  /** Exact versions bun.lock resolved, for `pinnedFromLockfile` dependencies. */
  lockedToolVersions: Record<string, string>;
}

export interface ToolContext {
  rootPackage: PackageJson | null;
  policy: ToolPolicy;
}

interface RegistryPackument {
  versions: Record<string, unknown>;
  'dist-tags'?: Record<string, string>;
}

const isRegistryPackument = typia.createIs<RegistryPackument>();
const rootDevDependencies: RequiredDependency[] = [
  { name: '@biomejs/biome', fallbackVersion: '^2.3.5', minimumVersion: '2.3.0', prefix: '^' },
  { name: '@nx/js', fallbackVersion: '23.1.0', minimumVersion: '23.1.0' },
  {
    name: '@smoothbricks/nx-plugin',
    fallbackVersion: '^0.3.0',
    minimumVersion: '0.3.0',
    prefix: '^',
    useWorkspaceRangeInCodebase: true,
  },
  { name: 'eslint', fallbackVersion: '^9.39.1', minimumVersion: '9.39.0', prefix: '^' },
  {
    name: 'eslint-stdout',
    fallbackVersion: '^1.1.1',
    minimumVersion: '1.1.1',
    prefix: '^',
    useWorkspaceRangeInCodebase: true,
  },
  { name: 'nx', fallbackVersion: '23.1.0', minimumVersion: '23.1.0' },
  { name: 'prettier', fallbackVersion: '^3.6.1', minimumVersion: '3.6.0', prefix: '^' },
  // ttsc participates in cache keys and vendors the Go SDK that
  // `smoo monorepo check` matches against devenv's pinned Go, so it is an exact
  // pin the repository owns, read back from bun.lock rather than restated here.
  { name: 'ttsc', fallbackVersion: '0.28.3', pinnedFromLockfile: true },
  // Nx and typescript-eslint still load the TypeScript JS API (6.x).
  // Compilation is exclusively delegated to ttsc by the Nx plugin targets.
  { name: 'typescript', fallbackVersion: '^6.0.3', minimumVersion: '6.0.0', prefix: '^' },
  // TS7 native compiler for ttsc (TTSC_TSGO_BINARY). Not the unscoped API package.
  // npm alias form required — Bun cannot install @typescript/typescript6 cleanly (bun#33834).
  { name: '@typescript/native', fallbackVersion: 'npm:typescript@^7.0.2' },
];

const cliPackageName = '@smoothbricks/cli';

// Go and Node are deliberately absent: ./devenv.smoo.nix supplies both, and
// every repository is required to import it (validateDevenvModuleImport). Go
// comes from the dedicated `nixpkgs-go` input pinned to the patch release ttsc
// vendors, and Node as an explicit major. Requiring them again here made
// `smoo monorepo update` append a bare `go` — which resolves from the default
// channel and reintroduces the `compile: version does not match go tool
// version` skew the pinned input exists to prevent — and a `nodejs_latest`,
// which is the opposite of a pinned fleet-wide runtime.
const requiredDevenvPackages = ['bun', 'git', 'git-format-staged', 'jq', 'alejandra', 'coreutils', 'gnutar'];
// A repo-owned bare `go` shadows the pinned one on PATH, so it is rejected
// rather than merely not required. Anchored to a package-list entry, never a
// comment that only mentions Go.
const unpinnedGoPackagePattern = /^\s*go\s*(#.*)?$/m;
const requiredRustDevenvPackages = ['sccache'];
const linuxCompilerPackage = 'pkgs.stdenv.cc';
const ignoredNativeManifestDirectories = new Set([
  '.devenv',
  '.direnv',
  '.git',
  '.nx',
  'dist',
  'node_modules',
  'target',
  'vendor',
]);

const obsoleteTtscPatchedDependencyKey = 'ttsc@0.19.3';

export async function applyToolConfigDefaults(root: string): Promise<void> {
  const context = await readToolContext(root);
  await applyRootPackageToolDefaults(root, context);
  applyToolingPackageDefaults(root, context.policy);
  applyDevenvPackageDefaults(root);
}

export async function validateToolConfig(root: string): Promise<number> {
  const context = await readToolContext(root);
  return (
    validateRootDevDependencies(context.policy, context.rootPackage) +
    validateObsoleteTtscPatchRemoved(context.rootPackage) +
    validateToolingPackage(root, context.policy) +
    validateToolingWorkspace(context.rootPackage) +
    validateDevenvPackages(root)
  );
}

export async function applyRootDevDependencyDefaults(root: string, context: ToolContext): Promise<void> {
  const pkg = context.rootPackage;
  if (!pkg) {
    return;
  }
  let changed = false;
  const devDependencies = ensureDependencyMap(pkg, 'devDependencies');
  for (const dependency of rootDevDependencies) {
    const current = devDependencies[dependency.name];
    if (typeof current !== 'string' || !satisfiesDependencyPolicy(context.policy, current, dependency)) {
      const version = await resolveDependencyVersion(context.policy, dependency);
      changed = setStringProperty(devDependencies, dependency.name, version) || changed;
    }
  }
  // `delete` returns true for an absent key, so an unguarded delete reported
  // "updated" and rewrote package.json on every run — which is why nothing this
  // command wrote was ever scrutinised.
  if (cliPackageName in devDependencies) {
    delete devDependencies[cliPackageName];
    changed = true;
  }
  changed = removeObsoleteTtscPatch(pkg) || changed;
  if (changed) {
    writeJsonObject(join(root, 'package.json'), pkg);
    console.log('updated        package.json workspace tool dependencies');
  } else {
    console.log('unchanged      package.json workspace tool dependencies');
  }
}

async function applyRootPackageToolDefaults(root: string, context: ToolContext): Promise<void> {
  const pkg = context.rootPackage;
  if (!pkg) {
    return;
  }
  let dependencyChanged = false;
  let workspaceChanged = false;
  const devDependencies = ensureDependencyMap(pkg, 'devDependencies');
  for (const dependency of rootDevDependencies) {
    const current = devDependencies[dependency.name];
    if (typeof current !== 'string' || !satisfiesDependencyPolicy(context.policy, current, dependency)) {
      const version = await resolveDependencyVersion(context.policy, dependency);
      dependencyChanged = setStringProperty(devDependencies, dependency.name, version) || dependencyChanged;
    }
  }
  if (cliPackageName in devDependencies) {
    delete devDependencies[cliPackageName];
    dependencyChanged = true;
  }
  workspaceChanged = addWorkspacePattern(pkg, 'tooling');
  dependencyChanged = removeObsoleteTtscPatch(pkg) || dependencyChanged;
  if (dependencyChanged || workspaceChanged) {
    writeJsonObject(join(root, 'package.json'), pkg);
  }
  console.log(
    dependencyChanged
      ? 'updated        package.json workspace tool dependencies'
      : 'unchanged      package.json workspace tool dependencies',
  );
  console.log(
    workspaceChanged
      ? 'updated        package.json tooling workspace'
      : 'unchanged      package.json tooling workspace',
  );
}

export function applyToolingPackageDefaults(root: string, policy: ToolPolicy): void {
  const path = join(root, 'tooling', 'package.json');
  const pkg = readJsonObject(path) ?? { name: policy.toolingPackageName, private: true, dependencies: {} };
  let changed = false;
  changed = setPackageStringField(pkg, 'name', policy.toolingPackageName) || changed;
  if (pkg.private !== true) {
    pkg.private = true;
    changed = true;
  }
  const dependencies = ensureDependencyMap(pkg, 'dependencies');
  changed = setStringProperty(dependencies, cliPackageName, policy.cliDependencyRange) || changed;
  if (changed || !existsSync(path)) {
    mkdirSync(dirname(path), { recursive: true });
    writeJsonObject(path, pkg);
    console.log('updated        tooling/package.json tooling dependencies');
  } else {
    console.log('unchanged      tooling/package.json tooling dependencies');
  }
}

export function applyToolingWorkspaceDefault(root: string): void {
  const path = join(root, 'package.json');
  const pkg = readJsonObject(path);
  if (!pkg) {
    return;
  }
  if (addWorkspacePattern(pkg, 'tooling')) {
    writeJsonObject(path, pkg);
    console.log('updated        package.json tooling workspace');
  } else {
    console.log('unchanged      package.json tooling workspace');
  }
}

export function applyDevenvPackageDefaults(root: string): void {
  const path = join(root, 'tooling', 'direnv', 'devenv.nix');
  if (!existsSync(path)) {
    return;
  }
  let content = readFileSync(path, 'utf8');
  let changed = false;
  const compilesRust = workspaceCompilesRust(root);
  const requiredPackages = [...requiredDevenvPackages, ...(compilesRust ? requiredRustDevenvPackages : [])];
  for (const name of requiredPackages) {
    if (hasNixPackage(content, name)) {
      continue;
    }
    const next = addNixPackage(content, name, nixPackageComment(name));
    changed = next !== content || changed;
    content = next;
  }
  if (compilesRust && !hasLinuxCompilerPackage(content)) {
    const next = addLinuxCompilerPackage(content);
    changed = next !== content || changed;
    content = next;
  }
  if (changed) {
    writeFileSync(path, content);
    console.log('updated        tooling/direnv/devenv.nix packages');
  } else {
    console.log('unchanged      tooling/direnv/devenv.nix packages');
  }
}

export function validateRootDevDependencies(policy: ToolPolicy, rootPackage: PackageJson | null): number {
  const pkg = rootPackage;
  if (!pkg) {
    console.error('package.json not found or invalid');
    return 1;
  }
  const devDependencies = pkg.devDependencies;
  let failures = 0;
  for (const dependency of rootDevDependencies) {
    const version = devDependencies?.[dependency.name];
    if (typeof version !== 'string') {
      console.error(`package.json devDependencies.${dependency.name} must be defined`);
      failures++;
    } else if (!satisfiesDependencyPolicy(policy, version, dependency)) {
      console.error(
        `package.json devDependencies.${dependency.name} must be ${formatExpectedDependency(policy, dependency)}; found ${version}`,
      );
      failures++;
    }
  }
  if (typeof devDependencies?.[cliPackageName] === 'string') {
    console.error(`package.json devDependencies.${cliPackageName} must move to tooling/package.json dependencies`);
    failures++;
  }
  return failures;
}

function removeObsoleteTtscPatch(pkg: PackageJson): boolean {
  const patchedDependencies = pkg.patchedDependencies;
  if (!patchedDependencies || !(obsoleteTtscPatchedDependencyKey in patchedDependencies)) {
    return false;
  }
  delete patchedDependencies[obsoleteTtscPatchedDependencyKey];
  if (Object.keys(patchedDependencies).length === 0) {
    delete pkg.patchedDependencies;
  }
  return true;
}

function validateObsoleteTtscPatchRemoved(rootPackage: PackageJson | null): number {
  const patchedDependencies = rootPackage?.patchedDependencies;
  if (!patchedDependencies || !(obsoleteTtscPatchedDependencyKey in patchedDependencies)) {
    return 0;
  }
  console.error(
    `package.json patchedDependencies.${obsoleteTtscPatchedDependencyKey} must be removed; every supported ttsc emits typia declarations without the obsolete patch`,
  );
  return 1;
}

export function validateToolingPackage(root: string, policy: ToolPolicy): number {
  const path = join(root, 'tooling', 'package.json');
  const pkg = readJsonObject(path);
  if (!pkg) {
    console.error('tooling/package.json not found or invalid');
    return 1;
  }
  const dependencies = pkg.dependencies;
  let failures = 0;
  const actualName = pkg.name ?? null;
  if (actualName !== policy.toolingPackageName) {
    console.error(`tooling/package.json name must be ${policy.toolingPackageName}`);
    failures++;
  }
  const actualCliRange = dependencies?.[cliPackageName];
  if (actualCliRange !== policy.cliDependencyRange) {
    console.error(`tooling/package.json dependencies.${cliPackageName} must be ${policy.cliDependencyRange}`);
    failures++;
  }
  return failures;
}

export function validateToolingWorkspace(rootPackage: PackageJson | null): number {
  const pkg = rootPackage;
  if (!pkg) {
    console.error('package.json not found or invalid');
    return 1;
  }
  if (!hasWorkspacePattern(pkg, 'tooling')) {
    console.error('package.json workspaces must include tooling so tooling/package.json participates in installs');
    return 1;
  }
  return 0;
}

export function validateDevenvPackages(root: string): number {
  const path = join(root, 'tooling', 'direnv', 'devenv.nix');
  if (!existsSync(path)) {
    console.error('tooling/direnv/devenv.nix not found');
    return 1;
  }
  const content = readFileSync(path, 'utf8');
  let failures = 0;
  if (unpinnedGoPackagePattern.test(content)) {
    console.error(
      'tooling/direnv/devenv.nix packages must not include a bare `go`: ./devenv.smoo.nix supplies Go from the ' +
        'pinned nixpkgs-go input, matched to the SDK ttsc vendors. An unqualified `go` resolves from the default ' +
        'channel and shadows it on PATH, which fails as `compile: version does not match go tool version`.',
    );
    failures++;
  }
  const compilesRust = workspaceCompilesRust(root);
  const requiredPackages = [...requiredDevenvPackages, ...(compilesRust ? requiredRustDevenvPackages : [])];
  for (const name of requiredPackages) {
    if (!hasNixPackage(content, name)) {
      console.error(`tooling/direnv/devenv.nix packages must include ${name}`);
      failures++;
    }
  }
  if (compilesRust && !hasLinuxCompilerPackage(content)) {
    console.error(
      'tooling/direnv/devenv.nix packages must include pkgs.stdenv.cc in a Linux-only package block for Rust builds',
    );
    failures++;
  }
  return failures;
}

function addNixPackage(content: string, name: string, comment: string): string {
  const packageLine = `    ${name}${comment ? ` ${comment}` : ''}\n`;
  if (hasNixPackage(content, name)) {
    return content;
  }
  const directPackagesStart = content.indexOf('  packages = with pkgs; [');
  const groupedPackagesStart = content.indexOf('    (with pkgs; [');
  const packagesStart = directPackagesStart === -1 ? groupedPackagesStart : directPackagesStart;
  if (packagesStart === -1) {
    return content;
  }
  const insertAt = content.indexOf('\n', packagesStart) + 1;
  return `${content.slice(0, insertAt)}${packageLine}${content.slice(insertAt)}`;
}

function hasNixPackage(content: string, name: string): boolean {
  return new RegExp(`(^|\\s)${escapeRegex(name)}(\\s|#|$)`, 'm').test(content);
}

function workspaceCompilesRust(root: string): boolean {
  return containsCargoManifest(root);
}

function containsCargoManifest(directory: string): boolean {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    if (entry.isFile() && entry.name === 'Cargo.toml') {
      return true;
    }
    if (
      entry.isDirectory() &&
      entry.name.startsWith('.') === false &&
      ignoredNativeManifestDirectories.has(entry.name) === false &&
      containsCargoManifest(join(directory, entry.name))
    ) {
      return true;
    }
  }
  return false;
}

function hasLinuxCompilerPackage(content: string): boolean {
  const linuxPackages = /\+\+\s+lib\.optionals\s+pkgs\.stdenv\.isLinux\s+\[([\s\S]*?)\]/g;
  return [...content.matchAll(linuxPackages)].some((match) => hasNixPackage(match[1] ?? '', linuxCompilerPackage));
}

function addLinuxCompilerPackage(content: string): string {
  if (hasLinuxCompilerPackage(content)) {
    return content;
  }
  const directStart = content.indexOf('  packages = with pkgs; [');
  if (directStart !== -1) {
    const listEnd = content.indexOf('\n  ];', directStart);
    if (listEnd === -1) {
      return content;
    }
    const listBody = content.slice(directStart + '  packages = with pkgs; ['.length, listEnd);
    const replacement = `  packages =\n    (with pkgs; [${listBody}\n    ])\n    ++ lib.optionals pkgs.stdenv.isLinux [\n      ${linuxCompilerPackage}\n    ];`;
    return `${content.slice(0, directStart)}${replacement}${content.slice(listEnd + '\n  ];'.length)}`;
  }

  const packagesStart = content.indexOf('  packages =');
  const nextOption = content.indexOf('\n\n  ', packagesStart);
  if (packagesStart === -1 || nextOption === -1) {
    return content;
  }
  const terminator = content.lastIndexOf(';', nextOption);
  if (terminator < packagesStart) {
    return content;
  }
  const linuxPackages = `\n    ++ lib.optionals pkgs.stdenv.isLinux [\n      ${linuxCompilerPackage}\n    ]`;
  return `${content.slice(0, terminator)}${linuxPackages}${content.slice(terminator)}`;
}

function nixPackageComment(name: string): string {
  if (name === 'coreutils') {
    return '# Provides fmt for commit message wrapping';
  }
  if (name === 'gnutar') {
    return '# Tarball inspection for package validation';
  }
  if (name === 'git') {
    return '# Git hooks and repository inspection';
  }
  if (name === 'sccache') {
    return '# Rust compiler cache; client of the host-owned daemon (cowshed sccache start)';
  }
  return '';
}

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function satisfiesDependencyPolicy(policy: ToolPolicy, version: string, dependency: RequiredDependency): boolean {
  if (workspaceDependencyExpected(policy, dependency)) {
    return version === 'workspace:*';
  }
  // Dual-package native compiler: must stay an npm:typescript@7 alias, never unscoped "typescript".
  if (dependency.name === '@typescript/native') {
    return isTypeScriptNativeAlias(version);
  }
  if (dependency.pinnedFromLockfile === true) {
    return version === expectedPinnedVersion(policy, dependency);
  }
  if (dependency.minimumVersion === undefined) {
    return version === dependency.fallbackVersion;
  }
  const parsed = parseVersion(version);
  const minimum = parseVersion(dependency.minimumVersion);
  if (!parsed || !minimum) {
    return false;
  }
  return compareVersions(parsed, minimum) >= 0;
}

function isTypeScriptNativeAlias(version: string): boolean {
  // Accept npm:typescript@7, npm:typescript@^7.0.2, npm:typescript@~7.0.2, etc.
  return /^npm:typescript@(?:\^|~)?7(?:\.|$)/.test(version.trim());
}

function formatExpectedDependency(policy: ToolPolicy, dependency: RequiredDependency): string {
  if (workspaceDependencyExpected(policy, dependency)) {
    return 'workspace:*';
  }
  if (dependency.name === '@typescript/native') {
    return 'npm:typescript@^7 (TypeScript 7 native compiler alias for ttsc)';
  }
  if (dependency.pinnedFromLockfile === true) {
    const expected = expectedPinnedVersion(policy, dependency);
    const locked = policy.lockedToolVersions[dependency.name];
    return locked === undefined
      ? `${expected} (no bun.lock entry yet; bootstrap seed)`
      : `${expected}, the version bun.lock resolved — change the pin by installing it, not by editing package.json alone`;
  }
  return dependency.minimumVersion ? `>= ${dependency.minimumVersion}` : dependency.fallbackVersion;
}

export async function readToolContext(root: string): Promise<ToolContext> {
  const rootPackage = readPackageJsonObject(join(root, 'package.json'));
  const toolingPackage = readJsonObject(join(root, 'tooling', 'package.json'));
  const configuredCliRange = toolingPackage?.dependencies?.[cliPackageName];
  return {
    rootPackage,
    policy: await toolPolicy(
      rootPackage,
      typeof configuredCliRange === 'string' ? configuredCliRange : null,
      readLockedToolVersions(root),
    ),
  };
}

async function toolPolicy(
  rootPackage: PackageJson | null,
  configuredCliRange: string | null,
  lockedToolVersions: Record<string, string>,
): Promise<ToolPolicy> {
  const name = rootPackage?.name ?? null;
  const isCodebase = isSmoothBricksCodebasePackageName(name ?? undefined);
  const toolingName = toolingPackageName(name);
  return {
    isSmoothBricksCodebase: isCodebase,
    toolingPackageName: toolingName,
    // Consumers pin the latest *published* CLI. Running a linked prerelease must not
    // freeze tooling/package.json on an older range or rewrite it to an unpublished -next.
    cliDependencyRange: isCodebase ? 'workspace:*' : await resolvePublishedCliDependencyRange(configuredCliRange),
    lockedToolVersions,
  };
}

/**
 * Exact versions bun.lock resolved for the dependencies the repository pins.
 *
 * A resolved package entry is `"<name>": ["<name>@<version>", …]`, which the
 * dependency maps elsewhere in the lock (`"<name>": "<range>"`) cannot match.
 * Read rather than parsed: bun's text lockfile is JSONC, and lockfile.ts
 * already works over it the same way.
 */
function readLockedToolVersions(root: string): Record<string, string> {
  const lockfilePath = join(root, 'bun.lock');
  if (!existsSync(lockfilePath)) {
    return {};
  }
  const lockfile = readFileSync(lockfilePath, 'utf8');
  const locked: Record<string, string> = {};
  for (const dependency of rootDevDependencies) {
    if (dependency.pinnedFromLockfile !== true) {
      continue;
    }
    const escaped = escapeRegex(dependency.name);
    const resolved = new RegExp(`"${escaped}":\\s*\\[\\s*"${escaped}@([^"]+)"`).exec(lockfile)?.[1];
    if (resolved !== undefined) {
      locked[dependency.name] = resolved;
    }
  }
  return locked;
}

function expectedPinnedVersion(policy: ToolPolicy, dependency: RequiredDependency): string {
  return policy.lockedToolVersions[dependency.name] ?? dependency.fallbackVersion;
}

function toolingPackageName(rootName: string | null): string {
  const name = rootName;
  const scope = name?.match(/^(@[^/]+)\//)?.[1];
  return scope ? `${scope}/tooling` : 'tooling';
}

function workspaceDependencyExpected(policy: ToolPolicy, dependency: RequiredDependency): boolean {
  return dependency.useWorkspaceRangeInCodebase === true && policy.isSmoothBricksCodebase;
}

async function resolveDependencyVersion(policy: ToolPolicy, dependency: RequiredDependency): Promise<string> {
  if (workspaceDependencyExpected(policy, dependency)) {
    return 'workspace:*';
  }
  if (dependency.pinnedFromLockfile === true) {
    return expectedPinnedVersion(policy, dependency);
  }
  if (!dependency.minimumVersion) {
    return dependency.fallbackVersion;
  }
  const latest = await fetchLatestPatchVersion(dependency.name, dependency.minimumVersion);
  return `${dependency.prefix ?? ''}${latest ?? stripRangePrefix(dependency.fallbackVersion)}`;
}

/**
 * Consumer monorepos pin `@smoothbricks/cli` to the latest stable release.
 * Prefer npm `dist-tags.latest`. Never write a running `-next` package version into
 * consumer manifests; never leave an older pin just because the running CLI is prerelease.
 */
async function resolvePublishedCliDependencyRange(configuredCliRange: string | null): Promise<string> {
  const latest = await fetchLatestStableVersion(cliPackageName);
  if (latest) {
    return `^${latest}`;
  }
  // Registry unavailable: if the running CLI is a stable publish, use it.
  if (!cliPackageVersion.includes('-')) {
    return `^${cliPackageVersion}`;
  }
  // Linked prerelease + offline down: keep an existing installable stable pin.
  if (configuredCliRange && parseVersion(configuredCliRange)) {
    return configuredCliRange;
  }
  // Last resort: caret of the prerelease base (0.10.5-next.0 → ^0.10.5) so manifests stay valid.
  const base = parseVersion(cliPackageVersion);
  return base ? `^${base.major}.${base.minor}.${base.patch}` : `^${cliPackageVersion}`;
}

async function fetchLatestStableVersion(packageName: string): Promise<string | null> {
  const url = `https://registry.npmjs.org/${encodeURIComponent(packageName).replace('%40', '@')}`;
  const response = await fetch(url, { headers: { accept: 'application/vnd.npm.install-v1+json' } });
  if (!response.ok) {
    return null;
  }
  const body: unknown = await response.json();
  if (!isRegistryPackument(body)) {
    return null;
  }
  const tagged = body['dist-tags']?.latest;
  if (typeof tagged === 'string' && parseVersion(tagged) && !tagged.includes('-')) {
    return tagged;
  }
  let latest: Version | null = null;
  for (const raw of Object.keys(body.versions)) {
    if (raw.includes('-')) {
      continue;
    }
    const version = parseVersion(raw);
    if (!version) {
      continue;
    }
    if (!latest || compareVersions(version, latest) > 0) {
      latest = version;
    }
  }
  return latest ? `${latest.major}.${latest.minor}.${latest.patch}` : null;
}

async function fetchLatestPatchVersion(packageName: string, minimumVersion: string): Promise<string | null> {
  const minorRange = sameMajorMinorRange(minimumVersion);
  const url = `https://registry.npmjs.org/${encodeURIComponent(packageName).replace('%40', '@')}`;
  const response = await fetch(url, { headers: { accept: 'application/vnd.npm.install-v1+json' } });
  if (!response.ok) {
    return null;
  }
  const body: unknown = await response.json();
  if (!isRegistryPackument(body)) {
    return null;
  }
  return latestVersionInSameMajorMinor(Object.keys(body.versions), minorRange);
}

function sameMajorMinorRange(minimumVersion: string): Version | null {
  return parseVersion(minimumVersion);
}

interface Version {
  major: number;
  minor: number;
  patch: number;
}

function parseVersion(version: string): Version | null {
  const match = /^[~^]?(\d+)\.(\d+)\.(\d+)(?:[-+].*)?$/.exec(version.trim());
  if (!match?.[1] || !match[2] || !match[3]) {
    return null;
  }
  return { major: Number(match[1]), minor: Number(match[2]), patch: Number(match[3]) };
}

function latestVersionInSameMajorMinor(versions: string[], minimum: Version | null): string | null {
  if (!minimum) {
    return null;
  }
  let latest: Version | null = null;
  for (const raw of versions) {
    const version = parseVersion(raw);
    if (!version || version.major !== minimum.major || version.minor !== minimum.minor) {
      continue;
    }
    if (compareVersions(version, minimum) < 0) {
      continue;
    }
    if (!latest || compareVersions(version, latest) > 0) {
      latest = version;
    }
  }
  return latest ? `${latest.major}.${latest.minor}.${latest.patch}` : null;
}

function compareVersions(left: Version, right: Version): number {
  return left.major - right.major || left.minor - right.minor || left.patch - right.patch;
}

function stripRangePrefix(version: string): string {
  return version.replace(/^[~^]/, '');
}

function addWorkspacePattern(pkg: PackageJson, pattern: string): boolean {
  const workspaces = pkg.workspaces;
  if (Array.isArray(workspaces)) {
    if (workspaces.includes(pattern)) {
      return false;
    }
    workspaces.push(pattern);
    return true;
  }
  if (workspaces && Array.isArray(workspaces.packages)) {
    if (workspaces.packages.includes(pattern)) {
      return false;
    }
    workspaces.packages.push(pattern);
    return true;
  }
  pkg.workspaces = ['packages/*', pattern];
  return true;
}

function hasWorkspacePattern(pkg: PackageJson, pattern: string): boolean {
  const workspaces = pkg.workspaces;
  if (Array.isArray(workspaces)) {
    return workspaces.includes(pattern);
  }
  return Boolean(workspaces && Array.isArray(workspaces.packages) && workspaces.packages.includes(pattern));
}
