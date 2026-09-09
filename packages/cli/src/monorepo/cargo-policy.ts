import { spawnSync } from 'node:child_process';
import { type Dirent, existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import typia from 'typia';
import { parsePackageJsonText } from '../lib/json.js';

interface CargoProfile {
  inherits?: string;
  incremental?: boolean;
  debug?: boolean | number | string;
  'split-debuginfo'?: string;
  'trim-paths'?: boolean | string | string[];
}

interface CargoManifest {
  workspace?: {
    members?: string[];
    exclude?: string[];
  };
  package?: {
    name?: string;
    workspace?: string | boolean;
  };
  profile?: Record<string, CargoProfile>;
  // Only the KEYS matter here: a workspace-hack is wired in by name, and every
  // dependency spelling — string, table, inherited — carries the same key.
  dependencies?: Record<string, unknown>;
  'dev-dependencies'?: Record<string, unknown>;
  'build-dependencies'?: Record<string, unknown>;
}

interface CargoConfigTarget {
  linker?: string;
  rustflags?: string[] | string;
}

interface CargoConfigEnvObject {
  value?: string;
  relative?: boolean;
  force?: boolean;
}

interface CargoConfig {
  build?: {
    'target-dir'?: string;
    rustflags?: string[] | string;
    incremental?: boolean;
  };
  target?: Record<string, CargoConfigTarget>;
  env?: Record<string, string | CargoConfigEnvObject>;
  resolver?: {
    'feature-unification'?: string;
  };
  unstable?: {
    'feature-unification'?: boolean;
  };
}

interface RustToolchainFile {
  toolchain?: {
    channel?: string;
  };
}

interface HakariConfig {
  'hakari-package'?: string;
}

interface LoadedManifest {
  path: string;
  directory: string;
  manifest: CargoManifest;
}

interface LoadedConfig {
  path: string;
  config: CargoConfig;
}

interface EffectiveProfile {
  incremental: boolean;
  debug: boolean | number | string;
}

const validateCargoManifest = typia.createValidate<CargoManifest>();
const validateCargoConfig = typia.createValidate<CargoConfig>();
const validateRustToolchainFile = typia.createValidate<RustToolchainFile>();
const validateHakariConfig = typia.createValidate<HakariConfig>();

/**
 * The one command this policy shells out to, behind a seam. `cargo hakari
 * verify` is the only authority on whether a generated workspace-hack still
 * unifies what the workspace resolves today; nothing in these files can answer
 * that. Tests supply their own so the policy stays runnable without the binary.
 */
export interface CargoHakariShell {
  run(directory: string, args: readonly string[]): { code: number; output: string; missing: boolean };
}

const defaultHakariShell: CargoHakariShell = {
  run(directory, args) {
    const result = spawnSync('cargo', ['hakari', ...args], { cwd: directory, encoding: 'utf8' });
    const output = `${result.stdout ?? ''}${result.stderr ?? ''}`.trim();
    // cargo itself exists; a missing subcommand is cargo's own error, not ENOENT.
    const missing =
      (result.error as NodeJS.ErrnoException | undefined)?.code === 'ENOENT' || output.includes('no such command');
    return { code: result.status ?? 1, output, missing };
  },
};

/** Cargo's nightly-only workspace feature unification, and the config that turns it on. */
const RESOLVER_UNIFICATION_BLOCK = [
  '# One feature resolution for the whole workspace. Without it every per-crate',
  '# cargo invocation resolves features for its own selection, so one shared',
  '# dependency is compiled once per selection and no warm target directory is',
  '# reused across crates. Requires the nightly channel the managed devenv',
  '# module pins; on a stable toolchain a cargo-hakari workspace-hack does the',
  '# same job. Both keys are needed: cargo ignores [resolver] without the',
  '# [unstable] opt-in.',
  '[unstable]',
  'feature-unification = true',
  '',
  '[resolver]',
  'feature-unification = "workspace"',
].join('\n');

const SKIPPED_DIRECTORY_NAMES = new Set([
  'node_modules',
  'target',
  '.git',
  'dist',
  '.devenv',
  '.direnv',
  '.venv',
  '.cache',
  '.nx',
]);

const ABSOLUTE_PATH = /\/(?:[A-Za-z0-9._~+@%,-]+(?:\/[A-Za-z0-9._~+@%,-]*)*)/g;
const CARGO_INCREMENTAL_ASSIGNMENT = /\bCARGO_INCREMENTAL\s*(?:=|:=|:)/;
const MANIFEST_DIRECTORY_MACRO = /env!\(\s*"CARGO_MANIFEST_DIR"\s*\)/g;
const CARGO_POLICY_IGNORE_MARKER = '# smoo-cargo-policy: ignore';
const BUILTIN_PROFILES: Record<string, EffectiveProfile> = {
  dev: { incremental: true, debug: 2 },
  test: { incremental: true, debug: 2 },
  release: { incremental: false, debug: 0 },
  bench: { incremental: false, debug: 0 },
};

const BUILTIN_INHERITS: Record<string, string> = {
  test: 'dev',
  bench: 'release',
};

function report(path: string, message: string): number {
  console.error(`${path}: ${message}`);
  return 1;
}

function isSkippedDirectory(name: string): boolean {
  return SKIPPED_DIRECTORY_NAMES.has(name) || name.startsWith('.');
}
function pathWithin(directory: string, path: string): boolean {
  const relativePath = relative(directory, path);
  return (
    relativePath === '' || (relativePath !== '..' && !relativePath.startsWith(`..${sep}`) && !isAbsolute(relativePath))
  );
}

function discoverIgnoredSubtrees(manifestPaths: string[]): string[] {
  const ignoredDirectories: string[] = [];
  for (const path of manifestPaths) {
    try {
      if (!readFileSync(path, 'utf8').includes(CARGO_POLICY_IGNORE_MARKER)) {
        continue;
      }
    } catch {
      continue;
    }
    const directory = dirname(path);
    if (ignoredDirectories.some((ignored) => pathWithin(ignored, directory))) {
      continue;
    }
    ignoredDirectories.push(directory);
    console.error(
      `${path}: Cargo cache policy skips this manifest and its subtree because of ${CARGO_POLICY_IGNORE_MARKER}; fix the exception by removing the marker when the tree should be linted.`,
    );
  }
  return ignoredDirectories;
}

function filterIgnoredPaths(paths: string[], ignoredDirectories: string[]): string[] {
  return paths.filter((path) => !ignoredDirectories.some((ignored) => pathWithin(ignored, path)));
}

/** Walk repository-owned files without descending into generated, dependency, or hidden trees. */
function discoverFiles(root: string, matches: (name: string) => boolean): string[] {
  const files: string[] = [];

  function visit(directory: string): void {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) {
        // `.cargo` is hidden by convention but its config is explicitly part of the policy.
        if (entry.name === '.cargo') {
          const configPath = join(path, 'config.toml');
          if (matches('config.toml') && existsSync(configPath)) {
            files.push(configPath);
          }
          continue;
        }
        if (!isSkippedDirectory(entry.name)) {
          visit(path);
        }
        continue;
      }
      if (entry.isFile() && matches(entry.name)) {
        files.push(path);
      }
    }
  }

  visit(root);
  files.sort();
  return files;
}

function discoverWorkflowFiles(root: string): string[] {
  const directory = join(root, '.github', 'workflows');
  let entries: Dirent[];
  try {
    entries = readdirSync(directory, { withFileTypes: true });
  } catch {
    return [];
  }
  return entries
    .filter((entry) => entry.isFile() && entry.name.endsWith('.yml'))
    .map((entry) => join(directory, entry.name))
    .sort();
}

function loadManifest(path: string): CargoManifest | null {
  try {
    const parsed: unknown = Bun.TOML.parse(readFileSync(path, 'utf8'));
    const validation = validateCargoManifest(parsed);
    if (!validation.success) {
      report(path, 'is not a valid Cargo manifest; fix the TOML shape before relying on Cargo cache policy');
      return null;
    }
    return validation.data;
  } catch {
    report(path, 'cannot be parsed as Cargo.toml; fix the TOML syntax before relying on Cargo cache policy');
    return null;
  }
}

function loadConfig(path: string): CargoConfig | null {
  try {
    const parsed: unknown = Bun.TOML.parse(readFileSync(path, 'utf8'));
    const validation = validateCargoConfig(parsed);
    if (!validation.success) {
      report(path, 'is not a valid Cargo config; fix the TOML shape before relying on Cargo cache policy');
      return null;
    }
    return validation.data;
  } catch {
    report(path, 'cannot be parsed as Cargo config; fix the TOML syntax before relying on Cargo cache policy');
    return null;
  }
}

function globRegExp(pattern: string): RegExp {
  const normalized = pattern.replaceAll('\\', '/').replace(/^\.\//, '').replace(/\/$/, '');
  let source = '^';
  for (let index = 0; index < normalized.length; index += 1) {
    const character = normalized[index];
    if (character === '*') {
      if (normalized[index + 1] === '*') {
        source += '.*';
        index += 1;
      } else {
        source += '[^/]*';
      }
    } else if (character === '?') {
      source += '[^/]';
    } else {
      source += character.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }
  }
  return new RegExp(`${source}$`);
}

function workspaceContains(root: LoadedManifest, member: LoadedManifest): boolean {
  if (root.directory === member.directory) {
    return false;
  }
  const memberPath = relative(root.directory, member.directory).split(sep).join('/');
  if (memberPath === '' || memberPath === '..' || memberPath.startsWith('../')) {
    return false;
  }
  const members = root.manifest.workspace?.members ?? [];
  const excludes = root.manifest.workspace?.exclude ?? [];
  if (!members.some((pattern) => globRegExp(pattern).test(memberPath))) {
    return false;
  }
  return !excludes.some((pattern) => globRegExp(pattern).test(memberPath));
}

function hasAncestorWorkspace(manifest: LoadedManifest, workspaceRoots: LoadedManifest[]): boolean {
  return workspaceRoots.some(
    (workspaceRoot) =>
      workspaceRoot.directory !== manifest.directory &&
      (() => {
        const path = relative(workspaceRoot.directory, manifest.directory);
        return path !== '' && path !== '..' && !path.startsWith(`..${sep}`) && !isAbsolute(path);
      })(),
  );
}

/**
 * The channel that will actually compile this repository.
 *
 * The managed devenv module wins when it exists: devenv resolves the toolchain
 * through rust-overlay and ignores `rust-toolchain.toml` unless
 * `languages.rust.toolchainFile` names it, so a rust-toolchain file beside that
 * module is decoration. Without the module, rustup's file is the answer. With
 * neither, stable is the safe verdict: it is the channel on which the nightly
 * resolver keys silently do nothing.
 */
function isNightlyToolchain(repositoryRoot: string, workspaceDirectory: string): boolean {
  const devenvModule = join(repositoryRoot, 'tooling/direnv/devenv.smoo.nix');
  if (existsSync(devenvModule)) {
    const text = readFileSync(devenvModule, 'utf8');
    const rustIndex = text.indexOf('languages.rust');
    const channel = rustIndex === -1 ? null : /channel\s*=\s*"([^"]+)"/.exec(text.slice(rustIndex))?.[1];
    return channel?.startsWith('nightly') === true;
  }
  for (const directory of [workspaceDirectory, repositoryRoot]) {
    const path = join(directory, 'rust-toolchain.toml');
    if (!existsSync(path)) {
      continue;
    }
    try {
      const validation = validateRustToolchainFile(Bun.TOML.parse(readFileSync(path, 'utf8')));
      return validation.success && validation.data.toolchain?.channel?.startsWith('nightly') === true;
    } catch {
      return false;
    }
  }
  return false;
}

/**
 * Cargo merges `.cargo/config.toml` from the invocation directory upward, with
 * the deepest file winning. Only the configs at or above the workspace root can
 * govern a command run there, so the effective value is the deepest of those.
 */
function effectiveConfigValue<T>(
  configs: LoadedConfig[],
  workspaceDirectory: string,
  read: (config: CargoConfig) => T | undefined,
): T | undefined {
  let deepest: { directory: string; value: T } | null = null;
  for (const loaded of configs) {
    const directory = dirname(dirname(loaded.path));
    if (!pathWithin(directory, workspaceDirectory)) {
      continue;
    }
    const value = read(loaded.config);
    if (value === undefined) {
      continue;
    }
    if (deepest === null || directory.length > deepest.directory.length) {
      deepest = { directory, value };
    }
  }
  return deepest?.value;
}

function workspaceCrates(root: LoadedManifest, manifests: LoadedManifest[]): LoadedManifest[] {
  const members = manifests.filter((manifest) => workspaceContains(root, manifest));
  return root.manifest.package === undefined ? members : [root, ...members];
}

const FEATURE_UNIFICATION_FIX = 'Run: smoo monorepo update';

/**
 * A workspace of two or more crates must resolve features ONCE for all of them.
 *
 * Without that, every per-crate cargo invocation resolves features for its own
 * selection, and one shared dependency is compiled once per selection: the same
 * `nx run cargo-test-<crate>` that should reuse a warm target directory rebuilds
 * its half of the graph instead. Cargo's own mechanism is nightly-only, so a
 * stable toolchain reaches the identical result through a cargo-hakari
 * workspace-hack — a generated crate that depends on the union of features, which
 * every member then depends on.
 *
 * One crate needs neither: there is only one selection to unify.
 */
function featureUnificationPolicy(
  repositoryRoot: string,
  workspaceRoots: LoadedManifest[],
  manifests: LoadedManifest[],
  configs: LoadedConfig[],
  shell: CargoHakariShell,
): number {
  let failures = 0;
  for (const root of workspaceRoots) {
    const crates = workspaceCrates(root, manifests);
    if (crates.length < 2) {
      continue;
    }
    const nightly = isNightlyToolchain(repositoryRoot, root.directory);
    const resolver = effectiveConfigValue(
      configs,
      root.directory,
      (config) => config.resolver?.['feature-unification'],
    );
    const unstable = effectiveConfigValue(
      configs,
      root.directory,
      (config) => config.unstable?.['feature-unification'],
    );
    if (nightly && resolver === 'workspace') {
      if (unstable !== true) {
        failures += report(
          join(root.directory, '.cargo/config.toml'),
          'sets [resolver] feature-unification = "workspace" without [unstable] feature-unification = true, ' +
            `so cargo ignores it and every crate still resolves features on its own. ${FEATURE_UNIFICATION_FIX}`,
        );
      }
      continue;
    }
    if (existsSync(join(root.directory, '.config/hakari.toml'))) {
      failures += hakariWorkspaceHackPolicy(root, crates, shell);
      continue;
    }
    failures += report(
      root.path,
      `workspace of ${crates.length} crates has no workspace-wide feature unification: ` +
        (nightly
          ? 'add [unstable] feature-unification = true and [resolver] feature-unification = "workspace" to .cargo/config.toml'
          : 'the [resolver] feature-unification key needs the nightly channel, so this stable toolchain needs a ' +
            'cargo-hakari workspace-hack (.config/hakari.toml)') +
        `. Every per-crate cargo invocation otherwise recompiles the shared graph for its own feature selection. ${FEATURE_UNIFICATION_FIX}`,
    );
  }
  return failures;
}

function hakariWorkspaceHackPolicy(root: LoadedManifest, crates: LoadedManifest[], shell: CargoHakariShell): number {
  const configPath = join(root.directory, '.config/hakari.toml');
  let hackName: string | undefined;
  try {
    const validation = validateHakariConfig(Bun.TOML.parse(readFileSync(configPath, 'utf8')));
    hackName = validation.success ? validation.data['hakari-package'] : undefined;
  } catch {
    hackName = undefined;
  }
  if (hackName === undefined || hackName.length === 0) {
    return report(configPath, `must declare hakari-package = "<crate>". ${FEATURE_UNIFICATION_FIX}`);
  }
  const hack = crates.find((crate) => crate.manifest.package?.name === hackName);
  if (hack === undefined) {
    return report(
      configPath,
      `names hakari-package "${hackName}", which is not a member of this workspace. ${FEATURE_UNIFICATION_FIX}`,
    );
  }
  const unwired = crates
    .filter((crate) => crate !== hack)
    .filter(
      (crate) =>
        ![crate.manifest.dependencies, crate.manifest['dev-dependencies'], crate.manifest['build-dependencies']].some(
          (table) => table !== undefined && hackName in table,
        ),
    )
    .map((crate) => crate.manifest.package?.name ?? crate.path);
  if (unwired.length > 0) {
    return report(
      root.path,
      `these crates do not depend on the "${hackName}" workspace-hack, so their features are still resolved ` +
        `separately: ${unwired.join(', ')}. ${FEATURE_UNIFICATION_FIX}`,
    );
  }
  const verify = shell.run(root.directory, ['verify']);
  if (verify.missing) {
    return report(
      configPath,
      'needs cargo-hakari, which is not installed; the managed devenv module provides it — reload the shell (direnv reload).',
    );
  }
  if (verify.code !== 0) {
    return report(
      root.path,
      `cargo hakari verify rejected the workspace-hack: ${verify.output || 'no output'}. ${FEATURE_UNIFICATION_FIX}`,
    );
  }
  return 0;
}

function isWorkspaceMember(manifest: LoadedManifest, workspaceRoots: LoadedManifest[]): boolean {
  if (manifest.manifest.package?.workspace !== undefined) {
    return true;
  }
  return workspaceRoots.some((workspaceRoot) => workspaceContains(workspaceRoot, manifest));
}

function resolveProfile(manifest: CargoManifest, name: string): EffectiveProfile {
  const resolving = new Set<string>();

  function visit(profileName: string): EffectiveProfile {
    if (resolving.has(profileName)) {
      return BUILTIN_PROFILES[profileName] ?? { incremental: true, debug: 2 };
    }
    resolving.add(profileName);
    const profile = manifest.profile?.[profileName];
    const inheritedName = profile?.inherits ?? BUILTIN_INHERITS[profileName];
    const inherited = inheritedName
      ? visit(inheritedName)
      : (BUILTIN_PROFILES[profileName] ?? { incremental: true, debug: 2 });
    const effective = {
      incremental: profile?.incremental ?? inherited.incremental,
      debug: profile?.debug ?? inherited.debug,
    };
    resolving.delete(profileName);
    return effective;
  }

  return visit(name);
}

function carriesDebuginfo(debug: EffectiveProfile['debug']): boolean {
  return debug !== 0 && debug !== false && debug !== 'none';
}

function reportManifestPolicy(manifests: LoadedManifest[], workspaceRoots: LoadedManifest[]): number {
  let failures = 0;
  for (const loaded of manifests) {
    const { manifest } = loaded;
    const ancestorWorkspace = hasAncestorWorkspace(loaded, workspaceRoots);
    const effectiveRoot = manifest.workspace !== undefined || (manifest.package !== undefined && !ancestorWorkspace);
    if (effectiveRoot && manifest.profile?.test?.incremental !== false) {
      failures += report(
        loaded.path,
        'the effective Cargo workspace root is missing [profile.test] incremental = false; Cargo test inherits the incremental dev profile, and only non-incremental units can be shared across cowshed workspaces. Fix it by adding [profile.test] with incremental = false.',
      );
    }

    if (manifest.profile !== undefined) {
      if (isWorkspaceMember(loaded, workspaceRoots)) {
        failures += report(
          loaded.path,
          'contains [profile.*] settings in a workspace member, but Cargo ignores profile tables outside the effective workspace root. Fix it by moving the profile table to the workspace root.',
        );
      }
      for (const profileName of Object.keys(manifest.profile)) {
        const effective = resolveProfile(manifest, profileName);
        if (effective.incremental === false && carriesDebuginfo(effective.debug)) {
          failures += report(
            loaded.path,
            `profile ${profileName} is cacheable (effective incremental = false) but carries debuginfo (effective debug = ${String(effective.debug)}). A workspace can hit a sibling's artifact and inherit the sibling's absolute source paths in debuginfo and panic output; fix it with debug = 0 or leave the profile incremental.`,
          );
        }
      }
    }
  }
  return failures;
}

function resolvedPathIsOutsideRoot(root: string, path: string): boolean {
  const resolved = resolve(root, path);
  const relativePath = relative(root, resolved);
  return (
    relativePath !== '' && (relativePath === '..' || relativePath.startsWith(`..${sep}`) || isAbsolute(relativePath))
  );
}

function absolutePathsIn(value: string): string[] {
  return [...value.matchAll(ABSOLUTE_PATH)].map((match) => match[0]);
}

function rustflags(config: CargoConfig): string[] {
  const values: string[] = [];
  if (config.build?.rustflags !== undefined) {
    values.push(...(Array.isArray(config.build.rustflags) ? config.build.rustflags : [config.build.rustflags]));
  }
  for (const target of Object.values(config.target ?? {})) {
    if (target.rustflags !== undefined) {
      values.push(...(Array.isArray(target.rustflags) ? target.rustflags : [target.rustflags]));
    }
  }
  return values;
}

function configPolicy(loaded: LoadedConfig, repositoryRoot: string): number {
  let failures = 0;
  const { config, path } = loaded;
  const targetDirectory = config.build?.['target-dir'];
  if (targetDirectory !== undefined && resolvedPathIsOutsideRoot(repositoryRoot, targetDirectory)) {
    failures += report(
      path,
      `build.target-dir resolves outside the repository root (${targetDirectory}); SCCACHE_BASEDIR_CWD normalises only paths at or below the build's working directory. Fix it by using a repository-relative target directory.`,
    );
  }

  for (const [targetName, target] of Object.entries(config.target ?? {})) {
    if (
      target.linker !== undefined &&
      isAbsolute(target.linker) &&
      resolvedPathIsOutsideRoot(repositoryRoot, target.linker)
    ) {
      failures += report(
        path,
        `[target.${targetName}] linker is an absolute path outside the repository root (${target.linker}); SCCACHE_BASEDIR_CWD normalises only paths at or below the build's working directory. Fix it by using a repository-relative linker.`,
      );
    }
  }

  for (const value of rustflags(config)) {
    for (const absolutePath of absolutePathsIn(value)) {
      failures += report(
        path,
        `rustflags contains the absolute path ${absolutePath}; SCCACHE_BASEDIR_CWD normalises only paths at or below the build's working directory, so this absolute path pins the sccache key to one machine. Fix it by using a repository-relative rustflag or removing the absolute path.`,
      );
    }
  }

  for (const [name, value] of Object.entries(config.env ?? {})) {
    const text = typeof value === 'string' ? value : value.value;
    if (text !== undefined && isAbsolute(text) && resolvedPathIsOutsideRoot(repositoryRoot, text)) {
      failures += report(
        path,
        `[env] ${name} is an absolute path outside the repository root (${text}); SCCACHE_BASEDIR_CWD normalises only paths at or below the build's working directory. Fix it by using a repository-relative environment value.`,
      );
    }
  }
  return failures;
}

function packageScriptsSetCargoIncremental(path: string): boolean {
  try {
    const packageJson = parsePackageJsonText(readFileSync(path, 'utf8'));
    return Object.values(packageJson?.scripts ?? {}).some((script) => CARGO_INCREMENTAL_ASSIGNMENT.test(script));
  } catch {
    return false;
  }
}

function reportCargoIncremental(path: string): number {
  return report(
    path,
    "sets CARGO_INCREMENTAL in committed configuration. Fix: leave CARGO_INCREMENTAL unset. CARGO_INCREMENTAL=1 hard-fails the sccache wrapper at Cargo's version probe, while CARGO_INCREMENTAL=0 surrenders dev incrementality for nothing.",
  );
}
function cargoIncrementalPolicy(
  repositoryRoot: string,
  configs: LoadedConfig[],
  packageJsonPaths: string[],
  justfilePaths: string[],
  ignoredDirectories: string[],
): number {
  let failures = 0;
  const reported = new Set<string>();
  for (const loaded of configs) {
    if (loaded.config.env?.CARGO_INCREMENTAL !== undefined) {
      reported.add(loaded.path);
      failures += reportCargoIncremental(loaded.path);
    }
  }

  const rootPackage = join(repositoryRoot, 'package.json');
  const packagePaths = filterIgnoredPaths([...packageJsonPaths, rootPackage], ignoredDirectories);
  for (const path of packagePaths) {
    if (!reported.has(path) && packageScriptsSetCargoIncremental(path)) {
      reported.add(path);
      failures += reportCargoIncremental(path);
    }
  }

  const workflowPaths = filterIgnoredPaths(discoverWorkflowFiles(repositoryRoot), ignoredDirectories);
  const committedConfigurationPaths = filterIgnoredPaths(
    [...justfilePaths, join(repositoryRoot, 'tooling/direnv/devenv.nix')],
    ignoredDirectories,
  );
  for (const path of [...committedConfigurationPaths, ...workflowPaths]) {
    if (reported.has(path)) {
      continue;
    }
    let text: string;
    try {
      text = readFileSync(path, 'utf8');
    } catch {
      continue;
    }
    if (CARGO_INCREMENTAL_ASSIGNMENT.test(text)) {
      reported.add(path);
      failures += reportCargoIncremental(path);
    }
  }
  return failures;
}

/// Blank out comments while preserving every newline, so a match's line number is unchanged.
///
/// The advisory exists to find the macro COMPILED into a crate, because rustc records those as
/// `# env-dep:` and the patched sccache never normalises them. An occurrence inside a comment
/// compiles to nothing, and this repository documents the sccache behaviour by naming the macro
/// in prose — so a raw text scan reports the documentation as the defect it describes.
///
/// String and raw-string states are tracked because a literal may legitimately contain `//`
/// or `/*`; mistaking one for a comment would blank real code and silently lose a finding.
function stripRustComments(text: string): string {
  const out = Array.from(text);
  let index = 0;
  const blank = (from: number, to: number): void => {
    for (let cursor = from; cursor < to; cursor += 1) {
      if (out[cursor] !== '\n') {
        out[cursor] = ' ';
      }
    }
  };
  while (index < text.length) {
    const character = text[index];
    if (character === '"') {
      index += 1;
      while (index < text.length && text[index] !== '"') {
        index += text[index] === '\\' ? 2 : 1;
      }
      index += 1;
      continue;
    }
    if (character === 'r' && (text[index + 1] === '"' || text[index + 1] === '#')) {
      let hashes = 0;
      while (text[index + 1 + hashes] === '#') {
        hashes += 1;
      }
      if (text[index + 1 + hashes] === '"') {
        const terminator = `"${'#'.repeat(hashes)}`;
        const end = text.indexOf(terminator, index + 2 + hashes);
        index = end === -1 ? text.length : end + terminator.length;
        continue;
      }
    }
    if (character === '/' && text[index + 1] === '/') {
      const end = text.indexOf('\n', index);
      const stop = end === -1 ? text.length : end;
      blank(index, stop);
      index = stop;
      continue;
    }
    if (character === '/' && text[index + 1] === '*') {
      let depth = 1;
      let cursor = index + 2;
      while (cursor < text.length && depth > 0) {
        if (text[cursor] === '/' && text[cursor + 1] === '*') {
          depth += 1;
          cursor += 2;
        } else if (text[cursor] === '*' && text[cursor + 1] === '/') {
          depth -= 1;
          cursor += 2;
        } else {
          cursor += 1;
        }
      }
      blank(index, cursor);
      index = cursor;
      continue;
    }
    index += 1;
  }
  return out.join('');
}

function reportManifestDirectoryAdvisories(repositoryRoot: string, ignoredDirectories: string[]): void {
  const advisories: Array<{ path: string; line: number }> = [];
  for (const path of filterIgnoredPaths(
    discoverFiles(repositoryRoot, (name) => name.endsWith('.rs')),
    ignoredDirectories,
  )) {
    const relativePath = relative(repositoryRoot, path).split(sep);
    const fileName = basename(path);
    if (relativePath.includes('tests') || /(?:_test|_tests)\.rs$/.test(fileName)) {
      continue;
    }
    let text: string;
    try {
      text = readFileSync(path, 'utf8');
    } catch {
      continue;
    }
    const code = stripRustComments(text);
    for (const match of code.matchAll(MANIFEST_DIRECTORY_MACRO)) {
      const line = text.slice(0, match.index ?? 0).split('\n').length;
      advisories.push({ path, line });
    }
  }
  if (advisories.length === 0) {
    return;
  }
  console.error('Cargo cache policy advisories (informational only; these do not fail the check):');
  for (const advisory of advisories) {
    console.error(
      `${advisory.path}:${advisory.line}: env!("CARGO_MANIFEST_DIR") is not path-stable; the patched sccache never normalises env-dep values, so this crate and everything downstream misses on every workspace at a new mount path. Fix it by removing the macro from production code or replacing it with a path-stable value.`,
    );
  }
}

export interface CargoPolicyOptions {
  /** Injected in tests; production shells out to the real `cargo hakari`. */
  shell?: CargoHakariShell;
}

export function validateCargoCachePolicy(root: string, options: CargoPolicyOptions = {}): number {
  const repositoryRoot = resolve(root);
  const discoveredManifestPaths = discoverFiles(repositoryRoot, (name) => name === 'Cargo.toml');
  const ignoredDirectories = discoverIgnoredSubtrees(discoveredManifestPaths);
  const manifestPaths = filterIgnoredPaths(discoveredManifestPaths, ignoredDirectories);
  const configPaths = filterIgnoredPaths(
    discoverFiles(repositoryRoot, (name) => name === 'config.toml').filter(
      (path) => basename(dirname(path)) === '.cargo',
    ),
    ignoredDirectories,
  );
  const packageJsonPaths = filterIgnoredPaths(
    discoverFiles(repositoryRoot, (name) => name === 'package.json'),
    ignoredDirectories,
  );
  const justfilePaths = filterIgnoredPaths(
    discoverFiles(repositoryRoot, (name) => name === 'Justfile' || name === 'justfile'),
    ignoredDirectories,
  );

  let failures = 0;
  const manifests: LoadedManifest[] = [];
  for (const path of manifestPaths) {
    const manifest = loadManifest(path);
    if (manifest === null) {
      failures += 1;
      continue;
    }
    manifests.push({ path, directory: dirname(path), manifest });
  }

  const configs: LoadedConfig[] = [];
  for (const path of configPaths) {
    const config = loadConfig(path);
    if (config === null) {
      failures += 1;
      continue;
    }
    configs.push({ path, config });
  }

  const workspaceRoots = manifests.filter((loaded) => loaded.manifest.workspace !== undefined);
  failures += reportManifestPolicy(manifests, workspaceRoots);
  failures += featureUnificationPolicy(
    repositoryRoot,
    workspaceRoots,
    manifests,
    configs,
    options.shell ?? defaultHakariShell,
  );
  failures += cargoIncrementalPolicy(repositoryRoot, configs, packageJsonPaths, justfilePaths, ignoredDirectories);
  for (const config of configs) {
    failures += configPolicy(config, repositoryRoot);
  }
  reportManifestDirectoryAdvisories(repositoryRoot, ignoredDirectories);
  return failures;
}

/**
 * The fix `smoo monorepo update` applies for the policy above: write the
 * nightly resolver configuration, or generate and wire the workspace-hack a
 * stable toolchain needs. Both are idempotent — the second run of either does
 * nothing — because update runs on every repository, not only broken ones.
 */
export function applyCargoFeatureUnification(root: string, options: CargoPolicyOptions = {}): void {
  const repositoryRoot = resolve(root);
  const shell = options.shell ?? defaultHakariShell;
  const discoveredManifestPaths = discoverFiles(repositoryRoot, (name) => name === 'Cargo.toml');
  const manifestPaths = filterIgnoredPaths(discoveredManifestPaths, discoverIgnoredSubtrees(discoveredManifestPaths));
  const manifests: LoadedManifest[] = [];
  for (const path of manifestPaths) {
    const manifest = loadManifest(path);
    if (manifest !== null) {
      manifests.push({ path, directory: dirname(path), manifest });
    }
  }
  for (const root of manifests.filter((loaded) => loaded.manifest.workspace !== undefined)) {
    if (workspaceCrates(root, manifests).length < 2) {
      continue;
    }
    if (isNightlyToolchain(repositoryRoot, root.directory)) {
      writeResolverUnification(root.directory);
      continue;
    }
    const hakariConfig = join(root.directory, '.config/hakari.toml');
    const relativeRoot = relative(repositoryRoot, root.directory) || '.';
    if (!existsSync(hakariConfig)) {
      console.log(`generating     cargo workspace-hack in ${relativeRoot} (cargo hakari init workspace-hack)`);
      runHakari(shell, root.directory, ['init', 'workspace-hack']);
    }
    console.log(`unifying       cargo features in ${relativeRoot} (cargo hakari generate, manage-deps)`);
    runHakari(shell, root.directory, ['generate']);
    runHakari(shell, root.directory, ['manage-deps', '--yes']);
  }
}

function runHakari(shell: CargoHakariShell, directory: string, args: readonly string[]): void {
  const result = shell.run(directory, args);
  if (result.missing) {
    console.error(
      `cargo hakari is not installed, so the workspace-hack in ${directory} was not updated; the managed devenv module provides it — reload the shell (direnv reload).`,
    );
    return;
  }
  if (result.code !== 0) {
    console.error(`cargo hakari ${args.join(' ')} failed in ${directory}: ${result.output || 'no output'}`);
  }
}

/**
 * Append the two keys, never rewrite the file. A `.cargo/config.toml` carries
 * linkers, env and target settings whose ORDER and comments are load-bearing;
 * re-emitting parsed TOML would lose both. If either table already exists with
 * some other content, appending a second one is invalid TOML — so that case is
 * reported for a human instead of being guessed at.
 */
function writeResolverUnification(workspaceDirectory: string): void {
  const path = join(workspaceDirectory, '.cargo/config.toml');
  const existing = existsSync(path) ? readFileSync(path, 'utf8') : null;
  if (existing === null) {
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, `${RESOLVER_UNIFICATION_BLOCK}\n`);
    console.log(`writing        ${path}`);
    return;
  }
  const parsed = loadConfig(path);
  if (parsed === null) {
    return;
  }
  if (parsed.resolver?.['feature-unification'] === 'workspace' && parsed.unstable?.['feature-unification'] === true) {
    return;
  }
  if (parsed.resolver !== undefined || parsed.unstable !== undefined) {
    console.error(
      `${path}: already declares [resolver] or [unstable]; add feature-unification to the existing tables by hand:\n${RESOLVER_UNIFICATION_BLOCK}`,
    );
    return;
  }
  writeFileSync(path, `${existing.replace(/\n*$/, '\n')}\n${RESOLVER_UNIFICATION_BLOCK}\n`);
  console.log(`updating       ${path}`);
}
