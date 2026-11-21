import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import type { PluginConfiguration } from 'nx/src/config/nx-json.js';
import type { Tree } from 'nx/src/devkit-exports.js';
import { readJson, writeJson } from 'nx/src/devkit-exports.js';
import typia from 'typia';

import { CARGO_TEST_TARGET } from './cargo-workspace.js';
import { cargoCrossTestTargetName } from './cross-check-policy.js';
import { writeJsonFile } from './managed-files/managed-format.js';

export const MANAGED_FILES_SYNC_GENERATOR = '@smoothbricks/nx-plugin:managed-files';
const validatePlugins = typia.createValidate<PluginConfiguration[]>();

export interface NxPolicyIssue {
  path: string;
  message: string;
}

export const BUILD_OUTPUT_DEPENDENCIES = [
  '*-js',
  '*-web',
  '*-html',
  '*-css',
  '*-android',
  '*-native',
  '*-napi',
  '*-bun',
  '*-wasm',
] as const;

export {
  LINUX_PLATFORM_TARGET_GLOBS,
  MACOS_PLATFORM_TARGET_GLOBS,
  PLATFORM_TARGET_GLOBS,
  RELEASE_CONFIGURATION,
} from './platform-targets.js';

const buildOutputTargetSuffixes = BUILD_OUTPUT_DEPENDENCIES.map((glob) => glob.slice(1));

/**
 * The glob list above, spelled as a target name: a target whose name ends in a
 * reserved family suffix EMITS that kind of artifact. Inference collects
 * exactly these names into the `nx:noop` build aggregate, which makes this the
 * same rule that decides whether a hand-declared aggregate names an edge
 * inference could have produced on its own — one definition, two readers, so
 * the removal policy cannot drift away from what it judges redundant.
 */
export function isBuildOutputTargetName(name: string): boolean {
  return buildOutputTargetSuffixes.some((suffix) => name.endsWith(suffix));
}

/**
 * The runners under the `test` namespace: `test`, anything under `test-`/`test:`, and the plugin's own
 * `cargo-test-<crate>` namespace. Inference gives a declared one `test`'s build prerequisites, which a suite named for
 * what it tests (`launcher-test`) never gets: it states its own producers.
 */
export function isTestNamespaceTargetName(name: string): boolean {
  return (
    name === 'test' || name.startsWith('test-') || name.startsWith('test:') || name.startsWith(`${CARGO_TEST_TARGET}-`)
  );
}

/**
 * The plugin's foreign-triple test namespace, `cargo-cross-test-archive-<triple>` and `cargo-cross-test-<triple>`. It
 * sits beside `cargo-test-<crate>`, not under it, so a triple never reads as a crate.
 */
const CARGO_CROSS_TEST_NAMESPACE = cargoCrossTestTargetName('');

/**
 * A target whose job is to RUN tests or to compile the binaries they run: the `test` namespace above, any
 * `<suite>-test` (the `cargo-test` aggregate, `napi-test`, `launcher-test`) and the cross namespace. Deliberately
 * name-based: the aggregate is assembled from declared AND inferred target names, and a declaration carries no marker
 * saying "this executes rather than emits".
 *
 * A runner collides with the families by construction whenever its name ends in one: a crate named `…-js` puts
 * `cargo-test-<crate>` in the family, a triple ending `-android` does the same to `cargo-cross-test-<triple>`, and a
 * bun suite named `test-bun` reads as "tool=test, output=bun". All would drag a test suite into `build`, so the rule
 * is the CLASS, not one namespace.
 */
export function isTestRunnerTargetName(name: string): boolean {
  return isTestNamespaceTargetName(name) || name.endsWith('-test') || name.startsWith(CARGO_CROSS_TEST_NAMESPACE);
}

const nxJsTypescriptPlugin = '@nx/js/typescript';
const smoothBricksNxPlugin = '@smoothbricks/nx-plugin';
/**
 * The executor of the retired `clean` target, which deleted declared outputs file by file. Build state is removed whole
 * instead (cowshed rm, mbx gc), so a `targetDefaults.clean` naming it is refused and removed.
 */
const retiredCleanOutputsExecutor = '@smoothbricks/nx-plugin:clean-outputs';
const ciWorkflowNamedInput = ['{workspaceRoot}/.github/workflows/ci.yml'];
const inferredTestTypecheckTarget = 'typecheck-tests';
/**
 * Markdown is documentation: no inferred task reads it, so a README or spec edit must not re-key every task of the
 * project and, through `^default`/`^production`, of every project depending on it. A target that does read Markdown
 * (a docs site build, a binary that `include_str!`s a shipped skill) names those files in its own inputs.
 */
const MARKDOWN_EXCLUSION = '!{projectRoot}/**/*.md';
const defaultProductionNamedInput = [
  '{projectRoot}/src/**/*',
  '{projectRoot}/package.json',
  '!{projectRoot}/**/__tests__/**',
  '!{projectRoot}/**/*.test.*',
  '!{projectRoot}/**/*.spec.*',
  MARKDOWN_EXCLUSION,
];
const impreciseProductionInputs = new Set(['default', '{projectRoot}/**/*', '{projectRoot}/**']);

/**
 * Every checkout keeps its Nx state in its own `.nx` (specs/cowshed/16_build_volumes.md, "Every checkout has exactly
 * one Nx state"). With no `NX_*_DIRECTORY` set, as in a host shell, the pinned Nx 24.0.6 otherwise moves the task cache
 * and its database to a per-user `~/.nx/<id>` keyed by the repository's remote (`utils/cache-directory.js`,
 * `computeSharedDataLocation`), shared by every checkout of the repository. A configured `cacheDirectory` keeps both
 * local: the cache here, the task database in Nx's default `.nx/workspace-data`. nx.json has no key for the latter.
 */
export const CHECKOUT_NX_CACHE_DIRECTORY = '.nx/cache';

/**
 * Check workspace config policy on an in-memory nx.json object.
 */
export function checkWorkspaceConfig(nxJson: Record<string, unknown>): NxPolicyIssue[] {
  const issues: NxPolicyIssue[] = [];

  // Colon target defaults
  const targetDefaults = recordProperty(nxJson, 'targetDefaults');
  if (nxJson.targetDefaults !== undefined && targetDefaults === null) {
    issues.push({ path: 'nx.json', message: 'targetDefaults must be an object; smoo does not rewrite it' });
  }
  if (targetDefaults) {
    for (const targetName of Object.keys(targetDefaults)) {
      if (targetName.includes(':')) {
        issues.push({
          path: 'nx.json',
          message:
            `targetDefaults.${targetName} must not use colon target names. ` +
            'Nx CLI syntax already uses project:target:configuration, so smoo Nx target names must be unambiguous tool-output names.',
        });
      }
      if (targetName === inferredTestTypecheckTarget) {
        issues.push({
          path: 'nx.json',
          message:
            `targetDefaults.${inferredTestTypecheckTarget} must not be configured. ` +
            'Per-project inference selects current library or build outputs before test typechecking.',
        });
      }
      if (retiredCleanEntries(targetDefaults, targetName).length !== 0) {
        issues.push({
          path: 'nx.json',
          message: `targetDefaults.clean names the retired ${retiredCleanOutputsExecutor}; run smoo monorepo update to remove it`,
        });
      }
      if (targetDefault(nxJson, targetName).form === 'malformed') {
        issues.push({ path: 'nx.json', message: malformedTargetDefaultMessage(targetName) });
      }
    }
  }

  // Build target default
  validateBuildTargetDefault(nxJson, 'nx.json', issues);

  // Test target default: dependsOn is inferred per project (cargo-test vs ^build/build)
  validateTestTargetDefault(nxJson, 'nx.json', issues);

  validateLintTargetDefault(nxJson, 'nx.json', issues);

  // Named input defaults
  validateNamedInputDefaults(nxJson, 'nx.json', issues);

  if (nxJson.cacheDirectory !== CHECKOUT_NX_CACHE_DIRECTORY) {
    issues.push({
      path: 'nx.json',
      message:
        `cacheDirectory must be "${CHECKOUT_NX_CACHE_DIRECTORY}". ` +
        "Without it Nx keeps the task cache and its database in a per-user ~/.nx/<id> shared by every checkout, not in the checkout's own .nx.",
    });
  }

  // Plugin configuration
  const plugins = validatePlugins(nxJson.plugins === undefined ? [] : nxJson.plugins);
  if (!plugins.success) {
    issues.push({
      path: 'nx.json',
      message: `plugins is malformed: ${plugins.errors.map(({ path, expected }) => `${path}: expected ${expected}`).join('; ')}`,
    });
  } else {
    if (plugins.data.some((plugin) => nxPluginName(plugin) === nxJsTypescriptPlugin)) {
      issues.push({
        path: 'nx.json',
        message:
          `plugins must not configure ${nxJsTypescriptPlugin}. ` +
          'Smoo owns transformer-aware TypeScript targets; Nx native inference only supports tsc/tsgo.',
      });
    }
    if (!plugins.data.some((plugin) => nxPluginName(plugin) === smoothBricksNxPlugin)) {
      issues.push({
        path: 'nx.json',
        message:
          `plugins must include ${smoothBricksNxPlugin}. ` +
          'Smoo relies on this plugin to infer convention targets that official Nx does not provide, including typecheck-tests, non-TypeScript build-tool targets, and aggregate build/lint targets.',
      });
    }
  }

  return issues;
}

/**
 * Apply workspace config policy to an in-memory nx.json object.
 * Mutates in place, returns whether anything changed.
 */
export function applyWorkspaceConfig(nxJson: Record<string, unknown>): boolean {
  let changed = setStringProperty(nxJson, 'cacheDirectory', CHECKOUT_NX_CACHE_DIRECTORY);
  changed = removeDisallowedTargetDefaults(nxJson) || changed;
  changed = applyBuildTargetDefault(nxJson) || changed;
  changed = applyTestTargetDefault(nxJson) || changed;
  changed = applyLintTargetDefault(nxJson) || changed;
  changed = applyNamedInputDefaults(nxJson) || changed;
  const plugins = validatePlugins(nxJson.plugins === undefined ? [] : nxJson.plugins);
  if (plugins.success) {
    const nextPlugins = plugins.data.filter((plugin) => nxPluginName(plugin) !== nxJsTypescriptPlugin);
    if (!nextPlugins.some((plugin) => nxPluginName(plugin) === smoothBricksNxPlugin)) {
      nextPlugins.push(smoothBricksNxPlugin);
    }
    if (JSON.stringify(plugins.data) !== JSON.stringify(nextPlugins)) {
      nxJson.plugins = nextPlugins;
      changed = true;
    }
  }
  const sync = recordProperty(nxJson, 'sync') ?? {};
  const generators = Array.isArray(sync.globalGenerators) ? sync.globalGenerators : [];
  if (!generators.includes(MANAGED_FILES_SYNC_GENERATOR)) {
    nxJson.sync = { ...sync, globalGenerators: [...generators, MANAGED_FILES_SYNC_GENERATOR] };
    changed = true;
  }
  return changed;
}

/**
 * Check workspace config policy using an Nx Tree.
 */
export function checkWorkspaceConfigTree(tree: Tree): NxPolicyIssue[] {
  if (!tree.exists('nx.json')) {
    return [{ path: 'nx.json', message: 'nx.json not found' }];
  }
  return checkWorkspaceConfig(readJson(tree, 'nx.json'));
}

/**
 * Apply workspace config policy using an Nx Tree.
 * Returns whether anything changed.
 */
export function applyWorkspaceConfigTree(tree: Tree): boolean {
  if (!tree.exists('nx.json')) {
    return false;
  }
  const nxJson = readJson<Record<string, unknown>>(tree, 'nx.json');
  const changed = applyWorkspaceConfig(nxJson);
  if (changed) writeJson(tree, 'nx.json', nxJson);
  return changed;
}

/**
 * Check nx.json workspace config policy, returns issues found.
 */
export function checkWorkspaceConfigPolicy(root: string): NxPolicyIssue[] {
  const nxJsonPath = join(root, 'nx.json');
  const nxJson = readJsonObject(nxJsonPath);
  if (!nxJson) {
    return [{ path: nxJsonPath, message: 'nx.json not found or invalid' }];
  }
  const coreIssues = checkWorkspaceConfig(nxJson);
  // Remap 'nx.json' paths to absolute paths for filesystem callers
  return coreIssues.map((issue) => ({
    ...issue,
    path: issue.path === 'nx.json' ? nxJsonPath : issue.path,
  }));
}

/**
 * Fix nx.json workspace config policy. Returns whether anything changed.
 */
export function applyWorkspaceConfigPolicy(root: string): boolean {
  const nxJsonPath = join(root, 'nx.json');
  const nxJson = readJsonObject(nxJsonPath);
  if (!nxJson) {
    return false;
  }
  const changed = applyWorkspaceConfig(nxJson);
  if (changed) {
    writeJsonFile(nxJsonPath, nxJson);
  }
  return changed;
}

function validateBuildTargetDefault(
  nxJson: Record<string, unknown>,
  nxJsonPath: string,
  issues: NxPolicyIssue[],
): void {
  const build = targetDefault(nxJson, 'build');
  if (build.form === 'malformed') {
    return;
  }
  const entries = build.form === 'absent' ? [] : build.entries;
  if (catchAllValue(entries, 'cache') !== true) {
    issues.push({
      path: nxJsonPath,
      message:
        build.form === 'list'
          ? 'targetDefaults.build.cache must be true in an entry without a filter'
          : 'targetDefaults.build.cache must be true',
    });
  }
  if (entries.some((entry) => 'filter' in entry && 'cache' in entry && entry.cache !== true)) {
    issues.push({ path: nxJsonPath, message: 'targetDefaults.build.cache must be true in every entry that sets it' });
  }
  // dependsOn and outputs are both owned by the inference plugin, and Nx gives
  // targetDefaults precedence over inferred values, so anything set here lands
  // on every project regardless of what inference knows about that project.
  //
  // dependsOn: the inferred aggregate carries the output families PLUS this
  // host's platform-suffixed binary targets; a static list here would clobber
  // the host-aware edge.
  //
  // outputs: `build` is an `nx:noop` aggregate that runs no command and writes
  // no file — every artifact under dist belongs to the concrete target that
  // emitted it. A workspace-wide `{projectRoot}/dist` makes the aggregate claim
  // its children's trees, which caches those bytes a second time under the
  // aggregate's hash and, worse, makes `github-ci nx-run-many
  // --collect-outputs` attribute to `build` every file that happens to sit
  // under dist: artifacts of platform targets the collect deliberately
  // excluded, and stale artifacts of a target that did not run at all. The
  // publish workflow then applies two collected trees that both claim the same
  // platform binary and `apply-outputs` rejects the overlap. A project whose
  // `build` genuinely emits files declares its own outputs in package.json,
  // where the claim is scoped to the project that can honour it.
  for (const property of ['dependsOn', 'outputs'] as const) {
    if (entries.some((entry) => property in entry)) {
      issues.push({
        path: nxJsonPath,
        message: `targetDefaults.build.${property} must not be set; @smoothbricks/nx-plugin infers the build aggregate`,
      });
    }
  }
}

function validateNamedInputDefaults(
  nxJson: Record<string, unknown>,
  nxJsonPath: string,
  issues: NxPolicyIssue[],
): void {
  const namedInputs = recordProperty(nxJson, 'namedInputs');
  if (!namedInputs) {
    issues.push({
      path: nxJsonPath,
      message: 'namedInputs must be configured so production builds have precise cache inputs.',
    });
    return;
  }
  if (!Array.isArray(namedInputs.default)) {
    issues.push({
      path: nxJsonPath,
      message: 'namedInputs.default must be an array; smoo allows it to remain broad for non-production tasks.',
    });
  } else if (!namedInputs.default.includes(MARKDOWN_EXCLUSION)) {
    issues.push({ path: nxJsonPath, message: markdownExclusionMessage('default') });
  }
  if (
    Array.isArray(namedInputs.default) &&
    namedInputs.default.includes('sharedGlobals') &&
    stringArrayEquals(namedInputs.sharedGlobals, ciWorkflowNamedInput)
  ) {
    issues.push({
      path: nxJsonPath,
      message:
        'namedInputs.default must not include CI-only sharedGlobals; workflow targets may name their CI inputs explicitly.',
    });
  }
  const production = namedInputs.production;
  if (!Array.isArray(production)) {
    issues.push({
      path: nxJsonPath,
      message: 'namedInputs.production must be an array of precise production inputs.',
    });
    return;
  }
  if (!isPreciseProductionNamedInput(production)) {
    issues.push({
      path: nxJsonPath,
      message:
        'namedInputs.production must enumerate precise production inputs. Do not include default or broad {projectRoot}/** globs; use language/tool-specific paths such as {projectRoot}/src/**/*, {projectRoot}/package.json, or {projectRoot}/pyproject.toml.',
    });
  } else {
    const rustInputs = production.filter(isRustCrateSourceInput);
    if (rustInputs.length > 0) {
      issues.push({
        path: nxJsonPath,
        message: `namedInputs.production must not name Rust crate sources (${rustInputs.join(', ')}): TypeScript targets see Rust only as the producer target's outputs (dependsOn + dependentTasksOutputFiles); a target that reads crate source as text names it in its own inputs; cargo targets are keyed by cargoClosure.`,
      });
    }
    if (!production.includes(MARKDOWN_EXCLUSION)) {
      issues.push({ path: nxJsonPath, message: markdownExclusionMessage('production') });
    }
  }
}

function markdownExclusionMessage(name: 'default' | 'production'): string {
  return `namedInputs.${name} must exclude ${MARKDOWN_EXCLUSION.slice(1)}: Markdown is no task's input unless a target names it.`;
}

function applyBuildTargetDefault(nxJson: Record<string, unknown>): boolean {
  const build = targetDefault(nxJson, 'build');
  if (build.form === 'malformed') {
    return false;
  }
  if (build.form === 'absent') {
    const targetDefaults = recordProperty(nxJson, 'targetDefaults');
    if (nxJson.targetDefaults !== undefined && targetDefaults === null) {
      return false;
    }
    getOrCreateRecord(nxJson, 'targetDefaults').build = { cache: true };
    return true;
  }
  let changed = false;
  if (catchAllValue(build.entries, 'cache') !== true) {
    const catchAll = build.entries.filter((entry) => !('filter' in entry)).at(-1);
    if (catchAll !== undefined) {
      changed = setBooleanProperty(catchAll, 'cache', true);
    } else if (build.form === 'list') {
      // A list with no entry for every build gets one that caches; the entries it holds stay as written.
      build.list.push({ cache: true });
      changed = true;
    } else {
      changed = setBooleanProperty(build.record, 'cache', true);
    }
  }
  for (const entry of build.entries) {
    if ('filter' in entry && 'cache' in entry && entry.cache !== true) {
      delete entry.cache;
      changed = true;
    }
  }
  return deleteMembers(build.entries, ['dependsOn', 'outputs']) || changed;
}

function validateTestTargetDefault(nxJson: Record<string, unknown>, nxJsonPath: string, issues: NxPolicyIssue[]): void {
  // Same clobber as targetDefaults.build.dependsOn: Nx replaces an inferred
  // list rather than merging, so a workspace-wide [^build, build] unwires
  // cargo-test from every Rust test aggregate. Cache stays allowed.
  if (targetDefaultEntries(nxJson, 'test').some((entry) => 'dependsOn' in entry)) {
    issues.push({
      path: nxJsonPath,
      message: 'targetDefaults.test.dependsOn must not be set; @smoothbricks/nx-plugin infers the test aggregate',
    });
  }
}

function applyTestTargetDefault(nxJson: Record<string, unknown>): boolean {
  return deleteMembers(targetDefaultEntries(nxJson, 'test'), ['dependsOn']);
}

const inferredLintProperties = ['executor', 'options', 'dependsOn', 'outputs', 'inputs'] as const;

/**
 * Whether an entry's inputs keep what lint infers. Every entry spreads what came before it (`...`); an entry for every
 * lint target also keeps the dependency declarations (`^production`), which a filtered entry inherits by spreading it.
 */
function preservesInferredLintInputs(entry: Record<string, unknown>): boolean {
  const inputs = entry.inputs;
  return Array.isArray(inputs) && inputs.includes('...') && ('filter' in entry || inputs.includes('^production'));
}

function offendingLintProperties(entry: Record<string, unknown>): string[] {
  return inferredLintProperties.filter(
    (property) => property in entry && !(property === 'inputs' && preservesInferredLintInputs(entry)),
  );
}

function validateLintTargetDefault(nxJson: Record<string, unknown>, nxJsonPath: string, issues: NxPolicyIssue[]): void {
  const offending = new Set(targetDefaultEntries(nxJson, 'lint').flatMap(offendingLintProperties));
  for (const property of inferredLintProperties) {
    if (offending.has(property)) {
      issues.push({
        path: nxJsonPath,
        message: `targetDefaults.lint.${property} must not be set; @smoothbricks/nx-plugin infers lint from project sources`,
      });
    }
  }
}

function applyLintTargetDefault(nxJson: Record<string, unknown>): boolean {
  let changed = false;
  for (const entry of targetDefaultEntries(nxJson, 'lint')) {
    changed = deleteMembers([entry], offendingLintProperties(entry)) || changed;
  }
  return changed;
}

function applyNamedInputDefaults(nxJson: Record<string, unknown>): boolean {
  const namedInputs = getOrCreateRecord(nxJson, 'namedInputs');
  let changed = false;
  const defaults = namedInputs.default;
  if (!Array.isArray(defaults)) {
    namedInputs.default = ['{projectRoot}/**/*', MARKDOWN_EXCLUSION];
    changed = true;
  } else if (defaults.includes('sharedGlobals') && stringArrayEquals(namedInputs.sharedGlobals, ciWorkflowNamedInput)) {
    namedInputs.default = defaults.filter((input) => input !== 'sharedGlobals');
    changed = true;
  }
  const production = Array.isArray(namedInputs.production) ? namedInputs.production : [];
  const withoutRust = production.filter((input) => !isRustCrateSourceInput(input));
  if (!isPreciseProductionNamedInput(withoutRust)) {
    namedInputs.production = defaultProductionNamedInput;
    changed = true;
  } else if (withoutRust.length < production.length) {
    namedInputs.production = withoutRust;
    changed = true;
  }
  for (const name of ['default', 'production'] as const) {
    const inputs = namedInputs[name];
    if (Array.isArray(inputs) && !inputs.includes(MARKDOWN_EXCLUSION)) {
      namedInputs[name] = [...inputs, MARKDOWN_EXCLUSION];
      changed = true;
    }
  }
  return changed;
}

function removeDisallowedTargetDefaults(nxJson: Record<string, unknown>): boolean {
  const targetDefaults = recordProperty(nxJson, 'targetDefaults');
  if (!targetDefaults) {
    return false;
  }
  let changed = false;
  for (const targetName of Object.keys(targetDefaults)) {
    if (targetName.includes(':') || targetName === inferredTestTypecheckTarget) {
      delete targetDefaults[targetName];
      changed = true;
      continue;
    }
    const retired = retiredCleanEntries(targetDefaults, targetName);
    if (retired.length === 0) {
      continue;
    }
    const value = targetDefaults[targetName];
    const kept = Array.isArray(value) ? value.filter((entry) => !retired.includes(entry)) : [];
    if (kept.length === 0) {
      delete targetDefaults[targetName];
    } else {
      targetDefaults[targetName] = kept;
    }
    changed = true;
  }
  return changed;
}

/** The entries of `targetName`'s default that name the retired clean executor, which the policy removes. */
function retiredCleanEntries(targetDefaults: Record<string, unknown>, targetName: string): Record<string, unknown>[] {
  if (targetName !== 'clean') {
    return [];
  }
  return targetDefaultEntries({ targetDefaults }, 'clean').filter(
    (entry) => entry.executor === retiredCleanOutputsExecutor,
  );
}

function isPreciseProductionNamedInput(production: unknown[]): boolean {
  let hasPositiveProjectInput = false;
  for (const input of production) {
    if (
      isRecord(input) &&
      input.json === '{projectRoot}/package.json' &&
      Array.isArray(input.excludeFields) &&
      input.excludeFields.every((field) => typeof field === 'string' && field.length > 0) &&
      Object.keys(input).every((key) => key === 'json' || key === 'excludeFields')
    ) {
      hasPositiveProjectInput = true;
      continue;
    }
    if (typeof input !== 'string') {
      return false;
    }
    const normalized = input.startsWith('!') ? input.slice(1) : input;
    if (impreciseProductionInputs.has(input) || impreciseProductionInputs.has(normalized)) {
      return false;
    }
    if (!input.startsWith('!') && normalized.startsWith('{projectRoot}/')) {
      hasPositiveProjectInput = true;
    }
  }
  return hasPositiveProjectInput;
}

/**
 * Rust crate source named by a positive `production` entry: a path under `{projectRoot}/crates`, a `.rs` file or a
 * `Cargo.toml`. `production` keys a project's TypeScript tasks and, through `^production`, its dependents'. They see
 * Rust only as the producer target's outputs (`dependsOn` + `dependentTasksOutputFiles`); cargo targets key on
 * `cargoClosure`; a target that reads crate source as text names it in its own inputs. An exclusion names nothing.
 */
function isRustCrateSourceInput(input: unknown): boolean {
  if (typeof input !== 'string' || input.startsWith('!')) {
    return false;
  }
  const name = input.slice(input.lastIndexOf('/') + 1);
  return (
    input === '{projectRoot}/crates' ||
    input.startsWith('{projectRoot}/crates/') ||
    name.endsWith('.rs') ||
    name === 'Cargo.toml'
  );
}

function nxPluginName(plugin: PluginConfiguration): string {
  return typeof plugin === 'string' ? plugin : plugin.plugin;
}

function readJsonObject(path: string): Record<string, unknown> | null {
  if (!existsSync(path)) {
    return null;
  }
  try {
    const parsed: unknown = JSON.parse(readFileSync(path, 'utf8'));
    return isRecord(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function recordProperty(record: Record<string, unknown>, key: string): Record<string, unknown> | null {
  const value = record[key];
  return isRecord(value) ? value : null;
}

/**
 * A target default as Nx 24 reads it: one record, or a list of records merged in document order, each applying to the
 * targets its optional `filter` matches; an entry without one applies to every target of the name. Anything else is
 * not a target default the policy can read, and it is refused by name rather than replaced.
 */
type TargetDefault =
  | { readonly form: 'absent' }
  | {
      readonly form: 'record';
      readonly entries: readonly Record<string, unknown>[];
      readonly record: Record<string, unknown>;
    }
  | {
      readonly form: 'list';
      readonly entries: readonly Record<string, unknown>[];
      readonly list: Record<string, unknown>[];
    }
  | { readonly form: 'malformed' };

function targetDefault(nxJson: Record<string, unknown>, targetName: string): TargetDefault {
  const value = recordProperty(nxJson, 'targetDefaults')?.[targetName];
  if (value === undefined) {
    return { form: 'absent' };
  }
  if (isRecord(value)) {
    return { form: 'record', entries: [value], record: value };
  }
  if (Array.isArray(value) && value.every(isRecord)) {
    return { form: 'list', entries: [...value], list: value };
  }
  return { form: 'malformed' };
}

/** The entries of a readable target default; none for an absent or malformed one, which validation reports. */
function targetDefaultEntries(nxJson: Record<string, unknown>, targetName: string): readonly Record<string, unknown>[] {
  const value = targetDefault(nxJson, targetName);
  return value.form === 'record' || value.form === 'list' ? value.entries : [];
}

/** What every target of the name receives for `key` from the entries without a filter: Nx merges them, last one wins. */
function catchAllValue(entries: readonly Record<string, unknown>[], key: string): unknown {
  let value: unknown;
  for (const entry of entries) {
    if (!('filter' in entry) && key in entry) {
      value = entry[key];
    }
  }
  return value;
}

/** Remove `members` from every entry that holds them; the entries themselves and their other members stay. */
function deleteMembers(entries: readonly Record<string, unknown>[], members: readonly string[]): boolean {
  let changed = false;
  for (const entry of entries) {
    for (const member of members) {
      if (member in entry) {
        delete entry[member];
        changed = true;
      }
    }
  }
  return changed;
}

function malformedTargetDefaultMessage(targetName: string): string {
  return `targetDefaults.${targetName} must be an object or a list of objects; smoo does not rewrite it`;
}
function getOrCreateRecord(record: Record<string, unknown>, key: string): Record<string, unknown> {
  const value = record[key];
  if (isRecord(value)) {
    return value;
  }
  const next: Record<string, unknown> = {};
  record[key] = next;
  return next;
}

function setBooleanProperty(record: Record<string, unknown>, key: string, value: boolean): boolean {
  if (record[key] === value) {
    return false;
  }
  record[key] = value;
  return true;
}

function setStringProperty(record: Record<string, unknown>, key: string, value: string): boolean {
  if (record[key] === value) {
    return false;
  }
  record[key] = value;
  return true;
}

function stringArrayEquals(value: unknown, expected: readonly string[]): boolean {
  return (
    Array.isArray(value) && value.length === expected.length && value.every((entry, index) => entry === expected[index])
  );
}
