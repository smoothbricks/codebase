import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

export interface NxPolicyIssue {
  path: string;
  message: string;
}

// ---------------------------------------------------------------------------
// Public constants
// ---------------------------------------------------------------------------

export const BUILD_OUTPUT_DEPENDENCIES = [
  '*-js',
  '*-web',
  '*-html',
  '*-css',
  '*-ios',
  '*-android',
  '*-native',
  '*-napi',
  '*-bun',
  '*-wasm',
];

// ---------------------------------------------------------------------------
// Internal constants
// ---------------------------------------------------------------------------

const nxJsTypescriptPlugin = '@nx/js/typescript';
const smoothBricksNxPlugin = '@smoothbricks/nx-plugin';
const expectedSharedGlobalsNamedInput = ['{workspaceRoot}/.github/workflows/ci.yml'];
const defaultProductionNamedInput = [
  '{projectRoot}/src/**/*',
  '{projectRoot}/package.json',
  '!{projectRoot}/**/__tests__/**',
  '!{projectRoot}/**/*.test.*',
  '!{projectRoot}/**/*.spec.*',
];
const impreciseProductionInputs = new Set(['default', '{projectRoot}/**/*', '{projectRoot}/**']);

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Check nx.json workspace config policy, returns issues found.
 */
export function checkWorkspaceConfigPolicy(root: string): NxPolicyIssue[] {
  const nxJsonPath = join(root, 'nx.json');
  const nxJson = readJsonObject(nxJsonPath);
  if (!nxJson) {
    return [{ path: nxJsonPath, message: 'nx.json not found or invalid' }];
  }
  const issues: NxPolicyIssue[] = [];

  // Colon target defaults
  const targetDefaults = recordProperty(nxJson, 'targetDefaults');
  if (targetDefaults) {
    for (const targetName of Object.keys(targetDefaults)) {
      if (targetName.includes(':')) {
        issues.push({
          path: nxJsonPath,
          message:
            `targetDefaults.${targetName} must not use colon target names. ` +
            'Nx CLI syntax already uses project:target:configuration, so smoo Nx target names must be unambiguous tool-output names.',
        });
      }
    }
  }

  // Build target default
  validateBuildTargetDefault(nxJson, nxJsonPath, issues);

  // Named input defaults
  validateNamedInputDefaults(nxJson, nxJsonPath, issues);

  // Plugin configuration
  const plugins = Array.isArray(nxJson.plugins) ? nxJson.plugins : [];
  const nxJsPlugin = plugins.find(isNxJsTypescriptPlugin);
  if (!nxJsPlugin) {
    issues.push({
      path: nxJsonPath,
      message:
        `plugins must configure ${nxJsTypescriptPlugin}. ` +
        'Official Nx owns TypeScript library inference; smoo configures it so tsconfig.lib.json produces tsc-js and leaves build available as an aggregate target.',
    });
  } else if (nxJsBuildTargetName(nxJsPlugin) !== 'tsc-js') {
    issues.push({
      path: nxJsonPath,
      message:
        `${nxJsTypescriptPlugin} build.targetName must be tsc-js. ` +
        'TypeScript library output is a concrete tool-output target; build is reserved for aggregate targets that depend on concrete build work.',
    });
  }
  if (!plugins.includes(smoothBricksNxPlugin) && !plugins.some(isSmoothBricksNxPluginRecord)) {
    issues.push({
      path: nxJsonPath,
      message:
        `plugins must include ${smoothBricksNxPlugin}. ` +
        'Smoo relies on this plugin to infer convention targets that official Nx does not provide, including typecheck-tests, non-TypeScript build-tool targets, and aggregate build/lint targets.',
    });
  }

  return issues;
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
  let changed = removeColonTargetDefaults(nxJson);
  changed = applyBuildTargetDefault(nxJson) || changed;
  changed = applyNamedInputDefaults(nxJson) || changed;
  const currentPlugins = Array.isArray(nxJson.plugins) ? nxJson.plugins : [];
  const nextPlugins = upsertNxPlugin(
    upsertNxPlugin(currentPlugins, expectedNxJsTypescriptPlugin()),
    smoothBricksNxPlugin,
  );
  if (JSON.stringify(currentPlugins) !== JSON.stringify(nextPlugins)) {
    nxJson.plugins = nextPlugins;
    changed = true;
  }
  if (changed) {
    writeJsonObject(nxJsonPath, nxJson);
  }
  return changed;
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

function validateBuildTargetDefault(
  nxJson: Record<string, unknown>,
  nxJsonPath: string,
  issues: NxPolicyIssue[],
): void {
  const targetDefaults = recordProperty(nxJson, 'targetDefaults');
  const build = targetDefaults ? recordProperty(targetDefaults, 'build') : null;
  if (!build || build.cache !== true) {
    issues.push({ path: nxJsonPath, message: 'targetDefaults.build.cache must be true' });
  }
  const outputs = build?.outputs;
  if (!Array.isArray(outputs) || outputs.length !== 1 || outputs[0] !== '{projectRoot}/dist') {
    issues.push({ path: nxJsonPath, message: 'targetDefaults.build.outputs must be ["{projectRoot}/dist"]' });
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
  }
  if (!stringArrayEquals(namedInputs.sharedGlobals, expectedSharedGlobalsNamedInput)) {
    issues.push({
      path: nxJsonPath,
      message: 'namedInputs.sharedGlobals must include only {workspaceRoot}/.github/workflows/ci.yml',
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
        'namedInputs.production must enumerate precise production inputs. Do not include default or broad {projectRoot}/** globs; use language/tool-specific paths such as {projectRoot}/src/**/*, {projectRoot}/Cargo.toml, or {projectRoot}/pyproject.toml.',
    });
  }
}

// ---------------------------------------------------------------------------
// Apply helpers
// ---------------------------------------------------------------------------

function applyBuildTargetDefault(nxJson: Record<string, unknown>): boolean {
  const targetDefaults = getOrCreateRecord(nxJson, 'targetDefaults');
  const build = getOrCreateRecord(targetDefaults, 'build');
  let changed = setBooleanProperty(build, 'cache', true);
  changed = setStringArrayProperty(build, 'outputs', ['{projectRoot}/dist']) || changed;
  return changed;
}

function applyNamedInputDefaults(nxJson: Record<string, unknown>): boolean {
  const namedInputs = getOrCreateRecord(nxJson, 'namedInputs');
  let changed = false;
  if (!Array.isArray(namedInputs.default)) {
    namedInputs.default = ['{projectRoot}/**/*', 'sharedGlobals'];
    changed = true;
  }
  changed = setStringArrayProperty(namedInputs, 'sharedGlobals', expectedSharedGlobalsNamedInput) || changed;
  const production = namedInputs.production;
  if (!Array.isArray(production) || !isPreciseProductionNamedInput(production)) {
    namedInputs.production = defaultProductionNamedInput;
    changed = true;
  }
  return changed;
}

function removeColonTargetDefaults(nxJson: Record<string, unknown>): boolean {
  const targetDefaults = recordProperty(nxJson, 'targetDefaults');
  if (!targetDefaults) {
    return false;
  }
  let changed = false;
  for (const targetName of Object.keys(targetDefaults)) {
    if (targetName.includes(':')) {
      delete targetDefaults[targetName];
      changed = true;
    }
  }
  return changed;
}

function isPreciseProductionNamedInput(production: unknown[]): boolean {
  let hasPositiveProjectInput = false;
  for (const input of production) {
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

// ---------------------------------------------------------------------------
// Plugin helpers
// ---------------------------------------------------------------------------

function expectedNxJsTypescriptPlugin(): Record<string, unknown> {
  return {
    plugin: nxJsTypescriptPlugin,
    options: {
      typecheck: { targetName: 'typecheck' },
      build: {
        targetName: 'tsc-js',
        configName: 'tsconfig.lib.json',
        buildDepsName: 'build-deps',
        watchDepsName: 'watch-deps',
      },
    },
  };
}

function upsertNxPlugin(plugins: readonly unknown[], plugin: string | Record<string, unknown>): unknown[] {
  const pluginName = typeof plugin === 'string' ? plugin : stringProperty(plugin, 'plugin');
  const next = plugins.filter((entry) => nxPluginName(entry) !== pluginName);
  next.push(plugin);
  return next;
}

function nxPluginName(value: unknown): string | null {
  if (typeof value === 'string') {
    return value;
  }
  return isRecord(value) ? stringProperty(value, 'plugin') : null;
}

function isNxJsTypescriptPlugin(value: unknown): value is Record<string, unknown> {
  return isRecord(value) && stringProperty(value, 'plugin') === nxJsTypescriptPlugin;
}

function isSmoothBricksNxPluginRecord(value: unknown): boolean {
  return isRecord(value) && stringProperty(value, 'plugin') === smoothBricksNxPlugin;
}

function nxJsBuildTargetName(plugin: Record<string, unknown>): string | null {
  const options = recordProperty(plugin, 'options');
  const build = options ? recordProperty(options, 'build') : null;
  return build ? stringProperty(build, 'targetName') : null;
}

// ---------------------------------------------------------------------------
// JSON helpers (self-contained, following bounded-test-policy.ts pattern)
// ---------------------------------------------------------------------------

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

function writeJsonObject(path: string, value: Record<string, unknown>): void {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function stringProperty(record: Record<string, unknown>, key: string): string | null {
  const value = record[key];
  return typeof value === 'string' && value.length > 0 ? value : null;
}

function recordProperty(record: Record<string, unknown>, key: string): Record<string, unknown> | null {
  const value = record[key];
  return isRecord(value) ? value : null;
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

function setStringArrayProperty(record: Record<string, unknown>, key: string, value: string[]): boolean {
  const current = record[key];
  if (stringArrayEquals(current, value)) {
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
