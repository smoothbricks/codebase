import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { $ } from 'bun';
import {
  ensureDependencyMap,
  ensureEngines,
  type PackageJson,
  parseStringArrayText,
  readJsonObject,
  setPackageStringField,
  setStringProperty,
  writeJsonObject,
} from '../lib/json.js';

export interface RuntimeVersions {
  node: string;
  bun: string;
}

/** Live runtime versions from the shell PATH — the single source sync AND validate derive from. */
export async function runtimeVersionsFromPath(root: string): Promise<RuntimeVersions> {
  const node = runtimeCommand(root, 'node');
  const bun = runtimeCommand(root, 'bun');
  return {
    node: (await $`${node} --version`.cwd(root).text()).trim().replace(/^v/, ''),
    bun: (await $`${bun} --version`.cwd(root).text()).trim(),
  };
}

export async function syncRootRuntimeVersions(root: string): Promise<void> {
  const packageJsonPath = join(root, 'package.json');
  const packageJson = readJsonObject(packageJsonPath);
  if (!packageJson) {
    throw new Error('package.json not found or invalid');
  }
  const { node: nodeVersion, bun: bunVersion } = await runtimeVersionsFromPath(root);
  const nodeMajor = nodeVersion.split('.', 1)[0];
  if (!nodeMajor) {
    throw new Error(`Unable to derive Node major version from ${nodeVersion}`);
  }

  let changed = false;
  const engines = ensureEngines(packageJson);
  changed = setStringProperty(engines, 'node', `>=${nodeMajor}.0.0`) || changed;
  changed = setPackageStringField(packageJson, 'packageManager', `bun@${bunVersion}`) || changed;
  const devDependencies = ensureDependencyMap(packageJson, 'devDependencies');
  changed =
    setStringProperty(
      devDependencies,
      '@types/node',
      await runtimeTypesRange(root, '@types/node', nodeVersion, 'major'),
    ) || changed;
  changed =
    setStringProperty(
      devDependencies,
      '@types/bun',
      await runtimeTypesRange(root, '@types/bun', bunVersion, 'exact'),
    ) || changed;

  if (changed) {
    writeJsonObject(packageJsonPath, packageJson);
    console.log('updated        package.json runtime versions');
  }
}

/**
 * Validate that package.json runtime pins agree with the LIVE PATH runtimes,
 * never a stored template. Offline by design: structural alignment only
 * (majors, exact bun pin, no `~major.0.0` floor) — the registry is not
 * consulted. Repair: `smoo monorepo init --runtime-only` inside the devenv shell.
 */
export async function validateRootRuntimeVersions(root: string): Promise<number> {
  const packageJson = readJsonObject(join(root, 'package.json'));
  if (!packageJson) {
    console.error('package.json not found or invalid');
    return 1;
  }
  return validateRuntimePins(packageJson, await runtimeVersionsFromPath(root));
}

export function validateRuntimePins(packageJson: PackageJson, runtime: RuntimeVersions): number {
  const nodeMajor = runtime.node.split('.', 1)[0];
  if (!nodeMajor) {
    throw new Error(`Unable to derive Node major version from ${runtime.node}`);
  }
  let failures = 0;
  const repair = 'run `smoo monorepo init --runtime-only` inside the devenv shell';
  const enginesNode = packageJson.engines?.node ?? null;
  if (enginesNode !== `>=${nodeMajor}.0.0`) {
    console.error(
      `package.json engines.node is ${enginesNode ?? 'missing'} but the PATH node is v${runtime.node} — expected >=${nodeMajor}.0.0; ${repair}`,
    );
    failures++;
  }
  const packageManager = packageJson.packageManager ?? null;
  if (packageManager !== `bun@${runtime.bun}`) {
    console.error(
      `package.json packageManager is ${packageManager ?? 'missing'} but the PATH bun is ${runtime.bun} — expected bun@${runtime.bun}; ${repair}`,
    );
    failures++;
  }
  const typesNode = packageJson.devDependencies?.['@types/node'] ?? null;
  const typesNodeParts = typesNode ? /^~(\d+)\.(\d+)\.(\d+)$/.exec(typesNode) : null;
  if (!typesNodeParts || typesNodeParts[1] !== nodeMajor) {
    console.error(
      `package.json @types/node is ${typesNode ?? 'missing'} but the PATH node is v${runtime.node} — types track the runtime major (~${nodeMajor}.x.y, newest published minor); ${repair}`,
    );
    failures++;
  } else if (typesNodeParts[2] === '0' && typesNodeParts[3] === '0') {
    console.error(
      `package.json @types/node is pinned to the ~${nodeMajor}.0.0 floor — tilde locks the first patch line and strands the repo on early broken releases; ${repair} to repin to the newest published ${nodeMajor}.x`,
    );
    failures++;
  }
  return failures;
}

type RuntimeTypesPinMode = 'major' | 'exact';

async function runtimeTypesRange(
  root: string,
  packageName: string,
  runtimeVersion: string,
  pinMode: RuntimeTypesPinMode,
): Promise<string> {
  const versionsText = await $`bun pm view ${packageName} versions --json`.cwd(root).text();
  const versions = parseStringArrayText(versionsText);
  if (!versions) {
    throw new Error(`Unable to read published ${packageName} versions`);
  }
  return runtimeTypesRangeForPublishedVersions(packageName, runtimeVersion, pinMode, versions);
}

export function runtimeTypesRangeForPublishedVersions(
  packageName: string,
  runtimeVersion: string,
  pinMode: RuntimeTypesPinMode,
  versions: readonly string[],
): string {
  const parsedRuntimeVersion = parseVersion(runtimeVersion);
  if (!parsedRuntimeVersion) {
    throw new Error(`Unable to parse runtime version ${runtimeVersion}`);
  }

  if (pinMode === 'major') {
    const runtimeMajor = parsedRuntimeVersion[0].toString();
    // Newest published types WITHIN the runtime major — never the `~major.0.0`
    // floor: tilde locks the patch line, so the floor can strand consumers on a
    // broken early release (e.g. @types/node 24.0.x's URLPattern/DOM TS2403
    // conflict, fixed in 24.13) with no path to the repaired minors.
    const latestInMajor = latestVersion(versions.filter((version) => versionMajor(version) === runtimeMajor));
    if (latestInMajor) {
      return `~${latestInMajor}`;
    }
  } else if (versions.includes(runtimeVersion)) {
    return runtimeVersion;
  }

  const latest = latestVersion(versions);
  if (!latest) {
    throw new Error(`Unable to find any published ${packageName} versions`);
  }

  console.warn(`${packageName} has no published types for runtime ${runtimeVersion}; using ${latest}`);
  return pinMode === 'major' ? `~${latest}` : latest;
}

function latestVersion(versions: readonly string[]): string | null {
  let latest: string | null = null;
  for (const version of versions) {
    if (!parseVersion(version)) {
      continue;
    }
    if (!latest || compareVersions(version, latest) > 0) {
      latest = version;
    }
  }
  return latest;
}

function compareVersions(left: string, right: string): number {
  const leftParts = parseVersion(left);
  const rightParts = parseVersion(right);
  if (!leftParts || !rightParts) {
    return 0;
  }
  for (let index = 0; index < leftParts.length; index++) {
    const diff = leftParts[index] - rightParts[index];
    if (diff !== 0) {
      return diff;
    }
  }
  return 0;
}

function versionMajor(version: string): string | null {
  return parseVersion(version)?.[0].toString() ?? null;
}

function parseVersion(version: string): [number, number, number] | null {
  const match = /^(\d+)\.(\d+)\.(\d+)$/.exec(version);
  return match ? [Number(match[1]), Number(match[2]), Number(match[3])] : null;
}

function runtimeCommand(root: string, name: 'bun' | 'node'): string {
  const candidates = [
    join(root, 'tooling', 'direnv', '.devenv', 'profile', 'bin', name),
    join(root, 'tooling', 'devenv', '.profile', 'bin', name),
  ];
  return candidates.find((path) => existsSync(path)) ?? name;
}
