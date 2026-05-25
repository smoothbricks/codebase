import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { $ } from 'bun';
import { getOrCreateRecord, readJsonObject, setStringProperty, writeJsonObject } from '../lib/json.js';

export async function syncRootRuntimeVersions(root: string): Promise<void> {
  const packageJsonPath = join(root, 'package.json');
  const packageJson = readJsonObject(packageJsonPath);
  if (!packageJson) {
    throw new Error('package.json not found or invalid');
  }
  const node = runtimeCommand(root, 'node');
  const bun = runtimeCommand(root, 'bun');
  const nodeVersion = (await $`${node} --version`.cwd(root).text()).trim().replace(/^v/, '');
  const bunVersion = (await $`${bun} --version`.cwd(root).text()).trim();
  const nodeMajor = nodeVersion.split('.', 1)[0];
  if (!nodeMajor) {
    throw new Error(`Unable to derive Node major version from ${nodeVersion}`);
  }

  let changed = false;
  const engines = getOrCreateRecord(packageJson, 'engines');
  changed = setStringProperty(engines, 'node', `>=${nodeMajor}.0.0`) || changed;
  changed = setStringProperty(packageJson, 'packageManager', `bun@${bunVersion}`) || changed;
  const devDependencies = getOrCreateRecord(packageJson, 'devDependencies');
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

type RuntimeTypesPinMode = 'major' | 'exact';

async function runtimeTypesRange(
  root: string,
  packageName: string,
  runtimeVersion: string,
  pinMode: RuntimeTypesPinMode,
): Promise<string> {
  const versionsText = await $`bun pm view ${packageName} versions --json`.cwd(root).text();
  const versions = JSON.parse(versionsText) as unknown;
  if (!Array.isArray(versions) || !versions.every((version) => typeof version === 'string')) {
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
    if (latestVersion(versions.filter((version) => versionMajor(version) === runtimeMajor))) {
      return `~${runtimeMajor}.0.0`;
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
