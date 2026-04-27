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
  changed = setStringProperty(devDependencies, '@types/node', `~${nodeMajor}.0.0`) || changed;
  changed = setStringProperty(devDependencies, '@types/bun', bunVersion) || changed;

  if (changed) {
    writeJsonObject(packageJsonPath, packageJson);
    console.log('updated        package.json runtime versions');
  } else {
    console.log('unchanged      package.json runtime versions');
  }
}

function runtimeCommand(root: string, name: 'bun' | 'node'): string {
  const candidates = [
    join(root, 'tooling', 'direnv', '.devenv', 'profile', 'bin', name),
    join(root, 'tooling', 'devenv', '.profile', 'bin', name),
  ];
  return candidates.find((path) => existsSync(path)) ?? name;
}
