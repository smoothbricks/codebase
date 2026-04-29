import { fileURLToPath } from 'node:url';
import { readJsonObject, stringProperty } from './json.js';

export const smoothBricksCodebasePackageName = '@smoothbricks/codebase';
export const cliPackageVersion = readCliPackageVersion();

function readCliPackageVersion(): string {
  const pkg = readJsonObject(fileURLToPath(new URL('../../package.json', import.meta.url)));
  const version = pkg ? stringProperty(pkg, 'version') : null;
  if (!version) {
    throw new Error('Unable to read @smoothbricks/cli package version.');
  }
  return version;
}

export function isSmoothBricksCodebasePackageName(name: string | undefined): boolean {
  return name === smoothBricksCodebasePackageName;
}
