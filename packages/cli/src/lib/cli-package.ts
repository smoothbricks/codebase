import { fileURLToPath } from 'node:url';
import { readJsonObject } from './json.js';

export {
  isSmoothBricksCodebasePackageName,
  smoothBricksCodebasePackageName,
} from '@smoothbricks/nx-plugin/workspace-package-policy';
export const cliPackageVersion = readCliPackageVersion();

function readCliPackageVersion(): string {
  const pkg = readJsonObject(fileURLToPath(new URL('../../package.json', import.meta.url)));
  const version = pkg?.version;
  if (!version) {
    throw new Error('Unable to read @smoothbricks/cli package version.');
  }
  return version;
}
