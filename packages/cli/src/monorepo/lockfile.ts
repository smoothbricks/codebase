import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { escapeRegex, getWorkspacePackages } from '../lib/workspace.js';

export function syncBunLockfileVersions(root: string): void {
  const lockfilePath = join(root, 'bun.lock');
  if (!existsSync(lockfilePath)) {
    throw new Error('bun.lock not found');
  }
  const packages = getWorkspacePackages(root);
  let lockfile = readFileSync(lockfilePath, 'utf8');
  let updated = 0;
  for (const pkg of packages) {
    const relativePath = pkg.path.replaceAll('\\', '/');
    const escaped = escapeRegex(relativePath);
    const pattern = new RegExp(`("${escaped}":\\s*\\{[^}]*"version":\\s*")([^"]+)(")`);
    const match = lockfile.match(pattern);
    if (!match) {
      console.log(`skip: ${relativePath} (not found in lockfile)`);
      continue;
    }
    const lockVersion = match[2];
    if (lockVersion === pkg.version) {
      console.log(`ok:   ${relativePath} = ${pkg.version}`);
      continue;
    }
    lockfile = lockfile.replace(pattern, `$1${pkg.version}$3`);
    console.log(`fix:  ${relativePath}: ${lockVersion} -> ${pkg.version}`);
    updated++;
  }
  if (updated > 0) {
    writeFileSync(lockfilePath, lockfile);
  }
  console.log(
    updated > 0 ? `Updated ${updated} workspace version(s) in bun.lock` : 'All workspace versions already in sync.',
  );
}
