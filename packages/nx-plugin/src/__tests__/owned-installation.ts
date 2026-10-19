/** This repository's fixtures own a copy of its Nx installation (`../owned-installation.ts`), kept in its tool cache. */
import { join } from 'node:path';
import { ownedInstallation, type TestFileInstallation } from '../owned-installation.js';

const repositoryRoot = join(import.meta.dir, '../../../..');

/**
 * The installation one test file owns, kept as `name`. Call once per file, at module scope, with a name no other file
 * uses: Bun runs many test files in one process, and each must own its copy.
 */
export function testFileInstallation(name: string): TestFileInstallation {
  return ownedInstallation(repositoryRoot, name);
}
