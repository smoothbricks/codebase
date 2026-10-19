/** This repository's fixtures own a copy of its Nx installation (`../owned-installation.ts`) in an Nx fixture root. */
import { join } from 'node:path';
import { ownedInstallation, type TestFileInstallation } from '../owned-installation.js';
import { nxFixtureRoot } from './fixture-nx-env.js';

const repositoryRoot = join(import.meta.dir, '../../../..');

/**
 * The installation one test file owns, in a fixture root named by `prefix`. Call once per file, at module scope:
 * Bun runs many test files in one process, and each must own its copy.
 */
export function testFileInstallation(prefix: string): TestFileInstallation {
  return ownedInstallation(repositoryRoot, () => nxFixtureRoot(prefix));
}
