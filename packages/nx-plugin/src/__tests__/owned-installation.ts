/** This repository's fixtures own a copy of its Nx installation (`../owned-installation.ts`), kept in its tool cache. */
import { join } from 'node:path';
import { ownedInstallation, type TestFileInstallation } from '../owned-installation.js';

const repositoryRoot = join(import.meta.dir, '../../../..');

/**
 * One test file's use of the copy every test file of this package shares. Call once per file, at module scope. One
 * process takes the copy once for all its files, and concurrent test processes share it under its lock, so a changed
 * host closure is copied once per run, not once per file.
 */
export function testFileInstallation(): TestFileInstallation {
  return ownedInstallation(repositoryRoot, 'nx-plugin-tests');
}
