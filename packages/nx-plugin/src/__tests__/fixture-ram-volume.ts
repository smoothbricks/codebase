import { expect } from 'bun:test';
import { join } from 'node:path';
import { RAM_TEMP_CAPACITY_BYTES, type RamTempPaths, ramTempPaths } from '../executors/bounded-exec/ram-temp.js';

/**
 * Assert that a host proof's RAM volume is its own: mounted under `<root>/t/`, and sharing no mountpoint, staging
 * directory, lock, volume name or size with the user's RAM temp volume (`ramTempPaths`, `RAM_TEMP_CAPACITY_BYTES`).
 */
export function expectFixtureVolume(paths: RamTempPaths, capacity: number): void {
  const own = ramTempPaths({ uid: paths.uid });
  expect(paths.mountpoint.startsWith(`${join(paths.root, 't')}/`)).toBe(true);
  expect(paths.staging.startsWith(`${join(paths.root, 't')}/`)).toBe(true);
  expect(paths.mountpoint).not.toBe(own.mountpoint);
  expect(paths.staging).not.toBe(own.staging);
  expect(paths.lockFile).not.toBe(own.lockFile);
  expect(paths.volumeName).not.toBe(own.volumeName);
  expect(capacity).not.toBe(RAM_TEMP_CAPACITY_BYTES);
}
