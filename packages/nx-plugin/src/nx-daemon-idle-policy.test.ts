import { expect, it } from 'bun:test';
import { createRequire } from 'node:module';

it('loads the five-minute inactivity policy from the actual installed Nx product', () => {
  const installedNx = createRequire(import.meta.url)('nx/src/daemon/server/shutdown-utils.js');
  expect(installedNx.SERVER_INACTIVITY_TIMEOUT_MS).toBe(5 * 60_000);
});
