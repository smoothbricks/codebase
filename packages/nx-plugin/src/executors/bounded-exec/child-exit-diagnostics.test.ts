import { expect, it } from 'bun:test';
import {
  childExitMemory,
  DIAGNOSTIC_CONTEXT_ENV,
  DIAGNOSTIC_MODULE_ENV,
  grantMemoryEnvironment,
} from './child-exit-diagnostics.js';
import type { RunMemoryReading } from './host-memory.js';

const bound: RunMemoryReading = { ok: true, memory: { bytes: 32 * 1024 ** 3, source: '/sys/fs/cgroup/memory.max' } };
const lastResident = { sampledAtMs: 123, reading: { ok: true as const, bytes: 456, source: '/proc/42/status' } };

it('publishes the actual grant and preserves its facts through uncharged sub-pools', async () => {
  const environment = await grantMemoryEnvironment(789, {}, async () => bound);
  expect(environment[DIAGNOSTIC_MODULE_ENV]).toBe(new URL('./child-exit-diagnostics.ts', import.meta.url).href);
  const nested = await grantMemoryEnvironment(null, environment, async () => {
    expect.unreachable('an uncharged sub-pool must not replace its ancestor memory facts');
  });
  expect(nested).toEqual(environment);
  expect(
    childExitMemory(environment[DIAGNOSTIC_CONTEXT_ENV], lastResident, (source) => {
      expect(source).toBe('/sys/fs/cgroup/memory.max');
      return { ok: true, usage: { anon: 100, shmem: 20, source: '/sys/fs/cgroup/memory.stat' } };
    }),
  ).toEqual({
    lastResident,
    pool: {
      ok: true,
      chargedBytes: 789,
      bound,
      live: { ok: true, usage: { anon: 100, shmem: 20, source: '/sys/fs/cgroup/memory.stat' } },
    },
  });
});

it('retains diagnostic refusal reasons instead of manufacturing zero memory', async () => {
  const unread: RunMemoryReading = {
    ok: false,
    error: { operation: 'read-run-memory', code: 'unreadable', subject: '/proc/meminfo', cause: 'EACCES' },
  };
  const environment = await grantMemoryEnvironment(0, {}, async () => unread);
  expect(childExitMemory(environment[DIAGNOSTIC_CONTEXT_ENV], lastResident).pool).toEqual({
    ok: true,
    chargedBytes: 0,
    bound: unread,
    live: { ok: false, reason: '/proc/meminfo: EACCES' },
  });
  expect(childExitMemory(undefined, lastResident).pool).toEqual({
    ok: false,
    reason: 'no memory-bearing CPU grant was inherited',
  });
  const malformed = childExitMemory('{', lastResident);
  expect(malformed.pool.ok).toBe(false);
  expect(malformed.lastResident).toEqual(lastResident);
});
