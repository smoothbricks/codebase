import { expect, it } from 'bun:test';
import type { RunMemoryReading } from './host-memory.js';
import { type FileRead, linuxMemory, linuxRunMemory } from './host-memory.linux.js';

const GiB = 1024 ** 3;
const MEMINFO = `MemTotal:       ${64 * 1024 * 1024} kB\nMemFree:         1024 kB\n`;
const MOUNTED = '30 24 0:26 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime shared:4 - cgroup2 cgroup2 rw\n';
const absent: FileRead = { ok: false, code: 'ENOENT', message: 'no such file or directory' };

/** A host of `files`: a path it does not name is absent. */
const host =
  (files: Record<string, string | FileRead>) =>
  async (path: string): Promise<FileRead> => {
    const file = files[path];
    return file === undefined ? absent : typeof file === 'string' ? { ok: true, text: file } : file;
  };

it("reads the run's memory as the least finite memory.max of its cgroup and its ancestors, and the host's MemTotal", async () => {
  const cases: { name: string; files: Record<string, string | FileRead>; reading: RunMemoryReading }[] = [
    {
      name: "a CI runner's container: its own cgroup's limit",
      files: {
        '/proc/meminfo': MEMINFO,
        '/proc/self/cgroup': '0::/\n',
        '/proc/self/mountinfo': MOUNTED,
        '/sys/fs/cgroup/memory.max': `${32 * GiB}\n`,
      },
      reading: { ok: true, memory: { bytes: 32 * GiB, source: '/sys/fs/cgroup/memory.max' } },
    },
    {
      name: "a job whose own memory.max is max has its class's limit, and the hierarchy's root has none",
      files: {
        '/proc/meminfo': MEMINFO,
        '/proc/self/cgroup': '0::/cowshed.slice/background/job-1-7\n',
        '/proc/self/mountinfo': MOUNTED,
        '/sys/fs/cgroup/cowshed.slice/background/job-1-7/memory.max': 'max\n',
        '/sys/fs/cgroup/cowshed.slice/background/memory.max': `${48 * GiB}\n`,
        '/sys/fs/cgroup/cowshed.slice/memory.max': 'max\n',
      },
      reading: { ok: true, memory: { bytes: 48 * GiB, source: '/sys/fs/cgroup/cowshed.slice/background/memory.max' } },
    },
    {
      name: 'a limit above the host is no constraint: MemTotal',
      files: {
        '/proc/meminfo': MEMINFO,
        '/proc/self/cgroup': '0::/user.slice\n',
        '/proc/self/mountinfo': MOUNTED,
        '/sys/fs/cgroup/user.slice/memory.max': '9223372036854771712\n',
      },
      reading: { ok: true, memory: { bytes: 64 * GiB, source: '/proc/meminfo MemTotal' } },
    },
    {
      name: 'a mount that shows a cgroup below the hierarchy root (a container without its own namespace)',
      files: {
        '/proc/meminfo': MEMINFO,
        '/proc/self/cgroup': '0::/docker/abc\n',
        '/proc/self/mountinfo': '30 24 0:26 /docker/abc /sys/fs/cgroup ro,nosuid shared:4 - cgroup2 cgroup2 rw\n',
        '/sys/fs/cgroup/memory.max': `${16 * GiB}\n`,
      },
      reading: { ok: true, memory: { bytes: 16 * GiB, source: '/sys/fs/cgroup/memory.max' } },
    },
    {
      name: 'a host with no cgroup v2 hierarchy: MemTotal',
      files: { '/proc/meminfo': MEMINFO, '/proc/self/cgroup': '4:memory:/user.slice\n' },
      reading: { ok: true, memory: { bytes: 64 * GiB, source: '/proc/meminfo MemTotal' } },
    },
    {
      name: 'a memory.max that is neither max nor bytes',
      files: {
        '/proc/meminfo': MEMINFO,
        '/proc/self/cgroup': '0::/a\n',
        '/proc/self/mountinfo': MOUNTED,
        '/sys/fs/cgroup/a/memory.max': 'lots\n',
      },
      reading: {
        ok: false,
        error: {
          operation: 'read-run-memory',
          code: 'malformed',
          subject: '/sys/fs/cgroup/a/memory.max',
          cause: 'it holds "lots", not max or bytes',
        },
      },
    },
    {
      name: 'a memory.max the run may not read',
      files: {
        '/proc/meminfo': MEMINFO,
        '/proc/self/cgroup': '0::/a\n',
        '/proc/self/mountinfo': MOUNTED,
        '/sys/fs/cgroup/a/memory.max': { ok: false, code: 'EACCES', message: 'permission denied' },
      },
      reading: {
        ok: false,
        error: {
          operation: 'read-run-memory',
          code: 'unreadable',
          subject: '/sys/fs/cgroup/a/memory.max',
          cause: 'EACCES: permission denied',
        },
      },
    },
    {
      name: 'a cgroup v2 hierarchy mounted nowhere',
      files: { '/proc/meminfo': MEMINFO, '/proc/self/cgroup': '0::/a\n', '/proc/self/mountinfo': '' },
      reading: {
        ok: false,
        error: {
          operation: 'read-run-memory',
          code: 'unmounted',
          subject: '/a',
          cause: "/proc/self/mountinfo lists no cgroup2 mount that shows the run's cgroup",
        },
      },
    },
    {
      name: 'a cgroup outside the mounted hierarchy',
      files: { '/proc/meminfo': MEMINFO, '/proc/self/cgroup': '0::/../../a\n', '/proc/self/mountinfo': MOUNTED },
      reading: {
        ok: false,
        error: {
          operation: 'read-run-memory',
          code: 'unmounted',
          subject: '/../../a',
          cause: "the run's cgroup lies outside the cgroup2 mount at /sys/fs/cgroup",
        },
      },
    },
    {
      name: 'a meminfo with no MemTotal',
      files: { '/proc/meminfo': 'MemFree: 1 kB\n' },
      reading: {
        ok: false,
        error: {
          operation: 'read-run-memory',
          code: 'malformed',
          subject: '/proc/meminfo',
          cause: 'it names no MemTotal in kB',
        },
      },
    },
  ];
  for (const check of cases) {
    expect(await linuxRunMemory(host(check.files)), check.name).toEqual(check.reading);
  }
});

it("reads a leaf's peak in bytes whichever unit its Bun reports Linux's ru_maxrss in", () => {
  // Bun 1.3 passes the kernel's KiB through; Bun 1.4.0 and later convert to bytes.
  expect(linuxMemory.peakBytes(4096, '1.3.13')).toBe(4 * 1024 * 1024);
  expect(linuxMemory.peakBytes(4 * 1024 * 1024, '1.4.0')).toBe(4 * 1024 * 1024);
  expect(linuxMemory.peakBytes(4 * 1024 * 1024, '1.4.4')).toBe(4 * 1024 * 1024);
});
