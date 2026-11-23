import { afterEach, describe, expect, it } from 'bun:test';
import { type ChildProcess, spawn } from 'node:child_process';
import { once } from 'node:events';
import { closeSync, fstatSync, openSync, readFileSync } from 'node:fs';
import { createServer } from 'node:net';
import { createInterface } from 'node:readline';
import { ADDON_DIRECTORIES } from './addon.js';
import {
  awakeTimeMs,
  descriptorNames,
  hostLoad,
  listeningPorts,
  ownDescriptorName,
  processArguments,
  processGroups,
  processLineages,
  processRunState,
  residentBytes,
  running,
  sandboxed,
} from './index.js';

const children: ChildProcess[] = [];

/** Kill and reap `child` unless it has exited already. */
async function stop(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = once(child, 'exit');
  child.kill('SIGKILL');
  await exited;
}

afterEach(async () => {
  await Promise.all(children.splice(0).map(stop));
});

/**
 * A `/bin/sh -c script` child, once it has printed its first line: its exec is complete. Every child is killed and
 * reaped after its test, however the test ends.
 */
async function started(
  script: string,
  options: { readonly args?: readonly string[]; readonly detached?: boolean } = {},
): Promise<{ readonly child: ChildProcess; readonly pid: number; readonly line: string }> {
  const child = spawn('/bin/sh', ['-c', script, ...(options.args ?? [])], {
    detached: options.detached ?? false,
    stdio: ['pipe', 'pipe', 'ignore'],
  });
  children.push(child);
  const pid = child.pid;
  if (pid === undefined || child.stdout === null) {
    // invariant throw: a spawn with piped stdout that did not fail at once has a pid and a stdout.
    throw new Error('a spawned child has a pid and a stdout');
  }
  const [line] = await once(createInterface({ input: child.stdout }), 'line');
  if (typeof line !== 'string') {
    // invariant throw: readline emits each line as a string.
    throw new Error('readline emits strings');
  }
  return { child, pid, line };
}

describe('package', () => {
  it('ships an addon for exactly the hosts package.json names', () => {
    const manifest: unknown = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
    expect(manifest).toMatchObject({
      napi: {
        binaryName: 'procinfo',
        targets: [
          'aarch64-apple-darwin',
          'x86_64-apple-darwin',
          'aarch64-unknown-linux-gnu',
          'x86_64-unknown-linux-gnu',
        ],
      },
    });
    expect([...ADDON_DIRECTORIES.values()]).toEqual(['darwin-arm64', 'darwin-x64', 'linux-arm64-gnu', 'linux-x64-gnu']);
  });
});

describe('awakeTimeMs', () => {
  it('advances by at least the time this process was held awake', () => {
    const before = awakeTimeMs();
    Bun.sleepSync(20);
    const after = awakeTimeMs();
    expect(after - before).toBeGreaterThanOrEqual(19);
  });
});

describe('processLineages', () => {
  it("reads each live pid's parent and group, and leaves out a reaped one", async () => {
    // A child in a session of its own leads its own group, so its group is known without a probe.
    const { child, pid } = await started('echo up; exec sleep 600', { detached: true });
    // No kernel assigns pid 2^31 - 1, so it is left out.
    expect(processLineages([pid, 2 ** 31 - 1])).toEqual([{ pid, ppid: process.pid, pgid: pid, zombie: false }]);
    await stop(child);
    expect(processLineages([pid])).toEqual([]);
  });

  it('refuses what is not a process id', () => {
    expect(() => processLineages([0])).toThrow(expect.objectContaining({ code: 'usage', hint: expect.any(String) }));
    expect(() => processLineages([1.5])).toThrow(RangeError);
  });
});

describe('running and processRunState', () => {
  it('follow a child from running to gone, and read this process running', async () => {
    const { child, pid } = await started('echo up; exec sleep 600');
    expect(running(pid)).toBe(true);
    expect(['running', 'sleeping', 'uninterruptible']).toContain(processRunState(pid).state);
    await stop(child);
    expect(running(pid)).toBe(false);
    expect(processRunState(pid)).toEqual({ state: 'gone' });
    expect(running(process.pid)).toBe(true);
    expect(processRunState(process.pid)).toEqual({ state: 'running' });
  });

  it('refuses what is not a process id', () => {
    expect(() => running(0)).toThrow(expect.objectContaining({ code: 'usage', hint: expect.any(String) }));
    expect(() => processRunState(2 ** 31)).toThrow(RangeError);
  });
});

describe('processArguments', () => {
  it("reads a child's argument vector byte for byte, an empty argument included", async () => {
    const script = 'echo up; read _';
    const { pid } = await started(script, { args: ['name', ''] });
    const read = processArguments(pid);
    const text = new TextDecoder();
    expect(read.argv.map((argument) => text.decode(argument))).toEqual(['/bin/sh', '-c', script, 'name', '']);
    expect(read.executable.length).toBeGreaterThan(0);
  });
});

describe('processGroups', () => {
  it('is offered exactly where the kernel lists a group: Darwin', () => {
    expect(processGroups === null).toBe(process.platform !== 'darwin');
  });

  it.skipIf(processGroups === null)('read a group as its leader and the member it started', async () => {
    const groups = processGroups;
    if (groups === null) throw new Error('skipped where procinfo offers no group facts');
    const { child, pid: leader, line } = await started('sleep 600 & echo $!; wait', { detached: true });
    const member = Number(line);
    try {
      expect(groups.pids(leader).sort()).toEqual([leader, member].sort());
      const members = groups.members(leader);
      expect(members.map(({ pid }) => pid).sort()).toEqual([leader, member].sort());
      for (const read of members) {
        expect(read.command.length).toBeGreaterThan(0);
        expect(['running', 'sleeping', 'uninterruptible']).toContain(read.state.state);
      }
      expect(groups.cpu(leader)).toBeGreaterThanOrEqual(0n);
    } finally {
      const exited = once(child, 'exit');
      process.kill(-leader, 'SIGKILL');
      await exited;
    }
  });
});

describe('residentBytes', () => {
  it("counts this process's memory and none for a reaped child", async () => {
    expect(residentBytes(process.pid)).toBeGreaterThan(0n);
    const { child, pid } = await started('echo up; exec sleep 600');
    await stop(child);
    expect(residentBytes(pid)).toBeNull();
  });
});

describe('listeningPorts', () => {
  it('names the TCP port this process listens on', async () => {
    const server = createServer();
    server.listen(0, '127.0.0.1');
    await once(server, 'listening');
    try {
      const address = server.address();
      if (address === null || typeof address === 'string') throw new Error('a TCP server has a port');
      expect(listeningPorts(process.pid)).toContain(address.port);
    } finally {
      server.close();
    }
  });
});

describe('descriptor names', () => {
  it("name this process's open file by its inode, and a closed descriptor as closed", () => {
    const fd = openSync(new URL('../package.json', import.meta.url), 'r');
    let open: ReturnType<typeof ownDescriptorName>;
    try {
      open = ownDescriptorName(fd);
      expect(open).toEqual({ kind: 'inode', inode: fstatSync(fd, { bigint: true }).ino });
      const listed = descriptorNames(process.pid, { devices: false, inodes: true });
      expect(listed).toMatchObject({ descriptors: expect.arrayContaining([{ fd, name: open }]) });
      expect(descriptorNames(process.pid, { devices: false, inodes: false })).toEqual({ descriptors: [] });
    } finally {
      closeSync(fd);
    }
    expect(ownDescriptorName(fd)).toEqual({ kind: 'closed' });
  });

  it('say a reaped child is gone', async () => {
    const { child, pid } = await started('echo up; exec sleep 600');
    await stop(child);
    expect(descriptorNames(pid, { devices: true, inodes: true })).toEqual({ unread: 'gone' });
  });
});

describe('sandboxed', () => {
  it('reads a child as confined as this process, and no live process at a reaped pid', async () => {
    const { child, pid } = await started('echo up; exec sleep 600');
    expect(sandboxed(pid)).toBe(sandboxed(process.pid));
    await stop(child);
    expect(sandboxed(pid)).toBe(true);
  });
});

describe('hostLoad', () => {
  it('reports all three averages', () => {
    const load = hostLoad();
    expect(load).not.toBeNull();
    for (const average of [load?.one, load?.five, load?.fifteen]) {
      expect(Number.isFinite(average) && (average ?? -1) >= 0).toBe(true);
    }
  });
});
