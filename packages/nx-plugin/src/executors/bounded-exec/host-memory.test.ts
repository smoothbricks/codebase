import { describe, expect, test } from 'bun:test';
import { HOST_MEMORY, readRunMemory } from './host-memory.js';

/**
 * The pool a run serves starts by reading the run's memory, so that read must settle. It spawned \`sysctl\`, and under
 * \`bun test\` that child's exit sometimes never settled: eight concurrent runs of the pool-silence cases at load ~50
 * hung in it 6 times out of 24, and two SB land gates (2026-11-03) failed nx-plugin:test-pool-silence there.
 */
describe('the run memory on macOS', () => {
  test('is hw.memsize, read in this process: it reads with no sysctl on PATH', async () => {
    if (process.platform !== 'darwin') return;
    const sysctl = Bun.spawnSync(['/usr/sbin/sysctl', '-n', 'hw.memsize']);
    const bytes = Number(sysctl.stdout.toString().trim());
    expect(bytes).toBeGreaterThan(0);
    const path = process.env.PATH;
    process.env.PATH = '';
    try {
      expect(await readRunMemory()).toEqual({ ok: true, memory: { bytes, source: 'hw.memsize' } });
    } finally {
      process.env.PATH = path;
    }
  });
});
describe('a process tree resident sum', () => {
  test('sums what the root and every descendant hold at once, and a root that has exited holds nothing', async () => {
    if (HOST_MEMORY === null) return;
    const allocated = 64 * 1024 * 1024;
    const holders = 4;
    // Each holder says it holds its buffer, then holds it until the tree's stdin closes; `<&0` hands a background
    // holder that stdin, which sh would otherwise replace with /dev/null.
    const hold = `node -e "const held = Buffer.alloc(${allocated}, 1); process.stdout.write('held\\n'); process.stdin.on('end', () => held.length); process.stdin.resume()"`;
    const tree = Bun.spawn(['/bin/sh', '-c', `for i in 1 2 3 4; do ${hold} <&0 & done; wait`], {
      stdin: 'pipe',
      stdout: 'pipe',
    });
    const decoder = new TextDecoder();
    let said = '';
    for await (const chunk of tree.stdout) {
      said += decoder.decode(chunk, { stream: true });
      if (said.split('\n').filter((line) => line === 'held').length === holders) break;
    }

    const held = await HOST_MEMORY.treeResidentBytes(tree.pid);
    await tree.stdin.end();
    expect(await tree.exited).toBe(0);

    expect(held).toBeGreaterThanOrEqual(holders * allocated);
    expect(await HOST_MEMORY.treeResidentBytes(tree.pid)).toBe(0);
  });
});
