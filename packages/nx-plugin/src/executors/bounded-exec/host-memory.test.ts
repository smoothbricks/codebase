import { describe, expect, test } from 'bun:test';
import { HOST_MEMORY } from './host-memory.js';

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
