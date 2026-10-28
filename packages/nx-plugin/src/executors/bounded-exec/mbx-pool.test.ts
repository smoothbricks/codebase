import { describe, expect, it } from 'bun:test';
import { mbxLeaf, mbxPool, withMbxLeaf } from './mbx-pool.js';

describe("mbx's one pool at the leaf", () => {
  it('is named only by the canonical mbx environment, at the priority of the job', () => {
    expect(mbxPool({})).toBeNull();
    expect(mbxPool({ MBX_SCHEDULER: '1' })).toBeNull();
    expect(mbxPool({ MBX_SCHEDULER: '0', MBX_CACHE_DIR: '/bc/mbx' })).toBeNull();
    expect(mbxPool({ MBX_SCHEDULER: '1', MBX_CACHE_DIR: '/bc/mbx' })).toEqual({ priority: 'low' });
    expect(mbxPool({ MBX_SCHEDULER: '1', MBX_CACHE_DIR: '/bc/mbx', MBX_SCHEDULER_PRIORITY: 'normal' })).toEqual({
      priority: 'normal',
    });
  });

  it('lets a Cargo leaf through mbx as it is and reserves any other leaf at the job count, once', () => {
    const pool = { priority: 'low' } as const;
    for (const kind of ['cargo', 'cargo-mutants', 'nextest'] as const) {
      expect(mbxLeaf(pool, kind, 8)).toEqual({ kind: 'cargo' });
      expect(withMbxLeaf('cargo test -p x', mbxLeaf(pool, kind, 8))).toBe('cargo test -p x');
    }
    const leaf = mbxLeaf(pool, 'process', 4);
    expect(leaf).toEqual({ kind: 'reserve', cpus: 4, priority: 'low' });
    expect(withMbxLeaf("bun test 'a b'", leaf)).toBe(
      "mbx reserve --cpus 4 --priority low -- /bin/sh -c 'bun test '\\''a b'\\'''",
    );
    expect(mbxLeaf(pool, 'vitest', 0)).toEqual({ kind: 'reserve', cpus: 1, priority: 'low' });
  });
});
