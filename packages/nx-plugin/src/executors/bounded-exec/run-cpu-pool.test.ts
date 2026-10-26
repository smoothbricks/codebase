import { describe, expect, it } from 'bun:test';
import { CPU_POOL_ENV, readInheritedCpuPool, takeParentCpuTokens } from './cpu-pool.js';
import { holdLease } from './gateway-lease.js';
import { closeRunCpuPool, serveRunCpuPool } from './run-cpu-pool.js';
import { CPU_TOKENS_ENV } from './runner-sizing.js';

describe("an Nx run's own CPU pool", () => {
  it('serves its tasks one pool of one token per core, so a second runner waits for the first', async () => {
    const env: NodeJS.ProcessEnv = {};
    await serveRunCpuPool({ id: 'run-477' }, env, 4);
    const pool = readInheritedCpuPool(env);
    if (pool === null) {
      throw new Error(`the run published no pool: ${JSON.stringify(env)}`);
    }
    expect(pool.tokens).toBe(4);
    // A Cargo that Nx's own `nx:run-commands` starts reads the same count through the run's jobserver fifo.
    expect(env.CARGO_MAKEFLAGS).toMatch(/^-j --jobserver-auth=fifo:\/\S+$/);
    expect(env.MAKEFLAGS).toBe(env.CARGO_MAKEFLAGS);
    // Nightly 477's shape: two mutation sweeps, each asking for the whole host. The first holds the run's cores;
    // the second queues for them instead of starting a second host's worth of workers beside it.
    const first = await takeParentCpuTokens(pool, 31);
    expect(first.granted && first.tokens).toBe(4);
    const { promise: queued, resolve: queue } = Promise.withResolvers<void>();
    const second = holdLease(
      pool.socket,
      { op: 'cpu-pool', want: 31 },
      'the run CPU pool',
      { ackMs: 2_000, afterCloseMs: 5_000, grantMs: null },
      queue,
    );
    let secondSettled = false;
    void second.then(() => {
      secondSettled = true;
    });
    await queued;
    expect(secondSettled).toBe(false);
    if (first.granted) first.release();
    const granted = await second;
    expect(granted.granted && granted.answer.tokens).toBe(4);
    if (granted.granted) granted.release();

    await closeRunCpuPool({ id: 'run-477' });
    // The run's end closes its pool: nothing is left to grant from it.
    const late = await takeParentCpuTokens(pool, 1);
    expect(late.granted === false && late.cause).toBe('absent');
  });

  it('leaves a run that inherits a pool to that pool', async () => {
    const env: NodeJS.ProcessEnv = { [CPU_POOL_ENV]: '/parent/pool.sock', [CPU_TOKENS_ENV]: '2' };
    await serveRunCpuPool({ id: 'nested' }, env, 4);
    expect(env).toEqual({ [CPU_POOL_ENV]: '/parent/pool.sock', [CPU_TOKENS_ENV]: '2' });
  });

  it("takes a cowshed job's smaller inherited count as its size, and never more than the cores", async () => {
    for (const [inherited, cores, size] of [
      ['3', 8, 3],
      ['16', 8, 8],
    ] as const) {
      const env: NodeJS.ProcessEnv = { [CPU_TOKENS_ENV]: inherited };
      await serveRunCpuPool({ id: `sized-${inherited}` }, env, cores);
      expect(readInheritedCpuPool(env)?.tokens).toBe(size);
      await closeRunCpuPool({ id: `sized-${inherited}` });
    }
  });
});
