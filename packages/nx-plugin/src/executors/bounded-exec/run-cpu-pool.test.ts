import { afterEach, describe, expect, it } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { CPU_POOL_ENV, readInheritedCpuPool, takeParentCpuTokens } from './cpu-pool.js';
import { CPU_TOKENS_ENV } from './cpu-tokens.js';
import { holdLease } from './gateway-lease.js';
import { closeRunCpuPool, serveRunCpuPool } from './run-cpu-pool.js';

const directories: string[] = [];

afterEach(() => {
  for (const directory of directories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

/** A gateway socket path in a fresh directory: absent unless the test makes it. */
function gatewayPath(): string {
  const directory = mkdtempSync(join(tmpdir(), 'run-cpu-pool-'));
  directories.push(directory);
  return join(directory, 'gateway.sock');
}

describe('a run without a gateway', () => {
  it('serves its tasks one pool of one token per core, so a second runner waits for the first', async () => {
    const env: NodeJS.ProcessEnv = {};
    await serveRunCpuPool({ id: 'run-477' }, env, gatewayPath(), 4);
    const pool = readInheritedCpuPool(env);
    if (pool === null) {
      throw new Error(`the run published no pool: ${JSON.stringify(env)}`);
    }
    expect(pool.tokens).toBe(4);
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
    await serveRunCpuPool({ id: 'nested' }, env, gatewayPath(), 4);
    expect(env).toEqual({ [CPU_POOL_ENV]: '/parent/pool.sock', [CPU_TOKENS_ENV]: '2' });
  });

  it('leaves a run on a host whose gateway socket exists to the gateway, answering or not', async () => {
    const gateway = gatewayPath();
    writeFileSync(gateway, '');
    const env: NodeJS.ProcessEnv = {};
    await serveRunCpuPool({ id: 'gateway-host' }, env, gateway, 4);
    expect(env).toEqual({});
  });
});
