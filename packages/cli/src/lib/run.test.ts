import { afterEach, describe, expect, it } from 'bun:test';
import { mergeEnv, runResult } from './run.js';

const PROBE = 'SMOO_RUN_ENV_PROBE';

afterEach(() => {
  delete process.env[PROBE];
});

describe('mergeEnv', () => {
  it('overlays this process environment without mutating it', () => {
    process.env[PROBE] = 'outer';
    const merged = mergeEnv({ [PROBE]: 'inner', SMOO_RUN_ENV_EXTRA: 'x' });
    expect(merged[PROBE]).toBe('inner');
    expect(merged.SMOO_RUN_ENV_EXTRA).toBe('x');
    expect(process.env[PROBE]).toBe('outer');
    expect(process.env.SMOO_RUN_ENV_EXTRA).toBeUndefined();
  });

  it('withholds inherited names after the overlay, still without mutating', () => {
    process.env[PROBE] = 'outer';
    const merged = mergeEnv({ [PROBE]: 'inner' }, [PROBE, 'PATH']);
    expect(merged[PROBE]).toBeUndefined();
    expect(merged.PATH).toBeUndefined();
    expect(process.env[PROBE]).toBe('outer');
  });
});

describe('runResult child environment', () => {
  it('spawns children with the overlay applied', async () => {
    process.env[PROBE] = 'outer';
    const result = await runResult('sh', ['-c', 'printf "%s" "$SMOO_RUN_ENV_PROBE"'], '.', {
      [PROBE]: 'inner',
    });
    expect(result.exitCode).toBe(0);
    expect(result.stdout).toBe('inner');
  });

  it('spawns children without names passed as unsetEnv', async () => {
    process.env[PROBE] = 'outer';
    const result = await runResult('sh', ['-c', 'printf "%s" "${SMOO_RUN_ENV_PROBE:-absent}"'], '.', undefined, [
      PROBE,
    ]);
    expect(result.exitCode).toBe(0);
    expect(result.stdout).toBe('absent');
  });
});
