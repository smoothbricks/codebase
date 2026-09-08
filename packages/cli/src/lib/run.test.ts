import { afterEach, describe, expect, it } from 'bun:test';
import { runResult } from './run.js';

const PROBE = 'SMOO_RUN_ENV_PROBE';

afterEach(() => {
  delete process.env[PROBE];
});

describe('runResult child environment', () => {
  it('spawns children with the overlay applied', async () => {
    process.env[PROBE] = 'outer';
    const result = await runResult('sh', ['-c', 'printf "%s" "$SMOO_RUN_ENV_PROBE"'], '.', {
      [PROBE]: 'inner',
    });
    expect(result.exitCode).toBe(0);
    expect(result.stdout).toBe('inner');
    expect(process.env[PROBE]).toBe('outer');
  });

  it('spawns children without names passed as unsetEnv', async () => {
    process.env[PROBE] = 'outer';
    const result = await runResult(
      'sh',
      ['-c', 'printf "%s" "${SMOO_RUN_ENV_PROBE:-absent}"'],
      '.',
      {
        [PROBE]: 'inner',
      },
      [PROBE],
    );
    expect(result.exitCode).toBe(0);
    expect(result.stdout).toBe('absent');
    expect(process.env[PROBE]).toBe('outer');
  });
});
