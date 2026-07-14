import { describe, expect, it } from 'bun:test';

const CONFIG_PATH = require.resolve('../metro.config.cjs');
const MODE_MODULE_REQUEST = './src/benchmark-mode';
const ENVIRONMENT_KEYS = [
  'LMAO_BENCH_MODE',
  'LMAO_BENCH_TRANSFORM',
  'EXPO_PUBLIC_LMAO_BENCH_MODE',
  'EXPO_PUBLIC_LMAO_BENCH_TRANSFORM',
];

function resolveModeModule(mode: string): string {
  process.env.LMAO_BENCH_MODE = mode;
  process.env.LMAO_BENCH_TRANSFORM = 'off';
  delete require.cache[CONFIG_PATH];

  const config = require(CONFIG_PATH);
  const resolution = config.resolver.resolveRequest(
    {
      resolveRequest: () => {
        throw new Error('The benchmark mode request must not use fallback resolution');
      },
    },
    MODE_MODULE_REQUEST,
    'ios',
  );
  return resolution.filePath;
}

describe('Metro benchmark mode resolution', () => {
  it('does not retain diagnostic mode when a later build selects cold mode', () => {
    const previousEnvironment = new Map(ENVIRONMENT_KEYS.map((key) => [key, process.env[key]]));
    const previousConfigModule = require.cache[CONFIG_PATH];

    try {
      const diagnosticPath = resolveModeModule('diagnostic');
      const coldPath = resolveModeModule('cold');

      expect(diagnosticPath).toEndWith('/src/benchmark-mode.diagnostic.ts');
      expect(coldPath).toEndWith('/src/benchmark-mode.cold.ts');
      expect(coldPath).not.toBe(diagnosticPath);
    } finally {
      delete require.cache[CONFIG_PATH];
      if (previousConfigModule !== undefined) {
        require.cache[CONFIG_PATH] = previousConfigModule;
      }

      for (const [key, value] of previousEnvironment) {
        if (value === undefined) {
          delete process.env[key];
        } else {
          process.env[key] = value;
        }
      }
    }
  });
});
