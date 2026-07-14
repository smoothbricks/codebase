'use strict';
const path = require('node:path');

const { getDefaultConfig } = require('expo/metro-config');
const { withTtsc } = require('@ttsc/metro');

const transformVariant = process.env.LMAO_BENCH_TRANSFORM;
const benchmarkMode = process.env.LMAO_BENCH_MODE;

if (transformVariant !== 'off' && transformVariant !== 'on') {
  throw new Error('LMAO_BENCH_TRANSFORM must be exactly "off" or "on".');
}
if (benchmarkMode !== 'cold' && benchmarkMode !== 'steady' && benchmarkMode !== 'diagnostic') {
  throw new Error('LMAO_BENCH_MODE must be exactly "cold", "steady", or "diagnostic".');
}

const config = getDefaultConfig(__dirname);
const defaultResolveRequest = config.resolver.resolveRequest;
const modeModulePath = path.join(__dirname, 'src', `benchmark-mode.${benchmarkMode}.ts`);
const transformVariantModulePath = path.join(__dirname, 'src', `transform-variant.${transformVariant}.ts`);

// Expo snapshots EXPO_PUBLIC variables before this config has finished evaluating, so
// late environment mutation can leave a previous mode or transformer label in Metro's cache.
// Resolve every build variant to a distinct source path so Nx configurations produce
// distinct graph/cache keys even when they share the same Metro worker cache.
config.resolver.resolveRequest = (context, moduleName, platform) => {
  if (moduleName === './src/transform-variant' || moduleName === './transform-variant') {
    return { filePath: transformVariantModulePath, type: 'sourceFile' };
  }
  if (moduleName === './src/benchmark-mode' || moduleName === './benchmark-mode') {
    return { filePath: modeModulePath, type: 'sourceFile' };
  }
  return defaultResolveRequest
    ? defaultResolveRequest(context, moduleName, platform)
    : context.resolveRequest(context, moduleName, platform);
};

const plugins = transformVariant === 'on' ? [{ transform: '@smoothbricks/lmao-ttsc/ttsc-plugin' }] : [];

module.exports = withTtsc(config, {
  project: 'tsconfig.json',
  plugins,
  include: ['App.tsx', 'index.ts', 'src/', '../lmao/benchmarks/plugin-scenario/'],
});
