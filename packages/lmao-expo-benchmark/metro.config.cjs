'use strict';

const { getDefaultConfig } = require('expo/metro-config');
const { withTtsc } = require('@ttsc/metro');

const transformVariant = process.env.LMAO_BENCH_TRANSFORM;

if (transformVariant !== 'off' && transformVariant !== 'on') {
  throw new Error('LMAO_BENCH_TRANSFORM must be exactly "off" or "on".');
}

process.env.EXPO_PUBLIC_LMAO_BENCH_TRANSFORM = transformVariant;

const plugins = transformVariant === 'on' ? [{ transform: '@smoothbricks/lmao-ttsc/ttsc-plugin' }] : [];

module.exports = withTtsc(getDefaultConfig(__dirname), {
  project: 'tsconfig.json',
  plugins,
  include: ['App.tsx', 'index.ts', 'src/', '../lmao/benchmarks/plugin-scenario/'],
});
