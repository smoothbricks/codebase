import type { BuildConfig } from 'bun';
import { $ } from 'bun';
import dts from 'bun-plugin-dts';

const defaultBuildConfig: BuildConfig = {
  entrypoints: ['./src/index.ts'],
  outdir: './dist',
  packages: 'bundle',
  splitting: true,
  sourcemap: 'external',
  external: ['react', 'react-dom'],
};

await $`rm -rf ./dist`;

const [esm, cjs] = await Promise.all([
  Bun.build({
    ...defaultBuildConfig,
    plugins: [dts()],
    format: 'esm',
    naming: '[dir]/[name].js',
  }),
  Bun.build({
    ...defaultBuildConfig,
    format: 'cjs',
    naming: '[dir]/[name].cjs',
  }),
]);

if (!esm.success) {
  console.log('ESM BUILD FAILED');
  console.log(esm);
} else if (!cjs.success) {
  console.log('CJS BUILD FAILED');
  console.log(cjs);
}
