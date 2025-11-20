import { $, build } from 'bun';

console.log('Building dep-updater CLI...');

// Step 1: Clean dist directory
await $`rm -rf ./dist`;
await $`mkdir -p ./dist`;

// Step 2: Bundle the CLI with Bun
console.log('Bundling CLI with Bun...');
const bundleResult = await build({
  entrypoints: ['./src/cli.ts'],
  outdir: './dist',
  target: 'node',
  format: 'esm',
  minify: false,
  sourcemap: 'none',
  external: [
    // Keep external dependencies
    '@anthropic-ai/sdk',
    'commander',
    'execa',
    'fast-glob',
    'yaml',
  ],
  naming: {
    entry: 'cli.js',
  },
});

if (!bundleResult.success) {
  console.error('CLI build failed:', bundleResult.logs);
  process.exit(1);
}

// Step 3: Add shebang to the bundled file
const cliPath = './dist/cli.js';
const content = await Bun.file(cliPath).text();
await Bun.write(cliPath, `#!/usr/bin/env node\n${content}`);

// Step 4: Make the CLI executable
await $`chmod +x ./dist/cli.js`;

console.log('CLI build complete! Output: dist/cli.js');
