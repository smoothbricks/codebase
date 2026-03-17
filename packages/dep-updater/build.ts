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

// Step 3: Bundle library entry point
console.log('Bundling library entry point...');
const libResult = await build({
  entrypoints: ['./src/index.ts'],
  outdir: './dist',
  target: 'node',
  format: 'esm',
  minify: false,
  sourcemap: 'none',
  external: ['commander', 'execa', 'fast-glob', 'yaml'],
  naming: {
    entry: 'index.js',
  },
});

if (!libResult.success) {
  console.error('Library build failed:', libResult.logs);
  process.exit(1);
}

console.log('Library build complete! Output: dist/index.js');

// Step 4: Generate TypeScript declarations
console.log('Generating type declarations...');
await $`tsc --build --emitDeclarationOnly`;
console.log('Type declarations generated!');

// Step 5: Add shebang to the CLI bundle
const cliPath = './dist/cli.js';
const content = await Bun.file(cliPath).text();
await Bun.write(cliPath, `#!/usr/bin/env node\n${content}`);

// Step 6: Make the CLI executable
await $`chmod +x ./dist/cli.js`;

console.log('Build complete! Output: dist/cli.js, dist/index.js, dist/index.d.ts');
