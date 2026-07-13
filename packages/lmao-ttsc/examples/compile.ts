#!/usr/bin/env bun
/**
 * Compile demo-source.ts through Bun's ttsc unplugin adapter.
 *
 * The explicit plugin entry is only needed because this example lives inside
 * the transformer package itself. Consumer projects get the same descriptor
 * through ttsc's direct-dependency discovery.
 */

import * as path from 'node:path';
import ttsc from '../src/index.js';

const examplesDir = path.dirname(new URL(import.meta.url).pathname);
const sourceFile = path.join(examplesDir, 'demo-source.ts');
const outputDir = path.join(examplesDir, 'dist');

console.log('='.repeat(70));
console.log('LMAO TTSC BUN ADAPTER - COMPILING');
console.log('='.repeat(70));
console.log(`\nSource: ${sourceFile}`);
console.log(`Output: ${outputDir}\n`);

const result = await Bun.build({
  entrypoints: [sourceFile],
  outdir: outputDir,
  target: 'bun',
  plugins: [
    ttsc({
      project: 'examples/tsconfig.json',
      plugins: [{ transform: '@smoothbricks/lmao-ttsc/ttsc-plugin' }],
    }),
  ],
});

if (!result.success) {
  for (const log of result.logs) console.error(log);
  throw new Error('LMAO ttsc Bun build failed');
}

const output = result.outputs[0];
if (!output) throw new Error('LMAO ttsc Bun build produced no output');
const transformed = await output.text();

console.log('='.repeat(70));
console.log('TRANSFORMED CODE');
console.log('='.repeat(70));
console.log(transformed);
console.log('='.repeat(70));
console.log('END TRANSFORMED CODE');
console.log('='.repeat(70));
