#!/usr/bin/env bun
/**
 * LMAO Transformer - Compile Script
 *
 * This script:
 * 1. Reads demo-source.ts
 * 2. Applies the LMAO transformer
 * 3. Prints the transformed code (so you can see the injections)
 * 4. Writes the compiled output to demo-compiled.js
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import ts from 'typescript';
import { createLmaoTransformer } from '../src/transformer.js';

const examplesDir = path.dirname(new URL(import.meta.url).pathname);
const sourceFile = path.join(examplesDir, 'demo-source.ts');
const outputFile = path.join(examplesDir, 'demo-compiled.js');

// Read source
const source = fs.readFileSync(sourceFile, 'utf-8');

console.log('='.repeat(70));
console.log('LMAO TRANSFORMER - COMPILING');
console.log('='.repeat(70));
console.log(`\nSource: ${sourceFile}`);
console.log(`Output: ${outputFile}\n`);

// Compile with transformer
const result = ts.transpileModule(source, {
  compilerOptions: {
    module: ts.ModuleKind.ESNext,
    target: ts.ScriptTarget.ESNext,
    esModuleInterop: true,
  },
  fileName: sourceFile,
  transformers: {
    before: [createLmaoTransformer()],
  },
});

// Print transformed code
console.log('='.repeat(70));
console.log('TRANSFORMED CODE');
console.log('='.repeat(70));
console.log();
console.log(result.outputText);
console.log('='.repeat(70));
console.log('END TRANSFORMED CODE');
console.log('='.repeat(70));

// Write output file
fs.writeFileSync(outputFile, result.outputText);
console.log(`\nCompiled output written to: ${outputFile}\n`);
