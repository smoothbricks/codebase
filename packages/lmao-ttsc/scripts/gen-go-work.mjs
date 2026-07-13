#!/usr/bin/env node
// Generates plugin/go.work pointing at the installed ttsc package's Go
// modules, so `go vet` / `go test` / gopls work locally without ttsc's
// build-time overlay (the pattern typia uses, resolved dynamically instead
// of assuming a sibling checkout). go.work is dev-only and gitignored;
// consumers never need it — ttsc generates its own overlay when building
// the published plugin source.

import fs from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const pluginDir = path.join(here, '..', 'plugin');
const require_ = createRequire(import.meta.url);
const ttscRoot = path.dirname(require_.resolve('ttsc/package.json'));

const shimDir = path.join(ttscRoot, 'shim');
const shims = fs
  .readdirSync(shimDir, { withFileTypes: true })
  .filter((d) => d.isDirectory())
  .map((d) => path.join(shimDir, d.name));
// vfs has nested modules (cachedvfs, osvfs)
for (const nested of ['cachedvfs', 'osvfs']) {
  const p = path.join(shimDir, 'vfs', nested);
  if (fs.existsSync(path.join(p, 'go.mod'))) shims.push(p);
}

const uses = ['.', ttscRoot, ...shims.filter((p) => fs.existsSync(path.join(p, 'go.mod')))];
const goWork = [
  'go 1.26',
  '',
  'use (',
  ...uses.map((u) => `\t${u === '.' ? '.' : path.relative(pluginDir, u)}`),
  ')',
  '',
  `replace github.com/samchon/ttsc/packages/ttsc v0.0.0 => ${path.relative(pluginDir, ttscRoot)}`,
  '',
].join('\n');

fs.writeFileSync(path.join(pluginDir, 'go.work'), goWork);
console.log(`wrote plugin/go.work (ttsc at ${ttscRoot})`);
