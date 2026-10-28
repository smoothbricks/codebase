import { afterEach, expect, it } from 'bun:test';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { StaleBuild } from './direnv/developer-links.js';
import { linkedRebuilds } from './rebuild-linked.js';

let root = '';
afterEach(() => rmSync(root, { recursive: true, force: true }));

it('asks the link judge about every workspace package and runs each rebuild it names once', () => {
  root = mkdtempSync(join(tmpdir(), 'rebuild-linked-'));
  for (const [directory, name] of [
    ['cowshed', '@smoothbricks/cowshed'],
    ['cli', '@smoothbricks/cli'],
    ['unnamed', undefined],
  ] as const) {
    mkdirSync(join(root, 'packages', directory), { recursive: true });
    writeFileSync(
      join(root, 'packages', directory, 'package.json'),
      JSON.stringify(name === undefined ? {} : { name }),
    );
  }
  writeFileSync(join(root, 'packages', 'README.md'), 'not a package');
  const asked: string[] = [];
  const stale = (packageName: string, unit: string, rebuild: string): StaleBuild => ({
    basis: 'recorded',
    packageName,
    directory: join(root, 'packages'),
    unit,
    rebuild,
    workspaceRoot: root,
    input: 'packages/cowshed/package.json',
    change: 'changed',
  });
  const rebuilds = linkedRebuilds(root, (packageName) => {
    asked.push(packageName);
    return packageName === '@smoothbricks/cowshed'
      ? [
          stale(packageName, 'packages/cowshed/.cache/bin/darwin-arm64', 'nx run cowshed:cli-arm64-macos'),
          stale(packageName, 'packages/cowshed/.cache/native-dev/darwin-arm64', 'nx run cowshed:napi-arm64-macos'),
          stale(packageName, 'packages/cowshed/dist/ts', 'nx run cowshed:build'),
          stale(packageName, 'packages/cowshed/dist/ts/bin', 'nx run cowshed:build'),
        ]
      : [];
  });
  expect(asked.sort()).toEqual(['@smoothbricks/cli', '@smoothbricks/cowshed']);
  expect(rebuilds).toEqual([
    'nx run cowshed:build',
    'nx run cowshed:cli-arm64-macos',
    'nx run cowshed:napi-arm64-macos',
  ]);
});
