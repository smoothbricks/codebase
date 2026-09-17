import assert from 'node:assert/strict';
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

// The original source already declares the provider tuple; use it as the authority.
let repair = readFileSync(process.env.REPAIR_SOURCE, 'utf8');
const lines = repair.split('\n');
const providerLines = lines.filter(line => line.startsWith("replace(pkg+'src/types.ts', \"export type SupportedProvider"));
assert.equal(providerLines.length, 1);
repair = lines.filter(line => !providerLines.includes(line)).join('\n');
repair = repair.replace("require('@typia/unplugin/package.json').version", "JSON.parse(read('package.json')).devDependencies['@typia/unplugin']");
const temporary = join(process.env.RUNNER_TEMP,'patchnote-repair.mjs');
writeFileSync(temporary,repair);
await import(pathToFileURL(temporary).href);

// Keep the public optional config shape. Only the internal default values need
// their known required members when merging a supplied partial section.
const path = 'packages/patchnote/src/config.ts';
let source = readFileSync(path,'utf8');
assert.equal(source.split('export const defaultConfig =').length, 2);
source = source.replace('export const defaultConfig =', 'const baseConfig =');
source = source.replace('} satisfies PatchnoteConfig;', '} satisfies PatchnoteConfig;\nexport const defaultConfig: PatchnoteConfig = baseConfig;');
for (const name of ['expo','syncpack','nix','provenanceCheck','deprecationCheck','git','semanticCommits','filters','lockFileMaintenance']) {
  source = source.replaceAll(`...defaultConfig.${name}`, `...baseConfig.${name}`);
}
writeFileSync(path,source);
const test = 'packages/patchnote/test/lint-boundaries.test.ts';
writeFileSync(test,readFileSync(test,'utf8').replace('defaultConfig.expo.enabled','defaultConfig.expo?.enabled'));
