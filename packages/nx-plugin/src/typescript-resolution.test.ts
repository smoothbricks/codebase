import { expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdir, realpath, symlink, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { join } from 'node:path';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

/**
 * The project graph must not depend on which `typescript` Bun's isolated linker hoists. A package that requires
 * `typescript` without declaring it (as @nx/js does) gets the copy in node_modules/.bun/node_modules, and beside the
 * `@typescript/native` alias that can be TypeScript 7, whose package exports no compiler API. A publish runner hoisted
 * it once: every project failed with "Cannot read properties of undefined (reading 'readFile')".
 *
 * The preload hoists TypeScript 7 for every undeclared `typescript` request, as that runner did, and leaves declared
 * ones and path-anchored resolutions (Nx's resolveFromWorkspace) alone.
 */
const repositoryRoot = join(import.meta.dir, '../../..');
const nxEntry = join(repositoryRoot, 'node_modules/.bin/nx');
const require = createRequire(join(repositoryRoot, 'package.json'));

const HOIST_TYPESCRIPT_7 = `
const Module = require('node:module');
const { existsSync, readFileSync } = require('node:fs');
const { dirname, join } = require('node:path');
const native = ${JSON.stringify(require.resolve('@typescript/native'))};
function declaresTypescript(file) {
  for (let directory = dirname(file); directory !== dirname(directory); directory = dirname(directory)) {
    const manifest = join(directory, 'package.json');
    if (!existsSync(manifest)) continue;
    const json = JSON.parse(readFileSync(manifest, 'utf8'));
    return ['dependencies', 'peerDependencies', 'optionalDependencies'].some((field) => json[field]?.typescript);
  }
  return false;
}
const resolve = Module._resolveFilename;
Module._resolveFilename = function (request, parent, isMain, options) {
  if (request === 'typescript' && options?.paths === undefined && parent?.filename && !declaresTypescript(parent.filename)) {
    return native;
  }
  return resolve.call(this, request, parent, isMain, options);
};
`;

for (const isolate of ['false', 'true']) {
  it(`reads a test tsconfig whichever typescript is hoisted (isolated plugins: ${isolate})`, async () => {
    await withNxFixture('nx-typescript-hoist-', async ({ workspace, socketDirectory }) => {
      await symlink(await realpath(join(repositoryRoot, 'node_modules')), join(workspace, 'node_modules'), 'dir');
      await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'hoist', private: true }));
      await writeFile(join(workspace, 'nx.json'), JSON.stringify({ plugins: ['@smoothbricks/nx-plugin'] }));
      // An ESLint config makes the plugin read the test program to key lint on its sources.
      await writeFile(join(workspace, 'eslint.config.mjs'), 'export default [];\n');
      await mkdir(join(workspace, 'packages/example/src'), { recursive: true });
      await writeFile(
        join(workspace, 'packages/example/package.json'),
        JSON.stringify({ name: 'example', private: true, scripts: { test: 'bun test' } }),
      );
      await writeFile(join(workspace, 'packages/example/tsconfig.test.json'), '{"include":["src/**/*.test.ts"]}\n');
      await writeFile(join(workspace, 'packages/example/src/a.test.ts'), 'export {};\n');
      const preload = join(workspace, 'hoist-typescript-7.cjs');
      await writeFile(preload, HOIST_TYPESCRIPT_7);

      const ran = spawnSync('node', [nxEntry, 'show', 'project', 'example', '--json'], {
        cwd: workspace,
        env: {
          ...fixtureNxEnv(workspace, socketDirectory),
          NX_ISOLATE_PLUGINS: isolate,
          NODE_OPTIONS: `--require ${preload}`,
        },
        encoding: 'utf8',
      });
      expect(ran.status, `${ran.stdout}${ran.stderr}`).toBe(0);
      // Lint keys the test program's sources, which only reading tsconfig.test.json yields.
      expect(ran.stdout).toContain('{workspaceRoot}/packages/example/src/a.test.ts');
    });
  });
}
