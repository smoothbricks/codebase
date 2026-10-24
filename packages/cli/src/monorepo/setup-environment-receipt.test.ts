/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: Shell fixture scripts are literal shell text. */
import { describe, expect, it } from 'bun:test';
import { existsSync, readFileSync } from 'node:fs';
import { mkdir, rm, symlink, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { HEALTHY, withManagedRepository } from './__tests__/helpers/shell-entry.js';

describe('Bun installation receipt link admission', () => {
  // Only this fixture's private cache is touched. The managed setup runs a real Bun install;
  // its prepare script plants the on-disk isolated topology the receipt must admit or refuse.
  const fixture =
    "import { appendFileSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';\nimport { dirname, join, relative } from 'node:path';\nconst root = process.cwd();\nconst mode = readFileSync('integrity-mode', 'utf8');\nconst store = join(root, 'node_modules', '.bun');\nconst cache = join(dirname(root), 'private-bun-cache');\nconst parentStore = join(cache, 'fixture-parent@1.0.0');\nconst parent = join(parentStore, 'node_modules', '@fixture', 'parent');\nconst child = join(cache, 'fixture-child@1.0.0', 'node_modules', '@fixture', 'child');\nconst storeAlias = join(store, '@fixture+parent@1.0.0');\nconst childAlias = join(parentStore, 'node_modules', '@fixture', 'child');\nconst hoistAlias = join(store, 'node_modules', '@fixture', 'child');\nrmSync(storeAlias, { force: true });\nrmSync(join(store, 'node_modules', '@fixture'), { recursive: true, force: true });\nrmSync(cache, { recursive: true, force: true });\nmkdirSync(parent, { recursive: true });\nmkdirSync(child, { recursive: true });\nmkdirSync(dirname(hoistAlias), { recursive: true });\nwriteFileSync(join(parent, 'package.json'), JSON.stringify({\n  name: '@fixture/parent', version: '1.0.0', dependencies: { '@fixture/child': '1.0.0' },\n  optionalDependencies: { '@fixture/omitted': '1.0.0' },\n}));\nif (mode !== 'missing-manifest') {\n  writeFileSync(join(child, 'package.json'), mode === 'malformed-manifest' ? '{' : JSON.stringify({ name: '@fixture/child', version: '1.0.0' }));\n}\nsymlinkSync(mode === 'broken-store' ? join(cache, 'missing-parent') : parentStore, storeAlias);\nconst childTarget = mode === 'broken-transitive' ? join(cache, 'missing-child') : child;\nsymlinkSync(relative(dirname(childAlias), childTarget), childAlias);\nif (mode === 'healthy' || mode === 'broken-hoist') {\n  symlinkSync(mode === 'broken-hoist' ? join(cache, 'missing-child') : child, hoistAlias);\n}\nappendFileSync('integrity-installs', 'x\\n');\n";
  const files = { 'integrity-fixture.mjs': fixture, 'integrity-mode': 'healthy' };
  const options = { prepare: 'bun integrity-fixture.mjs', files };
  const CI_SECRETS = { SMOO_NPM_TOKEN: 'registry-value', SMOO_TOKEN: 'shell-value' };

  for (const ci of [false, true]) {
    it(`${ci ? 'CI' : 'local'} refuses each broken dependency topology before writing an install receipt`, async () => {
      await withManagedRepository(options, async ({ root, enterShell: enter }) => {
        const defects = ['broken-store', 'broken-transitive', 'broken-hoist', 'missing-manifest', 'malformed-manifest'];
        expect(defects).toHaveLength(5);
        for (const defect of defects) {
          const label = `${ci ? 'CI' : 'local'} refuses ${defect} before writing an install receipt`;
          // The prepare script rebuilds its topology; retain the unrelated TypeScript API pin.
          await writeFile(join(root, 'integrity-installs'), '');
          await writeFile(join(root, 'integrity-mode'), 'healthy');
          expect(await enter(), label).toEqual(HEALTHY);
          const receipt = join(root, 'node_modules', '.smoo-install');
          await rm(receipt);
          await writeFile(join(root, 'integrity-mode'), defect);
          const refused = await enter(ci ? { ciSecrets: CI_SECRETS } : {});
          const cache = join(dirname(root), 'private-bun-cache');
          const storeAlias = join(root, 'node_modules', '.bun', '@fixture+parent@1.0.0');
          const actualPath =
            defect === 'broken-store'
              ? storeAlias
              : defect === 'broken-hoist'
                ? join(root, 'node_modules', '.bun', 'node_modules', '@fixture', 'child')
                : join(storeAlias, 'node_modules', '@fixture', 'child');
          const expectedTarget =
            defect === 'broken-store'
              ? join(cache, 'missing-parent')
              : defect === 'broken-transitive' || defect === 'broken-hoist'
                ? join(cache, 'missing-child')
                : join(cache, 'fixture-child@1.0.0', 'node_modules', '@fixture', 'child');
          expect(refused.exitCode, label).toBe(ci ? 1 : 0);
          expect(refused.stderr, label).toContain('Bun install integrity:');
          expect(refused.stderr, label).toContain(actualPath);
          expect(refused.stderr, label).toContain(`expected target ${expectedTarget}`);
          expect(refused.stderr, label).toContain(ci ? '--- ERROR:' : '--- WARNING:');
          if (!ci) expect(refused.stderr, label).toContain('The shell is loaded WITHOUT installed dependencies');
          expect(refused.stderr, label).not.toContain('! Failed to install dependencies with frozen lockfile');
          expect(existsSync(receipt), label).toBe(false);
          // The guard is outside CI's frozen-command catch: a refusal cannot trigger another install.
          expect(readFileSync(join(root, 'integrity-installs'), 'utf8'), label).toBe('x\nx\n');
        }
      });
    });
  }

  it('records complete transitive cache links after the normal CI frozen install', async () => {
    await withManagedRepository(options, async ({ root, enterShell: enter }) => {
      expect(await enter()).toEqual(HEALTHY);
      const receipt = join(root, 'node_modules', '.smoo-install');
      await rm(receipt);
      const installed = await enter({ ciSecrets: CI_SECRETS });
      expect(installed.exitCode).toBe(0);
      expect(installed.stderr).not.toContain('Bun install integrity:');
      expect(installed.stderr).not.toContain('git diff after install:');
      expect(existsSync(receipt)).toBe(true);
      expect(readFileSync(join(root, 'integrity-installs'), 'utf8')).toBe('x\nx\n');
    });
  });

  it('records complete scoped/transitive cache links and leaves unchanged entry installed', async () => {
    // An empty dependency graph need not produce bun.lock. Install a real local package so
    // this oracle checks the genuine lock digest without changing the install or receipt contract.
    await withManagedRepository(
      {
        ...options,
        dependencies: { 'fixture-lock-anchor': 'file:vendor/lock-anchor' },
        files: {
          ...files,
          'vendor/lock-anchor/package.json': JSON.stringify({ name: 'fixture-lock-anchor', version: '1.0.0' }),
        },
      },
      async ({ root, enterShell: enter }) => {
        expect(await enter()).toEqual(HEALTHY);
        const receipt = join(root, 'node_modules', '.smoo-install');
        const recorded = readFileSync(receipt);
        expect(JSON.parse(recorded.toString('utf8'))).toEqual({
          inputs: expect.stringMatching(/^[0-9a-f]{64}$/),
          lockfiles: {
            'bun.lock': new Bun.CryptoHasher('sha256').update(readFileSync(join(root, 'bun.lock'))).digest('hex'),
          },
        });
        expect(await enter()).toEqual(HEALTHY);
        expect(readFileSync(receipt)).toEqual(recorded);
        expect(readFileSync(join(root, 'integrity-installs'), 'utf8')).toBe('x\n');
      },
    );
  });

  it('admits each superseded variant a restored cache carried beside its intact replacement', async () => {
    const cases: ReadonlyArray<{ name: string; plant: (root: string) => Promise<void> }> = [
      {
        // A store entry from another machine's install: same package and version as the intact
        // entry, its link aimed at an install cache this machine never had.
        name: 'a superseded store variant whose link dangles',
        plant: async (root) => {
          await symlink(
            join(dirname(root), 'private-bun-cache', 'missing-stale-variant'),
            join(root, 'node_modules', '.bun', '@fixture+parent@1.0.0+stale'),
          );
        },
      },
      {
        // A whole superseded variant: its dependency alias aims at an install cache this
        // machine never had, and nothing in the live tree references it.
        name: 'a superseded parent variant whose own dependency alias dangles',
        plant: async (root) => {
          const stale = join(dirname(root), 'private-bun-cache', 'fixture-parent-stale@1.0.0');
          await mkdir(join(stale, 'node_modules', '@fixture', 'parent'), { recursive: true });
          await writeFile(
            join(stale, 'node_modules', '@fixture', 'parent', 'package.json'),
            JSON.stringify({ name: '@fixture/parent', version: '1.0.0' }),
          );
          await symlink(
            join(dirname(root), 'private-bun-cache', 'missing-child'),
            join(stale, 'node_modules', '@fixture', 'child'),
          );
          await symlink(stale, join(root, 'node_modules', '.bun', '@fixture+parent@1.0.0+stale'));
        },
      },
    ];
    for (const testCase of cases) {
      await withManagedRepository(options, async ({ root, enterShell: enter }) => {
        expect(await enter()).toEqual(HEALTHY);
        const receipt = join(root, 'node_modules', '.smoo-install');
        await rm(receipt);
        await testCase.plant(root);
        const entered = await enter({ ciSecrets: CI_SECRETS });
        expect(entered.exitCode, testCase.name).toBe(0);
        expect(entered.stderr, testCase.name).not.toContain('Bun install integrity:');
        expect(existsSync(receipt), testCase.name).toBe(true);
      });
    }
  });

  it('refuses each planted broken root or store reference', async () => {
    const cases: ReadonlyArray<{ name: string; script: string; plant: readonly string[] }> = [
      {
        name: 'a dangling variant the installed tree still references, however intact its replacement',
        script: 'plant-negative.mjs',
        plant: [
          "import { mkdirSync, symlinkSync } from 'node:fs';",
          "import { dirname, join } from 'node:path';",
          'const root = process.cwd();',
          "const stale = join(root, 'node_modules', '.bun', '@fixture+parent@1.0.0+stale');",
          "symlinkSync(join(dirname(root), 'private-bun-cache', 'missing-stale'), stale);",
          "mkdirSync(join(root, 'node_modules', '@fixture'), { recursive: true });",
          "symlinkSync(join('..', '.bun', '@fixture+parent@1.0.0+stale', 'node_modules', '@fixture', 'parent'), join(root, 'node_modules', '@fixture', 'parent'));",
        ],
      },
      {
        name: 'a live alias aimed at a version-named missing target, however intact its namesake',
        script: 'plant-alias.mjs',
        plant: [
          "import { rmSync, symlinkSync } from 'node:fs';",
          "import { dirname, join } from 'node:path';",
          'const root = process.cwd();',
          "const alias = join(root, 'node_modules', '.bun', '@fixture+parent@1.0.0', 'node_modules', '@fixture', 'child');",
          'rmSync(alias, { force: true });',
          "symlinkSync(join(dirname(root), 'private-bun-cache', 'missing', '@fixture+parent@1.0.0'), alias);",
        ],
      },
      {
        name: 'a root alias aimed into a store entry that does not exist, however intact its namesake',
        script: 'plant-ghost.mjs',
        plant: [
          "import { mkdirSync, symlinkSync } from 'node:fs';",
          "import { join } from 'node:path';",
          'const root = process.cwd();',
          "mkdirSync(join(root, 'node_modules', '@fixture'), { recursive: true });",
          "symlinkSync(join('..', '.bun', '@fixture+parent@1.0.0+nonexistent', 'node_modules', '@fixture', 'parent'), join(root, 'node_modules', '@fixture', 'parent'));",
        ],
      },
      {
        name: 'a root alias aimed at an absolute install-cache target that does not exist',
        script: 'plant-external.mjs',
        plant: [
          "import { mkdirSync, symlinkSync } from 'node:fs';",
          "import { dirname, join } from 'node:path';",
          'const root = process.cwd();',
          "const globalEntry = join(dirname(root), 'private-bun-cache', 'links', '@fixture+parent@1.0.0+old');",
          "symlinkSync(globalEntry, join(root, 'node_modules', '.bun', '@fixture+parent@1.0.0+old'));",
          "mkdirSync(join(root, 'node_modules', '@fixture'), { recursive: true });",
          "symlinkSync(join(globalEntry, 'node_modules', '@fixture', 'parent'), join(root, 'node_modules', '@fixture', 'parent'));",
        ],
      },
      {
        name: 'a superseded variant under a suffixed cache basename, reached by an absolute root alias',
        script: 'plant-suffixed-cache.mjs',
        plant: [
          "import { mkdirSync, symlinkSync, writeFileSync } from 'node:fs';",
          "import { dirname, join } from 'node:path';",
          'const root = process.cwd();',
          "const cacheEntry = join(dirname(root), 'private-bun-cache', 'links', '@fixture+parent@1.0.0+stale-CACHEHASH');",
          "mkdirSync(join(cacheEntry, 'node_modules', '@fixture', 'parent'), { recursive: true });",
          "writeFileSync(join(cacheEntry, 'node_modules', '@fixture', 'parent', 'package.json'), JSON.stringify({ name: '@fixture/parent', version: '1.0.0' }));",
          "symlinkSync(join(dirname(root), 'private-bun-cache', 'missing-child'), join(cacheEntry, 'node_modules', '@fixture', 'child'));",
          "symlinkSync(cacheEntry, join(root, 'node_modules', '.bun', '@fixture+parent@1.0.0+stale'));",
          "mkdirSync(join(root, 'node_modules', '@fixture'), { recursive: true });",
          "symlinkSync(join(cacheEntry, 'node_modules', '@fixture', 'parent'), join(root, 'node_modules', '@fixture', 'parent'));",
        ],
      },
      {
        name: 'a superseded variant reached through an intermediate external alias',
        script: 'plant-chain.mjs',
        plant: [
          "import { mkdirSync, symlinkSync, writeFileSync } from 'node:fs';",
          "import { dirname, join } from 'node:path';",
          'const root = process.cwd();',
          "const cacheEntry = join(dirname(root), 'private-bun-cache', 'links', '@fixture+parent@1.0.0+stale-CACHEHASH');",
          "mkdirSync(join(cacheEntry, 'node_modules', '@fixture', 'parent'), { recursive: true });",
          "writeFileSync(join(cacheEntry, 'node_modules', '@fixture', 'parent', 'package.json'), JSON.stringify({ name: '@fixture/parent', version: '1.0.0' }));",
          "symlinkSync(join(dirname(root), 'private-bun-cache', 'missing-child'), join(cacheEntry, 'node_modules', '@fixture', 'child'));",
          "symlinkSync(cacheEntry, join(root, 'node_modules', '.bun', '@fixture+parent@1.0.0+stale'));",
          "const intermediate = join(dirname(root), 'private-bun-cache', 'intermediate');",
          'mkdirSync(intermediate, { recursive: true });',
          "symlinkSync(join(cacheEntry, 'node_modules', '@fixture', 'parent'), join(intermediate, 'parent'));",
          "mkdirSync(join(root, 'node_modules', '@fixture'), { recursive: true });",
          "symlinkSync(join(intermediate, 'parent'), join(root, 'node_modules', '@fixture', 'parent'));",
        ],
      },
      {
        name: 'a dangling variant reached only through a symlinked root scope',
        script: 'plant-scope-link.mjs',
        plant: [
          "import { mkdirSync, readFileSync, symlinkSync, writeFileSync } from 'node:fs';",
          "import { dirname, join } from 'node:path';",
          'const root = process.cwd();',
          "const stale = join(dirname(root), 'private-bun-cache', 'fixture-parent-stale@1.0.0');",
          "mkdirSync(join(stale, 'node_modules', '@fixture', 'parent'), { recursive: true });",
          "writeFileSync(join(stale, 'node_modules', '@fixture', 'parent', 'package.json'), JSON.stringify({ name: '@fixture/parent', version: '1.0.0' }));",
          "symlinkSync(join(dirname(root), 'private-bun-cache', 'missing-child'), join(stale, 'node_modules', '@fixture', 'child'));",
          "symlinkSync(stale, join(root, 'node_modules', '.bun', '@fixture+parent@1.0.0+stale'));",
          "const scope = join(dirname(root), 'private-bun-cache', 'linked-scope');",
          'mkdirSync(scope, { recursive: true });',
          "symlinkSync(join(root, 'node_modules', '.bun', '@fixture+parent@1.0.0+stale', 'node_modules', '@fixture', 'parent'), join(scope, 'parent'));",
          "JSON.parse(readFileSync(join(scope, 'parent', 'package.json'), 'utf8'));",
          "symlinkSync(scope, join(root, 'node_modules', '@linked'));",
        ],
      },
      {
        name: 'a root package alias that resolves to a regular file, not a package directory',
        script: 'plant-file-alias.mjs',
        plant: [
          "import { mkdirSync, symlinkSync } from 'node:fs';",
          "import { join } from 'node:path';",
          'const root = process.cwd();',
          "mkdirSync(join(root, 'node_modules', '@fixture'), { recursive: true });",
          "symlinkSync(join(root, 'integrity-mode'), join(root, 'node_modules', '@fixture', 'filepkg'));",
        ],
      },
    ];
    for (const testCase of cases) {
      await withManagedRepository(
        {
          ...options,
          prepare: `bun integrity-fixture.mjs && bun ${testCase.script}`,
          files: { ...files, [testCase.script]: [...testCase.plant, ''].join('\n') },
        },
        async ({ enterShell: enter }) => {
          const refused = await enter({ ciSecrets: CI_SECRETS });
          expect(refused.exitCode, testCase.name).toBe(1);
          expect(refused.stderr, testCase.name).toContain('Bun install integrity:');
        },
      );
    }
  });

  it('refuses a workspace member alias aimed at a missing external target', async () => {
    await withManagedRepository(
      {
        ...options,
        workspaces: ['packages/*'],
        prepare: 'bun integrity-fixture.mjs && bun plant-member-alias.mjs',
        files: {
          ...files,
          'packages/member/package.json': JSON.stringify({ name: 'member', version: '0.0.0' }),
          'plant-member-alias.mjs': [
            "import { mkdirSync, symlinkSync } from 'node:fs';",
            "import { dirname, join } from 'node:path';",
            'const root = process.cwd();',
            "mkdirSync(join(root, 'packages', 'member', 'node_modules', '@fixture'), { recursive: true });",
            "symlinkSync(join(dirname(root), 'private-bun-cache', 'missing-member-target'), join(root, 'packages', 'member', 'node_modules', '@fixture', 'memberpkg'));",
            '',
          ].join('\n'),
        },
      },
      async ({ enterShell: enter }) => {
        const refused = await enter({ ciSecrets: CI_SECRETS });
        expect(refused.exitCode).toBe(1);
        expect(refused.stderr).toContain('Bun install integrity:');
      },
    );
  });

  it('admits a root scope that is itself a symlink to a directory of packages', async () => {
    await withManagedRepository(
      {
        ...options,
        prepare: 'bun integrity-fixture.mjs && bun plant-scope.mjs',
        files: {
          ...files,
          'plant-scope.mjs': [
            "import { mkdirSync, symlinkSync, writeFileSync } from 'node:fs';",
            "import { dirname, join } from 'node:path';",
            'const root = process.cwd();',
            "const scope = join(dirname(root), 'private-bun-cache', 'linked-scope');",
            "mkdirSync(join(scope, 'pkg'), { recursive: true });",
            "writeFileSync(join(scope, 'pkg', 'package.json'), JSON.stringify({ name: '@linked/pkg', version: '1.0.0' }));",
            "symlinkSync(scope, join(root, 'node_modules', '@linked'));",
            '',
          ].join('\n'),
        },
      },
      async ({ root, enterShell: enter }) => {
        const entered = await enter({ ciSecrets: CI_SECRETS });
        expect(entered.exitCode).toBe(0);
        expect(entered.stderr).not.toContain('Bun install integrity:');
        expect(existsSync(join(root, 'node_modules', '.smoo-install'))).toBe(true);
      },
    );
  });
});
