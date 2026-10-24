import { describe, expect, it } from 'bun:test';
import { existsSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { withManagedRepository } from './__tests__/helpers/shell-entry.js';

/**
 * What shell entry does when the admission in installed-bun-links.ts refuses what an install left in node_modules.
 * installed-bun-links.test.ts decides every topology the admission accepts or refuses, down to the link it stops at.
 * The real install here only runs the root prepare script that plants one dangling alias in a workspace member, so the
 * refusal also shows that the entry hands its workspace members to the admission.
 */
describe('Bun installation receipt link admission', () => {
  const CI_SECRETS = { SMOO_NPM_TOKEN: 'registry-value', SMOO_TOKEN: 'shell-value' };
  // Idempotent, because the entries below install into the same repository more than once.
  const plant = [
    "import { appendFileSync, mkdirSync, rmSync, symlinkSync } from 'node:fs';",
    "import { dirname, join } from 'node:path';",
    'const root = process.cwd();',
    "const alias = join(root, 'packages', 'member', 'node_modules', '@fixture', 'memberpkg');",
    'rmSync(alias, { force: true });',
    'mkdirSync(dirname(alias), { recursive: true });',
    "symlinkSync(join(dirname(root), 'private-bun-cache', 'missing-member-target'), alias);",
    "appendFileSync('integrity-installs', 'x\\n');",
    '',
  ].join('\n');

  it('refuses a broken tree before writing an install receipt: CI fails the entry after one install, a local entry loads the shell with a warning', async () => {
    await withManagedRepository(
      {
        workspaces: ['packages/*'],
        prepare: 'bun plant-member-alias.mjs',
        files: {
          'packages/member/package.json': JSON.stringify({ name: 'member', version: '0.0.0' }),
          'plant-member-alias.mjs': plant,
        },
      },
      async ({ root, enterShell: enter }) => {
        const receipt = join(root, 'node_modules', '.smoo-install');
        const installs = () => readFileSync(join(root, 'integrity-installs'), 'utf8');
        const alias = join(root, 'packages', 'member', 'node_modules', '@fixture', 'memberpkg');
        const missing = join(dirname(root), 'private-bun-cache', 'missing-member-target');
        const refusal = ['Bun install integrity:', alias, `expected target ${missing}`];

        const ci = await enter({ ciSecrets: CI_SECRETS });
        expect(ci.exitCode).toBe(1);
        for (const said of ['--- ERROR:', ...refusal]) expect(ci.stderr).toContain(said);
        expect(ci.stderr).not.toContain('! Failed to install dependencies with frozen lockfile');
        expect(existsSync(receipt)).toBe(false);
        // The guard is outside CI's frozen-command catch: a refusal cannot trigger another install.
        expect(installs()).toBe('x\n');

        // A local entry has no receipt to find current, so it installs and is refused too, but it must not take the
        // shell down with it: it says so and loads the shell.
        const local = await enter();
        expect(local.exitCode).toBe(0);
        for (const said of ['--- WARNING:', 'The shell is loaded WITHOUT installed dependencies', ...refusal]) {
          expect(local.stderr).toContain(said);
        }
        expect(existsSync(receipt)).toBe(false);
        expect(installs()).toBe('x\nx\n');
      },
    );
  });
});
