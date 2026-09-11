import { describe, expect, it } from 'bun:test';
import { existsSync, readFileSync } from 'node:fs';
import { copyFile, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

/**
 * The measurement THE RULE exists for, taken against the real script.
 *
 * `tooling/direnv/setup-environment.ts` is what a direnv reload runs, and the
 * cost the grouped rule removes is one provider invocation per shell entry
 * per credential. Counting that needs the whole script — the resolver alone
 * cannot show what shell entry does with it — so this builds a repository the
 * way smoo manages one and runs the script in it, three times, with the
 * declared provider commands pointed at a counter.
 */

const MANAGED = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..', 'managed', 'raw', 'tooling');

interface ShellEntry {
  readonly exitCode: number;
  readonly stderr: string;
}

interface Repository {
  readonly root: string;
  /** Runs the real setup-environment.ts exactly as the managed devenv shell does. */
  readonly enterShell: () => Promise<ShellEntry>;
  /** Provider invocations recorded so far, per declared variable. */
  readonly invocations: (name: string) => number;
}

/**
 * A managed repository on disk: the two managed direnv scripts, the git hooks
 * they link, a `.npmrc` that interpolates one declared variable, and a fake
 * TypeScript API package so the script's post-install pin check finds what it
 * looks for. Nothing here is a stub of the code under test — only of the
 * repository it runs in.
 */
async function withManagedRepository(
  options: { prepare?: string },
  run: (repository: Repository) => Promise<void>,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-shell-entry-'));
  const ledgers = await mkdtemp(join(tmpdir(), 'smoo-shell-entry-ledger-'));
  try {
    // `.devenv/` is devenv's own state directory, and the setup lock is
    // created inside it: a managed repository always has one by the time
    // this script runs.
    await mkdir(join(root, 'tooling', 'direnv', '.devenv'), { recursive: true });
    await mkdir(join(root, 'tooling', 'git-hooks'), { recursive: true });
    for (const name of ['setup-environment.ts', 'secret-references.ts']) {
      await copyFile(join(MANAGED, 'direnv', name), join(root, 'tooling', 'direnv', name));
    }
    for (const hook of ['pre-commit', 'commit-msg', 'pre-push']) {
      await writeFile(join(root, 'tooling', 'git-hooks', `${hook}.sh`), '#!/usr/bin/env bash\nexit 0\n');
    }
    await writeFile(
      join(root, 'package.json'),
      JSON.stringify({
        name: 'fixture',
        version: '0.0.0',
        private: true,
        ...(options.prepare === undefined ? {} : { scripts: { prepare: options.prepare } }),
        smoo: {
          secrets: {
            SMOO_NPM_TOKEN: { command: counter('SMOO_NPM_TOKEN', ledgers) },
            SMOO_TOKEN: { command: counter('SMOO_TOKEN', ledgers) },
          },
        },
      }),
    );
    await writeFile(
      join(root, '.npmrc'),
      '@acme:registry=https://npm.example.net\n//npm.example.net/:_authToken=${SMOO_NPM_TOKEN}\n',
    );
    // The post-install TypeScript pin is not what this measures, and an
    // install with no dependencies leaves nothing for it to find.
    const api = join(root, 'node_modules', '.bun', 'typescript@6.0.3', 'node_modules', 'typescript');
    await mkdir(api, { recursive: true });
    await writeFile(
      join(api, 'package.json'),
      JSON.stringify({ name: 'typescript', version: '6.0.3', main: 'index.js' }),
    );
    await writeFile(join(api, 'index.js'), "module.exports = { version: '6.0.3', readConfigFile: () => ({}) };\n");
    await git(root, ['init', '--quiet']);
    await run({
      root,
      enterShell: async () => enterShell(root),
      invocations: (name) => {
        const path = join(ledgers, name);
        return existsSync(path)
          ? readFileSync(path, 'utf8')
              .split('\n')
              .filter((line) => line.length > 0).length
          : 0;
      },
    });
  } finally {
    await rm(root, { recursive: true, force: true });
    await rm(ledgers, { recursive: true, force: true });
  }
}

/** A declared provider command that records each run and prints a value. */
function counter(name: string, ledgers: string): [string, ...string[]] {
  return [
    process.execPath,
    '-e',
    `const fs = require('node:fs');fs.appendFileSync(${JSON.stringify(join(ledgers, name))}, '1\\n');` +
      `process.stdout.write(${JSON.stringify(`${name}-value`)})`,
  ];
}

async function git(cwd: string, args: readonly string[]): Promise<void> {
  const proc = Bun.spawn({ cmd: ['git', ...args], cwd, stdout: 'ignore', stderr: 'pipe', stdin: 'ignore' });
  const [stderr, exitCode] = await Promise.all([new Response(proc.stderr).text(), proc.exited]);
  if (exitCode !== 0) throw new Error(`git ${args.join(' ')} failed: ${stderr}`);
}

/**
 * One shell entry: `bun "$DEVENV_ROOT/setup-environment.ts"`, which is
 * verbatim what the managed devenv `enterShell` runs. The environment carries
 * only what a developer machine has, so neither the CI branch nor the cowshed
 * branch can decide this run.
 */
async function enterShell(root: string): Promise<ShellEntry> {
  const proc = Bun.spawn({
    cmd: ['bun', join(root, 'tooling', 'direnv', 'setup-environment.ts')],
    cwd: root,
    env: {
      PATH: process.env['PATH'],
      HOME: process.env['HOME'],
      DEVENV_ROOT: join(root, 'tooling', 'direnv'),
    },
    stdout: 'pipe',
    stderr: 'pipe',
    stdin: 'ignore',
  });
  const [stderr, exitCode] = await Promise.all([
    new Response(proc.stderr).text(),
    (async () => {
      await new Response(proc.stdout).text();
      return proc.exited;
    })(),
  ]);
  return { exitCode, stderr };
}

describe('what shell entry costs, in provider invocations', () => {
  it('runs no provider for a registry credential across three shell entries, and one per entry for a shell secret', async () => {
    await withManagedRepository({}, async ({ enterShell: enter, invocations }) => {
      for (let entry = 0; entry < 3; entry += 1) {
        const { exitCode, stderr } = await enter();
        expect({ entry, exitCode, stderr }).toEqual({ entry, exitCode: 0, stderr: '' });
      }

      // The whole point, as a count: the registry credential's provider is
      // never asked, so there is no credential prompt on any reload.
      expect(invocations('SMOO_NPM_TOKEN')).toBe(0);
      // And nothing else changed: a shell secret still resolves once per
      // shell entry, exactly as it did before groups existed.
      expect(invocations('SMOO_TOKEN')).toBe(3);
    });
  });

  it('names the exact grouped command when the install it ran fails', async () => {
    // A failing root prepare script fails `bun install` locally without
    // needing a registry: the degraded path is what prints the deferrals.
    await withManagedRepository({ prepare: 'exit 1' }, async ({ enterShell: enter, invocations }) => {
      const { exitCode, stderr } = await enter();

      // A local failure must not take the shell down with it.
      expect(exitCode).toBe(0);
      expect(stderr).toContain('SMOO_NPM_TOKEN (registry)');
      expect(stderr).toContain('smoo secrets run registry bun install');
      expect(invocations('SMOO_NPM_TOKEN')).toBe(0);
    });
  });
});
