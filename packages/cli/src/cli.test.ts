import { afterEach, describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { reportFatal } from './cli.js';
import { mergeEnv } from './lib/run.js';

const originalConsoleError = console.error;

afterEach(() => {
  console.error = originalConsoleError;
});

describe('CLI fatal error reporting', () => {
  it('passes structured thrown values to the console without coercion', () => {
    const output: unknown[][] = [];
    console.error = (...args: unknown[]) => {
      output.push(args);
    };
    const failure = {
      message: 'Failed to load Nx plugins',
      errors: [{ message: 'router plugin received an invalid path' }],
    };

    reportFatal(failure);

    expect(output).toEqual([[failure]]);
  });
});

/**
 * An environment for every git process a test starts, its own and the CLI child's: git exports
 * these to whatever it runs (hooks, `rebase --exec`), and inherited they point `git init` and
 * `git rev-parse` at the repository running the tests instead of the fixture.
 */
function gitFreeEnvironment(): Record<string, string> {
  return mergeEnv(undefined, ['GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE', 'GIT_COMMON_DIR']);
}

describe('smoo wrangler cleanup-pr', () => {
  it('scopes a cleanup started in a project directory by the workspace root, naming its manifest', async () => {
    // Real path: git reports the resolved root, and macOS's temporary directory is a symlink.
    const root = await realpath(await mkdtemp(join(tmpdir(), 'smoo-cli-cleanup-')));
    try {
      const init = Bun.spawnSync(['git', 'init', '--quiet'], { cwd: root, env: gitFreeEnvironment() });
      expect(init.exitCode).toBe(0);
      await writeFile(join(root, 'nx.json'), '{}\n');
      await writeFile(join(root, 'package.json'), '{ "name": "@acme/app" }\n');
      const project = join(root, 'packages', 'web');
      await mkdir(project, { recursive: true });
      // A project manifest naming a repository must not stand in for the root's missing one.
      await writeFile(
        join(project, 'package.json'),
        '{ "name": "@acme/web", "repository": "https://github.com/acme/app" }\n',
      );
      // JavaScript, so the transform never looks for a tsconfig beside it.
      const entry = join(root, 'smoo.mjs');
      await writeFile(
        entry,
        `import { runCli } from ${JSON.stringify(join(import.meta.dir, 'cli.ts'))};\nawait runCli();\n`,
      );

      // The source runs only with typia's transform, which the workspace's own bunfig preloads.
      const preload = Bun.resolveSync('@smoothbricks/validation/bun/preload', import.meta.dir);
      const command = ['bun', '--preload', preload, entry, 'wrangler', 'cleanup-pr', '--pr', '7'];
      const child = Bun.spawnSync(command, {
        cwd: project,
        env: gitFreeEnvironment(),
      });

      expect(child.exitCode).toBe(1);
      expect(child.stderr.toString()).toContain(`${join(root, 'package.json')} declares no repository`);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
