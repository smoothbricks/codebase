import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { FsTree, flushChanges } from 'nx/src/generators/tree.js';
import typia from 'typia';
import { applyWorkspaceBoundedTestTargetPolicy } from '../bounded-test-policy.js';
import { managedAssetsRoot } from '../managed-assets.js';
import { applyPackageTargetPolicy } from '../package-target-policy.js';
import { applyReleaseConfigPolicy } from '../release-config-policy.js';
import { renderTypecheckTestFiles } from '../typecheck-test-policy.js';
import { applyWorkspaceConfigPolicy } from '../workspace-config-policy.js';
import { biomeHookArguments, formatJsonWithBiome, jsonFileText, writeJsonFile } from './managed-format.js';
import { inspectManagedPaths } from './paths.js';
import { assertNoManagedConflicts, stageManagedFiles } from './tree.js';

/** What a consumer's repository formats JSON as: the shape this repository's own biome.json declares. */
const SPACED_BIOME_CONFIG = {
  formatter: { enabled: true, indentStyle: 'space', indentWidth: 2, lineWidth: 120, lineEnding: 'lf' },
};

async function makeRepo(config: object = SPACED_BIOME_CONFIG): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-managed-json-'));
  await writeFile(join(root, 'biome.json'), `${JSON.stringify(config)}\n`);
  return root;
}

async function writeSourceJson(path: string, value: unknown): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}

/**
 * The exact command `.git-format-staged.yml` runs on commit for a `*.json` file,
 * read from the managed config and run as a subprocess, so the oracle is the
 * commit hook itself rather than a second call into the code under test.
 */
async function hookCommand(): Promise<string> {
  const config = typia.assert<{ formatters: { biome: { command: string } } }>(
    Bun.YAML.parse(await readFile(join(managedAssetsRoot, 'raw', 'git-format-staged.yml'), 'utf8')),
  );
  return config.formatters.biome.command;
}

async function hookOutput(root: string, path: string, text: string): Promise<string> {
  const command = (await hookCommand()).replaceAll('{}', path);
  const child = spawnSync('sh', ['-c', command], { cwd: root, input: text, encoding: 'utf8' });
  expect(child.status).toBe(0);
  return child.stdout;
}

async function theHookWouldRewrite(root: string, path: string): Promise<boolean> {
  const text = await readFile(join(root, path), 'utf8');
  return (await hookOutput(root, path, text)) !== text;
}

describe('JSON that smoo writes into a managed repository', () => {
  it('keeps a short array on one line, the way the commit hook leaves it', async () => {
    const root = await makeRepo();
    try {
      const text = jsonFileText(join(root, 'tsconfig.test.json'), { compilerOptions: { types: ['bun'] } });

      expect(text).toBe('{\n  "compilerOptions": {\n    "types": ["bun"]\n  }\n}\n');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('is a fixed point of the commit hook', async () => {
    const root = await makeRepo();
    try {
      const value = {
        extends: '../tsconfig.base.json',
        compilerOptions: { types: ['bun'], lib: ['es2024', 'dom'] },
        include: ['src/**/*.test.ts', 'src/**/*.spec.ts', 'src/**/__tests__/**/*.ts', 'src/**/__tests__/**/*.tsx'],
        references: [{ path: '../packages/cli/tsconfig.lib.json' }],
      };
      const text = jsonFileText(join(root, 'tooling/tsconfig.test.json'), value);

      expect(await hookOutput(root, 'tooling/tsconfig.test.json', text)).toBe(text);
      expect(JSON.parse(text)).toEqual(value);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("obeys the consumer's own Biome configuration", async () => {
    const root = await makeRepo({ formatter: { indentStyle: 'tab', lineWidth: 20 } });
    try {
      const text = jsonFileText(join(root, 'package.json'), { files: ['dist', 'src', 'README.md'] });

      expect(text).toBe('{\n\t"files": [\n\t\t"dist",\n\t\t"src",\n\t\t"README.md"\n\t]\n}\n');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('formats a file whose directory does not exist yet', async () => {
    const root = await makeRepo();
    try {
      const text = jsonFileText(join(root, 'packages/new/deep/tsconfig.json'), { include: ['src'] });

      expect(text).toBe('{\n  "include": ["src"]\n}\n');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('writes the formatted bytes to the file', async () => {
    const root = await makeRepo();
    try {
      writeJsonFile(join(root, 'nx.json'), { plugins: ['@smoothbricks/nx-plugin'] });

      expect(await readFile(join(root, 'nx.json'), 'utf8')).toBe('{\n  "plugins": ["@smoothbricks/nx-plugin"]\n}\n');
      expect(await theHookWouldRewrite(root, 'nx.json')).toBe(false);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('runs the command the managed commit hook runs', async () => {
    expect(`biome ${biomeHookArguments('{}').join(' ')}`).toBe((await hookCommand()).replaceAll("'", ''));
  });

  it('refuses to write what a Biome that rejects the repository configuration returned', async () => {
    const root = await makeRepo({ formatter: { lineWidth: 'wide' } });
    try {
      expect(() => jsonFileText(join(root, 'package.json'), { name: 'x' })).toThrow(/lineWidth/);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('passes the text through where no Biome is installed: that tree has no hook output to agree with', async () => {
    const root = await makeRepo();
    const path = process.env.PATH;
    try {
      process.env.PATH = '';
      const text = '{"a":[1]}\n';

      expect(formatJsonWithBiome(join(root, 'a.json'), text)).toBe(text);
    } finally {
      if (path === undefined) delete process.env.PATH;
      else process.env.PATH = path;
      await rm(root, { recursive: true, force: true });
    }
  });
});

describe('every update step that rewrites a JSON file leaves it as the commit hook would', () => {
  it('nx.json through the workspace config policy', async () => {
    const root = await makeRepo();
    try {
      await writeSourceJson(join(root, 'nx.json'), {
        plugins: [],
        targetDefaults: { build: { cache: false, dependsOn: ['^build'] } },
        namedInputs: { default: ['{projectRoot}/**/*'] },
      });

      expect(applyWorkspaceConfigPolicy(root)).toBe(true);
      expect(await theHookWouldRewrite(root, 'nx.json')).toBe(false);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('nx.json through the release config policy', async () => {
    const root = await makeRepo();
    try {
      await writeSourceJson(join(root, 'nx.json'), { namedInputs: { default: ['{projectRoot}/**/*'] } });

      expect(applyReleaseConfigPolicy(root)).toBe(true);
      expect(await theHookWouldRewrite(root, 'nx.json')).toBe(false);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('package.json through the package target policy', async () => {
    const root = await makeRepo();
    try {
      await writeSourceJson(join(root, 'package.json'), {
        name: '@scope/root',
        private: true,
        workspaces: ['packages/*'],
      });
      await writeSourceJson(join(root, 'packages/lib/package.json'), {
        name: '@scope/lib',
        files: ['dist'],
        scripts: { 'build:ts': 'nx run lib:build:ts' },
        nx: {
          name: 'lib',
          targets: {
            'build:ts': {
              executor: 'nx:run-commands',
              options: { command: 'tsc --build tsconfig.lib.json', cwd: '{projectRoot}' },
            },
            build: { executor: 'nx:noop', dependsOn: ['^build', 'build:ts'] },
          },
        },
      });

      expect(applyPackageTargetPolicy(root)).toBe(true);
      expect(await theHookWouldRewrite(root, 'packages/lib/package.json')).toBe(false);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('package.json through the bounded test target policy', async () => {
    const root = await makeRepo();
    try {
      await writeSourceJson(join(root, 'package.json'), {
        name: '@scope/root',
        private: true,
        workspaces: ['packages/*'],
      });
      await writeSourceJson(join(root, 'packages/app/package.json'), {
        name: '@scope/app',
        files: ['dist'],
        scripts: { test: 'bun test --pass-with-no-tests' },
        nx: { name: 'app' },
      });

      expect(applyWorkspaceBoundedTestTargetPolicy(root)).toBe(true);
      expect(await theHookWouldRewrite(root, 'packages/app/package.json')).toBe(false);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

describe('tsconfig.test.json the managed generator writes', () => {
  it('is what the commit hook would leave, so the workflow commit lints clean without the hook', async () => {
    const root = await makeRepo();
    try {
      await writeSourceJson(join(root, 'package.json'), {
        name: '@scope/root',
        private: true,
        workspaces: ['packages/*'],
      });
      await writeSourceJson(join(root, 'tsconfig.base.json'), { compilerOptions: {} });
      await writeSourceJson(join(root, 'packages/app/package.json'), {
        name: '@scope/app',
        scripts: { test: 'bun test' },
      });
      await mkdir(join(root, 'packages/app/src'), { recursive: true });
      await writeFile(join(root, 'packages/app/src/app.test.ts'), 'export {};\n');

      const tree = new FsTree(root, false);
      const files = renderTypecheckTestFiles(tree);
      assertNoManagedConflicts(
        stageManagedFiles(
          tree,
          files,
          inspectManagedPaths(
            root,
            files.map((file) => file.target),
          ),
        ),
      );
      flushChanges(root, tree.listChanges());

      expect(await readFile(join(root, 'packages/app/tsconfig.test.json'), 'utf8')).toContain('"types": ["bun"]');
      expect(await theHookWouldRewrite(root, 'packages/app/tsconfig.test.json')).toBe(false);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

describe('the Biome smoo runs', () => {
  async function repoWithBiome(script: string): Promise<string> {
    const root = await makeRepo();
    await mkdir(join(root, 'node_modules/.bin'), { recursive: true });
    await writeFile(join(root, 'node_modules/.bin/biome'), `#!/bin/sh\n${script}\n`);
    await chmod(join(root, 'node_modules/.bin/biome'), 0o755);
    return root;
  }

  it("is the repository's own, found above the file, before the one on PATH", async () => {
    const root = await repoWithBiome('echo LOCAL');
    try {
      expect(formatJsonWithBiome(join(root, 'packages/app/package.json'), '{}\n')).toBe('LOCAL\n');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('is an error when it exits non-zero, with what it said', async () => {
    const root = await repoWithBiome('echo broken config >&2; exit 3');
    try {
      expect(() => formatJsonWithBiome(join(root, 'a.json'), '{}\n')).toThrow(/exited 3[\s\S]*broken config/);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('is an error when it answers nothing to real input, never an empty file', async () => {
    const root = await repoWithBiome('exit 0');
    try {
      expect(() => formatJsonWithBiome(join(root, 'a.json'), '{}\n')).toThrow(/returned no content/);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
