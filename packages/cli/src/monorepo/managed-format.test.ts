import { describe, expect, it } from 'bun:test';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import typia from 'typia';
import { applyManagedFilesForContext, type ManagedFileContext } from './managed-files.js';
import { BIOME_OWNED_EXTENSIONS, formatManagedContent } from './managed-format.js';

const PRETTIER_CLI = join(dirname(fileURLToPath(import.meta.resolve('prettier'))), 'bin', 'prettier.cjs');
const GIT_FORMAT_STAGED_CONFIG = join(import.meta.dir, '..', '..', 'managed', 'raw', 'git-format-staged.yml');

/**
 * A consumer whose Prettier config disagrees with the generator's raw
 * rendering — a common shape: single quotes everywhere, double quotes in
 * workflow YAML. Nothing about that is unusual, and it is not this tool's
 * business to argue with it.
 */
const CONSUMER_PRETTIERRC = `${JSON.stringify(
  {
    proseWrap: 'always',
    printWidth: 120,
    tabWidth: 2,
    singleQuote: true,
    overrides: [{ files: '.github/workflows/*.yml', options: { singleQuote: false } }],
  },
  null,
  2,
)}\n`;

const context = (overrides: Partial<ManagedFileContext> = {}): ManagedFileContext => ({
  hasReleasePackages: true,
  hasStagingDeployTargets: false,
  hasProductionDeployTargets: true,
  hasProductionPushDeployTargets: false,
  hasBrowserTestTargets: false,
  hasE2eDeploymentTargets: false,
  ciPushBranches: ['main'],
  ciRunsOn: 'ubuntu-latest',
  macosRunsOn: 'macos-latest',
  ciDeploySecrets: { CLOUDFLARE_API_TOKEN: 'CLOUDFLARE_API_TOKEN', DEPLOY_KEY: 'DEPLOY_KEY' },
  ciE2eSecrets: {},
  nodeModulesCacheKey: 'key',
  repoName: '@acme/widgets',
  platformTargetGlobs: ['*-macos', '*-linux'],
  releasePlatformTargetGlobs: ['*-macos', '*-linux'],
  macosPlatformArchitectures: ['arm64'],
  crossTestArchives: [],
  ...overrides,
});

async function makeConsumerRepo(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-managed-format-'));
  await writeFile(join(root, '.prettierrc'), CONSUMER_PRETTIERRC);
  return root;
}

/**
 * The exact command `.git-format-staged.yml` runs on commit, as a subprocess,
 * so the oracle is the real formatter rather than a second call into the code
 * under test. `--list-different` names every file the hook would rewrite.
 */
async function filesTheCommitHookWouldRewrite(root: string, targets: readonly string[]): Promise<string[]> {
  const child = Bun.spawn([process.execPath, PRETTIER_CLI, '--ignore-unknown', '--list-different', ...targets], {
    cwd: root,
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  // 0 = all formatted, 1 = some differ. Anything else means Prettier itself failed.
  if (exitCode !== 0 && exitCode !== 1) {
    throw new Error(`prettier --list-different failed (${exitCode}): ${stderr}`);
  }
  return stdout.split('\n').filter((line) => line.length > 0);
}

const YAML_TARGETS = [
  '.github/workflows/ci.yml',
  '.github/workflows/publish.yml',
  '.github/workflows/managed-files.yml',
  '.github/actions/setup-devenv/action.yml',
  '.github/actions/cache-nix-devenv/action.yml',
  '.github/actions/save-nix-devenv/action.yml',
  '.github/actions/cache-node-modules/action.yml',
  '.github/actions/cache-ttsc-plugins/action.yml',
  '.github/actions/cache-nx/action.yml',
  '.git-format-staged.yml',
] as const;

describe('managed files in a consumer whose formatter disagrees with the generator', () => {
  it('writes bytes the consumer commit hook leaves alone', async () => {
    const root = await makeConsumerRepo();
    try {
      await applyManagedFilesForContext(root, 'update', context());

      // The generator's raw rendering single-quotes these; this repository's
      // config wants double quotes in workflow YAML. Before the writer went
      // through the consumer's formatter, the hook rewrote publish.yml on
      // every commit and `update` wrote it straight back.
      expect(await filesTheCommitHookWouldRewrite(root, YAML_TARGETS)).toEqual([]);
      expect(await readFile(join(root, '.github/workflows/publish.yml'), 'utf8')).toContain('default: ""');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  }, 30_000);

  it('reports no drift on a tree the consumer formatter has already touched', async () => {
    const root = await makeConsumerRepo();
    try {
      await applyManagedFilesForContext(root, 'update', context());
      const results = await applyManagedFilesForContext(root, 'check', context());

      expect(results.filter((result) => result.action === 'drifted')).toEqual([]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  }, 30_000);

  it('still reports and names a file whose content really changed', async () => {
    const root = await makeConsumerRepo();
    try {
      await applyManagedFilesForContext(root, 'update', context());
      const publishYml = join(root, '.github/workflows/publish.yml');
      const written = await readFile(publishYml, 'utf8');
      expect(written).toContain('DEPLOY_KEY: ${{ secrets.DEPLOY_KEY }}');
      // One meaningful edit: a secret the deploy step needs, dropped.
      await writeFile(
        publishYml,
        written
          .split('\n')
          .filter((line) => !line.includes('DEPLOY_KEY'))
          .join('\n'),
      );

      const results = await applyManagedFilesForContext(root, 'check', context());

      expect(results.filter((result) => result.action === 'drifted').map((result) => result.target)).toEqual([
        '.github/workflows/publish.yml',
      ]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  }, 30_000);

  it('is idempotent: a second update writes the same bytes, and so does the formatter', async () => {
    const root = await makeConsumerRepo();
    try {
      await applyManagedFilesForContext(root, 'update', context());
      const first = await Promise.all(YAML_TARGETS.map((target) => readFile(join(root, target), 'utf8')));

      await applyManagedFilesForContext(root, 'update', context());
      const second = await Promise.all(YAML_TARGETS.map((target) => readFile(join(root, target), 'utf8')));
      expect(second).toEqual(first);

      const child = Bun.spawn([process.execPath, PRETTIER_CLI, '--ignore-unknown', '--write', ...YAML_TARGETS], {
        cwd: root,
        stdout: 'ignore',
        stderr: 'pipe',
      });
      expect(await child.exited).toBe(0);
      const formatted = await Promise.all(YAML_TARGETS.map((target) => readFile(join(root, target), 'utf8')));
      expect(formatted).toEqual(first);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  }, 30_000);
});

describe('which formatter owns a managed target', () => {
  it('leaves the extensions the commit hook routes to Biome untouched', async () => {
    const config = Bun.YAML.parse(await readFile(GIT_FORMAT_STAGED_CONFIG, 'utf8'));
    const patterns = typia.assert<{ formatters: { biome: { patterns: string[] } } }>(config).formatters.biome.patterns;

    expect([...BIOME_OWNED_EXTENSIONS].sort()).toEqual(patterns.map((pattern) => pattern.replace('*', '')).sort());
  });

  it('passes content through for targets no Prettier parser claims', async () => {
    const root = await makeConsumerRepo();
    try {
      // Alejandra owns .nix and the hook leaves .sh to `--ignore-unknown`;
      // rewriting either here would invent drift rather than remove it.
      const nix = '{...}: {\n    programs.foo.enable   = true;\n}\n';
      const shell = "#!/usr/bin/env bash\nset -euo pipefail\necho    'spaced'\n";
      const typescript = "export const  value =   'kept';\n";

      expect(await formatManagedContent(root, 'tooling/direnv/devenv.smoo.nix', nix)).toBe(nix);
      expect(await formatManagedContent(root, 'tooling/git-hooks/pre-commit.sh', shell)).toBe(shell);
      expect(await formatManagedContent(root, 'tooling/direnv/setup-environment.ts', typescript)).toBe(typescript);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('obeys a repository that tells Prettier to keep its hands off a path', async () => {
    const root = await makeConsumerRepo();
    try {
      await writeFile(join(root, '.prettierignore'), '.github/workflows/publish.yml\n');
      const raw = "on:\n  workflow_dispatch:\n    inputs:\n      bump:\n        default: ''\n";

      expect(await formatManagedContent(root, '.github/workflows/publish.yml', raw)).toBe(raw);
      expect(await formatManagedContent(root, '.github/workflows/ci.yml', raw)).toContain('default: ""');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
