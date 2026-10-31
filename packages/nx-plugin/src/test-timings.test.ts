import { afterEach, describe, expect, it, spyOn } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import typia from 'typia';

import { runtimeInputValue } from './__tests__/runtime-input.js';
import shardTimingsExecutor from './executors/shard-timings/executor.js';
import {
  bunTestFileNamesInput,
  refreshTestTimingsCommand,
  type TimingManifest,
  timingsCoverage,
  timingsShardedSuite,
} from './test-timings.js';

const SHARD = (n: number) =>
  `bun test --timeout=0 --timings=test-timings.json --shard=${n}/2 --path-ignore-patterns='**/*-host.test.ts'`;
const SUITE_ARGS = ['--timeout=0', '--timings=test-timings.json', '--path-ignore-patterns=**/*-host.test.ts'];

function shardTargets(command: (n: number) => string = SHARD): Record<string, unknown> {
  return {
    test: { executor: 'nx:noop', dependsOn: ['test-shard1', 'test-shard2'] },
    'test-shard1': {
      executor: '@smoothbricks/nx-plugin:bounded-exec',
      options: { command: command(1), cwd: '{projectRoot}' },
    },
    'test-shard2': {
      executor: '@smoothbricks/nx-plugin:bounded-exec',
      options: { command: command(2), cwd: '{projectRoot}' },
    },
  };
}

describe('timingsShardedSuite', () => {
  it('reads one suite from shard targets that differ only in --shard', () => {
    expect(timingsShardedSuite('packages/example', shardTargets())).toEqual({
      ok: true,
      suite: { targets: ['test-shard1', 'test-shard2'], cwd: 'packages/example', args: SUITE_ARGS },
    });
  });

  it('finds no suite where shards balance by count, without a manifest', () => {
    expect(
      timingsShardedSuite(
        'packages/example',
        shardTargets((n) => `bun test --shard=${n}/2`),
      ),
    ).toEqual({
      ok: true,
      suite: null,
    });
  });

  it('refuses shards whose files cannot be read off their command', () => {
    const positional = timingsShardedSuite(
      'packages/example',
      shardTargets((n) => `bun test --timings test-timings.json --shard=${n}/2`),
    );
    expect(positional).toEqual({
      ok: false,
      error: expect.stringContaining('passes `test-timings.json` positionally'),
    });
    const shell = timingsShardedSuite(
      'packages/example',
      shardTargets((n) => `bun test --timings=t.json --shard=${n}/2 --path-ignore-patterns=**/*-host.test.ts`),
    );
    expect(shell).toEqual({ ok: false, error: expect.stringContaining('shell syntax beyond quoting') });
  });

  it('refuses shards of what are really two suites', () => {
    const result = timingsShardedSuite(
      'packages/example',
      shardTargets((n) => (n === 1 ? SHARD(1) : `bun test --timings=test-timings.json --shard=${n}/2`)),
    );
    expect(result).toEqual({ ok: false, error: expect.stringContaining('differ in more than --shard') });
  });
});

describe('shard-timings check', () => {
  let root: string | undefined;
  afterEach(async () => {
    if (root !== undefined) await rm(root, { recursive: true, force: true });
    root = undefined;
  });

  async function fixture(timed: readonly string[]): Promise<string> {
    root = await realpath(await mkdtemp(join(tmpdir(), 'smoothbricks-shard-timings-')));
    const files = [
      'src/a.test.ts',
      'src/b_spec.mts',
      'src/nested/c.test.tsx',
      // Never run by these shards: ignored, a dependency, hidden, or not a test name.
      'src/d-host.test.ts',
      'node_modules/dep/e.test.ts',
      '.cache/f.test.ts',
      'src/g.tests.ts',
    ];
    for (const file of files) {
      const path = join(root, 'packages/example', file);
      await mkdir(dirname(path), { recursive: true });
      await writeFile(path, '');
    }
    const manifest = { version: 1, files: Object.fromEntries(timed.map((file, index) => [file, 100 + index])) };
    await writeFile(join(root, 'packages/example/test-timings.json'), `${JSON.stringify(manifest)}\n`);
    return root;
  }

  async function check(workspaceRoot: string): Promise<{ success: boolean; stdout: string; stderr: string }> {
    const stdout = spyOn(console, 'log').mockImplementation(() => {});
    const stderr = spyOn(console, 'error').mockImplementation(() => {});
    try {
      const result = await shardTimingsExecutor(
        { cwd: 'packages/example', args: SUITE_ARGS },
        { root: workspaceRoot, projectName: '@scope/example' },
      );
      return { ...result, stdout: stdout.mock.calls.join('\n'), stderr: stderr.mock.calls.join('\n') };
    } finally {
      stdout.mockRestore();
      stderr.mockRestore();
    }
  }

  it('passes when the manifest names every file the shards run', async () => {
    const workspaceRoot = await fixture([
      'src/a.test.ts',
      'src/b_spec.mts',
      'src/nested/c.test.tsx',
      'src/gone.test.ts',
    ]);

    const result = await check(workspaceRoot);

    expect(result).toEqual({
      success: true,
      stdout: 'shard-timings: packages/example/test-timings.json names all 3 test files the shards run',
      stderr: '',
    });
  });

  it('fails naming each untimed file and the command that refreshes the manifest', async () => {
    const workspaceRoot = await fixture(['src/a.test.ts']);

    const result = await check(workspaceRoot);

    expect(result.success).toBe(false);
    expect(result.stderr).toBe(
      [
        'packages/example/test-timings.json names no duration for 2 of the 3 test files the shards of @scope/example run.',
        "Bun charges each the manifest's median and they cluster into one shard, which can push it past its deadline:",
        '  packages/example/src/b_spec.mts',
        '  packages/example/src/nested/c.test.tsx',
        `Refresh the manifest with: ${refreshTestTimingsCommand('@scope/example')}`,
      ].join('\n'),
    );
    expect(refreshTestTimingsCommand('@scope/example')).toBe('smoo nx refresh-test-timings @scope/example');
  });

  it("honours bunfig's root and ignore patterns where the command passes none", async () => {
    const workspaceRoot = await fixture([]);
    await writeFile(
      join(workspaceRoot, 'packages/example/bunfig.toml'),
      '[test]\nroot = "src"\npathIgnorePatterns = ["src/nested/**"]\n',
    );

    const coverage = await timingsCoverage(workspaceRoot, {
      cwd: 'packages/example',
      args: ['--timings=test-timings.json'],
    });

    expect(coverage).toEqual({
      ok: true,
      coverage: {
        manifests: ['test-timings.json'],
        files: ['src/a.test.ts', 'src/b_spec.mts', 'src/d-host.test.ts'],
        untimed: ['src/a.test.ts', 'src/b_spec.mts', 'src/d-host.test.ts'],
      },
    });
  });

  it('refuses a discovery it cannot replay, naming where, rather than guess what Bun runs', async () => {
    const workspaceRoot = await fixture([]);
    await symlink('loop.test.ts', join(workspaceRoot, 'packages/example/src/loop.test.ts'));
    // A link to itself resolves to nothing stat can return (ELOOP, or EPERM under a sandbox), unlike a missing target.
    expect(await timingsCoverage(workspaceRoot, { cwd: 'packages/example', args: SUITE_ARGS })).toEqual({
      ok: false,
      error: expect.stringMatching(/^cannot replay Bun's discovery under packages\/example: /),
    });
  });

  it('finds the test files Bun itself runs', async () => {
    const workspace = await realpath(await mkdtemp(join(tmpdir(), 'smoothbricks-bun-discovery-')));
    root = workspace;
    const project = join(workspace, 'packages/example');
    const external = join(workspace, 'external');
    const put = async (directory: string, file: string): Promise<void> => {
      await mkdir(dirname(join(directory, file)), { recursive: true });
      await writeFile(join(directory, file), "import { test } from 'bun:test';\n\ntest('runs', () => {});\n");
    };
    const names = ['.test', '_test', '.spec', '_spec'].flatMap((suffix) =>
      ['js', 'jsx', 'ts', 'tsx', 'mjs', 'cjs', 'mts', 'cts'].map((ext) => `src/names/${ext}${suffix}.${ext}`),
    );
    for (const file of [
      ...names,
      // Bun matches every name in lower case.
      'src/names/Upper.TEST.TS',
      'src/names/Node_Modules/dep/upper.test.ts',
      // Not test names.
      'src/names/test.ts',
      'src/names/helper.tests.ts',
      'src/nested/n.spec.jsx', // bunfig ignores it, and the CLI's patterns replace bunfig's
      'src/d-host.test.ts', // the CLI's pattern ignores it
      'src/ignored.test.ts', // .gitignore plays no part
      'src/.hidden.test.ts', // a hidden file runs, a hidden directory does not
      'src/.hidden/h.test.ts',
      'src/node_modules/dep/m.test.ts',
      'outside.test.ts', // outside bunfig's test.root
    ]) {
      await put(project, file);
    }
    await put(external, 'e.test.ts');
    await writeFile(join(project, '.gitignore'), 'src/ignored.test.ts\n');
    await writeFile(join(project, 'bunfig.toml'), '[test]\nroot = "src"\npathIgnorePatterns = ["src/nested/**"]\n');
    await writeFile(join(project, 'other.toml'), '[test]\nroot = "src/names"\n');
    // Bun follows a symlink wherever it leads, outside the project too, and skips one that leads nowhere.
    await symlink(external, join(project, 'src/linkdir'), 'dir');
    await symlink('../outside.test.ts', join(project, 'src/linkfile.test.ts'));
    await symlink('missing.test.ts', join(project, 'src/dangling.test.ts'));

    const runs = [
      { manifest: 'by-bunfig.json', args: [] },
      { manifest: 'by-cli.json', args: ['--path-ignore-patterns=**/*-host.test.ts'] },
      { manifest: 'by-config.json', args: ['--config=other.toml'] },
    ];
    const discovered = new Set<string>();
    for (const { manifest, args } of runs) {
      const suiteArgs = [`--timings=${manifest}`, ...args];
      // A run that matches no test exits 1 by design, yet writes every file it discovered to the manifest.
      const bun = spawnSync(process.execPath, ['test', ...suiteArgs, '--update-timings', '-t', '^__none__$'], {
        cwd: project,
        encoding: 'utf8',
      });
      expect(existsSync(join(project, manifest)), bun.stdout + bun.stderr).toBe(true);
      const written = typia.json.assertParse<TimingManifest>(await readFile(join(project, manifest), 'utf8'));

      const walker = await timingsCoverage(workspace, { cwd: 'packages/example', args: suiteArgs });
      if (!walker.ok) throw new Error(walker.error);
      expect(walker.coverage.files).toEqual(Object.keys(written.files).sort());
      for (const file of walker.coverage.files) discovered.add(file);
    }
    // The shard-timings key names every file any of these discoveries runs, so a file Bun would run changes it.
    await mkdir(join(workspace, 'node_modules/@smoothbricks'), { recursive: true });
    await symlink(
      fileURLToPath(new URL('..', import.meta.url)),
      join(workspace, 'node_modules/@smoothbricks/nx-plugin'),
    );
    const listed = new Set(
      (await runtimeInputValue(bunTestFileNamesInput('packages/example').runtime, workspace))
        .split('\n')
        .filter(Boolean)
        .map((line) => line.replace(/^\.\//, '')),
    );
    expect([...discovered].filter((file) => !listed.has(file))).toEqual([]);
    // A link to nothing is no file Bun runs until its target appears, and the key moves when it does.
    expect(listed.has('src/dangling.test.ts')).toBe(false);
    await put(project, 'src/missing.test.ts');
    expect(await runtimeInputValue(bunTestFileNamesInput('packages/example').runtime, workspace)).toContain(
      'src/dangling.test.ts',
    );
  });
});
