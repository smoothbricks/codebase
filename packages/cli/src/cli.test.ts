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
 * `git rev-parse` at the repository running the tests instead of the fixture. Also unsets
 * `GITHUB_REPOSITORY`: inherited from a GitHub Actions runner, it fails `stageRecordScope`'s CI
 * cross-check against the fixture's own `acme/app` manifest.
 */
function gitFreeEnvironment(): Record<string, string> {
  return mergeEnv(undefined, ['GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE', 'GIT_COMMON_DIR', 'GITHUB_REPOSITORY']);
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

/**
 * A workspace root, a project directory inside it, and a `smoo.mjs` entry whose `fetch` answers a
 * local fake Cloudflare: a pr7 stage holding one Worker and one R2 bucket (`stage`), or an account
 * with no record bucket at all (`empty`). For every scenario except `deleting`, any write, or any
 * read outside the fake's script, fails the child, so a green run proves the command only read.
 * `deleting` additionally accepts exactly the six DELETE calls the recorded stage's own cleanup
 * issues — nothing else, so a wrong key or a different stage's/bucket's object still fails the child.
 */
async function cleanupCliRoot(): Promise<{
  root: string;
  project: string;
  run: (args: string[], env?: Record<string, string>) => { exitCode: number; stdout: string; stderr: string };
}> {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'smoo-cli-dry-run-')));
  const init = Bun.spawnSync(['git', 'init', '--quiet'], { cwd: root, env: gitFreeEnvironment() });
  expect(init.exitCode).toBe(0);
  await writeFile(join(root, 'nx.json'), '{}\n');
  await writeFile(join(root, 'package.json'), '{ "name": "@acme/app", "repository": "https://github.com/acme/app" }\n');
  const project = join(root, 'packages', 'web');
  await mkdir(project, { recursive: true });
  const entry = join(root, 'smoo.mjs');
  const fake = `
globalThis.fetch = async (input, init) => {
  const method = init?.method ?? 'GET';
  const scenario = process.env.SMOO_TEST_SCENARIO ?? 'stage';
  const path = new URL(input).pathname.replace('/client/v4/accounts/account-1', '');
  const answer = (result) =>
    new Response(JSON.stringify({ success: true, errors: [], result }), { status: 200 });
  if (scenario === 'deleting' && method === 'DELETE') {
    if (
      path === '/workers/scripts/web-pr7' ||
      path === '/r2/buckets/media-pr7' ||
      path === '/r2/buckets/media-pr7/objects/one' ||
      path === '/r2/buckets/media-pr7/objects/nested/two' ||
      path === '/r2/buckets/smoo-stage-records/objects/v1/github.com%252Facme%252Fapp/pr7/web-pr7/worker' ||
      path === '/r2/buckets/smoo-stage-records/objects/v1/github.com%252Facme%252Fapp/pr7/web-pr7/r2/media-pr7'
    )
      return answer(null);
    console.error(\`unexpected DELETE \${path}\`);
    process.exit(8);
  }
  if (method !== 'GET') {
    console.error(\`the command must not \${method}\`);
    process.exit(9);
  }
  if (scenario === 'empty') {
    if (path === '/r2/buckets') return answer([]);
  } else {
    if (path === '/r2/buckets') return answer([{ name: 'smoo-stage-records' }, { name: 'media-pr7' }]);
    if (path === '/r2/buckets/smoo-stage-records/objects')
      return answer([
        { key: 'v1/github.com%2Facme%2Fapp/pr7/web-pr7/worker' },
        { key: 'v1/github.com%2Facme%2Fapp/pr7/web-pr7/r2/media-pr7' },
      ]);
    if (path === '/r2/buckets/media-pr7/objects') return answer([{ key: 'one' }, { key: 'nested/two' }]);
    if (path === '/workers/scripts') return answer([{ id: 'web-pr7' }]);
  }
  console.error(\`unexpected \${method} \${path}\`);
  process.exit(8);
};
const { runCli } = await import(${JSON.stringify(join(import.meta.dir, 'cli.ts'))});
await runCli();
`;
  await writeFile(entry, fake);
  const preload = Bun.resolveSync('@smoothbricks/validation/bun/preload', import.meta.dir);
  return {
    root,
    project,
    run: (args: string[], env: Record<string, string> = {}) => {
      const child = Bun.spawnSync(['bun', '--preload', preload, entry, ...args], {
        cwd: project,
        env: {
          ...gitFreeEnvironment(),
          CLOUDFLARE_ACCOUNT_ID: 'account-1',
          CLOUDFLARE_API_TOKEN: 'token-1',
          ...env,
        },
      });
      return {
        exitCode: child.exitCode,
        stdout: child.stdout.toString(),
        stderr: child.stderr.toString(),
      };
    },
  };
}

/**
 * The child's stderr, minus the ttsc routing preload's benign notice ("directory mismatch ... you
 * don't need to do anything", ANSI-dimmed when Bun colorizes), which the CLI cannot suppress. Anything
 * left is a real complaint.
 */
function realComplaints(stderr: string): string {
  return stderr
    .split('\n')
    .filter((line) => line !== '' && !line.includes('directory mismatch for directory'))
    .join('\n');
}

describe('smoo wrangler cleanup-pr --dry-run', () => {
  it('prints the JSON inventory of the recorded stage and writes nothing', async () => {
    const { root, run } = await cleanupCliRoot();
    try {
      const child = run(['wrangler', 'cleanup-pr', '--pr', '7', '--dry-run', '--json']);

      expect(realComplaints(child.stderr)).toBe('');
      expect(child.exitCode).toBe(0);
      expect(JSON.parse(child.stdout)).toEqual({
        stage: 'pr7',
        scope: 'github.com/acme/app',
        recorded: 2,
        candidates: {
          workers: ['web-pr7'],
          domains: [],
          routes: [],
          dnsRecords: [],
          kvNamespaces: [],
          r2Buckets: [{ name: 'media-pr7', objectCount: 2 }],
          d1Databases: [],
        },
        alreadyGone: 0,
        leftInPlace: [],
      });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('prints the dry-run sentence without --json', async () => {
    const { root, run } = await cleanupCliRoot();
    try {
      const child = run(['wrangler', 'cleanup-pr', '--pr', '7', '--dry-run']);

      expect(realComplaints(child.stderr)).toBe('');
      expect(child.exitCode).toBe(0);
      expect(child.stdout).toBe(
        'Dry run for pr7 of github.com/acme/app from 2 records: would delete 1 Worker, 0 custom domains, 0 routes, 0 DNS records, 0 KV namespaces, 1 R2 bucket (2 objects), 0 D1 databases; 0 recorded items are already gone. Nothing was deleted.\n',
      );
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('keeps the plain sentences without the new flags, and prints the inventory warning as JSON for a stage without records', async () => {
    const { root, run } = await cleanupCliRoot();
    try {
      const empty = { SMOO_TEST_SCENARIO: 'empty' };
      const plain = run(['wrangler', 'cleanup-pr', '--pr', '7'], empty);
      expect(plain.exitCode).toBe(0);
      expect(plain.stdout).toBe(
        'Nothing is recorded for pr7 of github.com/acme/app, so nothing was deleted. That is expected when the pull request deployed nothing or an earlier cleanup finished its stage. A stage an older smoo deployed, or one deployed while the root package.json named another repository, has no records here and may still be live, so check for its items by hand and delete what is left.\n',
      );

      const live = run(['wrangler', 'cleanup-pr', '--pr', '7', '--json'], empty);
      expect(live.exitCode).toBe(0);
      const liveResult = JSON.parse(live.stdout);
      expect(liveResult).toMatchObject({ stage: 'pr7', recorded: 0, alreadyGone: 0 });
      expect(liveResult.warning).toContain('an older smoo deployed');
      expect(liveResult.warning).toContain('named another repository');

      const dry = run(['wrangler', 'cleanup-pr', '--pr', '7', '--dry-run', '--json'], empty);
      expect(dry.exitCode).toBe(0);
      const dryResult = JSON.parse(dry.stdout);
      expect(dryResult).toMatchObject({ stage: 'pr7', recorded: 0 });
      expect(dryResult.warning).toContain('an older smoo deployed');
      expect(dryResult.warning).toContain('named another repository');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

describe('smoo wrangler cleanup-pr --json', () => {
  it('deletes the recorded stage and prints the deleting result as JSON, with no preview and no warning', async () => {
    const { root, run } = await cleanupCliRoot();
    try {
      const child = run(['wrangler', 'cleanup-pr', '--pr', '7', '--json'], { SMOO_TEST_SCENARIO: 'deleting' });

      expect(realComplaints(child.stderr)).toBe('');
      expect(child.exitCode).toBe(0);
      expect(JSON.parse(child.stdout)).toEqual({
        stage: 'pr7',
        scope: 'github.com/acme/app',
        recorded: 2,
        deleted: {
          workers: 1,
          routes: 0,
          domains: 0,
          dnsRecords: 0,
          kvNamespaces: 0,
          r2Buckets: 1,
          r2Objects: 2,
          d1Databases: 0,
        },
        alreadyGone: 0,
        leftInPlace: [],
      });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('refuses an invalid PR before any network call, and prints no JSON at all', async () => {
    const { root, run } = await cleanupCliRoot();
    try {
      const child = run(['wrangler', 'cleanup-pr', '--pr', '0', '--json']);

      expect(child.exitCode).toBe(1);
      expect(child.stdout).toBe('');
      expect(realComplaints(child.stderr)).toContain('1 through 999999999');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
