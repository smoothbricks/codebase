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

/**
 * The CLI exactly as it ships: `bin/smoo` running the built `dist`, which `nx run cli:test` builds
 * first (the test target depends on `build`). A child that ran the source instead paid the typia
 * transform of the whole CLI source graph in every process: 3-4 s per spawn in CI, and past the
 * 30 s test timeout under load, where the built CLI starts in a fraction of a second.
 */
const SMOO = join(import.meta.dir, '..', 'bin', 'smoo');

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
      const child = Bun.spawnSync(['bun', SMOO, 'wrangler', 'cleanup-pr', '--pr', '7'], {
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
 * The shipped CLI talks to an isolated HTTP server at its Cloudflare API boundary.
 * The server records every request and refuses anything outside the stage fixture.
 */
async function cleanupCliRoot(): Promise<{
  root: string;
  requests: string[];
  close: () => void;
  run: (
    args: string[],
    scenario?: 'stage' | 'empty' | 'deleting',
  ) => Promise<{
    exitCode: number;
    stdout: string;
    stderr: string;
  }>;
}> {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'smoo-cli-dry-run-')));
  const init = Bun.spawnSync(['git', 'init', '--quiet'], { cwd: root, env: gitFreeEnvironment() });
  expect(init.exitCode).toBe(0);
  await writeFile(join(root, 'nx.json'), '{}\n');
  await writeFile(join(root, 'package.json'), '{ "name": "@acme/app", "repository": "https://github.com/acme/app" }\n');
  const project = join(root, 'packages', 'web');
  await mkdir(project, { recursive: true });

  const requests: string[] = [];
  const unexpected: string[] = [];
  const deletions: Record<string, true> = {
    '/workers/scripts/web-pr7': true,
    '/r2/buckets/media-pr7': true,
    '/r2/buckets/media-pr7/objects/one': true,
    '/r2/buckets/media-pr7/objects/nested/two': true,
    '/r2/buckets/smoo-stage-records/objects/v1/github.com%252Facme%252Fapp/pr7/web-pr7/worker': true,
    '/r2/buckets/smoo-stage-records/objects/v1/github.com%252Facme%252Fapp/pr7/web-pr7/r2/media-pr7': true,
  };
  const records = [
    { key: 'v1/github.com%2Facme%2Fapp/pr7/web-pr7/worker' },
    { key: 'v1/github.com%2Facme%2Fapp/pr7/web-pr7/r2/media-pr7' },
  ];
  let scenario: 'stage' | 'empty' | 'deleting' = 'stage';
  const server = Bun.serve({
    hostname: '127.0.0.1',
    port: 0,
    fetch(request) {
      const url = new URL(request.url);
      const path = url.pathname.replace(/^\/client\/v4\/accounts\/account-1/, '');
      const call = `${request.method} ${path}`;
      requests.push(call);
      const answer = (result: unknown) => Response.json({ success: true, errors: [], result });
      if (
        request.headers.get('authorization') !== 'Bearer token-1' ||
        !url.pathname.startsWith('/client/v4/accounts/account-1/')
      ) {
        unexpected.push(`invalid endpoint or authorization: ${call}`);
      } else if (request.method === 'DELETE' && scenario === 'deleting' && deletions[path]) {
        return answer(null);
      } else if (request.method === 'GET') {
        if (scenario === 'empty' && path === '/r2/buckets') return answer([]);
        if (scenario !== 'empty') {
          if (path === '/r2/buckets') return answer([{ name: 'smoo-stage-records' }, { name: 'media-pr7' }]);
          if (path === '/r2/buckets/smoo-stage-records/objects') return answer(records);
          if (path === '/r2/buckets/media-pr7/objects') return answer([{ key: 'one' }, { key: 'nested/two' }]);
          if (path === '/workers/scripts') return answer([{ id: 'web-pr7' }]);
        }
        unexpected.push(`unexpected read: ${call}`);
      } else {
        unexpected.push(`unexpected mutation: ${call}`);
      }
      return Response.json({ success: false, errors: [{ message: `unexpected ${call}` }] }, { status: 500 });
    },
  });
  return {
    root,
    requests,
    close: () => server.stop(true),
    run: async (args, selected = 'stage') => {
      scenario = selected;
      const before = requests.length;
      const child = Bun.spawn(['bun', SMOO, ...args], {
        cwd: project,
        env: {
          ...gitFreeEnvironment(),
          CLOUDFLARE_ACCOUNT_ID: 'account-1',
          CLOUDFLARE_API_TOKEN: 'token-1',
          CLOUDFLARE_API_BASE_URL: `http://127.0.0.1:${server.port}/client/v4`,
        },
        stdout: 'pipe',
        stderr: 'pipe',
      });
      const [stdout, stderr, exitCode] = await Promise.all([
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
        child.exited,
      ]);
      expect(unexpected).toEqual([]);
      const calls = requests.slice(before);
      if (selected === 'deleting') {
        expect(calls.filter((call) => call.startsWith('DELETE ')).sort()).toEqual(
          Object.keys(deletions)
            .map((path) => `DELETE ${path}`)
            .sort(),
        );
      } else {
        expect(calls.every((call) => call.startsWith('GET '))).toBe(true);
      }
      return { exitCode, stdout, stderr };
    },
  };
}

describe('smoo wrangler cleanup-pr --dry-run', () => {
  it('prints the JSON inventory of the recorded stage and writes nothing', async () => {
    const { root, run, close } = await cleanupCliRoot();
    try {
      const child = await run(['wrangler', 'cleanup-pr', '--pr', '7', '--dry-run', '--json']);

      expect(child.stderr).toBe('');
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
      close();
      await rm(root, { recursive: true, force: true });
    }
  });

  it('prints the dry-run sentence without --json', async () => {
    const { root, run, close } = await cleanupCliRoot();
    try {
      const child = await run(['wrangler', 'cleanup-pr', '--pr', '7', '--dry-run']);

      expect(child.stderr).toBe('');
      expect(child.exitCode).toBe(0);
      expect(child.stdout).toBe(
        'Dry run for pr7 of github.com/acme/app from 2 records: would delete 1 Worker, 0 custom domains, 0 routes, 0 DNS records, 0 KV namespaces, 1 R2 bucket (2 objects), 0 D1 databases; 0 recorded items are already gone. Nothing was deleted.\n',
      );
    } finally {
      close();
      await rm(root, { recursive: true, force: true });
    }
  });

  it('keeps the plain sentences without the new flags, and prints the inventory warning as JSON for a stage without records', async () => {
    const { root, run, close } = await cleanupCliRoot();
    try {
      const plain = await run(['wrangler', 'cleanup-pr', '--pr', '7'], 'empty');
      expect(plain.exitCode).toBe(0);
      expect(plain.stdout).toBe(
        'Nothing is recorded for pr7 of github.com/acme/app, so nothing was deleted. That is expected when the pull request deployed nothing or an earlier cleanup finished its stage. A stage an older smoo deployed, or one deployed while the root package.json named another repository, has no records here and may still be live, so check for its items by hand and delete what is left.\n',
      );

      const live = await run(['wrangler', 'cleanup-pr', '--pr', '7', '--json'], 'empty');
      expect(live.exitCode).toBe(0);
      const liveResult = JSON.parse(live.stdout);
      expect(liveResult).toMatchObject({ stage: 'pr7', recorded: 0, alreadyGone: 0 });
      expect(liveResult.warning).toContain('an older smoo deployed');
      expect(liveResult.warning).toContain('named another repository');

      const dry = await run(['wrangler', 'cleanup-pr', '--pr', '7', '--dry-run', '--json'], 'empty');
      expect(dry.exitCode).toBe(0);
      const dryResult = JSON.parse(dry.stdout);
      expect(dryResult).toMatchObject({ stage: 'pr7', recorded: 0 });
      expect(dryResult.warning).toContain('an older smoo deployed');
      expect(dryResult.warning).toContain('named another repository');
    } finally {
      close();
      await rm(root, { recursive: true, force: true });
    }
  });
});

describe('smoo wrangler cleanup-pr --json', () => {
  it('deletes the recorded stage and prints the deleting result as JSON, with no preview and no warning', async () => {
    const { root, run, close } = await cleanupCliRoot();
    try {
      const child = await run(['wrangler', 'cleanup-pr', '--pr', '7', '--json'], 'deleting');

      expect(child.stderr).toBe('');
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
      close();
      await rm(root, { recursive: true, force: true });
    }
  });

  it('refuses an invalid PR as a usage error before any network call, and prints no JSON at all', async () => {
    const { root, run, close, requests } = await cleanupCliRoot();
    try {
      // `1e3` is a number to `Number`, which would have cleaned up pr1000.
      for (const pr of ['0', '1e3']) {
        const child = await run(['wrangler', 'cleanup-pr', '--pr', pr, '--json']);

        expect(child.exitCode).toBe(1);
        expect(child.stdout).toBe('');
        // The first line names the option, the value given and the allowed range; the command's help
        // follows, and no stack frame anywhere.
        const [refusal] = child.stderr.split('\n');
        expect(refusal).toContain('--pr');
        expect(refusal).toContain(`'${pr}'`);
        expect(refusal).toContain('1 through 999999999');
        expect(child.stderr).not.toContain('    at ');
      }
      expect(requests).toEqual([]);
    } finally {
      close();
      await rm(root, { recursive: true, force: true });
    }
  });
});
