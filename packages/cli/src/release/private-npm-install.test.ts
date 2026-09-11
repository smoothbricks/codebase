/** Contract tests against Bun itself: an absent token is not the same as an unavailable required dependency. */
import { afterEach, describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const roots: string[] = [];
afterEach(async () => {
  for (const root of roots.splice(0)) await rm(root, { recursive: true, force: true });
});

async function fixture() {
  const root = await mkdtemp(join(tmpdir(), 'smoo-npm-contract-'));
  roots.push(root);
  const home = join(root, 'home');
  const workspace = join(root, 'workspace');
  await mkdir(home);
  await mkdir(workspace);
  // Do not inherit a developer's npm configuration, authentication or Bun preloads.
  const env = {
    PATH: process.env.PATH ?? '',
    HOME: home,
    BUN_INSTALL_CACHE_DIR: join(root, 'cache'),
    BUN_CONFIG_NO_CLEAR_TERMINAL: '1',
  };
  const install = async (options: { token?: string; frozen?: boolean } = {}) => {
    const child = Bun.spawn(
      [
        process.execPath,
        'install',
        ...(options.frozen === false ? [] : ['--frozen-lockfile']),
        '--ignore-scripts',
        '--no-progress',
      ],
      {
        cwd: workspace,
        env: { ...env, ...(options.token === undefined ? {} : { SMOO_TEST_NPM_TOKEN: options.token }) },
        stdout: 'pipe',
        stderr: 'pipe',
      },
    );
    // Killing a hung install must not satisfy the expected authentication failure.
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill();
    }, 10_000);
    try {
      const [exitCode] = await Promise.all([
        child.exited,
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
      ]);
      expect(timedOut).toBe(false);
      return exitCode;
    } finally {
      clearTimeout(timer);
    }
  };
  return { root, workspace, install };
}

async function packageArchive(root: string, name: string) {
  const packageDirectory = join(root, 'package');
  await mkdir(packageDirectory);
  await writeFile(join(packageDirectory, 'package.json'), JSON.stringify({ name, version: '1.0.0' }));
  const tarball = join(root, 'package.tgz');
  const tar = Bun.spawn(['tar', '-czf', tarball, '-C', root, 'package'], { stdout: 'ignore', stderr: 'ignore' });
  expect(await tar.exited).toBe(0);
  return readFile(tarball);
}

describe('Bun private registry authentication contract', () => {
  it('does not require a token for an unused private registry', async () => {
    const { workspace, install } = await fixture();
    let requests = 0;
    const server = Bun.serve({
      hostname: '127.0.0.1',
      port: 0,
      fetch() {
        requests += 1;
        return new Response('locked', { status: 401 });
      },
    });
    try {
      await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'example-workspace', private: true }));
      await writeFile(
        join(workspace, '.npmrc'),
        `@example:registry=${server.url}\n//127.0.0.1:${server.port}/:_authToken=\${SMOO_TEST_NPM_TOKEN}\n`,
      );
      expect(await install()).toBe(0);
      expect(requests).toBe(0);
    } finally {
      server.stop(true);
    }
  });

  it('installs a public dependency while a different configured private registry stays locked', async () => {
    const { root, workspace, install } = await fixture();
    const archive = await packageArchive(root, 'example-public');
    let publicRequests = 0;
    let privateRequests = 0;
    const authorizationHeaders: (string | null)[] = [];
    const privateRegistry = Bun.serve({
      hostname: '127.0.0.1',
      port: 0,
      fetch() {
        privateRequests += 1;
        return new Response('locked', { status: 401 });
      },
    });
    const publicRegistry = Bun.serve({
      hostname: '127.0.0.1',
      port: 0,
      fetch(request) {
        publicRequests += 1;
        authorizationHeaders.push(request.headers.get('authorization'));
        if (new URL(request.url).pathname.endsWith('.tgz')) return new Response(archive);
        return Response.json({
          name: 'example-public',
          'dist-tags': { latest: '1.0.0' },
          versions: {
            '1.0.0': {
              name: 'example-public',
              version: '1.0.0',
              dist: { tarball: new URL('package.tgz', request.url).href },
            },
          },
        });
      },
    });
    try {
      await writeFile(
        join(workspace, 'package.json'),
        JSON.stringify({ name: 'example-workspace', private: true, dependencies: { 'example-public': '1.0.0' } }),
      );
      await writeFile(
        join(workspace, '.npmrc'),
        `registry=${publicRegistry.url}\n@example:registry=${privateRegistry.url}\n//127.0.0.1:${privateRegistry.port}/:_authToken=\${SMOO_TEST_NPM_TOKEN}\n`,
      );
      expect(await install({ frozen: false })).toBe(0);
      expect(publicRequests).toBeGreaterThan(0);
      expect(privateRequests).toBe(0);
      expect(authorizationHeaders.every((value) => value === null)).toBe(true);
      expect(await Bun.file(join(workspace, 'node_modules/example-public/package.json')).exists()).toBe(true);
    } finally {
      publicRegistry.stop(true);
      privateRegistry.stop(true);
    }
  });

  it('refuses a cold install of an inaccessible required private dependency', async () => {
    const { workspace, install } = await fixture();
    let requests = 0;
    const server = Bun.serve({
      hostname: '127.0.0.1',
      port: 0,
      fetch() {
        requests += 1;
        return new Response('locked', { status: 401 });
      },
    });
    try {
      await writeFile(
        join(workspace, 'package.json'),
        JSON.stringify({ name: 'example-workspace', private: true, dependencies: { '@example/private': '1.0.0' } }),
      );
      await writeFile(
        join(workspace, '.npmrc'),
        `@example:registry=${server.url}\n//127.0.0.1:${server.port}/:_authToken=\${SMOO_TEST_NPM_TOKEN}\n`,
      );
      expect(await install({ frozen: false })).not.toBe(0);
      expect(requests).toBeGreaterThan(0);
      expect(await Bun.file(join(workspace, 'node_modules/@example/private/package.json')).exists()).toBe(false);
    } finally {
      server.stop(true);
    }
  });

  it('can repeat an already installed frozen workspace without the token', async () => {
    const { root, workspace, install } = await fixture();
    const archive = await packageArchive(root, '@example/private');
    let requests = 0;
    const server = Bun.serve({
      hostname: '127.0.0.1',
      port: 0,
      fetch(request) {
        requests += 1;
        if (request.headers.get('authorization') !== 'Bearer test-only-token')
          return new Response('locked', { status: 401 });
        if (new URL(request.url).pathname.endsWith('.tgz')) return new Response(archive);
        return Response.json({
          name: '@example/private',
          'dist-tags': { latest: '1.0.0' },
          versions: {
            '1.0.0': {
              name: '@example/private',
              version: '1.0.0',
              dist: { tarball: new URL('package.tgz', request.url).href },
            },
          },
        });
      },
    });
    try {
      await writeFile(
        join(workspace, 'package.json'),
        JSON.stringify({ name: 'example-workspace', private: true, dependencies: { '@example/private': '1.0.0' } }),
      );
      await writeFile(
        join(workspace, '.npmrc'),
        `@example:registry=${server.url}\n//127.0.0.1:${server.port}/:_authToken=\${SMOO_TEST_NPM_TOKEN}\n`,
      );
      expect(await install({ frozen: false, token: 'test-only-token' })).toBe(0);
      const lock = await readFile(join(workspace, 'bun.lock'), 'utf8');
      requests = 0;
      expect(await install()).toBe(0);
      expect(requests).toBe(0);
      expect(await readFile(join(workspace, 'bun.lock'), 'utf8')).toBe(lock);
    } finally {
      server.stop(true);
    }
  });
});
