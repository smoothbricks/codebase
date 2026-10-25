/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: Shell fixture scripts are literal shell text. */
import { describe, expect, it } from 'bun:test';
import { existsSync, lstatSync, readdirSync, readFileSync, readlinkSync, realpathSync, symlinkSync } from 'node:fs';
import { chmod, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, relative, resolve } from 'node:path';
import {
  edit,
  finished,
  type Repository,
  type RepositoryOptions,
  type ShellProcess,
  withManagedRepository,
} from './__tests__/helpers/shell-entry.js';

/**
 * What an explicit missing-payload repair does, measured against the real
 * "setup-environment.ts --repair-missing-payload" mode, and what shell entry
 * relinks in Bun's store.
 *
 * Every install owns a private HTTP registry serving a real published tar:
 * real bytes, real sha512 integrity, a real "bun install" resolving it into
 * bun.lock under a private BUN_INSTALL_CACHE_DIR. A repair case breaks one
 * installed file physically and requests the repair through the checkout's
 * installed alias; the relink case removes an install-cache entry and runs
 * shell entry's "--links". Nothing here stubs the repair: the fetch, the
 * locked integrity comparison, the filesystem and the whole-directory exchange
 * are the real ones, and the atomic-failure case reaches its cause through
 * real file permissions rather than any fault flag. A successful repair
 * reports its stdout JSON protocol; the filesystem digests around each run are
 * the proof that the report is true.
 *
 * An install is the expensive part, so the cases that can share one do. A
 * refusal meets the install with its own single cause and withdraws it before
 * the next case, so none is explained by another's leftover; the case that
 * removes the witness cannot withdraw it and goes last. A failure names the
 * case that broke. The permission case revokes write access to the package's
 * parent and keeps an install of its own.
 */

interface PublishedRegistry {
  readonly origin: string;
  /** Serves tar bytes that hash differently from the integrity bun.lock records. */
  readonly tamper: (tampered: boolean) => void;
  readonly close: () => void;
}

const PUBLISHED_PACKAGE = JSON.stringify({ name: '@acme/payload', version: '1.0.0', main: 'index.js' });
const PUBLISHED_INDEX = "module.exports = 'published index';\n";
const PUBLISHED_MIDDLE = "module.exports = 'published middle';\n";
const PUBLISHED_INNER = "module.exports = 'published inner';\n";
const PUBLISHED_FILES: Readonly<Record<string, string>> = {
  'package.json': PUBLISHED_PACKAGE,
  'index.js': PUBLISHED_INDEX,
  'lib/middle.js': PUBLISHED_MIDDLE,
  'lib/deep/inner.js': PUBLISHED_INNER,
};

/** The published package served over real HTTP, with its real tar and integrity. */
async function publishedRegistry(): Promise<PublishedRegistry> {
  const scratch = realpathSync(await mkdtemp(join(tmpdir(), 'smoo-published-')));
  const stage = join(scratch, 'stage', 'package');
  for (const [file, content] of Object.entries(PUBLISHED_FILES)) {
    await mkdir(dirname(join(stage, file)), { recursive: true });
    await writeFile(join(stage, file), content);
  }
  const tgz = join(scratch, 'payload-1.0.0.tgz');
  const packed = Bun.spawnSync(['tar', '-czf', tgz, '-C', join(scratch, 'stage'), 'package']);
  if (packed.exitCode !== 0) {
    throw new Error(`tar failed: ${new TextDecoder().decode(packed.stderr)}`);
  }
  const real = new Uint8Array(readFileSync(tgz));
  const integrity = `sha512-${new Bun.CryptoHasher('sha512').update(real).digest('base64')}`;
  const tampered = Uint8Array.from(real);
  const last = tampered.length - 1;
  tampered[last] = (tampered[last] ?? 0) ^ 0xff;
  let served = real;
  const server = Bun.serve({
    // The origin below names 127.0.0.1, so bind exactly that. A wildcard bind lets another process's
    // 127.0.0.1 listener (a concurrent shard's fixture registry) take the same port number, and the
    // more specific bind then answers this fixture's requests.
    hostname: '127.0.0.1',
    port: 0,
    fetch(request) {
      const url = new URL(request.url);
      const path = url.pathname;
      if (path === '/@acme%2fpayload' || path === '/@acme/payload') {
        return Response.json({
          name: '@acme/payload',
          'dist-tags': { latest: '1.0.0' },
          versions: {
            '1.0.0': {
              name: '@acme/payload',
              version: '1.0.0',
              dist: {
                tarball: `${url.origin}/tarball/payload-1.0.0.tgz`,
                integrity,
              },
            },
          },
        });
      }
      if (path === '/tarball/payload-1.0.0.tgz') {
        return new Response(served, { headers: { 'content-type': 'application/octet-stream' } });
      }
      return new Response('not found', { status: 404 });
    },
  });
  return {
    origin: `http://127.0.0.1:${server.port}`,
    tamper: (on: boolean) => {
      served = on ? tampered : real;
    },
    close: () => {
      server.stop(true);
      void rm(scratch, { recursive: true, force: true });
    },
  };
}

/** Every byte and link under dir, keyed relative to it. */
function packageState(dir: string): Record<string, string> {
  const state: Record<string, string> = {};
  const walk = (current: string): void => {
    for (const entry of readdirSync(current).sort()) {
      const path = join(current, entry);
      const key = relative(dir, path);
      const stat = lstatSync(path);
      if (stat.isSymbolicLink()) state[key] = `link -> ${readlinkSync(path)}`;
      else if (stat.isDirectory()) walk(path);
      else state[key] = readFileSync(path, 'utf8');
    }
  };
  walk(dir);
  return state;
}

/** The link's target, or null where the path is a real directory or absent. */
function linkTarget(path: string): string | null {
  try {
    return readlinkSync(path);
  } catch {
    return null;
  }
}

/** The real script's run, with the stdout its repair protocol reports on. */
async function setupResult(proc: ShellProcess): Promise<{ exitCode: number; stdout: string; stderr: string }> {
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  return { exitCode, stdout, stderr };
}

/** The reported protocol fields of one repair run, read at the seam. */
function protocol(stdout: string): Record<string, unknown> {
  const report: unknown = JSON.parse(stdout);
  return Object.fromEntries(Object.entries(report ?? {}));
}

/**
 * One named step of a sequence that shares an install. A failure says which
 * step broke, because the runner's source frames are unreliable.
 */
async function step(name: string, run: () => Promise<void>): Promise<void> {
  try {
    await run();
  } catch (cause) {
    throw new Error(`${name}: ${cause instanceof Error ? cause.message : String(cause)}`, { cause });
  }
}

/** One run of the real script, with the private Bun install cache this fixture owns. */
function setupRun(root: string, cargoHome: string, args: readonly string[]): ShellProcess {
  const scratch = dirname(cargoHome);
  return Bun.spawn({
    cmd: ['bun', join(root, 'tooling', 'direnv', 'setup-environment.ts'), ...args],
    cwd: root,
    env: {
      PATH: `${join(scratch, 'bin')}:${process.env['PATH'] ?? ''}`,
      HOME: process.env['HOME'],
      TMPDIR: scratch,
      CARGO_HOME: cargoHome,
      XDG_CACHE_HOME: join(scratch, 'xdg-cache'),
      // The private cache lives inside this checkout: the repair contract
      // requires physically checkout-owned package payload.
      BUN_INSTALL_CACHE_DIR: join(root, '.bun-install-cache'),
      DEVENV_ROOT: join(root, 'tooling', 'direnv'),
      DEVENV_STATE: join(root, 'tooling', 'direnv', '.devenv', 'state'),
    },
    stdout: 'pipe',
    stderr: 'pipe',
    stdin: 'ignore',
  });
}

describe('a published payload installed in a checkout', () => {
  const alias = join('node_modules', '@acme', 'payload');
  const missing = join(alias, 'lib', 'middle.js');

  async function withInstalledPayload(
    registry: PublishedRegistry,
    options: RepositoryOptions,
    run: (repository: Repository) => Promise<void>,
    beforeInstall?: (repository: Repository) => Promise<void>,
  ): Promise<void> {
    await withManagedRepository(
      {
        ...options,
        dependencies: { '@acme/payload': '1.0.0', ...options.dependencies },
        files: {
          '.npmrc': `@acme:registry=${registry.origin}\n`,
          // The repo's own proven install declaration, verbatim: isolated linker
          // plus the global store, whose root is this fixture's private
          // BUN_INSTALL_CACHE_DIR. That is what produces the measured topology —
          // node_modules/.bun/<name>@<ver> absolute store links into
          // <cache>/links/<name>@<ver>-<hash> and relative alias symlinks above
          // them — and nothing reaches any consumer or host cache.
          'bunfig.toml': '[install]\nlinker = "isolated"\nglobalStore = true\n',
          ...options.files,
        },
      },
      async (repository) => {
        if (beforeInstall !== undefined) await beforeInstall(repository);
        // A healthy install reports the declared developer links it created on
        // one line, each entry naming its package and resolved absolute target;
        // a fixture without declarations reports nothing.
        const links = Object.entries(options.developerLinks ?? {});
        const report =
          links.length === 0
            ? ''
            : 'linked to local checkouts: ' +
              links.map(([name, to]) => `${name} -> ${resolve(repository.root, to)}`).join(', ') +
              '\n';
        expect(await finished(setupRun(repository.root, repository.cargoHome, []))).toEqual({
          exitCode: 0,
          stderr: report,
        });
        await run(repository);
      },
    );
  }

  /**
   * What a repair is judged against on disk: the package's bytes and links, the
   * lock, and the witness (null while there is none).
   */
  function evidence(root: string): { payload: Record<string, string>; lock: string; witness: string | null } {
    const receipt = join(root, 'node_modules', '.smoo-install');
    return {
      payload: packageState(join(root, alias)),
      lock: readFileSync(join(root, 'bun.lock'), 'utf8'),
      witness: existsSync(receipt) ? readFileSync(receipt, 'utf8') : null,
    };
  }

  it('refuses unsafe repairs unchanged, repairs under a stale witness, and relinks a dangling store link', async () => {
    const registry = await publishedRegistry();
    try {
      await withInstalledPayload(registry, {}, async ({ root, cargoHome }) => {
        const installed = join(root, missing);
        const receipt = join(root, 'node_modules', '.smoo-install');
        const repair = () => setupResult(setupRun(root, cargoHome, ['--repair-missing-payload', installed]));

        await step(
          'reinstalls when a store link the installed tree reaches dangles, and never for one nothing reaches',
          async () => {
            const check = async () => (await finished(setupRun(root, cargoHome, ['--links', '--check']))).exitCode;
            const store = join(root, 'node_modules', '.bun');
            const entry = join(store, '@acme+payload@1.0.0');
            const cached = readlinkSync(entry);
            expect(cached.startsWith(`${join(root, '.bun-install-cache')}/`)).toBe(true);
            expect(await check()).toBe(0);

            // A superseded variant of the same package and version, its cache entry gone: nothing installed reaches
            // it and no install changes it, so it is not out of date.
            symlinkSync(`${cached}-superseded`, join(store, '@acme+payload@1.0.0+superseded'));
            expect(await check()).toBe(0);

            // The cache entry the installed tree reaches is gone: the install is out of date.
            await rm(cached, { recursive: true });
            expect(await check()).toBe(1);
            const repaired = await finished(setupRun(root, cargoHome, ['--links']));
            expect(repaired.exitCode, repaired.stderr).toBe(0);
            expect(repaired.stderr).toContain(
              `! ${entry} -> ${cached}: that Bun install-cache entry no longer exists; reinstalling`,
            );
            expect(JSON.parse(readFileSync(join(root, alias, 'package.json'), 'utf8'))).toEqual(
              JSON.parse(PUBLISHED_PACKAGE),
            );
            expect(await check()).toBe(0);
          },
        );

        // A refusal meets this install with its own single cause and withdraws it before the next case, so none is
        // explained by another's leftover; removing the witness cannot be withdrawn, so it goes last. The named file
        // stays missing until the repair that restores it.
        await rm(installed);

        await step('refuses a tar whose bytes fail the locked integrity and changes nothing', async () => {
          const before = evidence(root);
          registry.tamper(true);
          const refused = await repair();
          registry.tamper(false);
          expect(refused.exitCode).not.toBe(0);
          expect(refused.stderr).toContain('integrity');
          expect(evidence(root)).toEqual(before);
        });

        await step('refuses an existing published-payload mismatch and changes nothing', async () => {
          const index = join(root, alias, 'index.js');
          await edit(index, "module.exports = 'local edit';\n");
          const before = evidence(root);
          const refused = await repair();
          expect(refused.exitCode).not.toBe(0);
          expect(refused.stderr).toContain('index.js');
          // The mismatch is refused, never silently repaired: package, stamp and
          // locks are all unchanged, and the named file stays missing.
          expect(evidence(root)).toEqual(before);
          // Withdrawn, so the next step meets no cause but its own.
          await writeFile(index, PUBLISHED_INDEX);
        });

        await step('preserves a stale witness and its stale inputs exactly', async () => {
          // The install inputs drift after the witness was recorded. The repair is
          // not an install: the stale inputs and the witness's recorded digests
          // stay exactly as they are, while the missing file is restored.
          await edit(join(root, 'package.json'), `${readFileSync(join(root, 'package.json'), 'utf8')}\n`);
          const witness = readFileSync(receipt, 'utf8');
          const stale = readFileSync(join(root, 'package.json'), 'utf8');
          const repaired = await repair();
          expect(repaired.exitCode).toBe(0);
          expect(readFileSync(installed, 'utf8')).toBe(PUBLISHED_MIDDLE);
          expect(readFileSync(receipt, 'utf8')).toBe(witness);
          expect(readFileSync(join(root, 'package.json'), 'utf8')).toBe(stale);
        });

        await step('refuses a repair with no prior success witness and mints no fresh proof', async () => {
          // The repair above restored the named file: it goes again, with the witness.
          await rm(receipt);
          await rm(installed);
          const before = evidence(root);
          const refused = await repair();
          expect(refused.exitCode).not.toBe(0);
          // Refused rather than minting fresh full-install proof: no witness
          // appears, and the package stays exactly as it was asked.
          expect(existsSync(receipt)).toBe(false);
          expect(evidence(root)).toEqual(before);
        });
      });
    } finally {
      registry.close();
    }
  });

  it('restores a missing file keeping links and metadata, and a repeated request installs nothing', async () => {
    const registry = await publishedRegistry();
    try {
      await withInstalledPayload(
        registry,
        {
          // A declaration links what the manifests depend on and never adds a
          // dependency, so the linked name is an installed package first.
          dependencies: { '@acme/notes': 'file:vendor/notes' },
          files: {
            'vendor/notes/package.json': JSON.stringify({ name: '@acme/notes', version: '1.0.0' }),
          },
          developerLinks: { '@acme/notes': '../notes-target' },
        },
        async ({ root, cargoHome, count }) => {
          const installed = join(root, missing);
          const aliasPath = join(root, alias);
          const notes = join(root, 'node_modules', '@acme', 'notes');
          const repair = () => setupResult(setupRun(root, cargoHome, ['--repair-missing-payload', installed]));
          // Each oracle names itself in its failure, because the runner's
          // source frames are unreliable.
          expect(lstatSync(aliasPath).isSymbolicLink() ? 'alias-link' : 'alias-not-a-link').toBe('alias-link');
          const beforeAlias = linkTarget(aliasPath);
          const beforeNotes = linkTarget(notes);
          expect(existsSync(notes) ? 'notes-link' : 'notes-absent').toBe('notes-link');
          const metadata = readFileSync(join(aliasPath, 'package.json'), 'utf8');
          await rm(installed);
          const broken = evidence(root);
          expect(Object.hasOwn(broken.payload, 'lib/middle.js')).toBe(false);
          const repaired = await repair();

          await step('restores a physically missing published file to its locked published bytes', async () => {
            expect(repaired.exitCode).toBe(0);
            // Missing-only: exactly the named file returns, at its locked published
            // bytes, and every other byte and link of the package, the lock and the
            // witness are untouched.
            expect(evidence(root)).toEqual({
              ...broken,
              payload: { ...broken.payload, 'lib/middle.js': PUBLISHED_MIDDLE },
            });
            const report = protocol(repaired.stdout);
            expect(report['operation']).toBe('repair-missing-payload');
            expect(report['package']).toBe('@acme/payload@1.0.0');
            expect(report['requestedPath']).toBe(installed);
            expect(report['publishedComplete']).toBe(true);
            expect(report['lockedHashesUnchanged']).toBe(true);
            expect(report['restoredFiles']).toBe(1);
            expect(report['publishedFiles']).toBe(4);
          });

          await step('retains alias links, package metadata and developer links across a repair', async () => {
            expect(lstatSync(aliasPath).isSymbolicLink() ? 'alias-link' : 'alias-not-a-link').toBe('alias-link');
            expect(linkTarget(aliasPath)).toBe(beforeAlias);
            expect(linkTarget(notes)).toBe(beforeNotes);
            expect(readFileSync(join(aliasPath, 'package.json'), 'utf8')).toBe(metadata);
          });

          await step('repeats a healthy request idempotently, installing nothing', async () => {
            const whole = evidence(root);
            const installs = count('install');
            const again = await repair();
            expect(again.exitCode).toBe(0);
            expect(protocol(again.stdout)['operation']).toBe('repair-missing-payload');
            expect(evidence(root)).toEqual(whole);
            expect(count('install')).toBe(installs);
          });
        },
        async ({ root }) => {
          const target = join(dirname(root), 'notes-target');
          await mkdir(target, { recursive: true });
          // The external target is a real package directory, as every existing
          // developer-target fixture provides.
          await writeFile(join(target, 'package.json'), JSON.stringify({ name: '@acme/notes', version: '9.9.9' }));
          await writeFile(join(target, 'README.md'), 'developer notes\n');
        },
      );
    } finally {
      registry.close();
    }
  });

  it('a failed atomic exchange leaves the live package unchanged and reports the cause', async () => {
    const registry = await publishedRegistry();
    try {
      await withInstalledPayload(registry, {}, async ({ root, cargoHome }) => {
        const installed = join(root, missing);
        await rm(installed);
        const broken = packageState(join(root, alias));
        const receipt = join(root, 'node_modules', '.smoo-install');
        const stamped = readFileSync(receipt, 'utf8');
        const locked = readFileSync(join(root, 'bun.lock'), 'utf8');
        // A real permission failure at the package's own parent: the exchange
        // cannot commit, and no fault flag or production seam is involved.
        const parent = dirname(realpathSync(join(root, alias)));
        // The installer's real copy runs through this private PATH wrapper: it
        // invokes the real cp unchanged, propagates its exit code, and only then
        // turns the resolved package parent read-only, so the exchange itself
        // fails on the real permission boundary. No sleeps, no fault flag and no
        // bound changes.
        const realCp = Bun.which('cp');
        const realChmod = Bun.which('chmod');
        if (realCp === null || realChmod === null) throw new Error('cp and chmod must be on PATH');
        await writeFile(
          join(dirname(cargoHome), 'bin', 'cp'),
          [
            '#!/usr/bin/env bash',
            `${JSON.stringify(realCp)} "$@"`,
            'status=$?',
            `${JSON.stringify(realChmod)} 555 ${JSON.stringify(parent)}`,
            'exit $status',
            '',
          ].join('\n'),
        );
        await chmod(join(dirname(cargoHome), 'bin', 'cp'), 0o755);
        try {
          const failed = await setupResult(setupRun(root, cargoHome, ['--repair-missing-payload', installed]));
          expect(failed.exitCode).not.toBe(0);
          expect(failed.stderr).toContain('Atomic package exchange failed');
          expect(packageState(join(root, alias))).toEqual(broken);
          expect(readFileSync(join(root, 'bun.lock'), 'utf8')).toBe(locked);
          expect(readFileSync(receipt, 'utf8')).toBe(stamped);
        } finally {
          await chmod(parent, 0o755);
        }
      });
    } finally {
      registry.close();
    }
  });
});
