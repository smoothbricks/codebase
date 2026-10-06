import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import {
  existsSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  readlinkSync,
  realpathSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { managedAssetsRoot } from '@smoothbricks/nx-plugin/managed-assets';
import { printCommandOutput } from '../lib/run.js';

const script = join(managedAssetsRoot, 'raw/tooling/direnv/nx-socket-dir.sh');
const PORT_BASE = '37376';

interface Scratch {
  readonly checkout: string;
  /** Where cowshed keeps its short runtime links; `/tmp` on a real host. */
  readonly links: string;
  /** The link cowshed binds a job's runtime to: `<links>/cs-<port base>`. */
  readonly link: string;
  /** The real socket leaf every boundary of the checkout must reach. */
  readonly leaf: string;
  /** The host's DEVENV_RUNTIME, outside the checkout. */
  readonly hostRuntime: string;
}

function withScratch(run: (scratch: Scratch) => void): void {
  const dir = realpathSync(mkdtempSync(join(tmpdir(), 'smoo-nx-socket-')));
  try {
    const checkout = join(dir, 'checkout');
    const links = join(dir, 'links');
    const hostRuntime = join(dir, 'devenv-host');
    for (const path of [checkout, links, hostRuntime]) mkdirSync(path, { recursive: true });
    run({
      checkout,
      links,
      link: join(links, `cs-${PORT_BASE}`),
      leaf: join(checkout, '.cowshed', 'run', 'nx'),
      hostRuntime,
    });
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

/** A cowshed checkout: its runtime tree and the `.cowshed/env` cowshed writes from the workspace's record. */
function cowshedCheckout(scratch: Scratch): void {
  mkdirSync(join(scratch.checkout, '.cowshed', 'run'), { recursive: true });
  writeFileSync(
    join(scratch.checkout, '.cowshed', 'env'),
    `export COWSHED_WORKSPACE_TOKEN=token\nexport COWSHED_PORT_BASE=${PORT_BASE}\nexport COWSHED_PORT_BLOCK_SIZE=64\n`,
  );
}

/** The Nx state overrides the script keeps or drops: `undefined` when unset. */
interface NxState {
  readonly NX_WORKSPACE_DATA_DIRECTORY: string | undefined;
  readonly NX_CACHE_DIRECTORY: string | undefined;
  readonly NX_DAEMON_SOCKET_DIR: string | undefined;
}

interface Entered {
  readonly NX_SOCKET_DIR: string;
  readonly NX_WORKSPACE_ROOT_PATH: string;
  readonly state: NxState;
  readonly stderr: string;
}

/** One printed line: `set:<value>` when the variable is set, empty when it is not. */
function setValue(line: string | undefined): string | undefined {
  return line?.startsWith('set:') ? line.slice('set:'.length) : undefined;
}

/**
 * Source the script the way devenv.smoo.nix's prologue does — from the
 * workspace root, naming the links directory — and report what it exported.
 */
function enterShell(scratch: Scratch, inherited: Record<string, string>): Entered {
  const result = spawnSync(
    'bash',
    [
      '-c',
      [
        '. "$1" "$2" && printf \'%s\\n\' "$NX_SOCKET_DIR" "$NX_WORKSPACE_ROOT_PATH"',
        '"${NX_WORKSPACE_DATA_DIRECTORY+set:}${NX_WORKSPACE_DATA_DIRECTORY-}"',
        '"${NX_CACHE_DIRECTORY+set:}${NX_CACHE_DIRECTORY-}"',
        '"${NX_DAEMON_SOCKET_DIR+set:}${NX_DAEMON_SOCKET_DIR-}"',
      ].join(' '),
      'shell',
      script,
      scratch.links,
    ],
    { cwd: scratch.checkout, encoding: 'utf8', env: { PATH: process.env.PATH ?? '', ...inherited } },
  );
  if (result.status !== 0) printCommandOutput(result.stdout ?? '', result.stderr ?? '');
  expect(result.status).toBe(0);
  const [socket = '', root = '', data, cache, daemon] = result.stdout.split('\n');
  return {
    NX_SOCKET_DIR: socket,
    NX_WORKSPACE_ROOT_PATH: root,
    state: {
      NX_WORKSPACE_DATA_DIRECTORY: setValue(data),
      NX_CACHE_DIRECTORY: setValue(cache),
      NX_DAEMON_SOCKET_DIR: setValue(daemon),
    },
    stderr: result.stderr,
  };
}

/** A host shell: devenv's own runtime, no cowshed variables. */
function hostShell(scratch: Scratch, inherited: Record<string, string> = {}): Entered {
  return enterShell(scratch, { DEVENV_RUNTIME: scratch.hostRuntime, ...inherited });
}

/**
 * A sandboxed job: the environment cowshed's supervisor hands it. The Nx
 * capability binds NX_SOCKET_DIR to `nx` below the runtime link, and devenv's
 * runtime lives below XDG_RUNTIME_DIR, that same link.
 */
function sandboxJob(scratch: Scratch): Entered {
  return enterShell(scratch, {
    COWSHED_PORT_BASE: PORT_BASE,
    XDG_RUNTIME_DIR: scratch.link,
    DEVENV_RUNTIME: join(scratch.link, 'devenv-sandbox'),
    NX_SOCKET_DIR: join(scratch.link, 'nx'),
  });
}

describe('nx-socket-dir.sh', () => {
  it('names one socket dir for the host shell and the sandboxed job of one cowshed checkout', () => {
    withScratch((scratch) => {
      cowshedCheckout(scratch);
      // The host enters first, before cowshed made its link: it makes cowshed's link itself.
      const host = hostShell(scratch);
      expect(readlinkSync(scratch.link)).toBe(join(scratch.checkout, '.cowshed', 'run'));
      const job = sandboxJob(scratch);
      // The sandbox admits a Unix socket by its literal path, and the daemon's plugin
      // workers bind under the NX_SOCKET_DIR of whichever client connected: the strings,
      // not just their targets, must agree.
      expect(host.NX_SOCKET_DIR).toBe(job.NX_SOCKET_DIR);
      expect(host.NX_SOCKET_DIR).toBe(join(scratch.link, 'nx'));
      expect(realpathSync(host.NX_SOCKET_DIR)).toBe(realpathSync(job.NX_SOCKET_DIR));
      expect(realpathSync(host.NX_SOCKET_DIR)).toBe(scratch.leaf);
      expect(lstatSync(scratch.leaf).isDirectory()).toBe(true);
      expect(host.NX_WORKSPACE_ROOT_PATH).toBe(scratch.checkout);
      expect(job.NX_WORKSPACE_ROOT_PATH).toBe(scratch.checkout);
      expect(`${host.stderr}${job.stderr}`).toBe('');
    });
  });

  it("ignores a host socket dir and port inherited from another workspace's shell", () => {
    withScratch((scratch) => {
      cowshedCheckout(scratch);
      const host = hostShell(scratch, {
        COWSHED_PORT_BASE: '40960',
        NX_SOCKET_DIR: join(scratch.hostRuntime, 'nxrun-another', 'nx'),
      });
      expect(host.NX_SOCKET_DIR).toBe(join(scratch.link, 'nx'));
      expect(existsSync(join(scratch.links, 'cs-40960'))).toBe(false);
    });
  });

  it('reports a cowshed link that leads to another checkout and leaves it in place', () => {
    withScratch((scratch) => {
      cowshedCheckout(scratch);
      const elsewhere = join(scratch.hostRuntime, 'another-checkout-run');
      mkdirSync(elsewhere);
      symlinkSync(elsewhere, scratch.link);
      const host = hostShell(scratch);
      expect(host.stderr).toContain(`${scratch.link} does not lead to this checkout's`);
      expect(readlinkSync(scratch.link)).toBe(elsewhere);
      expect(host.NX_SOCKET_DIR).toStartWith(`${scratch.hostRuntime}/nx-`);
      expect(existsSync(host.NX_SOCKET_DIR)).toBe(true);
    });
  });

  it('gives a checkout outside cowshed its own socket dir under DEVENV_RUNTIME', () => {
    withScratch((scratch) => {
      const host = hostShell(scratch, { NX_SOCKET_DIR: join(scratch.hostRuntime, 'another-checkouts-nx') });
      expect(host.NX_SOCKET_DIR).toStartWith(`${scratch.hostRuntime}/nx-`);
      expect(existsSync(host.NX_SOCKET_DIR)).toBe(true);
      expect(existsSync(scratch.link)).toBe(false);
    });
  });

  it("keeps a job's socket dir that resolves inside the checkout when cowshed names no port", () => {
    withScratch((scratch) => {
      mkdirSync(scratch.leaf, { recursive: true });
      const bound = join(scratch.hostRuntime, 'bound');
      symlinkSync(join(scratch.checkout, '.cowshed', 'run'), bound);
      const job = hostShell(scratch, { NX_SOCKET_DIR: join(bound, 'nx') });
      expect(job.NX_SOCKET_DIR).toBe(join(bound, 'nx'));
    });
  });

  it('drops the Nx state overrides a shell bound for another workspace left behind', () => {
    withScratch((scratch) => {
      cowshedCheckout(scratch);
      // A shell that was in main's checkout, entering a fork of it: main's devenv
      // made every override absolute, and the fork's own shell keeps an absolute
      // inherited value, so without the drop Nx here opens main's database.
      const main = join(scratch.hostRuntime, 'main');
      mkdirSync(main);
      const host = hostShell(scratch, {
        NX_WORKSPACE_ROOT_PATH: main,
        NX_WORKSPACE_DATA_DIRECTORY: join(main, '.nx/workspace-data'),
        NX_CACHE_DIRECTORY: join(main, '.nx/cache'),
        NX_DAEMON_SOCKET_DIR: join(main, '.nx/daemon'),
      });
      expect(host.state).toEqual({
        NX_WORKSPACE_DATA_DIRECTORY: undefined,
        NX_CACHE_DIRECTORY: undefined,
        NX_DAEMON_SOCKET_DIR: undefined,
      });
      expect(host.NX_WORKSPACE_ROOT_PATH).toBe(scratch.checkout);
      // A root that no longer exists names no workspace this shell owns either.
      const gone = hostShell(scratch, {
        NX_WORKSPACE_ROOT_PATH: join(scratch.hostRuntime, 'retired'),
        NX_WORKSPACE_DATA_DIRECTORY: '/elsewhere/.nx/workspace-data',
      });
      expect(gone.state.NX_WORKSPACE_DATA_DIRECTORY).toBeUndefined();
    });
  });

  it('keeps the Nx state overrides bound for this workspace or for no root at all', () => {
    withScratch((scratch) => {
      const overrides = {
        NX_WORKSPACE_DATA_DIRECTORY: '/shared/lane/workspace-data',
        NX_CACHE_DIRECTORY: '/shared/lane/cache',
        NX_DAEMON_SOCKET_DIR: '/shared/lane/daemon',
      };
      // CI binds a shared tree before entering the shell and names no root.
      expect(hostShell(scratch, overrides).state).toEqual(overrides);
      // A cowshed job: the supervisor binds the root to the checkout, by any spelling.
      const alias = join(scratch.hostRuntime, 'alias');
      symlinkSync(scratch.checkout, alias);
      expect(hostShell(scratch, { ...overrides, NX_WORKSPACE_ROOT_PATH: alias }).state).toEqual(overrides);
      expect(hostShell(scratch, { ...overrides, NX_WORKSPACE_ROOT_PATH: '.' }).state).toEqual(overrides);
    });
  });
});
