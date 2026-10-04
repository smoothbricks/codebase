import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import {
  cpSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { join } from 'node:path';
import { managedAssetsRoot } from '@smoothbricks/nx-plugin/managed-assets';
import { printCommandOutput } from '../lib/run.js';

const program = join(managedAssetsRoot, 'raw/tooling/direnv/inherited-devenv.ts');
const repositoryLock = join(managedAssetsRoot, '..', '..', '..', 'tooling', 'direnv', 'devenv.lock');
const timeout = 1_200_000;
type Workspace = { root: string; env: Record<string, string> };
const PROXY = ['HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'http_proxy', 'https_proxy', 'all_proxy'] as const;
// The caller's live routing and trust, as the caller has them: inside a cowshed
// workspace, its own gateway endpoint (token as userinfo), exclusions and CA.
const routing: Record<string, string> = {};
for (const name of [
  ...PROXY,
  'NO_PROXY',
  'no_proxy',
  'NIX_CONFIG',
  'NIX_SSL_CERT_FILE',
  'SSL_CERT_FILE',
  'NODE_EXTRA_CA_CERTS',
  'GIT_SSL_CAINFO',
]) {
  const value = process.env[name];
  if (value !== undefined) routing[name] = value;
}
const callerHome = process.env.HOME;
if (!callerHome) throw new Error('the real devenv fixture needs the caller HOME that locates its Nix cache');
// Nix's client cache, resolved as nix resolves it for the caller. Each fixture
// gets a private XDG_CACHE_HOME whose `nix` links to this machine-wide fetch
// index, exactly as cowshed shares one across workspaces: it is not checkout
// state, and without it every first entry downloads the locked nixpkgs tarball
// from GitHub again.
const nixCache = process.env.NIX_CACHE_HOME ?? join(process.env.XDG_CACHE_HOME ?? join(callerHome, '.cache'), 'nix');

function lock(): string {
  const original = JSON.parse(readFileSync(repositoryLock, 'utf8'));
  return `${JSON.stringify({
    nodes: {
      devenv: original.nodes.devenv,
      nixpkgs: original.nodes.nixpkgs,
      'nixpkgs-src': original.nodes['nixpkgs-src'],
      root: { inputs: { devenv: 'devenv', nixpkgs: 'nixpkgs' } },
    },
    root: 'root',
    version: original.version,
  })}\n`;
}

function workspace(scratch: string, name: string, nix: string): Workspace {
  const root = join(scratch, name);
  const dir = join(root, 'tooling', 'direnv');
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, 'devenv.nix'), nix);
  writeFileSync(join(dir, 'devenv.yaml'), 'inputs:\n  nixpkgs:\n    url: github:cachix/devenv-nixpkgs/rolling\n');
  writeFileSync(join(dir, 'devenv.lock'), lock());
  const env = {
    PATH: process.env.PATH ?? '',
    COWSHED_PORT_BASE: '49136',
    HOME: join(root, '.home'),
    TMPDIR: join(scratch, `tmp-${name}`),
    XDG_RUNTIME_DIR: join(scratch, `run-${name}`),
    XDG_DATA_HOME: join(root, '.data'),
    XDG_CACHE_HOME: join(root, '.cache'),
  };
  for (const directory of [
    env.HOME,
    env.TMPDIR,
    env.XDG_RUNTIME_DIR,
    env.XDG_CACHE_HOME,
    join(env.XDG_DATA_HOME, 'devenv'),
  ])
    mkdirSync(directory, { recursive: true });
  symlinkSync(nixCache, join(env.XDG_CACHE_HOME, 'nix'));
  writeFileSync(
    join(env.XDG_DATA_HOME, 'devenv', 'cachix_trusted_keys.json'),
    '{"devenv":"devenv.cachix.org-1:w1cLUi8dv3hnoSPGAuibQv+f9TZLr6cv/Hm9XgU50cw="}\n',
  );
  return { root, env: { ...routing, ...env } };
}

function clone(scratch: string, from: Workspace, name: string): Workspace {
  const to = workspace(scratch, name, readFileSync(join(from.root, 'tooling/direnv/devenv.nix'), 'utf8'));
  cpSync(join(from.root, 'tooling/direnv/.devenv'), join(to.root, 'tooling/direnv/.devenv'), {
    recursive: true,
    force: true,
    dereference: false,
  });
  return to;
}

function shell(
  ws: Workspace,
  options: readonly string[] = [],
): {
  script: string;
  stderr: string;
  imported: Record<string, string>;
  runsBeforeImport: number;
} {
  const dir = join(ws.root, 'tooling', 'direnv');
  const result = spawnSync('bun', [program, ws.root, ...options, 'direnv-export'], {
    cwd: dir,
    env: ws.env,
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024,
  });
  if (result.status !== 0) printCommandOutput(result.stdout ?? '', result.stderr ?? '');
  expect(result.status).toBe(0);
  const hookFile = join(dir, 'hook-runs');
  const runsBeforeImport = existsSync(hookFile) ? readFileSync(hookFile, 'utf8').split('\n').length - 1 : 0;
  const file = join(ws.env.TMPDIR, 'export.sh');
  writeFileSync(file, result.stdout);
  const evaluated = spawnSync('bash', ['-c', 'eval "$(cat "$1")"; env -0', 'import', file], {
    cwd: dir,
    env: ws.env,
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024,
  });
  if (evaluated.status !== 0) printCommandOutput(evaluated.stdout ?? '', evaluated.stderr ?? '');
  expect(evaluated.status).toBe(0);
  const imported: Record<string, string> = {};
  for (const entry of evaluated.stdout.split('\0')) {
    const separator = entry.indexOf('=');
    if (separator > 0) imported[entry.slice(0, separator)] = entry.slice(separator + 1);
  }
  return { script: result.stdout, stderr: result.stderr, imported, runsBeforeImport };
}

describe('private inherited devenv with real devenv', () => {
  it(
    'runs a checkout-local first-entry hook and computes its own Nx socket despite inherited caller state',
    () => {
      const scratch = realpathSync(mkdtempSync('/tmp/smoo-inh-'));
      try {
        const managedShell = readFileSync(join(managedAssetsRoot, 'raw/tooling/direnv/devenv.smoo.nix'), 'utf8');
        const socketHook = managedShell.match(/(nx_workspace_root=.*?mkdir -p "\$NX_SOCKET_DIR")/s)?.[1];
        if (!socketHook) throw new Error('the managed shell must compute its workspace-owned Nx socket');
        const main = workspace(
          scratch,
          'main',
          `{ ... }: { enterShell = ''
        ${socketHook}
        echo entered >> "$DEVENV_ROOT/hook-runs"
      ''; }
`,
        );
        const foreignSocket = join(scratch, 'another-checkouts-nx-socket');
        main.env.NX_SOCKET_DIR = foreignSocket;
        const first = shell(main);
        expect(first.runsBeforeImport).toBe(0);
        expect(readFileSync(join(main.root, 'tooling/direnv/hook-runs'), 'utf8')).toBe('entered\n');
        expect(first.imported.DEVENV_ROOT).toBe(join(main.root, 'tooling/direnv'));
        const firstSocket = first.imported.NX_SOCKET_DIR;
        if (!firstSocket) throw new Error('the entered shell did not publish its Nx socket');
        expect(first.imported.NX_WORKSPACE_ROOT_PATH).toBe(main.root);
        expect(firstSocket).toStartWith(`${first.imported.DEVENV_RUNTIME}/nx-`);
        expect(firstSocket).not.toBe(foreignSocket);
        expect(existsSync(firstSocket)).toBe(true);
        const inherited = join(main.root, 'tooling/direnv/.devenv/inherited-shell.json');
        expect(existsSync(inherited)).toBe(true);
        const recorded = readFileSync(inherited, 'utf8');
        expect(recorded).not.toContain(foreignSocket);
        expect(recorded).not.toContain('"NX_SOCKET_DIR":');
        const next = clone(scratch, main, 'next');
        next.env.NX_SOCKET_DIR = firstSocket;
        const second = shell(next);
        expect(second.runsBeforeImport).toBe(0);
        expect(second.stderr).toContain('reused this checkout');
        expect(readFileSync(join(main.root, 'tooling/direnv/hook-runs'), 'utf8')).toBe('entered\n');
        expect(readFileSync(join(next.root, 'tooling/direnv/hook-runs'), 'utf8')).toBe('entered\n');
        expect(second.imported.DEVENV_ROOT).toBe(join(next.root, 'tooling/direnv'));
        expect(second.imported.DEVENV_STATE).toBe(join(next.root, 'tooling/direnv/.devenv/state'));
        const secondSocket = second.imported.NX_SOCKET_DIR;
        if (!secondSocket) throw new Error('the inherited shell did not publish its Nx socket');
        expect(second.imported.NX_WORKSPACE_ROOT_PATH).toBe(next.root);
        expect(secondSocket).toStartWith(`${second.imported.DEVENV_RUNTIME}/nx-`);
        expect(secondSocket).not.toBe(firstSocket);
        expect(existsSync(secondSocket)).toBe(true);
        expect(second.script).not.toContain(main.root);
        writeFileSync(join(next.root, 'tooling/direnv/devenv.nix'), '{ ... }: { env.INPUT_CHANGED = "yes"; }\n');
        expect(shell(next).imported.INPUT_CHANGED).toBe('yes');
      } finally {
        rmSync(scratch, { recursive: true, force: true });
      }
    },
    timeout,
  );

  it(
    'publishes the private artifact from a host origin for a fresh cowshed clone',
    () => {
      const scratch = realpathSync(mkdtempSync('/tmp/smoo-inh-'));
      try {
        // An actual pipe must drain the entire export, not just Bun's first 64 KiB at exit.
        const padding = 'p'.repeat(70_000);
        const origin = workspace(
          scratch,
          'main',
          `{ ... }: { env.PROBE = "host-origin"; env.SHELL_PADDING = "${padding}"; }\n`,
        );
        // Host HOME is an ancestor of the checkout; a relocation must never replace
        // the new checkout's path again while moving the old HOME.
        origin.env.HOME = scratch;
        delete origin.env.COWSHED_PORT_BASE;
        origin.env.PRIVATE_CREDENTIAL = 'origin-only-secret-sentinel';
        // A live proxy credential the evaluator receives. Every fetch here is HTTPS,
        // so a plain-HTTP proxy routes nothing; any caller HTTPS routing stays as is.
        origin.env.http_proxy = 'http://cowshed:origin-proxy-token-sentinel@127.0.0.1:49136';
        const first = shell(origin);
        expect(first.imported.PROBE).toBe('host-origin');
        expect(first.imported.SHELL_PADDING).toBe(padding);
        expect(existsSync(join(origin.root, 'tooling/direnv/.devenv/inherited-shell.json'))).toBe(true);
        const artifact = readFileSync(join(origin.root, 'tooling/direnv/.devenv/inherited-shell.json'), 'utf8');
        for (const secret of [
          origin.env.PRIVATE_CREDENTIAL,
          'origin-proxy-token-sentinel',
          ...PROXY.flatMap((name) => origin.env[name] ?? []),
        ])
          expect(artifact).not.toContain(secret);
        const next = clone(scratch, origin, 'fresh-shed');
        next.env.http_proxy = 'http://cowshed:fresh-shed-proxy-token-sentinel@127.0.0.1:49200';
        const second = shell(next);
        expect(second.stderr).toContain('reused this checkout');
        expect(second.imported.PROBE).toBe('host-origin');
        expect(second.imported.SHELL_PADDING).toBe(padding);
        expect(second.imported.DEVENV_ROOT).toBe(join(next.root, 'tooling/direnv'));
        expect(second.script).not.toContain(origin.root);
        expect(second.script).not.toContain(origin.env.PRIVATE_CREDENTIAL);
        expect(second.script).not.toContain('origin-proxy-token-sentinel');
        // The reused shell routes through the clone's own proxy, never the origin's.
        for (const name of PROXY) expect(second.imported[name]).toBe(next.env[name]);
      } finally {
        rmSync(scratch, { recursive: true, force: true });
      }
    },
    timeout,
  );

  it(
    'evaluates with the live proxy credential but never publishes an export that captured it',
    () => {
      const scratch = realpathSync(mkdtempSync('/tmp/smoo-inh-'));
      try {
        const main = workspace(scratch, 'main', '{ ... }: { env.CAPTURED_PROXY = builtins.getEnv "http_proxy"; }\n');
        main.env.http_proxy = 'http://cowshed:captured-proxy-token-sentinel@127.0.0.1:49136';
        // Impure evaluation lets devenv.nix read the evaluator's environment. Only an
        // evaluator that received the live credential can embed it, and that export
        // stays this checkout's: it is refused before anything is published.
        const first = shell(main, ['--impure']);
        expect(first.stderr).toContain('proxy routing, which no clone may inherit; evaluating in place');
        expect(first.imported.CAPTURED_PROXY).toBe(main.env.http_proxy);
        expect(existsSync(join(main.root, 'tooling/direnv/.devenv/inherited-shell.json'))).toBe(false);
      } finally {
        rmSync(scratch, { recursive: true, force: true });
      }
    },
    timeout,
  );

  it(
    'ignores poisoned shared cache and refuses inherited artifact symlinks across sheds',
    () => {
      const scratch = realpathSync(mkdtempSync('/tmp/smoo-inh-'));
      try {
        const main = workspace(scratch, 'main', '{ ... }: { env.PROBE = "origin"; }\n');
        shell(main);
        const victim = clone(scratch, main, 'victim');
        const attacker = workspace(scratch, 'attacker', '{ ... }: { env.PROBE = "attacker"; }\n');
        const shared = join(scratch, 'shared-cache');
        mkdirSync(shared);
        const marker = join(attacker.root, 'poison-ran');
        const originArtifact = JSON.parse(
          readFileSync(join(main.root, 'tooling/direnv/.devenv/inherited-shell.json'), 'utf8'),
        );
        writeFileSync(
          join(shared, 'inherited-shell.json'),
          JSON.stringify({
            ...originArtifact,
            export: `${originArtifact.export}\nprintf poisoned > ${JSON.stringify(marker)}\n`,
          }),
        );
        victim.env.COWSHED_DEVENV_CACHE = shared;
        const artifact = join(victim.root, 'tooling/direnv/.devenv/inherited-shell.json');
        rmSync(artifact);
        symlinkSync(join(shared, 'inherited-shell.json'), artifact);
        const result = shell(victim);
        expect(result.imported.PROBE).toBe('origin');
        expect(result.stderr).not.toContain('reused this checkout');
        expect(readFileSync(join(shared, 'inherited-shell.json'), 'utf8')).toContain('poisoned');
        expect(existsSync(marker)).toBe(false);
      } finally {
        rmSync(scratch, { recursive: true, force: true });
      }
    },
    timeout,
  );
});
