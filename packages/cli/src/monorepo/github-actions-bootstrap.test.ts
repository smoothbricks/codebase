import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { managedAssetsRoot } from '@smoothbricks/nx-plugin/managed-assets';
import { printCommandOutput } from '../lib/run.js';

const script = join(managedAssetsRoot, 'raw/tooling/direnv/github-actions-bootstrap.sh');

// A stand-in `devenv` that emulates `devenv shell [flags] -- cmd...`: it
// exports what a real shell's enterShell hooks would (contract vars, a
// repo-local library path, some shell bookkeeping) and execs the command,
// so build-shell exercises the real capture/filter/persist path.
const DEVENV_STUB = `#!/usr/bin/env bash
set -euo pipefail
while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do shift; done
shift
# Real enterShell hooks print progress to stdout; the capture must survive it.
echo "Installing dependencies..."
echo "enterShell noise on stdout"
export LD_LIBRARY_PATH="/nix/store/test-gcc-lib/lib"
export TTSC_TSGO_BINARY="/repo/node_modules/@typescript/native/bin/tsc"
export TTSC_CACHE_DIR="/home/runner/.cowshed/caches/ttsc"
export MULTI_LINE_VALUE="first line
second line"
export CHANGED_BY_SHELL="inner-value"
export NIX_SSL_CERT_FILE="/nix/store/test-cert"
export DEVENV_ROOT="/somewhere/tooling/direnv"
export DIRENV_DIFF="bookkeeping"
exec "$@"
`;

interface BuildShellRun {
  githubEnv: string;
  githubPath: string;
  stdout: string;
}

function runBuildShell(): BuildShellRun {
  const dir = mkdtempSync(join(tmpdir(), 'gab-'));
  try {
    const bin = join(dir, 'bin');
    mkdirSync(bin);
    const stub = join(bin, 'devenv');
    writeFileSync(stub, DEVENV_STUB);
    chmodSync(stub, 0o755);
    const githubEnv = join(dir, 'github_env');
    const githubPath = join(dir, 'github_path');
    writeFileSync(githubEnv, '');
    writeFileSync(githubPath, '');
    const r = spawnSync('bash', [script, 'build-shell'], {
      encoding: 'utf8',
      env: {
        PATH: `${bin}:${process.env.PATH ?? ''}`,
        HOME: dir,
        GITHUB_ENV: githubEnv,
        GITHUB_PATH: githubPath,
        // Present before the shell and changed by it → must persist as the
        // shell's value.
        CHANGED_BY_SHELL: 'outer-value',
        // Present before the shell and untouched by it → must NOT persist.
        UNCHANGED_BY_SHELL: 'same-value',
      },
    });
    if (r.status !== 0) {
      printCommandOutput(r.stdout ?? '', r.stderr ?? '');
    }
    expect(r.status).toBe(0);
    return {
      githubEnv: readFileSync(githubEnv, 'utf8'),
      githubPath: readFileSync(githubPath, 'utf8'),
      stdout: r.stdout ?? '',
    };
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

describe('github-actions-bootstrap build-shell environment persistence', () => {
  it('persists what the devenv shell adds or changes, filters the rest', () => {
    const run = runBuildShell();

    // Added by the shell → persisted (the LD_LIBRARY_PATH case is the class
    // this exists for: Bun-spawned native bindings need it on NixOS runners).
    expect(run.githubEnv).toContain('LD_LIBRARY_PATH=/nix/store/test-gcc-lib/lib\n');
    expect(run.githubEnv).toContain('TTSC_TSGO_BINARY=/repo/node_modules/@typescript/native/bin/tsc\n');
    expect(run.githubEnv).toContain('TTSC_CACHE_DIR=/home/runner/.cowshed/caches/ttsc\n');

    // Changed by the shell → the shell's value wins.
    expect(run.githubEnv).toContain('CHANGED_BY_SHELL=inner-value\n');

    // Untouched by the shell → not persisted.
    expect(run.githubEnv).not.toContain('UNCHANGED_BY_SHELL');

    // Multi-line values ride the GITHUB_ENV heredoc form intact.
    expect(run.githubEnv).toContain(
      'MULTI_LINE_VALUE<<__SMOO_DEVENV_ENV__\nfirst line\nsecond line\n__SMOO_DEVENV_ENV__\n',
    );

    // Runner-owned and shell-bookkeeping variables never leak into steps.
    const persistedNames = run.githubEnv
      .split('\n')
      .filter((line) => /^[A-Za-z_][A-Za-z0-9_]*(=|<<)/.test(line))
      .map((line) => line.split(/=|<</, 1)[0]);
    for (const filtered of ['NIX_SSL_CERT_FILE', 'DEVENV_ROOT', 'DIRENV_DIFF', 'PATH', 'HOME', 'SHLVL']) {
      expect(persistedNames).not.toContain(filtered);
    }

    // The persisted names are announced for the step log.
    expect(run.stdout).toContain('devenv environment persisted for later steps:');
    expect(run.stdout).toContain('LD_LIBRARY_PATH');

    // add_repo_paths still runs after persistence.
    expect(run.githubPath).toContain('tooling/direnv/.devenv/profile/bin');
  });
});

// install-devenv resolves the devenv CLI from devenv.lock, so its harness needs
// a repo_root that actually holds tooling/direnv/devenv.lock — the raw template
// directory does not. Copy the script into a temp tree instead, stub away
// `nix profile add`, and delegate `nix eval` to the real nix so the lock-reading
// expression itself is under test rather than mocked.
const REAL_NIX = spawnSync('sh', ['-c', 'command -v nix'], { encoding: 'utf8' }).stdout.trim();

const NIX_STUB = `#!/usr/bin/env bash
if [ "$1" = eval ]; then exec "$REAL_NIX" "$@"; fi
printf '%s\\n' "nix $*" >> "$NIX_CALLS"
`;

interface InstallRun {
  status: number | null;
  stdout: string;
  stderr: string;
  nixCalls: string;
}

function runInstallDevenv(lock: string, options: { devenvVersion?: string; hostRunner?: boolean } = {}): InstallRun {
  const dir = mkdtempSync(join(tmpdir(), 'gab-install-'));
  try {
    const direnv = join(dir, 'root', 'tooling', 'direnv');
    const bin = join(dir, 'bin');
    mkdirSync(direnv, { recursive: true });
    mkdirSync(bin);
    writeFileSync(join(direnv, 'github-actions-bootstrap.sh'), readFileSync(script, 'utf8'));
    writeFileSync(join(direnv, 'devenv.lock'), lock);
    const nixCalls = join(dir, 'nix-calls');
    writeFileSync(nixCalls, '');
    const stub = join(bin, 'nix');
    writeFileSync(stub, NIX_STUB);
    chmodSync(stub, 0o755);
    if (options.devenvVersion !== undefined) {
      // Shaped like the real thing: `devenv <semver>+<short rev> (<system>)`.
      const devenv = join(bin, 'devenv');
      writeFileSync(devenv, `#!/usr/bin/env bash\necho "${options.devenvVersion}"\n`);
      chmodSync(devenv, 0o755);
    }
    const r = spawnSync('bash', [join(direnv, 'github-actions-bootstrap.sh'), 'install-devenv'], {
      encoding: 'utf8',
      // A bare PATH on purpose: an ambient devenv would decide these cases
      // instead of the stub.
      env: {
        PATH: `${bin}:${dirname(REAL_NIX)}:/usr/bin:/bin`,
        HOME: join(dir, 'home'),
        NIX_CALLS: nixCalls,
        REAL_NIX,
        ...(options.hostRunner ? { SMOO_HOST_RUNNER: 'true' } : {}),
      },
    });
    return {
      status: r.status,
      stdout: r.stdout ?? '',
      stderr: r.stderr ?? '',
      nixCalls: readFileSync(nixCalls, 'utf8'),
    };
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

const REV = 'f'.repeat(40);

const LOCK_WITH_REV = JSON.stringify({
  nodes: {
    devenv: {
      locked: { dir: 'src/modules', owner: 'cachix', repo: 'devenv', rev: REV, type: 'github' },
      original: { dir: 'src/modules', owner: 'cachix', repo: 'devenv', type: 'github' },
    },
  },
  root: 'root',
  version: 7,
});

describe('github-actions-bootstrap install-devenv', () => {
  it('installs the devenv rev devenv.lock names, never a floating branch', () => {
    const run = runInstallDevenv(LOCK_WITH_REV);
    if (run.status !== 0) {
      printCommandOutput(run.stdout, run.stderr);
    }
    expect(run.status).toBe(0);
    // The rev comes out of the lock, so the CLI is the commit whose modules the
    // shell is locked to. An unpinned `github:cachix/devenv` would install
    // whatever HEAD is on the day a cold runner misses the store cache.
    expect(run.nixCalls).toContain(`nix profile add --accept-flake-config github:cachix/devenv/${REV}`);
    expect(run.nixCalls).not.toContain('github:cachix/devenv\n');
    // Announced, so a run's log names the version it installed.
    expect(run.stdout).toContain(`github:cachix/devenv/${REV}`);
  });

  it('refuses to install anything when the lock names no devenv rev', () => {
    const run = runInstallDevenv(JSON.stringify({ nodes: {}, root: 'root', version: 7 }));
    expect(run.status).not.toBe(0);
    expect(run.stderr).toContain('cannot resolve .nodes.devenv.locked.rev');
    // Silently falling back to an unpinned flake is the failure being prevented.
    expect(run.nixCalls).not.toContain('profile add');
  });

  it('keeps a restored devenv built from the locked commit', () => {
    const run = runInstallDevenv(LOCK_WITH_REV, { devenvVersion: 'devenv 2.3.1+fffffff (aarch64-darwin)' });
    expect(run.status).toBe(0);
    expect(run.stdout).toContain('using locked devenv');
    // The warm path must cost nothing: no eval of the flake, no profile writes.
    expect(run.nixCalls).toBe('');
  });

  it('replaces a restored devenv that is a different commit than the lock', () => {
    // The real drift: the store cache restores ~/.nix-profile wholesale, so a
    // rotated key hands the job the CLI of whichever run last populated the
    // prefix. Observed in run 34369909403 as devenv 2.3.1+2a399e9 against a
    // lock naming 190959a.
    const run = runInstallDevenv(LOCK_WITH_REV, { devenvVersion: 'devenv 2.3.1+2a399e9 (aarch64-darwin)' });
    if (run.status !== 0) {
      printCommandOutput(run.stdout, run.stderr);
    }
    expect(run.status).toBe(0);
    // Removal first: `nix profile add` collides on bin/devenv at equal priority.
    const removeAt = run.nixCalls.indexOf('profile remove --all');
    const addAt = run.nixCalls.indexOf(`profile add --accept-flake-config github:cachix/devenv/${REV}`);
    expect(removeAt).toBeGreaterThanOrEqual(0);
    expect(addAt).toBeGreaterThan(removeAt);
    // Said out loud, with both commits, so a log explains the replacement.
    expect(run.stdout).toContain('replacing devenv');
    expect(run.stdout).toContain('2a399e9');
  });

  it('leaves a host runner its own devenv, whatever commit it is', () => {
    // Host runners own their Nix profile; a repository rewriting it would take
    // the whole fleet's shared store with it.
    const run = runInstallDevenv(LOCK_WITH_REV, {
      devenvVersion: 'devenv 2.3.1+2a399e9 (x86_64-linux)',
      hostRunner: true,
    });
    expect(run.status).toBe(0);
    expect(run.stdout).toContain('using host devenv');
    expect(run.nixCalls).toBe('');
  });

  it('installs from the floating branch on a host runner with no devenv', () => {
    // The one place drift is accepted deliberately. A host installs into a
    // long-lived profile that roots the fleet's shared store, and holding it
    // back to an older CLI than the fleet was running segfaulted the Rust
    // linker in run 34374650577. Refusing to install at all was also wrong —
    // that failed linux-release-candidate in run 34373420924.
    const run = runInstallDevenv(LOCK_WITH_REV, { hostRunner: true });
    if (run.status !== 0) {
      printCommandOutput(run.stdout, run.stderr);
    }
    expect(run.status).toBe(0);
    expect(run.nixCalls).toContain('nix profile add --accept-flake-config github:cachix/devenv\n');
    expect(run.nixCalls).not.toContain(REV);
    // Never on a host: the profile roots a store the whole fleet shares.
    expect(run.nixCalls).not.toContain('profile remove');
  });
});
