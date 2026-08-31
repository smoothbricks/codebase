import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { printCommandOutput } from '../lib/run.js';

const script = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '..',
  '..',
  'managed/raw/tooling/direnv/github-actions-bootstrap.sh',
);

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
