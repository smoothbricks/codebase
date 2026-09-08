import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { mergeDevenvEnv, parseNulEnv, withDevenvEnv } from './devenv.js';
import { runResult } from './run.js';

describe('devenv environment loader', () => {
  it('parses NUL-separated env output without splitting values on newlines or equals signs', () => {
    const bytes = new TextEncoder().encode('SIMPLE=value\0MULTILINE=line 1\nline 2\0TOKEN=a=b=c\0');

    expect(parseNulEnv(bytes)).toEqual({
      SIMPLE: 'value',
      MULTILINE: 'line 1\nline 2',
      TOKEN: 'a=b=c',
    });
  });

  it('merges devenv output over the existing environment instead of replacing it', () => {
    expect(
      mergeDevenvEnv(
        { GH_TOKEN: 'existing-token', NODE_AUTH_TOKEN: 'npm-token', PATH: '/usr/bin' },
        { DEVENV_ROOT: '/repo/tooling/direnv', PATH: '/nix/bin' },
      ),
    ).toEqual({
      DEVENV_ROOT: '/repo/tooling/direnv',
      GH_TOKEN: 'existing-token',
      NODE_AUTH_TOKEN: 'npm-token',
      PATH: '/nix/bin',
    });
  });

  it('keeps the selected toolchain ahead of login-profile tools', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-toolchain-env-'));
    const saved = { ...process.env };
    try {
      for (const directory of ['bin', 'selected', 'login', 'home', 'tooling/direnv']) {
        await mkdir(join(root, directory), { recursive: true });
      }
      await writeFile(
        join(root, 'bin/devenv'),
        '#!/bin/sh\nshift 2\nexport PATH="$SMOO_TEST_SELECTED:$PATH"\nexec "$@"\n',
        { mode: 0o755 },
      );
      await writeFile(join(root, 'selected/smoo-toolchain-probe'), '#!/bin/sh\nprintf selected\n', { mode: 0o755 });
      await writeFile(join(root, 'login/smoo-toolchain-probe'), '#!/bin/sh\nprintf login\n', { mode: 0o755 });
      await writeFile(join(root, 'home/.bash_profile'), 'export PATH="$SMOO_TEST_LOGIN:$PATH"\n');
      process.env.HOME = join(root, 'home');
      process.env.PATH = `${join(root, 'bin')}:${saved.PATH ?? '/usr/bin:/bin'}`;
      process.env.SMOO_TEST_SELECTED = join(root, 'selected');
      process.env.SMOO_TEST_LOGIN = join(root, 'login');
      delete process.env.BASH_ENV;
      const result = await withDevenvEnv(root, () => runResult('smoo-toolchain-probe', [], root));
      expect(result.exitCode).toBe(0);
      expect(result.stdout).toBe('selected');
      expect(process.env.PATH).toBe(`${join(root, 'bin')}:${saved.PATH ?? '/usr/bin:/bin'}`);
    } finally {
      for (const key of Object.keys(process.env)) delete process.env[key];
      Object.assign(process.env, saved);
      await rm(root, { recursive: true, force: true });
    }
  });
});
