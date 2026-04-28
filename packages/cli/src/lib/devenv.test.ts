import { describe, expect, it } from 'bun:test';
import { mergeDevenvEnv, parseNulEnv } from './devenv.js';

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
});
