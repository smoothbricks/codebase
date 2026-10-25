import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { join } from 'node:path';
import { managedAssetsRoot } from '@smoothbricks/nx-plugin/managed-assets';
import { printCommandOutput } from '../lib/run.js';

const script = join(managedAssetsRoot, 'raw/tooling/direnv/compiler-variables.sh');

/** Source the script with bash as devenv.smoo.nix's prologue does, naming `declared`; the environment it leaves. */
function enter(
  env: Record<string, string>,
  declared: readonly string[],
): { environment: Map<string, string>; stderr: string } {
  const result = spawnSync('bash', ['-c', '. "$0" "$@" && env -0', script, ...declared], { encoding: 'utf8', env });
  if (result.status !== 0) printCommandOutput(result.stdout, result.stderr);
  expect(result.status).toBe(0);
  const environment = new Map<string, string>();
  for (const entry of result.stdout.split('\0')) {
    const equals = entry.indexOf('=');
    if (equals > 0) environment.set(entry.slice(0, equals), entry.slice(equals + 1));
  }
  return { environment, stderr: result.stderr };
}

describe('compiler-variables.sh', () => {
  it('drops every inherited cc-rs compiler variable the shell does not declare, announcing each', () => {
    const { environment, stderr } = enter(
      {
        PATH: '/usr/bin:/bin',
        CC_aarch64_apple_darwin: '/usr/bin/clang',
        CXX_aarch64_apple_darwin: '/usr/bin/clang++',
        AR_aarch64_apple_darwin: '/usr/bin/ar',
        RANLIB_x86_64_unknown_linux_musl: 'ranlib',
        CFLAGS_aarch64_apple_darwin: '-O2',
        CXXFLAGS_wasm32_unknown_unknown: '-O2',
        ARFLAGS_aarch64_apple_darwin: 'crs',
        CC_x86_64_unknown_linux_gnu: '/nix/store/cc-wrapper/bin/cc',
        CXX_x86_64_unknown_linux_gnu: '/nix/store/cc-wrapper/bin/c++',
        RANLIBFLAGS_aarch64_apple_darwin: '-D',
        CFLAGS: '-O1',
        HOST_CC: 'cc',
        TARGET_CXXFLAGS: '-O3',
        CC: 'gcc',
        AR: 'ar',
        CC_ENABLE_DEBUG_OUTPUT: '1',
        HOST_NAME: 'kept',
        // A collating locale must not let a target's lowercase match CC_ENABLE_DEBUG_OUTPUT.
        LC_ALL: 'en_US.UTF-8',
      },
      ['CC_x86_64_unknown_linux_gnu', 'CXX_x86_64_unknown_linux_gnu'],
    );
    for (const name of [
      'CC_aarch64_apple_darwin',
      'CXX_aarch64_apple_darwin',
      'AR_aarch64_apple_darwin',
      'RANLIB_x86_64_unknown_linux_musl',
      'CFLAGS_aarch64_apple_darwin',
      'CXXFLAGS_wasm32_unknown_unknown',
      'ARFLAGS_aarch64_apple_darwin',
      'RANLIBFLAGS_aarch64_apple_darwin',
      'CFLAGS',
      'HOST_CC',
      'TARGET_CXXFLAGS',
    ]) {
      expect(environment.has(name), name).toBe(false);
    }
    // The shell's own declarations, the generic tool names the stdenv owns, cc-rs's settings and unrelated names stay.
    expect(environment.get('CC_x86_64_unknown_linux_gnu')).toBe('/nix/store/cc-wrapper/bin/cc');
    expect(environment.get('CXX_x86_64_unknown_linux_gnu')).toBe('/nix/store/cc-wrapper/bin/c++');
    expect(environment.get('CC')).toBe('gcc');
    expect(environment.get('AR')).toBe('ar');
    expect(environment.get('CC_ENABLE_DEBUG_OUTPUT')).toBe('1');
    expect(environment.get('HOST_NAME')).toBe('kept');
    expect(stderr).toContain(
      "devenv: dropping inherited CC_aarch64_apple_darwin (/usr/bin/clang); this shell declares cc-rs's compiler variables",
    );
    expect(stderr.split('\n').filter((line) => line.startsWith('devenv: dropping inherited '))).toHaveLength(11);
    // Its own working names leave with it.
    expect([...environment.keys()].filter((name) => name.startsWith('smoo_compiler_'))).toEqual([]);
  });

  it('says nothing and changes nothing in an environment that carries none', () => {
    const { environment, stderr } = enter({ PATH: '/usr/bin:/bin', CC: 'clang' }, []);
    expect(stderr).toBe('');
    expect(environment.get('CC')).toBe('clang');
  });
});
