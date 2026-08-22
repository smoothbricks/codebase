import { describe, expect, it } from 'bun:test';
import type { NxJson, NxProjectJson } from '../../lib/json.js';
import { collectBuildInputs, resolveBuildInputPatterns } from '../build-inputs.js';

const nxJson: NxJson = {
  namedInputs: {
    default: ['{projectRoot}/**/*', 'sharedGlobals'],
    production: [
      '{projectRoot}/src/**/*',
      '{projectRoot}/package.json',
      '!{projectRoot}/**/__tests__/**',
      '!{projectRoot}/**/*.test.*',
    ],
    sharedGlobals: ['{workspaceRoot}/.github/workflows/ci.yml'],
  },
};

/** A native package: `build` aggregates, and the cargo inputs live on the fanned-out targets. */
const nativeProject: NxProjectJson = {
  targets: {
    build: {
      executor: 'nx:noop',
      dependsOn: ['^build', '*-js', '*-napi', '*-macos'],
    },
    'cli-arm64-macos': {
      inputs: [
        '{projectRoot}/**/*.rs',
        '{projectRoot}/**/Cargo.toml',
        '{projectRoot}/**/Cargo.lock',
        '!{projectRoot}/**/target/**',
        '{projectRoot}/package.json',
      ],
    },
    'napi-arm64-macos': {
      inputs: ['{projectRoot}/crates/**/*.rs'],
    },
    'tsc-js': {
      inputs: ['{projectRoot}/src/**/*'],
    },
    'cargo-test': {
      inputs: ['{projectRoot}/**/*.rs'],
    },
  },
};

describe('resolveBuildInputPatterns', () => {
  /**
   * The regression: a glob in `dependsOn` read as a literal target name matches no target, so
   * every declared cargo input is invisible and the resolver falls back to `production`. A
   * Rust-only change then looks unreleasable, cowshed is dropped from the release, `*-macos`
   * matches nothing in the platform job, and the fix never reaches npm.
   */
  it('reads inputs from every target a dependsOn glob matches', () => {
    const patterns = resolveBuildInputPatterns(nativeProject, nxJson);

    expect(patterns).toContain('**/*.rs');
    expect(patterns).toContain('**/Cargo.toml');
    expect(patterns).toContain('**/Cargo.lock');
    expect(patterns).toContain('!**/target/**');
    expect(patterns).toContain('crates/**/*.rs');
    expect(patterns).toContain('src/**/*');
  });

  /** `*-macos` must not reach `cargo-test`: only the targets `build` actually depends on count. */
  it('ignores targets no dependsOn entry names', () => {
    const patterns = collectBuildInputs({
      build: { dependsOn: ['*-macos'] },
      'cli-arm64-macos': { inputs: ['{projectRoot}/**/*.rs'] },
      'cargo-test': { inputs: ['{projectRoot}/tests/**/*.rs'] },
    });

    expect(patterns).toEqual(['{projectRoot}/**/*.rs']);
  });

  it('keeps literal dependsOn names working and skips upstream-project edges', () => {
    const patterns = collectBuildInputs({
      build: { dependsOn: ['^build', 'tsc-js', 'missing-target'] },
      'tsc-js': { inputs: ['{projectRoot}/src/**/*'] },
    });

    expect(patterns).toEqual(['{projectRoot}/src/**/*']);
  });

  it('prefers the build target own inputs over its dependencies', () => {
    const patterns = collectBuildInputs({
      build: { inputs: ['{projectRoot}/explicit/**/*'], dependsOn: ['*-napi'] },
      'napi-arm64-macos': { inputs: ['{projectRoot}/**/*.rs'] },
    });

    expect(patterns).toEqual(['{projectRoot}/explicit/**/*']);
  });

  /** No declaration to read: `production` is the conservative fallback, not silence. */
  it('falls back to production when nothing declares an input', () => {
    expect(collectBuildInputs({ build: { dependsOn: ['*-napi'] } })).toEqual(['production']);
    expect(collectBuildInputs({ other: { inputs: ['{projectRoot}/src/**/*'] } })).toEqual(['production']);
    expect(resolveBuildInputPatterns({ targets: { build: {} } }, nxJson)).toEqual([
      'src/**/*',
      'package.json',
      '!**/__tests__/**',
      '!**/*.test.*',
    ]);
  });
});
