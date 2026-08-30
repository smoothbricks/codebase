import { describe, expect, it } from 'bun:test';

import { harnessArgsFor, parseCargoTestArtifacts } from './artifacts.js';

describe('parseCargoTestArtifacts', () => {
  it('keeps test-profile and test-kind executables and drops the rest', () => {
    const ndjson = [
      JSON.stringify({
        reason: 'compiler-artifact',
        executable: '/tmp/target/debug/deps/apfs_integration-abc',
        target: { name: 'apfs_integration', kind: ['test'] },
        profile: { test: true },
      }),
      JSON.stringify({
        reason: 'compiler-artifact',
        executable: '/tmp/target/debug/deps/cowshed_core-def',
        target: { name: 'cowshed_core', kind: ['lib'] },
        profile: { test: true },
      }),
      JSON.stringify({
        reason: 'compiler-artifact',
        executable: '/tmp/target/debug/cowshed',
        target: { name: 'cowshed', kind: ['bin'] },
        profile: { test: false },
      }),
      JSON.stringify({ reason: 'build-finished', success: true }),
      'not json',
    ].join('\n');

    const binaries = parseCargoTestArtifacts(ndjson);
    expect(binaries.map((binary) => binary.name)).toEqual(['apfs_integration', 'cowshed_core']);
    const apfs = binaries.find((binary) => binary.name === 'apfs_integration');
    const lib = binaries.find((binary) => binary.name === 'cowshed_core');
    expect(apfs && harnessArgsFor(apfs)).toEqual(['--test-threads', '1']);
    expect(lib && harnessArgsFor(lib)).toEqual([]);
  });
});
