import { expect, test } from 'bun:test';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { readCargoTestGeneratedInputs } from './cargo-test-generated-inputs.js';

//#region smoo/cowshed!n/build-precedes-reservation
test('generated Cargo source owners are ordered and keyed only for the leaves that read them', () => {
  const root = mkdtempSync(join(tmpdir(), 'native-generated-inputs-'));
  try {
    mkdirSync(join(root, 'codegen'));
    writeFileSync(
      join(root, 'package.json'),
      JSON.stringify({
        nx: {
          targets: {
            'generated-sources': {
              executor: 'nx:noop',
              dependsOn: ['tools:codec', 'tools:fixtures'],
              metadata: { cargoTestReads: { 'crates/reader/build.rs': ['shared/wire.js'] } },
            },
          },
        },
      }),
    );
    writeFileSync(
      join(root, 'codegen/project.json'),
      JSON.stringify({
        targets: {
          codec: {
            outputs: [
              '{workspaceRoot}/crates/reader/src/codec.rs',
              '{projectRoot}/unrelated.ts',
              '{workspaceRoot}/shared/wire.js',
            ],
          },
          fixtures: { outputs: ['{workspaceRoot}/crates/reader/tests/fixtures/'] },
        },
      }),
    );
    const projects = new Map([
      ['repo', '.'],
      ['tools', 'codegen'],
    ]);
    const generated = readCargoTestGeneratedInputs(root, 'repo:generated-sources', projects);
    expect(generated.forLeaf(['{workspaceRoot}/crates/reader/**/*'], 'crates/reader')).toEqual({
      dependsOn: ['tools:codec', 'tools:fixtures'],
      inputs: ['{workspaceRoot}/shared/wire.js', { dependentTasksOutputFiles: 'shared/wire.js', transitive: false }],
    });
    expect(generated.forLeaf(['{workspaceRoot}/crates/writer/**/*'], 'crates/writer')).toEqual({
      dependsOn: [],
      inputs: [],
    });
    expect(
      generated.forLeaf(['{workspaceRoot}/crates/reader/tests/fixtures/golden.bin'], 'crates/reader').dependsOn,
    ).toEqual(['tools:fixtures']);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('an ordinary repo without explicit generated ownership does not read declarations', () => {
  const generated = readCargoTestGeneratedInputs('/not/a/workspace', undefined, new Map());
  expect(generated.forLeaf(['{projectRoot}/src/**/*'], 'crates/reader')).toEqual({ dependsOn: [], inputs: [] });
});
//#endregion smoo/cowshed!n/build-precedes-reservation
