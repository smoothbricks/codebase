import { describe, expect, it } from 'bun:test';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { resolveOutputDirs } from './executor.js';

describe('clean-outputs executor', () => {
  it('resolves declared output patterns to safe concrete directories', () => {
    const workspaceRoot = join(tmpdir(), 'smoothbricks-clean-outputs');

    expect(
      resolveOutputDirs({
        outputs: ['{projectRoot}/dist/**/*.js', '{projectRoot}/dist/types', '{projectRoot}/dist-wasm-web'],
        projectName: 'example',
        projectRoot: 'packages/example',
        workspaceRoot,
      }).map((outputDir) => outputDir.slice(workspaceRoot.length + 1)),
    ).toEqual(['packages/example/dist', 'packages/example/dist-wasm-web']);
  });
});
