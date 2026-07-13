import { expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import * as adapterExports from '@smoothbricks/lmao-ttsc';

const fixturePath = fileURLToPath(new URL('./fixtures/typed-op.ts', import.meta.url));
const projectPath = fileURLToPath(new URL('../../tsconfig.test.json', import.meta.url));

test('the exported Bun adapter applies the explicitly selected native plugin', async () => {
  const adapterFactory: unknown = adapterExports.createBunTtscPlugin;
  if (typeof adapterFactory !== 'function') {
    throw new TypeError('@smoothbricks/lmao-ttsc did not export a Bun adapter factory');
  }
  const outputDir = await mkdtemp(join(tmpdir(), 'lmao-ttsc-bun-'));

  try {
    const result = await Bun.build({
      entrypoints: [fixturePath],
      outdir: outputDir,
      target: 'bun',
      external: ['@smoothbricks/lmao'],
      plugins: [
        adapterFactory({
          project: projectPath,
          plugins: [{ transform: '@smoothbricks/lmao-ttsc/ttsc-plugin' }],
        }),
      ],
    });

    if (!result.success) {
      throw new Error(result.logs.map((log) => log.message).join('\n'));
    }

    const output = result.outputs.find((candidate) => candidate.path.endsWith('.js'));
    if (output === undefined) {
      throw new Error('Bun.build did not emit JavaScript for the typed Op fixture');
    }

    const emitted = await output.text();
    expect(emitted).toMatch(/runtimeHint:\s*9568259,\s*logTemplateIds:\s*\[\s*"native fixture log"\s*\]/);
    expect('createLmaoTransformer' in adapterExports).toBe(false);
  } finally {
    await rm(outputDir, { recursive: true, force: true });
  }
});
