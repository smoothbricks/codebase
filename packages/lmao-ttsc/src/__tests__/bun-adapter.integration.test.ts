import { expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import * as adapterExports from '@smoothbricks/lmao-ttsc';

const fixturePath = fileURLToPath(new URL('./fixtures/typed-op.ts', import.meta.url));
const projectPath = fileURLToPath(new URL('../../tsconfig.test.json', import.meta.url));

test('the exported Bun adapter applies the explicitly selected native plugin', async () => {
  const adapterFactory: unknown = adapterExports.createBunTtscPlugin;
  if (typeof adapterFactory !== 'function') {
    throw new TypeError('@smoothbricks/lmao-ttsc did not export a Bun adapter factory');
  }
  const outputDir = await mkdtemp(join(dirname(fixturePath), '.bun-adapter-'));

  try {
    const result = await Bun.build({
      entrypoints: [fixturePath],
      outdir: outputDir,
      target: 'bun',
      external: ['@smoothbricks/lmao'],
      plugins: [
        adapterFactory({
          project: projectPath,
          plugins: [
            {
              transform: '@smoothbricks/lmao-ttsc/ttsc-plugin',
            },
          ],
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

    // The build output path is created per test, so a static import cannot address this module boundary.
    const builtModule = await import(pathToFileURL(output.path).href);
    const typedOp: unknown = builtModule.typedOp;
    if ((typeof typedOp !== 'object' && typeof typedOp !== 'function') || typedOp === null) {
      throw new TypeError('Bun adapter output did not export the transformed typed Op');
    }
    expect(Reflect.get(typedOp, 'runtimeHint')).toBe(9568260);
    expect('createLmaoTransformer' in adapterExports).toBe(false);
  } finally {
    await rm(outputDir, { recursive: true, force: true });
  }
});
