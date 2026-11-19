/// <reference types="bun" />

import { expect, it } from 'bun:test';
import { join } from 'node:path';
import { cargoTargetDirectory, NEXTEST_BINARIES_METADATA, NEXTEST_CARGO_METADATA } from '@smoothbricks/nextest-extract';

it('projects the public attachment through the built addon and controller', async () => {
  const checkout = join(import.meta.dir, '../../..');
  const target = await cargoTargetDirectory(checkout);
  const run = Bun.spawn(
    [
      'cargo',
      '--frozen',
      'nextest',
      'run',
      '--binaries-metadata',
      NEXTEST_BINARIES_METADATA,
      '--cargo-metadata',
      NEXTEST_CARGO_METADATA,
      '--target-dir-remap',
      target,
      '--workspace-remap',
      checkout,
      '--filterset',
      'binary_id(cowshed-core::project_runtime) and test(=napi_attachment_projects_the_input_view)',
      '--run-ignored',
      'only',
      '--no-tests=fail',
      '--user-config-file',
      'none',
      '--tool-config-file',
      `smoo:${join(checkout, 'packages/nx-plugin/nextest.toml')}`,
    ],
    { cwd: checkout, stdout: 'inherit', stderr: 'inherit' },
  );
  expect(await run.exited).toBe(0);
});
