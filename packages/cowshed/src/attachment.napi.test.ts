/// <reference types="bun" />

import { expect, it } from 'bun:test';
import { join } from 'node:path';

it('projects the public attachment through the built addon and controller', async () => {
  const checkout = join(import.meta.dir, '../../..');
  const fixture = process.env.BOUNDED_EXEC_FIXTURE_DIR;
  if (fixture === undefined) throw new Error('attachment test requires the native test fixture');
  const run = Bun.spawn(
    [
      'cargo',
      '--frozen',
      'nextest',
      'run',
      '--binaries-metadata',
      join(fixture, 'binaries-metadata.json'),
      '--cargo-metadata',
      join(fixture, 'cargo-metadata.json'),
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
