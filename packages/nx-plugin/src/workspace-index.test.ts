import { describe, expect, it } from 'bun:test';
import { mkdir, readdir, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { nxFixtureRoot } from './__tests__/fixture-nx-env.js';
import { globIndexedFiles } from './workspace-index.js';

/**
 * The job running Nx exports its checkout's data directory as an absolute NX_WORKSPACE_DATA_DIRECTORY (CI's
 * setup-devenv action, a cowshed job), and Nx resolves it for any root. A root other than this workspace, such as a
 * test fixture or a foreign checkout, globbed under it walks under that checkout's `nx_files.lock` and archive: a walk
 * that waits on the lock loads the archive another root's walk wrote. This suite's preload drops the variable; the
 * case sets it again, as a process without that preload inherits it.
 */
describe('globIndexedFiles', () => {
  it("globs a root other than this workspace under that root's own data directory", async () => {
    const root = await nxFixtureRoot('glob-foreign-');
    const shared = await nxFixtureRoot('glob-shared-data-');
    const inherited = process.env.NX_WORKSPACE_DATA_DIRECTORY;
    try {
      await writeFile(join(root, 'nx.json'), '{}\n');
      await writeFile(join(root, '.gitignore'), 'p/ignored.txt\n');
      await mkdir(join(root, 'p'));
      await writeFile(join(root, 'p', 'one.txt'), 'one\n');
      await writeFile(join(root, 'p', 'ignored.txt'), 'ignored\n');
      process.env.NX_WORKSPACE_DATA_DIRECTORY = shared;
      const files = await globIndexedFiles(root, ['**/*.txt']);
      expect([...files].sort(), "the root's own files, its ignore rules applied").toEqual(['p/one.txt']);
      expect(await readdir(shared), "the inherited data directory is not the root's").toEqual([]);
    } finally {
      if (inherited === undefined) delete process.env.NX_WORKSPACE_DATA_DIRECTORY;
      else process.env.NX_WORKSPACE_DATA_DIRECTORY = inherited;
      await rm(root, { recursive: true, force: true });
      await rm(shared, { recursive: true, force: true });
    }
  });
});
