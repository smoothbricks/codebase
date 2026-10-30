import { describe, expect, it } from 'bun:test';
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { runWranglerDeploy } from './executor.js';

describe('the wrangler deploy executor', () => {
  it("runs the stage's deploy in the project under the artifact hash, whichever hash the task ran under", async () => {
    const root = await mkdtemp(join(tmpdir(), 'wrangler-deploy-executor-'));
    try {
      const bin = join(root, 'node_modules', '.bin');
      await mkdir(bin, { recursive: true });
      await mkdir(join(root, 'packages', 'site'), { recursive: true });
      await writeFile(
        join(bin, 'smoo'),
        [
          '#!/bin/sh',
          'printf \'%s %s %s\\n\' "$PWD" "$NX_TASK_HASH" "$*" >> "$(dirname "$0")/calls"',
          'exit "$FAKE_EXIT"',
          '',
        ].join('\n'),
      );
      await chmod(join(bin, 'smoo'), 0o755);
      const context = {
        root,
        projectName: 'site',
        projectsConfigurations: { projects: { site: { root: 'packages/site' } } },
      };
      const env = (hash: string, exit: string) => ({ ...process.env, NX_TASK_HASH: hash, FAKE_EXIT: exit });

      expect(await runWranglerDeploy({ stage: 'pr7' }, context, env('16577780061662788004', '0'))).toEqual({
        success: true,
      });
      // The hasher could not confirm the live version: the task ran under its artifact hash and a nonce.
      expect(
        await runWranglerDeploy({ stage: 'staging' }, context, env('16577780061662788004-0123456789abcdef', '0')),
      ).toEqual({ success: true });
      expect(await runWranglerDeploy({ stage: 'staging' }, context, env('16577780061662788004', '3'))).toEqual({
        success: false,
      });
      const site = join(root, 'packages', 'site');
      expect(await readFile(join(bin, 'calls'), 'utf8')).toBe(
        [
          `${site} 16577780061662788004 wrangler deploy-stage --stage pr7`,
          `${site} 16577780061662788004 wrangler deploy-stage --stage staging`,
          `${site} 16577780061662788004 wrangler deploy-stage --stage staging`,
          '',
        ].join('\n'),
      );
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
