import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { resolvePrivateNpmWorkflowConfig } from '../private-npm.js';

const SCOPE = '@priv.test';
const READ_ENV = 'PRIV_NPM_READ_TOKEN';
const PUBLISH_ENV = 'PRIV_NPM_PUBLISH_TOKEN';
const REGISTRY = 'https://forge.example.test/api/packages/priv-owner/npm/';

describe('private npm workflow token selection', () => {
  it('injects only the read token when the repo consumes the private scope and does not publish it', async () => {
    await withRepo(
      {
        rootDeps: { [`${SCOPE}/sdk`]: '1.2.3' },
        npmrcAuthEnv: READ_ENV,
        declared: { scope: SCOPE, readTokenEnv: READ_ENV, publishTokenEnv: PUBLISH_ENV },
      },
      (root) => {
        expect(resolvePrivateNpmWorkflowConfig(root)).toEqual({
          scope: SCOPE,
          readTokenEnv: READ_ENV,
        });
      },
    );
  });

  it('injects only the publish token when the repo publishes the private scope from the workspace', async () => {
    await withRepo(
      {
        privatePackage: `${SCOPE}/sdk`,
        npmrcAuthEnv: PUBLISH_ENV,
        declared: { scope: SCOPE, publishTokenEnv: PUBLISH_ENV },
      },
      (root) => {
        expect(resolvePrivateNpmWorkflowConfig(root)).toEqual({
          scope: SCOPE,
          publishTokenEnv: PUBLISH_ENV,
        });
      },
    );
  });

  it('takes the auth env name from .npmrc when the root declaration omits token names', async () => {
    await withRepo(
      {
        rootDeps: { [`${SCOPE}/sdk`]: '^2.0.0' },
        npmrcAuthEnv: READ_ENV,
        declared: { scope: SCOPE },
      },
      (root) => {
        expect(resolvePrivateNpmWorkflowConfig(root)).toEqual({
          scope: SCOPE,
          readTokenEnv: READ_ENV,
        });
      },
    );
  });

  it('ignores workspace and link specs so a producer install does not look like a registry consume', async () => {
    await withRepo(
      {
        privatePackage: `${SCOPE}/sdk`,
        rootDeps: { [`${SCOPE}/sdk`]: 'workspace:*' },
        extraDeps: { [`${SCOPE}/other`]: 'link:@priv.test/other' },
        npmrcAuthEnv: PUBLISH_ENV,
        declared: { scope: SCOPE, readTokenEnv: READ_ENV, publishTokenEnv: PUBLISH_ENV },
      },
      (root) => {
        expect(resolvePrivateNpmWorkflowConfig(root)).toEqual({
          scope: SCOPE,
          publishTokenEnv: PUBLISH_ENV,
        });
      },
    );
  });

  it('returns nothing when the root did not opt in', async () => {
    await withRepo({ declared: null, npmrcAuthEnv: READ_ENV }, (root) => {
      expect(resolvePrivateNpmWorkflowConfig(root)).toBeUndefined();
    });
  });
});

async function withRepo(
  options: {
    declared: { scope: string; readTokenEnv?: string; publishTokenEnv?: string } | null;
    rootDeps?: Record<string, string>;
    extraDeps?: Record<string, string>;
    privatePackage?: string;
    npmrcAuthEnv?: string;
  },
  fn: (root: string) => void,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-private-npm-workflow-'));
  try {
    await writeFile(
      join(root, 'package.json'),
      `${JSON.stringify(
        {
          name: '@example/source',
          version: '0.0.0',
          private: true,
          workspaces: ['packages/*'],
          ...(options.rootDeps ? { dependencies: options.rootDeps } : {}),
          ...(options.extraDeps ? { devDependencies: options.extraDeps } : {}),
          ...(options.declared
            ? {
                smoo: { privateNpm: options.declared },
              }
            : {}),
          repository: { type: 'git', url: 'https://git.example.test/example/source.git' },
        },
        null,
        2,
      )}\n`,
    );
    await writeFile(
      join(root, '.npmrc'),
      `${SCOPE}:registry=${REGISTRY}\n//forge.example.test/api/packages/priv-owner/npm/:_authToken=\${${options.npmrcAuthEnv ?? READ_ENV}}\n`,
    );
    if (options.privatePackage !== undefined) {
      const dir = join(root, 'packages', 'sdk');
      await mkdir(dir, { recursive: true });
      await writeFile(
        join(dir, 'package.json'),
        `${JSON.stringify(
          {
            name: options.privatePackage,
            version: '1.0.0',
            nx: { tags: ['npm:private'] },
            repository: { type: 'git', url: 'https://git.example.test/example/source.git' },
          },
          null,
          2,
        )}\n`,
      );
    }
    fn(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}
