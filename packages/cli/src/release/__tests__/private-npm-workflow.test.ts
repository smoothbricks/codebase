import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  privateNpmTokenEnvForMode,
  requirePrivateNpmRegistry,
  resolvePrivateNpmWorkflowConfig,
} from '../private-npm.js';

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
        declared: { scope: SCOPE },
      },
      (root) => {
        expect(resolvePrivateNpmWorkflowConfig(root)).toEqual({
          scope: SCOPE,
          publishTokenEnv: PUBLISH_ENV,
        });
        expect(requirePrivateNpmRegistry(root).publishTokenEnv).toBe(PUBLISH_ENV);
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
        expect(requirePrivateNpmRegistry(root).readTokenEnv).toBe(READ_ENV);
      },
    );
  });

  it('uses the npmrc credential for both operations when a scope-only repo consumes and publishes', async () => {
    await withRepo(
      {
        privatePackage: `${SCOPE}/sdk`,
        rootDeps: { [`${SCOPE}/other`]: '1.0.0' },
        npmrcAuthEnv: READ_ENV,
        declared: { scope: SCOPE },
      },
      (root) => {
        expect(resolvePrivateNpmWorkflowConfig(root)).toEqual({
          scope: SCOPE,
          readTokenEnv: READ_ENV,
          publishTokenEnv: READ_ENV,
        });
        const registry = requirePrivateNpmRegistry(root);
        expect(registry.readTokenEnv).toBe(READ_ENV);
        expect(registry.publishTokenEnv).toBe(READ_ENV);
      },
    );
  });

  it('ignores workspace and link specs so a producer install does not look like a registry consume', async () => {
    // Without a declared read token, the .npmrc credential is a publish
    // credential and must never be promoted into the read role by inference.
    await withRepo(
      {
        privatePackage: `${SCOPE}/sdk`,
        rootDeps: { [`${SCOPE}/sdk`]: 'workspace:*' },
        extraDeps: { [`${SCOPE}/other`]: 'link:@priv.test/other' },
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

  it('renders a declared read token for a producer, whose later releases read the previous tag state', async () => {
    await withRepo(
      {
        privatePackage: `${SCOPE}/sdk`,
        rootDeps: { [`${SCOPE}/sdk`]: 'workspace:*' },
        npmrcAuthEnv: PUBLISH_ENV,
        declared: { scope: SCOPE, readTokenEnv: READ_ENV, publishTokenEnv: PUBLISH_ENV },
      },
      (root) => {
        expect(resolvePrivateNpmWorkflowConfig(root)).toEqual({
          scope: SCOPE,
          readTokenEnv: READ_ENV,
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

  it('resolves the npmrc read credential for a declared scope the workspace has not wired up yet', async () => {
    await withRepo({ declared: { scope: SCOPE }, npmrcAuthEnv: READ_ENV }, (root) => {
      // Nothing consumes or publishes the scope yet, so managed CI hands out
      // no secrets...
      expect(resolvePrivateNpmWorkflowConfig(root)).toBeUndefined();
      // ...but an explicit npm operation still has a declared destination and
      // the credential its own .npmrc names. Whether CI should expose a token
      // is not the same question as which token authenticates this call.
      const registry = requirePrivateNpmRegistry(root);
      expect(registry.readTokenEnv).toBe(READ_ENV);
      expect(registry.publishTokenEnv).toBeUndefined();
    });
  });

  it('never infers a publish credential for a workspace that does not publish the scope', async () => {
    await withRepo(
      { declared: { scope: SCOPE }, rootDeps: { [`${SCOPE}/sdk`]: '1.0.0' }, npmrcAuthEnv: READ_ENV },
      (root) => {
        const registry = requirePrivateNpmRegistry(root);

        expect(registry.readTokenEnv).toBe(READ_ENV);
        expect(registry.publishTokenEnv).toBeUndefined();
        expect(() => privateNpmTokenEnvForMode(registry, 'publish')).toThrow(/publish token/);
      },
    );
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
