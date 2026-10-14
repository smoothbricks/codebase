import { expect, test } from 'bun:test';
import { createHash } from 'node:crypto';
import { DependencyType } from 'nx/src/config/project-graph';
import { getBunTextLockfileDependencies, getBunTextLockfileNodes } from 'nx/src/plugins/js/lock-file/bun-parser';
import type { CreateDependenciesContext } from 'nx/src/project-graph/plugins';

// A local file package is not an Nx workspace merely because Bun copied it.
// Keep the real parser and native hasher: these assertions fail on stock Nx 23.2.1.
test('Bun file packages retain their external node and consumer edge without externalizing actual workspaces', () => {
  const lock = JSON.stringify({
    lockfileVersion: 3,
    workspaces: {
      '': {
        name: 'file-package-host',
        dependencies: {
          consumer: '1.0.0',
          '@fixture/sdk': 'file:./.managed/sdk',
          '@fixture/worker': 'file:./packages/worker',
        },
      },
      'packages/worker': { name: '@fixture/worker', version: '1.0.0' },
    },
    packages: {
      consumer: ['consumer@1.0.0', '', { dependencies: { '@fixture/sdk': 'file:.managed/sdk' } }, 'sha512-consumer'],
      '@fixture/sdk': ['@fixture/sdk@file:.managed/sdk', {}],
      '@fixture/worker': ['@fixture/worker@file:./packages/worker', {}],
    },
  });
  const hash = createHash('sha256').update(lock).digest('hex');
  const nodes = getBunTextLockfileNodes(lock, hash);
  expect(nodes['npm:@fixture/sdk@file:.managed/sdk']?.data.packageName).toBe('@fixture/sdk');
  expect(nodes['npm:@fixture/sdk@file:.managed/sdk']?.data.version).toBe('file:.managed/sdk');
  expect(Object.values(nodes).some((node) => node.data.packageName === '@fixture/worker')).toBe(false);
  const context: CreateDependenciesContext = {
    externalNodes: nodes,
    projects: { worker: { root: 'packages/worker', name: 'worker' } },
    nxJsonConfiguration: {},
    fileMap: { projectFileMap: {}, nonProjectFiles: [] },
    filesToProcess: { projectFileMap: {}, nonProjectFiles: [] },
    workspaceRoot: process.cwd(),
  };
  expect(getBunTextLockfileDependencies(lock, hash, context)).toContainEqual({
    source: 'npm:consumer@1.0.0',
    target: 'npm:@fixture/sdk@file:.managed/sdk',
    type: DependencyType.static,
  });
});
