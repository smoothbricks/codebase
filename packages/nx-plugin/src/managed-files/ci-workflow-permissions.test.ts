import { describe, expect, it } from 'bun:test';
import { cargoCrossTestArchiveFile } from '../cross-check-policy.js';
import { renderCiWorkflowYaml } from './ci-workflow.js';

const readPermissions = { actions: 'read', contents: 'read' };
const statusPermissions = { ...readPermissions, statuses: 'write' };

describe('CI job permissions', () => {
  for (const actionsProvider of ['github', 'forgejo'] as const) {
    for (const deploy of [false, true]) {
      it(`scopes writes to their consumers (${actionsProvider}, deploy=${deploy})`, () => {
        const triple = 'aarch64-apple-darwin';
        const workflow: unknown = Bun.YAML.parse(
          renderCiWorkflowYaml({
            actionsProvider,
            deploy,
            browserTests: false,
            e2eDeployment: deploy,
            productionOnPush: deploy,
            pushBranches: ['main'],
            crossTestArchives: [{ triple, path: cargoCrossTestArchiveFile(triple) }],
          }),
        );
        expect(workflow).toHaveProperty('permissions', readPermissions);
        expect(workflow).toHaveProperty('jobs.main.permissions', {
          actions: 'write',
          contents: 'read',
          ...(deploy ? { deployments: 'write' } : {}),
          statuses: 'write',
        });
        // This job runs the downloaded test archives without publishing commit statuses.
        expect(workflow).toHaveProperty('jobs.macos-cross-tests');
        expect(workflow).not.toHaveProperty('jobs.macos-cross-tests.permissions');
        if (deploy) {
          expect(workflow).toHaveProperty('jobs.e2e-deployment.permissions', statusPermissions);
          expect(workflow).toHaveProperty('jobs.deploy-production.permissions', statusPermissions);
        } else {
          expect(workflow).not.toHaveProperty('jobs.e2e-deployment');
          expect(workflow).not.toHaveProperty('jobs.deploy-production');
        }
      });
    }
  }
});
