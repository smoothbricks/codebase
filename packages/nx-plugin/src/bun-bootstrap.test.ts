import { describe, expect, it } from 'bun:test';

describe('Bun bootstrap imports', () => {
  it('imports the main plugin entry point', async () => {
    const plugin = await import('./index.js');
    expect(plugin.createNodesV2).toBeDefined();
    expect(Array.isArray(plugin.createNodesV2)).toBe(true);
  });

  it('imports bounded-test-policy', async () => {
    const policy = await import('./bounded-test-policy.js');
    expect(typeof policy.applyWorkspaceBoundedTestTargetPolicy).toBe('function');
    expect(typeof policy.checkWorkspaceBoundedTestTargetPolicy).toBe('function');
    expect(typeof policy.applyBoundedTestTargetPolicy).toBe('function');
    expect(typeof policy.checkBoundedTestTargetPolicy).toBe('function');
    expect(typeof policy.boundedTestScriptAlias).toBe('function');
    expect(typeof policy.resolveTestCommand).toBe('function');
    expect(typeof policy.BOUNDED_TEST_EXECUTOR).toBe('string');
    expect(typeof policy.BOUNDED_TEST_TIMEOUT_MS).toBe('number');
    expect(typeof policy.BOUNDED_TEST_KILL_AFTER_MS).toBe('number');
  });

  it('imports workspace-config-policy', async () => {
    const policy = await import('./workspace-config-policy.js');
    expect(typeof policy.checkWorkspaceConfigPolicy).toBe('function');
    expect(typeof policy.applyWorkspaceConfigPolicy).toBe('function');
    expect(Array.isArray(policy.BUILD_OUTPUT_DEPENDENCIES)).toBe(true);
  });

  it('imports release-config-policy', async () => {
    const policy = await import('./release-config-policy.js');
    expect(typeof policy.checkReleaseConfigPolicy).toBe('function');
    expect(typeof policy.applyReleaseConfigPolicy).toBe('function');
    expect(typeof policy.SMOO_NX_VERSION_ACTIONS).toBe('string');
    expect(typeof policy.SMOO_NX_RELEASE_TAG_PATTERN).toBe('string');
  });

  it('imports package-target-policy', async () => {
    const policy = await import('./package-target-policy.js');
    expect(typeof policy.checkPackageTargetPolicy).toBe('function');
    expect(typeof policy.applyPackageTargetPolicy).toBe('function');
    expect(typeof policy.nxRunAlias).toBe('function');
    expect(typeof policy.packageNxProjectName).toBe('function');
    expect(Array.isArray(policy.BUILD_OUTPUT_DEPENDENCIES)).toBe(true);
  });

  it('imports typecheck-test-policy', async () => {
    const policy = await import('./typecheck-test-policy.js');
    expect(typeof policy.checkTypecheckTestPolicy).toBe('function');
    expect(typeof policy.applyTypecheckTestPolicy).toBe('function');
  });
});
