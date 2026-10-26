import { describe, expect, it } from 'bun:test';
import { readFile } from 'node:fs/promises';
import type { TargetConfiguration } from 'nx/src/devkit-exports.js';
import typia from 'typia';

/** The minimal shape read from the real cowshed manifest: `nx.targets['host-controller-test']` dependencies. */
interface CowshedManifest {
  nx: {
    targets: {
      'host-controller-test': Pick<TargetConfiguration, 'dependsOn'>;
    };
  };
}

const parseCowshedManifest = typia.json.createAssertParse<CowshedManifest>();

/** The root edge that compiles the archive the Rust worker proofs run from. */
const ARCHIVE_EDGE = { target: 'cargo-test-archive', projects: ['@smoothbricks/codebase'] };

describe('cowshed host-controller-test wiring', () => {
  it('depends on the host CLI, addon, JavaScript and root archive it actually reads', async () => {
    const manifest = parseCowshedManifest(
      await readFile(new URL('../../cowshed/package.json', import.meta.url), 'utf8'),
    );
    const { dependsOn } = manifest.nx.targets['host-controller-test'];
    // The worker proof loads the compiled package without NAPI_DEBUG_ADDON, and starts the checkout's host CLI.
    for (const producer of ['cli-native', 'napi-native', 'tsc-js']) expect(dependsOn).toContain(producer);
    expect(dependsOn).not.toContain('build');
    expect(dependsOn).toContainEqual(ARCHIVE_EDGE);
  });
});
