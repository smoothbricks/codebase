/**
 * Bun test tracing preload.
 *
 * Registers LMAO SQLite-backed test tracing after the Typia preload has already
 * been registered in bunfig. Wiring-only: workspace resolution lives in
 * `../lib/testing/workspace-packages-dir.js`.
 */

import { autoSetupBunTestTracing } from '../lib/testing/bun-harness.js';
import { findWorkspacePackagesDir } from '../lib/testing/workspace-packages-dir.js';

const result = findWorkspacePackagesDir(process.cwd());
if (result.ok) {
  await autoSetupBunTestTracing({ packagesDir: result.packagesDir });
} else {
  // WHY loud: this preload is only ever loaded on purpose (bunfig `preload`),
  // so failing to install tracing must never be a silent no-op — that is how
  // stale .trace-results.db databases go unnoticed for months.
  const searched = result.searched.length > 0 ? result.searched.join(', ') : `no bun.lock ancestor of ${process.cwd()}`;
  console.error(
    `[lmao] trace-preload: no workspace packages directory found (searched: ${searched}). ` +
      'Test tracing is DISABLED for this run. Set LMAO_PACKAGES_DIR to your workspace packages directory to fix.',
  );
}
