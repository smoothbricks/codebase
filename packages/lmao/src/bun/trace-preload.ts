/**
 * Bun test tracing preload.
 *
 * Registers LMAO SQLite-backed test tracing after the Typia preload has already
 * been registered in bunfig. Wiring-only: consumer resolution lives in
 * `../lib/testing/consumer-package.js`.
 */

import { autoSetupBunTestTracing } from '../lib/testing/bun-harness.js';
import { resolveConsumerTarget } from '../lib/testing/consumer-package.js';

const target = resolveConsumerTarget(process.cwd());
if (target.kind === 'package') {
  await autoSetupBunTestTracing({ packageRoot: target.packageRoot });
} else if (target.kind === 'workspace-root') {
  await autoSetupBunTestTracing({ workspaceRoot: target.workspaceRoot, packageRoots: target.packageRoots });
} else {
  // WHY loud: this preload is only ever loaded on purpose (bunfig `preload`),
  // so failing to install tracing must never be a silent no-op — that is how
  // stale .trace-results.db databases go unnoticed for months.
  console.error(
    `[lmao] trace-preload: no package.json found above ${process.cwd()} ` +
      `(searched: ${target.searched.join(', ')}). Test tracing is DISABLED for this run — ` +
      'run bun test from inside a package.',
  );
}
