/**
 * Example: Script mode config file
 *
 * Instead of a declarative config, you can export a function
 * that has full control over the update process.
 *
 * Usage:
 *   1. Copy this to tooling/dep-updater.ts
 *   2. Run: dep-updater update-deps
 *   3. Your custom logic will execute
 */

import { loadConfig, updateBunDependencies } from 'dep-updater';

export default async function () {
  console.log('🚀 Running custom update script...\n');

  // Load config for settings
  const config = await loadConfig();

  // Run bun updater to detect available updates
  const result = await updateBunDependencies(config.repoRoot || process.cwd());

  if (result.updates.length === 0) {
    console.log('✓ No updates available');
    return;
  }

  console.log(`Found ${result.updates.length} updates:\n`);

  // Custom logic: Filter updates
  const filtered = result.updates.filter((update) => {
    // Skip React 19.x
    if (update.name === 'react' && update.toVersion.startsWith('19')) {
      console.log(`⏭  Skipping ${update.name} ${update.toVersion} (React 19 not ready)`);
      return false;
    }

    // Skip major version bumps for specific packages
    if (update.updateType === 'major' && ['typescript', 'eslint'].includes(update.name)) {
      console.log(`⏭  Skipping major update for ${update.name} (needs manual review)`);
      return false;
    }

    return true;
  });

  console.log(`\n${filtered.length} updates after filtering:\n`);
  for (const update of filtered) {
    console.log(`  • ${update.name}: ${update.fromVersion} → ${update.toVersion} (${update.updateType})`);
  }

  // Your custom logic here!
  // You could:
  // - Apply updates selectively
  // - Run custom validation
  // - Generate custom commit messages
  // - Create PRs with custom logic
  // - Integrate with your CI/CD

  console.log('\n✓ Script complete!');
  console.log('Note: This is an example - no actual changes were made.');
  console.log('Implement your own logic to apply updates.');
}
