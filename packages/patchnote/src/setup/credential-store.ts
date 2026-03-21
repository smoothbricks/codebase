/**
 * GitHub CLI wrappers for storing credentials as organization secrets/variables.
 * All functions follow the CommandExecutor dependency injection pattern.
 */

import { execa } from 'execa';
import type { CommandExecutor, GitHubAppCredentials } from '../types.js';

/**
 * Detect the GitHub organization from the current repository.
 * Uses `gh repo view` to get the repo owner.
 */
export async function detectOrg(executor: CommandExecutor = execa as unknown as CommandExecutor): Promise<string> {
  const { stdout } = await executor('gh', ['repo', 'view', '--json', 'owner', '--jq', '.owner.login']);
  return stdout.trim();
}

/**
 * Check if the GitHub CLI is authenticated.
 * Returns true if `gh auth status` succeeds, false otherwise.
 */
export async function checkAuthScopes(
  executor: CommandExecutor = execa as unknown as CommandExecutor,
): Promise<boolean> {
  try {
    await executor('gh', ['auth', 'status']);
    return true;
  } catch {
    return false;
  }
}

/**
 * Exchange a temporary manifest code for GitHub App credentials.
 * Calls `gh api POST /app-manifests/{code}/conversions` and parses the response.
 */
export async function exchangeCode(
  code: string,
  executor: CommandExecutor = execa as unknown as CommandExecutor,
): Promise<GitHubAppCredentials> {
  const { stdout } = await executor('gh', ['api', 'POST', `/app-manifests/${code}/conversions`]);
  const data = JSON.parse(stdout);
  return {
    id: data.id,
    pem: data.pem,
    webhookSecret: data.webhook_secret,
    slug: data.slug,
  };
}

/**
 * Store GitHub App credentials as organization-level secrets and variables.
 *
 * - App ID is stored as a variable (PATCHNOTE_APP_ID) via `gh variable set`
 * - Private key is stored as a secret (PATCHNOTE_APP_PRIVATE_KEY) via `gh secret set` with stdin pipe
 */
export async function storeCredentials(
  options: { org: string; appId: number; pem: string },
  executor: CommandExecutor = execa as unknown as CommandExecutor,
): Promise<void> {
  // Set App ID as org variable
  await executor('gh', [
    'variable',
    'set',
    'PATCHNOTE_APP_ID',
    '--org',
    options.org,
    '--visibility',
    'all',
    '--body',
    options.appId.toString(),
  ]);

  // Set PEM as org secret via stdin (handles multiline correctly)
  await executor('gh', ['secret', 'set', 'PATCHNOTE_APP_PRIVATE_KEY', '--org', options.org, '--visibility', 'all'], {
    input: options.pem,
  });
}

/**
 * Delete a GitHub App by slug. Best-effort rollback helper.
 * Silently catches errors since this is used during cleanup.
 */
export async function deleteApp(
  slug: string,
  executor: CommandExecutor = execa as unknown as CommandExecutor,
): Promise<void> {
  try {
    await executor('gh', ['api', '-X', 'DELETE', `/apps/${slug}`]);
  } catch {
    // Best-effort: silently ignore errors during rollback
  }
}
