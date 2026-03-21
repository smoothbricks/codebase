/**
 * Setup command for automating GitHub App creation via the manifest flow.
 *
 * Flow: detect org -> check auth -> prompt for app name -> confirm -> build manifest ->
 * start local server -> open browser -> wait for callback -> exchange code -> store credentials ->
 * show installation URL
 */

import * as p from '@clack/prompts';
import { execa } from 'execa';
import type { PatchnoteConfig } from '../config.js';
import { checkAuthScopes, deleteApp, detectOrg, exchangeCode, storeCredentials } from '../setup/credential-store.js';
import { startCallbackServer } from '../setup/local-server.js';
import { buildManifest, generateManifestPage } from '../setup/manifest.js';
import type { CommandExecutor, GitHubAppCredentials, SetupOptions } from '../types.js';

/**
 * Open a URL in the user's default browser using platform-specific commands.
 * Does NOT add the `open` npm package as a dependency.
 */
async function openBrowser(
  url: string,
  executor: CommandExecutor = execa as unknown as CommandExecutor,
): Promise<void> {
  const platform = process.platform;
  if (platform === 'darwin') {
    await executor('open', [url]);
  } else if (platform === 'win32') {
    await executor('cmd', ['/c', 'start', url]);
  } else {
    await executor('xdg-open', [url]);
  }
}

/**
 * Setup patchnote GitHub App authentication via the manifest flow.
 */
export async function setup(
  _config: PatchnoteConfig,
  options: SetupOptions,
  executor: CommandExecutor = execa as unknown as CommandExecutor,
): Promise<void> {
  p.intro('Setting up GitHub App for patchnote');

  // Step 1: Detect/prompt for org
  let org = options.org;
  if (!org) {
    const s = p.spinner();
    s.start('Detecting organization from repository');
    try {
      org = await detectOrg(executor);
      s.stop(`Detected organization: ${org}`);
    } catch {
      s.stop('Could not detect organization');
    }
  }

  if (!org) {
    const orgPrompt = await p.text({
      message: 'GitHub organization name',
      placeholder: 'my-org',
      validate: (value) => {
        if (!value.trim()) return 'Organization name is required';
        return undefined;
      },
    });
    if (p.isCancel(orgPrompt)) {
      p.cancel('Setup cancelled.');
      return;
    }
    org = orgPrompt;
  } else {
    // Show detected org and allow override
    p.note(`Organization: ${org}`, 'Detected from repository');
    const override = await p.confirm({
      message: `Use ${org} as the organization?`,
      initialValue: true,
    });
    if (p.isCancel(override)) {
      p.cancel('Setup cancelled.');
      return;
    }
    if (!override) {
      const orgPrompt = await p.text({
        message: 'GitHub organization name',
        initialValue: org,
      });
      if (p.isCancel(orgPrompt)) {
        p.cancel('Setup cancelled.');
        return;
      }
      org = orgPrompt;
    }
  }

  // Step 2: Check gh CLI prerequisites
  const authSpinner = p.spinner();
  authSpinner.start('Checking GitHub CLI authentication');
  const isAuthenticated = await checkAuthScopes(executor);
  if (!isAuthenticated) {
    authSpinner.stop('GitHub CLI is not authenticated');
    p.note(
      'Please authenticate the GitHub CLI first:\n\n' + '  gh auth login\n\n' + 'Then run this command again.',
      'Authentication Required',
    );
    return;
  }
  authSpinner.stop('GitHub CLI is authenticated');

  // Step 3: Prompt for app name
  const defaultAppName = `Patchnote - ${org}`;
  const appNamePrompt = await p.text({
    message: 'GitHub App name',
    initialValue: defaultAppName,
    validate: (value) => {
      if (!value.trim()) return 'App name is required';
      return undefined;
    },
  });
  if (p.isCancel(appNamePrompt)) {
    p.cancel('Setup cancelled.');
    return;
  }
  const appName = appNamePrompt;

  // Step 4: Show summary and confirm
  const permissionsList = ['contents: write', 'pull_requests: write', 'workflows: write', 'metadata: read'].join(
    '\n  ',
  );

  p.note(
    `App name: ${appName}\n` +
      `Organization: ${org}\n` +
      `Permissions:\n  ${permissionsList}\n\n` +
      'This will open your browser to create the GitHub App.',
    'Summary',
  );

  const confirm = await p.confirm({
    message: 'Create this GitHub App?',
    initialValue: true,
  });
  if (p.isCancel(confirm) || !confirm) {
    p.cancel('Setup cancelled.');
    return;
  }

  // Step 5: Dry-run handling
  if (options.dryRun) {
    const manifest = buildManifest({ org, appName, port: 0 });
    p.note(JSON.stringify(manifest, null, 2), 'Manifest (dry-run)');
    p.note(
      'Commands that would be executed:\n\n' +
        '  gh api POST /app-manifests/{code}/conversions\n' +
        `  gh variable set PATCHNOTE_APP_ID --org ${org} --visibility all --body {appId}\n` +
        `  gh secret set PATCHNOTE_APP_PRIVATE_KEY --org ${org} --visibility all (PEM via stdin)`,
      'Planned Actions (dry-run)',
    );
    p.outro('Dry-run complete. No changes were made.');
    return;
  }

  // Step 6: Execute manifest flow
  let credentials: GitHubAppCredentials | null = null;
  let serverClose: (() => void) | null = null;

  try {
    // Start callback server with empty page to get an OS-assigned port
    const actualServer = await startCallbackServer({ manifestPage: '' });
    serverClose = actualServer.close;

    // Build manifest and page with the actual port, then update the server
    const manifest = buildManifest({ org, appName, port: actualServer.port });
    const manifestPage = generateManifestPage(org, manifest);
    actualServer.setManifestPage(manifestPage);

    // Open browser
    const browserSpinner = p.spinner();
    browserSpinner.start('Opening browser for GitHub App creation...');
    try {
      await openBrowser(`http://localhost:${actualServer.port}/`, executor);
    } catch {
      browserSpinner.stop('Could not open browser automatically');
      p.note(`Please open this URL in your browser:\n\n  http://localhost:${actualServer.port}/`, 'Manual Step');
    }

    // Wait for callback
    const waitSpinner = p.spinner();
    waitSpinner.start('Waiting for GitHub App creation in browser... (complete the flow in your browser)');

    const code = await actualServer.waitForCode();
    serverClose = null; // Server auto-closes after callback
    waitSpinner.stop('GitHub App creation confirmed');

    // Step 7: Exchange code for credentials
    const exchangeSpinner = p.spinner();
    exchangeSpinner.start('Exchanging code for credentials...');
    credentials = await exchangeCode(code, executor);
    exchangeSpinner.stop('Credentials received');

    // Step 8: Store credentials
    const storeSpinner = p.spinner();
    storeSpinner.start('Storing credentials as organization secrets...');
    try {
      await storeCredentials({ org, appId: credentials.id, pem: credentials.pem }, executor);
      storeSpinner.stop('Credentials stored');
    } catch (storeError) {
      storeSpinner.stop('Failed to store credentials');

      // Rollback: attempt to delete the app
      p.note('Attempting to clean up the created app...', 'Rollback');
      await deleteApp(credentials.slug, executor);

      throw storeError;
    }
  } catch (error) {
    // Clean up server if still running
    if (serverClose) {
      serverClose();
    }

    if (error instanceof Error) {
      if (error.message.includes('Timeout')) {
        p.note(
          'The callback server timed out waiting for GitHub to redirect.\n\n' +
            'This usually means:\n' +
            '  - The browser flow was not completed\n' +
            '  - The redirect URL was not reached\n\n' +
            'Please try running the command again.',
          'Timeout',
        );
      } else {
        p.note(`Error: ${error.message}`, 'Setup Failed');
      }
    }
    return;
  }

  // Step 9: Show installation instructions
  p.note(
    'Install the app from this URL:\n\n' +
      `  https://github.com/apps/${credentials.slug}/installations/new\n\n` +
      'App installation cannot be automated -- you must click "Install" in the browser.\n\n' +
      'Scope recommendation:\n' +
      '  - "All repositories" for org-wide use\n' +
      '  - Or select specific repos that need dependency updates',
    'Install the GitHub App',
  );

  // Step 10: Show next steps
  p.note(
    '1. Install the app using the URL above\n' +
      '2. Run: patchnote validate-setup (to confirm everything works)\n' +
      '3. Run: patchnote generate-workflow (if not already done)\n' +
      '4. Commit and push the workflow file',
    'Next Steps',
  );

  p.outro('GitHub App setup complete!');
}
