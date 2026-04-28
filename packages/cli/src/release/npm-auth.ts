import type { ReleasePackageInfo } from './core.js';

export interface NpmPublishAuthFailureOptions {
  useBootstrapToken: boolean;
  tokenPresent: boolean;
  repository?: string;
}

export interface NpmPublishDiagnosticShell {
  publish(): Promise<void>;
  versionExists(): Promise<boolean>;
  log(message: string): void;
  error(message: string): void;
  appendSummary(markdown: string): Promise<void>;
}

export async function publishWithAuthDiagnostics(
  pkg: Pick<ReleasePackageInfo, 'name' | 'version'>,
  shell: NpmPublishDiagnosticShell,
  options: NpmPublishAuthFailureOptions,
): Promise<void> {
  try {
    await shell.publish();
  } catch (error) {
    const packageVersion = `${pkg.name}@${pkg.version}`;
    if (await shell.versionExists()) {
      shell.log(`${packageVersion}: publish result already visible on npm; continuing.`);
      return;
    }
    shell.error(npmPublishAuthFailureMessage(pkg, options));
    await shell.appendSummary(npmPublishAuthFailureMarkdown(pkg, options));
    throw new Error(
      `${packageVersion}: npm publish authentication failed. Run smoo release trust-publisher after the package exists on npm; see the warning banner above for details.`,
      { cause: error },
    );
  }
}

export function npmPublishAuthFailureMessage(
  pkg: Pick<ReleasePackageInfo, 'name' | 'version'>,
  options: NpmPublishAuthFailureOptions,
): string {
  const packageVersion = `${pkg.name}@${pkg.version}`;
  const lines = [
    `::error title=npm publish authentication failed::${packageVersion} could not be published. This usually means npm trusted publishing is not configured for this package/workflow/repo, or the bootstrap NPM_TOKEN is missing/invalid.`,
    '',
    `🚨 npm publish authentication failed for ${packageVersion}`,
    '',
  ];
  if (options.useBootstrapToken) {
    lines.push(
      'smoo expected a temporary npm automation token because this package does not exist on npm yet.',
      options.tokenPresent
        ? 'NODE_AUTH_TOKEN/NPM_TOKEN is set, but npm still rejected the bootstrap publish. Check that the token is valid, has publish rights for this scope, and is available to this workflow.'
        : 'NODE_AUTH_TOKEN/NPM_TOKEN is not set. Add a temporary NPM_TOKEN repository secret and rerun the Publish workflow.',
      '',
      'After the first successful publish, run:',
      '  smoo release trust-publisher',
      '',
      'Then remove the temporary bootstrap token path for future releases.',
    );
    return lines.join('\n');
  }

  lines.push(
    'smoo expected npm trusted publishing/OIDC because this package already exists on npm.',
    options.tokenPresent
      ? 'NODE_AUTH_TOKEN/NPM_TOKEN is set but unused: smoo intentionally clears token auth for existing packages; npm must authenticate through trusted publishing instead.'
      : 'NODE_AUTH_TOKEN/NPM_TOKEN is not set, which is expected for trusted publishing; npm did not authenticate the workflow as a trusted publisher.',
    '',
    'Fix:',
    '1. Run locally: smoo release trust-publisher',
    '2. Ensure the npm trusted publisher uses:',
    `   repository: ${trustedPublisherRepository(options)}`,
    '   workflow: publish.yml',
    '3. Rerun the Publish workflow.',
    '',
    'For first-ever package publishes, add a temporary NPM_TOKEN repository secret, publish once, then run smoo release trust-publisher.',
  );
  return lines.join('\n');
}

export function npmPublishAuthFailureMarkdown(
  pkg: Pick<ReleasePackageInfo, 'name' | 'version'>,
  options: NpmPublishAuthFailureOptions,
): string {
  const packageVersion = `${pkg.name}@${pkg.version}`;
  const lines = ['## 🚨 npm Publish Authentication Failed', '', `Package: \`${packageVersion}\``, ''];
  if (options.useBootstrapToken) {
    lines.push(
      'smoo expected a temporary npm automation token because this package does not exist on npm yet.',
      '',
      options.tokenPresent
        ? '`NODE_AUTH_TOKEN`/`NPM_TOKEN` is set, but npm still rejected the bootstrap publish. Check that the token is valid, has publish rights for this scope, and is available to this workflow.'
        : '`NODE_AUTH_TOKEN`/`NPM_TOKEN` is not set. Add a temporary `NPM_TOKEN` repository secret and rerun the Publish workflow.',
      '',
      'After the first successful publish, run `smoo release trust-publisher`, then remove the temporary bootstrap token path for future releases.',
    );
    return lines.join('\n');
  }

  lines.push(
    'smoo expected npm trusted publishing/OIDC because this package already exists on npm.',
    '',
    options.tokenPresent
      ? '`NODE_AUTH_TOKEN`/`NPM_TOKEN` is set but unused: smoo intentionally clears token auth for existing packages; npm must authenticate through trusted publishing instead.'
      : '`NODE_AUTH_TOKEN`/`NPM_TOKEN` is not set, which is expected for trusted publishing; npm did not authenticate the workflow as a trusted publisher.',
    '',
    'Fix:',
    '',
    '1. Run locally: `smoo release trust-publisher`',
    `2. Ensure the npm trusted publisher uses repository \`${trustedPublisherRepository(options)}\` and workflow \`publish.yml\``,
    '3. Rerun the Publish workflow.',
    '',
    'For first-ever package publishes, add a temporary `NPM_TOKEN` repository secret, publish once, then run `smoo release trust-publisher`.',
  );
  return lines.join('\n');
}

function trustedPublisherRepository(options: Pick<NpmPublishAuthFailureOptions, 'repository'>): string {
  return options.repository ?? 'the current GitHub repository';
}
