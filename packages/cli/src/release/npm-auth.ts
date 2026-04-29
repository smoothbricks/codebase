import type { ReleasePackageInfo } from './core.js';

export interface NpmPublishAuthFailureOptions {
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
      `${packageVersion}: npm publish authentication failed. Run smoo release trust-publisher; see the warning banner above for details.`,
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
    `::error title=npm publish authentication failed::${packageVersion} could not be published. This usually means npm trusted publishing is not configured for this package/workflow/repo.`,
    '',
    `🚨 npm publish authentication failed for ${packageVersion}`,
    '',
  ];
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
    'For first-ever package publishes, run locally: smoo release trust-publisher --bootstrap.',
  );
  return lines.join('\n');
}

export function npmPublishAuthFailureMarkdown(
  pkg: Pick<ReleasePackageInfo, 'name' | 'version'>,
  options: NpmPublishAuthFailureOptions,
): string {
  const packageVersion = `${pkg.name}@${pkg.version}`;
  const lines = ['## 🚨 npm Publish Authentication Failed', '', `Package: \`${packageVersion}\``, ''];
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
    'For first-ever package publishes, run `smoo release trust-publisher --bootstrap` locally.',
  );
  return lines.join('\n');
}

function trustedPublisherRepository(options: Pick<NpmPublishAuthFailureOptions, 'repository'>): string {
  return options.repository ?? 'the current GitHub repository';
}
