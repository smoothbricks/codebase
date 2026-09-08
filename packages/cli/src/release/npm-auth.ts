import type { ReleasePackageInfo } from './core.js';

export type GithubActionsRunnerEnv = Readonly<Record<string, string | undefined>>;

export interface NpmPublishAuthFailureOptions {
  tokenPresent: boolean;
  repository?: string;
  /** Actual `npm publish` argv. Preflight keys off `--provenance`. */
  npmArgs?: readonly string[];
  env?: GithubActionsRunnerEnv;
}

export interface NpmPublishDiagnosticShell {
  publish(): Promise<void>;
  versionExists(): Promise<boolean>;
  log(message: string): void;
  error(message: string): void;
  appendSummary(markdown: string): Promise<void>;
}

/**
 * npmjs accepts `--provenance` only from GitHub-hosted runners. A GitHub
 * Actions self-hosted job that still passes `--provenance` is runner policy,
 * not a trusted-publishing credential miss. Detect it from the env npm itself
 * uses, before any registry write.
 */
export function selfHostedGithubProvenanceRefusal(npmArgs: readonly string[], env: GithubActionsRunnerEnv): boolean {
  return npmArgs.includes('--provenance') && env.GITHUB_ACTIONS === 'true' && env.RUNNER_ENVIRONMENT === 'self-hosted';
}

export async function publishWithAuthDiagnostics(
  pkg: Pick<ReleasePackageInfo, 'name' | 'version'>,
  shell: NpmPublishDiagnosticShell,
  options: NpmPublishAuthFailureOptions,
): Promise<void> {
  const packageVersion = `${pkg.name}@${pkg.version}`;
  if (selfHostedGithubProvenanceRefusal(options.npmArgs ?? [], options.env ?? {})) {
    shell.error(npmProvenanceRunnerRefusalMessage(pkg));
    await shell.appendSummary(npmProvenanceRunnerRefusalMarkdown(pkg));
    throw new Error(
      `${packageVersion}: npm provenance is refused on self-hosted GitHub Actions runners. Publish from a github-hosted runner; do not retry without --provenance.`,
    );
  }

  try {
    await shell.publish();
  } catch (error) {
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

export function npmProvenanceRunnerRefusalMessage(pkg: Pick<ReleasePackageInfo, 'name' | 'version'>): string {
  const packageVersion = `${pkg.name}@${pkg.version}`;
  return [
    `::error title=npm provenance requires a GitHub-hosted runner::${packageVersion} cannot be published with provenance from RUNNER_ENVIRONMENT=self-hosted. npmjs accepts provenance only from github-hosted runners.`,
    '',
    `npm provenance refused for ${packageVersion}`,
    '',
    'This GitHub Actions job has RUNNER_ENVIRONMENT=self-hosted. npmjs accepts --provenance only from github-hosted runners.',
    '',
    'This is GitHub Actions runner policy, not an npm trusted-publishing credential failure. Do not run smoo release trust-publisher, and do not retry without --provenance.',
    '',
    'Fix:',
    '1. Run the public npm publish job on a GitHub-hosted runner (RUNNER_ENVIRONMENT=github-hosted).',
    '2. Keep --provenance.',
  ].join('\n');
}

export function npmProvenanceRunnerRefusalMarkdown(pkg: Pick<ReleasePackageInfo, 'name' | 'version'>): string {
  const packageVersion = `${pkg.name}@${pkg.version}`;
  return [
    '## npm provenance requires a GitHub-hosted runner',
    '',
    `Package: \`${packageVersion}\``,
    '',
    'This GitHub Actions job has `RUNNER_ENVIRONMENT=self-hosted`. npmjs accepts `--provenance` only from github-hosted runners.',
    '',
    'This is GitHub Actions runner policy, not an npm trusted-publishing credential failure. Do not run `smoo release trust-publisher`, and do not retry without `--provenance`.',
    '',
    'Fix:',
    '',
    '1. Run the public npm publish job on a GitHub-hosted runner (`RUNNER_ENVIRONMENT=github-hosted`).',
    '2. Keep `--provenance`.',
  ].join('\n');
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
