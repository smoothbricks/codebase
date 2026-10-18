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
    // Packing and spawning fail before npm says anything; that error is the report.
    if (!(error instanceof NpmPublishCommandError)) {
      throw error;
    }
    const failure = classifyNpmPublishFailure(error.stderr);
    switch (failure.kind) {
      case 'auth':
        shell.error(npmPublishAuthFailureMessage(pkg, options));
        await shell.appendSummary(npmPublishAuthFailureMarkdown(pkg, options));
        throw new Error(
          `${packageVersion}: npm publish authentication failed (${failure.code}). Run smoo release trust-publisher; see the warning banner above for details.`,
          { cause: error },
        );
      case 'too-large':
        shell.error(npmPublishTooLargeMessage(pkg, failure.packageSize));
        await shell.appendSummary(npmPublishTooLargeMarkdown(pkg, failure.packageSize));
        throw new Error(
          `${packageVersion}: npm rejected the tarball as too large (E413 Payload Too Large, package size ${failure.packageSize ?? 'not reported'}; npm's ceiling is ~200 MB).`,
          { cause: error },
        );
      case 'npm':
        shell.error(npmPublishFailureMessage(pkg, failure));
        await shell.appendSummary(npmPublishFailureMarkdown(pkg, failure));
        throw new Error(
          `${packageVersion}: npm publish failed (${failure.code ?? `exit code ${error.exitCode}`}):\n${failure.detail}`,
          { cause: error },
        );
    }
  }
}

/**
 * `npm publish` exited non-zero. Carries npm's stderr -- where npm writes its
 * `npm error code ...` lines and its `npm notice package size:` line -- so the
 * failure is classified from what npm said rather than assumed.
 */
export class NpmPublishCommandError extends Error {
  constructor(
    readonly npmArgs: readonly string[],
    readonly exitCode: number,
    readonly stderr: string,
  ) {
    super(`npm ${npmArgs.join(' ')} failed with exit code ${exitCode}`);
    this.name = 'NpmPublishCommandError';
  }
}

/**
 * npm codes meaning the registry did not authenticate the publish. A failed
 * trusted-publishing OIDC exchange surfaces as ENEEDAUTH or as a 404 on the PUT
 * of a package that exists (npm/cli#9088); publish checks existence first.
 */
const NPM_AUTH_ERROR_CODES: Record<NpmAuthErrorCode, true> = { E401: true, E403: true, E404: true, ENEEDAUTH: true };

export type NpmAuthErrorCode = 'E401' | 'E403' | 'E404' | 'ENEEDAUTH';

export type NpmPublishFailure =
  | { kind: 'auth'; code: NpmAuthErrorCode }
  | { kind: 'too-large'; packageSize: string | undefined }
  | { kind: 'npm'; code: string | undefined; detail: string };

/** Classifies a failed `npm publish` from npm's own stderr. */
export function classifyNpmPublishFailure(stderr: string): NpmPublishFailure {
  const code = /^npm (?:error|ERR!) code (\S+)/m.exec(stderr)?.[1];
  if (code === 'E413') {
    return { kind: 'too-large', packageSize: /^npm notice package size:\s*(.+?)\s*$/m.exec(stderr)?.[1] };
  }
  if (code !== undefined && isNpmAuthErrorCode(code)) {
    return { kind: 'auth', code };
  }
  return { kind: 'npm', code, detail: npmErrorDetail(stderr) };
}

function isNpmAuthErrorCode(code: string): code is NpmAuthErrorCode {
  return Object.hasOwn(NPM_AUTH_ERROR_CODES, code);
}

/** npm's own error lines, verbatim; the stderr tail when npm printed none. */
function npmErrorDetail(stderr: string): string {
  const lines = stderr.split('\n').map((line) => line.trimEnd());
  const errorLines = lines.filter((line) => /^npm (?:error|ERR!)/.test(line));
  if (errorLines.length > 0) {
    return errorLines.join('\n');
  }
  const tail = lines.filter((line) => line.length > 0).slice(-12);
  return tail.length > 0 ? tail.join('\n') : 'npm printed nothing to stderr.';
}

const NPM_TARBALL_CEILING =
  "npm's registry rejects tarballs above roughly 200 MB (https://github.com/npm/documentation/issues/1691).";

export function npmPublishTooLargeMessage(
  pkg: Pick<ReleasePackageInfo, 'name' | 'version'>,
  packageSize: string | undefined,
): string {
  const packageVersion = `${pkg.name}@${pkg.version}`;
  const size = packageSize ?? 'not reported by npm';
  return [
    `::error title=npm rejected the package as too large::${packageVersion} could not be published: the registry answered E413 Payload Too Large (package size ${size}).`,
    '',
    `npm publish rejected ${packageVersion}: tarball too large (E413)`,
    '',
    `Package size: ${size}`,
    NPM_TARBALL_CEILING,
    'This is not an authentication failure; do not run smoo release trust-publisher.',
    '',
    'Fix: shrink the packed package below the ceiling (see `npm pack --dry-run` for what it ships), then rerun the Publish workflow.',
  ].join('\n');
}

export function npmPublishTooLargeMarkdown(
  pkg: Pick<ReleasePackageInfo, 'name' | 'version'>,
  packageSize: string | undefined,
): string {
  return [
    '## npm rejected the package as too large',
    '',
    `Package: \`${pkg.name}@${pkg.version}\``,
    '',
    `The registry answered \`E413 Payload Too Large\`. Package size: ${packageSize ?? 'not reported by npm'}.`,
    '',
    NPM_TARBALL_CEILING,
    '',
    'This is not an authentication failure; do not run `smoo release trust-publisher`. Shrink the packed package below the ceiling, then rerun the Publish workflow.',
  ].join('\n');
}

export function npmPublishFailureMessage(
  pkg: Pick<ReleasePackageInfo, 'name' | 'version'>,
  failure: { code: string | undefined; detail: string },
): string {
  const packageVersion = `${pkg.name}@${pkg.version}`;
  return [
    `::error title=npm publish failed::${packageVersion} could not be published: npm error code ${failure.code ?? '(none)'}.`,
    '',
    `npm publish failed for ${packageVersion}. npm said:`,
    failure.detail,
  ].join('\n');
}

export function npmPublishFailureMarkdown(
  pkg: Pick<ReleasePackageInfo, 'name' | 'version'>,
  failure: { code: string | undefined; detail: string },
): string {
  return [
    '## npm publish failed',
    '',
    `Package: \`${pkg.name}@${pkg.version}\``,
    '',
    'npm said:',
    '',
    '```text',
    failure.detail,
    '```',
  ].join('\n');
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
