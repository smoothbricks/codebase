/**
 * Provider-neutral local secret resolution for smoo-managed repositories.
 *
 * Reads the root package.json `smoo.secrets` map — the bootstrap twin of
 * `PackageSecretCommand` and `PackageSmooConfig.secrets` in
 * packages/cli/src/lib/json.ts. This file is a managed raw script: it runs
 * from `tooling/direnv/setup-environment.ts` BEFORE `bun install`, when no
 * workspace package and no Typia transform exist yet, so the map is
 * hand-validated here against the exact shape json.ts declares. Keep the two
 * aligned; smoo's Typia validation fails the manifest at generation time for
 * anything this file would reject at runtime.
 *
 * Routing per declared variable, in first-match order:
 *
 *  1. An existing nonempty environment value wins; no command runs.
 *  2. CI (`CI` set): variables must be injected from the CI secret store;
 *     provider commands never run there. Missing variables refuse with
 *     injected-secret guidance instead of a confusing auth failure later.
 *  3. Inside a cowshed workspace (`COWSHED_WORKSPACE_TOKEN` set), a variable
 *     referenced from `.npmrc` is registry routing intent: it is excluded
 *     from local provider resolution and refuses with names-only
 *     gateway-enrollment guidance. Generic variables still use env/provider.
 *  4. Otherwise the command argv executes directly, without a shell; stdout
 *     with one terminal newline trimmed becomes the value.
 *
 * Failures aggregate so a single direnv reload surfaces every problem.
 * Error text names variables and exit codes only: secret values, provider
 * stdout/stderr, and command arguments are never echoed.
 */
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

export interface SecretSpec {
  /** Executed directly, without a shell; stdout supplies the secret value. */
  readonly command: readonly [string, ...string[]];
}

export type SecretCommandRunner = (argv: readonly string[]) => Promise<string>;

export interface SecretResolutionRequest {
  readonly secrets: Readonly<Record<string, SecretSpec>>;
  /** Env names referenced from `.npmrc`; registry routing intent inside a cowshed workspace. */
  readonly registryIntentEnvs: ReadonlySet<string>;
  /** Environment consulted for existing values and mode flags; read, never written. */
  readonly env: Readonly<Record<string, string | undefined>>;
  /** Overrides the default direct-argv runner (test seam). */
  readonly runCommand?: SecretCommandRunner;
}

export interface SecretResolutionOptions {
  /** Repository root holding package.json and .npmrc. */
  readonly root: string;
  /** Environment to consult; defaults to process.env. Read, never written. */
  readonly env?: Record<string, string | undefined>;
  /** Overrides the default direct-argv runner (test seam). */
  readonly runCommand?: SecretCommandRunner;
}

/**
 * The env name pattern every shell accepts, so the same regex classifies
 * declared names and `.npmrc` references without echoing values.
 */
const ENV_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;

/** The `.npmrc` interpolations Bun substitutes while reading auth and registry config. */
const NPMRC_ENV_REFERENCE = /\$\{([A-Za-z_][A-Za-z0-9_]*)\}/g;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isNonemptyEnvValue(value: string | undefined): boolean {
  return value !== undefined && value.length > 0;
}

/**
 * Validates the `smoo.secrets` map out of an already-parsed package.json.
 * Absent `smoo` or `smoo.secrets` is an empty map; a malformed declaration
 * throws naming the offending variable — never a value.
 */
export function parseSmooSecrets(packageJson: unknown): Readonly<Record<string, SecretSpec>> {
  if (!isRecord(packageJson)) return {};
  const smoo = packageJson['smoo'];
  if (!isRecord(smoo)) return {};
  const secrets = smoo['secrets'];
  if (secrets === undefined) return {};
  if (!isRecord(secrets)) {
    throw new Error('smoo.secrets must map environment variable names to { command: [string, ...] }');
  }
  const parsed: Record<string, SecretSpec> = {};
  for (const [name, spec] of Object.entries(secrets)) {
    parsed[name] = parseSecretSpec(name, spec);
  }
  return parsed;
}

function parseSecretSpec(name: string, spec: unknown): SecretSpec {
  if (!ENV_NAME.test(name)) {
    throw new Error(
      `smoo.secrets: "${name}" is not a valid environment variable name (expected [A-Za-z_][A-Za-z0-9_]*)`,
    );
  }
  if (!isRecord(spec)) {
    throw new Error(`smoo.secrets.${name}: each entry must be an object with a command array`);
  }
  const raw = spec['command'];
  if (!Array.isArray(raw) || raw.length === 0) {
    throw new Error(`smoo.secrets.${name}.command: must be an array of at least one string`);
  }
  for (const part of raw) {
    if (typeof part !== 'string') {
      throw new Error(`smoo.secrets.${name}.command: must be an array of at least one string`);
    }
  }
  const [first, ...rest] = raw;
  return { command: [first, ...rest] };
}

/** Every `${VAR}` referenced from `.npmrc` text; an absent file contributes nothing. */
export function registryAuthEnvNames(npmrcText: string | null): ReadonlySet<string> {
  const names = new Set<string>();
  if (npmrcText === null) return names;
  for (const match of npmrcText.matchAll(NPMRC_ENV_REFERENCE)) {
    names.add(match[1]);
  }
  return names;
}

type SecretOutcome =
  | { readonly name: string; readonly kind: 'env-wins' }
  | { readonly name: string; readonly kind: 'resolved'; readonly value: string }
  | { readonly name: string; readonly kind: 'failed'; readonly guidance: string };

export async function resolveSecrets(request: SecretResolutionRequest): Promise<Readonly<Record<string, string>>> {
  const run = request.runCommand ?? runSecretCommand;
  const env = request.env;
  const ci = isNonemptyEnvValue(env['CI']);
  const cowshed = isNonemptyEnvValue(env['COWSHED_WORKSPACE_TOKEN']);
  const outcomes = await Promise.all(
    Object.entries(request.secrets).map(async ([name, spec]): Promise<SecretOutcome> => {
      if (isNonemptyEnvValue(env[name])) {
        return { name, kind: 'env-wins' };
      }
      if (ci) {
        return {
          name,
          kind: 'failed',
          guidance:
            'CI does not run secret provider commands — inject this variable into the job environment from the CI secret store',
        };
      }
      if (cowshed && request.registryIntentEnvs.has(name)) {
        return {
          name,
          kind: 'failed',
          guidance:
            'referenced from .npmrc and absent in this cowshed workspace — enroll registry credentials through the cowshed gateway; gateway-managed workspaces never resolve .npmrc registry variables via local provider commands',
        };
      }
      try {
        const value = (await run(spec.command)).replace(/\r?\n$/, '');
        if (value.length === 0) {
          return {
            name,
            kind: 'failed',
            guidance:
              'provider command produced no output — run it locally to see why (its arguments and output are never logged here)',
          };
        }
        return { name, kind: 'resolved', value };
      } catch (error) {
        return { name, kind: 'failed', guidance: describeCommandFailure(error) };
      }
    }),
  );
  const failures = outcomes.filter(
    (outcome): outcome is Extract<SecretOutcome, { kind: 'failed' }> => outcome.kind === 'failed',
  );
  if (failures.length > 0) {
    throw new Error(
      [
        'smoo secret resolution failed; dependencies were not installed:',
        ...failures.map((failure) => `- ${failure.name}: ${failure.guidance}`),
      ].join('\n'),
    );
  }
  const resolved: Record<string, string> = {};
  for (const outcome of outcomes) {
    if (outcome.kind === 'resolved') {
      resolved[outcome.name] = outcome.value;
    }
  }
  return resolved;
}

class SecretCommandExitError extends Error {
  constructor(readonly exitCode: number) {
    super('secret provider command failed');
  }
}

class SecretCommandStartError extends Error {
  constructor() {
    super('secret provider command could not be started');
  }
}

function describeCommandFailure(error: unknown): string {
  if (error instanceof SecretCommandExitError) {
    return `provider command exited with code ${error.exitCode}; its arguments and output are suppressed — run it locally to debug`;
  }
  if (error instanceof SecretCommandStartError) {
    return 'provider command could not be started — check that its program exists and is executable, then run it locally to debug';
  }
  // Foreign runner errors may carry argv or provider output; only the generic
  // suppression note survives.
  return 'provider command failed — its arguments and output are suppressed; run it locally to debug';
}

/** The program name is committed config, but an argument may embed a credential, so argv is never echoed. */
function startSecretCommand(argv: readonly string[]) {
  try {
    return Bun.spawn({
      cmd: [...argv],
      stdin: 'ignore',
      stdout: 'pipe',
      // Provider stderr can quote arguments or the secret itself; it is never surfaced.
      stderr: 'ignore',
    });
  } catch {
    throw new SecretCommandStartError();
  }
}

const runSecretCommand: SecretCommandRunner = async (argv) => {
  const child = startSecretCommand(argv);
  const stdout = await new Response(child.stdout).text();
  const exitCode = await child.exited;
  if (exitCode !== 0) {
    throw new SecretCommandExitError(exitCode);
  }
  return stdout;
};

export async function resolveSecretEnvironment(
  options: SecretResolutionOptions,
): Promise<Readonly<Record<string, string>>> {
  return resolveSecrets({
    secrets: parseSmooSecrets(readPackageJson(options.root)),
    registryIntentEnvs: registryAuthEnvNames(readNpmrcText(options.root)),
    env: options.env ?? process.env,
    runCommand: options.runCommand,
  });
}

function readPackageJson(root: string): unknown {
  const packageJsonPath = join(root, 'package.json');
  let text: string;
  try {
    text = readFileSync(packageJsonPath, 'utf8');
  } catch (error) {
    throw new Error(
      `smoo secrets: cannot read ${packageJsonPath} (${error instanceof Error ? error.message : String(error)})`,
    );
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (error) {
    throw new Error(
      `smoo secrets: ${packageJsonPath} is not valid JSON (${error instanceof Error ? error.message : String(error)})`,
    );
  }
  return parsed;
}

function readNpmrcText(root: string): string | null {
  const npmrcPath = join(root, '.npmrc');
  if (!existsSync(npmrcPath)) return null;
  return readFileSync(npmrcPath, 'utf8');
}
