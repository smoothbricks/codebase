/**
 * Reconciles the three places a repository's secrets are described, all of
 * which smoo already owns:
 *
 * 1. **Workers declare names.** `.dev.vars.example` per wrangler project — the
 *    file `wrangler types --env-file` reads and `prepare-env` prompts from.
 * 2. **Workflows declare where values come from.** `smoo.github.deploySecrets`
 *    and `e2eSecrets` map an env name to a repository secret, and the managed
 *    workflows render exactly those into the job environment.
 * 3. **A developer shell declares how to fetch one locally.** `smoo.secrets`
 *    names a command whose stdout is the value.
 *
 * Nothing joined them, so a Worker could declare a secret no workflow supplies
 * and no repository holds. That fails at deploy time, on a stage, with a
 * fail-closed message about the value rather than about the missing
 * declaration — the shape that costs an afternoon: a Worker declares
 * `STRIPE_PUBLISHABLE_KEY`, `ci.yml` passes `secrets.STRIPE_PUBLISHABLE_KEY`,
 * the repository holds no such secret, and the empty value surfaces as the
 * payment library's own `publishable_key_mismatch` refusal.
 */

import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { repositoryOwnerFromUrl, repositorySecretMapping } from '../lib/secret-names.js';
import { readPackageJsonObject, repositoryInfo } from '../lib/workspace.js';
import { parseDevVarsExample } from '../wrangler/prepare-env.js';

/** Where an env name is described, which repository secret carries it, and whether that exists. */
export interface SecretRow {
  name: string;
  /** The repository secret this env name reads from, by convention or declaration. */
  repositorySecret: string;
  /** Wrangler projects whose `.dev.vars.example` declares it. */
  declaredByWorkers: string[];
  /** True when a managed workflow renders `secrets.<name>` into a job. */
  suppliedByWorkflow: boolean;
  /** True when `smoo.secrets` can fetch it for a developer shell. */
  fetchableLocally: boolean;
  /** True when the repository holds a secret of this name. */
  onRepository: boolean;
}

export interface SecretSources {
  /** Worker label -> env names it declares. */
  workerSecrets: Record<string, readonly string[]>;
  /** Env names a managed workflow passes into a job. */
  workflowSecrets: readonly string[];
  /** Env name -> repository secret, by convention with declared exceptions. */
  secretNames: Readonly<Record<string, string>>;
  /** Env names `smoo.secrets` can fetch locally. */
  localCommands: readonly string[];
  /** Repository secret names GitHub currently holds. */
  repositorySecrets: readonly string[];
}

/**
 * One row per name known to any source, sorted, so a reader sees the whole
 * picture rather than one source's view of it.
 */
export function reconcileSecrets(sources: SecretSources): SecretRow[] {
  const declaredByAnyWorker: string[] = Object.values(sources.workerSecrets).flatMap((names) => [...names]);
  const names = new Set<string>([...declaredByAnyWorker, ...sources.workflowSecrets, ...sources.localCommands]);
  // A repository secret that already carries a known env name is that name's
  // row, not a row of its own: listing ACME_GITHUB_CLIENT_SECRET beside
  // GITHUB_CLIENT_SECRET would report one value as two secrets, one of them
  // permanently "declared by nothing".
  const carriesKnownEnvName = new Set(
    names.size > 0 ? [...names].map((name) => sources.secretNames[name] ?? name) : [],
  );
  for (const secret of sources.repositorySecrets) {
    if (!carriesKnownEnvName.has(secret)) names.add(secret);
  }
  return [...names]
    .sort((left, right) => left.localeCompare(right))
    .map((name) => ({
      name,
      repositorySecret: sources.secretNames[name] ?? name,
      declaredByWorkers: Object.entries(sources.workerSecrets)
        .filter(([, declared]) => declared.includes(name))
        .map(([label]) => label)
        .sort((left, right) => left.localeCompare(right)),
      suppliedByWorkflow: sources.workflowSecrets.includes(name),
      fetchableLocally: sources.localCommands.includes(name),
      onRepository: sources.repositorySecrets.includes(sources.secretNames[name] ?? name),
    }));
}

/**
 * The rows a CI deploy cannot satisfy: a Worker declares the name, a workflow
 * promises to pass it, and the repository has no value to pass. Reported
 * separately from "declared but not wired into any workflow", because the
 * remedies differ — set a secret, versus declare it in `smoo.github`.
 */
export function unsatisfiedSecrets(rows: readonly SecretRow[]): SecretRow[] {
  return rows.filter((row) => row.suppliedByWorkflow && !row.onRepository);
}

/** Worker-declared names no managed workflow passes: a CI deploy will run without them. */
export function unwiredSecrets(rows: readonly SecretRow[]): SecretRow[] {
  return rows.filter((row) => row.declaredByWorkers.length > 0 && !row.suppliedByWorkflow);
}

/** Env names the managed workflows pass into a job, from the declarations that render them. */
export function workflowSecretNames(root: string): string[] {
  const manifest = readPackageJsonObject(join(root, 'package.json'));
  const smoo = manifest?.smoo;
  const github = smoo?.github;
  const names = new Set<string>([
    ...Object.keys(github?.deploySecrets ?? {}),
    ...Object.keys(github?.e2eSecrets ?? {}),
  ]);
  if (smoo?.remoteCache?.tokenSecret) names.add(smoo.remoteCache.tokenSecret);
  for (const origin of github?.cargoCredentials?.gitOrigins ?? []) {
    if (origin.tokenEnv) names.add(origin.tokenEnv);
  }
  for (const source of github?.sourceCheckouts ?? []) {
    if (source.tokenEnv) names.add(source.tokenEnv);
  }
  if (smoo?.privateNpm?.readTokenEnv) names.add(smoo.privateNpm.readTokenEnv);
  if (smoo?.privateNpm?.publishTokenEnv) names.add(smoo.privateNpm.publishTokenEnv);
  return [...names].sort((left, right) => left.localeCompare(right));
}

/**
 * Env name -> repository secret for every name in play, following the naming
 * convention and honouring the declared exceptions a repository still needs.
 */
export function secretNameMapping(root: string, envNames: readonly string[]): Record<string, string> {
  const manifest = readPackageJsonObject(join(root, 'package.json'));
  const declared = { ...(manifest?.smoo?.github?.deploySecrets ?? {}), ...(manifest?.smoo?.github?.e2eSecrets ?? {}) };
  const repository = manifest ? repositoryInfo(manifest) : null;
  const owner = repository ? (repositoryOwnerFromUrl(repository.url) ?? '') : '';
  return repositorySecretMapping(envNames, owner, declared);
}

/** Every wrangler project's declared secret names, keyed by the project directory. */
export function workerSecretNames(root: string, workspaceDirs: readonly string[]): Record<string, string[]> {
  const byWorker: Record<string, string[]> = {};
  for (const dir of workspaceDirs) {
    const examplePath = join(root, dir, '.dev.vars.example');
    if (!existsSync(examplePath)) continue;
    const names = parseDevVarsExample(readFileSync(examplePath, 'utf8'));
    if (names.length > 0) byWorker[dir] = names;
  }
  return byWorker;
}

/** Names `smoo.secrets` declares a fetch command for. */
export function localSecretCommandNames(root: string): string[] {
  const manifest = readPackageJsonObject(join(root, 'package.json'));
  return Object.keys(manifest?.smoo?.secrets ?? {}).sort((left, right) => left.localeCompare(right));
}

/**
 * Resolve one declared secret through its command. The value is returned, never
 * logged: callers hand it to `gh secret set` over stdin.
 */
export function fetchLocalSecret(
  root: string,
  name: string,
): { ok: true; value: string } | { ok: false; reason: string } {
  const manifest = readPackageJsonObject(join(root, 'package.json'));
  const declared = manifest?.smoo?.secrets?.[name];
  if (!declared) {
    return { ok: false, reason: `smoo.secrets declares no command for ${name}` };
  }
  const [command, ...args] = declared.command;
  const result = spawnSync(command, args, { encoding: 'utf8' });
  if (result.status !== 0) {
    // Name the command, never its output: a failing secret fetch prints
    // credentials often enough that it is worth refusing to.
    return {
      ok: false,
      reason: `${name}: \`${declared.command.join(' ')}\` exited ${result.status ?? 'without status'}`,
    };
  }
  const value = result.stdout.replace(/\n$/, '');
  if (value.length === 0) {
    return { ok: false, reason: `${name}: \`${declared.command.join(' ')}\` produced no value` };
  }
  return { ok: true, value };
}
