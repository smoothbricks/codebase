/**
 * Reads the three places a repository's secrets are described, all of which
 * smoo already owns:
 *
 * 1. **Workers declare names.** `.dev.vars.example` per wrangler project — the
 *    file `wrangler types --env-file` reads and `prepare-env` prompts from —
 *    and `smoo.wrangler.secretStages` beside it says which stages each name
 *    belongs to.
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
 *
 * The join itself is ./status.ts: pure, and the owner of the document type
 * these readings are projected into.
 */

import { spawnSync } from 'node:child_process';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { repositoryOwnerFromUrl, repositorySecretMapping } from '../lib/secret-names.js';
import { readPackageJsonObject, repositoryInfo } from '../lib/workspace.js';
import { parseDevVarsExample } from '../wrangler/prepare-env.js';
import { readSecretStageMap, type SecretStageMap } from '../wrangler/stage-secrets.js';

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
 * GitHub Environments the rendered workflows bind, read from the workflow
 * files rather than from config: a binding may be hand-authored inside a
 * `smoo-local` block, and the file is what GitHub executes either way. A job
 * bound to an environment resolves `secrets.X` from that environment first and
 * from the repository second, so these names are the scopes a value can live
 * in. Expressions are skipped - a computed environment names no fixed scope.
 */
export function workflowEnvironments(root: string): string[] {
  const directory = join(root, '.github', 'workflows');
  if (!existsSync(directory)) return [];
  const environments = new Set<string>();
  for (const entry of readdirSync(directory)) {
    if (!entry.endsWith('.yml') && !entry.endsWith('.yaml')) continue;
    for (const bound of environmentBindings(readFileSync(join(directory, entry), 'utf8'))) {
      environments.add(bound);
    }
  }
  return [...environments].sort((left, right) => left.localeCompare(right));
}

/**
 * The environments one workflow file binds, in both spellings GitHub accepts:
 * `environment: staging`, and the block form whose `name:` sits under it when
 * the job also records a deployment URL. Only a `name:` indented inside an
 * `environment:` block is a binding - a workflow's, a job's and a step's are
 * not, and reading those as environments would send an operator to set
 * secrets in scopes that do not exist.
 */
function environmentBindings(text: string): string[] {
  const lines = text.split('\n');
  const bound: string[] = [];
  for (let index = 0; index < lines.length; index += 1) {
    const binding = /^(\s*)environment:(.*)$/.exec(lines[index] ?? '');
    if (!binding) continue;
    const indent = (binding[1] ?? '').length;
    const value = binding[2] ?? '';
    // Nothing but a comment after the colon is the block form; anything else
    // is the value itself.
    if (!/^\s*(?:#.*)?$/.test(value)) {
      const name = literalEnvironmentName(value);
      if (name !== null) bound.push(name);
      continue;
    }
    for (let next = index + 1; next < lines.length; next += 1) {
      const line = lines[next] ?? '';
      const content = line.trimStart();
      if (content.length === 0 || content.startsWith('#')) continue;
      if (line.length - content.length <= indent) break;
      const named = /^name:(.*)$/.exec(content);
      if (!named) continue;
      const name = literalEnvironmentName(named[1] ?? '');
      if (name !== null) bound.push(name);
      break;
    }
  }
  return bound;
}

/**
 * The fixed name a YAML value denotes, or null when it denotes no fixed scope:
 * an expression is computed per run, and a flow mapping or a list is not a
 * name. Names may contain spaces, which is why the renderer quotes them.
 */
function literalEnvironmentName(value: string): string | null {
  const scalar = value.replace(/\s+#.*$/, '').trim();
  if (scalar.length === 0 || scalar.includes('${{')) return null;
  const unquoted = /^(['"])(.*)\1$/.exec(scalar)?.[2] ?? scalar;
  return /^[A-Za-z0-9._-][A-Za-z0-9._ -]*$/.test(unquoted) ? unquoted : null;
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

/**
 * Every wrangler project's `smoo.wrangler.secretStages`, keyed by the project
 * directory — the same keys `workerSecretNames` uses, so a name's scopes and
 * its declarations are joined on one label.
 *
 * A malformed block is a refusal rather than a throw: it is the same class of
 * problem as a repository `gh` will not talk to, and `smoo secrets status`
 * reports those instead of dying with a stack. Silently reading it as "no
 * scopes" is the one thing this must not do — that is the direction where
 * every secret quietly reaches every stage.
 */
export function workerSecretStages(
  root: string,
  workspaceDirs: readonly string[],
): { ok: true; byWorker: Record<string, SecretStageMap> } | { ok: false; reason: string } {
  const byWorker: Record<string, SecretStageMap> = {};
  for (const dir of workspaceDirs) {
    if (!existsSync(join(root, dir, '.dev.vars.example'))) continue;
    try {
      const scopes = readSecretStageMap(join(root, dir));
      if (Object.keys(scopes).length > 0) byWorker[dir] = scopes;
    } catch (error) {
      return { ok: false, reason: error instanceof Error ? error.message : String(error) };
    }
  }
  return { ok: true, byWorker };
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
