/**
 * `smoo secrets` — the operator side of the reconciliation in ./status.ts.
 *
 * Values are handed to `gh` over stdin, never through argv (a process list is
 * world-readable) and never printed. Reading one from the terminal disables
 * echo, so a pasted key does not end up in a scrollback buffer or a screen
 * share.
 *
 * A value lives at repository scope or inside a named GitHub Environment, and
 * a job bound to an environment resolves `secrets.X` from that environment
 * first and from the repository second — which is how one name carries test
 * credentials on a preview stage and live ones in production. Every scope the
 * workflows bind is read before anything is called missing: reading only the
 * repository reports values that do exist as absent, and sends an operator
 * hunting for a secret that is already set.
 */

import { spawn, spawnSync } from 'node:child_process';
import { getWorkspacePackages } from '../lib/workspace.js';
import {
  fetchLocalSecret,
  localSecretGroups,
  secretNameMapping,
  workerSecretNames,
  workerSecretStages,
  workflowEnvironments,
  workflowSecretNames,
} from './index.js';
import { resolveRepository } from './repository.js';
import {
  type LocalSecret,
  projectSecretsStatus,
  reconcileSecrets,
  type SecretRow,
  type SecretsStatusDocument,
  type SecretsStatusEnvironment,
  stringifySecretsStatusDocument,
  unwiredSecrets,
} from './status.js';

/**
 * Every secret this checkout declares that the repository does not hold, in
 * the order an operator should fix them: the ones a workflow already promises
 * break a deploy today, the rest break the next one.
 */
export function secretsNeedingValues(rows: readonly SecretRow[]): SecretRow[] {
  const missing = rows.filter((row) => !row.onRepository);
  return [...missing.filter((row) => row.suppliedByWorkflow), ...missing.filter((row) => !row.suppliedByWorkflow)];
}

/**
 * Every secret this checkout declares that the named environment does not
 * hold. A name the repository already holds is still offered: an environment
 * value overrides the repository's for a job bound to it, and overriding one
 * stage's credentials is the reason to write at environment scope at all. A
 * repository secret no source declares is not offered — that is a stale value
 * to review, not one to copy into an environment.
 */
export function secretsMissingInEnvironment(rows: readonly SecretRow[], environment: string): SecretRow[] {
  const missing = rows.filter(
    (row) =>
      !row.heldByEnvironment.includes(environment) &&
      (row.declaredByWorkers.length > 0 || row.suppliedByWorkflow || row.fetchableLocally),
  );
  return [...missing.filter((row) => row.suppliedByWorkflow), ...missing.filter((row) => !row.suppliedByWorkflow)];
}

/** Why a secret is wanted, so a prompt states what will break without it. */
export function describeNeed(row: SecretRow): string {
  const parts: string[] = [];
  if (row.declaredByWorkers.length > 0) parts.push(`declared by ${row.declaredByWorkers.join(', ')}`);
  if (row.suppliedByWorkflow) parts.push('passed by a managed workflow');
  if (row.fetchableLocally) parts.push('fetchable locally via smoo.secrets');
  if (row.repositorySecret !== row.name) parts.push(`read as ${row.name}`);
  // Naming the environments that already hold it stops an operator from
  // answering a per-stage prompt with a repository-wide value.
  if (row.heldByEnvironment.length > 0) parts.push(`held by ${row.heldByEnvironment.join(', ')}`);
  return parts.length > 0 ? ` - ${parts.join('; ')}` : '';
}

/**
 * Secret names GitHub currently holds at the given scope, or a refusal naming
 * what failed. With no environment that is repository scope; with one it is
 * that environment's own secrets, which are the values a bound job reads
 * first.
 */
export function readRepositorySecrets(
  repo: string | undefined,
  environment?: string,
): { ok: true; names: string[] } | { ok: false; reason: string } {
  const args = ['secret', 'list', '--json', 'name'];
  if (repo) args.push('--repo', repo);
  if (environment) args.push('--env', environment);
  const result = spawnSync('gh', args, { encoding: 'utf8' });
  if (result.status !== 0) {
    return {
      ok: false,
      reason: `gh ${args.join(' ')} exited ${result.status ?? 'without status'}: ${result.stderr.trim()}`,
    };
  }
  const parsed: unknown = JSON.parse(result.stdout);
  if (!Array.isArray(parsed)) {
    return { ok: false, reason: 'gh secret list did not return a list' };
  }
  const names: string[] = [];
  for (const row of parsed) {
    if (typeof row !== 'object' || row === null || !('name' in row)) continue;
    // `in` narrows the property to unknown, so the typeof check below is the
    // only thing that admits it — no assertion about gh's output shape.
    const name: unknown = row.name;
    if (typeof name === 'string') names.push(name);
  }
  return { ok: true, names: names.sort((left, right) => left.localeCompare(right)) };
}

/** Write one secret at the given scope, value over stdin so it never reaches argv. */
export async function writeRepositorySecret(
  name: string,
  value: string,
  repo: string | undefined,
  environment?: string,
): Promise<{ ok: true } | { ok: false; reason: string }> {
  const args = ['secret', 'set', name];
  if (repo) args.push('--repo', repo);
  if (environment) args.push('--env', environment);
  const child = spawn('gh', args, { stdio: ['pipe', 'inherit', 'inherit'] });
  child.stdin.end(value);
  const status = await new Promise<number | null>((resolvePromise) => {
    child.on('close', (code) => resolvePromise(code));
  });
  return status === 0
    ? { ok: true }
    : { ok: false, reason: `gh ${args.join(' ')} exited ${status ?? 'without status'}` };
}

function collectSources(
  root: string,
  workspaceDirs: readonly string[],
  localSecrets: readonly LocalSecret[],
  repositorySecrets: readonly string[],
  environmentSecrets: Readonly<Record<string, readonly string[]>> = {},
) {
  const workerSecrets = workerSecretNames(root, workspaceDirs);
  const workflowSecrets = workflowSecretNames(root);
  const envNames = [
    ...new Set([
      ...Object.values(workerSecrets).flatMap((names) => [...names]),
      ...workflowSecrets,
      ...localSecrets.map((secret) => secret.name),
    ]),
  ];
  return {
    workerSecrets,
    workflowSecrets,
    secretNames: secretNameMapping(root, envNames),
    localSecrets,
    repositorySecrets,
    environmentSecrets,
  };
}

/** Every scope holding a value for this name, repository first. */
function heldBy(row: SecretRow): string {
  const scopes = row.onRepository ? ['repository', ...row.heldByEnvironment] : row.heldByEnvironment;
  return scopes.length > 0 ? scopes.join(', ') : 'ABSENT';
}

function describe(row: SecretRow, scopeWidth: number): string {
  const where = row.declaredByWorkers.length > 0 ? row.declaredByWorkers.join(', ') : '—';
  return [
    heldBy(row).padEnd(scopeWidth),
    row.name.padEnd(32),
    row.suppliedByWorkflow ? 'workflow' : '        ',
    row.fetchableLocally ? 'local' : '     ',
    where,
  ].join('  ');
}

/**
 * Everything `status` reports, read from git, `gh` and the checkout before a
 * single line is printed. Gathering ahead of rendering is what lets one run
 * answer either a human or a machine from exactly the same facts: `--json`
 * cannot drift from the table because there is nothing for it to drift from.
 */
async function gatherSecretsStatus(
  root: string,
  options: { repo?: string; env?: string },
): Promise<{ ok: true; document: SecretsStatusDocument } | { ok: false; reason: string }> {
  const resolved = resolveRepository(root, options.repo);
  if (!resolved.ok) return resolved;
  const { repo, source } = resolved.choice;
  const repositorySecrets = readRepositorySecrets(repo);
  if (!repositorySecrets.ok) return repositorySecrets;
  // The workflows decide which environments a job reads; an explicitly asked
  // for one is shown too, so an operator can inspect a scope before a workflow
  // binds it.
  const boundEnvironments = workflowEnvironments(root);
  const asked = options.env !== undefined && !boundEnvironments.includes(options.env) ? [options.env] : [];
  const environmentSecrets: Record<string, string[]> = {};
  const environments: SecretsStatusEnvironment[] = [];
  for (const name of [...boundEnvironments, ...asked]) {
    const bound = boundEnvironments.includes(name);
    const held = readRepositorySecrets(repo, name);
    // An environment that cannot be read is reported, never assumed empty:
    // guessing produces a refusal about a value that may well be set.
    if (!held.ok) {
      environments.push({ name, bound, readable: false, reason: held.reason });
      continue;
    }
    environmentSecrets[name] = held.names;
    environments.push({ name, bound, readable: true, secretCount: held.names.length });
  }
  const workspaceDirs = getWorkspacePackages(root).map((pkg) => pkg.path);
  const stageScopes = workerSecretStages(root, workspaceDirs);
  if (!stageScopes.ok) return stageScopes;
  // A `smoo.secrets` declaration smoo cannot read is a refusal, not an empty
  // list: reporting "nothing is fetchable locally" for a manifest that says
  // otherwise is the confident wrong answer.
  const localSecrets = await localSecretGroups(root);
  if (!localSecrets.ok) return localSecrets;
  return {
    ok: true,
    document: projectSecretsStatus({
      repository: { repo, source, secretCount: repositorySecrets.names.length },
      environments,
      rows: reconcileSecrets(
        collectSources(root, workspaceDirs, localSecrets.secrets, repositorySecrets.names, environmentSecrets),
      ),
      stageScopes: stageScopes.byWorker,
    }),
  };
}

/**
 * Print every known secret and the scopes holding it, or - with `json` - the
 * same facts as one document on stdout and nothing else. Exit code 1 when a
 * workflow promises a value no scope a bound job reads can supply — that
 * combination is the one that fails a deploy, and it fails talking about the
 * value rather than the missing secret. `--json` reports that refusal in
 * `unsatisfied` and still exits 1: a machine-readable status that always
 * succeeded would be a status nobody could gate on.
 */
export async function secretsStatus(
  root: string,
  options: { repo?: string; env?: string; json?: boolean },
): Promise<number> {
  const gathered = await gatherSecretsStatus(root, options);
  if (!gathered.ok) {
    console.error(gathered.reason);
    return 1;
  }
  const { document } = gathered;
  const { repo, source } = document.repository;
  if (options.json) {
    console.log(stringifySecretsStatusDocument(document));
    return document.unsatisfied.length > 0 ? 1 : 0;
  }
  const rows = document.secrets;

  console.log(`repository ${repo} (${source}), holding ${document.repository.secretCount} secrets`);
  for (const environment of document.environments) {
    if (environment.readable)
      console.log(`environment ${environment.name}, holding ${environment.secretCount} secrets`);
  }
  for (const environment of document.environments) {
    if (environment.readable) continue;
    console.log(
      `environment ${environment.name} could not be read, so nothing below claims what it holds: ${environment.reason}`,
    );
  }
  const scopeWidth = Math.max('held by'.length, ...rows.map((row) => heldBy(row).length));
  console.log(`${'held by'.padEnd(scopeWidth)}  ${'name'.padEnd(32)}  workflow  local  declared by`);
  for (const row of rows) console.log(describe(row, scopeWidth));

  const unwired = unwiredSecrets(rows);
  if (unwired.length > 0) {
    console.log('');
    for (const row of unwired) {
      console.log(
        `note: ${row.name} is declared by ${row.declaredByWorkers.join(', ')} but no managed workflow passes it; ` +
          'declare it in smoo.github.deploySecrets to have CI supply it.',
      );
    }
  }
  // A declared `group` that disagrees with what this repository's own
  // declarations derive is the override working — and a silent override is
  // how the next reader loses an hour wondering why a `.npmrc` credential
  // resolves at shell entry. It is stated here, where a human is already
  // looking at every declared secret.
  const overridden = rows.filter(
    (row) => row.localGroup !== undefined && row.localGroup.resolves !== row.localGroup.derived,
  );
  if (overridden.length > 0) {
    console.log('');
    for (const row of overridden) {
      const local = row.localGroup;
      if (local === undefined) continue;
      console.log(
        `note: ${row.name} declares group \`${local.resolves}\`, overriding the \`${local.derived}\` this ` +
          `repository's declarations derive; \`smoo secrets run ${local.resolves} <command>\` is what resolves it.`,
      );
    }
  }
  // A value an environment holds is not a value to duplicate at repository
  // scope: offering it invites two sources of truth for one credential, and
  // the table above already shows where it lives. Only a name no scope holds
  // is a name to set here.
  const needed = secretsNeedingValues(rows).filter((row) => !row.onRepository && row.heldByEnvironment.length === 0);
  if (needed.length > 0) {
    console.log('');
    console.log(`${needed.length} secret(s) have no value in any scope on ${repo}:`);
    for (const row of needed) console.log(`  ${row.repositorySecret}${describeNeed(row)}`);
    console.log('');
    console.log(`set them all, one prompt each:  smoo secrets set -R ${repo}`);
    console.log(`set one:                        smoo secrets set ${needed[0]?.repositorySecret ?? 'NAME'} -R ${repo}`);
  }

  if (document.unsatisfied.length === 0) return 0;
  console.log('');
  for (const { name, repositorySecret, missingIn } of document.unsatisfied) {
    const scopes = missingIn.length > 0 ? missingIn.join(', ') : 'the repository';
    console.error(`missing: ${name} — a workflow passes secrets.${name} and no value exists in ${scopes}.`);
    if (missingIn.length === 0) {
      console.error(`  smoo secrets set ${repositorySecret} -R ${repo}`);
      continue;
    }
    for (const environment of missingIn) {
      const named = environment.includes(' ') ? `'${environment}'` : environment;
      console.error(`  smoo secrets set ${repositorySecret} -R ${repo} --env ${named}`);
    }
    console.error(`  or one value for every environment:  smoo secrets set ${repositorySecret} -R ${repo}`);
  }
  return 1;
}

/** Read a value with echo disabled when stdin is a terminal; otherwise read piped input. */
async function readSecretValue(prompt: string): Promise<string> {
  if (!process.stdin.isTTY) {
    const chunks: Buffer[] = [];
    for await (const chunk of process.stdin) chunks.push(Buffer.from(chunk));
    return Buffer.concat(chunks).toString('utf8').replace(/\n$/, '');
  }
  process.stderr.write(prompt);
  process.stdin.setRawMode(true);
  let value = '';
  try {
    for await (const chunk of process.stdin) {
      const text = Buffer.from(chunk).toString('utf8');
      if (text === '\r' || text === '\n' || text === '\u0004') break;
      if (text === '\u0003') throw new Error('cancelled');
      if (text === '\u007f') {
        value = value.slice(0, -1);
        continue;
      }
      value += text;
    }
  } finally {
    process.stdin.setRawMode(false);
    process.stderr.write('\n');
  }
  return value;
}

/**
 * Set one secret, or - with no name - walk every secret this checkout declares
 * and the target scope does not hold, prompting once each. The walk is the
 * point of the command: an operator should never have to transcribe names out
 * of a status table to know what to paste.
 */
export async function secretsSet(
  root: string,
  name: string | undefined,
  options: { repo?: string; env?: string },
): Promise<number> {
  const resolved = resolveRepository(root, options.repo);
  if (!resolved.ok) {
    console.error(resolved.reason);
    return 1;
  }
  const { repo, source } = resolved.choice;
  const environment = options.env;
  if (name !== undefined) return setOne(name, name, repo, environment);

  const held = readRepositorySecrets(repo);
  if (!held.ok) {
    console.error(held.reason);
    return 1;
  }
  const environmentSecrets: Record<string, string[]> = {};
  if (environment !== undefined) {
    const inEnvironment = readRepositorySecrets(repo, environment);
    if (!inEnvironment.ok) {
      console.error(inEnvironment.reason);
      return 1;
    }
    environmentSecrets[environment] = inEnvironment.names;
  }
  const workspaceDirs = getWorkspacePackages(root).map((pkg) => pkg.path);
  const local = await localSecretGroups(root);
  if (!local.ok) {
    console.error(local.reason);
    return 1;
  }
  const rows = reconcileSecrets(collectSources(root, workspaceDirs, local.secrets, held.names, environmentSecrets));
  const needed =
    environment === undefined ? secretsNeedingValues(rows) : secretsMissingInEnvironment(rows, environment);
  const target =
    environment === undefined ? `${repo} (${source})` : `environment ${environment} on ${repo} (${source})`;
  if (needed.length === 0) {
    console.log(`${target} already holds every secret this checkout declares.`);
    return 0;
  }
  console.log(`${needed.length} secret(s) to set on ${target}. Empty input skips one.`);
  let failed = 0;
  let set = 0;
  for (const row of needed) {
    console.log('');
    console.log(`${row.repositorySecret}${describeNeed(row)}`);
    const value = await readSecretValue('  paste value (not echoed, empty to skip): ');
    if (value.length === 0) {
      console.log(`skip    ${row.repositorySecret}`);
      continue;
    }
    const written = await writeRepositorySecret(row.repositorySecret, value, repo, environment);
    if (!written.ok) {
      console.error(`fail    ${row.repositorySecret}: ${written.reason}`);
      failed += 1;
      continue;
    }
    console.log(`set     ${row.repositorySecret}`);
    set += 1;
  }
  console.log('');
  console.log(`${set} set, ${needed.length - set - failed} skipped, ${failed} failed.`);
  return failed === 0 ? 0 : 1;
}

async function setOne(
  envName: string,
  repositorySecret: string,
  repo: string,
  environment: string | undefined,
): Promise<number> {
  const scope = environment === undefined ? '' : ` in ${environment}`;
  const value = await readSecretValue(`Value for ${repositorySecret}${scope} (not echoed): `);
  if (value.length === 0) {
    console.error(`${repositorySecret}: refusing to set an empty value.`);
    return 1;
  }
  const written = await writeRepositorySecret(repositorySecret, value, repo, environment);
  if (!written.ok) {
    console.error(written.reason);
    return 1;
  }
  console.log(`set     ${repositorySecret}${scope}${envName === repositorySecret ? '' : ` (reads as ${envName})`}`);
  return 0;
}

/**
 * Push every locally fetchable secret to the repository, or to one environment.
 * Only names `smoo.secrets` declares a command for: everything else has no
 * source here and must be pasted with `smoo secrets set`.
 */
export async function secretsSync(root: string, options: { repo?: string; env?: string }): Promise<number> {
  const resolved = resolveRepository(root, options.repo);
  if (!resolved.ok) {
    console.error(resolved.reason);
    return 1;
  }
  const { repo } = resolved.choice;
  const environment = options.env;
  const local = await localSecretGroups(root);
  if (!local.ok) {
    console.error(local.reason);
    return 1;
  }
  // Every group: a repository secret store holds the credential whatever
  // resolves it locally, and a `smoo secrets run` group is about which
  // command pays the provider prompt, not about which values CI needs.
  const names = local.secrets.map((secret) => secret.name);
  if (names.length === 0) {
    console.error('smoo.secrets declares no fetch commands; nothing to sync.');
    return 1;
  }
  const scope = environment === undefined ? '' : ` in ${environment}`;
  let failed = 0;
  for (const name of names) {
    const fetched = fetchLocalSecret(root, name);
    if (!fetched.ok) {
      console.error(`skip    ${name}: ${fetched.reason}`);
      failed += 1;
      continue;
    }
    const written = await writeRepositorySecret(name, fetched.value, repo, environment);
    if (!written.ok) {
      console.error(`fail    ${name}: ${written.reason}`);
      failed += 1;
      continue;
    }
    console.log(`set     ${name}${scope}`);
  }
  return failed === 0 ? 0 : 1;
}
