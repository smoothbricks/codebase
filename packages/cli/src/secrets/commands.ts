/**
 * `smoo secrets` — the operator side of the reconciliation in ./index.ts.
 *
 * Values are handed to `gh` over stdin, never through argv (a process list is
 * world-readable) and never printed. Reading one from the terminal disables
 * echo, so a pasted key does not end up in a scrollback buffer or a screen
 * share.
 */

import { spawn, spawnSync } from 'node:child_process';
import { getWorkspacePackages } from '../lib/workspace.js';
import {
  fetchLocalSecret,
  localSecretCommandNames,
  reconcileSecrets,
  type SecretRow,
  secretNameMapping,
  unsatisfiedSecrets,
  unwiredSecrets,
  workerSecretNames,
  workflowSecretNames,
} from './index.js';

/** Repository secret names GitHub currently holds, or a refusal naming what failed. */
export function readRepositorySecrets(
  repo: string | undefined,
): { ok: true; names: string[] } | { ok: false; reason: string } {
  const args = ['secret', 'list', '--json', 'name'];
  if (repo) args.push('--repo', repo);
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

/** Write one secret, value over stdin so it never reaches argv. */
export async function writeRepositorySecret(
  name: string,
  value: string,
  repo: string | undefined,
): Promise<{ ok: true } | { ok: false; reason: string }> {
  const args = ['secret', 'set', name];
  if (repo) args.push('--repo', repo);
  const child = spawn('gh', args, { stdio: ['pipe', 'inherit', 'inherit'] });
  child.stdin.end(value);
  const status = await new Promise<number | null>((resolvePromise) => {
    child.on('close', (code) => resolvePromise(code));
  });
  return status === 0
    ? { ok: true }
    : { ok: false, reason: `gh ${args.join(' ')} exited ${status ?? 'without status'}` };
}

function collectSources(root: string, repositorySecrets: readonly string[]) {
  const workspaceDirs = getWorkspacePackages(root).map((pkg) => pkg.path);
  const workerSecrets = workerSecretNames(root, workspaceDirs);
  const workflowSecrets = workflowSecretNames(root);
  const localCommands = localSecretCommandNames(root);
  const envNames = [
    ...new Set([...Object.values(workerSecrets).flatMap((names) => [...names]), ...workflowSecrets, ...localCommands]),
  ];
  return {
    workerSecrets,
    workflowSecrets,
    secretNames: secretNameMapping(root, envNames),
    localCommands,
    repositorySecrets,
  };
}

function describe(row: SecretRow): string {
  const where = row.declaredByWorkers.length > 0 ? row.declaredByWorkers.join(', ') : '—';
  return [
    row.onRepository ? 'set ' : 'ABSENT',
    row.name.padEnd(32),
    row.suppliedByWorkflow ? 'workflow' : '        ',
    row.fetchableLocally ? 'local' : '     ',
    where,
  ].join('  ');
}

/**
 * Print every known secret with its sources. Exit code 1 when a workflow
 * promises a value the repository does not hold — that combination is the one
 * that fails a deploy, and it fails talking about the value rather than the
 * missing secret.
 */
export function secretsStatus(root: string, options: { repo?: string }): number {
  const repositorySecrets = readRepositorySecrets(options.repo);
  if (!repositorySecrets.ok) {
    console.error(repositorySecrets.reason);
    return 1;
  }
  const rows = reconcileSecrets(collectSources(root, repositorySecrets.names));
  console.log('state   name                              workflow  local  declared by');
  for (const row of rows) console.log(describe(row));

  const unsatisfied = unsatisfiedSecrets(rows);
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
  if (unsatisfied.length > 0) {
    console.log('');
    for (const row of unsatisfied) {
      console.error(`missing: ${row.name} — a workflow passes secrets.${row.name} and the repository has no value.`);
      console.error(
        `         set it with: smoo secrets set ${row.name}${options.repo ? ` --repo ${options.repo}` : ''}`,
      );
    }
    return 1;
  }
  return 0;
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

/** Set one secret from a pasted value. */
export async function secretsSet(name: string, options: { repo?: string }): Promise<number> {
  const value = await readSecretValue(`Value for ${name} (not echoed): `);
  if (value.length === 0) {
    console.error(`${name}: refusing to set an empty value.`);
    return 1;
  }
  const written = await writeRepositorySecret(name, value, options.repo);
  if (!written.ok) {
    console.error(written.reason);
    return 1;
  }
  console.log(`set     ${name}`);
  return 0;
}

/**
 * Push every locally fetchable secret to the repository. Only names
 * `smoo.secrets` declares a command for: everything else has no source here
 * and must be pasted with `smoo secrets set`.
 */
export async function secretsSync(root: string, options: { repo?: string }): Promise<number> {
  const names = localSecretCommandNames(root);
  if (names.length === 0) {
    console.error('smoo.secrets declares no fetch commands; nothing to sync.');
    return 1;
  }
  let failed = 0;
  for (const name of names) {
    const fetched = fetchLocalSecret(root, name);
    if (!fetched.ok) {
      console.error(`skip    ${name}: ${fetched.reason}`);
      failed += 1;
      continue;
    }
    const written = await writeRepositorySecret(name, fetched.value, options.repo);
    if (!written.ok) {
      console.error(`fail    ${name}: ${written.reason}`);
      failed += 1;
      continue;
    }
    console.log(`set     ${name}`);
  }
  return failed === 0 ? 0 : 1;
}
