/**
 * `smoo secrets run <group> <command...>` — the half of THE RULE that shell
 * entry is not.
 *
 * The rule lives in tooling/direnv/secret-references.ts: shell entry resolves
 * the `shell` group and nothing else, because a provider authorises per
 * requesting process lineage and every direnv reload is a new one. Every
 * other group belongs to the command that needs it, and this is that command.
 * One deliberate `smoo secrets run registry bun install` instead of a
 * credential prompt on every shell.
 *
 * Exactly one group is resolved. A repository that declares a registry
 * credential and a cache token runs one provider command here, not two: a
 * wrapper that resolved everything declared would fire provider commands for
 * credentials the command never touches, which is the prompt this exists to
 * remove.
 *
 * The value reaches the child the only way that leaves no trace: its
 * environment. Never a file, never argv — argv is world-readable through
 * `ps` — and never this process's stdout.
 */

import { spawn } from 'node:child_process';
import { constants } from 'node:os';
import { type GroupedSecret, readSecretGroups, resolveSecretGroup } from './resolver.js';

/**
 * Nothing ran, and the listing is the answer: no group named, a group this
 * repository does not declare, or a group with no command to run. Distinct
 * from 1, which is a resolution that refused, and from any other code, which
 * is the child's own.
 */
const REFUSED = 2;

/** A command that could not be started at all, as a shell reports it. */
const NOT_EXECUTABLE = 127;

export async function secretsRun(root: string, group: string | undefined, command: readonly string[]): Promise<number> {
  let declared: GroupedSecret[];
  try {
    declared = await readSecretGroups(root);
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    return 1;
  }

  if (group === undefined) {
    console.error(
      'smoo secrets run <group> <command...> runs one command with one group of secrets in its environment.',
    );
    printGroups(declared);
    return REFUSED;
  }

  const members = declared.filter((secret) => secret.group === group);
  if (members.length === 0) {
    console.error(`smoo secrets run: no declared secret is in group \`${group}\`.`);
    printGroups(declared);
    return REFUSED;
  }

  const [program, ...args] = command;
  if (program === undefined) {
    console.error(
      `smoo secrets run ${group} <command...>: name the command to run with ` +
        `${members.map((secret) => secret.name).join(', ')} in its environment.`,
    );
    return REFUSED;
  }

  let values: Readonly<Record<string, string>>;
  try {
    ({ values } = await resolveSecretGroup(root, group));
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    return 1;
  }

  return await runWithSecrets(program, args, values);
}

/**
 * The child's exit status, reproduced: a code as itself, death by signal as
 * 128+signum, and a program that could not be started as 127. It shares this
 * process group, so terminal signals reach it directly; it is spawned rather
 * than exec'd only because neither Bun nor Node exposes `execve`.
 */
async function runWithSecrets(
  program: string,
  args: readonly string[],
  values: Readonly<Record<string, string>>,
): Promise<number> {
  return await new Promise<number>((resolve) => {
    const child = spawn(program, [...args], { stdio: 'inherit', env: { ...process.env, ...values } });
    child.on('error', (error: Error) => {
      console.error(`smoo secrets run: cannot run \`${program}\`: ${error.message}`);
      resolve(NOT_EXECUTABLE);
    });
    child.on('close', (code: number | null, signal: NodeJS.Signals | null) => {
      if (signal !== null) {
        resolve(128 + constants.signals[signal]);
        return;
      }
      resolve(code ?? 1);
    });
  });
}

/**
 * The groups this repository declares and the secrets in each. Most people
 * meet this command exactly once, at a 401, after reading a message that
 * named it — so the refusal answers "which group?" from this checkout's own
 * declarations rather than printing a usage line that sends them to the
 * manifest to work it out.
 */
function printGroups(declared: readonly GroupedSecret[]): void {
  if (declared.length === 0) {
    console.error('This repository declares no smoo.secrets, so there is no group to run.');
    return;
  }
  const members = new Map<string, string[]>();
  for (const secret of declared) {
    const named = members.get(secret.group);
    if (named === undefined) {
      members.set(secret.group, [secret.name]);
      continue;
    }
    named.push(secret.name);
  }
  const groups = [...members.keys()].sort((left, right) => left.localeCompare(right));
  const width = Math.max(...groups.map((group) => group.length));
  console.error('');
  console.error('Groups this repository declares:');
  for (const group of groups) {
    console.error(`  ${group.padEnd(width)}  ${(members.get(group) ?? []).join(', ')}`);
  }
  // An override is stated, never left for the next reader to discover by
  // wondering why a `.npmrc` credential resolves at shell entry.
  for (const secret of declared) {
    if (secret.group === secret.derivedGroup) continue;
    console.error(
      `  note: ${secret.name} declares group \`${secret.group}\`, overriding the \`${secret.derivedGroup}\` ` +
        "this repository's declarations derive.",
    );
  }
  console.error('');
  console.error(`  smoo secrets run ${groups[0] ?? '<group>'} <command...>`);
}
