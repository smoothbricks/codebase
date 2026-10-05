/**
 * A public repository's tree and history must not name what is private to the people working on it,
 * and the list of those names cannot live in the tree without publishing them. So the list lives in
 * this clone's local git config (or the environment) and the repository carries only the guard:
 *
 *   git config --add smoothbricks.publicDenylist '<pattern>'
 *
 * Each value is one Perl-compatible regex, matched case-insensitively against every text file of
 * the revisions judged and against every line of the message of each commit being published. No
 * list configured means nothing to guard — CI and every clone that never set one pass silently.
 */

import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

/** Local git config key: one pattern per value. */
export const PUBLIC_DENYLIST_CONFIG_KEY = 'smoothbricks.publicDenylist';

/** Environment source for the same list: one pattern per line. */
export const PUBLIC_DENYLIST_ENV = 'SMOOTHBRICKS_PUBLIC_DENYLIST';

export type PublicDenylistScan =
  | { readonly outcome: 'unconfigured' }
  | { readonly outcome: 'clean'; readonly patterns: number }
  /**
   * `git grep -n` rows: `<sha>:message:<line>:<text>` for a commit message, then
   * `<revision>:<path>:<line>:<text>` for a tree.
   */
  | { readonly outcome: 'denied'; readonly matches: string }
  | { readonly outcome: 'failed'; readonly reason: string };

/** What one `git grep` pass found: its rows (empty when nothing matched), or why it did not run. */
type Judged = { readonly rows: string } | { readonly failure: string };

type Environment = Readonly<Record<string, string | undefined>>;

const UNCONFIGURED: PublicDenylistScan = { outcome: 'unconfigured' };

/**
 * Judge what a push publishes, never the working tree: the message of every commit reachable from
 * `revisions`, then their trees. With `remote`, only the commits that remote's tracking branches do
 * not already hold are judged — what the push adds; without it, all the history behind `revisions`.
 * `env` is the whole environment the scan reads and runs git under.
 */
export async function scanPublicDenylist(
  root: string,
  revisions: readonly string[],
  env: Environment = process.env,
  remote?: string,
): Promise<PublicDenylistScan> {
  const config = git(root, ['config', '--get-all', PUBLIC_DENYLIST_CONFIG_KEY], env);
  // git config --get-all: 0 = values printed, 1 = key unset, anything else = config unreadable.
  if (config.exitCode > 1) {
    return { outcome: 'failed', reason: `git config --get-all ${PUBLIC_DENYLIST_CONFIG_KEY}: ${config.stderr}` };
  }
  const patterns = [...config.stdout.split('\n'), ...(env[PUBLIC_DENYLIST_ENV] ?? '').split('\n')].filter(
    (line) => line.trim().length > 0,
  );
  if (patterns.length === 0) {
    return UNCONFIGURED;
  }
  if (remote === '') {
    // `--remotes=` with an empty pattern names every remote, which would exempt a commit that only
    // some other remote holds — so a blank name is refused, not widened.
    return {
      outcome: 'failed',
      reason: 'an empty remote name would match every remote, not scope the commit messages',
    };
  }
  const search = ['-i', '-P', '-n', ...patterns.flatMap((pattern) => ['-e', pattern])];
  const messages = await judgeMessages(root, revisions, remote, search, env);
  if ('failure' in messages) {
    return { outcome: 'failed', reason: messages.failure };
  }
  const trees = grep(root, ['-I', ...search, ...revisions, '--'], env, revisions.join(' '));
  if ('failure' in trees) {
    return { outcome: 'failed', reason: trees.failure };
  }
  const matches = messages.rows + trees.rows;
  return matches === '' ? { outcome: 'clean', patterns: patterns.length } : { outcome: 'denied', matches };
}

/** The CLI and pre-push face of {@link scanPublicDenylist}: silent unconfigured, throws on refusal. */
export async function checkPublicDenylist(root: string, revisions: readonly string[], remote?: string): Promise<void> {
  const scan = await scanPublicDenylist(root, revisions, process.env, remote);
  switch (scan.outcome) {
    case 'unconfigured':
      return;
    case 'clean':
      console.log(
        `public denylist: ${scan.patterns} pattern(s), no match in ${revisions.join(' ')} or its commit messages`,
      );
      return;
    case 'denied':
      console.error(scan.matches.trimEnd());
      throw new Error(
        `Refusing: ${revisions.join(' ')} publishes text matching the public denylist (git config ${PUBLIC_DENYLIST_CONFIG_KEY} / ${PUBLIC_DENYLIST_ENV}). Reword the lines above; a commit message takes git commit --amend or a rebase.`,
      );
    case 'failed':
      throw new Error(`public denylist check did not run: ${scan.reason.trimEnd()}`);
  }
}

/**
 * `git log` names the commits and their messages; git can only match a pattern line by line inside
 * files, so each message becomes a file named by its sha in a scratch directory and the same
 * `git grep -P -i` that judges the trees judges them — one regex engine, one set of semantics.
 */
async function judgeMessages(
  root: string,
  revisions: readonly string[],
  remote: string | undefined,
  search: string[],
  env: Environment,
): Promise<Judged> {
  const unpublished = remote === undefined ? [] : ['--not', `--remotes=${remote}`];
  const log = git(root, ['log', '-z', '--format=%H%n%B', ...revisions, ...unpublished, '--'], env);
  if (log.exitCode !== 0) {
    return { failure: `git log ${revisions.join(' ')}: ${log.stderr}` };
  }
  // `-z` ends each `<sha>\n<message>` record with NUL, which a commit message cannot contain.
  const commits = log.stdout.split('\0').filter((record) => record.length > 0);
  if (commits.length === 0) {
    return { rows: '' };
  }
  const scratch = await mkdtemp(join(tmpdir(), 'smoo-public-denylist-'));
  try {
    await Promise.all(
      commits.map((record) => {
        const sha = record.slice(0, record.indexOf('\n'));
        return writeFile(join(scratch, sha), record.slice(sha.length + 1));
      }),
    );
    // -a: a message that git would call binary is still published text.
    const judged = grep(
      scratch,
      ['--no-index', '--no-exclude-standard', '-a', ...search, '--', '.'],
      env,
      'commit messages',
    );
    if ('failure' in judged) {
      return judged;
    }
    return {
      rows: judged.rows
        .split('\n')
        .map((row) => row.replace(/^[0-9a-f]+:/, '$&message:'))
        .join('\n'),
    };
  } finally {
    await rm(scratch, { recursive: true, force: true });
  }
}

function grep(cwd: string, args: string[], env: Environment, subject: string): Judged {
  const found = git(cwd, ['grep', ...args], env);
  // git grep: 0 = matches, 1 = none, anything else = the scan did not run (bad pattern, no PCRE
  // support, unknown revision) — which must refuse, or a typo would disable the guard.
  switch (found.exitCode) {
    case 0:
      return { rows: found.stdout };
    case 1:
      return { rows: '' };
    default:
      return { failure: `git grep ${subject}: ${found.stderr}` };
  }
}

// Bun.spawnSync rather than the shared runResult: the scan's environment is the whole child
// environment, not an overlay on this process's, so a caller can withhold inherited git variables.
function git(root: string, args: string[], env: Environment): { exitCode: number; stdout: string; stderr: string } {
  const child = Bun.spawnSync(['git', ...args], { cwd: root, env });
  return { exitCode: child.exitCode, stdout: child.stdout.toString(), stderr: child.stderr.toString() };
}
