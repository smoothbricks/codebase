/**
 * A public repository's tree must not name what is private to the people working on it, and the
 * list of those names cannot live in the tree without publishing them. So the list lives in this
 * clone's local git config (or the environment) and the repository carries only the guard:
 *
 *   git config --add smoothbricks.publicDenylist '<pattern>'
 *
 * Each value is one Perl-compatible regex, matched case-insensitively against every text file of
 * the revisions judged. No list configured means nothing to guard — CI and every clone that never
 * set one pass silently.
 */

/** Local git config key: one pattern per value. */
export const PUBLIC_DENYLIST_CONFIG_KEY = 'smoothbricks.publicDenylist';

/** Environment source for the same list: one pattern per line. */
export const PUBLIC_DENYLIST_ENV = 'SMOOTHBRICKS_PUBLIC_DENYLIST';

export type PublicDenylistScan =
  | { readonly outcome: 'unconfigured' }
  | { readonly outcome: 'clean'; readonly patterns: number }
  /** `git grep -n` rows, `<revision>:<path>:<line>:<text>`. */
  | { readonly outcome: 'denied'; readonly matches: string }
  | { readonly outcome: 'failed'; readonly reason: string };

type Environment = Readonly<Record<string, string | undefined>>;

const UNCONFIGURED: PublicDenylistScan = { outcome: 'unconfigured' };

/**
 * Judge the trees of `revisions` — what a push publishes, never the working tree. `env` is the
 * whole environment the scan reads and runs git under.
 */
export async function scanPublicDenylist(
  root: string,
  revisions: readonly string[],
  env: Environment = process.env,
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
  const grep = git(
    root,
    ['grep', '-I', '-i', '-P', '-n', ...patterns.flatMap((pattern) => ['-e', pattern]), ...revisions, '--'],
    env,
  );
  // git grep: 0 = matches, 1 = none, anything else = the scan did not run (bad pattern, no PCRE
  // support, unknown revision) — which must refuse, or a typo would disable the guard.
  switch (grep.exitCode) {
    case 0:
      return { outcome: 'denied', matches: grep.stdout };
    case 1:
      return { outcome: 'clean', patterns: patterns.length };
    default:
      return { outcome: 'failed', reason: `git grep ${revisions.join(' ')}: ${grep.stderr}` };
  }
}

/** The CLI and pre-push face of {@link scanPublicDenylist}: silent unconfigured, throws on refusal. */
export async function checkPublicDenylist(root: string, revisions: readonly string[]): Promise<void> {
  const scan = await scanPublicDenylist(root, revisions);
  switch (scan.outcome) {
    case 'unconfigured':
      return;
    case 'clean':
      console.log(`public denylist: ${scan.patterns} pattern(s), no match in ${revisions.join(' ')}`);
      return;
    case 'denied':
      console.error(scan.matches.trimEnd());
      throw new Error(
        `Refusing: ${revisions.join(' ')} contains text matching the public denylist (git config ${PUBLIC_DENYLIST_CONFIG_KEY} / ${PUBLIC_DENYLIST_ENV}). Reword the lines above.`,
      );
    case 'failed':
      throw new Error(`public denylist check did not run: ${scan.reason.trimEnd()}`);
  }
}

// Bun.spawnSync rather than the shared runResult: the scan's environment is the whole child
// environment, not an overlay on this process's, so a caller can withhold inherited git variables.
function git(root: string, args: string[], env: Environment): { exitCode: number; stdout: string; stderr: string } {
  const child = Bun.spawnSync(['git', ...args], { cwd: root, env });
  return { exitCode: child.exitCode, stdout: child.stdout.toString(), stderr: child.stderr.toString() };
}
