/**
 * Pre-push judges published trees and messages using the clone's configured PCRE patterns.
 * Commit hooks use the same patterns and regex engine, plus locally discovered sibling projects
 * and HOME. Only added index lines are judged, so old private text can be removed incrementally.
 * A merge receiving a side proven public against the clone's declared tracking ref is judged on
 * the lines it introduces, not on the already-public lines it carries in.
 */

import { existsSync, readFileSync } from 'node:fs';
import { mkdtemp, readdir, readFile, realpath, rm, stat, writeFile } from 'node:fs/promises';
import { homedir, tmpdir } from 'node:os';
import { basename, dirname, isAbsolute, join, resolve } from 'node:path';
import typia from 'typia';

/** Local git config key: one pattern per value. */
export const PUBLIC_DENYLIST_CONFIG_KEY = 'smoothbricks.publicDenylist';

/**
 * Git config key: one pattern per value, judged only against a commit message (the commit-msg
 * hook), never against staged lines or pushed trees. A repository may commit it through its
 * included workspace git config: some text belongs in files but not in history messages.
 */
export const COMMIT_MESSAGE_DENYLIST_CONFIG_KEY = 'smoothbricks.commitMessageDenylist';

/**
 * Local git config key naming the one public tracking ref (a full `refs/remotes/...` ref) whose
 * history is already public. Clone-local on purpose: a tree cannot declare its own exemption.
 */
export const PUBLIC_TRACKING_REF_CONFIG_KEY = 'smoothbricks.publicTrackingRef';

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
const NUMERIC_REPOSITORY_NAME = /^\d+$/;

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
  const configured = configuredPatterns(root, env);
  if ('failure' in configured) {
    return { outcome: 'failed', reason: configured.failure };
  }
  const patterns = configured.patterns;
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
      standaloneGitEnvironment(env),
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

/** Tab-separated public-reference exemptions, read from the index rather than the working tree. */
export const PUBLIC_DENYLIST_ALLOWLIST_PATH = 'tooling/public-denylist-allowlist.tsv';

export type PublicCommitSubject = { readonly kind: 'staged' } | { readonly kind: 'message'; readonly path: string };

type AddedLine = { readonly path: string; readonly line: number; readonly text: string };
type Allowlist = {
  readonly scopes: readonly string[];
  readonly repositories: readonly string[];
  readonly lines: readonly { readonly path: string; readonly text: string }[];
};

const parseWorkspaceMarker = typia.json.createValidateParse<{ projectRoot: string }>();

function gitConfigPatterns(root: string, key: string, env: Environment): { patterns: string[] } | { failure: string } {
  const config = git(root, ['config', '--get-all', key], env);
  // An unset key is the only nonzero status that means no configuration.
  if (config.exitCode !== 0 && config.exitCode !== 1) {
    return { failure: `git config --get-all ${key}: ${config.stderr}` };
  }
  return { patterns: config.stdout.split('\n').filter((line) => line.trim().length > 0) };
}

function configuredPatterns(root: string, env: Environment): { patterns: string[] } | { failure: string } {
  const config = gitConfigPatterns(root, PUBLIC_DENYLIST_CONFIG_KEY, env);
  if ('failure' in config) return config;
  return {
    patterns: [
      ...config.patterns,
      ...(env[PUBLIC_DENYLIST_ENV] ?? '').split('\n').filter((line) => line.trim().length > 0),
    ],
  };
}

/** Fail closed, including when local discovery, the index, or a configured regex cannot be read. */
export async function scanPublicCommitDenylist(
  root: string,
  subject: PublicCommitSubject,
  env: Environment = process.env,
): Promise<PublicDenylistScan> {
  try {
    const shared = configuredPatterns(root, env);
    if ('failure' in shared) return { outcome: 'failed', reason: shared.failure };
    const messageOnly =
      subject.kind === 'message' ? gitConfigPatterns(root, COMMIT_MESSAGE_DENYLIST_CONFIG_KEY, env) : { patterns: [] };
    if ('failure' in messageOnly) return { outcome: 'failed', reason: messageOnly.failure };
    const configured = { patterns: [...shared.patterns, ...messageOnly.patterns] };
    const siblings = await siblingPatterns(root, env);
    if ('failure' in siblings) return { outcome: 'failed', reason: siblings.failure };
    const allowlist = readAllowlist(root, env);
    if ('failure' in allowlist) return { outcome: 'failed', reason: allowlist.failure };
    const home = env.HOME ?? homedir();
    if (!isAbsolute(home) || home === '/') {
      return { outcome: 'failed', reason: 'HOME must name an absolute home directory, not the filesystem root' };
    }
    const homePattern = `${escapeRegex(home.replace(/\/+$/, ''))}(?![\\w.-])`;
    let lines: AddedLine[];
    if (subject.kind === 'message') {
      const path = resolve(root, subject.path);
      try {
        lines = (await readFile(path, 'utf8')).split('\n').map((text, index) => ({
          path: '',
          line: index + 1,
          text,
        }));
      } catch (error) {
        return { outcome: 'failed', reason: `${path}: ${errorReason(error)}` };
      }
    } else {
      const staged = addedIndexLines(root, env);
      if ('failure' in staged) return { outcome: 'failed', reason: staged.failure };
      lines = staged.lines;
    }
    const judged = await judgeAddedLines(lines, configured.patterns, siblings.patterns, homePattern, allowlist, env);
    if ('failure' in judged) return { outcome: 'failed', reason: judged.failure };
    return judged.rows === ''
      ? { outcome: 'clean', patterns: configured.patterns.length + siblings.patterns.length + 1 }
      : { outcome: 'denied', matches: judged.rows };
  } catch (error) {
    return { outcome: 'failed', reason: errorReason(error) };
  }
}

/** Operational refusal is an exit status, not a thrown programmer error. */
export async function checkPublicCommitDenylist(root: string, subject: PublicCommitSubject): Promise<void> {
  const scan = await scanPublicCommitDenylist(root, subject);
  if (scan.outcome === 'denied') {
    console.error(scan.matches.trimEnd());
    console.error('Refusing commit: reword the added lines or commit message matching the public denylist.');
    process.exitCode = 1;
  } else if (scan.outcome === 'failed') {
    console.error(`public denylist check did not run: ${scan.reason.trimEnd()}`);
    process.exitCode = 1;
  }
}

async function siblingPatterns(root: string, env: Environment): Promise<{ patterns: string[] } | { failure: string }> {
  const markerPath = join(root, '.cowshed', 'workspace.json');
  let anchor = root;
  let markerText: string | undefined;
  try {
    markerText = await readFile(markerPath, 'utf8');
  } catch (error) {
    if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) {
      return { failure: `${markerPath}: ${errorReason(error)}` };
    }
  }
  if (markerText !== undefined) {
    try {
      const marker = parseWorkspaceMarker(markerText);
      if (!marker.success || !isAbsolute(marker.data.projectRoot)) {
        return { failure: `${markerPath}: expected an absolute projectRoot` };
      }
      anchor = marker.data.projectRoot;
    } catch (error) {
      return { failure: `${markerPath}: ${errorReason(error)}` };
    }
  }
  anchor = await realpath(anchor);
  if (!(await stat(anchor)).isDirectory()) return { failure: `${anchor}: projectRoot is not a directory` };
  const parent = dirname(anchor);
  // Git supplies GIT_DIR and GIT_INDEX_FILE to hooks. They describe the committing repository,
  // not its siblings; never let a sibling probe accidentally query the current repository.
  const probeEnv = standaloneGitEnvironment(env);
  const patterns: string[] = [];
  for (const entry of await readdir(parent, { withFileTypes: true })) {
    if (!entry.isDirectory() || entry.name.startsWith('.') || entry.name === basename(anchor)) continue;
    const sibling = join(parent, entry.name);
    try {
      await stat(join(sibling, '.git'));
    } catch (error) {
      if (error instanceof Error && 'code' in error && error.code === 'ENOENT') continue;
      return { failure: `${sibling}/.git: ${errorReason(error)}` };
    }
    const probe = git(sibling, ['rev-parse', '--show-toplevel'], probeEnv);
    if (probe.exitCode !== 0) {
      return { failure: `${sibling}: git rev-parse --show-toplevel: ${probe.stderr}` };
    }
    if ((await realpath(probe.stdout.trimEnd())) !== (await realpath(sibling))) {
      return { failure: `${sibling}: .git does not identify this sibling as a repository root` };
    }
    const boundary = NUMERIC_REPOSITORY_NAME.test(entry.name) ? '[\\w.]' : '\\w';
    patterns.push(`(?<!${boundary})${escapeRegex(entry.name)}(?!${boundary})`);
  }
  return { patterns };
}

function readAllowlist(root: string, env: Environment): Allowlist | { failure: string } {
  const listed = git(root, ['ls-files', '--stage', '--', PUBLIC_DENYLIST_ALLOWLIST_PATH], env);
  if (listed.exitCode !== 0) return { failure: `git ls-files ${PUBLIC_DENYLIST_ALLOWLIST_PATH}: ${listed.stderr}` };
  if (listed.stdout === '') return { scopes: [], repositories: [], lines: [] };
  const content = git(root, ['show', `:${PUBLIC_DENYLIST_ALLOWLIST_PATH}`], env);
  if (content.exitCode !== 0)
    return { failure: `${PUBLIC_DENYLIST_ALLOWLIST_PATH}: git show index: ${content.stderr}` };
  const scopes: string[] = [];
  const repositories: string[] = [];
  const lines: { path: string; text: string }[] = [];
  for (const [index, row] of content.stdout.split('\n').entries()) {
    if (row === '' || row.startsWith('#')) continue;
    const [kind, value, text, ...extra] = row.split('\t');
    if (
      extra.length === 0 &&
      kind === 'package-scope' &&
      text === undefined &&
      value?.match(/^@[a-z0-9][a-z0-9._-]*$/)
    ) {
      scopes.push(value);
    } else if (
      extra.length === 0 &&
      kind === 'repository' &&
      text === undefined &&
      value?.match(/^[a-zA-Z0-9][a-zA-Z0-9._-]*$/)
    ) {
      repositories.push(value);
    } else if (
      extra.length === 0 &&
      kind === 'line' &&
      value !== undefined &&
      value !== '' &&
      !isAbsolute(value) &&
      !value.split('/').includes('..') &&
      text !== undefined &&
      text !== ''
    ) {
      lines.push({ path: value, text });
    } else {
      return { failure: `${PUBLIC_DENYLIST_ALLOWLIST_PATH}:${index + 1}: invalid tab-separated public reference rule` };
    }
  }
  return { scopes, repositories, lines };
}

/**
 * The tree the staged index is diffed against, or null for git's ordinary HEAD diff. During a
 * merge, lines the incoming side carries were judged when they were published, not when they
 * are received: only when MERGE_HEAD names a single commit proven reachable from the declared
 * public tracking ref is the index diffed against that incoming tree, so new-resolution and
 * ours-only lines are still judged and already-public lines are not judged again. MERGE_HEAD
 * itself is never the anchor — a merge cannot exempt itself — so an incoming side that is not
 * proven public falls back to the HEAD diff, which judges its lines as added. A merge with no
 * declared ref, a declared ref that does not resolve, or any Git failure refuses: fail closed.
 */
function stagedDiffBase(root: string, env: Environment): { base: string | null } | { failure: string } {
  const mergeHeadPathResult = git(root, ['rev-parse', '--git-path', 'MERGE_HEAD'], env);
  if (mergeHeadPathResult.exitCode !== 0) {
    return { failure: `git rev-parse --git-path MERGE_HEAD: ${mergeHeadPathResult.stderr}` };
  }
  const mergeHeadPath = resolve(root, mergeHeadPathResult.stdout.trim());
  if (!existsSync(mergeHeadPath)) return { base: null };
  let contents: string;
  try {
    contents = readFileSync(mergeHeadPath, 'utf8');
  } catch (error) {
    return { failure: `${mergeHeadPath}: ${errorReason(error)}` };
  }
  const heads = contents
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line !== '');
  if (heads.length !== 1 || heads[0] === undefined) return { base: null };
  const incoming = heads[0];
  const declared = git(root, ['config', '--get-all', PUBLIC_TRACKING_REF_CONFIG_KEY], env);
  if (declared.exitCode === 1) {
    return {
      failure:
        `a merge is in progress but no public tracking ref is declared; set git config ${PUBLIC_TRACKING_REF_CONFIG_KEY} ` +
        'to the clone-local public tracking ref (for example refs/remotes/origin-https/main), or finish the merge without this hook',
    };
  }
  if (declared.exitCode !== 0) {
    return { failure: `git config --get-all ${PUBLIC_TRACKING_REF_CONFIG_KEY}: ${declared.stderr}` };
  }
  const refs = declared.stdout
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line !== '');
  if (refs.length !== 1 || refs[0] === undefined || !refs[0].startsWith('refs/remotes/')) {
    return {
      failure: `git config ${PUBLIC_TRACKING_REF_CONFIG_KEY} must name exactly one refs/remotes/ public tracking ref`,
    };
  }
  const ref = refs[0];
  const resolved = git(root, ['rev-parse', '--verify', '--quiet', `${ref}^{commit}`], env);
  if (resolved.exitCode !== 0) {
    return { failure: `declared public tracking ref ${ref} does not resolve to a commit` };
  }
  const ancestry = git(root, ['merge-base', '--is-ancestor', incoming, ref], env);
  if (ancestry.exitCode === 0) return { base: incoming };
  if (ancestry.exitCode === 1) return { base: null };
  return { failure: `git merge-base --is-ancestor ${incoming} ${ref}: ${ancestry.stderr}` };
}

function addedIndexLines(root: string, env: Environment): { lines: AddedLine[] } | { failure: string } {
  const diffBase = stagedDiffBase(root, env);
  if ('failure' in diffBase) return diffBase;
  // Omitting HEAD makes git handle an unborn branch as the empty tree. All commands inherit
  // GIT_INDEX_FILE, including a partial commit's temporary index.
  const base = diffBase.base === null ? [] : [diffBase.base];
  const paths = git(
    root,
    ['diff', '--cached', ...base, '--name-only', '-z', '--no-renames', '--no-ext-diff', '--'],
    env,
  );
  if (paths.exitCode !== 0) return { failure: `git diff --cached --name-only: ${paths.stderr}` };
  const lines: AddedLine[] = [];
  for (const path of paths.stdout.split('\0')) {
    if (path === '') continue;
    const diff = git(
      root,
      [
        '--literal-pathspecs',
        'diff',
        '--cached',
        ...base,
        '--no-color',
        '--no-ext-diff',
        '--no-textconv',
        '--no-renames',
        '--unified=0',
        '--',
        path,
      ],
      env,
    );
    if (diff.exitCode !== 0) return { failure: `${path}: git diff --cached: ${diff.stderr}` };
    let line = 0;
    let inHunk = false;
    for (const row of diff.stdout.split('\n')) {
      const hunk = /^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@/.exec(row);
      if (hunk) {
        line = Number(hunk[1]);
        inHunk = true;
      } else if (row.startsWith('diff --git ')) {
        inHunk = false;
      } else if (inHunk && row.startsWith('+')) {
        lines.push({ path, line, text: row.slice(1) });
        line++;
      } else if (inHunk && row.startsWith(' ')) {
        line++;
      }
    }
  }
  return { lines };
}

type Exemption =
  | { readonly wholeLine: true }
  | { readonly wholeLine: false; readonly spans: readonly { readonly start: number; readonly end: number }[] };

const WHOLE_LINE_EXEMPTION: Exemption = { wholeLine: true };

function referenceExemption(line: AddedLine, allowlist: Allowlist, references: readonly RegExp[]): Exemption {
  for (const allowed of allowlist.lines) {
    if (
      (line.path === allowed.path && line.text === allowed.text) ||
      (line.path === PUBLIC_DENYLIST_ALLOWLIST_PATH && line.text === `line\t${allowed.path}\t${allowed.text}`)
    ) {
      return WHOLE_LINE_EXEMPTION;
    }
  }
  if (line.path === PUBLIC_DENYLIST_ALLOWLIST_PATH) {
    for (const scope of allowlist.scopes) {
      if (line.text === `package-scope\t${scope}`) return WHOLE_LINE_EXEMPTION;
    }
  }
  const spans: { start: number; end: number }[] = [];
  for (const reference of references) {
    let match = reference.exec(line.text);
    while (match !== null) {
      // git's --column is a byte offset, not a JavaScript UTF-16 string index.
      const start = Buffer.byteLength(line.text.slice(0, match.index));
      spans.push({ start, end: start + Buffer.byteLength(match[0]) });
      match = reference.exec(line.text);
    }
  }
  return { wholeLine: false, spans };
}

async function judgeAddedLines(
  lines: readonly AddedLine[],
  configured: readonly string[],
  siblings: readonly string[],
  homePattern: string,
  allowlist: Allowlist,
  env: Environment,
): Promise<Judged> {
  const scratch = await mkdtemp(join(tmpdir(), 'smoo-public-denylist-'));
  const scratchEnv = standaloneGitEnvironment(env);
  try {
    // Explicit PCRE policy and HOME are authoritative on original bytes. The repo allowlist
    // exempts only automatically discovered sibling terms, never explicit clone configuration.
    await writeFile(join(scratch, 'lines'), lines.map((line) => line.text).join('\n'));
    const strict = grep(
      scratch,
      [
        '--no-index',
        '--no-exclude-standard',
        '--color=never',
        '-a',
        '-i',
        '-P',
        '-n',
        '-e',
        homePattern,
        ...configured.flatMap((pattern) => ['-e', pattern]),
        '--',
        'lines',
      ],
      scratchEnv,
      'HOME / configured added lines / message',
    );
    if ('failure' in strict) return strict;
    const matched = new Set<number>();
    if (siblings.length > 0) {
      // Every sibling regex consumes a nonempty, escaped directory name, so --only-matching
      // enumerates real byte spans without the zero-width ambiguity of arbitrary user PCRE.
      const spans = grep(
        scratch,
        [
          '--no-index',
          '--no-exclude-standard',
          '--color=never',
          '-a',
          '-i',
          '-P',
          '-n',
          '--column',
          '--only-matching',
          ...siblings.flatMap((pattern) => ['-e', pattern]),
          '--',
          'lines',
        ],
        scratchEnv,
        'sibling reference spans',
      );
      if ('failure' in spans) return spans;
      if (spans.rows !== '') {
        const references = [
          ...allowlist.scopes.map((scope) => new RegExp(`${escapeRegex(scope)}(?:/|%2[fF])[a-zA-Z0-9._-]+`, 'g')),
          ...allowlist.repositories.map((repository) => new RegExp(`(?<!\\w)${escapeRegex(repository)}(?!\\w)`, 'g')),
        ];
        const exemptions = new Map<number, Exemption>();
        const siblingRegexes = siblings.map((pattern) => new RegExp(pattern, 'gi'));
        const flagged = new Set<number>();
        for (const row of spans.rows.split('\n')) {
          if (row === '') continue;
          const found = /^lines:(\d+):(\d+):/.exec(row);
          if (!found) return { failure: `git grep returned an unrecognized match diagnostic: ${row}` };
          const index = Number(found[1]) - 1;
          if (!lines[index]) return { failure: `git grep returned an out-of-range line: ${index + 1}` };
          flagged.add(index);
        }
        for (const index of flagged) {
          const line = lines[index];
          if (!line) return { failure: `git grep returned an out-of-range line: ${index + 1}` };
          let exemption = exemptions.get(index);
          if (!exemption) {
            exemption = referenceExemption(line, allowlist, references);
            exemptions.set(index, exemption);
          }
          if (exemption.wholeLine) continue;
          // git grep's --column is unreliable for later matches on a line, so recompute
          // sibling spans with the same patterns before judging allowlist containment.
          let unexempted = false;
          for (const siblingRegex of siblingRegexes) {
            let match = siblingRegex.exec(line.text);
            while (match !== null) {
              const start = Buffer.byteLength(line.text.slice(0, match.index));
              const end = start + Buffer.byteLength(match[0]);
              if (!exemption.spans.some((allowed) => start >= allowed.start && end <= allowed.end)) unexempted = true;
              match = siblingRegex.exec(line.text);
            }
          }
          if (unexempted) matched.add(index);
        }
      }
    }
    for (const row of strict.rows.split('\n')) {
      if (row === '') continue;
      const found = /^lines:(\d+):/.exec(row);
      if (!found) return { failure: `git grep returned an unrecognized line diagnostic: ${row}` };
      matched.add(Number(found[1]) - 1);
    }
    let diagnostics = '';
    for (const index of [...matched].sort((left, right) => left - right)) {
      const line = lines[index];
      if (!line) return { failure: `git grep returned an out-of-range line: ${index + 1}` };
      diagnostics += `${line.path === '' ? 'message' : `staged:${line.path}`}:${line.line}:${line.text}\n`;
    }
    return { rows: diagnostics };
  } finally {
    await rm(scratch, { recursive: true, force: true });
  }
}

function escapeRegex(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/** Repository pointers from a hook must not redirect probes or scratch-file grep commands. */
function standaloneGitEnvironment(env: Environment): Environment {
  const standalone = { ...env };
  for (const key of ['GIT_DIR', 'GIT_WORK_TREE', 'GIT_COMMON_DIR', 'GIT_INDEX_FILE', 'GIT_PREFIX']) {
    delete standalone[key];
  }
  return standalone;
}

function errorReason(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
