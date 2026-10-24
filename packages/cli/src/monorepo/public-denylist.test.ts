import { afterAll, beforeAll, describe, expect, it } from 'bun:test';
import { readFileSync } from 'node:fs';
import { cp, mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { mergeEnv } from '../lib/run.js';
import {
  PUBLIC_DENYLIST_ALLOWLIST_PATH,
  type PublicDenylistScan,
  scanPublicCommitDenylist,
  scanPublicDenylist,
} from './public-denylist.js';

/**
 * The environment the check and the fixture's own git commands run under: the fixture repository's
 * config is the only config, so a denylist in the developer's global or system config cannot decide
 * these results, and inherited `GIT_DIR`-style variables (set when the suite runs under a hook or
 * `rebase --exec`) cannot point git at the repository running the tests.
 */
function environment(denylist?: string): Record<string, string> {
  const env = mergeEnv({ GIT_CONFIG_GLOBAL: '/dev/null', GIT_CONFIG_SYSTEM: '/dev/null' }, [
    'GIT_DIR',
    'GIT_WORK_TREE',
    'GIT_INDEX_FILE',
    'GIT_COMMON_DIR',
    'SMOOTHBRICKS_PUBLIC_DENYLIST',
  ]);
  if (denylist !== undefined) {
    env.SMOOTHBRICKS_PUBLIC_DENYLIST = denylist;
  }
  return env;
}

/** The HOME the scans run under when HOME is the subject: an absolute path that is no real directory. */
const FIXTURE_HOME = '/denylist-fixture/home';

/**
 * A scratch container holding the repository under test (`root`); a sibling project is a directory beside it.
 * Every suite builds its fixture once, in `beforeAll`, and its scans only read it. A case that must change a
 * repository takes a copy of one: a file copy costs milliseconds where building the repository costs a half dozen
 * git processes.
 */
interface Workspace {
  readonly container: string;
  readonly root: string;
}

const scratchContainers: string[] = [];
let template: Workspace;

async function scratch(): Promise<Workspace> {
  // Real path: git reports the resolved root, and macOS's temporary directory is a symlink.
  const container = await realpath(await mkdtemp(join(tmpdir(), 'smoo-public-denylist-')));
  scratchContainers.push(container);
  return { container, root: join(container, 'repository') };
}

function runGit(cwd: string, env: Record<string, string>, ...args: string[]): string {
  const child = Bun.spawnSync(['git', ...args], { cwd, env });
  if (child.exitCode !== 0) {
    throw new Error(`git ${args.join(' ')} failed: ${child.stderr.toString()}`);
  }
  return child.stdout.toString().trim();
}

function git(ws: Workspace, ...args: string[]): string {
  return runGit(ws.root, environment(), ...args);
}

beforeAll(async () => {
  template = await scratch();
  await mkdir(template.root);
  git(template, 'init', '--quiet', '--initial-branch=main');
  git(template, 'config', 'user.email', 'denylist@example.test');
  git(template, 'config', 'user.name', 'Denylist Test');
  await writeFile(join(template.root, 'README.md'), 'A neutral readme.\n');
  git(template, 'add', 'README.md');
  git(template, 'commit', '--quiet', '-m', 'add README.md');
});

afterAll(async () => {
  await Promise.all(scratchContainers.map((container) => rm(container, { recursive: true, force: true })));
});

/** A new workspace holding a file copy of `source`'s repository, sharing nothing with it. */
async function copyOf(source: Workspace): Promise<Workspace> {
  const ws = await scratch();
  await cp(source.root, ws.root, { recursive: true });
  return ws;
}

/** A sibling project: a git root of its own beside the repository, copied from the template. */
async function sibling(ws: Workspace, name = 'secret-consumer'): Promise<string> {
  const path = join(ws.container, name);
  await cp(template.root, path, { recursive: true });
  return path;
}

/** Write each file into the repository and stage it. */
async function stage(ws: Workspace, files: Readonly<Record<string, string>>): Promise<void> {
  for (const [path, text] of Object.entries(files)) {
    await mkdir(dirname(join(ws.root, path)), { recursive: true });
    await writeFile(join(ws.root, path), text);
  }
  git(ws, 'add', '--', ...Object.keys(files));
}

const IDENTITY = 'Denylist Test <denylist@example.test> 0 +0000';

/** One commit for {@link importHistory}: its `files` are added to the tree of `from`, or of the branch's tip. */
interface ImportedCommit {
  readonly branch: string;
  readonly from?: string;
  readonly message: string;
  readonly files?: Readonly<Record<string, string>>;
}

/**
 * Write commits and ref pointers into the repository with one `git fast-import`, where a checkout, add, commit and
 * rev-parse per commit would each spawn git. The working tree and index stay as they are. Returns a lookup from each
 * commit to the sha git gave it.
 */
async function importHistory(
  ws: Workspace,
  commits: readonly ImportedCommit[],
  pointers: Readonly<Record<string, string>> = {},
): Promise<(commit: ImportedCommit) => string> {
  const data = (text: string): string => `data ${Buffer.byteLength(text)}\n${text}`;
  const stream = [
    ...commits.map((commit, index) =>
      [
        `commit refs/heads/${commit.branch}`,
        `mark :${index + 1}`,
        `committer ${IDENTITY}`,
        data(`${commit.message}\n`),
        ...(commit.from === undefined ? [] : [`from ${commit.from}`]),
        ...Object.entries(commit.files ?? {}).flatMap(([path, text]) => [`M 100644 inline ${path}`, data(text)]),
      ].join('\n'),
    ),
    ...Object.entries(pointers).map(([ref, target]) => `reset ${ref}\nfrom ${target}`),
  ].join('\n\n');
  const marks = join(ws.container, 'fast-import.marks');
  const child = Bun.spawnSync(['git', 'fast-import', '--quiet', `--export-marks=${marks}`], {
    cwd: ws.root,
    env: environment(),
    stdin: Buffer.from(`${stream}\n`),
  });
  if (child.exitCode !== 0) {
    throw new Error(`git fast-import failed: ${child.stderr.toString()}`);
  }
  const shas = new Map<ImportedCommit, string>();
  for (const row of (await readFile(marks, 'utf8')).split('\n')) {
    const [mark = '', sha = ''] = row.split(' ');
    const commit = commits[Number(mark.slice(1)) - 1];
    if (commit !== undefined && sha !== '') shas.set(commit, sha);
  }
  return (commit) => {
    const sha = shas.get(commit);
    if (sha === undefined) throw new Error(`commit ${commit.message} was not imported`);
    return sha;
  };
}

const MAIN = 'refs/heads/main';

/** A commit on top of main, on a branch of its own. */
function onMain(branch: string, message: string, files?: Readonly<Record<string, string>>): ImportedCommit {
  return { branch, from: MAIN, message, files };
}

/** `env` as a hook sees it: git exports the committing repository's own pointers. */
function hookEnvironment(ws: Workspace, env = environment()): Record<string, string> {
  return {
    ...env,
    GIT_DIR: join(ws.root, '.git'),
    GIT_WORK_TREE: ws.root,
    GIT_COMMON_DIR: join(ws.root, '.git'),
    GIT_INDEX_FILE: join(ws.root, '.git', 'index'),
  };
}

function scanStaged(ws: Workspace, env = environment()): Promise<PublicDenylistScan> {
  return scanPublicCommitDenylist(ws.root, { kind: 'staged' }, env);
}

async function scanMessage(ws: Workspace, text: string, env = environment()): Promise<PublicDenylistScan> {
  const path = join(ws.root, '.git', 'COMMIT_EDITMSG');
  await writeFile(path, text);
  return scanPublicCommitDenylist(ws.root, { kind: 'message', path }, env);
}

/** The rows a scan denied, none when it is clean. A scan that did not judge fails the test with its outcome. */
function deniedRows(scan: PublicDenylistScan): readonly string[] {
  switch (scan.outcome) {
    case 'denied':
      return scan.matches.split('\n').filter((row) => row !== '');
    case 'clean':
      return [];
    case 'unconfigured':
    case 'failed':
      throw new Error(`the scan did not judge: ${JSON.stringify(scan)}`);
  }
}

/** Why a scan refused to run. A scan that did anything else fails the test with its outcome. */
function failureOf(scan: PublicDenylistScan): string {
  if (scan.outcome !== 'failed') throw new Error(`the scan should have failed closed: ${JSON.stringify(scan)}`);
  return scan.reason;
}

/** One line of a staged file or a commit message, and whether the scan under test must deny it. */
interface Line {
  readonly text: string;
  readonly denied: boolean;
}

/** A behaviour, shown by the lines it makes a scan deny and the lines it leaves alone. */
interface LineCase {
  readonly name: string;
  readonly lines: readonly Line[];
}

/** A file whose lines are `lines`. */
function fileOf(lines: readonly Line[]): string {
  return `${lines.map((line) => line.text).join('\n')}\n`;
}

/** The rows of `rows` that name `label`: `staged:<path>` for staged lines, `message` for a commit message. */
function rowsUnder(rows: readonly string[], label: string): readonly string[] {
  return rows.filter((row) => row.startsWith(`${label}:`));
}

/**
 * Each of `checked` is denied in `rows` exactly when it says so, as exactly its own row. A line is named by its
 * position in `file`, the lines of the one file whose scan produced `rows`.
 */
function expectLines(rows: readonly string[], label: string, file: readonly Line[], checked: readonly Line[]): void {
  for (const line of checked) {
    const index = file.indexOf(line);
    if (index < 0) throw new Error(`${line.text} is not a line of the file`);
    const prefix = `${label}:${index + 1}:`;
    expect(rows.filter((row) => row.startsWith(prefix))).toEqual(line.denied ? [`${prefix}${line.text}`] : []);
  }
}

describe('public denylist scan', () => {
  let ws: Workspace;
  let shaOf: (commit: ImportedCommit) => string;

  // Each commit sits on its own branch, so a scan judges only the history its case builds.
  const tree = onMain('tree', 'add notes.md', { 'notes.md': 'first line\nWe borrowed this from Secret-Codename.\n' });
  const side = onMain('side', 'add notes.md', { 'notes.md': 'secret-codename\n' });
  const deniedMessage = onMain('message-denied', 'tidy the adapters\n\nMoves the Acme-Secret adapter\nout of core.');
  const cleanMessage = onMain('message-clean', 'tidy the adapters\n\nNo private names here.');
  const pcreMessage = onMain('message-pcre', 'rename Acme\tSecret');
  const messageThenTree = onMain('message-then-tree', 'document acme-secret', { 'notes.md': 'acme-secret\n' });
  const old = onMain('behind', 'mention acme-secret');
  const tip: ImportedCommit = { branch: 'behind', message: 'a clean follow-up' };
  const published = onMain('published', 'mention acme-secret');
  const followUp: ImportedCommit = { branch: 'follow-up', from: 'refs/heads/published', message: 'a clean follow-up' };
  const unpublished = onMain('unpublished', 'mention acme-secret');

  /** A scan of this suite's repository under the denylist `denylist`, a pattern per line. */
  const scan = (revisions: readonly string[], denylist?: string, remote?: string) =>
    scanPublicDenylist(ws.root, revisions, environment(denylist), remote);

  beforeAll(async () => {
    ws = await copyOf(template);
    // Never committed: a scan judges revisions, not the working tree.
    await writeFile(join(ws.root, 'README.md'), 'an uncommitted secret-codename edit\n');
    shaOf = await importHistory(
      ws,
      [
        tree,
        side,
        deniedMessage,
        cleanMessage,
        pcreMessage,
        messageThenTree,
        old,
        tip,
        published,
        followUp,
        unpublished,
      ],
      // `origin` holds the published commit; `upstream` holds everything up to main.
      { 'refs/remotes/origin/main': 'refs/heads/published', 'refs/remotes/upstream/main': MAIN },
    );
  });

  it('denies a tree that contains a configured word in any casing, naming revision, file and line', async () => {
    expect(await scan(['tree'], 'secret-codename\nother-codename')).toEqual({
      outcome: 'denied',
      matches: 'tree:notes.md:2:We borrowed this from Secret-Codename.\n',
    });
  });

  it('passes a tree that contains none of the configured words', async () => {
    expect(await scan(['main'], 'secret-codename')).toEqual({ outcome: 'clean', patterns: 1 });
  });

  it('is unconfigured, not a match, when no denylist exists, whatever the tree contains', async () => {
    expect(await scan(['tree'])).toEqual({ outcome: 'unconfigured' });
  });

  it('reads one pattern per line of SMOOTHBRICKS_PUBLIC_DENYLIST alongside the git config', async () => {
    // The one case that configures the repository itself: every other scan reads patterns from the environment.
    const own = await copyOf(template);
    git(own, 'config', '--add', 'smoothbricks.publicDenylist', 'config-codename');
    await importHistory(own, [
      onMain('second', 'add notes.md', { 'notes.md': 'only the second-codename appears here\n' }),
    ]);

    expect(await scanPublicDenylist(own.root, ['second'], environment('first-codename\nsecond-codename\n'))).toEqual({
      outcome: 'denied',
      matches: 'second:notes.md:1:only the second-codename appears here\n',
    });
    expect(await scanPublicDenylist(own.root, ['second'], environment('first-codename\n\n'))).toEqual({
      outcome: 'clean',
      patterns: 2,
    });
  });

  it('judges the given revisions, not the working tree', async () => {
    expect(await scan(['main'], 'secret-codename')).toEqual({ outcome: 'clean', patterns: 1 });
    expect(await scan(['main', shaOf(side)], 'secret-codename')).toEqual({
      outcome: 'denied',
      matches: `${shaOf(side)}:notes.md:1:secret-codename\n`,
    });
  });

  it('denies a commit message that contains a configured word, naming sha and line', async () => {
    expect(await scan(['message-denied'], 'acme-secret')).toEqual({
      outcome: 'denied',
      matches: `${shaOf(deniedMessage)}:message:3:Moves the Acme-Secret adapter\n`,
    });
  });

  it('passes commit messages that contain none of the configured words', async () => {
    expect(await scan(['message-clean'], 'acme-secret')).toEqual({ outcome: 'clean', patterns: 1 });
  });

  it('judges a commit message with the same PCRE as a tree', async () => {
    // \h is PCRE's horizontal space; JavaScript reads it as a literal "h".
    expect(await scan(['message-pcre'], 'acme\\hsecret')).toEqual({
      outcome: 'denied',
      matches: `${shaOf(pcreMessage)}:message:1:rename Acme\tSecret\n`,
    });
  });

  it('reports a commit message before the tree it published', async () => {
    expect(await scan(['message-then-tree'], 'acme-secret')).toEqual({
      outcome: 'denied',
      matches: `${shaOf(messageThenTree)}:message:1:document acme-secret\nmessage-then-tree:notes.md:1:acme-secret\n`,
    });
  });

  it('judges every commit behind the revisions, not only the tip', async () => {
    expect(await scan(['behind'], 'acme-secret')).toEqual({
      outcome: 'denied',
      matches: `${shaOf(old)}:message:1:mention acme-secret\n`,
    });
  });

  it('given a remote, judges only the commits that remote does not hold', async () => {
    expect(await scan(['follow-up'], 'acme-secret', 'origin')).toEqual({ outcome: 'clean', patterns: 1 });
    // A commit another remote holds is still unpublished to this one.
    expect(await scan(['follow-up'], 'acme-secret', 'private')).toEqual({
      outcome: 'denied',
      matches: `${shaOf(published)}:message:1:mention acme-secret\n`,
    });
  });

  it('judges an unpublished commit message even when the remote holds its parents', async () => {
    expect(await scan(['unpublished'], 'acme-secret', 'upstream')).toEqual({
      outcome: 'denied',
      matches: `${shaOf(unpublished)}:message:1:mention acme-secret\n`,
    });
  });

  it.each([
    {
      name: 'fails instead of passing when a configured pattern is not a valid regex',
      revisions: ['main'],
      denylist: '(unclosed',
      remote: undefined,
      reason: 'missing closing parenthesis',
    },
    {
      name: 'refuses an empty remote name instead of reading it as every remote',
      revisions: ['main'],
      denylist: 'acme-secret',
      remote: '',
      reason: 'empty remote name',
    },
    {
      name: 'fails on a bad pattern even when the remote already holds every commit',
      revisions: ['main'],
      denylist: '(unclosed',
      remote: 'upstream',
      reason: 'missing closing parenthesis',
    },
    {
      name: 'fails instead of passing when a revision does not resolve',
      revisions: ['no-such-revision'],
      denylist: 'acme-secret',
      remote: undefined,
      reason: 'no-such-revision',
    },
  ])('$name', async ({ revisions, denylist, remote, reason }) => {
    expect(failureOf(await scan(revisions, denylist, remote))).toContain(reason);
  });

  it('isolates the scratch grep of commit messages from the committing repository pointers', async () => {
    const hook = hookEnvironment(ws, environment('secret-codename\nacme-secret'));

    expect(await scanPublicDenylist(ws.root, ['tree', 'message-denied'], hook)).toEqual({
      outcome: 'denied',
      matches: `${shaOf(deniedMessage)}:message:3:Moves the Acme-Secret adapter\ntree:notes.md:2:We borrowed this from Secret-Codename.\n`,
    });
  });
});

describe('commit-time public denylist', () => {
  describe('sibling projects', () => {
    const cases: readonly LineCase[] = [
      {
        name: 'discovers sibling git roots, ignores plain dirs, dot dirs, files and its own basename',
        lines: [{ text: 'tooling packages repository .hidden-consumer', denied: false }],
      },
      {
        name: 'discovers sibling Git worktrees with a .git file',
        lines: [{ text: 'linked-consumer', denied: true }],
      },
      {
        name: 'matches sibling names case-insensitively as whole words, never substrings',
        lines: [
          { text: 'prefixsecret-consumer secret-consumers secret-consumer_suffix', denied: false },
          { text: 'SECRET-CONSUMER', denied: true },
        ],
      },
      {
        name: 'keeps the term of a three-character sibling and drops the term of a two-character one',
        lines: [
          { text: 'ab', denied: false },
          { text: 'ab and ABC', denied: true },
        ],
      },
      {
        name: 'regex-escapes sibling names instead of interpreting their punctuation',
        lines: [
          { text: 'plus-consumer111', denied: false },
          { text: 'plus-consumer+(1)', denied: true },
        ],
      },
    ];
    const notes = cases.flatMap((row) => row.lines);
    const numbers: readonly Line[] = [
      { text: '@@ -379,36 +384,111 @@', denied: false },
      { text: '/Dev/111/x', denied: false },
      { text: '111 project', denied: false },
    ];

    let ws: Workspace;
    let scan: PublicDenylistScan;

    beforeAll(async () => {
      ws = await copyOf(template);
      // An enclosing git repository must not turn ordinary sibling folders into projects.
      runGit(ws.container, environment(), 'init', '--quiet');
      await mkdir(join(ws.container, 'tooling'));
      await writeFile(join(ws.container, 'packages'), 'ordinary file');
      // Git roots, but no projects: a dot directory, two characters, only digits.
      for (const name of ['.hidden-consumer', 'ab', '111']) await sibling(ws, name);
      // Projects: whole-word terms, one of them with regex punctuation in its name.
      for (const name of ['secret-consumer', 'plus-consumer+(1)', 'abc']) await sibling(ws, name);
      // Only digits, so no term and no probe: a broken .git there must not fail the scan.
      await mkdir(join(ws.container, '222'));
      await writeFile(join(ws.container, '222', '.git'), 'not a gitfile\n');
      git(ws, 'worktree', 'add', '--quiet', '--detach', join(ws.container, 'linked-consumer'), 'HEAD');
      await stage(ws, { 'notes.txt': fileOf(notes), 'numbers.txt': fileOf(numbers) });
      scan = await scanStaged(ws);
    });

    it.each(cases)('$name', ({ lines }) => {
      expectLines(deniedRows(scan), 'staged:notes.txt', notes, lines);
    });

    it('derives no term from a sibling named only with digits, so hunk headers and numbers pass', async () => {
      expectLines(deniedRows(scan), 'staged:numbers.txt', numbers, numbers);
      // Four terms and HOME: the digits-only, two-character and dot-directory siblings add none.
      expect(await scanMessage(ws, 'fix: refresh the @@ -379,36 +384,111 @@ hunk\n\n/Dev/111/x\n')).toEqual({
        outcome: 'clean',
        patterns: 5,
      });
    });

    it('does not probe a sibling whose name yields no term, even with a broken .git', () => {
      expect(scan.outcome).toBe('denied');
    });

    it('still judges a number through an explicitly configured pattern beside a digits-only sibling', async () => {
      const explicit = deniedRows(await scanStaged(ws, environment('111')));

      expect(rowsUnder(explicit, 'staged:numbers.txt')).toEqual(
        numbers.map((line, index) => `staged:numbers.txt:${index + 1}:${line.text}`),
      );
    });

    it('isolates sibling probes and scratch grep from the committing repository pointers', async () => {
      expect(deniedRows(await scanStaged(ws, hookEnvironment(ws)))).toEqual(deniedRows(scan));
    });
  });

  describe('the adopted workspace marker', () => {
    let ws: Workspace;
    const marker = () => join(ws.root, '.cowshed', 'workspace.json');

    /** The marker as a file with `text`; a directory is made by the case itself. */
    async function writeMarker(text: string): Promise<void> {
      await rm(marker(), { recursive: true, force: true });
      await mkdir(dirname(marker()), { recursive: true });
      await writeFile(marker(), text);
    }

    beforeAll(async () => {
      ws = await copyOf(template);
    });

    it('uses the adopted projectRoot, never neighboring shed mount names', async () => {
      const adoptedParent = join(ws.container, 'adopted');
      const adopted = join(adoptedParent, 'repository');
      await mkdir(adopted, { recursive: true });
      await cp(template.root, join(adoptedParent, 'secret-consumer'), { recursive: true });
      await sibling(ws, 'other-workspace');
      await writeMarker(JSON.stringify({ projectRoot: adopted, workspace: 'example' }));
      await stage(ws, { 'notes.txt': 'other-workspace\nSecret-Consumer\n' });

      expect(await scanStaged(ws)).toEqual({ outcome: 'denied', matches: 'staged:notes.txt:2:Secret-Consumer\n' });
    });

    it('fails closed for malformed workspace markers rather than using shed neighbors', async () => {
      for (const text of ['{broken', '{"projectRoot":123}', '{"projectRoot":"relative"}']) {
        await writeMarker(text);

        expect(failureOf(await scanStaged(ws))).toContain(marker());
      }
    });

    it('fails closed for filesystem errors and missing adopted anchors', async () => {
      await rm(marker(), { recursive: true, force: true });
      await mkdir(marker(), { recursive: true });
      expect(failureOf(await scanStaged(ws))).toContain('workspace.json');

      await writeMarker(JSON.stringify({ projectRoot: join(ws.container, 'missing') }));
      expect(failureOf(await scanStaged(ws))).toContain('missing');
    });
  });

  describe('the staged diff', () => {
    // Every case stages its own files beside the others', so one scan judges them all.
    let ws: Workspace;
    let rows: readonly string[];

    beforeAll(async () => {
      ws = await copyOf(template);
      await sibling(ws);
      const files = {
        'context.txt': 'secret-consumer\nkeep secret-consumer as old context\nneutral\n',
        'removed.txt': 'secret-consumer\n',
        'old.txt': 'secret-consumer\n',
        'notes[1]: extra.txt': 'first\nsecond\nthird\nfourth\nfifth\n',
      };
      for (const [path, text] of Object.entries(files)) await writeFile(join(ws.root, path), text);
      await symlink('neutral-target', join(ws.root, 'secret-consumer.txt'));
      git(ws, 'add', '--', ...Object.keys(files), 'secret-consumer.txt');
      git(ws, 'commit', '--quiet', '-m', 'base content');

      await stage(ws, {
        'context.txt': 'keep secret-consumer as old context\nnew neutral line\n',
        'notes[1]: extra.txt': 'secret-consumer\nsecond\nthird\nfourth\nSecret-Consumer\n',
        message: 'secret-consumer\n',
        'staged-neutral.txt': 'neutral\n',
        'staged-secret.txt': 'secret-consumer\n',
      });
      git(ws, 'rm', '--quiet', 'removed.txt');
      git(ws, 'mv', 'old.txt', 'new.txt');
      await rm(join(ws.root, 'secret-consumer.txt'));
      await stage(ws, { 'secret-consumer.txt': 'neutral text\n' });
      // The staged snapshot is what is judged: not this, and not a file git was never told about.
      await writeFile(join(ws.root, 'staged-neutral.txt'), 'secret-consumer\n');
      await writeFile(join(ws.root, 'staged-secret.txt'), 'neutral again\n');
      await writeFile(join(ws.root, 'untracked.txt'), 'secret-consumer\n');
      rows = deniedRows(await scanStaged(ws));
    });

    /** What each case expects of the rows under the files it staged: none, or exactly these rows. */
    interface DiffCase {
      readonly name: string;
      readonly denied: Readonly<Record<string, readonly string[]>>;
    }
    const cases: readonly DiffCase[] = [
      {
        name: 'allows removal and unchanged context containing old private text',
        denied: { 'context.txt': [], 'removed.txt': [] },
      },
      {
        name: 'judges the staged snapshot, not the working tree or untracked files',
        denied: {
          'staged-neutral.txt': [],
          'untracked.txt': [],
          'staged-secret.txt': ['staged:staged-secret.txt:1:secret-consumer'],
        },
      },
      {
        name: 'does not exempt renamed copies of old private lines as unchanged content',
        denied: { 'old.txt': [], 'new.txt': ['staged:new.txt:1:secret-consumer'] },
      },
      {
        name: 'reports real added line numbers across multiple diff hunks and literal pathspec names',
        denied: {
          'notes[1]: extra.txt': [
            'staged:notes[1]: extra.txt:1:secret-consumer',
            'staged:notes[1]: extra.txt:5:Secret-Consumer',
          ],
        },
      },
      {
        name: 'does not scan type-change patch headers or confuse a staged file named message with a commit message',
        denied: { 'secret-consumer.txt': [], message: ['staged:message:1:secret-consumer'] },
      },
    ];
    const under = (path: string) => rowsUnder(rows, `staged:${path}`);

    it.each(cases)('$name', ({ denied }) => {
      for (const [path, expected] of Object.entries(denied)) expect(under(path)).toEqual(expected);
    });

    it('judges nothing else', () => {
      expect(rows).toHaveLength(cases.flatMap((row) => Object.values(row.denied).flat()).length);
    });

    it('handles an initial commit with no HEAD', async () => {
      const unborn = await copyOf(template);
      git(unborn, 'update-ref', '-d', 'refs/heads/main');
      await sibling(unborn);
      await stage(unborn, { 'notes.txt': 'secret-consumer\n' });

      expect(await scanStaged(unborn)).toEqual({ outcome: 'denied', matches: 'staged:notes.txt:1:secret-consumer\n' });
    });

    it('respects GIT_INDEX_FILE without inspecting or changing the real index', async () => {
      const env = { ...environment(), GIT_INDEX_FILE: join(ws.container, 'alternate-index') };
      runGit(ws.root, env, 'read-tree', 'HEAD');
      await writeFile(join(ws.root, 'alternate.txt'), 'secret-consumer\n');
      runGit(ws.root, env, 'add', 'alternate.txt');

      expect(await scanStaged(ws, env)).toEqual({
        outcome: 'denied',
        matches: 'staged:alternate.txt:1:secret-consumer\n',
      });
      expect(under('alternate.txt')).toEqual([]);
      expect(git(ws, 'ls-files', '--', 'alternate.txt')).toBe('');
    });
  });

  describe('a merge in progress', () => {
    const publicRef = 'refs/remotes/origin-https/main';
    const publicSide = onMain('public-side', 'add notes.txt', { 'notes.txt': `${FIXTURE_HOME}/project\n` });
    const privateSide = onMain('private-side', 'add notes.txt', { 'notes.txt': 'secret-consumer\n' });
    const otherPublic = onMain('other-public', 'add public.txt', { 'public.txt': 'neutral public line\n' });
    const neutralSide = onMain('neutral-side', 'add notes.txt', { 'notes.txt': 'neutral\n' });

    function declarePublicRef(ws: Workspace, sha?: string): void {
      git(ws, 'config', 'smoothbricks.publicTrackingRef', publicRef);
      if (sha !== undefined) git(ws, 'update-ref', publicRef, sha);
    }

    /** The merges the scans below read: three repositories, each left mid-merge and never changed again. */
    type MergeState = 'unproven' | 'proven' | 'resolved';
    interface MergeCase {
      readonly name: string;
      readonly state: MergeState;
      readonly env: () => Record<string, string>;
      readonly denied: readonly string[];
    }
    const cases: readonly MergeCase[] = [
      {
        name: 'cannot launder private lines through a merge whose incoming side is not proven public',
        state: 'unproven',
        env: () => environment(),
        denied: ['staged:notes.txt:1:secret-consumer'],
      },
      {
        name: 'accepts already-public lines a proven-public merge carries in, without judging them again',
        state: 'proven',
        env: () => ({ ...environment(), HOME: FIXTURE_HOME }),
        denied: [],
      },
      {
        name: 'still refuses new HOME, sibling and custom-pattern lines the merge resolution introduces',
        state: 'resolved',
        env: () => ({ ...environment('secret-codename'), HOME: FIXTURE_HOME }),
        denied: [
          `staged:notes.txt:2:${FIXTURE_HOME}/other`,
          'staged:notes.txt:3:secret-consumer',
          'staged:notes.txt:4:Secret-Codename',
        ],
      },
    ];

    let base: Workspace;
    let merges: Readonly<Record<MergeState, Workspace>>;
    let shaOf: (commit: ImportedCommit) => string;

    beforeAll(async () => {
      base = await copyOf(template);
      shaOf = await importHistory(base, [publicSide, privateSide, otherPublic, neutralSide]);
      // The incoming side is not proven public: the declared ref holds some other public commit.
      const unproven = await copyOf(base);
      await sibling(unproven);
      declarePublicRef(unproven, shaOf(otherPublic));
      git(unproven, 'merge', '--quiet', '--no-commit', '--no-ff', 'private-side');
      // The incoming side is proven public: the declared ref holds it.
      const proven = await copyOf(base);
      declarePublicRef(proven, shaOf(publicSide));
      git(proven, 'merge', '--quiet', '--no-commit', '--no-ff', 'public-side');
      // The same merge, resolved with lines the incoming side does not carry.
      const resolved = await copyOf(proven);
      await sibling(resolved);
      await stage(resolved, {
        'notes.txt': `${FIXTURE_HOME}/project\n${FIXTURE_HOME}/other\nsecret-consumer\nSecret-Codename\n`,
      });
      merges = { unproven, proven, resolved };
    });

    it.each(cases)('$name', async ({ state, env, denied }) => {
      expect(deniedRows(await scanStaged(merges[state], env()))).toEqual(denied);
    });

    it('fails closed during a merge when the declared public ref is missing, undeclared or malformed', async () => {
      const ws = await copyOf(base);
      git(ws, 'merge', '--quiet', '--no-commit', '--no-ff', 'neutral-side');
      expect((await scanStaged(ws)).outcome).toBe('failed');
      git(ws, 'config', 'smoothbricks.publicTrackingRef', publicRef);
      expect((await scanStaged(ws)).outcome).toBe('failed');
      git(ws, 'config', 'smoothbricks.publicTrackingRef', 'main');
      expect((await scanStaged(ws)).outcome).toBe('failed');
      declarePublicRef(ws, shaOf(neutralSide));
      expect((await scanStaged(ws)).outcome).toBe('clean');
    });

    it('keeps GIT_INDEX_FILE authoritative for a receiving merge and leaves the real index alone', async () => {
      const { proven } = merges;
      const alternate = join(proven.container, 'merge-index');
      await writeFile(alternate, await readFile(join(proven.root, '.git', 'index')));
      const env = { ...environment(), HOME: FIXTURE_HOME, GIT_INDEX_FILE: alternate };

      expect((await scanStaged(proven, env)).outcome).toBe('clean');
      expect(git(proven, 'show', ':notes.txt')).toBe(`${FIXTURE_HOME}/project`);
    });
  });

  describe('configured patterns, HOME and commit messages', () => {
    const stagedCases: readonly LineCase[] = [
      {
        name: 'reads configured PCRE patterns from config and environment in staged mode',
        lines: [
          { text: 'Secret\tConsumer', denied: true },
          { text: 'Env-Codename', denied: true },
        ],
      },
      {
        name: 'rejects the actual HOME prefix, not neutral user examples or longer home basenames',
        lines: [
          { text: '/Users/example/Dev/project', denied: false },
          { text: `${FIXTURE_HOME}-other/project`, denied: false },
          { text: `${FIXTURE_HOME}/project`, denied: true },
          { text: FIXTURE_HOME, denied: true },
        ],
      },
    ];
    const messageCases: readonly LineCase[] = [
      {
        name: 'checks commit message lines with the same PCRE and sibling discovery',
        lines: [
          { text: 'fix: neutral summary', denied: false },
          { text: '', denied: false },
          { text: 'Secret-Consumer', denied: true },
          { text: 'secret\tconsumer', denied: true },
        ],
      },
      {
        name: 'rejects the actual HOME prefix in a commit message',
        lines: [{ text: `${FIXTURE_HOME}/project`, denied: true }],
      },
    ];
    const stagedFile = stagedCases.flatMap((row) => row.lines);
    const messageFile = messageCases.flatMap((row) => row.lines);

    let stagedRows: readonly string[];
    let messageRows: readonly string[];

    beforeAll(async () => {
      const ws = await copyOf(template);
      git(ws, 'config', '--add', 'smoothbricks.publicDenylist', 'secret\\hconsumer');
      await sibling(ws);
      await stage(ws, { 'notes.txt': fileOf(stagedFile) });
      const env = { ...environment('env-codename'), HOME: FIXTURE_HOME };
      stagedRows = deniedRows(await scanStaged(ws, env));
      messageRows = deniedRows(await scanMessage(ws, fileOf(messageFile), env));
    });

    it.each(stagedCases)('$name', ({ lines }) => {
      expectLines(stagedRows, 'staged:notes.txt', stagedFile, lines);
    });

    it.each(messageCases)('$name', ({ lines }) => {
      expectLines(messageRows, 'message', messageFile, lines);
    });
  });

  describe('public references', () => {
    const fixture = readFileSync(
      fileURLToPath(new URL('../../../../tooling/public-denylist-allowlist.tsv', import.meta.url)),
      'utf8',
    ).replace(/^package-scope\t[^\n]+$/m, 'package-scope\t@public-cms');

    /** The repository's own exact-line rule: its file, its exact text, and the project that text names. */
    const attribution = (() => {
      const declaration = fixture.split('\n').find((line) => line.startsWith('line\t'));
      if (!declaration) throw new Error('missing attribution fixture');
      const [, path, text] = declaration.split('\t');
      if (!path || !text) throw new Error('invalid attribution fixture');
      const project = /\bfrom the (\S+) fork;/.exec(text)?.[1];
      if (!project) throw new Error('missing attribution source project');
      return { path, text, project };
    })();

    describe('exempted from a sibling match', () => {
      const cases: readonly LineCase[] = [
        {
          name: 'allows public package and encoded patch references but still denies private prose on their lines',
          lines: [
            { text: 'import { cmsIntegration } from "@public-cms/astro-cms/node";', denied: false },
            { text: 'patches/@public-cms%2Fcontent-api.patch', denied: false },
            { text: '@public-cms/content-api secret-consumer', denied: true },
            { text: '@public-cms/content-api public-cms prose', denied: true },
          ],
        },
        {
          name: 'compares UTF-8 PCRE columns with byte spans for public references',
          lines: [
            { text: 'é @public-cms/content-api', denied: false },
            // The sibling match ends where the reference does, so a span counted in UTF-16 units falls short of it.
            { text: 'é @public-cms/git-backup', denied: false },
            { text: 'é @public-cms/content-api secret-consumer', denied: true },
          ],
        },
        {
          name: 'allows approved public repository tokens, not another sibling mentioned alongside them',
          lines: [
            { text: 'git-format-staged and git-backup', denied: false },
            { text: 'git-format-staged secret-consumer', denied: true },
          ],
        },
        {
          name: 'exempts an allowed public repository name in every casing the sibling match accepts, not another cased term',
          lines: [
            { text: 'smoothbricks SmoothBricks SMOOTHBRICKS', denied: false },
            { text: 'SmoothBricks Secret-Consumer', denied: true },
          ],
        },
        {
          name: 'exempts an allowed package scope in every casing the sibling match accepts, not another cased term',
          lines: [
            { text: '@Public-CMS/content-api patches/@PUBLIC-CMS%2Fcontent-api.patch', denied: false },
            { text: '@Public-CMS/content-api Secret-Consumer', denied: true },
          ],
        },
      ];
      // The exact line the allowlist declares, in a file it does not name.
      const elsewhere: Line = { text: attribution.text, denied: true };
      const notes = [...cases.flatMap((row) => row.lines), elsewhere];
      const attributed: readonly Line[] = [
        { text: attribution.text, denied: false },
        { text: `${attribution.text} secret-consumer`, denied: true },
      ];
      const messageLines: readonly Line[] = [
        { text: 'fix: update @public-cms/content-api', denied: false },
        { text: '', denied: false },
        { text: 'fix: update @public-cms/content-api for Secret-Consumer', denied: true },
      ];
      // The repository's own rules, and one more for the casing case: a public repository name no rule names yet.
      const allowlist = [fixture.trimEnd(), 'repository\tsmoothbricks', ''].join('\n');

      let rows: readonly string[];
      let messageRows: readonly string[];

      beforeAll(async () => {
        const ws = await copyOf(template);
        const siblings = ['public-cms', 'secret-consumer', 'git-format-staged', 'git-backup', 'smoothbricks'];
        for (const name of [...siblings, attribution.project]) await sibling(ws, name);
        await stage(ws, {
          [PUBLIC_DENYLIST_ALLOWLIST_PATH]: allowlist,
          'notes.txt': fileOf(notes),
          [attribution.path]: fileOf(attributed),
        });
        rows = deniedRows(await scanStaged(ws));
        messageRows = deniedRows(await scanMessage(ws, fileOf(messageLines)));
      });

      it.each(cases)('$name', ({ lines }) => {
        expectLines(rows, 'staged:notes.txt', notes, lines);
      });

      it('permits the original attribution only at its exact file and exact text', () => {
        expectLines(rows, `staged:${attribution.path}`, attributed, attributed);
        expectLines(rows, 'staged:notes.txt', notes, [elsewhere]);
      });

      it('applies public reference exemptions to messages without exempting private prose', () => {
        expectLines(messageRows, 'message', messageLines, messageLines);
      });

      it('exempts the rows of the staged allowlist, which name the references they declare', () => {
        expect(rowsUnder(rows, `staged:${PUBLIC_DENYLIST_ALLOWLIST_PATH}`)).toEqual([]);
      });

      it('judges no other line', () => {
        expect(rows).toHaveLength([...notes, ...attributed].filter((line) => line.denied).length);
      });
    });

    describe('keeps explicit PCRE authoritative over public references, including composites and zero-width alternatives', () => {
      const reference = '@public-cms/content-api';
      const composite = '@public-cms/content-api secret-consumer';
      let ws: Workspace;

      beforeAll(async () => {
        ws = await copyOf(template);
        await sibling(ws, 'public-cms');
        // Committed, so that only the case's own file is added: the allowlist's rows name public-cms too.
        await stage(ws, { [PUBLIC_DENYLIST_ALLOWLIST_PATH]: fixture });
        git(ws, 'commit', '--quiet', '-m', 'base public references');
      });

      it.each([
        {
          name: 'a pattern that matches nothing leaves the public reference alone',
          text: reference,
          pattern: '\\h',
          denied: false,
        },
        {
          name: 'a pattern inside a public reference denies its line',
          text: reference,
          pattern: 'public-cms',
          denied: true,
        },
        {
          name: 'a composite pattern over the reference and private prose denies the line',
          text: composite,
          pattern: '@public-cms/content-api.*secret-consumer',
          denied: true,
        },
        {
          name: 'a zero-width pattern on the private prose denies the line',
          text: composite,
          pattern: '(?=secret-consumer)',
          denied: true,
        },
        {
          name: 'an alternative of the reference and a zero-width pattern denies the line',
          text: composite,
          pattern: 'public-cms|(?=secret-consumer)',
          denied: true,
        },
      ])('$name', async ({ text, pattern, denied }) => {
        await stage(ws, { 'notes.txt': `${text}\n` });

        expect(deniedRows(await scanStaged(ws, environment(pattern)))).toEqual(
          denied ? [`staged:notes.txt:1:${text}`] : [],
        );
      });

      it('a zero-width pattern denies a message line that names a public reference', async () => {
        const scan = await scanMessage(ws, 'fix: update @public-cms/content-api\n', environment('(?=public-cms)'));

        expect(deniedRows(scan)).toEqual(['message:1:fix: update @public-cms/content-api']);
      });
    });

    it('never exempts HOME because an allowed package or exact line contains it', async () => {
      const ws = await copyOf(template);
      await stage(ws, {
        [PUBLIC_DENYLIST_ALLOWLIST_PATH]: `package-scope\t@public-cms\nline\tnotes.txt\t${FIXTURE_HOME}/project\n`,
        'notes.txt': `${FIXTURE_HOME}/project\n@public-cms/content-api ${FIXTURE_HOME}/project\n`,
      });

      const rows = deniedRows(await scanStaged(ws, { ...environment(), HOME: FIXTURE_HOME }));

      expect(rows).toContain(`staged:notes.txt:1:${FIXTURE_HOME}/project`);
      expect(rows).toContain(`staged:notes.txt:2:@public-cms/content-api ${FIXTURE_HOME}/project`);
    });

    it('does not trust an untracked or unstaged allowlist edit', async () => {
      const ws = await copyOf(template);
      await sibling(ws);
      await stage(ws, { [PUBLIC_DENYLIST_ALLOWLIST_PATH]: 'repository\tgit-backup\n' });
      git(ws, 'commit', '--quiet', '-m', 'base allowlist');
      await writeFile(join(ws.root, PUBLIC_DENYLIST_ALLOWLIST_PATH), 'repository\tsecret-consumer\n');
      await stage(ws, { 'notes.txt': 'secret-consumer\n' });

      expect(await scanStaged(ws)).toEqual({ outcome: 'denied', matches: 'staged:notes.txt:1:secret-consumer\n' });
      git(ws, 'rm', '--cached', '--quiet', PUBLIC_DENYLIST_ALLOWLIST_PATH);
      expect((await scanStaged(ws)).outcome).toBe('denied');
    });
  });

  describe('fails closed', () => {
    let ws: Workspace;
    let badConfig: string;
    let badIndex: string;

    beforeAll(async () => {
      ws = await copyOf(template);
      badConfig = join(ws.container, 'bad.gitconfig');
      badIndex = join(ws.container, 'invalid-index');
      await writeFile(badConfig, '[unclosed\n');
      await writeFile(badIndex, 'not an index');
    });

    it.each([
      {
        name: 'invalid regex even when no lines are staged',
        env: () => environment('(unclosed'),
        reason: 'missing closing parenthesis',
      },
      {
        name: 'invalid git configuration',
        env: () => ({ ...environment(), GIT_CONFIG_GLOBAL: badConfig }),
        reason: 'git config',
      },
      {
        name: 'an unreadable or corrupt index',
        env: () => ({ ...environment(), GIT_INDEX_FILE: badIndex }),
        reason: 'index',
      },
    ])('fails closed for $name', async ({ env, reason }) => {
      expect(failureOf(await scanStaged(ws, env()))).toContain(reason);
    });

    it.each(['', '/', 'relative'])(
      'refuses invalid HOME %j instead of silently disabling personal-path protection',
      async (home) => {
        expect(failureOf(await scanStaged(ws, { ...environment(), HOME: home }))).toContain('HOME');
      },
    );

    it('fails closed for a missing message file, naming the file', async () => {
      const path = join(ws.root, '.git', 'missing-message');
      const scan = await scanPublicCommitDenylist(ws.root, { kind: 'message', path }, environment());

      expect(failureOf(scan)).toContain(path);
    });

    it('fails closed when a declared sibling git repository is broken', async () => {
      const own = await copyOf(template);
      await mkdir(join(own.container, 'secret-consumer', '.git'), { recursive: true });
      const reason = failureOf(await scanStaged(own));

      expect(reason).toContain('secret-consumer');
      expect(reason).toContain('git rev-parse');
    });

    it('fails closed for a malformed staged allowlist, naming its line', async () => {
      const own = await copyOf(template);
      await stage(own, { [PUBLIC_DENYLIST_ALLOWLIST_PATH]: 'repository\tgit-backup\nunknown\tvalue\n' });

      expect(failureOf(await scanStaged(own))).toContain(`${PUBLIC_DENYLIST_ALLOWLIST_PATH}:2`);
    });
  });
});
