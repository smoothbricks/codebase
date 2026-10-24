import { describe, expect, it } from 'bun:test';
import {
  type AddedLine,
  type Allowlist,
  PUBLIC_DENYLIST_ALLOWLIST_PATH,
  parseAddedLines,
  parseAllowlist,
  publicReferencePatterns,
  referenceExemption,
} from './public-denylist.js';

const EMPTY: Allowlist = { scopes: [], repositories: [], lines: [] };

const rule = (...fields: string[]): string => fields.join('\t');

const invalidRule = (line: number) => ({
  failure: `${PUBLIC_DENYLIST_ALLOWLIST_PATH}:${line}: invalid tab-separated public reference rule`,
});

describe('parseAllowlist', () => {
  it('reads nothing from empty content, blank lines and comments', () => {
    expect(parseAllowlist('')).toEqual(EMPTY);
    expect(parseAllowlist('\n\n# a comment\n#another\n')).toEqual(EMPTY);
  });

  it('reads package-scope, repository and line rules in file order', () => {
    const content = [
      '# public references',
      rule('package-scope', '@public-cms'),
      '',
      rule('repository', 'git-backup'),
      rule('package-scope', '@second.scope_1'),
      rule('line', 'docs/attribution.md', 'Exact text; with punctuation, and é.'),
      rule('repository', 'Git-Format-Staged'),
    ].join('\n');

    expect(parseAllowlist(`${content}\n`)).toEqual({
      scopes: ['@public-cms', '@second.scope_1'],
      repositories: ['git-backup', 'Git-Format-Staged'],
      lines: [{ path: 'docs/attribution.md', text: 'Exact text; with punctuation, and é.' }],
    });
  });

  it('names the line of the first malformed rule, counting blank and comment lines', () => {
    expect(parseAllowlist('repository\tgit-backup\nunknown\tvalue\n')).toEqual(invalidRule(2));
    expect(parseAllowlist('# comment\n\nrepository\tgit-backup\nunknown\tvalue\nrepository\t-also-bad\n')).toEqual(
      invalidRule(4),
    );
  });

  it.each([
    { why: 'a package scope without its @', content: rule('package-scope', 'public-cms') },
    { why: 'an uppercase package scope', content: rule('package-scope', '@Public-CMS') },
    { why: 'a bare @ as the package scope', content: rule('package-scope', '@') },
    { why: 'a package scope that starts with punctuation', content: rule('package-scope', '@-cms') },
    { why: 'a package scope with a path', content: rule('package-scope', '@public-cms/api') },
    { why: 'a package scope without a value', content: 'package-scope' },
    { why: 'a package scope with a third field', content: rule('package-scope', '@public-cms', 'text') },
    { why: 'a package scope with a trailing tab', content: `${rule('package-scope', '@public-cms')}\t` },
    { why: 'a repository that starts with punctuation', content: rule('repository', '-git') },
    { why: 'a repository with a path', content: rule('repository', 'git/backup') },
    { why: 'a repository with a space', content: rule('repository', 'git backup') },
    { why: 'a repository without a value', content: rule('repository', '') },
    { why: 'a repository with a third field', content: rule('repository', 'git-backup', 'text') },
    { why: 'a line rule without text', content: rule('line', 'docs/a.md') },
    { why: 'a line rule with empty text', content: rule('line', 'docs/a.md', '') },
    { why: 'a line rule without a path', content: rule('line', '', 'text') },
    { why: 'a line rule with an absolute path', content: rule('line', '/etc/hosts', 'text') },
    { why: 'a line rule that climbs out of the repository', content: rule('line', '../a.md', 'text') },
    { why: 'a line rule that climbs out in the middle of its path', content: rule('line', 'docs/../a.md', 'text') },
    { why: 'a line rule with a fourth field', content: rule('line', 'docs/a.md', 'text', 'more') },
    { why: 'an unknown rule kind', content: rule('unknown', 'value') },
    { why: 'a rule kind in the wrong case', content: rule('Repository', 'git-backup') },
    { why: 'a row of only a tab', content: '\t' },
  ])('fails the whole allowlist on $why, after valid rules', ({ content }) => {
    expect(parseAllowlist(`${rule('repository', 'git-backup')}\n${content}\n`)).toEqual(invalidRule(2));
  });
});

const added = (path: string, line: number, text: string): AddedLine => ({ path, line, text });

/** `git diff --cached --unified=0` of a file the commit creates. */
const NEW_FILE = [
  'diff --git a/fresh.txt b/fresh.txt',
  'new file mode 100644',
  'index 0000000..de98044',
  '--- /dev/null',
  '+++ b/fresh.txt',
  '@@ -0,0 +1,3 @@',
  '+a',
  '+b',
  '+c',
  '',
].join('\n');

/** The same, for a file that keeps lines 2-4 and replaces its first and last. */
const TWO_HUNKS = [
  'diff --git a/notes.txt b/notes.txt',
  'index 78c7cf6..ca7ecd0 100644',
  '--- a/notes.txt',
  '+++ b/notes.txt',
  '@@ -1 +1 @@',
  '-first',
  '+secret-consumer',
  '@@ -5 +5 @@ fourth',
  '-fifth',
  '+Secret-Consumer',
  '',
].join('\n');

/** A symbolic link replaced by a regular file: git shows the deletion and the creation as two patches. */
const TYPE_CHANGE = [
  'diff --git a/link.txt b/link.txt',
  'deleted file mode 120000',
  'index 4bc4f33..0000000',
  '--- a/link.txt',
  '+++ /dev/null',
  '@@ -1 +0,0 @@',
  '-neutral-target',
  '\\ No newline at end of file',
  'diff --git a/link.txt b/link.txt',
  'new file mode 100644',
  'index 0000000..24d9a17',
  '--- /dev/null',
  '+++ b/link.txt',
  '@@ -0,0 +1 @@',
  '+neutral text',
  '',
].join('\n');

describe('parseAddedLines', () => {
  it('numbers the lines of a new file from one', () => {
    expect(parseAddedLines('fresh.txt', NEW_FILE)).toEqual([
      added('fresh.txt', 1, 'a'),
      added('fresh.txt', 2, 'b'),
      added('fresh.txt', 3, 'c'),
    ]);
  });

  it('numbers the lines of every hunk from its own header, across hunks', () => {
    expect(parseAddedLines('notes.txt', TWO_HUNKS)).toEqual([
      added('notes.txt', 1, 'secret-consumer'),
      added('notes.txt', 5, 'Secret-Consumer'),
    ]);
  });

  it('starts at the new-file line of a hunk header that carries counts', () => {
    const diff = ['diff --git a/a.txt b/a.txt', '--- a/a.txt', '+++ b/a.txt', '@@ -379,36 +384,111 @@', '+x', '+y', ''];

    expect(parseAddedLines('a.txt', diff.join('\n'))).toEqual([added('a.txt', 384, 'x'), added('a.txt', 385, 'y')]);
  });

  it('reads no header of a type change as an added line', () => {
    expect(parseAddedLines('link.txt', TYPE_CHANGE)).toEqual([added('link.txt', 1, 'neutral text')]);
  });

  it('counts context rows, which a wider diff shows between the added lines', () => {
    const diff = [
      'diff --git a/ctx.txt b/ctx.txt',
      'index de98044..7be73ce 100644',
      '--- a/ctx.txt',
      '+++ b/ctx.txt',
      '@@ -1,3 +1,3 @@',
      ' a',
      '-b',
      '+B',
      ' c',
      '',
    ];

    expect(parseAddedLines('ctx.txt', diff.join('\n'))).toEqual([added('ctx.txt', 2, 'B')]);
  });

  it('reads nothing from removals and end-of-file markers', () => {
    const diff = [
      'diff --git a/old.txt b/old.txt',
      'deleted file mode 100644',
      'index 1111111..0000000',
      '--- a/old.txt',
      '+++ /dev/null',
      '@@ -1,2 +0,0 @@',
      '-secret-consumer',
      '-second',
      '\\ No newline at end of file',
      '',
    ];

    expect(parseAddedLines('old.txt', diff.join('\n'))).toEqual([]);
  });

  it('keeps an added line that looks like a hunk header or a patch header as text', () => {
    const diff = [
      'diff --git a/notes.txt b/notes.txt',
      '--- a/notes.txt',
      '+++ b/notes.txt',
      '@@ -0,0 +1,4 @@',
      '+@@ -379,36 +384,111 @@',
      '+diff --git a/secret-consumer b/secret-consumer',
      '+++ not a header',
      '+last',
      '',
    ];

    expect(parseAddedLines('notes.txt', diff.join('\n'))).toEqual([
      added('notes.txt', 1, '@@ -379,36 +384,111 @@'),
      added('notes.txt', 2, 'diff --git a/secret-consumer b/secret-consumer'),
      added('notes.txt', 3, '++ not a header'),
      added('notes.txt', 4, 'last'),
    ]);
  });

  it('reads nothing from an empty diff or from headers before the first hunk', () => {
    expect(parseAddedLines('a.txt', '')).toEqual([]);
    expect(parseAddedLines('a.txt', 'diff --git a/a.txt b/a.txt\n--- a/a.txt\n+++ b/a.txt\n')).toEqual([]);
  });
});

const ALLOWLIST: Allowlist = {
  scopes: ['@public-cms'],
  repositories: ['git-backup'],
  lines: [{ path: 'docs/attribution.md', text: 'Extracted from the public-cms fork.' }],
};
const REFERENCES = publicReferencePatterns(ALLOWLIST);

const row = (path: string, text: string): AddedLine => ({ path, line: 1, text });
const exempt = (line: AddedLine) => referenceExemption(line, ALLOWLIST, REFERENCES);
const spansOf = (...spans: [start: number, end: number][]) => ({
  wholeLine: false,
  spans: spans.map(([start, end]) => ({ start, end })),
});

describe('publicReferencePatterns', () => {
  const found = (text: string): string[] => REFERENCES.flatMap((reference) => text.match(reference) ?? []);

  it('is empty for an allowlist that declares no scope and no repository', () => {
    expect(publicReferencePatterns(EMPTY)).toEqual([]);
    expect(publicReferencePatterns({ ...EMPTY, lines: ALLOWLIST.lines })).toEqual([]);
  });

  it('matches a declared package scope and a package name, in any casing, plain or %2F-encoded', () => {
    expect(found('@public-cms/content-api')).toEqual(['@public-cms/content-api']);
    expect(found('import { x } from "@Public-CMS/astro-cms/node";')).toEqual(['@Public-CMS/astro-cms']);
    expect(found('patches/@public-cms%2Fcontent-api.patch')).toEqual(['@public-cms%2Fcontent-api.patch']);
    expect(found('@PUBLIC-CMS%2fcontent-api')).toEqual(['@PUBLIC-CMS%2fcontent-api']);
  });

  it('does not match a bare package scope, a scope without the @, or another scope', () => {
    expect(found('@public-cms')).toEqual([]);
    expect(found('@public-cms/')).toEqual([]);
    expect(found('public-cms/content-api')).toEqual([]);
    expect(found('@other-cms/content-api')).toEqual([]);
  });

  it('matches a declared repository only as a whole word, in any casing', () => {
    expect(found('git-backup GIT-BACKUP Git-Backup')).toEqual(['git-backup', 'GIT-BACKUP', 'Git-Backup']);
    expect(found('git-backups xgit-backup git-backup_2 git-backup2')).toEqual([]);
  });
});

describe('referenceExemption', () => {
  it('excuses the whole line the allowlist names exactly, only in its own file', () => {
    const text = 'Extracted from the public-cms fork.';

    expect(exempt(row('docs/attribution.md', text))).toEqual({ wholeLine: true });
    expect(exempt(row('docs/other.md', text))).toEqual(spansOf());
    expect(exempt(row('docs/attribution.md', `${text} More.`))).toEqual(spansOf());
  });

  it("excuses the allowlist file's own declaration of an exact-line rule and of a package scope", () => {
    const declaration = (text: string) => exempt(row(PUBLIC_DENYLIST_ALLOWLIST_PATH, text));

    expect(declaration('line\tdocs/attribution.md\tExtracted from the public-cms fork.')).toEqual({ wholeLine: true });
    expect(declaration('package-scope\t@public-cms')).toEqual({ wholeLine: true });
    expect(declaration('line\tdocs/other.md\tExtracted from the public-cms fork.')).toEqual(spansOf());
    expect(declaration('package-scope\t@other')).toEqual(spansOf());
    expect(exempt(row('notes.txt', 'package-scope\t@public-cms'))).toEqual(spansOf());
  });

  it('reports the byte spans of public references, not their UTF-16 positions', () => {
    // 'é' is two bytes and one UTF-16 unit; '🙂' is four bytes and two units.
    expect(exempt(row('notes.txt', 'é @public-cms/content-api'))).toEqual(spansOf([3, 26]));
    expect(exempt(row('notes.txt', '🙂 git-backup'))).toEqual(spansOf([5, 15]));
  });

  it('reports every reference on the line, whichever kind it is', () => {
    const exemption = exempt(row('notes.txt', 'git-backup, @public-cms/api and GIT-BACKUP'));
    if (exemption.wholeLine) throw new Error('expected byte spans, got a whole-line exemption');

    expect([...exemption.spans].sort((left, right) => left.start - right.start)).toEqual(
      spansOf([0, 10], [12, 27], [32, 42]).spans,
    );
  });

  it('keeps no state between lines or calls', () => {
    const text = 'git-backup git-backup';
    const first = exempt(row('a.txt', text));
    exempt(row('b.txt', '@public-cms/x and git-backup'));

    expect(first).toEqual(spansOf([0, 10], [11, 21]));
    expect(exempt(row('a.txt', text))).toEqual(first);
  });

  it('excuses nothing without references', () => {
    expect(referenceExemption(row('a.txt', 'git-backup'), EMPTY, [])).toEqual(spansOf());
  });
});
