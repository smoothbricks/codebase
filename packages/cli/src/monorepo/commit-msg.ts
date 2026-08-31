import { spawnSync } from 'node:child_process';
import typia from 'typia';

/** The only fields of a deleted manifest this guard needs. */
interface DeletedManifest {
  name?: string;
  nx?: { tags?: string[] };
}

/** `git show` output is an untrusted JSON boundary, so it is validated rather than cast. */
const parseDeletedManifest = typia.json.createIsParse<DeletedManifest>();

const validCommitTypes = new Set([
  'build',
  'chore',
  'ci',
  'docs',
  'feat',
  'fix',
  'perf',
  'refactor',
  'revert',
  'style',
  'test',
  'types',
]);

export interface FormatCommitMessageOptions {
  wrapBody?: (paragraph: string) => string;
}

export interface ValidateCommitMessageOptions {
  validScopes?: ReadonlySet<string>;
}

export function validateCommitMessage(message: string, options: ValidateCommitMessageOptions = {}): string | null {
  const subject = message.split('\n', 1)[0]?.trim() ?? '';
  if (!subject) {
    return 'Commit message subject is empty.';
  }
  if (isGitGeneratedSubject(subject)) {
    return null;
  }
  const match = /^(?<type>[a-z]+)(\((?<scope>[a-z0-9._/@-]+(?:,[a-z0-9._/@-]+)*)\))?(?<breaking>!)?: .+$/.exec(subject);
  const type = match?.groups?.type;
  if (!type || !validCommitTypes.has(type)) {
    return `Invalid conventional commit subject: ${subject}

Expected examples:
  feat(statebus-core): add optimistic transactions
  fix(money): round negative amounts consistently
  chore(release): publish 1.2.3
  feat!: remove deprecated API`;
  }
  const scope = match.groups?.scope;
  if (scope && options.validScopes) {
    const invalidScopes = scope.split(',').filter((entry) => !options.validScopes?.has(entry));
    if (invalidScopes.length > 0) {
      return `Invalid conventional commit scope: ${invalidScopes.join(',')}

Use package.json nx.name values, for example:
  feat(statebus-core): add optimistic transactions
  fix(money): round negative amounts consistently`;
    }
  }
  return null;
}

/**
 * A commit that deletes a published package's manifest is breaking for anyone
 * depending on it, but conventional-commit TYPE selection decides the changelog:
 * `refactor` does not release at all and `fix` files under Fixes, so such a
 * deletion can ship with no breaking notice in the release notes. That happened
 * to `@smoothbricks/lmao-rs`, which was removed in a `refactor(lmao):` commit and
 * is absent from the lmao@0.3.6 changelog entirely. Requiring an explicit marker
 * puts the disclosure in the notes consumers actually read.
 */
export function validateBreakingDisclosure(message: string, deletedPublicPackages: readonly string[]): string | null {
  if (deletedPublicPackages.length === 0) {
    return null;
  }
  const subject = message.split('\n', 1)[0]?.trim() ?? '';
  if (isGitGeneratedSubject(subject)) {
    return null;
  }
  const declaresBreaking = /^[a-z]+(\([a-z0-9._/@,-]+\))?!: /.test(subject) || /^BREAKING[ -]CHANGE:/m.test(message);
  if (declaresBreaking) {
    return null;
  }
  const names = deletedPublicPackages.join(', ');
  return `This commit deletes published package manifest(s): ${names}. Removing an npm:public package breaks every dependant, so the commit must declare it — add "!" after the type/scope, or a "BREAKING CHANGE:" footer explaining what replaces it. Without a marker the release notes omit the removal entirely.`;
}

/** Manifests of `npm:public` packages deleted in the staged change, relative to HEAD. */
export function stagedDeletedPublicPackages(root: string): string[] {
  const deleted = spawnSync('git', ['diff', '--cached', '--diff-filter=D', '--name-only'], {
    cwd: root,
    encoding: 'utf8',
  });
  if (deleted.status !== 0) {
    return [];
  }
  const manifests = (deleted.stdout ?? '')
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => /^packages\/[^/]+\/package\.json$/.test(line));
  const names: string[] = [];
  for (const manifest of manifests) {
    // The manifest is gone from the worktree, so its tags are only readable at HEAD.
    const shown = spawnSync('git', ['show', `HEAD:${manifest}`], { cwd: root, encoding: 'utf8' });
    if (shown.status !== 0) {
      continue;
    }
    const parsed = parseDeletedManifest(shown.stdout ?? '');
    if (parsed && (parsed.nx?.tags ?? []).includes('npm:public')) {
      names.push(parsed.name ?? manifest);
    }
  }
  return names;
}

export function formatCommitMessage(message: string, options: FormatCommitMessageOptions = {}): string {
  const wrapBody = options.wrapBody ?? wrapBodyWithFmt;
  const normalized = message.replace(/\r\n?/g, '\n');
  const lines = normalized.split('\n');
  const subject = lines.shift()?.trimEnd() ?? '';
  const formatted = [subject];
  let paragraph: string[] = [];
  let inFence = false;

  const flushParagraph = () => {
    if (paragraph.length === 0) {
      return;
    }
    formatted.push(wrapBody(paragraph.join('\n')).trimEnd());
    paragraph = [];
  };

  for (const line of lines) {
    const trimmedEnd = line.trimEnd();
    if (trimmedEnd.startsWith('```') || trimmedEnd.startsWith('~~~')) {
      flushParagraph();
      inFence = !inFence;
      formatted.push(trimmedEnd);
      continue;
    }
    if (inFence || shouldPreserveLine(trimmedEnd)) {
      flushParagraph();
      formatted.push(trimmedEnd);
      continue;
    }
    if (trimmedEnd === '') {
      flushParagraph();
      formatted.push('');
      continue;
    }
    paragraph.push(trimmedEnd);
  }
  flushParagraph();

  while (formatted.length > 1 && formatted.at(-1) === '') {
    formatted.pop();
  }
  return `${formatted.join('\n')}\n`;
}

function wrapBodyWithFmt(paragraph: string): string {
  const result = spawnSync('fmt', ['-w', '72'], { input: `${paragraph}\n`, encoding: 'utf8' });
  if (result.error) {
    throw new Error(
      `fmt is required to format commit messages. Install it through devenv and retry.\n${result.error.message}`,
    );
  }
  if (result.status !== 0) {
    const output = [result.stderr.trim(), result.stdout.trim()].filter(Boolean).join('\n');
    throw new Error(`fmt -w 72 failed with exit code ${result.status ?? 1}${output ? `:\n${output}` : ''}`);
  }
  return result.stdout;
}

function shouldPreserveLine(line: string): boolean {
  return (
    line.startsWith('#') ||
    line.startsWith('>') ||
    /^\s/.test(line) ||
    /^[-*+]\s+/.test(line) ||
    /^\d+\.\s+/.test(line) ||
    /^https?:\/\//.test(line) ||
    /^[A-Za-z][A-Za-z0-9-]*: /.test(line) ||
    /^BREAKING CHANGE: /.test(line)
  );
}

function isGitGeneratedSubject(subject: string): boolean {
  return /^(Merge|Revert ")/.test(subject) || /^(fixup|squash)! /.test(subject);
}
