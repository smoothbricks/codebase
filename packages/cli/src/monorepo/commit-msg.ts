import { spawnSync } from 'node:child_process';

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
    throw new Error(
      [result.stderr.trim(), result.stdout.trim()].filter(Boolean).join('\n') ||
        'fmt failed to wrap commit message body.',
    );
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
