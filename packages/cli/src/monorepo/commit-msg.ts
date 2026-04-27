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
]);

export function validateCommitMessage(message: string): string | null {
  const subject = message.split('\n', 1)[0]?.trim() ?? '';
  if (!subject) {
    return 'Commit message subject is empty.';
  }
  if (/^(Merge|Revert ")/.test(subject) || /^(fixup|squash)! /.test(subject)) {
    return null;
  }
  const match = /^(?<type>[a-z]+)(\([a-z0-9._/-]+\))?(?<breaking>!)?: .+$/.exec(subject);
  const type = match?.groups?.type;
  if (type && validCommitTypes.has(type)) {
    return null;
  }
  return `Invalid conventional commit subject: ${subject}

Expected examples:
  feat(statebus-core): add optimistic transactions
  fix(money): round negative amounts consistently
  chore(release): publish 1.2.3
  feat!: remove deprecated API`;
}
