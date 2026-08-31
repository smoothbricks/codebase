import { describe, expect, it } from 'bun:test';
import { formatCommitMessage, validateBreakingDisclosure, validateCommitMessage } from './commit-msg.js';

describe('commit message validation', () => {
  it('accepts Nx conventional commit types and configured scopes', () => {
    expect(validateCommitMessage('types(cli): expose public API\n', { validScopes: new Set(['cli']) })).toBeNull();
  });

  it('rejects scopes outside configured Nx names', () => {
    expect(
      validateCommitMessage('fix(@smoothbricks/cli): repair release\n', { validScopes: new Set(['cli']) }),
    ).toContain('Invalid conventional commit scope');
  });
});

describe('commit message formatting', () => {
  it('wraps prose and preserves markdown blocks', () => {
    const message = formatCommitMessage(
      [
        'fix(cli): wrap commit bodies   ',
        '',
        'This paragraph is intentionally long enough to be wrapped by the injected formatter callback while preserving non-prose markdown sections.',
        '',
        '```',
        'long log errors should stay exactly as they are because this is a fenced block',
        '```',
        '',
        '> quoted markdown should also stay as a single untouched line even when it is long enough to otherwise need wrapping',
      ].join('\n'),
      { wrapBody: (paragraph) => paragraph.replace(' while ', '\nwhile ') },
    );

    expect(message).toBe(`fix(cli): wrap commit bodies

This paragraph is intentionally long enough to be wrapped by the injected formatter callback
while preserving non-prose markdown sections.

\`\`\`
long log errors should stay exactly as they are because this is a fenced block
\`\`\`

> quoted markdown should also stay as a single untouched line even when it is long enough to otherwise need wrapping
`);
  });
});

describe('breaking-change disclosure for deleted published packages', () => {
  const deleted = ['@smoothbricks/lmao-rs'];

  it('refuses a refactor that deletes a published package without a marker', () => {
    // The real lmao@0.3.6 case: `refactor` does not release, so the deletion was
    // absent from the changelog and consumers got no notice at all.
    const error = validateBreakingDisclosure(
      'refactor(lmao): absorb the Rust workspace, delete the lmao-rs package\n',
      deleted,
    );
    expect(error).toContain('@smoothbricks/lmao-rs');
    expect(error).toContain('BREAKING CHANGE:');
  });

  it('refuses a fix that deletes a published package without a marker', () => {
    expect(validateBreakingDisclosure('fix(lmao): drop the old package\n', deleted)).not.toBeNull();
  });

  it('accepts a bang marker on the subject', () => {
    expect(validateBreakingDisclosure('refactor(lmao)!: delete the lmao-rs package\n', deleted)).toBeNull();
  });

  it('accepts a BREAKING CHANGE footer', () => {
    const message = [
      'refactor(lmao): absorb the Rust workspace',
      '',
      'BREAKING CHANGE: @smoothbricks/lmao-rs is removed; its crates ship inside',
      '@smoothbricks/lmao under crates/.',
      '',
    ].join('\n');
    expect(validateBreakingDisclosure(message, deleted)).toBeNull();
  });

  it('stays silent when no published manifest was deleted', () => {
    expect(validateBreakingDisclosure('refactor(lmao): move a file\n', [])).toBeNull();
  });

  it('does not block git-generated subjects', () => {
    expect(validateBreakingDisclosure('Merge branch main into topic\n', deleted)).toBeNull();
  });
});
