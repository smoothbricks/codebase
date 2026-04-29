import { describe, expect, it } from 'bun:test';
import { formatCommitMessage, validateCommitMessage } from './commit-msg.js';

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
