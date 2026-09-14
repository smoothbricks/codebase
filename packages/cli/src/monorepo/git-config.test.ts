import { describe, expect, it } from 'bun:test';
import { postCommitHookFileForTest } from './git-config.js';

const FOREIGN = `#!/bin/sh
# >>> git-backup post-commit >>>
command -v git-backup >/dev/null 2>&1 && git-backup agent kick >/dev/null 2>&1 || true
# <<< git-backup post-commit <<<
`;

describe('post-commit hook file', () => {
  it('creates an executable shell file that calls the managed script', () => {
    const created = postCommitHookFileForTest('');

    expect(created.startsWith('#!/usr/bin/env bash\n')).toBe(true);
    expect(created).toContain('tooling/git-hooks/post-commit.sh');
    expect(created.endsWith('\n')).toBe(true);
  });

  it('keeps another tool block, which is why this slot is not a symlink', () => {
    const installed = postCommitHookFileForTest(FOREIGN);

    expect(installed).toContain('git-backup agent kick');
    expect(installed.startsWith('#!/bin/sh\n')).toBe(true);
    expect(installed.indexOf('git-backup agent kick')).toBeLessThan(installed.indexOf('# >>> smoo post-commit >>>'));
  });

  it('replaces its own block instead of repeating it', () => {
    const once = postCommitHookFileForTest(FOREIGN);

    expect(postCommitHookFileForTest(once)).toBe(once);
    expect(once.split('# >>> smoo post-commit >>>')).toHaveLength(2);
  });

  it('replaces a block whose end marker was lost', () => {
    const truncated = `${FOREIGN}# >>> smoo post-commit >>>\n"$(git rev-parse --show-toplevel)/tooling/git-hooks/post-commit.sh"\n`;

    const repaired = postCommitHookFileForTest(truncated);

    expect(repaired).toBe(postCommitHookFileForTest(FOREIGN));
    expect(repaired.split('post-commit.sh"')).toHaveLength(2);
  });
});
