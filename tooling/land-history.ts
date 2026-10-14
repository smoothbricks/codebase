#!/usr/bin/env bun
// The ancestry cutoff is a correctness guard, independent of the land gate's result.
import { spawnSync } from 'node:child_process';

/** The unpublished main whose ancestry the public rewrite replaced. */
export const PRE_REWRITE_MAIN = 'e9e596563bfaa8b337893bb7df9937f9f65d06ea';

export type HistoryVerdict =
  | { readonly kind: 'clean' }
  | { readonly kind: 'refused'; readonly why: string; readonly remedy: string }
  | { readonly kind: 'unavailable'; readonly why: string };

/** Judge actual source ancestry without needing the forbidden object in a clean clone. */
export function judgeHistory(checkout: string, newMain: string, forbiddenAncestor = PRE_REWRITE_MAIN): HistoryVerdict {
  const history = spawnSync('git', ['-C', checkout, 'rev-list', 'HEAD'], { encoding: 'utf8' });
  if (history.status !== 0) {
    return { kind: 'unavailable', why: history.error?.message ?? history.stderr.trim() };
  }
  if (history.stdout.split('\n').includes(forbiddenAncestor)) {
    return {
      kind: 'refused',
      why: 'source ancestry contains pre-rewrite main ' + forbiddenAncestor,
      remedy: 'git rebase --onto ' + newMain + ' ' + forbiddenAncestor.slice(0, 9),
    };
  }
  return { kind: 'clean' };
}

if (import.meta.main) {
  const [checkout, newMain] = process.argv.slice(2);
  if (checkout === undefined || newMain === undefined) {
    process.stderr.write('usage: land-history.ts CHECKOUT NEW_MAIN\n');
    process.exit(64);
  }
  const verdict = judgeHistory(checkout, newMain);
  if (verdict.kind !== 'clean') {
    process.stderr.write(
      'land-history: ' + verdict.why + (verdict.kind === 'refused' ? '; ' + verdict.remedy : '') + '\n',
    );
    process.exit(1);
  }
}
