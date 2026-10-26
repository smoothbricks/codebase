import { describe, expect, it } from 'bun:test';
import { type EnvrcVerdict, envrcVerdict, workspaceArgument } from './land-decide.ts';

// These cases are the decisions land-host.test.ts reaches only through a spawned script. Each one is a plain call,
// so the branch is proven without a process; land-host.test.ts keeps the one real run per behavior.

describe('workspaceArgument', () => {
  it('asks for usage when no workspace follows --gate', () => {
    expect(workspaceArgument([])).toEqual({ kind: 'usage' });
  });

  it('refuses main as the workspace', () => {
    expect(workspaceArgument(['main'])).toEqual({ kind: 'main' });
  });

  it('takes the one argument as the workspace', () => {
    expect(workspaceArgument(['feat'])).toEqual({ kind: 'workspace', name: 'feat' });
  });

  it('asks for usage when anything follows the workspace', () => {
    expect(workspaceArgument(['feat', '--no-push'])).toEqual({ kind: 'usage' });
    expect(workspaceArgument(['main', 'feat'])).toEqual({ kind: 'usage' });
  });

  it('refuses only the exact name main', () => {
    expect(workspaceArgument(['main-2'])).toEqual({ kind: 'workspace', name: 'main-2' });
  });
});

describe('envrcVerdict', () => {
  // A byte-identical .envrc is allowed whatever direnv reports; a differing one is approved only when direnv reports
  // it approved as a whole line, and refused otherwise.
  const cases: readonly (readonly [sameBytes: boolean, direnvStatus: string, verdict: EnvrcVerdict])[] = [
    [true, '', 'allow'],
    [true, 'Found RC allowed 1', 'allow'],
    [false, 'loading\nFound RC allowed 0\n', 'approved'],
    [false, 'Found RC allowed 0', 'approved'],
    [false, 'Found RC allowed 1', 'refuse'],
    [false, 'xFound RC allowed 0', 'refuse'],
    [false, '', 'refuse'],
  ];

  it('allows identical bytes, approves only a whole-line direnv approval, and refuses the rest', () => {
    for (const [sameBytes, direnvStatus, verdict] of cases) {
      expect(envrcVerdict(sameBytes, direnvStatus), JSON.stringify([sameBytes, direnvStatus])).toBe(verdict);
    }
  });
});
