import { describe, expect, it } from 'bun:test';
import {
  afterLand,
  afterRebase,
  type EnvrcVerdict,
  envrcVerdict,
  fenceReason,
  workspaceArgument,
} from './land-decide.ts';
import { PRE_REWRITE_MAIN, verdictFromRevList } from './land-history.ts';

// These cases are the decisions land-host.test.ts reaches only through a spawned script. Each one is a plain call,
// so the branch is proven without a process; land-host.test.ts keeps the one real run per behavior.

const NEW_MAIN = 'b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0';
const OBSERVED = 'c0ffee00c0ffee00c0ffee00c0ffee00c0ffee00';

/** The envelope cowshed prints for a fence refusal under `--json`. */
function fenceEnvelope(reason: string): string {
  return JSON.stringify({ ok: false, error: { code: 'conflict', fence: { reason, observed: OBSERVED } } });
}

describe('workspaceArgument', () => {
  it('asks for usage when no argument is given', () => {
    expect(workspaceArgument([])).toEqual({ kind: 'usage' });
  });

  it('refuses main as the workspace, whatever follows it', () => {
    expect(workspaceArgument(['main', '--expected-target-head', OBSERVED])).toEqual({ kind: 'main' });
  });

  it('takes the first argument as the workspace and leaves the rest to cowshed', () => {
    const argument = workspaceArgument(['feat', '--expected-target-head', OBSERVED]);
    expect(argument).toEqual({ kind: 'workspace', name: 'feat' });
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

describe('fenceReason', () => {
  it('reads error.fence.reason from the cowshed envelope', () => {
    expect(fenceReason(fenceEnvelope('ontoMoved'))).toBe('ontoMoved');
  });

  it('is undefined for a conflict that carries no fence', () => {
    expect(fenceReason(JSON.stringify({ ok: false, error: { code: 'conflict' } }))).toBeUndefined();
  });

  it('is undefined for output that is not a JSON envelope', () => {
    expect(fenceReason('error: main moved (ontoMoved)')).toBeUndefined();
    expect(fenceReason('')).toBeUndefined();
  });

  it('is undefined when the reason is not a string or the envelope is not an object', () => {
    expect(fenceReason('null')).toBeUndefined();
    expect(fenceReason(JSON.stringify({ error: { fence: { reason: 3 } } }))).toBeUndefined();
  });

  it('reads the fence, not a reason quoted inside some other message', () => {
    const quoted = JSON.stringify({ ok: false, error: { code: 'conflict', message: '"reason":"ontoMoved"' } });
    expect(fenceReason(quoted)).toBeUndefined();
  });
});

describe('afterRebase', () => {
  it('retries only when main moved past the head the rebase was told', () => {
    expect(afterRebase(fenceEnvelope('ontoMoved'))).toBe('retry');
  });

  it('stops on every other refusal', () => {
    for (const reason of ['sourceMoved', 'targetMoved', 'incarnationMoved', 'notFastForward']) {
      expect(afterRebase(fenceEnvelope(reason))).toBe('stop');
    }
    expect(afterRebase('not an envelope')).toBe('stop');
  });
});

describe('afterLand', () => {
  it('gates again only when the target moved while the gate ran', () => {
    expect(afterLand(fenceEnvelope('targetMoved'))).toBe('gateAgain');
  });

  it('stops on every other refusal, never retrying a red gate', () => {
    for (const reason of ['ontoMoved', 'sourceMoved', 'incarnationMoved', 'notFastForward']) {
      expect(afterLand(fenceEnvelope(reason))).toBe('stop');
    }
    expect(afterLand(JSON.stringify({ ok: false, error: { code: 'conflict' } }))).toBe('stop');
  });
});

describe('verdictFromRevList', () => {
  it('refuses a history that still contains the forbidden ancestor, naming the onto rebase as remedy', () => {
    const history = { status: 0, stdout: `${NEW_MAIN}\n${PRE_REWRITE_MAIN}\n`, stderr: '' };
    expect(verdictFromRevList(history, NEW_MAIN, PRE_REWRITE_MAIN)).toEqual({
      kind: 'refused',
      why: 'source ancestry contains pre-rewrite main ' + PRE_REWRITE_MAIN,
      remedy: 'git rebase --onto ' + NEW_MAIN + ' ' + PRE_REWRITE_MAIN.slice(0, 9),
    });
  });

  it('is clean when the forbidden ancestor is absent', () => {
    const history = { status: 0, stdout: `${NEW_MAIN}\n`, stderr: '' };
    expect(verdictFromRevList(history, NEW_MAIN, PRE_REWRITE_MAIN)).toEqual({ kind: 'clean' });
  });

  it('matches the ancestor as a whole line, not as a prefix of one', () => {
    const history = { status: 0, stdout: `${PRE_REWRITE_MAIN}x\n`, stderr: '' };
    expect(verdictFromRevList(history, NEW_MAIN, PRE_REWRITE_MAIN)).toEqual({ kind: 'clean' });
  });

  it('is unavailable with git stderr when rev-list exits nonzero', () => {
    const history = { status: 128, stdout: '', stderr: 'fatal: not a git repository\n' };
    expect(verdictFromRevList(history, NEW_MAIN, PRE_REWRITE_MAIN)).toEqual({
      kind: 'unavailable',
      why: 'fatal: not a git repository',
    });
  });

  it('is unavailable with the spawn error when git could not start, ahead of any stderr', () => {
    const history = { status: null, stdout: '', stderr: '', error: new Error('spawnSync git ENOENT') };
    expect(verdictFromRevList(history, NEW_MAIN, PRE_REWRITE_MAIN)).toEqual({
      kind: 'unavailable',
      why: 'spawnSync git ENOENT',
    });
  });
});
