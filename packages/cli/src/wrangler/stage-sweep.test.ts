import { describe, expect, it } from 'bun:test';
import type { CloudflareClient, R2Bucket } from './cloudflare.js';
import type { CleanupResult, ProcessRunner } from './deploy-stage.js';
import { DURABLE_OBJECT_LIMIT_HINT, wranglerJson } from './deploy-stage.js';
import { recordedPullRequests, STAGE_RECORDS_BUCKET, scopeRecordPrefix, stageRecordPrefix } from './stage-records.js';
import {
  describeSweep,
  isCollectable,
  namespaceCapacity,
  type PullRequestState,
  sweepNeedsAttention,
  sweepPullRequestStages,
} from './stage-sweep.js';

const scope = 'github.com/acme/app';
const repository = 'acme/app';

const closed: PullRequestState = { state: 'closed', headRepository: repository, baseRepository: repository };
const open: PullRequestState = { ...closed, state: 'open' };

/** An account holding `keys` in the records bucket and `namespaces` Durable Object namespaces. */
class Account implements Pick<CloudflareClient, 'listR2Buckets' | 'listR2Objects' | 'listDurableObjectNamespaces'> {
  constructor(
    readonly keys: string[],
    public namespaces: number,
    readonly buckets: R2Bucket[] = [{ name: STAGE_RECORDS_BUCKET }],
  ) {}
  async listR2Buckets(): Promise<R2Bucket[]> {
    return this.buckets;
  }
  async listR2Objects(bucket: string, prefix = ''): Promise<string[]> {
    expect(bucket).toBe(STAGE_RECORDS_BUCKET);
    return this.keys.filter((key) => key.startsWith(prefix));
  }
  async listDurableObjectNamespaces(): Promise<{ id: string }[]> {
    return Array.from({ length: this.namespaces }, (_, index) => ({ id: `ns-${index}` }));
  }
}

const keyFor = (pr: number, scopeName = scope) => `${stageRecordPrefix(scopeName, `pr${pr}`)}worker/worker`;

function cleanupResult(pr: number): CleanupResult {
  return {
    stage: `pr${pr}`,
    scope,
    recorded: 1,
    deleted: {
      workers: 1,
      routes: 0,
      domains: 0,
      dnsRecords: 0,
      kvNamespaces: 0,
      r2Buckets: 0,
      r2Objects: 0,
      d1Databases: 0,
    },
    alreadyGone: 0,
    leftInPlace: [],
  };
}

/** A sweep over `states`, recording which pull requests it cleaned up; each cleanup frees 3 namespaces. */
async function sweep(account: Account, states: Record<number, PullRequestState | Error>, only?: number) {
  const cleaned: number[] = [];
  const result = await sweepPullRequestStages({
    cloudflare: account,
    scope,
    repository,
    ...(only === undefined ? {} : { only }),
    pullRequestState: async (pr) => {
      const state = states[pr];
      if (state === undefined || state instanceof Error) throw state ?? new Error(`no state for ${pr}`);
      return state;
    },
    cleanup: async (pr) => {
      cleaned.push(pr);
      account.namespaces -= 3;
      return cleanupResult(pr);
    },
  });
  return { result, cleaned };
}

describe('recordedPullRequests', () => {
  it('reads each recorded pull request once, ascending, from this scope only', () => {
    const keys = [
      keyFor(70),
      keyFor(7),
      keyFor(7),
      keyFor(12, 'github.com/acme/other'),
      `${scopeRecordPrefix(scope)}staging/x`,
    ];
    expect(recordedPullRequests(scope, keys)).toEqual([7, 70]);
  });

  it('never reads a non-canonical number as a stage', () => {
    expect(
      recordedPullRequests(scope, [`${scopeRecordPrefix(scope)}pr07/x`, `${scopeRecordPrefix(scope)}pr0/x`]),
    ).toEqual([]);
  });
});

describe('isCollectable', () => {
  it('collects only a closed pull request from this repository', () => {
    expect(isCollectable(closed, repository)).toBe(true);
    expect(isCollectable({ ...closed, headRepository: 'ACME/App' }, repository)).toBe(true);
    expect(isCollectable(open, repository)).toBe(false);
    expect(isCollectable({ ...closed, headRepository: 'fork/app' }, repository)).toBe(false);
    expect(isCollectable({ ...closed, headRepository: null }, repository)).toBe(false);
    expect(isCollectable({ ...closed, baseRepository: 'acme/other' }, repository)).toBe(false);
  });
});

describe('namespaceCapacity', () => {
  it('alerts above 80% of the 500-namespace limit', () => {
    expect(namespaceCapacity(400)).toEqual({ count: 400, limit: 500, alertAbove: 400, over: false });
    expect(namespaceCapacity(401).over).toBe(true);
  });
});

describe('sweepPullRequestStages', () => {
  it('cleans up closed pull requests and keeps open ones', async () => {
    const account = new Account([keyFor(1), keyFor(2), keyFor(3)], 100);
    const { result, cleaned } = await sweep(account, {
      1: closed,
      2: open,
      3: { ...closed, headRepository: 'fork/app' },
    });

    expect(cleaned).toEqual([1]);
    expect(result.recorded).toEqual([1, 2, 3]);
    expect(result.kept).toEqual([
      { pr: 2, state: 'open' },
      { pr: 3, state: 'closed' },
    ]);
    expect(result.failed).toEqual([]);
    // Counted after collecting, so the report shows what the sweep freed.
    expect(result.namespaces.count).toBe(97);
    expect(sweepNeedsAttention(result)).toBe(false);
  });

  it('never cleans up a pull request whose state the forge cannot answer, and goes on with the rest', async () => {
    const account = new Account([keyFor(1), keyFor(2)], 10);
    const { result, cleaned } = await sweep(account, { 1: new Error('HTTP 502'), 2: closed });

    expect(cleaned).toEqual([2]);
    expect(result.failed).toEqual([{ pr: 1, error: 'HTTP 502' }]);
    expect(sweepNeedsAttention(result)).toBe(true);
    expect(describeSweep(result)).toContain('pr1 failed:** HTTP 502');
  });

  it('sweeps only the named pull request, still only if closed', async () => {
    const account = new Account([keyFor(1), keyFor(2)], 10);
    expect((await sweep(account, { 1: closed, 2: closed }, 2)).cleaned).toEqual([2]);
    expect((await sweep(account, { 1: open, 2: closed }, 1)).cleaned).toEqual([]);
  });

  it('finds nothing to collect in an account without a records bucket', async () => {
    const { result, cleaned } = await sweep(new Account([keyFor(1)], 0, []), { 1: closed });
    expect(cleaned).toEqual([]);
    expect(result.recorded).toEqual([]);
  });

  it('fails loudly when the account stays above 80% of its namespaces after collecting', async () => {
    const { result } = await sweep(new Account([keyFor(1)], 450), { 1: open });
    expect(result.namespaces.over).toBe(true);
    expect(sweepNeedsAttention(result)).toBe(true);
    expect(describeSweep(result)).toContain('450 of 500 Durable Object namespaces');
  });
});

describe('a deploy that hits the namespace limit', () => {
  it('says how to free namespaces', async () => {
    const runner: ProcessRunner = {
      run: async () => ({
        exitCode: 1,
        stdout: '',
        stderr: 'You have exceeded the limit of 500 Durable Object namespaces on your account. [code: 10067]',
      }),
    };
    const failure = wranglerJson(runner, ['deploy'], { cwd: '/' });
    await expect(failure).rejects.toThrow(DURABLE_OBJECT_LIMIT_HINT);
    await expect(failure).rejects.toThrow('PR Stage Sweep');
  });
});
