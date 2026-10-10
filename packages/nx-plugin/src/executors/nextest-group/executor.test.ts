import { describe, expect, it } from 'bun:test';
import {
  groupBudgetMs,
  MAX_TIMER_MS,
  NEXTEST_GROUP_PER_TEST_MS,
  type SelectedRunnableTests,
  selectedRunnableTestCount,
} from './executor.js';

describe('nextest-group count stage', () => {
  it('counts the matching, non-ignored tests of every suite and nothing else', () => {
    const listing = JSON.stringify({
      'test-count': 4,
      'rust-suites': {
        'cowshed-core::apfs_native_macos': {
          testcases: {
            'a::real_apfs_one': { ignored: false, 'filter-match': { status: 'matches' } },
            'a::real_apfs_two': { ignored: false, 'filter-match': { status: 'matches' } },
            'a::real_apfs_ignored': { ignored: true, 'filter-match': { status: 'matches' } },
            'a::unrelated': { ignored: false, 'filter-match': { status: 'mismatch', reason: 'filter' } },
          },
        },
        'cowshed-cli::command_dispatch': {
          testcases: {
            'b::real_apfs_three': { ignored: false, 'filter-match': { status: 'matches' } },
          },
        },
      },
    });
    expect(selectedRunnableTestCount(listing)).toEqual({ ok: true, count: 3 });
  });

  it('refuses a listing that is not JSON, so no bound is sized from it', () => {
    expect(selectedRunnableTestCount('')).toEqual({
      ok: false,
      error: expect.stringContaining('printed no JSON listing'),
    });
    expect(selectedRunnableTestCount('Compiling cowshed-core v0.1.0')).toEqual({
      ok: false,
      error: expect.stringContaining('printed no JSON listing'),
    });
  });

  it('refuses a listing with no rust-suites object', () => {
    expect(selectedRunnableTestCount('{"test-count": 0}')).toEqual({
      ok: false,
      error: 'nextest list JSON has no rust-suites object',
    });
  });

  it('refuses a listing that selects no runnable test, since a zero count has no bound', () => {
    const listing = JSON.stringify({
      'rust-suites': {
        'cowshed-core::apfs_native_macos': {
          testcases: {
            'a::real_apfs_ignored': { ignored: true, 'filter-match': { status: 'matches' } },
            'a::unrelated': { ignored: false, 'filter-match': { status: 'mismatch', reason: 'filter' } },
          },
        },
      },
    });
    expect(selectedRunnableTestCount(listing)).toEqual({
      ok: false,
      error: 'nextest list selects no runnable test',
    });
  });

  it('refuses a testcase without a boolean ignored flag, a filter-match, or a known filter-match status', () => {
    const malformed: SelectedRunnableTests = {
      ok: false,
      error: 'a nextest list testcase has no ignored flag or filter-match status',
    };
    const missingIgnored = JSON.stringify({
      'rust-suites': { s: { testcases: { 'a::t': { 'filter-match': { status: 'matches' } } } } },
    });
    const missingFilterMatch = JSON.stringify({
      'rust-suites': { s: { testcases: { 'a::t': { ignored: false } } } },
    });
    const unknownStatus = JSON.stringify({
      'rust-suites': { s: { testcases: { 'a::t': { ignored: false, 'filter-match': { status: 'maybe' } } } } },
    });
    expect(selectedRunnableTestCount(missingIgnored)).toEqual(malformed);
    expect(selectedRunnableTestCount(missingFilterMatch)).toEqual(malformed);
    expect(selectedRunnableTestCount(unknownStatus)).toEqual(malformed);
  });
});

describe('nextest-group budget', () => {
  it('sizes the run at the selected tests times the per-test share', () => {
    expect(groupBudgetMs(3)).toEqual({ ok: true, timeoutMs: 3 * NEXTEST_GROUP_PER_TEST_MS });
  });

  it('accepts the largest count a Node timer holds, and refuses one more rather than clamping', () => {
    const largest = Math.floor(MAX_TIMER_MS / NEXTEST_GROUP_PER_TEST_MS);
    expect(groupBudgetMs(largest)).toEqual({ ok: true, timeoutMs: largest * NEXTEST_GROUP_PER_TEST_MS });
    expect(groupBudgetMs(largest + 1)).toEqual({
      ok: false,
      error: expect.stringContaining('longer than a timer can hold'),
    });
  });
});
