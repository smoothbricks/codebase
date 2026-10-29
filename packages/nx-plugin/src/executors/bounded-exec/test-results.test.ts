import { expect, it } from 'bun:test';
import { mkdtemp, readFile, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { TestResultWriter } from './test-results.js';
import {
  createTestResultWriter,
  isClosedTestResult,
  parseTestSourceRef,
  TEST_FAILURES_FILE,
  TEST_RESULTS_FILE,
} from './test-results.js';
import { bunJunitResults } from './verdict.js';

it('publishes UTF-8 cause ranges and resumable cursors, and gives a replacement run a new identity', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'closed-results-'));
  const writers: TestResultWriter[] = [];
  try {
    const opened = await createTestResultWriter(directory, { cwd: join(directory, 'package'), root: directory });
    if (!opened.ok) throw new Error(opened.error);
    writers.push(opened.writer);
    const failureOutput = 'assertion é failed\n';
    expect(
      opened.writer.append({
        id: 'suite::é',
        status: 'fail',
        source: 'native',
        durationMs: 1.5,
        sourceRef: 'test.rs:7',
        failureOutput,
      }),
    ).toBeNull();
    const firstBytes = await readFile(join(directory, TEST_RESULTS_FILE));
    const first: unknown = JSON.parse(firstBytes.toString('utf8'));
    if (!isClosedTestResult(first)) throw new Error('writer emitted an invalid public record');
    expect(first.cursor).toBe(firstBytes.length);
    expect(first.sourceRef).toBe('package/test.rs:7');
    expect(parseTestSourceRef(first.sourceRef)).toEqual({ path: 'package/test.rs', line: 7 });
    expect(parseTestSourceRef('no-line')).toBeNull();
    expect(first.causeRef).toEqual({
      path: TEST_FAILURES_FILE,
      startByte: 0,
      endByte: Buffer.byteLength(failureOutput),
    });
    const causes = await readFile(join(directory, TEST_FAILURES_FILE));
    expect(causes.subarray(first.causeRef?.startByte, first.causeRef?.endByte).toString('utf8')).toBe(failureOutput);
    expect(
      opened.writer.append({
        id: 'suite::skip',
        status: 'skip',
        source: 'native',
        durationMs: null,
        sourceRef: null,
        failureOutput: null,
      }),
    ).toBeNull();
    const all = await readFile(join(directory, TEST_RESULTS_FILE));
    const second: unknown = JSON.parse(all.subarray(first.cursor).toString('utf8'));
    if (!isClosedTestResult(second)) throw new Error('writer emitted an invalid second record');
    expect(second.cursor).toBe(all.length);
    expect(second.runId).toBe(first.runId);
    expect(second.causeRef).toBeNull();
    const inode = (await stat(join(directory, TEST_RESULTS_FILE))).ino;
    const replaced = await createTestResultWriter(directory, { cwd: directory, root: directory });
    if (!replaced.ok) throw new Error(replaced.error);
    writers.push(replaced.writer);
    expect((await stat(join(directory, TEST_RESULTS_FILE))).ino).not.toBe(inode);
    expect(await readFile(join(directory, TEST_RESULTS_FILE), 'utf8')).toBe('');
    expect(
      replaced.writer.append({
        id: 'suite::pass',
        status: 'pass',
        source: 'native',
        durationMs: 0,
        sourceRef: null,
        failureOutput: null,
      }),
    ).toBeNull();
    const replacement: unknown = JSON.parse(await readFile(join(directory, TEST_RESULTS_FILE), 'utf8'));
    if (!isClosedTestResult(replacement)) throw new Error('invalid replacement record');
    expect(replacement.runId).not.toBe(first.runId);
    expect(await replaced.writer.close()).toBeNull();
    await rm(directory, { recursive: true });
    expect(
      replaced.writer.append({
        id: 'suite::lost',
        status: 'fail',
        source: 'native',
        durationMs: null,
        sourceRef: null,
        failureOutput: 'lost',
      }),
    ).toContain('appending test result suite::lost');
  } finally {
    try {
      expect(await Promise.all(writers.map((writer) => writer.close()))).toEqual(writers.map(() => null));
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  }
});

it('uses native Bun JUnit nesting and appends only missing results or status contradictions', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'closed-reconcile-'));
  const writers: TestResultWriter[] = [];
  try {
    const opened = await createTestResultWriter(directory, { cwd: directory, root: directory });
    if (!opened.ok) throw new Error(opened.error);
    writers.push(opened.writer);
    expect(
      opened.writer.append({
        id: 'case.test.ts::outer > inner > test',
        status: 'fail',
        source: 'progress',
        durationMs: 1,
        sourceRef: null,
        failureOutput: 'provisional',
      }),
    ).toBeNull();
    const firstBytes = await readFile(join(directory, TEST_RESULTS_FILE));
    const results = bunJunitResults(
      '<testsuites><testsuite name="case.test.ts" file="case.test.ts"><testsuite name="outer"><testsuite name="inner"><testcase name="test" classname="inner &gt; outer" file="case.test.ts" line="9" time="0.002"/><testcase name="skipped" file="case.test.ts" line="10"><skipped/></testcase></testsuite></testsuite></testsuite></testsuites>',
    );
    expect(results.map(({ id, sourceRef, status }) => ({ id, sourceRef, status }))).toEqual([
      { id: 'case.test.ts::outer > inner > test', sourceRef: 'case.test.ts:9', status: 'pass' },
      { id: 'case.test.ts::outer > inner > skipped', sourceRef: 'case.test.ts:10', status: 'skip' },
    ]);
    expect(opened.writer.reconcile(results)).toBeNull();
    const lines = (await readFile(join(directory, TEST_RESULTS_FILE), 'utf8')).trimEnd().split('\n');
    expect(lines).toHaveLength(3);
    const correction: unknown = JSON.parse(lines[1] ?? 'null');
    if (!isClosedTestResult(correction)) throw new Error('invalid correction record');
    expect(correction.source).toBe('junit');
    expect(correction.status).toBe('pass');
    expect(correction.contradictsCursor).toBe(firstBytes.length);
    expect(isClosedTestResult({ ...correction, cursor: -1 })).toBe(false);
    expect(isClosedTestResult({ ...correction, extra: true })).toBe(false);
    const failure = bunJunitResults(
      '<testsuite name="case.test.ts"><testcase name="failed" file="case.test.ts"><failure message="assertion"><![CDATA[error: é\n]]></failure></testcase></testsuite>',
    );
    expect(failure[0]?.failureOutput).toBe('error: é\n');
    expect(
      bunJunitResults(
        '<testsuite><testcase name="literal" file="f.test.ts"><failure><![CDATA[error: &lt; &#1114112;]]></failure></testcase></testsuite>',
      )[0]?.failureOutput,
    ).toBe('error: &lt; &#1114112;');
    expect(
      bunJunitResults(
        '<testsuite><testcase name="attribute" file="f.test.ts"><failure message="&amp;lt;"/></testcase></testsuite>',
      )[0]?.failureOutput,
    ).toBe('&lt;');
    expect(
      bunJunitResults(
        '<testsuite><testcase name="xml" file="f.test.ts"><failure><![CDATA[expected </failure> <!-- literal --> here]]></failure></testcase></testsuite>',
      )[0]?.failureOutput,
    ).toBe('expected </failure> <!-- literal --> here');
  } finally {
    try {
      expect(await Promise.all(writers.map((writer) => writer.close()))).toEqual(writers.map(() => null));
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  }
});

it('pins overlapping writers to separate artifact inodes and byte offsets', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'closed-overlap-'));
  const writers: TestResultWriter[] = [];
  try {
    const first = await createTestResultWriter(directory, { cwd: directory, root: directory });
    if (!first.ok) throw new Error(first.error);
    writers.push(first.writer);
    const second = await createTestResultWriter(directory, { cwd: directory, root: directory });
    if (!second.ok) throw new Error(second.error);
    writers.push(second.writer);
    expect(
      first.writer.append({
        id: 'suite::old',
        status: 'fail',
        source: 'native',
        durationMs: 1,
        sourceRef: null,
        failureOutput: 'old cause',
      }),
    ).toBeNull();
    expect(await readFile(join(directory, TEST_RESULTS_FILE), 'utf8')).toBe('');
    expect(await readFile(join(directory, TEST_FAILURES_FILE), 'utf8')).toBe('');
    expect(
      second.writer.append({
        id: 'suite::new',
        status: 'pass',
        source: 'native',
        durationMs: 2,
        sourceRef: null,
        failureOutput: null,
      }),
    ).toBeNull();
    const bytes = await readFile(join(directory, TEST_RESULTS_FILE));
    const record: unknown = JSON.parse(bytes.toString('utf8'));
    if (!isClosedTestResult(record)) throw new Error('invalid replacement record');
    expect(record.id).toBe('suite::new');
    expect(record.cursor).toBe(bytes.length);
  } finally {
    try {
      expect(await Promise.all(writers.map((writer) => writer.close()))).toEqual(writers.map(() => null));
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  }
});
