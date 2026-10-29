import { randomUUID } from 'node:crypto';
import { appendFileSync } from 'node:fs';
import { type FileHandle, open, rename, rm } from 'node:fs/promises';
import { join, relative, resolve, sep } from 'node:path';
import typia, { type tags } from 'typia';

import { TEST_FAILURES_FILE, TEST_RESULTS_FILE } from './verdict.js';

export { TEST_FAILURES_FILE, TEST_RESULTS_FILE, taskDirectory } from './verdict.js';

type ByteOffset = number & tags.Type<'uint64'> & tags.Maximum<9007199254740991>;

/** Live observations, not gate evidence: report.xml and verdict.json remain authoritative. */
export interface ClosedTestResult {
  readonly runId: string & tags.Format<'uuid'>;
  readonly id: string;
  readonly status: 'pass' | 'fail' | 'skip';
  readonly source: 'native' | 'progress' | 'junit';
  /** Set only on a terminal JUnit correction of a provisional progress observation. */
  readonly contradictsCursor?: ByteOffset;
  /** Null when the native runner reports no duration (for example a skipped test). */
  readonly durationMs: (number & tags.Minimum<0>) | null;
  /** Native file:line, never a location inferred from human output. */
  readonly sourceRef: string | null;
  /** UTF-8 byte range [startByte, endByte), relative to the task artifact directory. */
  readonly causeRef: {
    readonly path: typeof TEST_FAILURES_FILE;
    readonly startByte: ByteOffset;
    readonly endByte: ByteOffset;
  } | null;
  /** Next UTF-8 byte offset in test-results.jsonl, including this record's LF. */
  readonly cursor: ByteOffset;
}

export const isClosedTestResult = typia.createEquals<ClosedTestResult>();

/** A runner's closed observation, before the artifact writer assigns its references. */
export interface NativeTestResult {
  readonly id: string;
  readonly status: ClosedTestResult['status'];
  readonly source: ClosedTestResult['source'];
  readonly contradictsCursor?: number;
  readonly durationMs: number | null;
  readonly sourceRef: string | null;
  readonly failureOutput: string | null;
}

export interface TestResultWriter {
  /** Completes the evidence write before publishing its referring JSONL record. */
  append(result: NativeTestResult): string | null;
  reconcile(results: readonly NativeTestResult[]): string | null;
  close(): Promise<string | null>;
}

/** The cursor includes its own decimal digits: converge on their width before publishing. */
function resultLine(result: Omit<ClosedTestResult, 'cursor'>, offset: number): { line: string; cursor: number } {
  let cursor = offset;
  for (;;) {
    const line = `${JSON.stringify({ ...result, cursor })}\n`;
    const next = offset + Buffer.byteLength(line);
    if (next === cursor) return { line, cursor };
    cursor = next;
  }
}

/** A producer-normalized workspace-relative declaration location, never inferred from a test name. */
export function parseTestSourceRef(sourceRef: string | null): { path: string; line: number } | null {
  if (sourceRef === null) return null;
  const match = /^(.+):([1-9]\d*)$/.exec(sourceRef);
  const path = match?.[1];
  const line = Number(match?.[2]);
  return path !== undefined && Number.isSafeInteger(line) ? { path, line } : null;
}

function normalizeSourceRef(sourceRef: string | null, sourceRoot: { cwd: string; root: string }): string | null {
  const location = parseTestSourceRef(sourceRef);
  if (location === null) return null;
  return `${relative(sourceRoot.root, resolve(sourceRoot.cwd, location.path)).split(sep).join('/')}:${location.line}`;
}
async function closeResultFiles(
  files: readonly FileHandle[],
  stagedPaths: readonly string[] = [],
): Promise<string | null> {
  const results = await Promise.allSettled([
    ...files.map((file) => file.close()),
    ...stagedPaths.map((path) => rm(path, { force: true })),
  ]);
  const failures = results.flatMap((result) => (result.status === 'rejected' ? [String(result.reason)] : []));
  return failures.length === 0 ? null : `closing test result artifacts: ${failures.join('; ')}`;
}
/** A new inode and UUID identify a replacement run, even when it has the same byte length. */
export async function createTestResultWriter(
  directory: string,
  sourceRoot: { readonly cwd: string; readonly root: string },
): Promise<{ readonly ok: true; readonly writer: TestResultWriter } | { readonly ok: false; readonly error: string }> {
  const runId = randomUUID();
  const target = join(directory, TEST_RESULTS_FILE);
  const staged = `${target}.${runId}`;
  const causePath = join(directory, TEST_FAILURES_FILE);
  const stagedCauses = `${causePath}.${runId}`;
  const [records, causes] = await Promise.allSettled([open(staged, 'ax'), open(stagedCauses, 'ax')]);
  if (records.status !== 'fulfilled' || causes.status !== 'fulfilled') {
    const cleanup = await closeResultFiles(
      [
        ...(records.status === 'fulfilled' ? [records.value] : []),
        ...(causes.status === 'fulfilled' ? [causes.value] : []),
      ],
      [staged, stagedCauses],
    );
    const failures = [records, causes].flatMap((result) =>
      result.status === 'rejected' ? [String(result.reason)] : [],
    );
    return {
      ok: false,
      error: `initializing test result stream ${target}: ${failures.join('; ')}${cleanup === null ? '' : `; ${cleanup}`}`,
    };
  }
  try {
    await rename(stagedCauses, causePath);
    await rename(staged, target);
  } catch (cause) {
    const cleanup = await closeResultFiles([records.value, causes.value], [staged, stagedCauses]);
    return {
      ok: false,
      error: `initializing test result stream ${target}: ${String(cause)}${cleanup === null ? '' : `; ${cleanup}`}`,
    };
  }
  let offset = 0;
  let causeOffset = 0;
  let failure: string | null = null;
  let closing: Promise<string | null> | null = null;
  const provisional = new Map<string, { status: ClosedTestResult['status']; cursor: number }>();
  return {
    ok: true,
    writer: {
      append(result) {
        if (failure !== null) return failure;
        if (closing !== null) return `appending test result ${result.id} to ${target}: stream is closed`;
        try {
          let causeRef: ClosedTestResult['causeRef'] = null;
          if (result.status === 'fail' && result.failureOutput !== null) {
            const endByte = causeOffset + Buffer.byteLength(result.failureOutput);
            appendFileSync(causes.value.fd, result.failureOutput);
            causeRef = { path: TEST_FAILURES_FILE, startByte: causeOffset, endByte };
            causeOffset = endByte;
          }
          const { line, cursor } = resultLine(
            {
              runId,
              id: result.id,
              status: result.status,
              source: result.source,
              ...(result.contradictsCursor === undefined ? {} : { contradictsCursor: result.contradictsCursor }),
              durationMs: result.durationMs,
              sourceRef: normalizeSourceRef(result.sourceRef, sourceRoot),
              causeRef,
            },
            offset,
          );
          // appendFileSync has no user-space buffering: each LF is visible before append returns.
          appendFileSync(records.value.fd, line);
          offset = cursor;
          if (result.source === 'progress') provisional.set(result.id, { status: result.status, cursor });
          return null;
        } catch (cause) {
          failure = `appending test result ${result.id} to ${target}: ${String(cause)}`;
          return failure;
        }
      },
      reconcile(results) {
        for (const result of results) {
          const prior = provisional.get(result.id);
          if (prior !== undefined && prior.status === result.status) continue;
          const error = this.append({
            ...result,
            source: 'junit',
            ...(prior === undefined ? {} : { contradictsCursor: prior.cursor }),
          });
          if (error !== null) return error;
        }
        return null;
      },
      close() {
        closing ??= closeResultFiles([records.value, causes.value]);
        return closing;
      },
    },
  };
}
