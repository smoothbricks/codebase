import typia, { type tags } from 'typia';

export const TEST_RESULTS_FILE = 'test-results.jsonl';
export const TEST_FAILURES_FILE = 'test-failures.log';

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

/** A producer-normalized workspace-relative declaration location, never inferred from a test name. */
export function parseTestSourceRef(sourceRef: string | null): { path: string; line: number } | null {
  if (sourceRef === null) return null;
  const match = /^(.+):([1-9]\d*)$/.exec(sourceRef);
  const path = match?.[1];
  const line = Number(match?.[2]);
  return path !== undefined && Number.isSafeInteger(line) ? { path, line } : null;
}
