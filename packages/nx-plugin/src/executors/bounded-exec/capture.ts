/**
 * What a `mutationReport` run's executor (`mutation-report.ts`) and its capture reader (`capture-reader.ts`) agree on:
 * the closed tool selector, each tool's fixed report leaf beneath the capture, the descriptor the capture is inherited
 * on, and the reader's exit for a run that wrote no report.
 */

/** Each tool's report beneath the capture, by path component: the directories the tool makes, then the file. */
export const CAPTURE_LEAVES = {
  // The shared Stryker config's `jsonReporter.fileName` when BOUNDED_EXEC_MUTATION_REPORT_DIR names the capture.
  stryker: ['mutation.json'],
  // cargo-mutants' `--output <capture>`.
  'cargo-mutants': ['mutants.out', 'outcomes.json'],
} as const satisfies Record<string, readonly [string, ...string[]]>;

export type MutationReportTool = keyof typeof CAPTURE_LEAVES;

export function isMutationReportTool(name: string | undefined): name is MutationReportTool {
  return name !== undefined && Object.hasOwn(CAPTURE_LEAVES, name);
}

/** The descriptor the capture reader inherits the capture directory on. */
export const CAPTURE_FD = 3;

/** The capture reader's exit when the run wrote no report. */
export const CAPTURE_READ_ABSENT = 3;
