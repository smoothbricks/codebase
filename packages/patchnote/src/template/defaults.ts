/**
 * Default PR body template.
 *
 * Each variable occupies its own line(s) with blank lines between sections.
 * When a variable resolves to empty string, collapseBlankLines() will
 * clean up the extra whitespace so empty sections vanish.
 *
 * Section order matches the current hardcoded assembly:
 * 1. provenanceWarnings (prepended in generateCommitData)
 * 2. aiSummary OR table (mutually exclusive -- AI mode vs fallback)
 * 3. nixUpdates (appended inside analyzeChangelogs)
 * 4. downgrades (appended inside analyzeChangelogs)
 * 5. releaseNotes (appended inside analyzeChangelogs / generateFallbackSummary)
 * 6. deprecationWarnings (appended in generateCommitData)
 */
export const DEFAULT_PR_BODY_TEMPLATE = `{{provenanceWarnings}}

{{aiSummary}}

{{table}}

{{nixUpdates}}

{{downgrades}}

{{releaseNotes}}

{{deprecationWarnings}}`;
