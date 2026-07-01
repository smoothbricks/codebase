/**
 * Shared detection of Git merge-conflict markers.
 *
 * Real conflict markers always sit at column 0 and are a 7-character run of
 * `<`, `|`, or `>` followed by a space (the diff3 base marker `|||||||` and the
 * ours/theirs markers `<<<<<<< ` / `>>>>>>> `). We deliberately do NOT match a
 * bare `=======` separator: it is ambiguous with Markdown h1 underlines and adds
 * no signal (a real conflict always carries the angle/pipe markers too). The
 * column-0 anchor also means quoted marker strings inside source (which are
 * indented) never false-positive.
 */
export const CONFLICT_MARKER_PATTERN = '^(<<<<<<< |\\|\\|\\|\\|\\|\\|\\| |>>>>>>> )';

const conflictMarkerRe = new RegExp(CONFLICT_MARKER_PATTERN);

export interface ConflictMarkerHit {
  /** Repo-relative path of the offending file. */
  readonly file: string;
  /** 1-based line numbers carrying a conflict marker. */
  readonly lines: number[];
}

/** 1-based line numbers of every conflict-marker line in `text`. */
export function findMarkerLines(text: string): number[] {
  const hits: number[] = [];
  const lines = text.split('\n');
  for (let i = 0; i < lines.length; i++) {
    if (conflictMarkerRe.test(lines[i])) {
      hits.push(i + 1);
    }
  }
  return hits;
}

export interface MarkerScanShell {
  runResult(
    command: string,
    args: string[],
    cwd: string,
  ): Promise<{ exitCode: number; stdout: string; stderr: string }>;
}

/**
 * Scan tracked files in the working tree for conflict markers via `git grep`.
 * `pathspecs` optionally restricts the scan (e.g. release package roots).
 */
export async function scanTrackedForMarkers(
  shell: MarkerScanShell,
  root: string,
  pathspecs: string[] = [],
): Promise<ConflictMarkerHit[]> {
  const args = ['grep', '-nI', '-E', CONFLICT_MARKER_PATTERN];
  if (pathspecs.length > 0) {
    args.push('--', ...pathspecs);
  }
  const { exitCode, stdout, stderr } = await shell.runResult('git', args, root);
  // git grep: 0 = matches found, 1 = no matches, >1 = error.
  if (exitCode > 1) {
    throw new Error(`git grep for conflict markers failed: ${stderr.trim() || `exit ${exitCode}`}`);
  }
  return parseGitGrep(stdout);
}

/** Thrown by {@link assertNoConflictMarkers} so callers/tests can inspect the hits. */
export class ConflictMarkersError extends Error {
  constructor(
    readonly hits: ConflictMarkerHit[],
    context: string,
  ) {
    super(`Refusing to ${context}: conflict markers found in ${hits.length} file(s):\n${formatMarkerHits(hits)}`);
    this.name = 'ConflictMarkersError';
  }
}

/**
 * Reusable publish/merge guard: throw {@link ConflictMarkersError} when any
 * tracked file carries conflict markers. `context` names the blocked operation
 * (e.g. `'publish'`) for the message.
 */
export async function assertNoConflictMarkers(shell: MarkerScanShell, root: string, context: string): Promise<void> {
  const hits = await scanTrackedForMarkers(shell, root);
  if (hits.length > 0) {
    throw new ConflictMarkersError(hits, context);
  }
}

/**
 * Scan the files changed between `baseRef` and `headRef` (three-dot, i.e. since
 * their merge base) for conflict markers, reading blobs at `headRef` — no
 * checkout or working-tree mutation required.
 */
export async function scanRefChangedForMarkers(
  shell: MarkerScanShell,
  root: string,
  baseRef: string,
  headRef: string,
): Promise<ConflictMarkerHit[]> {
  const diff = await shell.runResult('git', ['diff', '--name-only', `${baseRef}...${headRef}`], root);
  if (diff.exitCode !== 0) {
    throw new Error(`git diff ${baseRef}...${headRef} failed: ${diff.stderr.trim() || `exit ${diff.exitCode}`}`);
  }
  const files = diff.stdout.split('\n').filter((line) => line.length > 0);
  const hits: ConflictMarkerHit[] = [];
  for (const file of files) {
    const show = await shell.runResult('git', ['show', `${headRef}:${file}`, '--textconv'], root);
    if (show.exitCode !== 0) {
      continue; // deleted at head / binary / unreadable — not a marker source
    }
    const lines = findMarkerLines(show.stdout);
    if (lines.length > 0) {
      hits.push({ file, lines });
    }
  }
  return hits;
}

/** Parse `path:line:content` rows from `git grep -n` into per-file hits. */
export function parseGitGrep(stdout: string): ConflictMarkerHit[] {
  const byFile = new Map<string, number[]>();
  for (const row of stdout.split('\n')) {
    if (row.length === 0) {
      continue;
    }
    const firstColon = row.indexOf(':');
    if (firstColon < 0) {
      continue;
    }
    const secondColon = row.indexOf(':', firstColon + 1);
    if (secondColon < 0) {
      continue;
    }
    const file = row.slice(0, firstColon);
    const line = Number.parseInt(row.slice(firstColon + 1, secondColon), 10);
    if (!Number.isFinite(line)) {
      continue;
    }
    const existing = byFile.get(file);
    if (existing) {
      existing.push(line);
    } else {
      byFile.set(file, [line]);
    }
  }
  return [...byFile.entries()].map(([file, lines]) => ({ file, lines }));
}

/** Human-readable one-liner summary of marker hits, e.g. for CLI output. */
export function formatMarkerHits(hits: ConflictMarkerHit[]): string {
  return hits.map((hit) => `  ${hit.file} (lines ${hit.lines.join(', ')})`).join('\n');
}
