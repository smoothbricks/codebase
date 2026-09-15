import {
  extractInlineLocalBlocks,
  ManagedContentConflict,
  reinsertInlineLocalBlocks,
  splitLocalSection,
} from './managed-content.js';

export interface ManagedContent {
  readonly content: string;
  readonly executable: boolean;
}

export interface ManagedFileSpec {
  readonly target: string;
  /** null disables this descriptor; it does not authorize deletion of an existing file. */
  readonly desired: ManagedContent | null;
}

export type ManagedSnapshot =
  | { readonly kind: 'missing' }
  | ({ readonly kind: 'file' } & ManagedContent)
  | ({ readonly kind: 'symlink'; readonly destination: string } & ManagedContent)
  | { readonly kind: 'blocked'; readonly reason: string };

export interface ManagedFileInput extends ManagedFileSpec {
  readonly current: ManagedSnapshot;
}

export interface FileResult {
  target: string;
  action: 'created' | 'updated' | 'unchanged' | 'skipped' | 'skipped-symlink' | 'drifted' | 'ok-symlink';
  reason?: string;
}

export interface ManagedWrite extends ManagedContent {
  readonly target: string;
}

export interface ManagedPlan {
  readonly results: readonly FileResult[];
  readonly writes: readonly ManagedWrite[];
  readonly conflicts: readonly { readonly target: string; readonly reason: string }[];
}

/** Canonical, workspace-relative paths only; usable by both a filesystem and a virtual Tree adapter. */
export function isManagedTarget(target: string): boolean {
  return (
    target.length > 0 &&
    !target.includes('\0') &&
    !/[\\:]/.test(target) &&
    target.split('/').every((part) => part !== '' && part !== '.' && part !== '..')
  );
}

/**
 * One desired-state computation for update, check and diff. No I/O or mode
 * branching: callers decide whether to apply its writes. Conflicts invalidate
 * the whole write set rather than leaving a usable prefix of a failed plan.
 */
export function planManagedFiles(files: readonly ManagedFileInput[]): ManagedPlan {
  const results: FileResult[] = [];
  const writes: ManagedWrite[] = [];
  const conflicts: { target: string; reason: string }[] = [];
  const targets = new Set<string>();
  const declaredTargets = new Set(files.map(({ target }) => target));
  const conflict = (target: string, reason: string) => {
    conflicts.push({ target, reason });
    results.push({ target, action: 'drifted', reason });
  };
  for (const { target, desired, current } of files) {
    if (!isManagedTarget(target)) {
      conflict(target, 'expected a canonical workspace-relative path');
      continue;
    }
    if (targets.has(target)) {
      conflict(target, 'duplicate managed target');
      continue;
    }
    targets.add(target);
    const parts = target.split('/');
    if (parts.some((_, index) => index > 0 && declaredTargets.has(parts.slice(0, index).join('/')))) {
      conflict(target, 'a managed file is also declared as a parent directory');
      continue;
    }
    if (desired === null) {
      results.push({ target, action: 'skipped' });
      continue;
    }
    if (current.kind === 'blocked') {
      conflict(target, current.reason);
      continue;
    }
    if (current.kind === 'missing') {
      results.push({ target, action: 'created' });
      writes.push({ target, ...desired });
      continue;
    }
    try {
      const { managed, localTail } = splitLocalSection(current.content);
      const { withoutInline, blocks } = extractInlineLocalBlocks(managed);
      // Check anchors even in read-only modes and when the managed bytes match.
      const rendered = reinsertInlineLocalBlocks(desired.content, blocks);
      const contentMatches =
        withoutInline === desired.content || (localTail !== '' && withoutInline === `${desired.content}\n`);
      const modeMatches = current.executable === desired.executable;
      if (current.kind === 'symlink') {
        if (contentMatches && modeMatches) {
          results.push({ target, action: 'ok-symlink' });
        } else {
          conflict(target, 'symlink content or executable mode differs; update its source explicitly');
        }
        continue;
      }
      if (contentMatches && modeMatches) {
        results.push({ target, action: 'unchanged' });
        continue;
      }
      results.push({ target, action: 'updated' });
      writes.push({
        target,
        content: contentMatches ? current.content : localTail === '' ? rendered : `${rendered}\n${localTail}`,
        executable: desired.executable,
      });
    } catch (error) {
      if (!(error instanceof ManagedContentConflict)) throw error;
      conflict(target, error.message);
    }
  }
  return { results, writes: conflicts.length === 0 ? writes : [], conflicts };
}
