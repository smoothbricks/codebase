/**
 * Which of a run's bounds is armed, and when each expires: the decision bounded-exec's timers carry out, kept here as
 * a pure function of the run's events and the times they happened at, so a table can test it with no clock and no
 * process (verdict.ts says why the bounds are separate).
 *
 * - A run queued in its pool (`mbx reserve` before the grant) arms nothing, and nothing it reports moves a clock:
 *   every bound below starts at the grant instead of the spawn (specs/cowshed/05_gateway.md, "Parallelism inside a
 *   job": a wait for the pool is not the command's time).
 * - The total ceiling runs from the spawn.
 * - The fixture ceiling runs from the spawn while a supervised fixture runs, and ends with the fixture's segment.
 * - The progress bound runs only while the runner is testing (runner-lifecycle.ts): from the spawn when the runner is
 *   testing then (startup is bounded until a build declares itself), restarted by a finished test or by the clock
 *   starting again after a build, and stopped while nothing tests.
 */

/** The limits a run was given, in milliseconds; an absent one never expires. */
export interface BoundLimits {
  readonly idleMs?: number;
  readonly fixtureMs?: number;
  readonly totalMs?: number;
}

/**
 * When each armed bound expires, in the clock's milliseconds; `null` when it is not armed. `queued`: the run waits for
 * its pool's grant, so none is armed yet.
 */
export interface Bounds {
  readonly idle: number | null;
  readonly fixture: number | null;
  readonly total: number | null;
  readonly queued: boolean;
}

export type BoundKind = 'idle' | 'fixture' | 'total';

export type BoundEvent =
  /**
   * The command started; `testing` is the lifecycle's answer then, `fixture` whether a supervised fixture leads it,
   * `queued` whether it waits in its pool for a grant first.
   */
  | {
      readonly kind: 'spawn';
      readonly at: number;
      readonly testing: boolean;
      readonly fixture: boolean;
      readonly queued: boolean;
    }
  /** The pool granted a queued command: its bounds start now, as a spawn's would. */
  | { readonly kind: 'granted'; readonly at: number; readonly testing: boolean; readonly fixture: boolean }
  /**
   * The runner's state may have moved: a test finished (`finished`), a phase changed, or the lifecycle failed.
   * `wasTesting` is the answer before the event, `testing` the answer after it.
   */
  | {
      readonly kind: 'progress';
      readonly at: number;
      readonly testing: boolean;
      readonly wasTesting: boolean;
      readonly finished: boolean;
    }
  /** The fixture's segment ended: its ceiling no longer applies. */
  | { readonly kind: 'fixture-done' };

export const UNARMED: Bounds = { idle: null, fixture: null, total: null, queued: false };

/** The bounds a command that starts running at `at` arms. */
function armed(limits: BoundLimits, at: number, testing: boolean, fixture: boolean): Bounds {
  return {
    idle: testing && limits.idleMs !== undefined ? at + limits.idleMs : null,
    fixture: fixture && limits.fixtureMs !== undefined ? at + limits.fixtureMs : null,
    total: limits.totalMs !== undefined ? at + limits.totalMs : null,
    queued: false,
  };
}

/** The bounds after `event`. */
export function stepBounds(bounds: Bounds, limits: BoundLimits, event: BoundEvent): Bounds {
  switch (event.kind) {
    case 'spawn':
      return event.queued ? { ...UNARMED, queued: true } : armed(limits, event.at, event.testing, event.fixture);
    case 'granted':
      return bounds.queued ? armed(limits, event.at, event.testing, event.fixture) : bounds;
    case 'progress': {
      if (bounds.queued) return bounds;
      if (!event.testing) return { ...bounds, idle: null };
      const restart = event.finished || !event.wasTesting;
      if (!restart || limits.idleMs === undefined) return bounds;
      return { ...bounds, idle: event.at + limits.idleMs };
    }
    case 'fixture-done':
      return { ...bounds, fixture: null };
  }
}

/** The bound that expires first, or `null` when none is armed. A tie names the total, then the fixture, then idle. */
export function earliestBound(bounds: Bounds): { readonly kind: BoundKind; readonly at: number } | null {
  let earliest: { kind: BoundKind; at: number } | null = null;
  for (const kind of ['total', 'fixture', 'idle'] as const) {
    const at = bounds[kind];
    if (at !== null && (earliest === null || at < earliest.at)) earliest = { kind, at };
  }
  return earliest;
}
